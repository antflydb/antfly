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

//! Parquet compact-Thrift footer metadata parsing.
//!
//! This is the bridge from validated Parquet footer bytes to Antfly's external
//! inventory row-group and column-chunk model. Data page decoding is separate.

const std = @import("std");
const Allocator = std.mem.Allocator;
const bounded_decode = @import("../bounded_decode.zig");
const external_source = @import("../external_source/types.zig");
const external_binding = @import("../external_source/catalog_binding.zig");
const lake_scan_plan = @import("lake_scan_plan.zig");
const range_io = @import("lake_range_io.zig");

pub const DecodeLimits = struct {
    max_footer_bytes: usize = range_io.max_parquet_footer_metadata_bytes,
    max_struct_allocation_bytes: usize = 256 * 1024 * 1024,
    max_elements: usize = 1_000_000,
    max_skip_operations: usize = 4_000_000,
    max_nesting_depth: usize = 64,

    pub fn validate(self: DecodeLimits) !void {
        if (self.max_footer_bytes == 0 or
            self.max_struct_allocation_bytes == 0 or
            self.max_elements == 0 or
            self.max_skip_operations == 0 or
            self.max_nesting_depth == 0)
        {
            return error.InvalidParquetMetadataLimits;
        }
    }
};

const CompactType = enum(u4) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
};

fn decodeCompactType(raw: u8) !CompactType {
    if (raw > @intFromEnum(CompactType.struct_)) return error.InvalidParquetMetadata;
    return @enumFromInt(raw);
}

pub const ParsedFooter = struct {
    version: i32,
    row_count: u64,
    row_groups: []external_source.RowGroup,

    pub fn deinit(self: *ParsedFooter, alloc: Allocator) void {
        for (self.row_groups) |*row_group| row_group.deinit(alloc);
        alloc.free(self.row_groups);
        self.* = undefined;
    }
};

pub const FileFooter = struct {
    file_id: []const u8,
    footer: ParsedFooter,
};

pub fn parseFooterMetadataAlloc(
    alloc: Allocator,
    footer_metadata: []const u8,
    file_len: u64,
) !ParsedFooter {
    return try parseFooterMetadataAllocWithLimits(alloc, footer_metadata, file_len, .{});
}

pub fn parseFooterMetadataAllocWithLimits(
    alloc: Allocator,
    footer_metadata: []const u8,
    file_len: u64,
    limits: DecodeLimits,
) !ParsedFooter {
    try limits.validate();
    if (footer_metadata.len == 0) return error.InvalidParquetMetadata;
    var reader = Reader{
        .bytes = footer_metadata,
        .budget = bounded_decode.Budget.init(footer_metadata.len, .{
            .max_artifact_bytes = limits.max_footer_bytes,
            .max_allocation_bytes = limits.max_struct_allocation_bytes,
            .max_elements = limits.max_elements,
        }) catch |err| return mapDecodeLimitError(err),
        .remaining_skip_operations = limits.max_skip_operations,
        .max_nesting_depth = limits.max_nesting_depth,
    };
    var footer = try parseFileMetadata(alloc, &reader, file_len);
    errdefer footer.deinit(alloc);
    if (reader.cursor != reader.bytes.len) return error.InvalidParquetMetadata;
    return footer;
}

fn mapDecodeLimitError(err: anyerror) anyerror {
    return switch (err) {
        error.DecodedArtifactTooLarge => error.ParquetMetadataTooLarge,
        error.InvalidEncodedCount => error.InvalidParquetMetadata,
        else => err,
    };
}

pub fn enrichInventoryFilesWithFootersAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    footers: []const FileFooter,
) !external_source.Inventory {
    try inventory.validate();
    if (inventory.format != .parquet and inventory.format != .iceberg) return error.InvalidParquetMetadata;
    if (footers.len == 0) return error.InvalidParquetMetadata;
    for (footers, 0..) |entry, idx| {
        if (entry.file_id.len == 0) return error.InvalidParquetMetadata;
        if (entry.footer.row_count == 0) return error.InvalidParquetMetadata;
        for (footers[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.file_id, entry.file_id)) return error.InvalidParquetMetadata;
        }
    }

    const files = try alloc.alloc(external_source.FileEntry, inventory.files.len);
    errdefer alloc.free(files);
    var initialized: usize = 0;
    errdefer {
        for (files[0..initialized]) |*file| file.deinit(alloc);
    }

    var matched_footers = try alloc.alloc(bool, footers.len);
    defer alloc.free(matched_footers);
    @memset(matched_footers, false);

    for (inventory.files, 0..) |file, idx| {
        if (footerForFile(footers, file.file_id)) |match| {
            files[idx] = try cloneFileWithFooterAlloc(alloc, file, match.footer);
            matched_footers[match.idx] = true;
        } else {
            files[idx] = try cloneFileAlloc(alloc, file);
        }
        initialized += 1;
    }
    for (matched_footers) |matched| {
        if (!matched) return error.ParquetInventoryFileNotFound;
    }

    const source_id = try alloc.dupe(u8, inventory.source_id);
    errdefer alloc.free(source_id);
    const source_uri = try alloc.dupe(u8, inventory.source_uri);
    errdefer alloc.free(source_uri);
    const snapshot_id = try alloc.dupe(u8, inventory.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, inventory.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);

    var out = external_source.Inventory{
        .format = inventory.format,
        .source_id = source_id,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .files = files,
    };
    errdefer out.deinit(alloc);
    try out.validate();
    return out;
}

pub fn enrichInventoryFileWithFooterAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    footer: ParsedFooter,
) !external_source.Inventory {
    try inventory.validate();
    if (inventory.format != .parquet and inventory.format != .iceberg) return error.InvalidParquetMetadata;
    if (file_id.len == 0) return error.InvalidParquetMetadata;
    if (footer.row_count == 0) return error.InvalidParquetMetadata;

    const files = try alloc.alloc(external_source.FileEntry, inventory.files.len);
    errdefer alloc.free(files);
    var initialized: usize = 0;
    errdefer {
        for (files[0..initialized]) |*file| file.deinit(alloc);
    }

    var found = false;
    for (inventory.files, 0..) |file, idx| {
        if (std.mem.eql(u8, file.file_id, file_id)) {
            files[idx] = try cloneFileWithFooterAlloc(alloc, file, footer);
            found = true;
        } else {
            files[idx] = try cloneFileAlloc(alloc, file);
        }
        initialized += 1;
    }
    if (!found) return error.ParquetInventoryFileNotFound;

    const source_id = try alloc.dupe(u8, inventory.source_id);
    errdefer alloc.free(source_id);
    const source_uri = try alloc.dupe(u8, inventory.source_uri);
    errdefer alloc.free(source_uri);
    const snapshot_id = try alloc.dupe(u8, inventory.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, inventory.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);

    var out = external_source.Inventory{
        .format = inventory.format,
        .source_id = source_id,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .files = files,
    };
    errdefer out.deinit(alloc);
    try out.validate();
    return out;
}

fn footerForFile(footers: []const FileFooter, file_id: []const u8) ?struct { idx: usize, footer: ParsedFooter } {
    for (footers, 0..) |entry, idx| {
        if (std.mem.eql(u8, entry.file_id, file_id)) return .{ .idx = idx, .footer = entry.footer };
    }
    return null;
}

fn cloneFileAlloc(alloc: Allocator, file: external_source.FileEntry) !external_source.FileEntry {
    const file_id = try alloc.dupe(u8, file.file_id);
    errdefer alloc.free(file_id);
    const object_uri = try alloc.dupe(u8, file.object_uri);
    errdefer alloc.free(object_uri);
    const etag: []u8 = if (file.etag.len == 0) &.{} else try alloc.dupe(u8, file.etag);
    errdefer if (etag.len > 0) alloc.free(etag);
    const version_id: []u8 = if (file.version_id.len == 0) &.{} else try alloc.dupe(u8, file.version_id);
    errdefer if (version_id.len > 0) alloc.free(version_id);
    const row_groups = try cloneRowGroupsAlloc(alloc, file.row_groups);
    errdefer freeMaybeOwnedRowGroups(alloc, row_groups);
    const partition_values = try clonePartitionValuesAlloc(alloc, file.partition_values);
    errdefer freeMaybeOwnedPartitionValues(alloc, partition_values);

    return .{
        .file_id = file_id,
        .object_uri = object_uri,
        .etag = etag,
        .version_id = version_id,
        .byte_len = file.byte_len,
        .row_count = file.row_count,
        .data_sequence_number = file.data_sequence_number,
        .partition_spec_id = file.partition_spec_id,
        .partition_field_count = file.partition_field_count,
        .partition_values = partition_values,
        .row_groups = row_groups,
    };
}

fn cloneFileWithFooterAlloc(
    alloc: Allocator,
    file: external_source.FileEntry,
    footer: ParsedFooter,
) !external_source.FileEntry {
    const file_id = try alloc.dupe(u8, file.file_id);
    errdefer alloc.free(file_id);
    const object_uri = try alloc.dupe(u8, file.object_uri);
    errdefer alloc.free(object_uri);
    const etag: []u8 = if (file.etag.len == 0) &.{} else try alloc.dupe(u8, file.etag);
    errdefer if (etag.len > 0) alloc.free(etag);
    const version_id: []u8 = if (file.version_id.len == 0) &.{} else try alloc.dupe(u8, file.version_id);
    errdefer if (version_id.len > 0) alloc.free(version_id);
    const row_groups = try cloneRowGroupsAlloc(alloc, footer.row_groups);
    errdefer freeMaybeOwnedRowGroups(alloc, row_groups);
    const partition_values = try clonePartitionValuesAlloc(alloc, file.partition_values);
    errdefer freeMaybeOwnedPartitionValues(alloc, partition_values);

    return .{
        .file_id = file_id,
        .object_uri = object_uri,
        .etag = etag,
        .version_id = version_id,
        .byte_len = file.byte_len,
        .row_count = footer.row_count,
        .data_sequence_number = file.data_sequence_number,
        .partition_spec_id = file.partition_spec_id,
        .partition_field_count = file.partition_field_count,
        .partition_values = partition_values,
        .row_groups = row_groups,
    };
}

fn clonePartitionValuesAlloc(
    alloc: Allocator,
    values: []const external_source.PartitionValue,
) ![]external_source.PartitionValue {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(external_source.PartitionValue, values.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer for (out[0..initialized]) |*value| value.deinit(alloc);
    for (values, 0..) |value, idx| {
        const column_id = try alloc.dupe(u8, value.column_id);
        errdefer alloc.free(column_id);
        out[idx] = .{
            .column_id = column_id,
            .string_value = try alloc.dupe(u8, value.string_value),
        };
        initialized += 1;
    }
    return out;
}

fn freeMaybeOwnedPartitionValues(alloc: Allocator, values: []external_source.PartitionValue) void {
    for (values) |*value| value.deinit(alloc);
    if (values.len > 0) alloc.free(values);
}

fn freeMaybeOwnedRowGroups(alloc: Allocator, row_groups: []external_source.RowGroup) void {
    for (row_groups) |*row_group| row_group.deinit(alloc);
    if (row_groups.len > 0) alloc.free(row_groups);
}

fn cloneRowGroupsAlloc(alloc: Allocator, row_groups: []const external_source.RowGroup) ![]external_source.RowGroup {
    if (row_groups.len == 0) return &.{};
    const out = try alloc.alloc(external_source.RowGroup, row_groups.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*row_group| row_group.deinit(alloc);
    }
    for (row_groups, 0..) |row_group, idx| {
        out[idx] = .{
            .ordinal = row_group.ordinal,
            .row_count = row_group.row_count,
            .file_offset = row_group.file_offset,
            .total_byte_len = row_group.total_byte_len,
            .column_chunks = try cloneColumnChunksAlloc(alloc, row_group.column_chunks),
        };
        initialized += 1;
    }
    return out;
}

fn cloneColumnChunksAlloc(alloc: Allocator, chunks: []const external_source.ColumnChunk) ![]external_source.ColumnChunk {
    if (chunks.len == 0) return &.{};
    const out = try alloc.alloc(external_source.ColumnChunk, chunks.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*chunk| chunk.deinit(alloc);
    }
    for (chunks, 0..) |chunk, idx| {
        out[idx] = try cloneColumnChunkAlloc(alloc, chunk);
        initialized += 1;
    }
    return out;
}

fn cloneColumnChunkAlloc(alloc: Allocator, chunk: external_source.ColumnChunk) !external_source.ColumnChunk {
    const column_id = try alloc.dupe(u8, chunk.column_id);
    errdefer alloc.free(column_id);
    const compression_codec: []u8 = if (chunk.compression_codec.len == 0) &.{} else try alloc.dupe(u8, chunk.compression_codec);
    errdefer if (compression_codec.len > 0) alloc.free(compression_codec);
    const encoding: []u8 = if (chunk.encoding.len == 0) &.{} else try alloc.dupe(u8, chunk.encoding);
    errdefer if (encoding.len > 0) alloc.free(encoding);
    const physical_type: []u8 = if (chunk.physical_type.len == 0) &.{} else try alloc.dupe(u8, chunk.physical_type);
    errdefer if (physical_type.len > 0) alloc.free(physical_type);
    const logical_type: []u8 = if (chunk.logical_type.len == 0) &.{} else try alloc.dupe(u8, chunk.logical_type);
    errdefer if (logical_type.len > 0) alloc.free(logical_type);
    const stats_min_bytes: ?[]u8 = if (chunk.stats_min_bytes) |value| try alloc.dupe(u8, value) else null;
    errdefer if (stats_min_bytes) |value| alloc.free(value);
    const stats_max_bytes: ?[]u8 = if (chunk.stats_max_bytes) |value| try alloc.dupe(u8, value) else null;
    errdefer if (stats_max_bytes) |value| alloc.free(value);
    return .{
        .column_id = column_id,
        .file_offset = chunk.file_offset,
        .compressed_len = chunk.compressed_len,
        .uncompressed_len = chunk.uncompressed_len,
        .compression_codec = compression_codec,
        .encoding = encoding,
        .physical_type = physical_type,
        .type_length = chunk.type_length,
        .logical_type = logical_type,
        .decimal_precision = chunk.decimal_precision,
        .decimal_scale = chunk.decimal_scale,
        .stats_min_i64 = chunk.stats_min_i64,
        .stats_max_i64 = chunk.stats_max_i64,
        .stats_min_bytes = stats_min_bytes,
        .stats_max_bytes = stats_max_bytes,
        .stats_min_bool = chunk.stats_min_bool,
        .stats_max_bool = chunk.stats_max_bool,
        .stats_min_f64 = chunk.stats_min_f64,
        .stats_max_f64 = chunk.stats_max_f64,
        .nullable = chunk.nullable,
        .field_id = chunk.field_id,
    };
}

fn parseFileMetadata(alloc: Allocator, reader: *Reader, file_len: u64) !ParsedFooter {
    var previous_field_id: i16 = 0;
    var version: ?i32 = null;
    var row_count: ?u64 = null;
    var schema_columns: ?[]SchemaColumn = null;
    var row_groups: ?[]external_source.RowGroup = null;
    errdefer if (schema_columns) |columns| freeSchemaColumns(alloc, columns);
    errdefer if (row_groups) |groups| {
        for (groups) |*group| group.deinit(alloc);
        alloc.free(groups);
    };

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => version = try reader.readRequiredI32(field.type),
            2 => schema_columns = try parseSchemaColumnsAlloc(alloc, reader, field.type),
            3 => row_count = try reader.readRequiredU64(field.type),
            4 => {
                if (field.type != .list) return error.InvalidParquetMetadata;
                row_groups = try parseRowGroupList(alloc, reader, file_len);
            },
            else => try reader.skip(field.type),
        }
    }

    const got_version = version orelse return error.InvalidParquetMetadata;
    const got_row_count = row_count orelse return error.InvalidParquetMetadata;
    const got_row_groups = row_groups orelse return error.InvalidParquetMetadata;
    if (schema_columns) |columns| {
        try applySchemaNullability(alloc, got_row_groups, columns);
        freeSchemaColumns(alloc, columns);
        schema_columns = null;
    }

    var total_rows: u64 = 0;
    for (got_row_groups) |group| {
        total_rows = std.math.add(u64, total_rows, group.row_count) catch return error.InvalidParquetMetadata;
    }
    if (got_row_groups.len != 0 and total_rows != got_row_count) return error.InvalidParquetMetadata;
    row_groups = null;

    return .{
        .version = got_version,
        .row_count = got_row_count,
        .row_groups = got_row_groups,
    };
}

const SchemaElement = struct {
    name: []u8,
    repetition_type: ?i32 = null,
    type_length: i32 = 0,
    child_count: u32 = 0,
    logical_type: []u8 = &.{},
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
    field_id: ?i32 = null,

    fn deinit(self: *SchemaElement, alloc: Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        if (self.logical_type.len > 0) alloc.free(self.logical_type);
        self.* = undefined;
    }
};

const SchemaColumn = struct {
    column_id: []u8,
    nullable: bool,
    type_length: i32 = 0,
    logical_type: []u8 = &.{},
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
    field_id: ?i32 = null,

    fn deinit(self: *SchemaColumn, alloc: Allocator) void {
        if (self.column_id.len > 0) alloc.free(self.column_id);
        if (self.logical_type.len > 0) alloc.free(self.logical_type);
        self.* = undefined;
    }
};

fn parseSchemaColumnsAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]SchemaColumn {
    if (field_type != .list) return error.InvalidParquetMetadata;
    const list = try reader.readListHeader();
    if (list.elem_type != .struct_) return error.InvalidParquetMetadata;
    if (list.len == 0) return error.InvalidParquetMetadata;

    try reader.admitAllocation(SchemaElement, list.len);
    // A flattened schema cannot contain more leaf columns than elements.
    try reader.admitAllocation(SchemaColumn, list.len);
    try reader.admitAllocation([]const u8, list.len);
    const elements = try alloc.alloc(SchemaElement, list.len);
    errdefer alloc.free(elements);
    var initialized: usize = 0;
    errdefer {
        for (elements[0..initialized]) |*element| element.deinit(alloc);
    }
    for (elements) |*element| {
        element.* = try parseSchemaElement(alloc, reader);
        initialized += 1;
    }

    var columns = std.ArrayListUnmanaged(SchemaColumn).empty;
    errdefer {
        for (columns.items) |*column| column.deinit(alloc);
        columns.deinit(alloc);
    }

    var path = std.ArrayListUnmanaged([]const u8).empty;
    defer path.deinit(alloc);
    var cursor: usize = 1;
    try collectSchemaColumnsAlloc(alloc, reader, elements, &cursor, elements[0].child_count, false, &path, &columns, 0);
    if (cursor != elements.len) return error.InvalidParquetMetadata;

    for (elements) |*element| element.deinit(alloc);
    alloc.free(elements);
    return try columns.toOwnedSlice(alloc);
}

fn parseSchemaElement(alloc: Allocator, reader: *Reader) !SchemaElement {
    var previous_field_id: i16 = 0;
    var name: ?[]u8 = null;
    var repetition_type: ?i32 = null;
    var type_length: i32 = 0;
    var child_count: u32 = 0;
    var logical_type: []u8 = &.{};
    var decimal_precision: i32 = 0;
    var decimal_scale: i32 = 0;
    var field_id: ?i32 = null;
    errdefer if (name) |value| alloc.free(value);
    errdefer if (logical_type.len > 0) alloc.free(logical_type);

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            2 => type_length = try reader.readRequiredI32(field.type),
            3 => repetition_type = try reader.readRequiredI32(field.type),
            4 => {
                if (field.type != .binary) return error.InvalidParquetMetadata;
                name = try reader.readBinaryAlloc(alloc);
            },
            5 => child_count = @intCast(try reader.readRequiredI32NonNegative(field.type)),
            6 => {
                if (logical_type.len > 0) {
                    alloc.free(logical_type);
                    logical_type = &.{};
                }
                logical_type = try logicalTypeNameForConvertedTypeAlloc(alloc, try reader.readRequiredI32(field.type));
            },
            7 => decimal_scale = try reader.readRequiredI32(field.type),
            8 => decimal_precision = try reader.readRequiredI32(field.type),
            9 => field_id = try reader.readRequiredI32(field.type),
            10 => {
                if (logical_type.len > 0) {
                    alloc.free(logical_type);
                    logical_type = &.{};
                }
                const logical_annotation = try parseLogicalTypeAnnotationAlloc(alloc, reader, field.type);
                logical_type = logical_annotation.name;
                decimal_precision = logical_annotation.decimal_precision;
                decimal_scale = logical_annotation.decimal_scale;
            },
            else => try reader.skip(field.type),
        }
    }

    const got_name = name orelse return error.InvalidParquetMetadata;
    if (got_name.len == 0) return error.InvalidParquetMetadata;
    name = null;
    return .{
        .name = got_name,
        .repetition_type = repetition_type,
        .type_length = type_length,
        .child_count = child_count,
        .logical_type = logical_type,
        .decimal_precision = decimal_precision,
        .decimal_scale = decimal_scale,
        .field_id = field_id,
    };
}

fn collectSchemaColumnsAlloc(
    alloc: Allocator,
    reader: *Reader,
    elements: []const SchemaElement,
    cursor: *usize,
    child_count: u32,
    inherited_nullable: bool,
    path: *std.ArrayListUnmanaged([]const u8),
    columns: *std.ArrayListUnmanaged(SchemaColumn),
    depth: usize,
) !void {
    if (depth >= reader.max_nesting_depth) return error.ParquetMetadataTooLarge;
    for (0..child_count) |_| {
        if (cursor.* >= elements.len) return error.InvalidParquetMetadata;
        const element = elements[cursor.*];
        cursor.* += 1;

        const element_nullable = inherited_nullable or isNullableRepetition(element.repetition_type);
        try path.append(alloc, element.name);
        defer _ = path.pop();

        if (element.child_count == 0) {
            const column_id = try joinSchemaPathAlloc(alloc, reader, path.items);
            errdefer alloc.free(column_id);
            const logical_type: []u8 = if (element.logical_type.len == 0) &.{} else blk: {
                try reader.admitAllocation(u8, element.logical_type.len);
                break :blk try alloc.dupe(u8, element.logical_type);
            };
            errdefer if (logical_type.len > 0) alloc.free(logical_type);
            try columns.append(alloc, .{
                .column_id = column_id,
                .nullable = element_nullable,
                .type_length = element.type_length,
                .logical_type = logical_type,
                .decimal_precision = element.decimal_precision,
                .decimal_scale = element.decimal_scale,
                .field_id = element.field_id,
            });
        } else {
            try collectSchemaColumnsAlloc(alloc, reader, elements, cursor, element.child_count, element_nullable, path, columns, depth + 1);
        }
    }
}

fn isNullableRepetition(repetition_type: ?i32) bool {
    return switch (repetition_type orelse 0) {
        1, 2 => true,
        else => false,
    };
}

fn joinSchemaPathAlloc(alloc: Allocator, reader: *Reader, parts: []const []const u8) ![]u8 {
    if (parts.len == 0) return error.InvalidParquetMetadata;
    var total_len: usize = parts.len - 1;
    for (parts) |part| {
        if (part.len == 0) return error.InvalidParquetMetadata;
        total_len = std.math.add(usize, total_len, part.len) catch return error.ParquetMetadataTooLarge;
    }
    try reader.admitAllocation(u8, total_len);
    const out = try alloc.alloc(u8, total_len);
    var cursor: usize = 0;
    for (parts, 0..) |part, idx| {
        if (idx != 0) {
            out[cursor] = '.';
            cursor += 1;
        }
        @memcpy(out[cursor .. cursor + part.len], part);
        cursor += part.len;
    }
    return out;
}

fn applySchemaNullability(alloc: Allocator, row_groups: []external_source.RowGroup, columns: []const SchemaColumn) !void {
    for (row_groups) |*row_group| {
        for (row_group.column_chunks) |*chunk| {
            const schema_column = schemaColumnForId(columns, chunk.column_id) orelse return error.InvalidParquetMetadata;
            chunk.nullable = schema_column.nullable;
            chunk.type_length = schema_column.type_length;
            if (schema_column.logical_type.len != 0) {
                const logical_type = try alloc.dupe(u8, schema_column.logical_type);
                if (chunk.logical_type.len > 0) alloc.free(chunk.logical_type);
                chunk.logical_type = logical_type;
            }
            chunk.decimal_precision = schema_column.decimal_precision;
            chunk.decimal_scale = schema_column.decimal_scale;
            chunk.field_id = schema_column.field_id;
        }
    }
}

fn schemaColumnForId(columns: []const SchemaColumn, column_id: []const u8) ?SchemaColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.column_id, column_id)) return column;
    }
    return null;
}

fn freeSchemaColumns(alloc: Allocator, columns: []SchemaColumn) void {
    for (columns) |*column| column.deinit(alloc);
    alloc.free(columns);
}

fn parseRowGroupList(alloc: Allocator, reader: *Reader, file_len: u64) ![]external_source.RowGroup {
    const list = try reader.readListHeader();
    if (list.elem_type != .struct_) return error.InvalidParquetMetadata;

    try reader.admitAllocation(external_source.RowGroup, list.len);
    const row_groups = try alloc.alloc(external_source.RowGroup, list.len);
    errdefer alloc.free(row_groups);
    var initialized: usize = 0;
    errdefer {
        for (row_groups[0..initialized]) |*group| group.deinit(alloc);
    }

    for (row_groups, 0..) |*group, idx| {
        group.* = try parseRowGroup(alloc, reader, file_len, @intCast(idx));
        initialized += 1;
    }
    return row_groups;
}

fn parseRowGroup(
    alloc: Allocator,
    reader: *Reader,
    file_len: u64,
    fallback_ordinal: u32,
) !external_source.RowGroup {
    var previous_field_id: i16 = 0;
    var row_count: ?u64 = null;
    var total_byte_len: u64 = 0;
    var file_offset: u64 = 0;
    var ordinal: u32 = fallback_ordinal;
    var column_chunks: ?[]external_source.ColumnChunk = null;
    errdefer if (column_chunks) |chunks| {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    };

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => {
                if (field.type != .list) return error.InvalidParquetMetadata;
                column_chunks = try parseColumnChunkList(alloc, reader, file_len);
            },
            2 => total_byte_len = try reader.readRequiredU64(field.type),
            3 => row_count = try reader.readRequiredU64(field.type),
            5 => file_offset = try reader.readRequiredU64(field.type),
            7 => ordinal = @intCast(try reader.readRequiredU64(field.type)),
            else => try reader.skip(field.type),
        }
    }

    const got_chunks = column_chunks orelse return error.InvalidParquetMetadata;
    column_chunks = null;
    const group = external_source.RowGroup{
        .ordinal = ordinal,
        .row_count = row_count orelse return error.InvalidParquetMetadata,
        .file_offset = file_offset,
        .total_byte_len = total_byte_len,
        .column_chunks = got_chunks,
    };
    group.validate(file_len) catch return error.InvalidParquetMetadata;
    return group;
}

fn parseColumnChunkList(alloc: Allocator, reader: *Reader, file_len: u64) ![]external_source.ColumnChunk {
    const list = try reader.readListHeader();
    if (list.elem_type != .struct_) return error.InvalidParquetMetadata;

    try reader.admitAllocation(external_source.ColumnChunk, list.len);
    const chunks = try alloc.alloc(external_source.ColumnChunk, list.len);
    errdefer alloc.free(chunks);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(alloc);
    }

    for (chunks) |*chunk| {
        chunk.* = try parseColumnChunk(alloc, reader, file_len);
        initialized += 1;
    }
    return chunks;
}

fn parseColumnChunk(alloc: Allocator, reader: *Reader, file_len: u64) !external_source.ColumnChunk {
    var previous_field_id: i16 = 0;
    var chunk_file_offset: ?u64 = null;
    var metadata: ?ColumnMetadata = null;
    errdefer if (metadata) |*meta| meta.deinit(alloc);

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            2 => chunk_file_offset = try reader.readRequiredU64(field.type),
            3 => {
                if (field.type != .struct_) return error.InvalidParquetMetadata;
                metadata = try parseColumnMetadata(alloc, reader);
            },
            else => try reader.skip(field.type),
        }
    }

    var meta = metadata orelse return error.InvalidParquetMetadata;
    metadata = null;
    errdefer meta.deinit(alloc);
    const start_offset = meta.dictionary_page_offset orelse meta.data_page_offset orelse chunk_file_offset orelse return error.InvalidParquetMetadata;
    const chunk = external_source.ColumnChunk{
        .column_id = meta.column_id,
        .file_offset = start_offset,
        .compressed_len = meta.total_compressed_size,
        .uncompressed_len = meta.total_uncompressed_size,
        .compression_codec = meta.compression_codec,
        .encoding = meta.encoding,
        .physical_type = meta.physical_type,
        .stats_min_i64 = meta.stats_min_i64,
        .stats_max_i64 = meta.stats_max_i64,
        .stats_min_bytes = meta.stats_min_bytes,
        .stats_max_bytes = meta.stats_max_bytes,
        .stats_min_bool = meta.stats_min_bool,
        .stats_max_bool = meta.stats_max_bool,
        .stats_min_f64 = meta.stats_min_f64,
        .stats_max_f64 = meta.stats_max_f64,
        .nullable = false,
    };
    chunk.validate(file_len) catch return error.InvalidParquetMetadata;
    meta.disown();
    return chunk;
}

const ColumnMetadata = struct {
    column_id: []u8,
    compression_codec: []u8,
    encoding: []u8,
    physical_type: []u8,
    total_compressed_size: u64,
    total_uncompressed_size: u64,
    data_page_offset: ?u64 = null,
    dictionary_page_offset: ?u64 = null,
    stats_min_i64: ?i64 = null,
    stats_max_i64: ?i64 = null,
    stats_min_bytes: ?[]u8 = null,
    stats_max_bytes: ?[]u8 = null,
    stats_min_bool: ?bool = null,
    stats_max_bool: ?bool = null,
    stats_min_f64: ?f64 = null,
    stats_max_f64: ?f64 = null,

    fn deinit(self: *ColumnMetadata, alloc: Allocator) void {
        if (self.column_id.len > 0) alloc.free(self.column_id);
        if (self.compression_codec.len > 0) alloc.free(self.compression_codec);
        if (self.encoding.len > 0) alloc.free(self.encoding);
        if (self.physical_type.len > 0) alloc.free(self.physical_type);
        if (self.stats_min_bytes) |value| alloc.free(value);
        if (self.stats_max_bytes) |value| alloc.free(value);
        self.* = undefined;
    }

    fn disown(self: *ColumnMetadata) void {
        self.column_id = &.{};
        self.compression_codec = &.{};
        self.encoding = &.{};
        self.physical_type = &.{};
        self.stats_min_bytes = null;
        self.stats_max_bytes = null;
    }
};

fn parseColumnMetadata(alloc: Allocator, reader: *Reader) !ColumnMetadata {
    var previous_field_id: i16 = 0;
    var column_id: ?[]u8 = null;
    var compression_codec: ?[]u8 = null;
    var encoding: []u8 = &.{};
    var physical_type: ?[]u8 = null;
    var total_compressed_size: ?u64 = null;
    var total_uncompressed_size: ?u64 = null;
    var data_page_offset: ?u64 = null;
    var dictionary_page_offset: ?u64 = null;
    var raw_statistics: RawColumnStatistics = .{};
    defer raw_statistics.deinit(alloc);
    errdefer if (column_id) |value| alloc.free(value);
    errdefer if (compression_codec) |value| alloc.free(value);
    errdefer if (encoding.len > 0) alloc.free(encoding);
    errdefer if (physical_type) |value| alloc.free(value);

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => physical_type = try physicalTypeNameAlloc(alloc, try reader.readRequiredI32(field.type)),
            2 => encoding = try parseFirstEncodingAlloc(alloc, reader, field.type),
            3 => column_id = try parsePathInSchemaAlloc(alloc, reader, field.type),
            4 => compression_codec = try compressionCodecNameAlloc(alloc, try reader.readRequiredI32(field.type)),
            6 => total_uncompressed_size = try reader.readRequiredU64(field.type),
            7 => total_compressed_size = try reader.readRequiredU64(field.type),
            9 => data_page_offset = try reader.readRequiredU64(field.type),
            11 => dictionary_page_offset = try reader.readRequiredU64(field.type),
            12 => raw_statistics = try parseColumnStatisticsAlloc(alloc, reader, field.type),
            else => try reader.skip(field.type),
        }
    }

    const got_physical_type = physical_type orelse try alloc.dupe(u8, "unknown");
    physical_type = null;
    errdefer alloc.free(got_physical_type);
    const stats = try decodeColumnStatsAlloc(alloc, raw_statistics, got_physical_type);
    errdefer if (stats.min_bytes) |value| alloc.free(value);
    errdefer if (stats.max_bytes) |value| alloc.free(value);

    return .{
        .column_id = column_id orelse return error.InvalidParquetMetadata,
        .compression_codec = compression_codec orelse try alloc.dupe(u8, "unknown"),
        .encoding = encoding,
        .physical_type = got_physical_type,
        .total_compressed_size = total_compressed_size orelse return error.InvalidParquetMetadata,
        .total_uncompressed_size = total_uncompressed_size orelse 0,
        .data_page_offset = data_page_offset,
        .dictionary_page_offset = dictionary_page_offset,
        .stats_min_i64 = stats.min_i64,
        .stats_max_i64 = stats.max_i64,
        .stats_min_bytes = stats.min_bytes,
        .stats_max_bytes = stats.max_bytes,
        .stats_min_bool = stats.min_bool,
        .stats_max_bool = stats.max_bool,
        .stats_min_f64 = stats.min_f64,
        .stats_max_f64 = stats.max_f64,
    };
}

const RawColumnStatistics = struct {
    min: ?[]u8 = null,
    max: ?[]u8 = null,

    fn deinit(self: *RawColumnStatistics, alloc: Allocator) void {
        if (self.min) |value| alloc.free(value);
        if (self.max) |value| alloc.free(value);
        self.* = undefined;
    }
};

const NumericStats = struct {
    min_i64: ?i64 = null,
    max_i64: ?i64 = null,
    min_bytes: ?[]u8 = null,
    max_bytes: ?[]u8 = null,
    min_bool: ?bool = null,
    max_bool: ?bool = null,
    min_f64: ?f64 = null,
    max_f64: ?f64 = null,
};

fn parseColumnStatisticsAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) !RawColumnStatistics {
    if (field_type != .struct_) return error.InvalidParquetMetadata;
    var previous_field_id: i16 = 0;
    var stats = RawColumnStatistics{};
    errdefer stats.deinit(alloc);
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1, 5 => {
                if (field.type != .binary) return error.InvalidParquetMetadata;
                if (stats.max) |value| alloc.free(value);
                stats.max = try reader.readBinaryAlloc(alloc);
            },
            2, 6 => {
                if (field.type != .binary) return error.InvalidParquetMetadata;
                if (stats.min) |value| alloc.free(value);
                stats.min = try reader.readBinaryAlloc(alloc);
            },
            else => try reader.skip(field.type),
        }
    }
    return stats;
}

fn decodeColumnStatsAlloc(alloc: Allocator, raw: RawColumnStatistics, physical_type: []const u8) !NumericStats {
    const min = raw.min orelse return .{};
    const max = raw.max orelse return .{};
    if (std.ascii.eqlIgnoreCase(physical_type, "int64")) {
        if (min.len != 8 or max.len != 8) return error.InvalidParquetMetadata;
        const min_i64 = std.mem.readInt(i64, min[0..8], .little);
        const max_i64 = std.mem.readInt(i64, max[0..8], .little);
        if (min_i64 > max_i64) return error.InvalidParquetMetadata;
        return .{ .min_i64 = min_i64, .max_i64 = max_i64 };
    }
    if (std.ascii.eqlIgnoreCase(physical_type, "int32")) {
        if (min.len != 4 or max.len != 4) return error.InvalidParquetMetadata;
        const min_i64: i64 = std.mem.readInt(i32, min[0..4], .little);
        const max_i64: i64 = std.mem.readInt(i32, max[0..4], .little);
        if (min_i64 > max_i64) return error.InvalidParquetMetadata;
        return .{ .min_i64 = min_i64, .max_i64 = max_i64 };
    }
    if (std.ascii.eqlIgnoreCase(physical_type, "byte_array") or std.ascii.eqlIgnoreCase(physical_type, "fixed_len_byte_array")) {
        if (std.mem.order(u8, min, max) == .gt) return error.InvalidParquetMetadata;
        const min_bytes = try alloc.dupe(u8, min);
        errdefer alloc.free(min_bytes);
        const max_bytes = try alloc.dupe(u8, max);
        return .{
            .min_bytes = min_bytes,
            .max_bytes = max_bytes,
        };
    }
    if (std.ascii.eqlIgnoreCase(physical_type, "boolean")) {
        if (min.len != 1 or max.len != 1) return error.InvalidParquetMetadata;
        if (min[0] > 1 or max[0] > 1) return error.InvalidParquetMetadata;
        if (min[0] > max[0]) return error.InvalidParquetMetadata;
        return .{
            .min_bool = min[0] != 0,
            .max_bool = max[0] != 0,
        };
    }
    if (std.ascii.eqlIgnoreCase(physical_type, "float")) {
        if (min.len != 4 or max.len != 4) return error.InvalidParquetMetadata;
        const min_f32: f32 = @bitCast(std.mem.readInt(u32, min[0..4], .little));
        const max_f32: f32 = @bitCast(std.mem.readInt(u32, max[0..4], .little));
        if (std.math.isNan(min_f32) or std.math.isNan(max_f32) or min_f32 > max_f32) return error.InvalidParquetMetadata;
        return .{
            .min_f64 = @floatCast(min_f32),
            .max_f64 = @floatCast(max_f32),
        };
    }
    if (std.ascii.eqlIgnoreCase(physical_type, "double")) {
        if (min.len != 8 or max.len != 8) return error.InvalidParquetMetadata;
        const min_f64: f64 = @bitCast(std.mem.readInt(u64, min[0..8], .little));
        const max_f64: f64 = @bitCast(std.mem.readInt(u64, max[0..8], .little));
        if (std.math.isNan(min_f64) or std.math.isNan(max_f64) or min_f64 > max_f64) return error.InvalidParquetMetadata;
        return .{
            .min_f64 = min_f64,
            .max_f64 = max_f64,
        };
    }
    return .{};
}

fn parsePathInSchemaAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]u8 {
    if (field_type != .list) return error.InvalidParquetMetadata;
    const list = try reader.readListHeader();
    if (list.elem_type != .binary) return error.InvalidParquetMetadata;
    if (list.len == 0) return error.InvalidParquetMetadata;

    try reader.admitAllocation([]u8, list.len);
    var parts = try alloc.alloc([]u8, list.len);
    defer alloc.free(parts);
    var initialized: usize = 0;
    errdefer {
        for (parts[0..initialized]) |part| alloc.free(part);
    }
    var total_len: usize = 0;
    for (parts) |*part| {
        part.* = try reader.readBinaryAlloc(alloc);
        if (part.len == 0) return error.InvalidParquetMetadata;
        total_len = std.math.add(usize, total_len, part.len) catch return error.ParquetMetadataTooLarge;
        initialized += 1;
    }
    const output_len = std.math.add(usize, total_len, parts.len - 1) catch return error.ParquetMetadataTooLarge;
    try reader.admitAllocation(u8, output_len);
    const out = try alloc.alloc(u8, output_len);
    var cursor: usize = 0;
    for (parts, 0..) |part, idx| {
        if (idx != 0) {
            out[cursor] = '.';
            cursor += 1;
        }
        @memcpy(out[cursor .. cursor + part.len], part);
        cursor += part.len;
    }
    for (parts) |part| alloc.free(part);
    return out;
}

fn parseFirstEncodingAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]u8 {
    if (field_type != .list) return error.InvalidParquetMetadata;
    const list = try reader.readListHeader();
    if (list.elem_type != .i32) return error.InvalidParquetMetadata;
    var first: ?i32 = null;
    for (0..list.len) |idx| {
        const value = try reader.readI32();
        if (idx == 0) first = value;
    }
    return try alloc.dupe(u8, encodingName(first orelse -1));
}

fn compressionCodecNameAlloc(alloc: Allocator, codec: i32) ![]u8 {
    return try alloc.dupe(u8, switch (codec) {
        0 => "uncompressed",
        1 => "snappy",
        2 => "gzip",
        3 => "lzo",
        4 => "brotli",
        5 => "lz4",
        6 => "zstd",
        7 => "lz4_raw",
        else => "unknown",
    });
}

fn physicalTypeNameAlloc(alloc: Allocator, physical_type: i32) ![]u8 {
    return try alloc.dupe(u8, switch (physical_type) {
        0 => "boolean",
        1 => "int32",
        2 => "int64",
        3 => "int96",
        4 => "float",
        5 => "double",
        6 => "byte_array",
        7 => "fixed_len_byte_array",
        else => "unknown",
    });
}

fn logicalTypeNameForConvertedTypeAlloc(alloc: Allocator, converted_type: i32) ![]u8 {
    return switch (converted_type) {
        5 => try alloc.dupe(u8, "decimal"),
        9 => try alloc.dupe(u8, "timestamp_millis"),
        10 => try alloc.dupe(u8, "timestamp_micros"),
        else => &.{},
    };
}

const LogicalTypeAnnotation = struct {
    name: []u8 = &.{},
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,

    fn deinit(self: *LogicalTypeAnnotation, alloc: Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        self.* = undefined;
    }
};

fn parseLogicalTypeAnnotationAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) !LogicalTypeAnnotation {
    if (field_type != .struct_) return error.InvalidParquetMetadata;
    var previous_field_id: i16 = 0;
    var annotation = LogicalTypeAnnotation{};
    errdefer annotation.deinit(alloc);
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            5 => {
                annotation.deinit(alloc);
                annotation = try parseDecimalLogicalTypeAnnotationAlloc(alloc, reader, field.type);
            },
            8 => {
                annotation.deinit(alloc);
                annotation = .{ .name = try parseTimestampLogicalTypeNameAlloc(alloc, reader, field.type) };
            },
            else => try reader.skip(field.type),
        }
    }
    return annotation;
}

fn parseDecimalLogicalTypeAnnotationAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) !LogicalTypeAnnotation {
    if (field_type != .struct_) return error.InvalidParquetMetadata;
    var previous_field_id: i16 = 0;
    var scale: i32 = 0;
    var precision: i32 = 0;
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => scale = try reader.readRequiredI32(field.type),
            2 => precision = try reader.readRequiredI32(field.type),
            else => try reader.skip(field.type),
        }
    }
    if (precision <= 0 or scale < 0 or scale > precision) return error.InvalidParquetMetadata;
    return .{
        .name = try alloc.dupe(u8, "decimal"),
        .decimal_precision = precision,
        .decimal_scale = scale,
    };
}

fn parseTimestampLogicalTypeNameAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]u8 {
    if (field_type != .struct_) return error.InvalidParquetMetadata;
    var previous_field_id: i16 = 0;
    var logical_type: []u8 = &.{};
    errdefer if (logical_type.len > 0) alloc.free(logical_type);
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            2 => {
                if (logical_type.len > 0) {
                    alloc.free(logical_type);
                    logical_type = &.{};
                }
                logical_type = try parseTimestampUnitNameAlloc(alloc, reader, field.type);
            },
            else => try reader.skip(field.type),
        }
    }
    return logical_type;
}

fn parseTimestampUnitNameAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]u8 {
    if (field_type != .struct_) return error.InvalidParquetMetadata;
    var previous_field_id: i16 = 0;
    var logical_type: []u8 = &.{};
    errdefer if (logical_type.len > 0) alloc.free(logical_type);
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        const name: []const u8 = switch (field.id) {
            1 => "timestamp_millis",
            2 => "timestamp_micros",
            3 => "timestamp_nanos",
            else => "",
        };
        if (name.len == 0) {
            try reader.skip(field.type);
            continue;
        }
        if (field.type != .struct_) return error.InvalidParquetMetadata;
        try reader.skip(field.type);
        if (logical_type.len > 0) alloc.free(logical_type);
        logical_type = try alloc.dupe(u8, name);
    }
    return logical_type;
}

fn encodingName(encoding: i32) []const u8 {
    return switch (encoding) {
        0 => "plain",
        1 => "plain_dictionary",
        2 => "rle",
        3 => "bit_packed",
        4 => "delta_binary_packed",
        5 => "delta_length_byte_array",
        6 => "delta_byte_array",
        7 => "rle_dictionary",
        8 => "byte_stream_split",
        else => "unknown",
    };
}

const Field = struct {
    id: i16,
    type: CompactType,
};

const ListHeader = struct {
    elem_type: CompactType,
    len: usize,
};

const Reader = struct {
    bytes: []const u8,
    cursor: usize = 0,
    budget: bounded_decode.Budget,
    remaining_skip_operations: usize,
    max_nesting_depth: usize,

    fn admitAllocation(self: *Reader, comptime T: type, count: usize) !void {
        self.budget.admitAllocation(T, count) catch |err| return mapDecodeLimitError(err);
    }

    fn admitElements(self: *Reader, raw_count: anytype, min_encoded_bytes: usize) !usize {
        return self.budget.admitElements(
            raw_count,
            self.bytes.len - self.cursor,
            min_encoded_bytes,
        ) catch |err| return mapDecodeLimitError(err);
    }

    fn readFieldHeader(self: *Reader, previous_field_id: *i16) !?Field {
        const raw = try self.readByte();
        const field_type = try decodeCompactType(raw & 0x0f);
        if (field_type == .stop) return null;
        const delta: i16 = @intCast(raw >> 4);
        const field_id = if (delta == 0)
            try self.readI16()
        else
            std.math.add(i16, previous_field_id.*, delta) catch return error.InvalidParquetMetadata;
        previous_field_id.* = field_id;
        return .{ .id = field_id, .type = field_type };
    }

    fn readListHeader(self: *Reader) !ListHeader {
        const raw = try self.readByte();
        const elem_type = try decodeCompactType(raw & 0x0f);
        const inline_len = raw >> 4;
        const raw_len = if (inline_len == 15) try self.readVarintUsize() else inline_len;
        const len = try self.admitElements(raw_len, 0);
        return .{ .elem_type = elem_type, .len = len };
    }

    fn readRequiredI32(self: *Reader, field_type: CompactType) !i32 {
        if (field_type != .i32) return error.InvalidParquetMetadata;
        return try self.readI32();
    }

    fn readRequiredI32NonNegative(self: *Reader, field_type: CompactType) !i32 {
        const value = try self.readRequiredI32(field_type);
        if (value < 0) return error.InvalidParquetMetadata;
        return value;
    }

    fn readRequiredU64(self: *Reader, field_type: CompactType) !u64 {
        if (field_type != .i64) return error.InvalidParquetMetadata;
        const value = try self.readI64();
        if (value < 0) return error.InvalidParquetMetadata;
        return @intCast(value);
    }

    fn readI16(self: *Reader) !i16 {
        return @intCast(zigzagDecode(try self.readVarintU64()));
    }

    fn readI32(self: *Reader) !i32 {
        return @intCast(zigzagDecode(try self.readVarintU64()));
    }

    fn readI64(self: *Reader) !i64 {
        return zigzagDecode(try self.readVarintU64());
    }

    fn readBinaryAlloc(self: *Reader, alloc: Allocator) ![]u8 {
        const len = try self.readVarintUsize();
        if (len > self.bytes.len - self.cursor) return error.InvalidParquetMetadata;
        try self.admitAllocation(u8, len);
        const out = try alloc.dupe(u8, self.bytes[self.cursor .. self.cursor + len]);
        self.cursor += len;
        return out;
    }

    fn readByte(self: *Reader) !u8 {
        if (self.cursor >= self.bytes.len) return error.InvalidParquetMetadata;
        const out = self.bytes[self.cursor];
        self.cursor += 1;
        return out;
    }

    fn readVarintUsize(self: *Reader) !usize {
        return std.math.cast(usize, try self.readVarintU64()) orelse error.InvalidParquetMetadata;
    }

    fn readVarintU64(self: *Reader) !u64 {
        var shift: u6 = 0;
        var result: u64 = 0;
        while (true) {
            const byte = try self.readByte();
            result |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return result;
            if (shift >= 63) return error.InvalidParquetMetadata;
            shift += 7;
        }
    }

    fn skip(self: *Reader, field_type: CompactType) !void {
        return try self.skipAtDepth(field_type, 0);
    }

    fn skipAtDepth(self: *Reader, field_type: CompactType, depth: usize) !void {
        if (depth >= self.max_nesting_depth) return error.ParquetMetadataTooLarge;
        if (self.remaining_skip_operations == 0) return error.ParquetMetadataTooLarge;
        self.remaining_skip_operations -= 1;
        switch (field_type) {
            .stop => {},
            .boolean_true, .boolean_false => {},
            .byte => _ = try self.readByte(),
            .i16 => _ = try self.readI16(),
            .i32 => _ = try self.readI32(),
            .i64 => _ = try self.readI64(),
            .double => {
                if (self.bytes.len - self.cursor < 8) return error.InvalidParquetMetadata;
                self.cursor += 8;
            },
            .binary => {
                const len = try self.readVarintUsize();
                if (len > self.bytes.len - self.cursor) return error.InvalidParquetMetadata;
                self.cursor += len;
            },
            .list, .set => {
                const list = try self.readListHeader();
                for (0..list.len) |_| try self.skipAtDepth(list.elem_type, depth + 1);
            },
            .map => {
                const count = try self.readVarintUsize();
                if (count == 0) return;
                const element_count = std.math.mul(usize, count, 2) catch return error.ParquetMetadataTooLarge;
                _ = try self.admitElements(element_count, 0);
                const types = try self.readByte();
                const key_type = try decodeCompactType(types >> 4);
                const value_type = try decodeCompactType(types & 0x0f);
                for (0..count) |_| {
                    try self.skipAtDepth(key_type, depth + 1);
                    try self.skipAtDepth(value_type, depth + 1);
                }
            },
            .struct_ => {
                var previous_field_id: i16 = 0;
                while (try self.readFieldHeader(&previous_field_id)) |field| try self.skipAtDepth(field.type, depth + 1);
            },
        }
    }
};

fn zigzagDecode(raw: u64) i64 {
    return @as(i64, @bitCast(raw >> 1)) ^ -@as(i64, @intCast(raw & 1));
}

fn appendField(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, previous: *i16, id: i16, field_type: CompactType) !void {
    const delta = id - previous.*;
    if (delta > 0 and delta <= 15) {
        try out.append(alloc, (@as(u8, @intCast(delta)) << 4) | @intFromEnum(field_type));
    } else {
        try out.append(alloc, @intFromEnum(field_type));
        try appendI16(out, alloc, id);
    }
    previous.* = id;
}

fn appendStop(out: *std.ArrayListUnmanaged(u8), alloc: Allocator) !void {
    try out.append(alloc, 0);
}

fn appendListHeader(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, elem_type: CompactType, len: usize) !void {
    if (len < 15) {
        try out.append(alloc, (@as(u8, @intCast(len)) << 4) | @as(u8, @intFromEnum(elem_type)));
    } else {
        try out.append(alloc, 0xf0 | @as(u8, @intFromEnum(elem_type)));
        try appendVarint(out, alloc, len);
    }
}

fn appendI16(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i16) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i32) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI64(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i64) !void {
    try appendZigzag(out, alloc, value);
}

fn appendZigzag(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: anytype) !void {
    const Int = @TypeOf(value);
    const Unsigned = std.meta.Int(.unsigned, @bitSizeOf(Int));
    const encoded: Unsigned = @bitCast((value << 1) ^ (value >> (@bitSizeOf(Int) - 1)));
    try appendVarint(out, alloc, encoded);
}

fn appendVarint(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: anytype) !void {
    var remaining: u64 = @intCast(value);
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn appendBinary(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    try appendVarint(out, alloc, bytes.len);
    try out.appendSlice(alloc, bytes);
}

fn appendPlainI32StatBinary(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, &bytes, value, .little);
    try appendBinary(out, alloc, &bytes);
}

test "parquet metadata parser extracts row groups and column chunks" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixture(alloc);
    defer bytes.deinit(alloc);

    var footer = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer.deinit(alloc);

    try std.testing.expectEqual(@as(i32, 1), footer.version);
    try std.testing.expectEqual(@as(u64, 2), footer.row_count);
    try std.testing.expectEqual(@as(usize, 1), footer.row_groups.len);
    try std.testing.expectEqual(@as(u32, 0), footer.row_groups[0].ordinal);
    try std.testing.expectEqual(@as(u64, 2), footer.row_groups[0].row_count);
    try std.testing.expectEqual(@as(u64, 100), footer.row_groups[0].file_offset);
    try std.testing.expectEqual(@as(u64, 80), footer.row_groups[0].total_byte_len);
    try std.testing.expectEqual(@as(usize, 1), footer.row_groups[0].column_chunks.len);
    try std.testing.expectEqualStrings("amount", footer.row_groups[0].column_chunks[0].column_id);
    try std.testing.expectEqualStrings("zstd", footer.row_groups[0].column_chunks[0].compression_codec);
    try std.testing.expectEqualStrings("plain", footer.row_groups[0].column_chunks[0].encoding);
    try std.testing.expectEqual(@as(u64, 100), footer.row_groups[0].column_chunks[0].file_offset);
    try std.testing.expectEqual(@as(u64, 40), footer.row_groups[0].column_chunks[0].compressed_len);
    try std.testing.expectEqual(@as(?i64, 10), footer.row_groups[0].column_chunks[0].stats_min_i64);
    try std.testing.expectEqual(@as(?i64, 20), footer.row_groups[0].column_chunks[0].stats_max_i64);
}

test "parquet metadata parser rejects forged list counts before allocation" {
    const alloc = std.testing.allocator;
    // FileMetaData.version followed by a schema list claiming five structs.
    const forged = [_]u8{ 0x15, 0x02, 0x19, 0xfc, 0x05 };
    try std.testing.expectError(error.ParquetMetadataTooLarge, parseFooterMetadataAllocWithLimits(
        alloc,
        &forged,
        1024,
        .{ .max_elements = 4 },
    ));
}

test "parquet metadata parser derives nullable columns from schema repetition" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixtureWithSchema(alloc, true);
    defer bytes.deinit(alloc);

    var footer = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), footer.row_groups.len);
    try std.testing.expectEqual(@as(usize, 1), footer.row_groups[0].column_chunks.len);
    try std.testing.expectEqualStrings("amount", footer.row_groups[0].column_chunks[0].column_id);
    try std.testing.expect(footer.row_groups[0].column_chunks[0].nullable);
    try std.testing.expectEqual(@as(?i32, 2), footer.row_groups[0].column_chunks[0].field_id);

    var required_bytes = try buildSingleColumnMetadataFixtureWithSchema(alloc, false);
    defer required_bytes.deinit(alloc);
    var required_footer = try parseFooterMetadataAlloc(alloc, required_bytes.items, 1024);
    defer required_footer.deinit(alloc);
    try std.testing.expect(!required_footer.row_groups[0].column_chunks[0].nullable);
}

test "parquet metadata parser derives logical timestamp columns from schema annotations" {
    const alloc = std.testing.allocator;
    var converted_bytes = try buildSingleColumnTimestampConvertedTypeMetadataFixture(alloc, 10);
    defer converted_bytes.deinit(alloc);
    var converted_footer = try parseFooterMetadataAlloc(alloc, converted_bytes.items, 1024);
    defer converted_footer.deinit(alloc);
    try std.testing.expectEqualStrings("timestamp_micros", converted_footer.row_groups[0].column_chunks[0].logical_type);

    var logical_bytes = try buildSingleColumnTimestampLogicalTypeMetadataFixture(alloc);
    defer logical_bytes.deinit(alloc);
    var logical_footer = try parseFooterMetadataAlloc(alloc, logical_bytes.items, 1024);
    defer logical_footer.deinit(alloc);
    try std.testing.expectEqualStrings("timestamp_nanos", logical_footer.row_groups[0].column_chunks[0].logical_type);
}

test "parquet metadata parser derives decimal columns from schema annotations" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnDecimalConvertedTypeMetadataFixture(alloc);
    defer bytes.deinit(alloc);
    var footer = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer.deinit(alloc);
    try std.testing.expectEqualStrings("decimal", footer.row_groups[0].column_chunks[0].logical_type);
    try std.testing.expectEqual(@as(i32, 9), footer.row_groups[0].column_chunks[0].decimal_precision);
    try std.testing.expectEqual(@as(i32, 2), footer.row_groups[0].column_chunks[0].decimal_scale);
    try std.testing.expectEqual(@as(i32, 8), footer.row_groups[0].column_chunks[0].type_length);
}

test "parquet metadata parser decodes float and double statistics" {
    const alloc = std.testing.allocator;

    var min_f32_bytes: [4]u8 = undefined;
    var max_f32_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &min_f32_bytes, @bitCast(@as(f32, 1.5)), .little);
    std.mem.writeInt(u32, &max_f32_bytes, @bitCast(@as(f32, 2.5)), .little);
    var raw_f32 = RawColumnStatistics{
        .min = try alloc.dupe(u8, &min_f32_bytes),
        .max = try alloc.dupe(u8, &max_f32_bytes),
    };
    defer raw_f32.deinit(alloc);
    const stats_f32 = try decodeColumnStatsAlloc(alloc, raw_f32, "float");
    try std.testing.expectEqual(@as(?f64, 1.5), stats_f32.min_f64);
    try std.testing.expectEqual(@as(?f64, 2.5), stats_f32.max_f64);

    var min_f64_bytes: [8]u8 = undefined;
    var max_f64_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &min_f64_bytes, @bitCast(@as(f64, 10.25)), .little);
    std.mem.writeInt(u64, &max_f64_bytes, @bitCast(@as(f64, 20.5)), .little);
    var raw_f64 = RawColumnStatistics{
        .min = try alloc.dupe(u8, &min_f64_bytes),
        .max = try alloc.dupe(u8, &max_f64_bytes),
    };
    defer raw_f64.deinit(alloc);
    const stats_f64 = try decodeColumnStatsAlloc(alloc, raw_f64, "double");
    try std.testing.expectEqual(@as(?f64, 10.25), stats_f64.min_f64);
    try std.testing.expectEqual(@as(?f64, 20.5), stats_f64.max_f64);
}

test "parquet metadata parser rejects inconsistent row counts and ranges" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixture(alloc);
    defer bytes.deinit(alloc);

    try std.testing.expectError(error.InvalidParquetMetadata, parseFooterMetadataAlloc(alloc, bytes.items, 120));

    // Patch FileMetaData.num_rows from 2 to 3. This offset is stable for the
    // fixture: field1(version) is two bytes, field3 header is the third byte,
    // and the value byte follows it.
    bytes.items[3] = 6;
    try std.testing.expectError(error.InvalidParquetMetadata, parseFooterMetadataAlloc(alloc, bytes.items, 1024));
}

test "parquet metadata parser enriches raw inventory for projected scan planning" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixture(alloc);
    defer bytes.deinit(alloc);
    var footer = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer.deinit(alloc);

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 0,
        .partition_spec_id = 7,
        .partition_field_count = 1,
        .partition_values = try alloc.dupe(external_source.PartitionValue, &.{.{
            .column_id = try alloc.dupe(u8, "region"),
            .string_value = try alloc.dupe(u8, "west"),
        }}),
        .row_groups = &.{},
    };
    try raw.validate();

    var enriched = try enrichInventoryFileWithFooterAlloc(alloc, raw, "part-a.parquet", footer);
    defer enriched.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), enriched.files[0].row_count);
    try std.testing.expectEqual(@as(usize, 1), enriched.files[0].row_groups.len);
    try std.testing.expectEqualStrings("amount", enriched.files[0].row_groups[0].column_chunks[0].column_id);
    try std.testing.expectEqual(@as(?i64, 10), enriched.files[0].row_groups[0].column_chunks[0].stats_min_i64);
    try std.testing.expectEqual(@as(?i64, 20), enriched.files[0].row_groups[0].column_chunks[0].stats_max_i64);
    try std.testing.expectEqual(@as(?i32, 7), enriched.files[0].partition_spec_id);
    try std.testing.expectEqual(@as(u32, 1), enriched.files[0].partition_field_count);
    try std.testing.expectEqual(@as(usize, 1), enriched.files[0].partition_values.len);
    try std.testing.expectEqualStrings("region", enriched.files[0].partition_values[0].column_id);
    try std.testing.expectEqualStrings("west", enriched.files[0].partition_values[0].string_value);

    const binding = external_binding.Binding{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://bucket/events",
        .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
        .schema_fingerprint = "schema-v1",
    };
    var plan = try lake_scan_plan.planProjectedScanAlloc(alloc, .{
        .binding = binding,
        .inventory = enriched,
        .projected_columns = &[_][]const u8{"amount"},
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.logical_reads.len);
    try std.testing.expectEqual(@as(u64, 100), plan.logical_reads[0].range.offset);
    try std.testing.expectEqual(@as(u64, 40), plan.logical_reads[0].range.len);
}

test "parquet metadata parser enriches multiple inventory files with footers" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixture(alloc);
    defer bytes.deinit(alloc);
    var footer_a = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer_a.deinit(alloc);
    var footer_b = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer_b.deinit(alloc);

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 2),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 0,
        .row_groups = &.{},
    };
    raw.files[1] = .{
        .file_id = try alloc.dupe(u8, "part-b.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-b.parquet"),
        .etag = try alloc.dupe(u8, "etag-b"),
        .byte_len = 1024,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const footers = [_]FileFooter{
        .{ .file_id = "part-a.parquet", .footer = footer_a },
        .{ .file_id = "part-b.parquet", .footer = footer_b },
    };
    var enriched = try enrichInventoryFilesWithFootersAlloc(alloc, raw, &footers);
    defer enriched.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 2), enriched.files[0].row_count);
    try std.testing.expectEqual(@as(u64, 2), enriched.files[1].row_count);
    try std.testing.expectEqualStrings("amount", enriched.files[0].row_groups[0].column_chunks[0].column_id);
    try std.testing.expectEqualStrings("amount", enriched.files[1].row_groups[0].column_chunks[0].column_id);
    try std.testing.expectError(error.InvalidParquetMetadata, enrichInventoryFilesWithFootersAlloc(alloc, raw, &[_]FileFooter{
        .{ .file_id = "part-a.parquet", .footer = footer_a },
        .{ .file_id = "part-a.parquet", .footer = footer_b },
    }));
    try std.testing.expectError(error.ParquetInventoryFileNotFound, enrichInventoryFilesWithFootersAlloc(alloc, raw, &[_]FileFooter{
        .{ .file_id = "missing.parquet", .footer = footer_a },
    }));
}

fn buildSingleColumnMetadataFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var file_prev: i16 = 0;
    try appendField(&out, alloc, &file_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &file_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &file_prev, 4, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var rg_prev: i16 = 0;
    try appendField(&out, alloc, &rg_prev, 1, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var chunk_prev: i16 = 0;
    try appendField(&out, alloc, &chunk_prev, 2, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &chunk_prev, 3, .struct_);

    var meta_prev: i16 = 0;
    try appendField(&out, alloc, &meta_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &meta_prev, 2, .list);
    try appendListHeader(&out, alloc, .i32, 1);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &meta_prev, 3, .list);
    try appendListHeader(&out, alloc, .binary, 1);
    try appendBinary(&out, alloc, "amount");
    try appendField(&out, alloc, &meta_prev, 4, .i32);
    try appendI32(&out, alloc, 6);
    try appendField(&out, alloc, &meta_prev, 5, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &meta_prev, 6, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &meta_prev, 7, .i64);
    try appendI64(&out, alloc, 40);
    try appendField(&out, alloc, &meta_prev, 9, .i64);
    try appendI64(&out, alloc, 120);
    try appendField(&out, alloc, &meta_prev, 11, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &meta_prev, 12, .struct_);
    var stats_prev: i16 = 0;
    try appendField(&out, alloc, &stats_prev, 1, .binary);
    try appendPlainI32StatBinary(&out, alloc, 20);
    try appendField(&out, alloc, &stats_prev, 2, .binary);
    try appendPlainI32StatBinary(&out, alloc, 10);
    try appendStop(&out, alloc);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);

    try appendField(&out, alloc, &rg_prev, 2, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &rg_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &rg_prev, 5, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &rg_prev, 7, .i64);
    try appendI64(&out, alloc, 0);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

fn buildSingleColumnMetadataFixtureWithSchema(alloc: Allocator, nullable: bool) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var file_prev: i16 = 0;
    try appendField(&out, alloc, &file_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &file_prev, 2, .list);
    try appendListHeader(&out, alloc, .struct_, 2);

    var root_prev: i16 = 0;
    try appendField(&out, alloc, &root_prev, 4, .binary);
    try appendBinary(&out, alloc, "schema");
    try appendField(&out, alloc, &root_prev, 5, .i32);
    try appendI32(&out, alloc, 1);
    try appendStop(&out, alloc);

    var leaf_prev: i16 = 0;
    try appendField(&out, alloc, &leaf_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &leaf_prev, 3, .i32);
    try appendI32(&out, alloc, if (nullable) 1 else 0);
    try appendField(&out, alloc, &leaf_prev, 4, .binary);
    try appendBinary(&out, alloc, "amount");
    try appendField(&out, alloc, &leaf_prev, 9, .i32);
    try appendI32(&out, alloc, 2);
    try appendStop(&out, alloc);

    try appendField(&out, alloc, &file_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &file_prev, 4, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var rg_prev: i16 = 0;
    try appendField(&out, alloc, &rg_prev, 1, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var chunk_prev: i16 = 0;
    try appendField(&out, alloc, &chunk_prev, 2, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &chunk_prev, 3, .struct_);

    var meta_prev: i16 = 0;
    try appendField(&out, alloc, &meta_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &meta_prev, 2, .list);
    try appendListHeader(&out, alloc, .i32, 1);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &meta_prev, 3, .list);
    try appendListHeader(&out, alloc, .binary, 1);
    try appendBinary(&out, alloc, "amount");
    try appendField(&out, alloc, &meta_prev, 4, .i32);
    try appendI32(&out, alloc, 6);
    try appendField(&out, alloc, &meta_prev, 5, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &meta_prev, 6, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &meta_prev, 7, .i64);
    try appendI64(&out, alloc, 40);
    try appendField(&out, alloc, &meta_prev, 9, .i64);
    try appendI64(&out, alloc, 120);
    try appendField(&out, alloc, &meta_prev, 11, .i64);
    try appendI64(&out, alloc, 100);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);

    try appendField(&out, alloc, &rg_prev, 2, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &rg_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &rg_prev, 5, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &rg_prev, 7, .i64);
    try appendI64(&out, alloc, 0);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

fn buildSingleColumnTimestampConvertedTypeMetadataFixture(alloc: Allocator, converted_type: i32) !std.ArrayListUnmanaged(u8) {
    return try buildSingleColumnTimestampMetadataFixture(alloc, converted_type, false);
}

fn buildSingleColumnDecimalConvertedTypeMetadataFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    return try buildSingleColumnTimestampMetadataFixture(alloc, 5, false);
}

fn buildSingleColumnTimestampLogicalTypeMetadataFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    return try buildSingleColumnTimestampMetadataFixture(alloc, null, true);
}

fn buildSingleColumnTimestampMetadataFixture(alloc: Allocator, converted_type: ?i32, logical_nanos: bool) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var file_prev: i16 = 0;
    try appendField(&out, alloc, &file_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &file_prev, 2, .list);
    try appendListHeader(&out, alloc, .struct_, 2);

    var root_prev: i16 = 0;
    try appendField(&out, alloc, &root_prev, 4, .binary);
    try appendBinary(&out, alloc, "schema");
    try appendField(&out, alloc, &root_prev, 5, .i32);
    try appendI32(&out, alloc, 1);
    try appendStop(&out, alloc);

    var leaf_prev: i16 = 0;
    try appendField(&out, alloc, &leaf_prev, 1, .i32);
    try appendI32(&out, alloc, if (converted_type == 5) 7 else 1);
    if (converted_type == 5) {
        try appendField(&out, alloc, &leaf_prev, 2, .i32);
        try appendI32(&out, alloc, 8);
    }
    try appendField(&out, alloc, &leaf_prev, 3, .i32);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &leaf_prev, 4, .binary);
    try appendBinary(&out, alloc, "amount");
    if (converted_type) |value| {
        try appendField(&out, alloc, &leaf_prev, 6, .i32);
        try appendI32(&out, alloc, value);
        if (value == 5) {
            try appendField(&out, alloc, &leaf_prev, 7, .i32);
            try appendI32(&out, alloc, 2);
            try appendField(&out, alloc, &leaf_prev, 8, .i32);
            try appendI32(&out, alloc, 9);
        }
    }
    if (logical_nanos) {
        try appendField(&out, alloc, &leaf_prev, 10, .struct_);
        var logical_prev: i16 = 0;
        try appendField(&out, alloc, &logical_prev, 8, .struct_);
        var timestamp_prev: i16 = 0;
        try appendField(&out, alloc, &timestamp_prev, 1, .boolean_true);
        try appendField(&out, alloc, &timestamp_prev, 2, .struct_);
        var unit_prev: i16 = 0;
        try appendField(&out, alloc, &unit_prev, 3, .struct_);
        try appendStop(&out, alloc);
        try appendStop(&out, alloc);
        try appendStop(&out, alloc);
        try appendStop(&out, alloc);
    }
    try appendStop(&out, alloc);

    try appendField(&out, alloc, &file_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &file_prev, 4, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var rg_prev: i16 = 0;
    try appendField(&out, alloc, &rg_prev, 1, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var chunk_prev: i16 = 0;
    try appendField(&out, alloc, &chunk_prev, 2, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &chunk_prev, 3, .struct_);

    var meta_prev: i16 = 0;
    try appendField(&out, alloc, &meta_prev, 1, .i32);
    try appendI32(&out, alloc, if (converted_type == 5) 7 else 1);
    try appendField(&out, alloc, &meta_prev, 2, .list);
    try appendListHeader(&out, alloc, .i32, 1);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &meta_prev, 3, .list);
    try appendListHeader(&out, alloc, .binary, 1);
    try appendBinary(&out, alloc, "amount");
    try appendField(&out, alloc, &meta_prev, 4, .i32);
    try appendI32(&out, alloc, 6);
    try appendField(&out, alloc, &meta_prev, 5, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &meta_prev, 6, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &meta_prev, 7, .i64);
    try appendI64(&out, alloc, 40);
    try appendField(&out, alloc, &meta_prev, 9, .i64);
    try appendI64(&out, alloc, 120);
    try appendField(&out, alloc, &meta_prev, 11, .i64);
    try appendI64(&out, alloc, 100);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);

    try appendField(&out, alloc, &rg_prev, 2, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &rg_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &rg_prev, 5, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &rg_prev, 7, .i64);
    try appendI64(&out, alloc, 0);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}
