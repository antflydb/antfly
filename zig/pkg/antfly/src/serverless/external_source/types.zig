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

//! Metadata artifacts for user-owned external lake files referenced by
//! serverless manifests.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Format = enum(u8) {
    parquet = 1,
    iceberg = 2,
    lance = 3,
};

pub const ColumnChunk = struct {
    column_id: []u8,
    file_offset: u64,
    compressed_len: u64,
    uncompressed_len: u64 = 0,
    compression_codec: []u8 = &.{},
    encoding: []u8 = &.{},
    physical_type: []u8 = &.{},
    type_length: i32 = 0,
    logical_type: []u8 = &.{},
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
    field_id: ?i32 = null,
    stats_min_i64: ?i64 = null,
    stats_max_i64: ?i64 = null,
    stats_min_bytes: ?[]u8 = null,
    stats_max_bytes: ?[]u8 = null,
    stats_min_bool: ?bool = null,
    stats_max_bool: ?bool = null,
    stats_min_f64: ?f64 = null,
    stats_max_f64: ?f64 = null,
    nullable: bool = false,

    pub fn deinit(self: *ColumnChunk, alloc: Allocator) void {
        alloc.free(self.column_id);
        if (self.compression_codec.len > 0) alloc.free(self.compression_codec);
        if (self.encoding.len > 0) alloc.free(self.encoding);
        if (self.physical_type.len > 0) alloc.free(self.physical_type);
        if (self.logical_type.len > 0) alloc.free(self.logical_type);
        if (self.stats_min_bytes) |value| alloc.free(value);
        if (self.stats_max_bytes) |value| alloc.free(value);
        self.* = undefined;
    }

    pub fn validate(self: ColumnChunk, file_len: u64) !void {
        if (self.column_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.compressed_len == 0) return error.InvalidExternalSourceInventory;
        if (self.file_offset > file_len) return error.InvalidExternalSourceInventory;
        if (self.compressed_len > file_len - self.file_offset) return error.InvalidExternalSourceInventory;
        if (self.field_id) |id| {
            if (id < 0) return error.InvalidExternalSourceInventory;
        }
        if (self.stats_min_i64 != null and self.stats_max_i64 != null and self.stats_min_i64.? > self.stats_max_i64.?) return error.InvalidExternalSourceInventory;
        if ((self.stats_min_bytes == null) != (self.stats_max_bytes == null)) return error.InvalidExternalSourceInventory;
        if (self.stats_min_bytes) |min| {
            const max = self.stats_max_bytes.?;
            if (std.mem.order(u8, min, max) == .gt) return error.InvalidExternalSourceInventory;
        }
        if ((self.stats_min_bool == null) != (self.stats_max_bool == null)) return error.InvalidExternalSourceInventory;
        if (self.stats_min_bool == true and self.stats_max_bool == false) return error.InvalidExternalSourceInventory;
        if ((self.stats_min_f64 == null) != (self.stats_max_f64 == null)) return error.InvalidExternalSourceInventory;
        if (self.stats_min_f64) |min| {
            const max = self.stats_max_f64.?;
            if (std.math.isNan(min) or std.math.isNan(max) or min > max) return error.InvalidExternalSourceInventory;
        }
    }
};

pub const RowGroup = struct {
    ordinal: u32,
    row_count: u64,
    file_offset: u64 = 0,
    total_byte_len: u64 = 0,
    column_chunks: []ColumnChunk = &.{},

    pub fn deinit(self: *RowGroup, alloc: Allocator) void {
        for (self.column_chunks) |*chunk| chunk.deinit(alloc);
        if (self.column_chunks.len > 0) alloc.free(self.column_chunks);
        self.* = undefined;
    }

    pub fn validate(self: RowGroup, file_len: u64) !void {
        if (self.row_count == 0) return error.InvalidExternalSourceInventory;
        if (self.total_byte_len != 0) {
            if (self.file_offset > file_len) return error.InvalidExternalSourceInventory;
            if (self.total_byte_len > file_len - self.file_offset) return error.InvalidExternalSourceInventory;
        }
        for (self.column_chunks, 0..) |chunk, idx| {
            try chunk.validate(file_len);
            for (self.column_chunks[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.column_id, chunk.column_id)) return error.InvalidExternalSourceInventory;
            }
        }
    }
};

pub const FileEntry = struct {
    file_id: []u8,
    object_uri: []u8,
    etag: []u8 = &.{},
    version_id: []u8 = &.{},
    byte_len: u64,
    row_count: u64,
    data_sequence_number: ?i64 = null,
    partition_values: []PartitionValue = &.{},
    row_groups: []RowGroup,

    pub fn deinit(self: *FileEntry, alloc: Allocator) void {
        alloc.free(self.file_id);
        alloc.free(self.object_uri);
        if (self.etag.len > 0) alloc.free(self.etag);
        if (self.version_id.len > 0) alloc.free(self.version_id);
        for (self.partition_values) |*partition| partition.deinit(alloc);
        if (self.partition_values.len > 0) alloc.free(self.partition_values);
        for (self.row_groups) |*row_group| row_group.deinit(alloc);
        if (self.row_groups.len > 0) alloc.free(self.row_groups);
        self.* = undefined;
    }

    pub fn validate(self: FileEntry) !void {
        if (self.file_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.object_uri.len == 0) return error.InvalidExternalSourceInventory;
        if (self.etag.len == 0 and self.version_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.byte_len == 0) return error.InvalidExternalSourceInventory;
        if (self.data_sequence_number) |sequence| {
            if (sequence < 0) return error.InvalidExternalSourceInventory;
        }
        for (self.partition_values, 0..) |partition, idx| {
            try partition.validate();
            for (self.partition_values[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.column_id, partition.column_id)) return error.InvalidExternalSourceInventory;
            }
        }
        var total_rows: u64 = 0;
        for (self.row_groups, 0..) |row_group, idx| {
            try row_group.validate(self.byte_len);
            if (row_group.ordinal != idx) return error.InvalidExternalSourceInventory;
            total_rows += row_group.row_count;
        }
        if (self.row_groups.len != 0 and total_rows != self.row_count) return error.InvalidExternalSourceInventory;
    }
};

pub const PartitionValue = struct {
    column_id: []u8,
    string_value: []u8,

    pub fn deinit(self: *PartitionValue, alloc: Allocator) void {
        alloc.free(self.column_id);
        alloc.free(self.string_value);
        self.* = undefined;
    }

    pub fn validate(self: PartitionValue) !void {
        if (self.column_id.len == 0) return error.InvalidExternalSourceInventory;
    }
};

pub const Inventory = struct {
    format: Format,
    source_id: []u8,
    source_uri: []u8,
    snapshot_id: []u8,
    schema_fingerprint: []u8,
    files: []FileEntry,

    pub fn deinit(self: *Inventory, alloc: Allocator) void {
        alloc.free(self.source_id);
        alloc.free(self.source_uri);
        alloc.free(self.snapshot_id);
        alloc.free(self.schema_fingerprint);
        for (self.files) |*file| file.deinit(alloc);
        alloc.free(self.files);
        self.* = undefined;
    }

    pub fn validate(self: Inventory) !void {
        if (self.source_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.source_uri.len == 0) return error.InvalidExternalSourceInventory;
        if (self.snapshot_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.schema_fingerprint.len == 0) return error.InvalidExternalSourceInventory;
        for (self.files, 0..) |file, idx| {
            try file.validate();
            for (self.files[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.file_id, file.file_id)) return error.InvalidExternalSourceInventory;
            }
        }
    }

    pub fn fileById(self: Inventory, file_id: []const u8) ?FileEntry {
        for (self.files) |file| {
            if (std.mem.eql(u8, file.file_id, file_id)) return file;
        }
        return null;
    }
};

pub fn freeInventory(alloc: Allocator, inventory: *Inventory) void {
    inventory.deinit(alloc);
}

test "external source inventory validates files and row groups" {
    const alloc = std.testing.allocator;
    var inventory = Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events/file-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-file-a"),
        .byte_len = 1024,
        .row_count = 3,
        .row_groups = try alloc.dupe(RowGroup, &[_]RowGroup{
            .{
                .ordinal = 0,
                .row_count = 1,
                .file_offset = 4,
                .total_byte_len = 100,
                .column_chunks = try alloc.dupe(ColumnChunk, &[_]ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 16,
                    .compressed_len = 40,
                    .uncompressed_len = 80,
                    .compression_codec = try alloc.dupe(u8, "zstd"),
                    .encoding = try alloc.dupe(u8, "plain"),
                }}),
            },
            .{ .ordinal = 1, .row_count = 2, .file_offset = 104, .total_byte_len = 120 },
        }),
    };

    try inventory.validate();
    try std.testing.expect(inventory.fileById("file-a.parquet") != null);
    try std.testing.expect(inventory.fileById("missing.parquet") == null);
}
