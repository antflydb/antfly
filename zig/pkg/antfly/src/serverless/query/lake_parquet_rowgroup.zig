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

//! RowSource batch assembly for the first supported Parquet scan path.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source = @import("../external_source/types.zig");
const rowsource_bridge = @import("../external_source/rowsource_bridge.zig");
const parquet_page = @import("lake_parquet_page.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const ColumnChunkInput = struct {
    column_id: []const u8,
    /// Raw column chunk bytes for a required, uncompressed, PLAIN i64 column.
    bytes: []const u8,

    pub fn validate(self: ColumnChunkInput) !void {
        if (self.column_id.len == 0) return error.InvalidParquetRowGroupBatch;
        if (self.bytes.len == 0) return error.InvalidParquetRowGroupBatch;
    }
};

pub const RowGroupInput = struct {
    file_id: []const u8,
    row_group_ordinal: u32,
    chunks: []const ColumnChunkInput,

    pub fn validate(self: RowGroupInput) !void {
        if (self.file_id.len == 0) return error.InvalidParquetRowGroupBatch;
        if (self.chunks.len == 0) return error.InvalidParquetRowGroupBatch;
        for (self.chunks) |chunk| try chunk.validate();
    }
};

pub const OwnedBatch = struct {
    batch: rowsource.ColumnBatch,
    row_refs: []rowsource.RowRef,
    columns: []rowsource.ColumnVector,
    column_names: [][]u8,
    i64_values: [][]i64,

    pub fn deinit(self: *OwnedBatch, alloc: Allocator) void {
        for (self.column_names) |name| alloc.free(name);
        alloc.free(self.column_names);
        for (self.i64_values) |values| alloc.free(values);
        alloc.free(self.i64_values);
        alloc.free(self.columns);
        alloc.free(self.row_refs);
        self.* = undefined;
    }
};

pub const RowGroupSource = struct {
    inventory: external_source.Inventory,
    row_groups: []const RowGroupInput,
    next_index: usize = 0,
    current: ?OwnedBatch = null,

    pub fn init(inventory: external_source.Inventory, row_groups: []const RowGroupInput) !RowGroupSource {
        try inventory.validate();
        if (inventory.format != .parquet) return error.InvalidParquetRowGroupBatch;
        if (row_groups.len == 0) return error.InvalidParquetRowGroupBatch;
        for (row_groups) |row_group| try row_group.validate();
        return .{
            .inventory = inventory,
            .row_groups = row_groups,
        };
    }

    pub fn deinit(self: *RowGroupSource, alloc: Allocator) void {
        self.clearCurrent(alloc);
        self.* = undefined;
    }

    pub fn rowSource(self: *RowGroupSource) rowsource.Source {
        return .{
            .kind = .external_parquet,
            .ctx = self,
            .next_batch = nextBatch,
            .deinit_fn = deinitSource,
        };
    }

    fn nextBatch(ctx: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        const self: *RowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
        if (self.next_index >= self.row_groups.len) return null;

        const input = self.row_groups[self.next_index];
        self.next_index += 1;
        self.current = try buildRequiredPlainI64RowGroupBatchAlloc(
            alloc,
            self.inventory,
            input.file_id,
            input.row_group_ordinal,
            input.chunks,
        );
        return self.current.?.batch;
    }

    fn deinitSource(ctx: *anyopaque, alloc: Allocator) void {
        const self: *RowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
    }

    fn clearCurrent(self: *RowGroupSource, alloc: Allocator) void {
        if (self.current) |*current| {
            current.deinit(alloc);
            self.current = null;
        }
    }
};

pub fn buildRequiredPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    try inventory.validate();
    if (projected_chunks.len == 0) return error.InvalidParquetRowGroupBatch;
    const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
    if (row_group_ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
    const row_group = file.row_groups[row_group_ordinal];
    const row_count: usize = std.math.cast(usize, row_group.row_count) orelse return error.InvalidParquetRowGroupBatch;

    const row_refs = try alloc.alloc(rowsource.RowRef, row_count);
    errdefer alloc.free(row_refs);
    for (row_refs, 0..) |*row_ref, idx| {
        row_ref.* = try rowsource_bridge.rowRefForInventoryRow(inventory, file_id, row_group_ordinal, idx);
    }

    const columns = try alloc.alloc(rowsource.ColumnVector, projected_chunks.len);
    errdefer alloc.free(columns);
    const column_names = try alloc.alloc([]u8, projected_chunks.len);
    errdefer alloc.free(column_names);
    const i64_values = try alloc.alloc([]i64, projected_chunks.len);
    errdefer alloc.free(i64_values);

    var initialized_names: usize = 0;
    errdefer {
        for (column_names[0..initialized_names]) |name| alloc.free(name);
    }
    var initialized_values: usize = 0;
    errdefer {
        for (i64_values[0..initialized_values]) |values| alloc.free(values);
    }

    for (projected_chunks, 0..) |input, idx| {
        try input.validate();
        _ = findColumnChunk(row_group, input.column_id) orelse return error.ParquetColumnNotFound;
        column_names[idx] = try alloc.dupe(u8, input.column_id);
        initialized_names += 1;
        i64_values[idx] = try parquet_page.scanUncompressedPlainI64ColumnChunkAlloc(alloc, input.bytes);
        initialized_values += 1;
        if (i64_values[idx].len != row_count) return error.ParquetRowGroupRowCountMismatch;
        columns[idx] = .{
            .name = column_names[idx],
            .values = .{ .i64 = i64_values[idx] },
        };
    }

    const binding = try rowsource_bridge.bindingFromInventory(inventory);
    const batch = rowsource.ColumnBatch{
        .snapshot = binding.snapshot(),
        .row_refs = row_refs,
        .columns = columns,
    };
    try rowsource_bridge.validateBatchAgainstInventory(inventory, batch);

    return .{
        .batch = batch,
        .row_refs = row_refs,
        .columns = columns,
        .column_names = column_names,
        .i64_values = i64_values,
    };
}

fn findColumnChunk(row_group: external_source.RowGroup, column_id: []const u8) ?external_source.ColumnChunk {
    for (row_group.column_chunks) |chunk| {
        if (std.mem.eql(u8, chunk.column_id, column_id)) return chunk;
    }
    return null;
}

fn appendField(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, previous: *i16, id: i16, field_type: enum(u4) {
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
}) !void {
    const delta = id - previous.*;
    if (delta > 0 and delta <= 15) {
        try out.append(alloc, (@as(u8, @intCast(delta)) << 4) | @as(u8, @intFromEnum(field_type)));
    } else {
        try out.append(alloc, @intFromEnum(field_type));
        try appendI16(out, alloc, id);
    }
    previous.* = id;
}

fn appendStop(out: *std.ArrayListUnmanaged(u8), alloc: Allocator) !void {
    try out.append(alloc, 0);
}

fn appendI16(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i16) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i32) !void {
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

fn appendPlainI64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    const byte_len: i32 = @intCast(values.len * 8);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 5, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 2);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

test "parquet row group batch assembles decoded i64 columns with external row refs" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 96,
                .uncompressed_len = 96,
                .encoding = try alloc.dupe(u8, "plain"),
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20 });
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{30});

    var owned = try buildRequiredPlainI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 3), owned.batch.rowCount());
    try std.testing.expectEqualStrings("amount", owned.batch.columns[0].name);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, owned.batch.columns[0].values.i64);
    const first_ref = owned.batch.row_refs[0].external;
    try std.testing.expectEqualStrings("events", first_ref.source_id);
    try std.testing.expectEqualStrings("sha256:objects", first_ref.snapshot_id);
    try std.testing.expectEqualStrings("part-a.parquet", first_ref.file_id);
    try std.testing.expectEqual(@as(u32, 0), first_ref.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), first_ref.row_ordinal);
}

test "parquet row group source scans through lake rows" {
    const alloc = std.testing.allocator;
    const lake_rows = @import("lake_rows.zig");

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = 100,
                .total_byte_len = 64,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "plain"),
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = 200,
                .total_byte_len = 64,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 200,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "plain"),
                }}),
            },
        }),
    };
    try inventory.validate();

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainI64DataPage(&first_chunk, alloc, &[_]i64{ 10, 20 });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainI64DataPage(&second_chunk, alloc, &[_]i64{ 30, 40 });

    const first_chunks = [_]ColumnChunkInput{.{ .column_id = "amount", .bytes = first_chunk.items }};
    const second_chunks = [_]ColumnChunkInput{.{ .column_id = "amount", .bytes = second_chunk.items }};
    const row_groups = [_]RowGroupInput{
        .{ .file_id = "part-a.parquet", .row_group_ordinal = 0, .chunks = &first_chunks },
        .{ .file_id = "part-a.parquet", .row_group_ordinal = 1, .chunks = &second_chunks },
    };
    var source = try RowGroupSource.init(inventory, &row_groups);
    defer source.deinit(alloc);

    const projection = [_][]const u8{"amount"};
    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 30,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 30), result.rows[0].find("amount").?.value.?.i64);
    const row_ref = result.rows[0].row_ref.external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u32, 1), row_ref.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), row_ref.row_ordinal);
}

test "parquet row group batch rejects mismatched decoded row counts" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 24,
            }}),
        }}),
    };

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    try std.testing.expectError(error.ParquetRowGroupRowCountMismatch, buildRequiredPlainI64RowGroupBatchAlloc(
        alloc,
        inventory,
        "part-a.parquet",
        0,
        &[_]ColumnChunkInput{.{ .column_id = "amount", .bytes = chunk.items }},
    ));
}
