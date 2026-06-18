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
const parquet_footer = @import("lake_parquet_footer.zig");
const parquet_metadata = @import("lake_parquet_metadata.zig");
const parquet_page = @import("lake_parquet_page.zig");
const range_io = @import("lake_range_io.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const snappy = @import("../../encoding/snappy.zig");

pub const ColumnChunkInput = struct {
    column_id: []const u8,
    /// Raw column chunk bytes for one supported uncompressed i64 column path.
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

pub const ObjectRangeReader = struct {
    ctx: *anyopaque,
    read_range_alloc: *const fn (
        ctx: *anyopaque,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        offset: u64,
        len: usize,
    ) anyerror![]u8,

    pub fn readAlloc(
        self: ObjectRangeReader,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        if (bucket.len == 0 or key.len == 0 or len == 0) return error.InvalidLakeRangeRead;
        return try self.read_range_alloc(self.ctx, alloc, bucket, key, offset, len);
    }
};

pub const ObjectRangeRowGroupInput = struct {
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,

    pub fn validate(self: ObjectRangeRowGroupInput) !void {
        if (self.file_id.len == 0) return error.InvalidParquetRowGroupBatch;
        if (self.projected_columns.len == 0) return error.InvalidParquetRowGroupBatch;
        for (self.projected_columns) |column| {
            if (column.len == 0) return error.InvalidParquetRowGroupBatch;
        }
    }
};

pub const ObjectRangeRowGroupPlan = struct {
    row_groups: []ObjectRangeRowGroupInput,

    pub fn deinit(self: *ObjectRangeRowGroupPlan, alloc: Allocator) void {
        alloc.free(self.row_groups);
        self.* = undefined;
    }
};

pub const DiscoveredObjectRangeRowGroupPlan = struct {
    inventory: external_source.Inventory,
    row_group_plan: ObjectRangeRowGroupPlan,

    pub fn deinit(self: *DiscoveredObjectRangeRowGroupPlan, alloc: Allocator) void {
        self.row_group_plan.deinit(alloc);
        self.inventory.deinit(alloc);
        self.* = undefined;
    }
};

pub const OwnedBatch = struct {
    batch: rowsource.ColumnBatch,
    row_refs: []rowsource.RowRef,
    columns: []rowsource.ColumnVector,
    column_names: [][]u8,
    i64_values: [][]i64,
    null_bitmaps: [][]u8,

    pub fn deinit(self: *OwnedBatch, alloc: Allocator) void {
        for (self.column_names) |name| alloc.free(name);
        alloc.free(self.column_names);
        for (self.i64_values) |values| alloc.free(values);
        alloc.free(self.i64_values);
        for (self.null_bitmaps) |nulls| {
            if (nulls.len > 0) alloc.free(nulls);
        }
        alloc.free(self.null_bitmaps);
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
        self.current = try buildSupportedI64RowGroupBatchAlloc(
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

pub const ObjectRangeRowGroupSource = struct {
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    row_groups: []const ObjectRangeRowGroupInput,
    next_index: usize = 0,
    current: ?OwnedBatch = null,

    pub fn init(
        reader: ObjectRangeReader,
        inventory: external_source.Inventory,
        row_groups: []const ObjectRangeRowGroupInput,
    ) !ObjectRangeRowGroupSource {
        try inventory.validate();
        if (inventory.format != .parquet) return error.InvalidParquetRowGroupBatch;
        if (row_groups.len == 0) return error.InvalidParquetRowGroupBatch;
        for (row_groups) |row_group| try row_group.validate();
        return .{
            .reader = reader,
            .inventory = inventory,
            .row_groups = row_groups,
        };
    }

    pub fn deinit(self: *ObjectRangeRowGroupSource, alloc: Allocator) void {
        self.clearCurrent(alloc);
        self.* = undefined;
    }

    pub fn rowSource(self: *ObjectRangeRowGroupSource) rowsource.Source {
        return .{
            .kind = .external_parquet,
            .ctx = self,
            .next_batch = nextBatch,
            .deinit_fn = deinitSource,
        };
    }

    fn nextBatch(ctx: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        const self: *ObjectRangeRowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
        if (self.next_index >= self.row_groups.len) return null;

        const input = self.row_groups[self.next_index];
        self.next_index += 1;
        self.current = try buildSupportedI64RowGroupBatchFromObjectRangeReaderAlloc(
            alloc,
            self.reader,
            self.inventory,
            input.file_id,
            input.row_group_ordinal,
            input.projected_columns,
        );
        return self.current.?.batch;
    }

    fn deinitSource(ctx: *anyopaque, alloc: Allocator) void {
        const self: *ObjectRangeRowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
    }

    fn clearCurrent(self: *ObjectRangeRowGroupSource, alloc: Allocator) void {
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
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{ .fixed = .required });
}

pub fn buildOptionalPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{ .fixed = .optional });
}

pub fn buildDictionaryPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{ .fixed = .dictionary_required });
}

pub fn buildSupportedI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .from_inventory);
}

const PlainI64Mode = enum {
    required,
    optional,
    dictionary_required,
};

const PlainI64ModeRequest = union(enum) {
    fixed: PlainI64Mode,
    from_inventory,
};

fn buildPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
    mode_request: PlainI64ModeRequest,
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
    const null_bitmaps = try alloc.alloc([]u8, projected_chunks.len);
    errdefer alloc.free(null_bitmaps);

    var initialized_names: usize = 0;
    errdefer {
        for (column_names[0..initialized_names]) |name| alloc.free(name);
    }
    var initialized_values: usize = 0;
    errdefer {
        for (i64_values[0..initialized_values]) |values| alloc.free(values);
    }
    var initialized_nulls: usize = 0;
    errdefer {
        for (null_bitmaps[0..initialized_nulls]) |nulls| {
            if (nulls.len > 0) alloc.free(nulls);
        }
    }

    for (projected_chunks, 0..) |input, idx| {
        try input.validate();
        const chunk = findColumnChunk(row_group, input.column_id) orelse return error.ParquetColumnNotFound;
        const mode = switch (mode_request) {
            .fixed => |fixed| fixed,
            .from_inventory => try plainI64ModeForColumnChunk(chunk),
        };
        const compression = try compressionCodecForColumnChunk(chunk);
        column_names[idx] = try alloc.dupe(u8, input.column_id);
        initialized_names += 1;
        switch (mode) {
            .required => {
                i64_values[idx] = try parquet_page.scanPlainI64ColumnChunkAlloc(alloc, input.bytes, compression);
                null_bitmaps[idx] = &.{};
            },
            .optional => {
                var decoded = try parquet_page.scanOptionalPlainI64ColumnChunkAlloc(alloc, input.bytes, compression);
                i64_values[idx] = decoded.values;
                null_bitmaps[idx] = decoded.nulls;
                decoded = undefined;
            },
            .dictionary_required => {
                i64_values[idx] = try parquet_page.scanDictionaryI64ColumnChunkAlloc(alloc, input.bytes, compression);
                null_bitmaps[idx] = &.{};
            },
        }
        initialized_values += 1;
        initialized_nulls += 1;
        if (i64_values[idx].len != row_count) return error.ParquetRowGroupRowCountMismatch;
        columns[idx] = .{
            .name = column_names[idx],
            .values = .{ .i64 = i64_values[idx] },
            .nulls = .{ .bytes = null_bitmaps[idx] },
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
        .null_bitmaps = null_bitmaps,
    };
}

pub fn buildRequiredPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
        alloc,
        reader,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .{ .fixed = .required },
    );
}

pub fn buildSupportedI64RowGroupBatchFromObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
        alloc,
        reader,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .from_inventory,
    );
}

fn buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
    mode_request: PlainI64ModeRequest,
) !OwnedBatch {
    try inventory.validate();
    if (projected_columns.len == 0) return error.InvalidParquetRowGroupBatch;
    const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
    if (row_group_ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
    const row_group = file.row_groups[row_group_ordinal];
    const object = try range_io.objectRefForExternalFileUri(file);

    const chunk_inputs = try alloc.alloc(ColumnChunkInput, projected_columns.len);
    errdefer alloc.free(chunk_inputs);
    const chunk_bytes = try alloc.alloc([]u8, projected_columns.len);
    errdefer alloc.free(chunk_bytes);
    var initialized_bytes: usize = 0;
    errdefer {
        for (chunk_bytes[0..initialized_bytes]) |bytes| alloc.free(bytes);
    }

    for (projected_columns, 0..) |column_id, idx| {
        if (column_id.len == 0) return error.InvalidParquetRowGroupBatch;
        const chunk = findColumnChunk(row_group, column_id) orelse return error.ParquetColumnNotFound;
        const read = try range_io.planColumnChunkRead(object, chunk);
        const read_len: usize = std.math.cast(usize, read.range.len) orelse return error.InvalidLakeRangeRead;
        chunk_bytes[idx] = try reader.readAlloc(alloc, read.object.bucket, read.object.key, read.range.offset, read_len);
        if (chunk_bytes[idx].len != read_len) return error.InvalidLakeRangeRead;
        initialized_bytes += 1;
        chunk_inputs[idx] = .{
            .column_id = column_id,
            .bytes = chunk_bytes[idx],
        };
    }

    var owned = try buildPlainI64RowGroupBatchAlloc(
        alloc,
        inventory,
        file_id,
        row_group_ordinal,
        chunk_inputs,
        mode_request,
    );
    errdefer owned.deinit(alloc);

    for (chunk_bytes[0..initialized_bytes]) |bytes| alloc.free(bytes);
    alloc.free(chunk_bytes);
    alloc.free(chunk_inputs);
    return owned;
}

pub fn planRequiredPlainI64ObjectRangeRowGroupsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
) !ObjectRangeRowGroupPlan {
    return try planObjectRangeRowGroupsAlloc(alloc, inventory, projected_columns, .none);
}

pub fn planSupportedI64ObjectRangeRowGroupsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
) !ObjectRangeRowGroupPlan {
    return try planObjectRangeRowGroupsAlloc(alloc, inventory, projected_columns, .supported_i64);
}

pub fn discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    raw_inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    footer_probe_bytes: u64,
) !DiscoveredObjectRangeRowGroupPlan {
    try raw_inventory.validate();
    if (raw_inventory.format != .parquet) return error.InvalidParquetRowGroupBatch;
    if (raw_inventory.files.len == 0) return error.InvalidParquetRowGroupBatch;
    if (footer_probe_bytes == 0) return error.InvalidLakeRangeRead;

    const footers = try alloc.alloc(parquet_metadata.FileFooter, raw_inventory.files.len);
    errdefer alloc.free(footers);
    var initialized_footers: usize = 0;
    errdefer {
        for (footers[0..initialized_footers]) |*entry| entry.footer.deinit(alloc);
    }

    for (raw_inventory.files, 0..) |file, idx| {
        const object = try range_io.objectRefForExternalFileUri(file);
        const tail_read = try range_io.planParquetFooterRead(object, footer_probe_bytes);
        const tail_len: usize = std.math.cast(usize, tail_read.range.len) orelse return error.InvalidLakeRangeRead;
        const tail = try reader.readAlloc(alloc, tail_read.object.bucket, tail_read.object.key, tail_read.range.offset, tail_len);
        defer alloc.free(tail);
        if (tail.len != tail_len) return error.InvalidLakeRangeRead;

        const preflight = try parquet_footer.parseFooterPreflight(object.byte_len, tail_read.range.offset, tail);
        const metadata_bytes = if (preflight.metadataSlice(tail)) |slice|
            try alloc.dupe(u8, slice)
        else blk: {
            const read = try parquet_footer.planFooterMetadataRead(object, tail_read.range.offset, tail);
            const read_len: usize = std.math.cast(usize, read.range.len) orelse return error.InvalidLakeRangeRead;
            const bytes = try reader.readAlloc(alloc, read.object.bucket, read.object.key, read.range.offset, read_len);
            errdefer alloc.free(bytes);
            if (bytes.len != read_len) return error.InvalidLakeRangeRead;
            break :blk bytes;
        };
        defer alloc.free(metadata_bytes);

        footers[idx] = .{
            .file_id = file.file_id,
            .footer = try parquet_metadata.parseFooterMetadataAlloc(alloc, metadata_bytes, file.byte_len),
        };
        initialized_footers += 1;
    }

    var enriched = try parquet_metadata.enrichInventoryFilesWithFootersAlloc(alloc, raw_inventory, footers);
    errdefer enriched.deinit(alloc);
    var row_group_plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, enriched, projected_columns);
    errdefer row_group_plan.deinit(alloc);

    for (footers[0..initialized_footers]) |*entry| entry.footer.deinit(alloc);
    alloc.free(footers);

    return .{
        .inventory = enriched,
        .row_group_plan = row_group_plan,
    };
}

const RowGroupPlanValidation = enum {
    none,
    supported_i64,
};

fn planObjectRangeRowGroupsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    validation: RowGroupPlanValidation,
) !ObjectRangeRowGroupPlan {
    try inventory.validate();
    if (inventory.format != .parquet) return error.InvalidParquetRowGroupBatch;
    if (projected_columns.len == 0) return error.InvalidParquetRowGroupBatch;
    for (projected_columns) |column| {
        if (column.len == 0) return error.InvalidParquetRowGroupBatch;
    }

    var total_row_groups: usize = 0;
    for (inventory.files) |file| {
        for (file.row_groups) |row_group| {
            for (projected_columns) |column| {
                const chunk = findColumnChunk(row_group, column) orelse return error.ParquetColumnNotFound;
                try validatePlannedChunk(chunk, validation);
            }
            total_row_groups += 1;
        }
    }
    if (total_row_groups == 0) return error.InvalidParquetRowGroupBatch;

    const row_groups = try alloc.alloc(ObjectRangeRowGroupInput, total_row_groups);
    errdefer alloc.free(row_groups);
    var out_idx: usize = 0;
    for (inventory.files) |file| {
        for (file.row_groups) |row_group| {
            row_groups[out_idx] = .{
                .file_id = file.file_id,
                .row_group_ordinal = row_group.ordinal,
                .projected_columns = projected_columns,
            };
            out_idx += 1;
        }
    }

    return .{ .row_groups = row_groups };
}

fn validatePlannedChunk(chunk: external_source.ColumnChunk, validation: RowGroupPlanValidation) !void {
    switch (validation) {
        .none => {},
        .supported_i64 => {
            _ = try plainI64ModeForColumnChunk(chunk);
            _ = try compressionCodecForColumnChunk(chunk);
        },
    }
}

fn findColumnChunk(row_group: external_source.RowGroup, column_id: []const u8) ?external_source.ColumnChunk {
    for (row_group.column_chunks) |chunk| {
        if (std.mem.eql(u8, chunk.column_id, column_id)) return chunk;
    }
    return null;
}

fn plainI64ModeForColumnChunk(chunk: external_source.ColumnChunk) !PlainI64Mode {
    if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .required;
    if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
        std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
    {
        return .dictionary_required;
    }
    return error.UnsupportedParquetPage;
}

fn compressionCodecForColumnChunk(chunk: external_source.ColumnChunk) !parquet_page.CompressionCodec {
    if (chunk.compression_codec.len == 0 or
        std.ascii.eqlIgnoreCase(chunk.compression_codec, "uncompressed") or
        std.ascii.eqlIgnoreCase(chunk.compression_codec, "none"))
    {
        return .uncompressed;
    }
    if (std.ascii.eqlIgnoreCase(chunk.compression_codec, "snappy")) return .snappy;
    return error.UnsupportedParquetPage;
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

fn appendListHeader(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, elem_type: enum(u4) {
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
}, len: usize) !void {
    if (len < 15) {
        try out.append(alloc, (@as(u8, @intCast(len)) << 4) | @as(u8, @intFromEnum(elem_type)));
    } else {
        try out.append(alloc, 0xf0 | @as(u8, @intFromEnum(elem_type)));
        try appendVarint(out, alloc, len);
    }
}

fn appendBinary(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    try appendVarint(out, alloc, bytes.len);
    try out.appendSlice(alloc, bytes);
}

fn appendPlainI64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    const byte_len: usize = values.len * 8;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendSnappyPlainI64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try payload.appendSlice(alloc, &buf);
    }

    const compressed = try snappy.encode(alloc, payload.items);
    defer alloc.free(compressed);
    try appendPlainI64DataPageHeader(out, alloc, values.len, payload.items.len, compressed.len);
    try out.appendSlice(alloc, compressed);
}

fn appendPlainI64DataPageHeader(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    value_count: usize,
    uncompressed_byte_len: usize,
    compressed_byte_len: usize,
) !void {
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, @intCast(uncompressed_byte_len));
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(compressed_byte_len));
    try appendField(out, alloc, &page_prev, 5, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(value_count));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 2);
    try appendStop(out, alloc);
    try appendStop(out, alloc);
}

fn appendPlainI64DictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    const byte_len: i32 = @intCast(values.len * 8);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendDictionaryI64DataPage(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    value_count: usize,
    bit_width: u8,
    encoded_indexes: []const u8,
) !void {
    const byte_len: i32 = @intCast(1 + encoded_indexes.len);
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
    try appendI32(out, alloc, @intCast(value_count));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, 7);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 2);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try out.append(alloc, bit_width);
    try out.appendSlice(alloc, encoded_indexes);
}

fn appendSingleColumnFooterMetadata(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    column_id: []const u8,
    row_count: usize,
    column_offset: usize,
    compressed_len: usize,
    uncompressed_len: usize,
    encoding: i32,
    compression_codec: i32,
) !void {
    var file_prev: i16 = 0;
    try appendField(out, alloc, &file_prev, 1, .i32);
    try appendI32(out, alloc, 1);
    try appendField(out, alloc, &file_prev, 3, .i64);
    try appendI64(out, alloc, @intCast(row_count));
    try appendField(out, alloc, &file_prev, 4, .list);
    try appendListHeader(out, alloc, .struct_, 1);

    var rg_prev: i16 = 0;
    try appendField(out, alloc, &rg_prev, 1, .list);
    try appendListHeader(out, alloc, .struct_, 1);

    var chunk_prev: i16 = 0;
    try appendField(out, alloc, &chunk_prev, 2, .i64);
    try appendI64(out, alloc, @intCast(column_offset));
    try appendField(out, alloc, &chunk_prev, 3, .struct_);

    var meta_prev: i16 = 0;
    try appendField(out, alloc, &meta_prev, 1, .i32);
    try appendI32(out, alloc, 1);
    try appendField(out, alloc, &meta_prev, 2, .list);
    try appendListHeader(out, alloc, .i32, 1);
    try appendI32(out, alloc, encoding);
    try appendField(out, alloc, &meta_prev, 3, .list);
    try appendListHeader(out, alloc, .binary, 1);
    try appendBinary(out, alloc, column_id);
    try appendField(out, alloc, &meta_prev, 4, .i32);
    try appendI32(out, alloc, compression_codec);
    try appendField(out, alloc, &meta_prev, 6, .i64);
    try appendI64(out, alloc, @intCast(uncompressed_len));
    try appendField(out, alloc, &meta_prev, 7, .i64);
    try appendI64(out, alloc, @intCast(compressed_len));
    try appendField(out, alloc, &meta_prev, 9, .i64);
    try appendI64(out, alloc, @intCast(column_offset));
    try appendStop(out, alloc);

    try appendStop(out, alloc);
    try appendField(out, alloc, &rg_prev, 2, .i64);
    try appendI64(out, alloc, @intCast(uncompressed_len));
    try appendField(out, alloc, &rg_prev, 3, .i64);
    try appendI64(out, alloc, @intCast(row_count));
    try appendField(out, alloc, &rg_prev, 5, .i64);
    try appendI64(out, alloc, @intCast(column_offset));
    try appendStop(out, alloc);

    try appendStop(out, alloc);
}

fn appendParquetTrailer(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, metadata_len: usize) !void {
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(metadata_len), .little);
    try out.appendSlice(alloc, &len_buf);
    try out.appendSlice(alloc, "PAR1");
}

fn appendOptionalPlainI64DataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?i64) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_count * 8);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
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

test "parquet row group batch assembles optional i64 columns with null bitmap" {
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
    try appendOptionalPlainI64DataPageV2(&chunk, alloc, &[_]?i64{ 10, null, 30 });

    var owned = try buildOptionalPlainI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 3), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 0, 30 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[0].nulls.bytes);
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(0));
    try std.testing.expect(owned.batch.columns[0].nulls.isNull(1));
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(2));
    const null_ref = owned.batch.row_refs[1].external;
    try std.testing.expectEqualStrings("part-a.parquet", null_ref.file_id);
    try std.testing.expectEqual(@as(u64, 1), null_ref.row_ordinal);
}

test "parquet row group batch assembles dictionary i64 columns" {
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
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 4,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 96,
                .uncompressed_len = 96,
                .encoding = try alloc.dupe(u8, "rle_dictionary"),
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&chunk, alloc, &[_]i64{ 11, 22, 33 });
    try appendDictionaryI64DataPage(&chunk, alloc, 4, 2, &[_]u8{ 3, 0b10000100, 0 });

    var owned = try buildDictionaryPlainI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 4), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 11, 22, 11, 33 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqual(@as(usize, 0), owned.batch.columns[0].nulls.bytes.len);
    const row_ref = owned.batch.row_refs[3].external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u64, 3), row_ref.row_ordinal);
}

test "parquet row group batch dispatches supported i64 encodings from inventory" {
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
        .byte_len = 2048,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 4,
            .file_offset = 100,
            .total_byte_len = 256,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30, 40 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&tenant_chunk, alloc, &[_]i64{ 7, 8 });
    try appendDictionaryI64DataPage(&tenant_chunk, alloc, 4, 1, &[_]u8{ 3, 0b00001010 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 4), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30, 40 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 8, 7, 8 }, owned.batch.columns[1].values.i64);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "amount", "tenant" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);

    inventory.files[0].row_groups[0].column_chunks[1].encoding[0] = 'd';
    try std.testing.expectError(error.UnsupportedParquetPage, planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"tenant"}));
    try std.testing.expectError(error.UnsupportedParquetPage, buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    }));
}

test "parquet row group batch dispatches supported compression from inventory" {
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
                .compression_codec = try alloc.dupe(u8, "snappy"),
                .encoding = try alloc.dupe(u8, "plain"),
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendSnappyPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, owned.batch.columns[0].values.i64);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"amount"});
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);

    inventory.files[0].row_groups[0].column_chunks[0].compression_codec[0] = 'z';
    try std.testing.expectError(error.UnsupportedParquetPage, planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"amount"}));
    try std.testing.expectError(error.UnsupportedParquetPage, buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }}));
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
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
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
    try appendPlainI64DictionaryPage(&second_chunk, alloc, &[_]i64{ 30, 40 });
    try appendDictionaryI64DataPage(&second_chunk, alloc, 2, 1, &[_]u8{ 3, 0b00000010 });

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

test "parquet object range row group source reads chunks into lake rows" {
    const alloc = std.testing.allocator;
    const lake_rows = @import("lake_rows.zig");

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainI64DataPage(&first_chunk, alloc, &[_]i64{ 10, 20 });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&second_chunk, alloc, &[_]i64{ 30, 40 });
    try appendDictionaryI64DataPage(&second_chunk, alloc, 2, 1, &[_]u8{ 3, 0b00000010 });

    const first_offset: usize = 100;
    const second_offset: usize = 200;
    const object_len = second_offset + second_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[first_offset..][0..first_chunk.items.len], first_chunk.items);
    @memcpy(object_bytes[second_offset..][0..second_chunk.items.len], second_chunk.items);

    const MemoryRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

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
        .byte_len = object_len,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = first_offset,
                .total_byte_len = first_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = first_offset,
                    .compressed_len = first_chunk.items.len,
                    .uncompressed_len = first_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = second_offset,
                .total_byte_len = second_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = second_offset,
                    .compressed_len = second_chunk.items.len,
                    .uncompressed_len = second_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                }}),
            },
        }),
    };
    try inventory.validate();

    const projection = [_][]const u8{"amount"};
    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &projection);
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), plan.row_groups.len);
    try std.testing.expectEqualStrings("part-a.parquet", plan.row_groups[0].file_id);
    try std.testing.expectEqual(@as(u32, 0), plan.row_groups[0].row_group_ordinal);
    try std.testing.expectEqual(@as(u32, 1), plan.row_groups[1].row_group_ordinal);
    try std.testing.expectError(
        error.ParquetColumnNotFound,
        planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"missing"}),
    );

    var source = try ObjectRangeRowGroupSource.init(range_reader.reader(), inventory, plan.row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 40,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 40), result.rows[0].find("amount").?.value.?.i64);
    const row_ref = result.rows[0].row_ref.external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u32, 1), row_ref.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 1), row_ref.row_ordinal);
}

test "parquet object range discovery reads footers and builds row group source" {
    const alloc = std.testing.allocator;
    const lake_rows = @import("lake_rows.zig");

    const column_offset: usize = 100;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    var object = std.ArrayListUnmanaged(u8).empty;
    defer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, column_offset);
    try object.appendSlice(alloc, chunk.items);
    const metadata_start = object.items.len;
    try appendSingleColumnFooterMetadata(
        &object,
        alloc,
        "amount",
        3,
        column_offset,
        chunk.items.len,
        chunk.items.len,
        0,
        0,
    );
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);

    const MemoryRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object.items,
    };

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
        .byte_len = object.items.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projection = [_][]const u8{"amount"};
    var discovered = try discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
        alloc,
        range_reader.reader(),
        raw,
        &projection,
        16,
    );
    defer discovered.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), discovered.inventory.files[0].row_count);
    try std.testing.expectEqual(@as(usize, 1), discovered.row_group_plan.row_groups.len);
    try std.testing.expectEqualStrings("amount", discovered.row_group_plan.row_groups[0].projected_columns[0]);

    var source = try ObjectRangeRowGroupSource.init(range_reader.reader(), discovered.inventory, discovered.row_group_plan.row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 20,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    const row_ref = result.rows[0].row_ref.external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u64, 1), row_ref.row_ordinal);
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
