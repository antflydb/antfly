// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Align independently paged flat columns without retaining a decoded row group.
const std = @import("std");
const parquet = @import("lake_parquet_rowgroup.zig");
const page = @import("lake_parquet_page.zig");
const external = @import("../external_source/types.zig");
const ranges = @import("lake_range_io.zig");
const types = @import("../../storage/rowsource/types.zig");
const A = std.mem.Allocator;
pub const Cursor = struct {
    a: A,
    reader: parquet.ObjectRangeReader,
    inventory: external.Inventory,
    file: external.FileEntry,
    group: external.RowGroup,
    limits: parquet.MaterializationLimits,
    columns: []Column,
    position: u64 = 0,
    output: std.heap.ArenaAllocator,
    const Column = struct {
        chunk: external.ColumnChunk,
        offset: u64,
        decoded: ?parquet.OwnedBatch = null,
        dictionary: []u8 = &.{},
        first: u64 = 0,
        count: usize = 0,
        consumed: usize = 0,
    };
    pub fn init(a: A, reader: parquet.ObjectRangeReader, inventory: external.Inventory, file_id: []const u8, ordinal: u32, names: []const []const u8, limits: parquet.MaterializationLimits) !Cursor {
        const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
        const group = file.row_groups[ordinal];
        const columns = try a.alloc(Column, names.len);
        errdefer a.free(columns);
        for (names, columns) |name, *column| {
            const chunk = for (group.column_chunks) |candidate| {
                if (std.mem.eql(u8, candidate.column_id, name)) break candidate;
            } else return error.ParquetColumnNotFound;
            column.* = .{ .chunk = chunk, .offset = chunk.file_offset };
        }
        return .{ .a = a, .reader = reader, .inventory = inventory, .file = file, .group = group, .columns = columns, .limits = limits, .output = std.heap.ArenaAllocator.init(a) };
    }
    pub fn deinit(self: *Cursor) void {
        for (self.columns) |*column| {
            if (column.decoded) |*decoded| decoded.deinit(self.a);
            self.a.free(column.dictionary);
        }
        self.a.free(self.columns);
        self.output.deinit();
    }
    fn read(self: *Cursor, offset: u64, len: usize) ![]u8 {
        return self.reader.readPlannedAlloc(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = offset, .len = len }, .purpose = .parquet_column_chunk });
    }
    fn advance(self: *Cursor, column: *Column) !void {
        if (column.decoded) |*decoded| decoded.deinit(self.a);
        column.decoded = null;
        column.first += column.count;
        column.count = 0;
        column.consumed = 0;
        const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
        while (column.offset < end) {
            var probe_size: usize = @intCast(@min(end - column.offset, 512));
            const parsed = while (true) {
                const probe = try self.read(column.offset, probe_size);
                defer self.a.free(probe);
                const parsed = page.parsePageHeader(probe) catch |err| {
                    const next_size = @min(end - column.offset, @min(probe_size * 2, 64 * 1024));
                    if (next_size == probe_size) return err;
                    probe_size = @intCast(next_size);
                    continue;
                };
                break parsed;
            };
            try parsed.header.validateResourceLimits();
            const len = std.math.add(usize, parsed.header_len, parsed.header.compressed_page_size) catch return error.ParquetPageTooLarge;
            if (len > end - column.offset) return error.InvalidParquetPage;
            if (len +| column.dictionary.len > self.limits.max_input_bytes / @max(@as(usize, 1), self.columns.len) or parsed.header.uncompressed_page_size > self.limits.max_decoded_bytes / @max(@as(usize, 1), self.columns.len)) return error.ParquetPageTooLarge;
            const encoded = try self.read(column.offset, len);
            defer self.a.free(encoded);
            column.offset += len;
            switch (parsed.header.page_type) {
                .dictionary_page => {
                    if (column.dictionary.len != 0 or column.first != 0) return error.InvalidParquetPage;
                    column.dictionary = try self.a.dupe(u8, encoded);
                    continue;
                },
                .data_page, .data_page_v2 => {},
                else => return error.UnsupportedParquetPage,
            }
            const count: usize = parsed.header.value_count;
            if (count == 0 or column.first + count > self.group.row_count) return error.ParquetRowGroupRowCountMismatch;
            const input = try self.a.alloc(u8, column.dictionary.len + encoded.len);
            defer self.a.free(input);
            @memcpy(input[0..column.dictionary.len], column.dictionary);
            @memcpy(input[column.dictionary.len..], encoded);
            var limits = self.limits;
            limits.max_decoded_bytes /= @max(@as(usize, 1), self.columns.len);
            limits.max_struct_allocation_bytes /= @max(@as(usize, 1), self.columns.len);
            limits.page_row_count = count;
            limits.max_rows = @max(limits.max_rows, count);
            column.decoded = try parquet.buildSupportedI64RowGroupBatchAllocWithLimits(self.a, self.inventory, self.file.file_id, self.group.ordinal, &.{.{ .column_id = column.chunk.column_id, .bytes = input }}, limits);
            column.count = count;
            if (column.first + count == self.group.row_count and column.offset != end) return error.ParquetRowGroupRowCountMismatch;
            return;
        }
        return error.ParquetRowGroupRowCountMismatch;
    }
    pub fn next(self: *Cursor) !?types.ColumnBatch {
        _ = self.output.reset(.free_all);
        if (self.position == self.group.row_count) return null;
        var count: usize = 4096;
        for (self.columns) |*column| {
            if (column.consumed == column.count) try self.advance(column);
            count = @min(count, column.count - column.consumed);
        }
        if (count == 0 or self.columns.len == 0) return error.InvalidParquetPage;
        const a = self.output.allocator();
        const refs = try a.alloc(types.RowRef, count);
        const binding = @import("../external_source/rowsource_bridge.zig").bindingFromValidatedInventory(self.inventory);
        for (refs, 0..) |*ref, index| ref.* = try @import("../../storage/rowsource/external.zig").makeRowRef(binding, self.file.file_id, self.group.ordinal, self.position + index);
        const vectors = try a.alloc(types.ColumnVector, self.columns.len);
        for (self.columns, vectors) |*column, *vector| {
            const decoded = column.decoded.?.columns[0];
            const start = column.consumed;
            vector.* = decoded;
            vector.values = switch (decoded.values) {
                inline else => |values, tag| @unionInit(types.ColumnValues, @tagName(tag), values[start..][0..count]),
            };
            if (decoded.nulls.bytes.len != 0) vector.nulls.bytes = decoded.nulls.bytes[start..][0..count];
            column.consumed += count;
        }
        self.position += count;
        return .{ .snapshot = binding.snapshot(), .row_refs = refs, .columns = vectors };
    }
};
