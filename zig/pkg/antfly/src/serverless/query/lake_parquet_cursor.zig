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
    shared_reader: ?*@import("lake_serving_cache.zig").Reader = null,
    dictionary_decodes: usize = 0,
    pages_decoded: usize = 0,
    output: std.heap.ArenaAllocator,
    const Column = struct {
        chunk: external.ColumnChunk,
        offset: u64,
        decoded: ?parquet.OwnedBatch = null,
        dictionary: ?page.Dictionary = null,
        first: u64 = 0,
        count: usize = 0,
        consumed: usize = 0,
    };
    pub fn init(a: A, reader: parquet.ObjectRangeReader, inventory: external.Inventory, file_id: []const u8, ordinal: u32, names: []const []const u8, limits: parquet.MaterializationLimits) !Cursor {
        const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
        if (ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
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
            if (column.dictionary) |*dictionary| dictionary.deinit(self.a);
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
            const parsed = try self.header(column);
            const len = std.math.add(usize, parsed.header_len, parsed.header.compressed_page_size) catch return error.ParquetPageTooLarge;
            if (len > end - column.offset) return error.InvalidParquetPage;
            const dictionary_bytes = if (column.dictionary) |dictionary| dictionary.retainedBytes() else 0;
            const share = @max(@as(usize, 1), self.columns.len);
            if (len > self.limits.max_input_bytes / share or parsed.header.uncompressed_page_size +| dictionary_bytes > self.limits.max_decoded_bytes / share) return error.ParquetPageTooLarge;
            const encoded = try self.read(column.offset, len);
            defer self.a.free(encoded);
            column.offset += len;
            switch (parsed.header.page_type) {
                .dictionary_page => {
                    if (column.dictionary != null or column.first != 0) return error.InvalidParquetPage;
                    var dictionary = try self.decodeDictionary(column.chunk, parsed.header, encoded[parsed.header_len..]);
                    errdefer dictionary.deinit(self.a);
                    if (dictionary.retainedBytes() > self.limits.max_decoded_bytes / share) return error.ParquetPageTooLarge;
                    column.dictionary = dictionary;
                    self.dictionary_decodes += 1;
                    continue;
                },
                .data_page, .data_page_v2 => {},
                else => return error.UnsupportedParquetPage,
            }
            const count: usize = parsed.header.value_count;
            if (count == 0 or column.first + count > self.group.row_count) return error.ParquetRowGroupRowCountMismatch;
            var limits = self.limits;
            limits.decimal_representation = .exact_string;
            limits.max_decoded_bytes = self.limits.max_decoded_bytes / share - dictionary_bytes;
            limits.max_struct_allocation_bytes /= @max(@as(usize, 1), self.columns.len);
            limits.page_row_count = count;
            limits.page_encoding = parsed.header.encoding;
            limits.max_rows = @max(limits.max_rows, count);
            column.decoded = try parquet.buildSupportedI64RowGroupBatchAllocWithLimits(self.a, self.inventory, self.file.file_id, self.group.ordinal, &.{.{ .column_id = column.chunk.column_id, .bytes = encoded, .dictionary = if (column.dictionary) |*dictionary| dictionary else null }}, limits);
            column.count = count;
            self.pages_decoded += 1;
            if (column.first + count == self.group.row_count and column.offset != end) return error.ParquetRowGroupRowCountMismatch;
            return;
        }
        return error.ParquetRowGroupRowCountMismatch;
    }
    fn header(self: *Cursor, column: *const Column) !page.ParsedHeader {
        const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
        if (column.offset >= end) return error.InvalidParquetPage;
        var probe_size: usize = @intCast(@min(end - column.offset, 512));
        while (true) {
            const probe = try self.read(column.offset, probe_size);
            defer self.a.free(probe);
            const parsed = page.parsePageHeader(probe) catch |err| {
                const next_size = @min(end - column.offset, @min(probe_size * 2, 64 * 1024));
                if (next_size == probe_size) return err;
                probe_size = @intCast(next_size);
                continue;
            };
            try parsed.header.validateResourceLimits();
            return parsed;
        }
    }
    fn decodeDictionary(self: *Cursor, chunk: external.ColumnChunk, header_value: page.Header, encoded: []const u8) !page.Dictionary {
        const payload = try page.decodePagePayloadAlloc(self.a, header_value, try parquet.compressionCodecForColumnChunk(chunk), encoded);
        defer payload.deinit(self.a);
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int32")) return .{ .i64 = try page.decodePlainI32DictionaryPageAsI64Alloc(self.a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int64") or chunk.physical_type.len == 0) return .{ .i64 = try page.decodePlainI64DictionaryPageAlloc(self.a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "float")) return .{ .f64 = try page.decodePlainF32DictionaryPageAsF64Alloc(self.a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "double")) return .{ .f64 = try page.decodePlainF64DictionaryPageAlloc(self.a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "fixed_len_byte_array")) {
            if (chunk.type_length <= 0) return error.UnsupportedParquetPage;
            return .{ .bytes = try page.decodePlainFixedLenByteArrayDictionaryPageAlloc(self.a, header_value, payload.bytes, @intCast(chunk.type_length)) };
        }
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "byte_array")) return .{ .bytes = try page.decodePlainByteArrayDictionaryPageAlloc(self.a, header_value, payload.bytes) };
        return error.UnsupportedParquetPage;
    }
    /// Inspect the next headers while this page is being consumed, then warm
    /// the exact versioned ranges used by advance. Parallelism is bounded by
    /// the shared reader's four workers and 32 MiB lookahead quota.
    fn prefetchPages(self: *Cursor) !void {
        const reader = self.shared_reader orelse return;
        if (reader.context.io == null or self.position == self.group.row_count) return;
        var reads: [4]ranges.RangeRead = undefined;
        var count: usize = 0;
        for (self.columns) |*column| {
            if (count == reads.len) break;
            if (column.consumed != column.count) continue;
            const parsed = try self.header(column);
            const len = std.math.add(usize, parsed.header_len, parsed.header.compressed_page_size) catch return error.ParquetPageTooLarge;
            const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
            if (len > end - column.offset or len > self.limits.max_input_bytes / @max(@as(usize, 1), self.columns.len)) return error.ParquetPageTooLarge;
            reads[count] = .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset, .len = len }, .purpose = .parquet_column_chunk };
            count += 1;
        }
        if (count != 0) try reader.prefetch(reads[0..count]);
    }
    const DecodeStats = struct { pages: usize, dictionaries: usize };
    const DecodeAllocator = struct {
        backing: A,
        mutex: std.atomic.Mutex = .unlocked,
        fn allocator(self: *DecodeAllocator) A {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
        }
        fn lock(self: *DecodeAllocator) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }
        fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            self.lock();
            defer self.mutex.unlock();
            return self.backing.rawAlloc(len, alignment, ra);
        }
        fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            self.lock();
            defer self.mutex.unlock();
            return self.backing.rawResize(bytes, alignment, len, ra);
        }
        fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            self.lock();
            defer self.mutex.unlock();
            return self.backing.rawRemap(bytes, alignment, len, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            self.lock();
            defer self.mutex.unlock();
            self.backing.rawFree(bytes, alignment, ra);
        }
    };
    fn decode(template: Cursor, column: *Column, a: A) anyerror!DecodeStats {
        // A worker mutates its own column only. The scoped allocator serializes
        // admission/arena mutations, while provider I/O and decoding overlap.
        var worker = template;
        worker.a = a;
        worker.pages_decoded = 0;
        worker.dictionary_decodes = 0;
        try worker.advance(column);
        return .{ .pages = worker.pages_decoded, .dictionaries = worker.dictionary_decodes };
    }
    fn advanceColumns(self: *Cursor) !void {
        const io = if (self.shared_reader) |reader| reader.context.io else null;
        if (io == null or self.columns.len < 2) {
            for (self.columns) |*column| if (column.consumed == column.count) try self.advance(column);
            return;
        }
        var allocator: DecodeAllocator = .{ .backing = self.a };
        var pending: [4]?std.Io.Future(anyerror!DecodeStats) = @splat(null);
        // Every worker joins on all error/cancellation paths before the scoped
        // allocator or the cursor metadata can leave scope.
        defer for (&pending) |*future| if (future.*) |*active| {
            _ = active.cancel(io.?) catch {};
            future.* = null;
        };
        var next_column: usize = 0;
        while (next_column < self.columns.len) {
            for (&pending) |*future| {
                while (next_column < self.columns.len and self.columns[next_column].consumed != self.columns[next_column].count) next_column += 1;
                if (next_column == self.columns.len) break;
                const column = &self.columns[next_column];
                future.* = io.?.concurrent(decode, .{ self.*, column, allocator.allocator() }) catch {
                    const stats = try decode(self.*, column, allocator.allocator());
                    self.pages_decoded += stats.pages;
                    self.dictionary_decodes += stats.dictionaries;
                    next_column += 1;
                    continue;
                };
                next_column += 1;
            }
            var failure: ?anyerror = null;
            for (&pending) |*future| if (future.*) |*active| {
                const stats = active.await(io.?) catch |err| {
                    future.* = null;
                    failure = failure orelse err;
                    continue;
                };
                future.* = null;
                self.pages_decoded += stats.pages;
                self.dictionary_decodes += stats.dictionaries;
            };
            if (failure) |err| return err;
        }
    }
    pub fn next(self: *Cursor) !?types.ColumnBatch {
        _ = self.output.reset(.free_all);
        if (self.position == self.group.row_count) return null;
        try self.advanceColumns();
        var count: usize = 4096;
        for (self.columns) |*column| {
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
        try self.prefetchPages();
        return .{ .snapshot = binding.snapshot(), .row_refs = refs, .columns = vectors };
    }
};
