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
    /// Residual evidence is evaluated before projected payloads are decoded.
    filter: ?Filter = null,
    prune_ptr: ?*anyopaque = null,
    prune_page: ?*const fn (*anyopaque, external.ColumnChunk, @import("lake_parquet_metadata.zig").IndexedPage) bool = null,
    shared_reader: ?*@import("lake_serving_cache.zig").Reader = null,
    dictionary_decodes: usize = 0,
    pages_decoded: usize = 0,
    output: std.heap.ArenaAllocator,
    pub const Filter = struct {
        ptr: *anyopaque,
        any_match: *const fn (*anyopaque, types.ColumnBatch) anyerror!bool,
    };
    const Column = struct {
        required: bool = true,
        directory: ?@import("lake_parquet_metadata.zig").PageDirectory = null,
        index_loaded: bool = false,
        directory_index: usize = 0,
        pruned: bool = false,
        chunk: external.ColumnChunk,
        offset: u64,
        decoded: ?parquet.OwnedBatch = null,
        cached: ?@import("lake_decoded_cache.zig").Lease = null,
        dictionary: ?page.Dictionary = null,
        dictionary_lease: ?@import("lake_decoded_cache.zig").Lease = null,
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
            if (column.cached) |lease| lease.release();
            if (column.directory) |*directory| directory.deinit();
            if (column.dictionary_lease) |lease| lease.release() else if (column.dictionary) |*dictionary| dictionary.deinit(self.a);
        }
        self.a.free(self.columns);
        self.output.deinit();
    }
    fn read(self: *Cursor, offset: u64, len: usize) !ranges.RangeLease {
        return self.reader.readPlannedLease(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = offset, .len = len }, .purpose = .parquet_column_chunk });
    }
    fn loadIndex(self: *Cursor, column: *Column) !void {
        if (column.index_loaded) return;
        column.index_loaded = true;
        const offset = column.chunk.offset_index_offset orelse return;
        const len = column.chunk.offset_index_length.?;
        const budget = self.limits.max_input_bytes / @max(@as(usize, 1), self.columns.len);
        if (len > budget or (column.chunk.column_index_length orelse 0) > budget - len) return error.ParquetPageTooLarge;
        const offsets = try self.reader.readPlannedLease(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = offset, .len = len }, .purpose = .parquet_page_index });
        defer offsets.release();
        const bounds = if (column.chunk.column_index_offset) |start| try self.reader.readPlannedLease(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = start, .len = column.chunk.column_index_length.? }, .purpose = .parquet_page_index }) else null;
        defer if (bounds) |bytes| bytes.release();
        column.directory = try @import("lake_parquet_metadata.zig").parsePageDirectory(self.a, offsets.bytes, if (bounds) |v| v.bytes else null, column.chunk, self.group.row_count, self.limits.max_struct_allocation_bytes / @max(@as(usize, 1), self.columns.len));
    }
    fn advance(self: *Cursor, column: *Column) !void {
        if (column.decoded) |*decoded| decoded.deinit(self.a);
        column.decoded = null;
        if (column.cached) |lease| lease.release();
        column.cached = null;
        column.first += column.count;
        column.count = 0;
        column.consumed = 0;
        column.pruned = false;
        if (self.filter != null) try self.loadIndex(column);
        const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
        while (column.offset < end) {
            if (column.directory) |directory| {
                // Offsets advance monotonically, including skipped pages. Walk each
                // directory entry once instead of searching from its beginning.
                while (column.directory_index < directory.pages.len and directory.pages[column.directory_index].offset < column.offset) column.directory_index += 1;
                const indexed = if (column.directory_index < directory.pages.len and directory.pages[column.directory_index].offset == column.offset) directory.pages[column.directory_index] else null;
                if (indexed) |entry| {
                    if (entry.first != column.first) return error.InvalidParquetMetadata;
                    if (entry.first + entry.rows <= self.position) {
                        column.offset += entry.len;
                        column.first += entry.rows;
                        continue;
                    }
                    if (column.required and self.prune_page != null and !self.prune_page.?(self.prune_ptr.?, column.chunk, entry)) {
                        column.pruned = true;
                        column.offset += entry.len;
                        column.count = entry.rows;
                        column.consumed = @intCast(self.position - column.first);
                        return;
                    }
                }
            }
            const parsed = try self.header(column);
            const len = std.math.add(usize, parsed.header_len, parsed.header.compressed_page_size) catch return error.ParquetPageTooLarge;
            if (len > end - column.offset) return error.InvalidParquetPage;
            // Skip payloads whose row ordinals precede the next survivor.
            // Header validation still fences malformed row counts/ranges.
            if (parsed.header.page_type == .data_page or parsed.header.page_type == .data_page_v2) {
                const count: usize = parsed.header.value_count;
                if (count == 0 or column.first + count > self.group.row_count) return error.ParquetRowGroupRowCountMismatch;
                if (column.first + count <= self.position) {
                    column.offset += len;
                    column.first += count;
                    continue;
                }
            }
            const dictionary_bytes = if (column.dictionary) |dictionary| dictionary.retainedBytes() else 0;
            const share = @max(@as(usize, 1), self.columns.len);
            if (len > self.limits.max_input_bytes / share or parsed.header.uncompressed_page_size +| dictionary_bytes > self.limits.max_decoded_bytes / share) return error.ParquetPageTooLarge;
            var cache_key: ?[32]u8 = null;
            if (self.shared_reader) |reader| if (parsed.header.page_type == .data_page or parsed.header.page_type == .data_page_v2) {
                // Resource policy is checked for each consumer, independently
                // of the immutable physical interpretation of a page.
                const interpretation = try std.json.Stringify.valueAlloc(self.a, .{ .version = "decoded-page-v2", .chunk = column.chunk, .decimal_representation = "exact_string", .preserve_dictionary = true }, .{});
                defer self.a.free(interpretation);
                cache_key = try reader.objectKey(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset, .len = len }, .purpose = .parquet_column_chunk }, interpretation);
                if (reader.cache.decoded.lookup(cache_key.?)) |lease| {
                    errdefer lease.release();
                    var admission = self.limits;
                    admission.max_rows = @max(admission.max_rows, parsed.header.value_count);
                    admission.max_struct_allocation_bytes /= share;
                    _ = try parquet.admitMaterialization(parsed.header.value_count, 1, admission);
                    column.cached = lease;
                    column.offset += len;
                    column.count = parsed.header.value_count;
                    column.consumed = @intCast(self.position - column.first);
                    return;
                }
            };
            const encoded_lease = try self.read(column.offset, len);
            defer encoded_lease.release();
            const encoded = encoded_lease.bytes;
            column.offset += len;
            switch (parsed.header.page_type) {
                .dictionary_page => {
                    if (column.dictionary != null or column.first != 0) return error.InvalidParquetPage;
                    if (self.shared_reader) |reader| {
                        const interpretation = try std.json.Stringify.valueAlloc(self.a, .{ .version = "chunk-dictionary-v1", .chunk = column.chunk }, .{});
                        defer self.a.free(interpretation);
                        const key = try reader.objectKey(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset - len, .len = len }, .purpose = .parquet_column_chunk }, interpretation);
                        const lease = reader.cache.decoded.lookup(key) orelse blk: {
                            const owned = try reader.cache.decoded.create(self.limits.max_decoded_bytes / share *| 4);
                            errdefer owned.release();
                            owned.item.payload = .{ .dictionary = try decodeDictionaryAlloc(owned.item.arena.allocator(), column.chunk, parsed.header, encoded[parsed.header_len..]) };
                            reader.cache.decoded.publish(key, owned);
                            break :blk owned;
                        };
                        errdefer lease.release();
                        if (lease.item.payload.dictionary.retainedBytes() > self.limits.max_decoded_bytes / share) return error.ParquetPageTooLarge;
                        column.dictionary_lease = lease;
                        column.dictionary = lease.item.payload.dictionary;
                    } else {
                        var dictionary = try decodeDictionaryAlloc(self.a, column.chunk, parsed.header, encoded[parsed.header_len..]);
                        errdefer dictionary.deinit(self.a);
                        if (dictionary.retainedBytes() > self.limits.max_decoded_bytes / share) return error.ParquetPageTooLarge;
                        column.dictionary = dictionary;
                    }
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
            limits.preserve_dictionary = true;
            limits.page_encoding = parsed.header.encoding;
            limits.max_rows = @max(limits.max_rows, count);
            const input = [_]parquet.ColumnChunkInput{.{ .column_id = column.chunk.column_id, .bytes = encoded, .dictionary = if (column.dictionary) |*dictionary| dictionary else null }};
            if (cache_key) |key| {
                const cache = &self.shared_reader.?.cache.decoded;
                const lease = try cache.create(self.limits.max_decoded_bytes / share *| 4 +| self.limits.max_struct_allocation_bytes);
                errdefer lease.release();
                if (column.dictionary_lease) |dictionary| {
                    cache.depend(lease, dictionary);
                    limits.borrow_dictionary = true;
                }
                const decoded = try parquet.buildSupportedI64RowGroupBatchAllocWithLimits(lease.item.arena.allocator(), self.inventory, self.file.file_id, self.group.ordinal, &input, limits);
                lease.item.payload = .{ .columns = decoded.columns };
                cache.publish(key, lease);
                column.cached = lease;
            } else {
                column.decoded = try parquet.buildSupportedI64RowGroupBatchAllocWithLimits(self.a, self.inventory, self.file.file_id, self.group.ordinal, &input, limits);
            }
            column.count = count;
            column.consumed = @intCast(self.position - column.first);
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
            const probe_lease = try self.read(column.offset, probe_size);
            defer probe_lease.release();
            const probe = probe_lease.bytes;
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
    fn decodeDictionaryAlloc(a: A, chunk: external.ColumnChunk, header_value: page.Header, encoded: []const u8) !page.Dictionary {
        const payload = try page.decodePagePayloadAlloc(a, header_value, try parquet.compressionCodecForColumnChunk(chunk), encoded);
        defer payload.deinit(a);
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int32")) return .{ .i64 = try page.decodePlainI32DictionaryPageAsI64Alloc(a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int64") or chunk.physical_type.len == 0) return .{ .i64 = try page.decodePlainI64DictionaryPageAlloc(a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "float")) return .{ .f64 = try page.decodePlainF32DictionaryPageAsF64Alloc(a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "double")) return .{ .f64 = try page.decodePlainF64DictionaryPageAlloc(a, header_value, payload.bytes) };
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "fixed_len_byte_array")) {
            if (chunk.type_length <= 0) return error.UnsupportedParquetPage;
            return .{ .bytes = try page.decodePlainFixedLenByteArrayDictionaryPageAlloc(a, header_value, payload.bytes, @intCast(chunk.type_length)) };
        }
        if (std.ascii.eqlIgnoreCase(chunk.physical_type, "byte_array")) return .{ .bytes = try page.decodePlainByteArrayDictionaryPageAlloc(a, header_value, payload.bytes) };
        return error.UnsupportedParquetPage;
    }
    /// Inspect the next headers while this page is being consumed, then warm
    /// the exact versioned ranges used by advance. Parallelism is bounded by
    /// the shared reader's four workers and 32 MiB lookahead quota.
    fn prefetchPages(self: *Cursor) !void {
        const reader = self.shared_reader orelse return;
        if (reader.context.io == null or self.position == self.group.row_count) return;
        try reader.context.ensureActive();
        self.planPrefetchPages(reader) catch {
            // Lookahead must not reject a page already decoded successfully.
            // Required advance() will report its own provider/format/budget
            // errors if this page is actually consumed. Cancellation remains
            // authoritative even when the failing read was speculative.
            try reader.context.ensureActive();
        };
    }
    fn planPrefetchPages(self: *Cursor, reader: *@import("lake_serving_cache.zig").Reader) !void {
        var reads: [4]ranges.RangeRead = undefined;
        var count: usize = 0;
        for (self.columns) |*column| {
            if (self.filter != null and !column.required) continue;
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
    const DecodeAllocator = @import("../../sql/parallel_scheduler.zig").LockedAllocator;
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
    fn advanceColumns(self: *Cursor, required_only: bool) !void {
        // Index arenas outlive this call. Their backing allocator must be the
        // cursor allocator, never the temporary worker locking adapter.
        if (self.filter != null) for (self.columns) |*column| {
            if (!required_only or column.required) try self.loadIndex(column);
        };
        const io = if (self.shared_reader) |reader| reader.context.io else null;
        if (io == null or self.columns.len < 2) {
            for (self.columns) |*column| {
                if (required_only and !column.required) continue;
                if (column.first + column.count <= self.position) try self.advance(column);
                column.consumed = @intCast(self.position - column.first);
            }
            return;
        }
        var allocator: DecodeAllocator = .{ .backing = self.a };
        var pending: [4]?@import("../../sql/parallel_scheduler.zig").Task(anyerror!DecodeStats) = @splat(null);
        // Every worker joins on all error/cancellation paths before the scoped
        // allocator or the cursor metadata can leave scope.
        defer for (&pending) |*future| if (future.*) |*active| {
            _ = active.cancel(io.?) catch {};
            future.* = null;
        };
        var next_column: usize = 0;
        while (next_column < self.columns.len) {
            for (&pending) |*future| {
                while (next_column < self.columns.len and ((required_only and !self.columns[next_column].required) or self.columns[next_column].first + self.columns[next_column].count > self.position)) next_column += 1;
                if (next_column == self.columns.len) break;
                const column = &self.columns[next_column];
                future.* = @import("../../sql/parallel_scheduler.zig").global().submit(io.?, (self.limits.max_input_bytes +| self.limits.max_decoded_bytes) / self.columns.len, decode, .{ self.*, column, allocator.allocator() }) orelse {
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
        for (self.columns) |*column| if (!required_only or column.required) {
            column.consumed = @intCast(self.position - column.first);
        };
    }
    /// Worker-owned lookahead prepares only predicate evidence. SQL residual
    /// evaluation and projection admission stay on the consuming pipeline.
    pub fn prepareEvidence(self: *Cursor) !void {
        try self.advanceColumns(true);
    }
    fn batch(self: *Cursor, count: usize, required_only: bool) !types.ColumnBatch {
        const a = self.output.allocator();
        const refs = try a.alloc(types.RowRef, count);
        const binding = @import("../external_source/rowsource_bridge.zig").bindingFromValidatedInventory(self.inventory);
        for (refs, 0..) |*ref, index| ref.* = try @import("../../storage/rowsource/external.zig").makeRowRef(binding, self.file.file_id, self.group.ordinal, self.position + index);
        var vectors: std.ArrayList(types.ColumnVector) = .empty;
        for (self.columns) |*column| {
            if (required_only and !column.required) continue;
            const decoded = if (column.cached) |lease| lease.item.payload.columns[0] else column.decoded.?.columns[0];
            const begin: usize = @intCast(self.position - column.first);
            var vector = decoded;
            vector.values = switch (decoded.values) {
                .dictionary_bytes => |values| .{ .dictionary_bytes = .{ .values = values.values, .indices = values.indices[begin..][0..count] } },
                inline else => |values, tag| @unionInit(types.ColumnValues, @tagName(tag), values[begin..][0..count]),
            };
            if (decoded.nulls.bytes.len != 0) vector.nulls.bytes = decoded.nulls.bytes[begin..][0..count];
            try vectors.append(a, vector);
        }
        return .{ .snapshot = binding.snapshot(), .row_refs = refs, .columns = vectors.items };
    }
    pub fn next(self: *Cursor) !?types.ColumnBatch {
        while (self.position < self.group.row_count) {
            _ = self.output.reset(.free_all);
            if (self.columns.len == 0) {
                const count: usize = @intCast(@min(@as(u64, 4096), self.group.row_count - self.position));
                const result = try self.batch(count, false);
                self.position += count;
                if (self.filter) |filter| if (!try filter.any_match(filter.ptr, result)) continue;
                return result;
            }
            if (self.filter) |filter| {
                try self.advanceColumns(true);
                var count: usize = 4096;
                for (self.columns) |column| if (column.required) {
                    count = @min(count, column.count - column.consumed);
                };
                if (count == 0 or self.columns.len == 0) return error.InvalidParquetPage;
                const rejected = for (self.columns) |column| {
                    if (column.required and column.pruned) break true;
                } else false;
                if (rejected or !try filter.any_match(filter.ptr, try self.batch(count, true))) {
                    self.position += count;
                    continue;
                }
            }
            try self.advanceColumns(false);
            var count: usize = 4096;
            for (self.columns) |column| count = @min(count, column.count - column.consumed);
            if (count == 0 or self.columns.len == 0) return error.InvalidParquetPage;
            const result = try self.batch(count, false);
            for (self.columns) |*column| column.consumed += count;
            self.position += count;
            try self.prefetchPages();
            return result;
        }
        return null;
    }
};
