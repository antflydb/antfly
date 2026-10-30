// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Align independently paged flat columns without retaining a decoded row group.
const std = @import("std");
const parquet = @import("lake_parquet_rowgroup.zig");
const page = @import("lake_parquet_page.zig");
const external = @import("../external_source/types.zig");
const ranges = @import("lake_range_io.zig");
const types = @import("../../storage/rowsource/types.zig");
const A = std.mem.Allocator;
const Admission = @import("lake_parquet_admission.zig");
pub const Cursor = struct {
    a: A,
    admission: *Admission,
    reader: parquet.ObjectRangeReader,
    inventory: external.Inventory,
    file: external.FileEntry,
    group: external.RowGroup,
    limits: parquet.MaterializationLimits,
    columns: []Column,
    position: u64 = 0,
    selected: []const bool = &.{},
    selected_values_decoded: usize = 0,
    encoded_predicate_pages: usize = 0,
    /// Residual evidence is evaluated before projected payloads are decoded.
    filter: ?Filter = null,
    prune_ptr: ?*anyopaque = null,
    prune_page: ?*const fn (*anyopaque, external.ColumnChunk, @import("lake_parquet_metadata.zig").IndexedPage) bool = null,
    shared_reader: ?*@import("lake_serving_cache.zig").Reader = null,
    scheduler: ?*@import("../../sql/parallel_scheduler.zig").Scheduler = null,
    dictionary_decodes: usize = 0,
    pages_decoded: usize = 0,
    output: std.heap.ArenaAllocator,
    pub const Filter = struct {
        encoded: bool = false,
        ptr: *anyopaque,
        any_match: *const fn (*anyopaque, types.ColumnBatch) anyerror!bool,
        select: ?*const fn (*anyopaque, types.ColumnBatch) anyerror![]const bool = null,
    };
    const Column = struct {
        credit: ?*Admission.Credit = null,
        dictionary_credit: ?*Admission.Credit = null,
        required: bool = true,
        projection: ?*@import("lake_parquet_selected.zig") = null,
        directory: ?@import("lake_parquet_metadata.zig").PageDirectory = null,
        directory_lease: ?@import("lake_decoded_cache.zig").Lease = null,
        index_loaded: bool = false,
        directory_index: usize = 0,
        pruned: bool = false,
        chunk: external.ColumnChunk,
        offset: u64,
        decoded: ?*SharedColumns = null,
        dictionary_owner: ?*SharedDictionary = null,
        cached: ?@import("lake_decoded_cache.zig").Lease = null,
        dictionary: ?page.Dictionary = null,
        dictionary_lease: ?@import("lake_decoded_cache.zig").Lease = null,
        first: u64 = 0,
        count: usize = 0,
        consumed: usize = 0,
    };
    fn Shared(comptime T: type) type {
        return struct {
            a: A,
            budget: @import("../../sql/memory_budget.zig"),
            refs: std.atomic.Value(usize) = .init(1),
            value: T = undefined,
            dictionary: ?*SharedDictionary = null,
            credit: *Admission.Credit,
            fn create(a: A, credit: *Admission.Credit) !*@This() {
                const owner = try a.create(@This());
                owner.* = .{ .a = a, .credit = credit.retain(), .budget = .{ .backing = credit.owner.allocator(), .limit = std.math.maxInt(usize) } };
                return owner;
            }
            fn retain(self: *@This()) *@This() {
                _ = self.refs.fetchAdd(1, .monotonic);
                return self;
            }
            fn release(self: *@This()) void {
                if (self.refs.fetchSub(1, .acq_rel) != 1) return;
                self.discard(true);
            }
            fn discard(self: *@This(), initialized: bool) void {
                const a = self.a;
                if (initialized) self.value.deinit(self.budget.allocator());
                std.debug.assert(self.budget.live == 0);
                if (self.dictionary) |dictionary| dictionary.release();
                const credit = self.credit;
                a.destroy(self);
                credit.release();
            }
            fn bytes(self: *const @This()) usize {
                return @sizeOf(@This()) +| self.budget.live +| if (self.dictionary) |dictionary| @sizeOf(SharedDictionary) +| dictionary.budget.live else 0;
            }
            fn stabilize(self: *@This(), a: A) void {
                self.a = a;
                // The heap-owned admission allocator already has stable lifetime.
            }
        };
    }
    const SharedDictionary = Shared(page.Dictionary);
    const SharedColumns = Shared(parquet.OwnedColumns);
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
        return .{ .a = a, .admission = try Admission.create(a, limits.max_decoded_bytes, limits.max_struct_allocation_bytes, limits.max_input_bytes), .reader = reader, .inventory = inventory, .file = file, .group = group, .columns = columns, .limits = limits, .output = std.heap.ArenaAllocator.init(a) };
    }
    pub fn deinit(self: *Cursor) void {
        for (self.columns) |*column| {
            if (column.projection) |projection| projection.destroyWithAllocator(self.admission.allocator());
            if (column.decoded) |decoded| decoded.release();
            if (column.cached) |lease| lease.release();
            if (column.directory_lease) |lease| lease.release() else if (column.directory) |*directory| directory.deinit();
            if (column.dictionary_lease) |lease| lease.release() else if (column.dictionary_owner) |owner| owner.release();
            if (column.credit) |credit| credit.release();
            if (column.dictionary_credit) |credit| credit.release();
        }
        self.admission.release();
        self.a.free(self.columns);
        self.output.deinit();
    }
    pub fn retainColumns(self: *Cursor, a: A) !?types.ColumnOwner {
        for (self.columns) |column| if (column.projection != null or (column.cached == null and column.decoded == null)) return null;
        const Held = union(enum) {
            cached: struct { lease: @import("lake_decoded_cache.zig").Lease, credit: *Admission.Credit },
            local: *SharedColumns,
            fn release(held: @This()) void {
                switch (held) {
                    .cached => |cached| {
                        cached.lease.release();
                        cached.credit.release();
                    },
                    .local => |owner| owner.release(),
                }
            }
        };
        const Owner = struct {
            a: A,
            held: []Held,
            fn release(raw: *anyopaque) void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                for (self_.held) |held| held.release();
                self_.a.free(self_.held);
                self_.a.destroy(self_);
            }
        };
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .held = try a.alloc(Held, self.columns.len) };
        var bytes: usize = 0;
        for (self.columns, owner.held) |column, *held| {
            if (column.cached) |cached| {
                held.* = .{ .cached = .{ .lease = cached.retain(), .credit = column.credit.?.retain() } };
                bytes +|= cached.item.budget.live;
                if (cached.item.dependency) |dependency| bytes +|= dependency.budget.live;
            } else {
                const local = column.decoded.?;
                held.* = .{ .local = local.retain() };
                bytes +|= local.bytes();
            }
        }
        return .{ .ptr = owner, .release_fn = Owner.release, .retained_bytes = bytes };
    }
    fn read(self: *Cursor, offset: u64, len: usize) !ranges.RangeLease {
        const context: @import("lake_read_context.zig").Context = if (self.shared_reader) |reader| reader.context else .{};
        try self.admission.reserveInput(len, context);
        errdefer self.admission.releaseInput(len, context);
        const lease = try self.reader.readPlannedLease(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = offset, .len = len }, .purpose = .parquet_column_chunk });
        errdefer lease.release();
        return self.admission.wrapInput(lease, len, context);
    }
    fn loadIndex(self: *Cursor, column: *Column) !void {
        if (column.index_loaded) return;
        column.index_loaded = true;
        const offset = column.chunk.offset_index_offset orelse return;
        const len = column.chunk.offset_index_length.?;
        const budget = self.limits.max_input_bytes / @max(@as(usize, 1), self.columns.len);
        // Navigation is optional. A large directory must not prevent a
        // streaming scan whose individual data pages fit the reader budget.
        if (len > budget) return;
        const bounds = self.filter != null and column.required and self.prune_page != null and
            (column.chunk.column_index_length orelse 0) <= budget - len;
        if (self.shared_reader) |shared| {
            const interpretation = try std.json.Stringify.valueAlloc(self.a, .{ .version = "page-directory-v2", .bounds = bounds, .chunk = column.chunk, .rows = self.group.row_count, .allocation_limit = self.limits.max_struct_allocation_bytes / @max(@as(usize, 1), self.columns.len) }, .{});
            defer self.a.free(interpretation);
            const key = try shared.objectKey(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = offset, .len = len }, .purpose = .parquet_page_index }, interpretation);
            var loader = struct {
                cursor: *Cursor,
                column: *Column,
                bounds: bool,
                fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
                    const value: *@This() = @ptrCast(@alignCast(raw));
                    const decoded = value.cursor.readIndex(value.column, item.arena.allocator(), value.bounds) catch |err| {
                        if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.DecodedArtifactTooLarge;
                        return err;
                    };
                    item.payload = .{ .page_directory = decoded };
                }
            }{ .cursor = self, .column = column, .bounds = bounds };
            const limit = @min(shared.cache.decoded.max_loading_bytes, self.limits.max_struct_allocation_bytes);
            const lease = shared.cache.decoded.acquire(key, limit, shared.context, .{ .ptr = &loader, .load = @TypeOf(loader).load }) catch |err| switch (err) {
                error.DecodedArtifactTooLarge, error.SqlMemoryLimitExceeded => return,
                else => return err,
            };
            column.directory_lease = lease;
            column.directory = lease.item.payload.page_directory;
        } else column.directory = self.readIndex(column, self.a, bounds) catch |err| switch (err) {
            error.DecodedArtifactTooLarge => return,
            else => return err,
        };
    }
    fn readIndex(self: *Cursor, column: *Column, a: A, include_bounds: bool) !@import("lake_parquet_metadata.zig").PageDirectory {
        const offset = column.chunk.offset_index_offset.?;
        const len = column.chunk.offset_index_length.?;
        const offsets = try self.reader.readPlannedLease(a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = offset, .len = len }, .purpose = .parquet_page_index });
        defer offsets.release();
        const bounds = if (if (include_bounds) column.chunk.column_index_offset else null) |start| try self.reader.readPlannedLease(a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = start, .len = column.chunk.column_index_length.? }, .purpose = .parquet_page_index }) else null;
        defer if (bounds) |bytes| bytes.release();
        return try @import("lake_parquet_metadata.zig").parsePageDirectory(a, offsets.bytes, if (bounds) |v| v.bytes else null, column.chunk, self.group.row_count, self.limits.max_struct_allocation_bytes / @max(@as(usize, 1), self.columns.len));
    }
    fn retire(self: *Cursor, column: *Column) void {
        if (column.projection) |projection| projection.destroyWithAllocator(self.admission.allocator());
        column.projection = null;
        if (column.decoded) |decoded| decoded.release();
        column.decoded = null;
        if (column.cached) |lease| lease.release();
        column.cached = null;
        if (column.credit) |credit| credit.release();
        column.credit = null;
        column.first += column.count;
        column.count = 0;
        column.consumed = 0;
        column.pruned = false;
    }
    fn cachedCredit(self: *Cursor, column: *const Column, lease: @import("lake_decoded_cache.zig").Lease, raw: usize) !*Admission.Credit {
        const credit = try self.admission.claim(raw, column.dictionary_credit);
        errdefer credit.release();
        try self.chargeCached(column, lease, credit, raw);
        return credit;
    }
    fn chargeCached(_: *Cursor, column: *const Column, lease: @import("lake_decoded_cache.zig").Lease, credit: *Admission.Credit, raw: usize) !void {
        var bytes = lease.item.budget.live;
        if (lease.item.dependency) |dictionary| {
            const covered = if (column.dictionary_lease) |held| held.item == dictionary else false;
            if (!covered) {
                bytes +|= dictionary.budget.live;
                try credit.growRaw(raw +| dictionary.payload.dictionary.retainedBytes());
            }
        }
        try credit.cached(bytes);
    }
    const Fetched = struct {
        parsed: page.ParsedHeader,
        len: usize,
        end: u64,
        lease: ranges.RangeLease,
        cache_key: ?[32]u8,
    };
    fn advance(self: *Cursor, column: *Column) !void {
        self.retire(column);
        try self.loadIndex(column);
        while (try self.fetch(column)) |fetched| {
            defer fetched.lease.release();
            if (try self.decodeFetched(column, fetched)) return;
        }
    }
    fn fetch(self: *Cursor, column: *Column) !?Fetched {
        const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
        while (column.offset < end) {
            if (column.directory) |directory| {
                if (column.offset >= directory.pages[0].offset and self.position > column.first) {
                    var lo: usize = column.directory_index;
                    var hi = directory.pages.len;
                    while (lo < hi) {
                        const mid = lo + (hi - lo) / 2;
                        if (directory.pages[mid].first <= self.position) lo = mid + 1 else hi = mid;
                    }
                    if (lo != 0 and directory.pages[lo - 1].offset > column.offset) {
                        column.directory_index = lo - 1;
                        column.offset = directory.pages[lo - 1].offset;
                        column.first = directory.pages[lo - 1].first;
                    }
                }
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
                        return null;
                    }
                }
            }
            if (try self.cachedIndexedPage(column)) |lease| {
                errdefer lease.release();
                column.credit = try self.cachedCredit(column, lease, lease.item.parquet_page_header.?.header.uncompressed_page_size);
                column.cached = lease;
                const parsed = lease.item.parquet_page_header.?;
                column.offset += parsed.header_len + parsed.header.compressed_page_size;
                column.count = parsed.header.value_count;
                column.consumed = @intCast(self.position - column.first);
                return null;
            }
            const probe = try self.headerRead(column);
            var probe_held = true;
            defer if (probe_held) probe.lease.release();
            const parsed = probe.parsed;
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
            if (len > self.limits.max_input_bytes or parsed.header.uncompressed_page_size > self.limits.max_decoded_bytes) return error.ParquetPageTooLarge;
            var cache_key: ?[32]u8 = null;
            if (self.shared_reader) |reader| if (parsed.header.page_type == .data_page or parsed.header.page_type == .data_page_v2) {
                // Resource policy is checked for each consumer, independently
                // of the immutable physical interpretation of a page.
                const interpretation = try std.json.Stringify.valueAlloc(self.a, .{ .version = "decoded-page-v5", .chunk = column.chunk, .decimal_representation = "native_decimal", .preserve_dictionary = true }, .{});
                defer self.a.free(interpretation);
                cache_key = try reader.objectKey(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset, .len = len }, .purpose = .parquet_column_chunk }, interpretation);
                if (reader.cache.decoded.lookup(cache_key.?)) |lease| {
                    errdefer lease.release();
                    var admission = self.limits;
                    admission.max_rows = @max(admission.max_rows, parsed.header.value_count);
                    _ = try parquet.admitColumnMaterialization(parsed.header.value_count, 1, admission);
                    column.credit = try self.cachedCredit(column, lease, parsed.header.uncompressed_page_size);
                    column.cached = lease;
                    column.offset += len;
                    column.count = parsed.header.value_count;
                    column.consumed = @intCast(self.position - column.first);
                    return null;
                }
            };
            // Small pages are often already complete in the header probe.
            // Borrow it instead of issuing a second overlapping provider GET
            // (exact range cache keys deliberately do not alias subranges).
            const reuse_probe = len <= probe.lease.bytes.len;
            if (!reuse_probe) {
                // Do not raise peak range residency for larger pages.
                probe.lease.release();
                probe_held = false;
            }
            const encoded_lease = if (reuse_probe) null else try self.read(column.offset, len);
            if (reuse_probe) probe_held = false;
            return .{ .parsed = parsed, .len = len, .end = end, .lease = encoded_lease orelse probe.lease, .cache_key = cache_key };
        }
        return error.ParquetRowGroupRowCountMismatch;
    }

    fn decodeFetched(self: *Cursor, column: *Column, fetched: Fetched) !bool {
        const parsed = fetched.parsed;
        const len = fetched.len;
        const end = fetched.end;
        const encoded = fetched.lease.bytes[0..len];
        const cache_key = fetched.cache_key;
        const prior_offset = column.offset;
        column.offset += len;
        errdefer column.offset = prior_offset;
        const credit = try self.admission.claim(parsed.header.uncompressed_page_size, if (parsed.header.page_type == .dictionary_page) null else column.dictionary_credit);
        defer credit.release();
        switch (parsed.header.page_type) {
            .dictionary_page => {
                if (column.dictionary != null or column.first != 0) return error.InvalidParquetPage;
                if (self.shared_reader) |reader| {
                    const interpretation = try std.json.Stringify.valueAlloc(self.a, .{ .version = "chunk-dictionary-v1", .chunk = column.chunk }, .{});
                    defer self.a.free(interpretation);
                    const key = try reader.objectKey(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset - len, .len = len }, .purpose = .parquet_column_chunk }, interpretation);
                    var loader = struct {
                        admission: *Admission,
                        chunk: external.ColumnChunk,
                        header: @TypeOf(parsed.header),
                        bytes: []const u8,
                        fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
                            const value: *@This() = @ptrCast(@alignCast(raw));
                            var loading: Admission.Loading = undefined;
                            try loading.begin(value.admission, item);
                            defer loading.end();
                            const decoded = decodeDictionaryAlloc(item.budget.allocator(), value.chunk, value.header, value.bytes) catch |err| {
                                if (loading.failed) return error.ParquetPageTooLarge;
                                return err;
                            };
                            item.payload = .{ .dictionary = decoded };
                            item.owned_dictionary = true;
                        }
                    }{ .admission = self.admission, .chunk = column.chunk, .header = parsed.header, .bytes = encoded[parsed.header_len..] };
                    const lease = try reader.cache.decoded.acquire(key, @min(reader.cache.decoded.max_loading_bytes, self.limits.max_decoded_bytes *| 4), reader.context, .{ .ptr = &loader, .load = @TypeOf(loader).load });
                    errdefer lease.release();
                    try credit.growRaw(lease.item.payload.dictionary.retainedBytes());
                    try credit.cached(lease.item.budget.live);
                    column.dictionary_credit = credit.retain();
                    column.dictionary_lease = lease;
                    column.dictionary = lease.item.payload.dictionary;
                } else {
                    const owner = try SharedDictionary.create(self.a, credit);
                    var initialized = false;
                    errdefer owner.discard(initialized);
                    owner.value = try decodeDictionaryAlloc(owner.budget.allocator(), column.chunk, parsed.header, encoded[parsed.header_len..]);
                    initialized = true;
                    try credit.growRaw(owner.value.retainedBytes());
                    column.dictionary_credit = credit.retain();
                    column.dictionary_owner = owner;
                    column.dictionary = owner.value;
                }
                self.dictionary_decodes += 1;
                return false;
            },
            .data_page, .data_page_v2 => {},
            else => return error.UnsupportedParquetPage,
        }
        const count: usize = parsed.header.value_count;
        if (count == 0 or column.first + count > self.group.row_count) return error.ParquetRowGroupRowCountMismatch;
        // A broad cohort benefits from a reusable complete decode. Choose
        // sparse projection from this page's actual mask intersection,
        // rather than a differently aligned predicate page's density.
        const start: usize = @intCast(self.position - column.first);
        const considered = @min(self.selected.len, count - start);
        var survivors: usize = 0;
        for (self.selected[0..considered]) |active| survivors += @intFromBool(active);
        const sparse = considered != 0 and survivors <= considered / 2;
        if ((!column.required and sparse) or (column.required and self.filter != null and self.filter.?.encoded and self.filter.?.select != null and !std.mem.eql(u8, column.chunk.logical_type, "decimal"))) {
            if (try @import("lake_parquet_selected.zig").create(self.admission.allocator(), column.chunk, parsed.header, encoded[parsed.header_len..], column.dictionary, self.limits.max_decoded_bytes *| 2 +| self.limits.max_struct_allocation_bytes)) |projection| {
                column.projection = projection;
                column.credit = credit.retain();
                self.encoded_predicate_pages += @intFromBool(column.required);
                column.count = count;
                column.consumed = @intCast(self.position - column.first);
                self.pages_decoded += 1;
                if (column.first + count == self.group.row_count and column.offset != end) return error.ParquetRowGroupRowCountMismatch;
                return true;
            }
        }
        var limits = self.limits;
        limits.decimal_representation = .native_decimal;
        limits.page_row_count = count;
        limits.preserve_dictionary = true;
        limits.page_encoding = parsed.header.encoding;
        limits.max_rows = @max(limits.max_rows, count);
        const input = [_]parquet.ColumnChunkInput{.{ .column_id = column.chunk.column_id, .bytes = encoded, .dictionary = if (column.dictionary) |*dictionary| dictionary else null }};
        if (cache_key) |key| {
            const cache = &self.shared_reader.?.cache.decoded;
            if (column.dictionary_lease != null) limits.borrow_dictionary = true;
            var loader = struct {
                cursor: *Cursor,
                input: []const parquet.ColumnChunkInput,
                limits: parquet.MaterializationLimits,
                dictionary: ?@import("lake_decoded_cache.zig").Lease,
                parsed: page.ParsedHeader,
                fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
                    const value: *@This() = @ptrCast(@alignCast(raw));
                    const cursor = value.cursor;
                    var loading: Admission.Loading = undefined;
                    try loading.begin(cursor.admission, item);
                    defer loading.end();
                    const decoded = parquet.buildSupportedColumnsAllocWithLimits(item.budget.allocator(), cursor.inventory, cursor.file.file_id, cursor.group.ordinal, value.input, value.limits) catch |err| {
                        if (loading.failed) return error.ParquetPageTooLarge;
                        return err;
                    };
                    item.payload = .{ .columns = decoded.columns };
                    item.owned_columns = decoded;
                    item.parquet_page_header = value.parsed;
                    if (value.dictionary) |dictionary| dictionary.cache.depend(.{ .cache = dictionary.cache, .item = item }, dictionary);
                }
            }{ .cursor = self, .input = &input, .limits = limits, .dictionary = column.dictionary_lease, .parsed = parsed };
            const lease = try cache.acquire(key, @min(cache.max_loading_bytes, self.limits.max_decoded_bytes *| 4 +| self.limits.max_struct_allocation_bytes), self.shared_reader.?.context, .{ .ptr = &loader, .load = @TypeOf(loader).load });
            errdefer lease.release();
            try self.chargeCached(column, lease, credit, parsed.header.uncompressed_page_size);
            column.cached = lease;
        } else {
            // Uncached page leases retain their dictionary owner across
            // cursor advancement and closure without copying dictionary values.
            limits.borrow_dictionary = true;
            const owner = try SharedColumns.create(self.a, credit);
            var initialized = false;
            errdefer owner.discard(initialized);
            owner.value = try parquet.buildSupportedColumnsAllocWithLimits(owner.budget.allocator(), self.inventory, self.file.file_id, self.group.ordinal, &input, limits);
            initialized = true;
            if (column.dictionary_owner) |dictionary| owner.dictionary = dictionary.retain();
            column.decoded = owner;
        }
        column.credit = credit.retain();
        column.count = count;
        column.consumed = @intCast(self.position - column.first);
        self.pages_decoded += 1;
        if (column.first + count == self.group.row_count and column.offset != end) return error.ParquetRowGroupRowCountMismatch;
        return true;
    }
    fn header(self: *Cursor, column: *const Column) !page.ParsedHeader {
        if (try self.cachedIndexedPage(column)) |lease| {
            defer lease.release();
            return lease.item.parquet_page_header.?;
        }
        const probe = try self.headerRead(column);
        defer probe.lease.release();
        return probe.parsed;
    }
    const HeaderRead = struct { parsed: page.ParsedHeader, lease: ranges.RangeLease };
    fn indexedPage(self: *const Cursor, column: *const Column) ?@import("lake_parquet_metadata.zig").IndexedPage {
        _ = self;
        const directory = column.directory orelse return null;
        var index = column.directory_index;
        while (index < directory.pages.len and directory.pages[index].offset < column.offset) index += 1;
        return if (index < directory.pages.len and directory.pages[index].offset == column.offset) directory.pages[index] else null;
    }
    /// Only a validated exact-range cache entry may bypass encoded I/O.
    /// Reapply all request-specific admission and directory checks on each hit.
    fn cachedIndexedPage(self: *Cursor, column: *const Column) !?@import("lake_decoded_cache.zig").Lease {
        const reader = self.shared_reader orelse return null;
        try reader.context.ensureActive();
        const entry = self.indexedPage(column) orelse return null;
        const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
        if (column.offset >= end or entry.len > end - column.offset) return error.InvalidParquetMetadata;
        const interpretation = try std.json.Stringify.valueAlloc(self.a, .{ .version = "decoded-page-v5", .chunk = column.chunk, .decimal_representation = "native_decimal", .preserve_dictionary = true }, .{});
        defer self.a.free(interpretation);
        const key = try reader.objectKey(self.a, .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset, .len = entry.len }, .purpose = .parquet_column_chunk }, interpretation);
        const lease = reader.cache.decoded.lookup(key) orelse return null;
        errdefer lease.release();
        const parsed = lease.item.parquet_page_header orelse {
            lease.release();
            return null;
        };
        try parsed.header.validateResourceLimits();
        if ((parsed.header.page_type != .data_page and parsed.header.page_type != .data_page_v2) or
            parsed.header_len + parsed.header.compressed_page_size != entry.len or parsed.header.value_count != entry.rows) return error.InvalidParquetMetadata;
        const count = parsed.header.value_count;
        const row_end = std.math.add(u64, entry.first, count) catch return error.ParquetRowGroupRowCountMismatch;
        if (count == 0 or row_end > self.group.row_count or
            (row_end == self.group.row_count and column.offset + entry.len != end)) return error.ParquetRowGroupRowCountMismatch;
        if (entry.len > self.limits.max_input_bytes or parsed.header.uncompressed_page_size > self.limits.max_decoded_bytes) return error.ParquetPageTooLarge;
        var admission = self.limits;
        admission.max_rows = @max(admission.max_rows, count);
        _ = try parquet.admitColumnMaterialization(count, 1, admission);
        return lease;
    }
    fn headerRead(self: *Cursor, column: *const Column) !HeaderRead {
        if (self.limits.max_input_bytes == 0) return error.ParquetPageTooLarge;
        const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
        if (column.offset >= end) return error.InvalidParquetPage;
        if (self.indexedPage(column)) |entry| {
            // OffsetIndex compressed_page_size includes the header. Validate
            // admission before the GET and validate the encoded header against
            // the directory before trusting either lengths or row ordinals.
            if (entry.len > end - column.offset) return error.InvalidParquetMetadata;
            if (entry.len > self.limits.max_input_bytes) return error.ParquetPageTooLarge;
            const lease = try self.read(column.offset, entry.len);
            errdefer lease.release();
            const parsed = try page.parsePageHeader(lease.bytes);
            try parsed.header.validateResourceLimits();
            if (parsed.header.page_type != .data_page and parsed.header.page_type != .data_page_v2) return error.InvalidParquetMetadata;
            const len = std.math.add(usize, parsed.header_len, parsed.header.compressed_page_size) catch return error.ParquetPageTooLarge;
            if (len != entry.len or parsed.header.value_count != entry.rows) return error.InvalidParquetMetadata;
            return .{ .parsed = parsed, .lease = lease };
        }
        var probe_size: usize = @intCast(@min(end - column.offset, @min(self.limits.max_input_bytes, 512)));
        while (true) {
            const probe_lease = try self.read(column.offset, probe_size);
            errdefer probe_lease.release();
            const probe = probe_lease.bytes;
            const parsed = page.parsePageHeader(probe) catch |err| {
                const next_size = @min(end - column.offset, @min(probe_size * 2, @min(self.limits.max_input_bytes, 64 * 1024)));
                if (next_size == probe_size) return err;
                probe_lease.release();
                probe_size = @intCast(next_size);
                continue;
            };
            try parsed.header.validateResourceLimits();
            return .{ .parsed = parsed, .lease = probe_lease };
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
    /// the shared reader's eight jobs and 32 MiB lookahead quota.
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
        var reads: [8]ranges.RangeRead = undefined;
        var count: usize = 0;
        var planned: usize = 0;
        for (self.columns) |*column| {
            if (self.filter != null and !column.required) continue;
            if (planned == 4) break;
            if (column.consumed != column.count) continue;
            const end = std.math.add(u64, column.chunk.file_offset, column.chunk.compressed_len) catch return error.InvalidParquetPage;
            if (column.offset >= end) continue;
            if (try self.cachedIndexedPage(column)) |lease| {
                lease.release();
                continue;
            }
            planned += 1;
            const limit = self.limits.max_input_bytes;
            if (self.indexedPage(column)) |entry| {
                if (entry.len > limit or entry.len > end - column.offset) continue;
                if (column.required and self.prune_page != null and !self.prune_page.?(self.prune_ptr.?, column.chunk, entry)) continue;
                reads[count] = .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset, .len = entry.len }, .purpose = .parquet_column_chunk };
                count += 1;
                // One adjacent page of lookahead per projected column. Filter
                // consumers defer future payload until its row window survives.
                if (self.filter == null and self.prune_page == null and count < reads.len) {
                    var following_column = column.*;
                    following_column.offset += entry.len;
                    if (self.indexedPage(&following_column)) |following| {
                        if (following.len <= limit and following.len <= end - following_column.offset) {
                            if (try self.cachedIndexedPage(&following_column)) |lease| {
                                lease.release();
                            } else {
                                reads[count] = .{ .object = reads[count - 1].object, .range = .{ .offset = following_column.offset, .len = following.len }, .purpose = .parquet_column_chunk };
                                count += 1;
                            }
                        }
                    }
                }
            } else {
                const probe: ranges.RangeRead = .{ .object = try ranges.objectRefForExternalFileUri(self.file), .range = .{ .offset = column.offset, .len = @min(end - column.offset, 512) }, .purpose = .parquet_column_chunk };
                try reader.prefetchPage(probe, end, limit);
            }
        }
        try reader.prefetch(reads[0..count]);
    }
    const DecodeStats = struct { pages: usize, dictionaries: usize, encoded_predicates: usize };
    const DecodedStep = struct { stats: DecodeStats, ready: bool };
    fn fetchWorker(template: Cursor, column: *Column) anyerror!?Fetched {
        var worker = template;
        worker.a = worker.admission.backing.allocator();
        return worker.fetch(column);
    }
    fn decodeWorker(template: Cursor, column: *Column, fetched: Fetched) anyerror!DecodedStep {
        var worker = template;
        worker.a = worker.admission.backing.allocator();
        worker.pages_decoded = 0;
        worker.dictionary_decodes = 0;
        worker.encoded_predicate_pages = 0;
        const ready = worker.decodeFetched(column, fetched) catch |err| {
            if (err == error.OutOfMemory and worker.admission.memory.isLocallyExhausted()) return error.ParquetPageTooLarge;
            return err;
        };
        return .{ .ready = ready, .stats = .{ .pages = worker.pages_decoded, .dictionaries = worker.dictionary_decodes, .encoded_predicates = worker.encoded_predicate_pages } };
    }
    fn addStats(self: *Cursor, stats: DecodeStats) void {
        self.pages_decoded += stats.pages;
        self.dictionary_decodes += stats.dictionaries;
        self.encoded_predicate_pages += stats.encoded_predicates;
    }
    fn advanceColumns(self: *Cursor, required_only: bool) !void {
        // Outer metadata owners return to the coordinator allocator after joining;
        // retained payloads keep the stable heap-owned admission allocator.
        defer for (self.columns) |column| {
            if (column.projection) |projection| projection.stabilize(self.admission.allocator());
            if (column.decoded) |owner| owner.stabilize(self.a);
            if (column.dictionary_owner) |owner| owner.stabilize(self.a);
        };
        // Retire the whole advancing cohort before admitting replacement pages.
        // Otherwise a pending worker's expired page can reject a healthy sibling.
        for (self.columns) |*column| {
            if ((!required_only or column.required) and column.first + column.count <= self.position) self.retire(column);
        }
        // Index arenas outlive this call. Their backing allocator must be the
        // cursor allocator, never the temporary worker locking adapter.
        for (self.columns) |*column| {
            if (!required_only or column.required) try self.loadIndex(column);
        }
        const io = if (self.shared_reader) |reader| reader.context.io else null;
        if (io == null or self.columns.len < 2) {
            for (self.columns) |*column| {
                if (required_only and !column.required) continue;
                if (column.first + column.count <= self.position) self.advance(column) catch |err| {
                    if (err == error.OutOfMemory and self.admission.memory.isLocallyExhausted()) return error.ParquetPageTooLarge;
                    return err;
                };
                column.consumed = @intCast(self.position - column.first);
            }
            return;
        }
        const scheduler = @import("../../sql/parallel_scheduler.zig");
        const Pending = union(enum) {
            fetch: scheduler.Task(anyerror!?Fetched),
            decode: struct { task: scheduler.Task(anyerror!DecodedStep), input: Fetched },
            retry: Fetched,
        };
        var pending: [4]?Pending = @splat(null);
        var columns: [4]?*Column = @splat(null);
        var changed: std.Io.Event = .unset;
        // Cancel and join before releasing any request metadata. A finished
        // but unjoined fetch still owns its input credit and must be drained.
        defer for (&pending) |*slot| if (slot.*) |*task| {
            switch (task.*) {
                .fetch => |*active| if (active.cancel(io.?)) |result| {
                    if (result) |fetched| fetched.lease.release();
                } else |_| {},
                .decode => |*active| {
                    _ = active.task.cancel(io.?) catch {};
                    active.input.lease.release();
                },
                .retry => |fetched| fetched.lease.release(),
            }
            slot.* = null;
        };
        var next_column: usize = 0;
        while (true) {
            changed.reset();
            var active_count: usize = 0;
            var progressed = false;
            for (&pending, &columns) |*slot, *column_slot| {
                if (slot.*) |*task| switch (task.*) {
                    .fetch => |*active| if (active.isComplete()) {
                        const outcome = active.await(io.?);
                        slot.* = null;
                        progressed = true;
                        if (try outcome) |fetched| {
                            // CPU admission begins only after the complete
                            // encoded lease is ready; input residency remains
                            // bounded by the independent live request owner.
                            const workspace = (self.limits.max_input_bytes +| self.limits.max_decoded_bytes) / @max(@as(usize, 1), self.columns.len);
                            if ((self.scheduler orelse scheduler.global()).submitNotified(io.?, workspace, &changed, decodeWorker, .{ self.*, column_slot.*.?, fetched })) |decode_task| {
                                slot.* = .{ .decode = .{ .task = decode_task, .input = fetched } };
                            } else {
                                const inline_outcome = decodeWorker(self.*, column_slot.*.?, fetched);
                                if (inline_outcome) |step| {
                                    fetched.lease.release();
                                    self.addStats(step.stats);
                                    if (step.ready) column_slot.* = null;
                                } else |err| {
                                    if (err == error.ParquetPageTooLarge) {
                                        slot.* = .{ .retry = fetched };
                                    } else {
                                        fetched.lease.release();
                                        return err;
                                    }
                                }
                            }
                        } else column_slot.* = null;
                    },
                    .decode => |*active| if (active.task.isComplete()) {
                        const outcome = active.task.await(io.?);
                        const fetched = active.input;
                        slot.* = null;
                        progressed = true;
                        if (outcome) |step| {
                            fetched.lease.release();
                            self.addStats(step.stats);
                            if (step.ready) column_slot.* = null;
                        } else |err| {
                            if (err == error.ParquetPageTooLarge) {
                                slot.* = .{ .retry = fetched };
                            } else {
                                fetched.lease.release();
                                return err;
                            }
                        }
                    },
                    .retry => {},
                };
                if (slot.* != null) {
                    active_count += 1;
                    continue;
                }
                while (column_slot.* == null and next_column < self.columns.len) {
                    const column = &self.columns[next_column];
                    next_column += 1;
                    if ((required_only and !column.required) or column.first + column.count > self.position) continue;
                    column_slot.* = column;
                }
                if (column_slot.*) |column| {
                    const workspace = self.limits.max_input_bytes / @max(@as(usize, 1), self.columns.len);
                    if ((self.scheduler orelse scheduler.global()).submitIoNotified(io.?, workspace, &changed, fetchWorker, .{ self.*, column })) |fetch_task| {
                        slot.* = .{ .fetch = fetch_task };
                        active_count += 1;
                        progressed = true;
                    }
                }
            }
            // Temporary decode work can exceed headroom even when the final
            // retained cohort fits. Drain other decoders before one bounded
            // retry with identical raw/resident policy. Never increase a quota
            // or discard the encoded input needed to retry without another GET.
            var decoding = false;
            for (pending) |task| if (task != null and task.? == .decode) {
                decoding = true;
                break;
            };
            if (!decoding) for (&pending, &columns) |*slot, *column_slot| {
                if (slot.* == null or slot.*.? != .retry) continue;
                const fetched = slot.*.?.retry;
                slot.* = null;
                defer fetched.lease.release();
                const step = try decodeWorker(self.*, column_slot.*.?, fetched);
                self.addStats(step.stats);
                if (step.ready) column_slot.* = null;
                progressed = true;
                break;
            };
            if (active_count == 0) {
                var waiting: ?*?*Column = null;
                for (&columns) |*column| if (column.* != null) {
                    waiting = column;
                    break;
                };
                if (waiting) |column_slot| {
                    // No live phase can own input here. A saturated global I/O
                    // lane uses one caller lane without parking behind our own
                    // completed encoded pages.
                    self.advance(column_slot.*.?) catch |err| {
                        if (err == error.OutOfMemory and self.admission.memory.isLocallyExhausted()) return error.ParquetPageTooLarge;
                        return err;
                    };
                    column_slot.* = null;
                    continue;
                }
                break;
            }
            if (!progressed) try changed.wait(io.?);
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
        const binding = @import("../external_source/rowsource_bridge.zig").bindingFromValidatedInventory(self.inventory);
        const first = try @import("../../storage/rowsource/external.zig").makeRowRef(binding, self.file.file_id, self.group.ordinal, self.position);
        var vectors: std.ArrayList(types.ColumnVector) = .empty;
        for (self.columns) |*column| {
            if (required_only and !column.required) continue;
            if (column.projection) |projection| {
                const begin: usize = @intCast(self.position - column.first);
                if (required_only) {
                    const evidence = (try projection.evidence(a, begin, count)) orelse return error.InvalidParquetPage;
                    try vectors.append(a, evidence);
                    continue;
                }
                if (count > self.selected.len) return error.InvalidParquetPage;
                const previous = projection.converted;
                try vectors.append(a, try projection.project(a, begin, self.selected[0..count]));
                self.selected_values_decoded += projection.converted - previous;
                continue;
            }
            const decoded = if (column.cached) |lease| lease.item.payload.columns[0] else column.decoded.?.value.columns[0];
            const begin: usize = @intCast(self.position - column.first);
            var vector = decoded;
            vector.values = switch (decoded.values) {
                .encoded => return error.InvalidParquetPage,
                .decimal128 => |values| .{ .decimal128 = values.slice(begin, count) },
                .dictionary_bytes => |values| .{ .dictionary_bytes = .{ .values = values.values, .indices = values.indices[begin..][0..count] } },
                inline .dictionary_i64, .dictionary_f64 => |values, tag| @unionInit(types.ColumnValues, @tagName(tag), .{ .values = values.values, .indices = values.indices[begin..][0..count] }),
                inline else => |values, tag| @unionInit(types.ColumnValues, @tagName(tag), values[begin..][0..count]),
            };
            vector.nulls = decoded.nulls.slice(begin, count);
            try vectors.append(a, vector);
        }
        return .{ .snapshot = binding.snapshot(), .external_span = .{ .first = first.external, .count = count }, .columns = vectors.items };
    }
    pub fn next(self: *Cursor) !?types.ColumnBatch {
        while (self.position < self.group.row_count) {
            _ = self.output.reset(.free_all);
            self.selected = &.{};
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
                if (rejected) {
                    self.position += count;
                    continue;
                }
                const evidence = try self.batch(count, true);
                if (filter.select) |select| {
                    self.selected = try select(filter.ptr, evidence);
                    if (self.selected.len != count) return error.InvalidParquetPage;
                    if (for (self.selected) |active| {
                        if (active) break false;
                    } else true) {
                        self.position += count;
                        continue;
                    }
                } else if (!try filter.any_match(filter.ptr, evidence)) {
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
