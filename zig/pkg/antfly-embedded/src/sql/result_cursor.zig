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

//! Statement-owned blocking results shared by nested relations and public streams.
const std = @import("std");

pub const Cursor = struct {
    manager: @import("spill.zig").Manager,
    shared: ?*@import("spill.zig").Manager = null,
    a: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    width: usize,
    rows: @import("spill.zig").Sequential,
    memory: ?*@import("replay_rows.zig").Replay = null,
    memory_reader: ?*@import("replay_rows.zig").Replay.Reader = null,
    sealed: bool = false,
    replay_failed: bool = false,
    replay_readers: usize = 0,
    borrowed_sorted: ?usize = null,
    index: usize = 0,
    sorted: ?*@import("operators.zig").TopK = null,
    sorted_offset: usize = 0,
    sorted_count: usize = 0,
    memory_leased: bool = false,
    block: ?*@import("spill.zig").Sequential.OwnedBlock = null,
    block_begin: usize = 0,
    payloads: ?*@import("spill.zig").JoinPayloads = null,
    payload_reader: ?*@import("spill.zig").PayloadReader = null,
    reference_reader: ?@import("spill.zig").Sequential.ReplayReader = null,
    reference_values: []@import("scalar.zig").Datum = &.{},
    reference_row_bytes: usize = 0,
    fn payloadReader(self: *Cursor) !*@import("spill.zig").PayloadReader {
        if (self.payload_reader == null) {
            const reader = try self.a.create(@import("spill.zig").PayloadReader);
            reader.* = .init(self.payloads.?);
            reader.row_bytes = self.reference_row_bytes;
            self.payload_reader = reader;
        }
        return self.payload_reader.?;
    }
    const HeapView = struct {
        top: *@import("operators.zig").TopK,
        begin: usize,
        count: usize,
        fn cell(raw: *anyopaque, a: std.mem.Allocator, row: usize, column: usize) anyerror!@import("scalar.zig").Datum {
            const view: *@This() = @ptrCast(@alignCast(raw));
            const position = view.begin + row;
            return if (view.top.nativeStore(position)) |store| store.cell(a, 0, column) else view.top.entries[position].row.values[column];
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const view: *@This() = @ptrCast(@alignCast(raw));
            return if (view.top.nativeStore(view.begin + row)) |store| store.frozenJson(0, column) else null;
        }
        fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
            const view: *@This() = @ptrCast(@alignCast(raw));
            return if (view.top.nativeStore(view.begin + row)) |store| store.encodedJson(0, column) else null;
        }
        fn identity(raw: *anyopaque, _: usize, column: usize) anyerror!?u64 {
            const view: *@This() = @ptrCast(@alignCast(raw));
            if (view.count != 1) return null; // Heap rows own independent stores.
            return if (view.top.nativeStore(view.begin)) |store| store.dictionaryId(0, column) else null;
        }
        fn dictionary(raw: *anyopaque, a: std.mem.Allocator, column: usize) anyerror!?@import("execution_batch.zig").Batch {
            const view: *@This() = @ptrCast(@alignCast(raw));
            if (view.count != 1) return null;
            const store = view.top.nativeStore(view.begin) orelse return null;
            return (@import("execution_batch.zig").Batch{ .retained = .{ .store = store, .count = 1 } }).dictionaryColumn(a, column);
        }
        fn batch(self: *@This(), width: usize) @import("execution_batch.zig").Batch {
            return .{ .reader = .{ .ptr = self, .read = cell, .read_frozen = frozen, .read_json = json, .read_identity = identity, .read_dictionary = dictionary, .count = self.count, .width = width } };
        }
    };
    fn heapAllocator(self: *Cursor) std.mem.Allocator {
        return @import("memory_budget.zig").reclaimingAllocator(self.a);
    }
    pub const Lease = struct {
        values: @import("execution_batch.zig").Batch,
        owner: ?*Cursor = null,
        block: ?*@import("spill.zig").Sequential.OwnedBlock = null,
        blocks: []const *@import("spill.zig").Sequential.OwnedBlock = &.{},
        payload_page: ?@import("spill.zig").PayloadReader.Page = null,
        sorted_page: ?@import("spill.zig").Sort.Page = null,
        ordinary_page: ?@import("spill.zig").Sort.ValuesPage = null,
        heap_view: ?*HeapView = null,
        pub fn deinit(self: Lease) void {
            if (self.payload_page) |page| page.release();
            if (self.sorted_page) |page| page.release();
            if (self.ordinary_page) |page| page.release();
            if (self.heap_view) |view| self.owner.?.heapAllocator().destroy(view);
            if (self.block) |block| block.release();
            for (self.blocks) |block| block.release();
            if (self.owner) |owner| owner.close();
        }
    };
    /// Sorted/native delivery owns its admitted metadata; sequential views
    /// use caller scratch. Pages retain the cursor through terminal cleanup;
    /// release them before the statement allocator closes.
    pub fn nextLease(self: *Cursor, a: std.mem.Allocator, maximum: usize, byte_limit: usize) !Lease {
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        self.releaseBorrowed();
        if (self.index == self.count()) return .{ .values = .{ .rows = &.{} } };
        if (self.payloads != null) {
            if (self.reference_reader == null) self.reference_reader = try self.rows.openReader();
            const input = try self.reference_reader.?.readBatchCurrent(a, self.index, @min(@min(64, maximum), @max(1, byte_limit / @max(1, self.reference_row_bytes))));
            const page = try (try self.payloadReader()).nativePage(a, input.values, self.width, byte_limit);
            errdefer page.release();
            self.index += page.values.len();
            _ = self.refs.fetchAdd(1, .monotonic);
            return .{ .values = page.values, .payload_page = page, .owner = self };
        }
        if (if (self.sorted == null) self.memory else null) |memory| {
            try memory.finish();
            const begin = self.index;
            var end = begin;
            var bytes: usize = 0;
            while (end < memory.count and end - begin < maximum) {
                for (memory.rows.items[end]) |value| bytes +|= try @import("operators.zig").datumBytes(value);
                end += 1;
                if (bytes >= byte_limit) break;
            }
            _ = self.refs.fetchAdd(1, .monotonic);
            self.index = end;
            return .{ .values = .{ .rows = memory.rows.items[begin..end] }, .owner = self };
        }
        if (self.sorted) |top| {
            if (top.external == null) self.memory_leased = true;
            const capacity = @min(64, @min(maximum, self.count() - self.index));
            if (top.external) |sort| {
                if (sort.order_sort == null) {
                    while (self.sorted_offset != 0) : (self.sorted_offset -= 1) if (!try sort.skip()) return error.InvalidSqlSpill;
                    const page = try sort.nextValuesPage(capacity, byte_limit);
                    errdefer page.release();
                    const values = page.batch();
                    if (values.len() == 0) return error.InvalidSqlSpill;
                    self.index += values.len();
                    _ = self.refs.fetchAdd(1, .monotonic);
                    return .{ .values = values, .ordinary_page = page, .owner = self };
                }
                while (self.sorted_offset != 0) : (self.sorted_offset -= 1) if (!try sort.skip()) return error.InvalidSqlSpill;
                const page = try sort.nextPageLeased(capacity, byte_limit);
                errdefer page.release();
                if (page.rows.len == 0) return error.InvalidSqlSpill;
                // Transport exposes payloads only. Release the separately
                // retained sort keys without expanding delivery descriptors.
                for (page.rows) |*row| {
                    if (row.key_block) |block| block.release();
                    row.key_block = null;
                    row.key_index = null;
                    row.keys = null;
                }
                self.index += page.rows.len;
                _ = self.refs.fetchAdd(1, .monotonic);
                return .{ .values = page.values, .sorted_page = page, .owner = self };
            }
            const stable = self.heapAllocator();
            if (@import("memory_budget.zig").headroom(stable)) |available| if (available < @sizeOf(HeapView)) return error.SqlProgramLimitExceeded;
            const view = try stable.create(HeapView);
            errdefer stable.destroy(view);
            view.* = .{ .top = top, .begin = self.sorted_offset + self.index, .count = capacity };
            const values = view.batch(self.width);
            var count_rows: usize = 0;
            var bytes: usize = 0;
            while (count_rows < capacity) {
                for (0..self.width) |column| bytes +|= try values.cellBytes(a, count_rows, column);
                count_rows += 1;
                if (bytes >= byte_limit) break;
            }
            view.count = count_rows;
            self.index += count_rows;
            _ = self.refs.fetchAdd(1, .monotonic);
            return .{ .values = view.batch(self.width), .heap_view = view, .owner = self };
        }
        try self.loadBlock();
        const block = self.block.?;
        const Batch = @import("execution_batch.zig").Batch;
        const source = try a.create(Batch);
        source.* = try block.batch(a, false);
        const begin = self.index - self.block_begin;
        var end = begin;
        var bytes: usize = 0;
        while (end < block.count() and end - begin < maximum) {
            for (0..self.width) |column| bytes +|= try source.cellBytes(a, end, column);
            end += 1;
            if (bytes >= byte_limit) break;
        }
        const selection = try a.alloc(usize, end - begin);
        for (selection, begin..) |*position, index| position.* = index;
        const selected = try source.select(a, selection);
        block.retain();
        _ = self.refs.fetchAdd(1, .monotonic);
        self.index += end - begin;
        return .{ .values = selected, .block = block, .owner = self };
    }
    fn loadBlock(self: *Cursor) !void {
        if (self.block) |block| {
            if (self.index >= self.block_begin and self.index - self.block_begin < block.count()) return;
            block.release();
            self.block = null;
        }
        self.block = try self.rows.readOwnedBlock(self.index);
        self.block_begin = self.index;
    }
    /// Borrow the statement spill owner, sharing its quota and cancellation.
    pub fn create(a: std.mem.Allocator, manager: *@import("spill.zig").Manager, width: usize) !*Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .manager = undefined, .shared = manager, .a = a, .width = width, .rows = try @import("spill.zig").Sequential.init(manager, @max(128, manager.buffer_bytes * 2)) };
        return self;
    }

    /// No-I/O execution uses the same lossless cursor and sort ownership
    /// transfer. Replay enforces a resident quota without a filesystem fallback.
    pub fn createMemory(a: std.mem.Allocator, width: usize, memory_bytes: usize) !*Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        const memory = try @import("replay_rows.zig").Replay.create(a, width, memory_bytes, null);
        self.* = .{ .manager = undefined, .a = a, .width = width, .rows = undefined, .memory = memory };
        return self;
    }
    pub fn next(self: *Cursor, a: std.mem.Allocator) !?[]const @import("scalar.zig").Datum {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        self.releaseBorrowed();
        if (self.index == self.count()) return null;
        const row = try self.read(a);
        if (self.ownsRead()) {
            self.index += 1;
            return row.values;
        }
        // Compact spill rows already allocated their descriptor span in a.
        // Own the payloads in place; resident/sorted spans remain borrowed.
        const values = if (self.payloads == null and self.block != null and self.block.?.encoded != null and self.sorted == null and self.memory == null) @constCast(row.values) else try a.alloc(@import("scalar.zig").Datum, row.values.len);
        for (row.values, values) |value, *out| out.* = try @import("operators.zig").cloneDatum(a, value);
        if (!self.sealed and !self.memory_leased) if (self.sorted) |top| if (top.external == null) top.releaseFinishedRow(self.sorted_offset + self.index);
        self.index += 1;
        return values;
    }

    /// One-pass ownership contract: resident rows are borrowed, external-sort
    /// values belong to a, and only a retaining consumer copies a payload. A
    /// resident sorted row expires on the next advancement (including EOF),
    /// reclaiming its arena before the next mutation image is prepared.
    pub fn nextBorrowed(self: *Cursor, a: std.mem.Allocator) !?[]const @import("scalar.zig").Datum {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        self.releaseBorrowed();
        if (self.index == self.count()) return null;
        const row = try self.read(a);
        if (!self.sealed and !self.memory_leased) if (self.sorted) |top| if (top.external == null) {
            self.borrowed_sorted = self.sorted_offset + self.index;
        };
        self.index += 1;
        return row.values;
    }

    fn releaseBorrowed(self: *Cursor) void {
        if (self.borrowed_sorted) |index| {
            self.sorted.?.releaseFinishedRow(index);
            self.borrowed_sorted = null;
        }
    }
    /// Gather blocking output into retained typed columns once. Spill row
    /// payloads live only in scratch; transport pages own their column store.
    pub fn nextBatch(self: *Cursor, a: std.mem.Allocator, maximum: usize, byte_limit: usize) !*@import("typed_store.zig").Store {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        self.releaseBorrowed();
        const store = try a.create(@import("typed_store.zig").Store);
        errdefer a.destroy(store);
        store.* = .init(a);
        errdefer store.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var bytes: usize = 0;
        while (self.index < self.count() and store.len < maximum) {
            _ = scratch.reset(.retain_capacity);
            const row = try self.read(scratch.allocator());
            for (row.values) |value| bytes +|= try @import("operators.zig").datumBytes(value);
            _ = try store.append(row.values);
            if (!self.sealed and !self.memory_leased) if (self.sorted) |top| if (top.external == null) top.releaseFinishedRow(self.sorted_offset + self.index);
            self.index += 1;
            if (bytes >= byte_limit) break;
        }
        return store;
    }
    pub fn count(self: *const Cursor) usize {
        return if (self.sorted != null) self.sorted_count else if (self.memory) |memory| memory.count else @intCast(self.rows.size);
    }
    pub fn takeSorted(raw: *anyopaque, top: *@import("operators.zig").TopK, offset: usize, limit: usize, implicit: bool) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.replay_failed or self.sealed or self.sorted != null or self.count() != 0) return error.InvalidSqlBackendResponse;
        const total = if (top.external) |sort| @min(sort.total, top.capacity) else top.count;
        const available = total -| offset;
        if (implicit and available > limit) return error.SqlResultTooLarge;
        const owned = try self.a.create(@import("operators.zig").TopK);
        errdefer self.a.destroy(owned);
        // Span views access the finished heap directly, without a duplicate
        // row-header snapshot. Its owner remains pinned by every output lease.
        if (top.external == null) try top.finishOrder();
        owned.* = top.*;
        top.* = .{ .alloc = top.alloc, .entries = &.{}, .orders = &.{}, .max_bytes = 0, .retained_bytes = 0 };
        self.sorted = owned;
        self.sorted_offset = @min(offset, total);
        self.sorted_count = @min(available, limit);
        if (owned.external == null) for (0..self.sorted_offset) |index| owned.releaseFinishedRow(index);
    }
    pub fn ownsRead(self: *const Cursor) bool {
        return if (self.sorted) |top| top.external != null else false;
    }
    pub fn read(self: *Cursor, a: std.mem.Allocator) !@import("operators.zig").Row {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        if (self.payloads != null) {
            if (self.reference_reader == null) self.reference_reader = try self.rows.openReader();
            const input = try self.reference_reader.?.readCurrent(self.index);
            const payload = try (try self.payloadReader()).read(try @import("spill.zig").PayloadReader.Reference.decode(input.row.values));
            if (self.block) |block| block.release();
            self.block = payload.block;
            if (self.reference_values.len == 0) self.reference_values = try self.a.alloc(@import("scalar.zig").Datum, self.width);
            if (payload.block.width() != self.width) return error.InvalidSqlSpill;
            for (self.reference_values, 0..) |*value, column| value.* = try payload.block.cell(payload.index, column);
            return .{ .values = self.reference_values, .keys = &.{}, .ordinal = self.index };
        }
        if (self.sorted) |top| {
            if (top.external) |sort| {
                while (self.sorted_offset != 0) : (self.sorted_offset -= 1) {
                    if (!try sort.skip()) return error.InvalidSqlSpill;
                }
                return (try sort.nextValues(a)) orelse error.InvalidSqlSpill;
            }
            return top.scalarRow(self.sorted_offset + self.index);
        }
        if (self.memory) |memory| {
            if (self.memory_reader == null) {
                try memory.finish();
                self.memory_reader = try memory.openReader();
            }
            self.memory_reader.?.index = self.index;
            return .{ .values = (try self.memory_reader.?.next()) orelse return error.InvalidSqlBackendResponse, .keys = &.{}, .ordinal = self.index };
        }
        try self.loadBlock();
        return self.block.?.row(a, self.index - self.block_begin);
    }
    pub fn append(raw: *anyopaque, values: []const @import("scalar.zig").Datum) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.replay_failed or self.sealed or values.len != self.width or self.sorted != null) return error.InvalidSqlBackendResponse;
        if (self.memory) |memory| return memory.append(values);
        _ = try self.rows.append(.{ .values = values, .keys = &.{}, .ordinal = self.rows.size }, @import("spill.zig").none);
    }
    pub fn appendBatch(raw: *anyopaque, values: @import("execution_batch.zig").Batch) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.replay_failed or self.sealed or self.sorted != null or values.width() != self.width) return error.InvalidSqlBackendResponse;
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        if (self.memory) |memory| {
            for (0..values.len()) |row| {
                _ = scratch.reset(.free_all);
                try memory.append(try values.row(scratch.allocator(), row));
            }
            return;
        }
        const ordinals = try scratch.allocator().alloc(u64, values.len());
        for (ordinals, 0..) |*ordinal, i| ordinal.* = self.rows.size + i;
        try self.rows.appendBatch(values, .{ .vectors = .{ .values = &.{}, .count = values.len() } }, ordinals);
    }
    pub fn close(self: *Cursor) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.replay_readers == 0);
        const a = self.a;
        if (self.block) |block| block.release();
        if (self.reference_reader) |*reader| reader.deinit();
        if (self.payload_reader) |reader| {
            reader.deinit();
            a.destroy(reader);
        }
        a.free(self.reference_values);
        if (self.payloads) |pages| {
            const backing = pages.file.manager.allocator();
            pages.close();
            backing.destroy(pages);
        }
        if (self.sorted) |top| {
            top.deinit();
            a.destroy(top);
        }
        if (self.memory) |memory| {
            if (self.memory_reader) |reader| reader.close();
            memory.deinit();
        } else {
            self.rows.close();
            if (self.shared == null) self.manager.deinit();
        }
        a.destroy(self);
    }

    /// Freeze a capture and start an independent borrowed pass. Resident sorted
    /// rows and existing runs are reused without copying. A forward-only sort
    /// merge seals its selected ordering references before replay; wide
    /// payload pages keep their original owner and are never rewritten.
    /// Partially consumed sorts reject admission because earlier rows may
    /// already have been retired.
    pub fn openReplayReader(self: *Cursor) !*ReplayReader {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        if (!self.sealed) {
            if (self.sorted) |top| {
                if (self.index != 0) return error.InvalidSqlBackendResponse;
                if (top.external != null) {
                    // Seal only the selected offset/limit window. Indirect
                    // sorts copy references; narrow sorts keep inline rows.
                    // Failed admission cannot retry a partially advanced merge.
                    errdefer self.replay_failed = true;
                    if (self.memory != null or self.rows.size != 0) return error.InvalidSqlBackendResponse;
                    var scratch = std.heap.ArenaAllocator.init(self.a);
                    defer scratch.deinit();
                    const sort = top.external.?;
                    if (sort.order_sort != null) {
                        while (self.sorted_offset != 0) : (self.sorted_offset -= 1) if (!try sort.skip()) return error.InvalidSqlSpill;
                        var ordinal: usize = 0;
                        while (ordinal < self.sorted_count) {
                            _ = scratch.reset(.free_all);
                            const cohort_rows = @min(64, self.sorted_count - ordinal);
                            var references: [64][2]@import("scalar.zig").Datum = undefined;
                            var rows: [64][]const @import("scalar.zig").Datum = undefined;
                            var ordinals: [64]u64 = undefined;
                            for (references[0..cohort_rows], rows[0..cohort_rows], ordinals[0..cohort_rows], 0..) |*reference, *row, *sequence, i| {
                                const lease = (try sort.nextReference()) orelse return error.InvalidSqlSpill;
                                defer lease.release();
                                reference.* = .{ try lease.cell(0), try lease.cell(1) };
                                row.* = reference;
                                sequence.* = ordinal + i;
                            }
                            try self.rows.appendBatch(.{ .rows = rows[0..cohort_rows] }, .{ .vectors = .{ .values = &.{}, .count = cohort_rows } }, ordinals[0..cohort_rows]);
                            ordinal += cohort_rows;
                        }
                        self.reference_row_bytes = sort.max_payload_bytes;
                        self.payloads = (try sort.takePayloads()).?;
                    } else for (0..self.sorted_count) |ordinal| {
                        _ = scratch.reset(.retain_capacity);
                        const row = try self.read(scratch.allocator());
                        _ = try self.rows.append(.{ .values = row.values, .keys = &.{}, .ordinal = ordinal }, @import("spill.zig").none);
                    }
                    try self.rows.seal();
                    top.deinit();
                    self.a.destroy(top);
                    self.sorted = null;
                }
            }
            if (self.memory) |memory| try memory.finish() else try self.rows.seal();
            self.sealed = true;
        }
        const reader = try self.a.create(ReplayReader);
        errdefer self.a.destroy(reader);
        reader.* = .{
            .owner = self,
            .storage = if (self.sorted != null) .sorted else if (self.memory) |memory| .{ .memory = try memory.openReader() } else .{ .disk = try self.rows.openReader() },
        };
        self.replay_readers += 1;
        return reader;
    }

    /// Stable-address reader owned by the cursor allocator. Borrowed values
    /// survive reads by other readers, but not this reader's next read
    /// or close. Consumers retaining a value must copy at their owned boundary.
    pub const ReplayReader = struct {
        owner: *Cursor,
        storage: union(enum) {
            sorted,
            memory: *@import("replay_rows.zig").Replay.Reader,
            disk: @import("spill.zig").Sequential.ReplayReader,
        },
        index: usize = 0,
        payload_reader: ?@import("spill.zig").PayloadReader = null,
        payload_blocks: [64]?*@import("spill.zig").Sequential.OwnedBlock = @splat(null),
        payload_page: ?@import("spill.zig").PayloadReader.Page = null,
        payload_scratch: ?std.heap.ArenaAllocator = null,
        fn retirePayloads(self: *ReplayReader) void {
            if (self.payload_page) |page| page.release();
            self.payload_page = null;
            for (&self.payload_blocks) |*block| if (block.*) |owner| {
                owner.release();
                block.* = null;
            };
            if (self.payload_scratch) |*arena| _ = arena.reset(.free_all);
        }
        fn payloadReader(self: *ReplayReader) *@import("spill.zig").PayloadReader {
            if (self.payload_reader == null) {
                self.payload_reader = .init(self.owner.payloads.?);
                self.payload_reader.?.row_bytes = self.owner.reference_row_bytes;
            }
            return &self.payload_reader.?;
        }

        pub fn next(self: *ReplayReader) !?[]const @import("scalar.zig").Datum {
            self.retirePayloads();
            if (self.index == self.owner.count()) return null;
            if (self.owner.payloads != null) {
                const input = try self.storage.disk.readCurrent(self.index);
                const payload = try self.payloadReader().read(try @import("spill.zig").PayloadReader.Reference.decode(input.row.values));
                self.payload_blocks[0] = payload.block;
                if (payload.block.width() != self.owner.width) return error.InvalidSqlSpill;
                if (self.payload_scratch == null) self.payload_scratch = .init(self.owner.a);
                const a = self.payload_scratch.?.allocator();
                const values = try a.alloc(@import("scalar.zig").Datum, self.owner.width);
                for (values, 0..) |*value, column| value.* = try payload.block.cell(payload.index, column);
                self.index += 1;
                return values;
            }
            const values = switch (self.storage) {
                .sorted => (try self.owner.sorted.?.scalarRow(self.owner.sorted_offset + self.index)).values,
                .memory => |reader| (try reader.next()) orelse return error.InvalidSqlBackendResponse,
                .disk => |*reader| (try reader.readCurrent(self.index)).row.values,
            };
            if (values.len != self.owner.width) return error.InvalidSqlBackendResponse;
            self.index += 1;
            return values;
        }

        pub fn nextBatch(self: *ReplayReader, a: std.mem.Allocator, maximum: usize) !@import("execution_batch.zig").Batch {
            if (maximum == 0) return error.InvalidSqlLimit;
            self.retirePayloads();
            if (self.index == self.owner.count()) return .{ .rows = &.{} };
            return switch (self.storage) {
                .disk => |*reader| blk: {
                    const bound = if (self.owner.payloads != null) @min(@min(64, maximum), @max(1, (128 * 1024) / @max(1, self.owner.reference_row_bytes))) else maximum;
                    const batch = try reader.readBatchCurrent(a, self.index, bound);
                    if (self.owner.payloads != null) {
                        const page = try self.payloadReader().nativePage(a, batch.values, self.owner.width, 128 * 1024);
                        self.payload_page = page;
                        self.index += page.values.len();
                        break :blk page.values;
                    }
                    self.index = @intCast(batch.following);
                    break :blk batch.values;
                },
                .memory => |reader| blk: {
                    const batch = try reader.nextBatch(a, maximum);
                    self.index += batch.len();
                    break :blk batch;
                },
                .sorted => blk: {
                    const count_rows = @min(maximum, self.owner.count() - self.index);
                    const View = struct {
                        top: *@import("operators.zig").TopK,
                        begin: usize,
                        fn cell(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize) anyerror!@import("scalar.zig").Datum {
                            const self_: *@This() = @ptrCast(@alignCast(raw));
                            const position = self_.begin + row;
                            return if (self_.top.nativeStore(position)) |store| store.cell(alloc, 0, column) else self_.top.entries[position].row.values[column];
                        }
                        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                            const self_: *@This() = @ptrCast(@alignCast(raw));
                            return if (self_.top.nativeStore(self_.begin + row)) |store| store.frozenJson(0, column) else null;
                        }
                        fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
                            const self_: *@This() = @ptrCast(@alignCast(raw));
                            return if (self_.top.nativeStore(self_.begin + row)) |store| store.encodedJson(0, column) else null;
                        }
                    };
                    const view = try a.create(View);
                    view.* = .{ .top = self.owner.sorted.?, .begin = self.owner.sorted_offset + self.index };
                    self.index += count_rows;
                    break :blk .{ .reader = .{ .ptr = view, .read = View.cell, .read_frozen = View.frozen, .read_json = View.json, .count = count_rows, .width = self.owner.width } };
                },
            };
        }

        pub fn rewind(self: *ReplayReader) void {
            self.retirePayloads();
            self.index = 0;
            if (self.storage == .memory) self.storage.memory.rewind();
        }

        pub fn close(self: *ReplayReader) void {
            const owner = self.owner;
            self.retirePayloads();
            if (self.payload_reader) |*reader| reader.deinit();
            if (self.payload_scratch) |*arena| arena.deinit();
            switch (self.storage) {
                .sorted => {},
                .memory => |reader| reader.close(),
                .disk => |*reader| reader.deinit(),
            }
            owner.replay_readers -= 1;
            owner.a.destroy(self);
        }
    };
};

test "SQL no-I/O blocking cursors preserve arrays bound memory and unwind allocation failures" {
    const Scenario = struct {
        fn run(a: std.mem.Allocator) !void {
            const Datum = @import("scalar.zig").Datum;
            var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
            const cursor = try Cursor.createMemory(a, 1, 64 * 1024);
            defer cursor.close();
            try Cursor.append(cursor, &.{Datum.typedArray(&array)});
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const row = (try cursor.next(arena.allocator())).?;
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
            try std.testing.expectEqual(@as(i32, -2), row[0].array.?.dimensions[0].lower);
            try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
            try std.testing.expect(try cursor.next(arena.allocator()) == null);
        }
    };
    try @import("allocation_test_support.zig").checkAllAllocationFailures(Scenario.run, .{});
    const cursor = try Cursor.createMemory(std.testing.allocator, 1, 0);
    defer cursor.close();
    try std.testing.expectError(error.SqlProgramLimitExceeded, Cursor.append(cursor, &.{.{}}));
}

test "SQL sealed typed captures replay independent borrowed passes without a second spool" {
    const Scenario = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, disk: bool) !void {
            const Datum = @import("scalar.zig").Datum;
            const arrays = @import("array_value.zig");
            var token: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = checkpoint, .compression = .none };
            defer manager.deinit();
            {
                const cursor = if (disk) try Cursor.create(a, &manager, 4) else try Cursor.createMemory(a, 4, 2 * 1024 * 1024);
                defer cursor.close();
                var elements = [_]Datum{ Datum.json(.{ .integer = 9007199254740993 }), .{} };
                var array = try arrays.Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &elements, .{});
                var json = try arrays.Value.init(.jsonb, &.{.{ .length = 2 }}, &.{ Datum.json(.null), .{} }, .{});
                // Allocation-fault coverage needs only a small resident run;
                // the disk fixture crosses several encoded block boundaries.
                const count: usize = if (disk) 1024 else 2;
                for (0..count) |_| try Cursor.append(cursor, &.{ Datum.typedArray(&array), .{}, Datum.json(.null), Datum.typedArray(&json) });
                elements[0].value = .{ .integer = 42 };
                const first = try cursor.openReplayReader();
                defer first.close();
                const second = try cursor.openReplayReader();
                defer second.close();
                try std.testing.expectError(error.InvalidSqlBackendResponse, Cursor.append(cursor, &.{ .{}, .{}, .{}, .{} }));
                const retained = (try first.next()).?;
                const files = manager.files;
                for (0..count) |_| {
                    const row = (try second.next()).?;
                    try verify(row);
                    if (!disk) try std.testing.expect(retained[0].array == row[0].array or second.index != 1);
                }
                try std.testing.expect(try second.next() == null);
                try verify(retained); // The other reader cannot retire our block.
                for (0..3) |_| {
                    first.rewind();
                    for (0..count) |_| try verify((try first.next()).?);
                    try std.testing.expect(try first.next() == null);
                }
                second.rewind();
                try verify((try second.next()).?);
                try std.testing.expectEqual(files, manager.files);
                try std.testing.expectEqual(@as(usize, if (disk) 1 else 0), files);
                try std.testing.expectEqual(@as(usize, 0), cursor.index);
                // Existing owning consumption remains independent of replay.
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                try verify((try cursor.next(scratch.allocator())).?);
            }
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
        fn verify(row: []const @import("scalar.zig").Datum) !void {
            try std.testing.expectEqual(@as(usize, 4), row.len);
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
            try std.testing.expectEqual(@as(i32, -2), row[0].array.?.dimensions[0].lower);
            try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
            try std.testing.expect(row[1].sql_null);
            try std.testing.expect(!row[2].sql_null and row[2].value == .null);
            try std.testing.expect(!row[3].array.?.flatCell(0).sql_null);
            try std.testing.expect(row[3].array.?.flatCell(0).value == .null);
            try std.testing.expect(row[3].array.?.flatCell(1).sql_null);
        }
    };
    try @import("allocation_test_support.zig").checkAllAllocationFailures(Scenario.run, .{false});
    try Scenario.run(std.testing.allocator, true);
}

test "SQL replay freezes resident sorted owners and seals external sort windows once" {
    const Scenario = struct {
        fn checkpoint(ptr: *anyopaque) !void {
            if (@as(*bool, @ptrCast(@alignCast(ptr))).*) return error.QueryCanceled;
        }
        fn run(a: std.mem.Allocator, disk: bool, cancel: bool, consumed: bool) !void {
            const operators = @import("operators.zig");
            const Datum = @import("scalar.zig").Datum;
            var canceled = false;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &canceled, .checkpoint = checkpoint, .compression = .none };
            defer manager.deinit();
            {
                const cursor = if (disk) try Cursor.create(a, &manager, 2) else try Cursor.createMemory(a, 2, 64 * 1024);
                defer cursor.close();
                const count: usize = if (disk) 256 else 8;
                var top = try operators.TopK.initWithSpill(a, count, &.{.{ .descending = true }}, if (disk) 16 * 1024 else 64 * 1024, if (disk) &manager else null);
                defer top.deinit();
                var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
                for (0..count) |ordinal| {
                    const id = Datum.json(.{ .integer = @intCast(ordinal) });
                    try top.add(.{ .values = &.{ id, Datum.typedArray(&array) }, .keys = &.{id}, .ordinal = ordinal });
                }
                try Cursor.takeSorted(cursor, &top, 2, 3, false);
                try std.testing.expectEqual(disk, cursor.sorted.?.external != null);
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                if (consumed) {
                    _ = (try cursor.next(scratch.allocator())).?;
                    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.openReplayReader());
                    return;
                }
                canceled = cancel;
                if (cancel) {
                    try std.testing.expectError(error.QueryCanceled, cursor.openReplayReader());
                    canceled = false;
                    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.openReplayReader());
                    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.next(scratch.allocator()));
                    return;
                }
                const first = try cursor.openReplayReader();
                defer first.close();
                const second = try cursor.openReplayReader();
                defer second.close();
                try std.testing.expectEqual(@as(usize, 3), cursor.count());
                if (disk) {
                    try std.testing.expect(cursor.sorted == null);
                    try std.testing.expectEqual(@as(usize, 1), manager.files);
                } else try std.testing.expect(cursor.sorted != null);
                const row = (try first.next()).?;
                if (!disk) try std.testing.expect(row[1].array == cursor.sorted.?.entries[cursor.sorted_offset].row.values[1].array);
                for (0..3) |_| {
                    second.rewind();
                    for (0..3) |ordinal| {
                        const value = (try second.next()).?;
                        try std.testing.expectEqual(@as(i64, @intCast(count - 3 - ordinal)), value[0].value.integer);
                        try std.testing.expectEqual(@as(i64, 9007199254740993), value[1].array.?.flatCell(0).value.integer);
                        try std.testing.expectEqual(@as(i32, -2), value[1].array.?.dimensions[0].lower);
                        try std.testing.expect(value[1].array.?.flatCell(1).sql_null);
                    }
                    try std.testing.expect(try second.next() == null);
                }
                _ = (try cursor.next(scratch.allocator())).?;
                try std.testing.expectEqual(@as(i64, 9007199254740993), row[1].array.?.flatCell(0).value.integer);
            }
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
    };
    try @import("allocation_test_support.zig").checkAllAllocationFailures(Scenario.run, .{ false, false, false });
    try Scenario.run(std.testing.allocator, false, false, true);
    try Scenario.run(std.testing.allocator, true, false, false);
    try Scenario.run(std.testing.allocator, true, false, true);
    try Scenario.run(std.testing.allocator, true, true, false);
}

test "SQL forward borrowed sorted rows allocate nothing and retire before the next image" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const cursor = try Cursor.createMemory(a, 1, 64 * 1024);
    defer cursor.close();
    var top = try @import("operators.zig").TopK.init(a, 4, &.{.{}}, 64 * 1024);
    defer top.deinit();
    var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
    for (0..4) |ordinal| try top.add(.{ .values = &.{Datum.typedArray(&array)}, .keys = &.{Datum.json(.{ .integer = @intCast(ordinal) })}, .ordinal = ordinal });
    try Cursor.takeSorted(cursor, &top, 1, 3, false);
    try std.testing.expectEqual(@as(usize, 1), cursor.sorted.?.released);
    for (0..3) |ordinal| {
        // Any accidental per-row cloning is an immediate allocation failure.
        const row = (try cursor.nextBorrowed(std.testing.failing_allocator)).?;
        try std.testing.expectEqual(@as(usize, 1) + ordinal, cursor.sorted.?.released);
        try std.testing.expect(row[0].array == cursor.sorted.?.entries[1 + ordinal].row.values[0].array);
        try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
        try std.testing.expectEqual(@as(i32, -2), row[0].array.?.dimensions[0].lower);
        try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
    }
    try std.testing.expect(try cursor.nextBorrowed(std.testing.failing_allocator) == null);
    try std.testing.expectEqual(@as(usize, 4), cursor.sorted.?.released);
    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.openReplayReader());
}

test "SQL blocking result blocks preserve exact values and avoid row directories" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    {
        const cursor = try Cursor.create(a, &manager, 3);
        defer cursor.close();
        for (0..1024) |_| try Cursor.append(cursor, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.null) });
        try std.testing.expectEqual(@as(usize, 1), manager.files);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count: usize = 0;
        while (true) {
            _ = arena.reset(.retain_capacity);
            const row = (try cursor.next(arena.allocator())) orelse break;
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].value.integer);
            try std.testing.expect(row[1].sql_null);
            try std.testing.expect(!row[2].sql_null and row[2].value == .null);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1024), count);
        try std.testing.expect(manager.read_calls < 128);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL array replay and no-I/O leases coexist without retiring borrowed sort rows" {
    const Scenario = struct {
        fn run(a: std.mem.Allocator, sorted: bool) !void {
            const Datum = @import("scalar.zig").Datum;
            var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
            const cursor = try Cursor.createMemory(a, 1, 64 * 1024);
            var owned = true;
            defer if (owned) cursor.close();
            var top = try @import("operators.zig").TopK.init(a, 2, &.{}, 64 * 1024);
            defer top.deinit();
            for (0..2) |ordinal| {
                if (sorted) try top.add(.{ .values = &.{Datum.typedArray(&array)}, .keys = &.{}, .ordinal = ordinal }) else try Cursor.append(cursor, &.{Datum.typedArray(&array)});
            }
            if (sorted) try Cursor.takeSorted(cursor, &top, 0, 2, false);
            const replay = try cursor.openReplayReader();
            var replay_live = true;
            defer if (replay_live) replay.close();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const lease = try cursor.nextLease(arena.allocator(), 1, 4096);
            defer lease.deinit();
            _ = (try cursor.nextBorrowed(arena.allocator())).?;
            try std.testing.expect(try cursor.nextBorrowed(arena.allocator()) == null);
            for (0..2) |_| {
                const row = (try replay.next()).?;
                try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
            }
            replay.close();
            replay_live = false;
            cursor.close();
            owned = false;
            const retained = try lease.values.cell(a, 0, 0);
            try std.testing.expectEqual(@as(i32, -2), retained.array.?.dimensions[0].lower);
            try std.testing.expectEqual(@as(i64, 9007199254740993), retained.array.?.flatCell(0).value.integer);
            try std.testing.expect(retained.array.?.flatCell(1).sql_null);
        }
    };
    for ([_]bool{ false, true }) |sorted| try @import("allocation_test_support.zig").checkAllAllocationFailures(Scenario.run, .{sorted});
}

test "SQL blocking leases survive subsequent pulls for sorted and sequential wide results" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const spill = @import("spill.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]enum { sequential, memory, external }{ .sequential, .memory, .external }) |mode| {
        const sorted = mode != .sequential;
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
        defer manager.deinit();
        const cursor = try Cursor.create(a, &manager, 3);
        var cursor_owned = true;
        defer if (cursor_owned) cursor.close();
        var top = try @import("operators.zig").TopK.initWithSpill(a, 1000, &.{.{}}, if (mode == .external) 256 * 1024 else 8 * 1024 * 1024, &manager);
        defer top.deinit();
        var payload: [8192]u8 = @splat('x');
        for (0..300) |index| {
            const key = Datum.json(.{ .integer = @intCast(if (sorted) 299 - index else index) });
            const values = &.{ key, Datum.json(.{ .string = &payload }), Datum{} };
            if (sorted) try top.add(.{ .values = values, .keys = &.{key}, .ordinal = index }) else try Cursor.append(cursor, values);
        }
        if (sorted) {
            try std.testing.expectEqual(mode == .external, top.external != null);
            try Cursor.takeSorted(cursor, &top, 0, 300, false);
        }
        var first_arena = std.heap.ArenaAllocator.init(a);
        defer first_arena.deinit();
        const first = try cursor.nextLease(first_arena.allocator(), 17, 32768);
        defer first.deinit();
        try std.testing.expect(first.values.len() > 0);
        const first_value = try first.values.cell(a, 0, 1);
        try std.testing.expectEqual(@as(i64, 0), (try first.values.cell(a, 0, 0)).value.integer);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count_rows = first.values.len();
        while (cursor.index < cursor.count()) {
            _ = arena.reset(.free_all);
            if (count_rows % 5 == 0) {
                const row = (try cursor.next(arena.allocator())).?;
                try std.testing.expectEqual(@as(i64, @intCast(count_rows)), row[0].value.integer);
                count_rows += 1;
            } else {
                const page = try cursor.nextLease(arena.allocator(), 17, 32768);
                defer page.deinit();
                for (0..page.values.len()) |row| {
                    try std.testing.expectEqual(@as(i64, @intCast(count_rows)), (try page.values.cell(a, row, 0)).value.integer);
                    try std.testing.expect((try page.values.cell(a, row, 2)).sql_null);
                    count_rows += 1;
                }
            }
        }
        try std.testing.expectEqual(@as(usize, 300), count_rows);
        cursor_owned = false;
        cursor.close();
        try std.testing.expectEqualStrings(&payload, first_value.value.string);
        try std.testing.expectEqualStrings(&payload, (try first.values.cell(a, 0, 1)).value.string);
    }
}

test "SQL blocking native leases size frozen JSON without DOM expansion and preserve dictionary selections" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, cardinality: usize, measure: bool) !void {
            const spill = @import("spill.zig");
            const Datum = @import("scalar.zig").Datum;
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const cursor = try Cursor.create(a, &manager, 2);
            defer cursor.close();
            cursor.rows.block_bytes = 1024 * 1024;
            var array = std.json.Array.init(sa);
            for (0..cardinality) |index| try array.append(.{ .integer = @intCast(index) });
            const datum = Datum.json(.{ .array = array });
            const row = &.{ datum, Datum.json(.{ .string = "repeated dictionary body" }) };
            // More rows make the physical string dictionary worthwhile.
            const rows = try sa.alloc([]const Datum, 8);
            @memset(rows, row);
            try Cursor.appendBatch(cursor, .{ .rows = rows });
            try cursor.loadBlock();
            const block = cursor.block.?;
            const before = block.retainedBytes();
            const first = try cursor.nextLease(sa, 2, 1024 * 1024);
            defer first.deinit();
            try std.testing.expect((try first.values.frozenJson(0, 0)) != null);
            const dictionary = (try first.values.dictionaryColumn(sa, 1)).?;
            try std.testing.expectEqual(@as(usize, 2), dictionary.len());
            try std.testing.expectEqualStrings("repeated dictionary body", (try dictionary.cell(sa, 1, 0)).value.string);
            const after = block.retainedBytes();
            try std.testing.expectEqual(before, after);
            const next = try cursor.nextLease(sa, 6, 1024 * 1024);
            defer next.deinit();
            try std.testing.expect((try first.values.frozenJson(1, 0)) != null);
            try std.testing.expect((try next.values.frozenJson(0, 0)) != null);
            if (measure) std.debug.print("BLOCKING_FROZEN_LEASE rows={d} elements={d} before={d} after={d}\n", .{ first.values.len(), cardinality, before, after });
        }
    };
    try Fixture.run(std.testing.allocator, 4096, true);
    try @import("allocation_test_support.zig").checkAllAllocationFailures(Fixture.run, .{ @as(usize, 16), false });
}

test "SQL native sorted heap leases and replay preserve frozen rows after producer retirement" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, elements: usize) !void {
            const spill = @import("spill.zig");
            const Datum = @import("scalar.zig").Datum;
            const Reader = struct {
                block: *spill.ColumnarBlock,
                fn cell(raw: *anyopaque, _: std.mem.Allocator, row: usize, column: usize) anyerror!Datum {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    return self.block.cell(row, column);
                }
                fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    return self.block.frozenJson(row, column, false);
                }
            };
            var token: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check };
            defer manager.deinit();
            const cursor = try Cursor.create(a, &manager, 1);
            defer cursor.close();
            var top = try @import("operators.zig").TopK.initWithSpill(a, 2, &.{.{}}, 1024 * 1024, &manager);
            defer top.deinit();
            {
                var source = std.heap.ArenaAllocator.init(a);
                defer source.deinit();
                const sa = source.allocator();
                var array = std.json.Array.init(sa);
                for (0..elements) |index| try array.append(.{ .integer = @intCast(index) });
                const wire = try spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .array = array })}, .keys = &.{}, .ordinal = 0 }}, 1024 * 1024, .v2);
                var block = try spill.decodeColumnarBlockForForwarding(sa, a, wire, 1024 * 1024);
                defer block.deinit();
                var reader: Reader = .{ .block = &block };
                for (0..8) |index| {
                    const key = Datum.json(.{ .integer = @intCast(8 - index) });
                    try top.addBatch(sa, .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 1, .width = 1 } }, &.{&.{key}}, &.{index});
                }
                try std.testing.expect(top.external == null);
                try Cursor.takeSorted(cursor, &top, 0, 2, false);
            }
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const reader = try cursor.openReplayReader();
            defer reader.close();
            const batch = try reader.nextBatch(scratch.allocator(), 2);
            for (0..2) |index| try std.testing.expect((try batch.frozenJson(index, 0)) != null);
            // A forward transport lease can coexist with an independent replay.
            const lease = try cursor.nextLease(scratch.allocator(), 2, 1024 * 1024);
            defer lease.deinit();
            try std.testing.expect((try lease.values.frozenJson(1, 0)) != null);
            reader.rewind();
            const values = (try reader.next()).?;
            try std.testing.expectEqual(elements, values[0].value.array.items.len);
            try std.testing.expectEqual(@as(i64, @intCast(elements - 1)), values[0].value.array.items[elements - 1].integer);
            try std.testing.expectEqual(@as(u64, 0), manager.written_bytes);
        }
    };
    try Fixture.run(std.testing.allocator, 4096);
    try @import("allocation_test_support.zig").checkAllAllocationFailures(Fixture.run, .{@as(usize, 16)});
}

test "SQL bounded native delivery sorted cursor output admission" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const spill = @import("spill.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]usize{ 4000, 6000, 8000, 12000, 16000 }) |available| for ([_]usize{ 1, 64 }) |maximum| for ([_]bool{ false, true }) |via_cursor| {
        var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 2 * 1024 * 1024 };
        defer std.debug.assert(budget.live == 0);
        var token: u8 = 0;
        var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = Hook.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        const cursor = try Cursor.create(budget.allocator(), &manager, 2);
        defer cursor.close();
        var top = try @import("operators.zig").TopK.initWithSpill(budget.allocator(), 64, &.{.{}}, 8192, &manager);
        defer top.deinit();
        const body: [4096]u8 = @splat('x');
        var values: [64][2]Datum = undefined;
        var rows: [64][]const Datum = undefined;
        var key_values: [64]Datum = undefined;
        var keys: [64][]const Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..64) |i| {
            values[i] = .{ Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = &body }) };
            rows[i] = &values[i];
            key_values[i] = Datum.json(.{ .integer = @intCast(i) });
            keys[i] = key_values[i..][0..1];
            ordinals[i] = i;
        }
        var input_arena = std.heap.ArenaAllocator.init(a);
        defer input_arena.deinit();
        try top.addBatch(input_arena.allocator(), .{ .rows = &rows }, &keys, &ordinals);
        try Cursor.takeSorted(cursor, &top, 0, 64, false);
        const sort = cursor.sorted.?.external.?;
        const warm = try sort.nextPageLeased(1, 128 * 1024);
        warm.release();
        cursor.index = 1;
        try std.testing.expect(sort.order_sort != null);
        const ballast = try budget.allocator().alloc(u8, @import("memory_budget.zig").headroom(manager.alloc).? - available);
        var ballast_live = true;
        defer if (ballast_live) budget.allocator().free(ballast);
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        var seen: usize = 0;
        if (via_cursor) {
            const lease = try cursor.nextLease(scratch.allocator(), maximum, 128 * 1024);
            defer lease.deinit();
            // Sorted delivery metadata belongs to the lease, not caller scratch.
            _ = scratch.reset(.free_all);
            seen = lease.values.len();
            for (0..seen) |i| try std.testing.expectEqual(@as(i64, @intCast(i + 1)), (try lease.values.cell(scratch.allocator(), i, 0)).value.integer);
        } else {
            const page = try sort.nextPageLeased(maximum, 128 * 1024);
            defer page.release();
            seen = page.rows.len;
            for (page.rows, 1..) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i)), (try row.cell(0)).value.integer);
        }
        try std.testing.expect(!budget.isExhausted());
        try std.testing.expect(seen > 0);
        std.debug.print("NATIVE_CURSOR via_cursor={} available={d} maximum={d} seen={d} peak={d}\n", .{ via_cursor, available, maximum, seen, budget.peak });
        // Remaining ranks must survive admission denials, including a key
        // owner loaded before a denied head-scratch allocation.
        budget.allocator().free(ballast);
        ballast_live = false;
        var expected = seen + 1;
        while (expected < 64) {
            if (via_cursor) {
                const lease = try cursor.nextLease(scratch.allocator(), 64, 128 * 1024);
                defer lease.deinit();
                try std.testing.expect(lease.values.len() != 0);
                for (0..lease.values.len()) |row| {
                    try std.testing.expectEqual(@as(i64, @intCast(expected)), (try lease.values.cell(scratch.allocator(), row, 0)).value.integer);
                    expected += 1;
                }
            } else {
                const page = try sort.nextPageLeased(64, 128 * 1024);
                defer page.release();
                try std.testing.expect(page.rows.len != 0);
                for (page.rows) |row| {
                    try std.testing.expectEqual(@as(i64, @intCast(expected)), (try row.cell(0)).value.integer);
                    expected += 1;
                }
            }
        }
        try std.testing.expectEqual(@as(usize, 64), expected);
        try std.testing.expect(!budget.isExhausted());
    };
}

test "SQL bounded native delivery sorted cursor owns exact pages across faults and early close" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, compressed: bool) !void {
            const spill = @import("spill.zig");
            const Datum = @import("scalar.zig").Datum;
            var token: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var cursor: ?*Cursor = try Cursor.create(a, &manager, 2);
            defer if (cursor) |owner| owner.close();
            var top = try @import("operators.zig").TopK.initWithSpill(a, 16, &.{.{}}, 8192, &manager);
            defer top.deinit();
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const body: [4096]u8 = @splat('x');
            var values: [16][2]Datum = undefined;
            var rows: [16][]const Datum = undefined;
            var keys: [16][]const Datum = undefined;
            var ordinals: [16]u64 = undefined;
            for (0..16) |i| {
                values[i] = .{ Datum.json(.{ .integer = @intCast(15 - i) }), Datum.json(.{ .string = &body }) };
                rows[i] = &values[i];
                keys[i] = values[i][0..1];
                ordinals[i] = i;
            }
            try top.addBatch(source.allocator(), .{ .rows = &rows }, &keys, &ordinals);
            try Cursor.takeSorted(cursor.?, &top, 0, 16, false);
            try std.testing.expect(cursor.?.sorted.?.external.?.order_sort != null);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const first = try cursor.?.nextLease(scratch.allocator(), 8, 128 * 1024);
            defer first.deinit();
            const second = try cursor.?.nextLease(scratch.allocator(), 8, 128 * 1024);
            defer second.deinit();
            try std.testing.expect(first.values.len() != 0 and second.values.len() != 0);
            cursor.?.close();
            cursor = null;
            _ = scratch.reset(.free_all);
            try std.testing.expectEqual(@as(i64, 0), (try first.values.cell(scratch.allocator(), 0, 0)).value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(first.values.len())), (try second.values.cell(scratch.allocator(), 0, 0)).value.integer);
            try std.testing.expectEqualStrings(&body, (try first.values.cell(scratch.allocator(), 0, 1)).value.string);
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        try Harness.run(std.testing.allocator, compressed);
        try @import("allocation_test_support.zig").checkAllAllocationFailures(Harness.run, .{compressed});
    }
}

test "SQL compact delivery narrow cursor admission" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const spill = @import("spill.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |external| for ([_]usize{ 1024, 2048, 4096 }) |available| for ([_]usize{ 1, 64 }) |maximum| {
        var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 2 * 1024 * 1024 };
        defer std.debug.assert(budget.live == 0);
        var token: u8 = 0;
        var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = Hook.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        const cursor = try Cursor.create(budget.allocator(), &manager, 1);
        defer cursor.close();
        var top = try @import("operators.zig").TopK.initWithSpill(budget.allocator(), 64, &.{.{}}, if (external) 8192 else 1024 * 1024, &manager);
        defer top.deinit();
        var source = std.heap.ArenaAllocator.init(a);
        defer source.deinit();
        var cells: [64]Datum = undefined;
        var rows: [64][]const Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..64) |i| {
            cells[i] = Datum.json(.{ .integer = @intCast(i) });
            rows[i] = cells[i..][0..1];
            ordinals[i] = i;
        }
        try top.addBatch(source.allocator(), .{ .rows = &rows }, &rows, &ordinals);
        try std.testing.expectEqual(external, top.external != null);
        try Cursor.takeSorted(cursor, &top, 0, 64, false);
        {
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const warm = try cursor.nextLease(scratch.allocator(), 1, 128 * 1024);
            defer warm.deinit();
            try std.testing.expectEqual(@as(i64, 0), (try warm.values.cell(scratch.allocator(), 0, 0)).value.integer);
        }
        if (cursor.sorted.?.external) |sort| try std.testing.expect(sort.order_sort == null);
        const ballast = try budget.allocator().alloc(u8, @import("memory_budget.zig").headroom(manager.alloc).? - available);
        var ballast_live = true;
        defer if (ballast_live) budget.allocator().free(ballast);
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        var failure: ?anyerror = null;
        var seen: usize = 0;
        const lease = cursor.nextLease(scratch.allocator(), maximum, 128 * 1024) catch |err| blk: {
            failure = err;
            break :blk null;
        };
        if (lease) |value| {
            defer value.deinit();
            _ = scratch.reset(.free_all);
            seen = value.values.len();
            for (0..seen) |i| try std.testing.expectEqual(@as(i64, @intCast(i + 1)), (try value.values.cell(scratch.allocator(), i, 0)).value.integer);
        }
        try std.testing.expect(!budget.isExhausted());
        if (external and available == 1024) try std.testing.expectEqual(error.SqlProgramLimitExceeded, failure.?);
        if (available >= 2048 or !external) {
            try std.testing.expect(failure == null);
            try std.testing.expect(seen > 0);
            try std.testing.expect(!budget.isExhausted());
        }
        budget.allocator().free(ballast);
        ballast_live = false;
        var expected: usize = seen + 1;
        while (expected < 64 and failure == null) {
            const next = try cursor.nextLease(scratch.allocator(), 64, 128 * 1024);
            defer next.deinit();
            try std.testing.expect(next.values.len() != 0);
            for (0..next.values.len()) |i| {
                try std.testing.expectEqual(@as(i64, @intCast(expected)), (try next.values.cell(scratch.allocator(), i, 0)).value.integer);
                expected += 1;
            }
        }
        if (failure == null) try std.testing.expectEqual(@as(usize, 64), expected);
        std.debug.print("COMPACT_NARROW external={} available={d} maximum={d} seen={d} failure={?} exhausted={} peak={d}\n", .{ external, available, maximum, seen, failure, budget.isExhausted(), budget.peak });
    };
}

test "SQL compact delivery heap resident sort and ordinary spill pages survive faults offsets scratch reset and early close" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, mode: u8, compressed: bool) !void {
            const spill = @import("spill.zig");
            const Datum = @import("scalar.zig").Datum;
            var token: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var cursor: ?*Cursor = try Cursor.create(a, &manager, 2);
            defer if (cursor) |owner| owner.close();
            var top = try @import("operators.zig").TopK.initWithSpill(a, if (mode == 1) 1024 else 16, &.{.{}}, if (mode == 2) 1024 else if (mode == 1) 128 * 1024 else 1024 * 1024, &manager);
            defer top.deinit();
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            var values: [16][2]Datum = undefined;
            var rows: [16][]const Datum = undefined;
            var keys: [16][]const Datum = undefined;
            var ordinals: [16]u64 = undefined;
            for (0..16) |i| {
                values[i] = .{ Datum.json(.{ .integer = @intCast(15 - i) }), if (i % 2 == 0) Datum.json(.null) else .{} };
                rows[i] = &values[i];
                keys[i] = values[i][0..1];
                ordinals[i] = i;
            }
            // Mode 1 exercises a resident ordinary Sort behind an external
            // TopK; mode 2 forces physical runs; mode 0 stays in the heap.
            try top.addBatch(source.allocator(), .{ .rows = &rows }, &keys, &ordinals);
            try Cursor.takeSorted(cursor.?, &top, 3, 7, false);
            if (cursor.?.sorted.?.external) |sort| {
                try std.testing.expect(sort.order_sort == null);
                try sort.finish();
                try std.testing.expectEqual(mode == 2, sort.output_count != 0);
            } else try std.testing.expect(mode == 0);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const first = try cursor.?.nextLease(scratch.allocator(), 2, 128 * 1024);
            defer first.deinit();
            const second = try cursor.?.nextLease(scratch.allocator(), 2, 128 * 1024);
            defer second.deinit();
            try std.testing.expect(first.values.len() != 0 and second.values.len() != 0);
            const expected_second = 3 + first.values.len();
            cursor.?.close();
            cursor = null;
            _ = scratch.reset(.free_all);
            try std.testing.expectEqual(@as(i64, 3), (try first.values.cell(scratch.allocator(), 0, 0)).value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(expected_second)), (try second.values.cell(scratch.allocator(), 0, 0)).value.integer);
            try std.testing.expect(!(try first.values.cell(scratch.allocator(), 0, 1)).sql_null);
        }
    };
    for ([_]u8{ 0, 1, 2 }) |mode| for ([_]bool{ false, true }) |compressed| {
        try Harness.run(std.testing.allocator, mode, compressed);
        try @import("allocation_test_support.zig").checkAllAllocationFailures(Harness.run, .{ mode, compressed });
    };
}
