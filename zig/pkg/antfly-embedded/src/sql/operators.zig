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

//! Bounded physical SQL operators shared by local and distributed execution.
//! Inputs may borrow a scan page. Competitive top-K rows and aggregate extrema
//! take ownership before that page closes; discarded rows allocate nothing.
const std = @import("std");
pub const TupleMembership = @import("tuple_membership.zig").Index;
test {
    _ = @import("tuple_membership.zig");
}
pub const OrderedAggregate = @import("ordered_aggregate.zig").State;
test {
    _ = @import("ordered_aggregate.zig");
    _ = @import("ordered_grouped.zig");
}
const scalar = @import("scalar.zig");
const ast = @import("ast.zig");
const Allocator = std.mem.Allocator;
const Datum = scalar.Datum;
const Json = std.json.Value;
const MemoryBudget = @import("memory_budget.zig");

pub const Order = struct { descending: bool = false, nulls_first: ?bool = null };
pub const Row = struct { values: []const Datum, keys: []const Datum, ordinal: u64, normalized: ?@import("sort_key.zig").Key = null };

pub fn compareRows(left: Row, right: Row, orders: []const Order) !std.math.Order {
    if (left.keys.len != orders.len or right.keys.len != orders.len) return error.InvalidSqlBackendResponse;
    if (left.normalized) |l| if (right.normalized) |r| if (@import("sort_key.zig").compare(l, r)) |order| return if (order == .eq) std.math.order(left.ordinal, right.ordinal) else order;
    for (left.keys, right.keys, orders) |a, b, order| {
        if (a.sql_null or b.sql_null) {
            if (a.sql_null and b.sql_null) continue;
            const nulls_first = order.nulls_first orelse order.descending;
            return if (a.sql_null == nulls_first) .lt else .gt;
        }
        const comparison = try scalar.compareDatums(a, b);
        if (comparison != .eq) return if (order.descending) comparison.invert() else comparison;
    }
    return std.math.order(left.ordinal, right.ordinal);
}

// Stable owner: column stores and lazy caches must never borrow a movable
// heap-entry arena. Its child budget measures all retained vector capacity.
const NativeRow = struct {
    budget: MemoryBudget,
    store: @import("typed_store.zig").Store,
    fn create(a: Allocator, batch: @import("execution_batch.zig").Batch, pool: *@import("typed_store.zig").Store.JsonPool, lease: *@import("typed_store.zig").Store.JsonLease) !*NativeRow {
        const self = try a.create(NativeRow);
        self.budget = .{ .backing = a, .limit = std.math.maxInt(usize) };
        self.store = .init(self.budget.allocator());
        self.store.json_pool = pool;
        errdefer {
            self.store.deinit();
            a.destroy(self);
        }
        lease.forgetOwned();
        try self.store.appendBatchLeased(batch, lease);
        return self;
    }
    fn deinit(self: *NativeRow) void {
        const a = self.budget.backing;
        self.store.deinit();
        std.debug.assert(self.budget.live == 0);
        a.destroy(self);
    }
};
const OwnedRow = struct {
    arena: std.heap.ArenaAllocator,
    row: Row,
    native: ?*NativeRow = null,

    fn cloneInto(arena: *std.heap.ArenaAllocator, row: Row) !Row {
        const owned = arena.allocator();
        const values = try owned.alloc(Datum, row.values.len);
        const keys = try owned.alloc(Datum, row.keys.len);
        for (row.values, values) |value, *out| out.* = try cloneDatum(owned, value);
        for (row.keys, keys) |value, *out| out.* = try cloneDatum(owned, value);
        return .{ .values = values, .keys = keys, .ordinal = row.ordinal, .normalized = row.normalized };
    }

    pub fn init(alloc: Allocator, row: Row) !OwnedRow {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const cloned = try cloneInto(&arena, row);
        return .{ .arena = arena, .row = cloned };
    }

    pub fn deinit(self: *OwnedRow) void {
        if (self.native) |owner| owner.deinit();
        self.arena.deinit();
    }

    fn allocatedBytes(self: OwnedRow) usize {
        return arenaBytes(&self.arena) +| if (self.native) |owner| owner.budget.live +| @sizeOf(NativeRow) else @as(usize, 0);
    }
};

fn arenaBytes(arena: *const std.heap.ArenaAllocator) usize {
    var bytes = arena.queryCapacity();
    inline for (.{ arena.state.used_list, arena.state.free_list }) |head| {
        var next = head;
        while (next) |node| : (next = node.next) bytes += @sizeOf(@TypeOf(node.*));
    }
    return bytes;
}

/// Retains at most k rows. The heap root is the worst admitted row, so a
/// noncompetitive input performs comparisons only, with no allocation/copy.
pub const TopK = struct {
    alloc: Allocator,
    entries: []OwnedRow,
    count: usize = 0,
    orders: []Order,
    max_bytes: usize,
    retained_bytes: usize,
    comparisons: usize = 0,
    copied_rows: usize = 0,
    finished: bool = false,
    released: usize = 0,
    capacity: usize = 0,
    spill_manager: ?*@import("spill.zig").Manager = null,
    external: ?*@import("spill.zig").Sort = null,
    // Replacement exchanges a candidate with the displaced root. Retaining
    // that root's arena avoids one allocator round-trip per competitive row.
    scratch: ?std.heap.ArenaAllocator = null,
    json_pool: ?*@import("typed_store.zig").Store.JsonPool = null,
    pool_bytes: usize = 0,
    scalar_mutex: std.atomic.Mutex = .unlocked,
    fn syncPool(self: *TopK) void {
        const bytes = if (self.json_pool) |pool| pool.budget.live +| @sizeOf(@TypeOf(pool.*)) else @as(usize, 0);
        self.retained_bytes = self.retained_bytes - self.pool_bytes +| bytes;
        self.pool_bytes = bytes;
    }

    fn displacedBytes(self: *const TopK) usize {
        if (self.count != self.capacity or self.count == 0) return 0;
        const entry = self.entries[0];
        return entry.allocatedBytes() +| if (entry.native) |native| native.store.exclusiveJsonBytes() else @as(usize, 0);
    }
    pub fn init(alloc: Allocator, k: usize, orders: []const Order, max_bytes: usize) !TopK {
        const bytes = std.math.mul(usize, orders.len, @sizeOf(Order)) catch return error.SqlProgramLimitExceeded;
        if (bytes > max_bytes or orders.len > 256) return error.SqlProgramLimitExceeded;
        const owned_orders = try alloc.dupe(Order, orders);
        return .{ .alloc = alloc, .entries = &.{}, .orders = owned_orders, .max_bytes = max_bytes, .retained_bytes = bytes, .capacity = k };
    }

    pub fn initWithSpill(alloc: Allocator, k: usize, orders: []const Order, max_bytes: usize, manager: ?*@import("spill.zig").Manager) !TopK {
        var top = try init(alloc, k, orders, if (manager != null) max_bytes - max_bytes / 4 else max_bytes);
        top.spill_manager = manager;
        errdefer top.deinit();
        // Reserve the bounded external-sort path before a large nested sort
        // competes with its parent for the shared statement allocator.
        if (manager != null and k > (max_bytes / 4) / @sizeOf(OwnedRow)) try top.startSpill();
        return top;
    }

    // LIMIT/scan bounds describe maximum retained rows, not actual cardinality.
    // Grow only for rows observed, charging both live arrays during a copy.
    fn growEntries(self: *TopK) !void {
        if (self.count < self.entries.len or self.count == self.capacity) return;
        const old_len = self.entries.len;
        var wanted = @min(self.capacity, @max(@as(usize, 8), old_len *| 2));
        const available = (self.max_bytes -| self.retained_bytes) / @sizeOf(OwnedRow);
        wanted = @min(wanted, old_len +| available);
        if (wanted <= old_len) return error.SqlProgramLimitExceeded;
        if (!self.alloc.resize(self.entries, wanted)) {
            wanted = @min(wanted, available);
            if (wanted <= old_len) return error.SqlProgramLimitExceeded;
            const entries = try self.alloc.alloc(OwnedRow, wanted);
            @memcpy(entries[0..self.count], self.entries[0..self.count]);
            self.alloc.free(self.entries);
            self.entries = entries;
        } else self.entries.len = wanted;
        self.retained_bytes += (wanted - old_len) * @sizeOf(OwnedRow);
    }
    fn startSpill(self: *TopK) !void {
        const manager = self.spill_manager orelse return error.SqlProgramLimitExceeded;
        const sort = try self.alloc.create(@import("spill.zig").Sort);
        errdefer self.alloc.destroy(sort);
        sort.* = @import("spill.zig").Sort.init(self.alloc, manager, self.orders, self.max_bytes / 4);
        errdefer sort.deinit();
        while (self.released < self.count) {
            const entry = &self.entries[self.released];
            if (entry.native) |native| {
                const begin = self.released;
                var end = begin + 1;
                while (end < self.count and end - begin < 64) : (end += 1) {
                    const next = self.entries[end].native orelse break;
                    if (next.store.columns.len != native.store.columns.len) break;
                }
                const View = struct {
                    entries: []const OwnedRow,
                    fn cell(raw: *anyopaque, a: Allocator, row: usize, column: usize) anyerror!Datum {
                        const self_: *@This() = @ptrCast(@alignCast(raw));
                        return self_.entries[row].native.?.store.cell(a, 0, column);
                    }
                    fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                        const self_: *@This() = @ptrCast(@alignCast(raw));
                        return self_.entries[row].native.?.store.frozenJson(0, column);
                    }
                    fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
                        const self_: *@This() = @ptrCast(@alignCast(raw));
                        return self_.entries[row].native.?.store.encodedJson(0, column);
                    }
                };
                var view: View = .{ .entries = self.entries[begin..end] };
                var keys: [64][]const Datum = undefined;
                var ordinals: [64]u64 = undefined;
                for (view.entries, keys[0 .. end - begin], ordinals[0 .. end - begin]) |owned, *key, *ordinal| {
                    key.* = owned.row.keys;
                    ordinal.* = owned.row.ordinal;
                }
                try sort.addBatch(.{ .reader = .{ .ptr = &view, .read = View.cell, .read_frozen = View.frozen, .read_json = View.json, .count = end - begin, .width = native.store.columns.len } }, keys[0 .. end - begin], ordinals[0 .. end - begin]);
                while (self.released < end) : (self.released += 1) {
                    const retired = &self.entries[self.released];
                    self.retained_bytes -= retired.allocatedBytes();
                    retired.deinit();
                    self.syncPool();
                }
            } else {
                try sort.add(entry.row);
                self.retained_bytes -= entry.allocatedBytes();
                entry.deinit();
                self.released += 1;
            }
        }
        self.released = 0;
        if (self.scratch) |*arena| arena.deinit();
        self.scratch = null;
        self.alloc.free(self.entries);
        self.entries = &.{};
        self.count = 0;
        if (self.json_pool) |pool| pool.deinit();
        self.json_pool = null;
        self.pool_bytes = 0;
        self.retained_bytes = 0;
        self.external = sort;
    }
    /// Return only the requested output window; spilled OFFSET rows are read
    /// and discarded through one scratch arena rather than retained in memory.
    /// Consume external results one row at a time; offsets never allocate rows.
    pub fn drain(self: *TopK, alloc: Allocator, offset: usize, limit: usize, implicit: bool, sink: anytype) !void {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        if (self.external) |sort| {
            self.finished = true;
            var index: usize = 0;
            var emitted: usize = 0;
            while (index < self.capacity) : (index += 1) {
                _ = arena.reset(.free_all);
                if (index < offset) {
                    if (!try sort.skip()) break;
                    continue;
                }
                if (emitted == limit) {
                    if (implicit and try sort.skip()) return error.SqlResultTooLarge;
                    break;
                }
                const row = (try sort.next(arena.allocator())) orelse break;
                try sink.append(sink.ptr, row.values);
                emitted += 1;
            }
        } else {
            const rows = try self.finishNative(arena.allocator());
            const begin = @min(offset, rows.len);
            if (implicit and rows.len - begin > limit) return error.SqlResultTooLarge;
            for (begin..begin + @min(limit, rows.len - begin)) |index| try sink.append(sink.ptr, (try self.scalarRow(index)).values);
        }
    }
    pub fn finishPage(self: *TopK, alloc: Allocator, offset: usize, limit: usize) ![]const Row {
        if (self.external) |sort| {
            self.finished = true;
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            for (0..@min(offset, self.capacity)) |_| {
                _ = arena.reset(.free_all);
                if (!try sort.skip()) return &.{};
            }
            var rows: std.ArrayList(Row) = .empty;
            for (0..@min(limit, self.capacity -| offset)) |_| {
                const row = try sort.next(alloc) orelse break;
                try rows.append(alloc, row);
            }
            return rows.toOwnedSlice(alloc);
        }
        const rows = try self.finishNative(alloc);
        const begin = @min(offset, rows.len);
        for (0..begin) |index| self.releaseFinishedRow(index);
        const page = @constCast(rows[begin..][0..@min(limit, rows.len - begin)]);
        for (page, begin..) |*row, index| row.* = try self.scalarRow(index);
        return page;
    }
    pub fn deinit(self: *TopK) void {
        if (self.external) |sort| {
            sort.deinit();
            self.alloc.destroy(sort);
        }
        for (self.entries[self.released..self.count]) |*entry| entry.deinit();
        if (self.scratch) |*scratch| scratch.deinit();
        self.alloc.free(self.entries);
        self.alloc.free(self.orders);
        if (self.json_pool) |pool| pool.deinit();
        self.* = undefined;
    }

    fn compare(self: *TopK, a: Row, b: Row) !std.math.Order {
        self.comparisons += 1;
        return compareRows(a, b, self.orders);
    }

    pub fn add(self: *TopK, input: Row) !void {
        var row = input;
        row.normalized = @import("sort_key.zig").encode(row.keys, self.orders);
        if (self.external) |sort| return sort.add(row);
        self.addInMemory(row) catch |err| return self.spillCandidate(row, err);
    }
    fn spillCandidate(self: *TopK, row: Row, err: anyerror) !void {
        if (self.spill_manager == null or (err != error.SqlProgramLimitExceeded and err != error.OutOfMemory)) return err;
        try self.startSpill();
        try self.external.?.add(row);
    }
    pub fn addBatch(self: *TopK, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, ordinals: []const u64) !void {
        if (batch.len() != keys.len or keys.len != ordinals.len or self.finished) return error.InvalidSqlBackendResponse;
        var lease = @import("typed_store.zig").Store.JsonLease.init(self.alloc);
        defer lease.deinit();
        var index: usize = 0;
        while (index < batch.len()) : (index += 1) {
            if (index != 0 and index % 64 == 0) {
                lease.deinit();
                lease = .init(self.alloc);
            }
            if (self.external) |sort| {
                const selection = try a.alloc(usize, batch.len() - index);
                for (selection, index..) |*position, i| position.* = i;
                return sort.addBatch(try batch.select(a, selection), keys[index..], ordinals[index..]);
            }
            var row: Row = .{ .values = &.{}, .keys = keys[index], .ordinal = ordinals[index], .normalized = @import("sort_key.zig").encode(keys[index], self.orders) };
            if (row.keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
            for (row.keys) |key| if (!key.sql_null) {
                _ = try scalar.compareDatums(key, key);
            };
            if (self.capacity == 0 or (self.count == self.capacity and (try self.compare(row, self.entries[0].row)) != .lt)) continue;
            // Reject losers before reading payload cells. A small LIMIT keeps
            // its bounded heap rather than sorting the complete input on disk.
            var native_payload = false;
            for (0..batch.width()) |column| {
                if (try batch.frozenJson(index, column) != null or try batch.encodedJson(index, column) != null) {
                    native_payload = true;
                    break;
                }
            }
            if (native_payload) {
                const selected = try batch.select(a, &.{index});
                if (self.json_pool == null) self.json_pool = try @import("typed_store.zig").Store.JsonPool.create(self.alloc);
                self.syncPool();
                var probe = @import("typed_store.zig").Store.init(self.alloc);
                probe.json_pool = self.json_pool;
                lease.forgetOwned();
                var estimate: usize = @sizeOf(NativeRow) +| @import("typed_store.zig").columnMetadataBytes(batch.width()) +| 2048 +| try probe.batchAppendBytesLeased(a, selected, std.math.maxInt(usize), &lease);
                for (row.keys) |key| estimate +|= try datumBytes(key);
                const room = MemoryBudget.headroom(self.alloc) orelse std.math.maxInt(usize);
                const displaced = self.displacedBytes();
                // The old winner remains live until publication; parent
                // headroom covers both, while the heap cap covers survivors.
                if (estimate > self.max_bytes -| (self.retained_bytes -| displaced) or estimate +| estimate / 2 +| 4096 > room) {
                    if (self.spill_manager == null) return error.SqlProgramLimitExceeded;
                    try self.startSpill();
                    try self.external.?.addBatch(selected, keys[index..][0..1], ordinals[index..][0..1]);
                    continue;
                }
                self.retainNative(selected, row, &lease) catch |err| {
                    if (err != error.SqlProgramLimitExceeded or self.spill_manager == null) return err;
                    try self.startSpill();
                    try self.external.?.addBatch(selected, keys[index..][0..1], ordinals[index..][0..1]);
                };
            } else {
                row.values = try batch.row(a, index);
                self.retainInMemory(row) catch |err| try self.spillCandidate(row, err);
            }
        }
    }
    fn addInMemory(self: *TopK, row: Row) !void {
        if (self.finished) return error.InvalidSqlBackendResponse;
        if (row.keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
        if (self.capacity == 0) return;
        // Validate values before mutation, including when there is no prior
        // row to compare; malformed input cannot poison a partly sorted heap.
        for (row.keys) |key| if (!key.sql_null) {
            _ = try scalar.compareDatums(key, key);
        };
        if (self.count == self.capacity and (try self.compare(row, self.entries[0].row)) != .lt) return;
        return self.retainInMemory(row);
    }
    fn retainNative(self: *TopK, batch: @import("execution_batch.zig").Batch, row: Row, lease: *@import("typed_store.zig").Store.JsonLease) !void {
        defer self.syncPool();
        try self.growEntries();
        var owned: OwnedRow = .{ .arena = .init(self.alloc), .row = row };
        errdefer owned.deinit();
        const a = owned.arena.allocator();
        const keys = try a.alloc(Datum, row.keys.len);
        for (keys, row.keys) |*out, key| out.* = try cloneDatum(a, key);
        owned.row.keys = keys;
        owned.native = try NativeRow.create(self.alloc, batch, self.json_pool.?, lease);
        self.syncPool();
        try self.publish(owned);
    }
    pub fn nativeStore(self: *const TopK, index: usize) ?*const @import("typed_store.zig").Store {
        return if (self.entries[index].native) |owner| &owner.store else null;
    }
    pub fn scalarRow(self: *TopK, index: usize) !Row {
        while (!self.scalar_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.scalar_mutex.unlock();
        const entry = &self.entries[index];
        if (entry.native) |native| if (entry.row.values.len == 0) {
            const before = entry.allocatedBytes();
            defer {
                self.retained_bytes +|= entry.allocatedBytes() -| before;
                self.syncPool();
            }
            entry.row.values = try native.store.row(entry.arena.allocator(), 0);
        };
        return entry.row;
    }
    fn retainInMemory(self: *TopK, row: Row) !void {
        try self.growEntries();
        var estimate: usize = 0;
        for (row.values) |value| estimate = std.math.add(usize, estimate, try datumBytes(value)) catch return error.SqlProgramLimitExceeded;
        for (row.keys) |value| estimate = std.math.add(usize, estimate, try datumBytes(value)) catch return error.SqlProgramLimitExceeded;
        if (estimate > self.max_bytes -| self.retained_bytes) return error.SqlProgramLimitExceeded;
        var candidate_arena = self.scratch orelse std.heap.ArenaAllocator.init(self.alloc);
        self.scratch = null;
        errdefer {
            _ = candidate_arena.reset(.retain_capacity);
            if (self.retained_bytes +| arenaBytes(&candidate_arena) <= self.max_bytes) {
                self.scratch = candidate_arena;
            } else candidate_arena.deinit();
        }
        const cloned = try OwnedRow.cloneInto(&candidate_arena, row);
        const owned: OwnedRow = .{ .arena = candidate_arena, .row = cloned };
        return self.publish(owned);
    }
    fn publish(self: *TopK, owned: OwnedRow) !void {
        const bytes = owned.allocatedBytes();
        const displaced_bytes = self.displacedBytes();
        if (bytes > self.max_bytes -| (self.retained_bytes -| displaced_bytes)) return error.SqlProgramLimitExceeded;
        self.copied_rows += 1;
        if (self.count < self.entries.len) {
            var index = self.count;
            // Determine the insertion path before changing ownership. Scalar
            // type errors therefore leave the existing heap intact.
            while (index > 0) {
                const parent = (index - 1) / 2;
                if ((try self.compare(owned.row, self.entries[parent].row)) != .gt) break;
                index = parent;
            }
            var destination = self.count;
            while (destination > index) {
                const parent = (destination - 1) / 2;
                self.entries[destination] = self.entries[parent];
                destination = parent;
            }
            self.entries[index] = owned;
            self.count += 1;
            self.retained_bytes += bytes;
        } else {
            // Compute a full bounded path first. Replacement then cannot fail
            // after releasing the old root's owned page data.
            var path: [@bitSizeOf(usize)]usize = undefined;
            var path_count: usize = 0;
            var index: usize = 0;
            while (index * 2 + 1 < self.count) {
                var child = index * 2 + 1;
                if (child + 1 < self.count and (try self.compare(self.entries[child + 1].row, self.entries[child].row)) == .gt) child += 1;
                if ((try self.compare(self.entries[child].row, owned.row)) != .gt) break;
                path[path_count] = child;
                path_count += 1;
                index = child;
            }
            self.retained_bytes -= self.entries[0].allocatedBytes();
            if (self.entries[0].native) |native| native.deinit();
            self.syncPool();
            var displaced = self.entries[0].arena;
            index = 0;
            for (path[0..path_count]) |child| {
                self.entries[index] = self.entries[child];
                index = child;
            }
            self.entries[index] = owned;
            self.retained_bytes += bytes;
            _ = displaced.reset(.retain_capacity);
            if (self.retained_bytes +| arenaBytes(&displaced) <= self.max_bytes) {
                if (self.scratch) |*previous| previous.deinit();
                self.scratch = displaced;
            } else displaced.deinit();
        }
    }

    /// Once sorted, result encoding consumes each row and promptly releases
    /// its source arena. Peak memory need not retain two complete result sets.
    pub fn releaseFinishedRow(self: *TopK, index: usize) void {
        if (self.external != null) return;
        std.debug.assert(self.finished and index == self.released and index < self.count);
        self.retained_bytes -= self.entries[index].allocatedBytes();
        self.entries[index].deinit();
        self.syncPool();
        self.released += 1;
    }

    /// Returns rows in requested order, with original ordinal as a stable tie
    /// breaker. Returned cell data remains owned by this operator until deinit.
    pub fn finish(self: *TopK, alloc: Allocator) ![]const Row {
        const rows = try self.finishNative(alloc);
        for (@constCast(rows), 0..) |*row, index| row.* = try self.scalarRow(index);
        return rows;
    }
    /// Establish rank order without allocating a duplicate row-header vector.
    /// Span consumers keep the heap owner alive and access entries directly.
    pub fn finishOrder(self: *TopK) !void {
        if (self.released != 0) return error.InvalidSqlBackendResponse;
        if (!self.finished) {
            var end = self.count;
            while (end > 1) {
                end -= 1;
                std.mem.swap(OwnedRow, &self.entries[0], &self.entries[end]);
                var index: usize = 0;
                while (index * 2 + 1 < end) {
                    var child = index * 2 + 1;
                    if (child + 1 < end and (try self.compare(self.entries[child + 1].row, self.entries[child].row)) == .gt) child += 1;
                    if ((try self.compare(self.entries[child].row, self.entries[index].row)) != .gt) break;
                    std.mem.swap(OwnedRow, &self.entries[child], &self.entries[index]);
                    index = child;
                }
            }
            self.finished = true;
        }
    }
    pub fn finishNative(self: *TopK, alloc: Allocator) ![]const Row {
        try self.finishOrder();
        const rows = try alloc.alloc(Row, self.count);
        for (self.entries[0..self.count], rows) |entry, *row| row.* = entry.row;
        return rows;
    }
};

pub const Aggregate = struct {
    pub const Kind = enum { count, sum, avg, min, max, bool_and, bool_or, pattern_set };
    alloc: Allocator,
    kind: Kind,
    input_type: ?ast.ColumnType,
    input_element: ?@import("array_value.zig").ElementType = null,
    numeric: ?*@import("numeric_aggregate.zig").Reducer = null,
    count: u64 = 0,
    integer_sum: i128 = 0,
    number_sum: f64 = 0,
    compensation: f64 = 0,
    mean: f64 = 0,
    boolean: bool = false,
    selected: ?OwnedRow = null,
    patterns: ?*PatternState = null,
    distinct: bool = false,
    distinct_heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    distinct_values: std.ArrayList(struct { row: OwnedRow, next: ?usize }) = .empty,
    const PatternState = struct { values: std.json.Array, has_null: bool = false };

    pub fn validate(kind: Kind, input_type: ?ast.ColumnType) !void {
        if ((kind == .sum or kind == .avg) and input_type != null and input_type != .integer and input_type != .number) return error.SqlTypeMismatch;
        if ((kind == .bool_and or kind == .bool_or) and input_type != null and input_type != .boolean) return error.SqlTypeMismatch;
        if (kind == .pattern_set and input_type != null and input_type != .string) return error.SqlTypeMismatch;
    }

    pub fn init(alloc: Allocator, kind: Kind, input_type: ?ast.ColumnType) !Aggregate {
        return initTyped(alloc, kind, input_type, null);
    }

    pub fn initTyped(alloc: Allocator, kind: Kind, input_type: ?ast.ColumnType, input_element: ?@import("array_value.zig").ElementType) !Aggregate {
        try validate(kind, input_type);
        var result: Aggregate = .{ .alloc = alloc, .kind = kind, .input_type = input_type, .input_element = input_element, .boolean = kind == .bool_and };
        if (input_element == .numeric and (kind == .sum or kind == .avg)) {
            if (input_type != .number) return error.SqlTypeMismatch;
            const reducer = try alloc.create(@import("numeric_aggregate.zig").Reducer);
            reducer.* = .{ .alloc = alloc };
            result.numeric = reducer;
        }
        if (kind == .pattern_set) {
            const state = try alloc.create(PatternState);
            state.* = .{ .values = .init(alloc) };
            result.patterns = state;
        }
        return result;
    }

    pub fn deinit(self: *Aggregate) void {
        if (self.numeric) |reducer| {
            reducer.deinit();
            self.alloc.destroy(reducer);
        }
        if (self.selected) |*value| value.deinit();
        if (self.patterns) |state| {
            state.values.deinit();
            self.alloc.destroy(state);
        }
        for (self.distinct_values.items) |*entry| entry.row.deinit();
        self.distinct_values.deinit(self.alloc);
        self.distinct_heads.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn update(self: *Aggregate, value: Datum) !void {
        if (value.sql_null) {
            if (self.patterns) |state| {
                if (!state.has_null) {
                    try state.values.append(.null);
                    state.has_null = true;
                }
            }
            return;
        }
        if (self.kind == .pattern_set and value.value != .string) return error.SqlTypeMismatch;
        if (self.distinct) {
            const hash = try scalar.semanticHashDatum(value);
            var slot = self.distinct_heads.get(hash);
            while (slot) |index| {
                const entry = self.distinct_values.items[index];
                if ((try scalar.compareDatums(value, entry.row.row.values[0])) == .eq) return;
                slot = entry.next;
            }
            try self.distinct_values.ensureUnusedCapacity(self.alloc, 1);
            try self.distinct_heads.ensureUnusedCapacity(self.alloc, 1);
            const owned = try OwnedRow.init(self.alloc, .{ .values = &.{value}, .keys = &.{}, .ordinal = 0 });
            const index = self.distinct_values.items.len;
            self.distinct_values.appendAssumeCapacity(.{ .row = owned, .next = self.distinct_heads.get(hash) });
            self.distinct_heads.putAssumeCapacity(hash, index);
        }
        if (self.count == std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        switch (self.kind) {
            .count => {},
            .sum, .avg => if (self.numeric) |reducer| {
                try reducer.add((value.numeric orelse return error.SqlTypeMismatch).*);
            } else {
                const number: f64 = switch (value.value) {
                    .integer => |v| @floatFromInt(v),
                    .float => |v| v,
                    else => return error.SqlTypeMismatch,
                };
                if (self.kind == .sum) {
                    if (self.input_type == .integer) {
                        if (value.value != .integer) return error.SqlTypeMismatch;
                        self.integer_sum = std.math.add(i128, self.integer_sum, value.value.integer) catch return error.SqlNumericOutOfRange;
                    } else if (self.input_element == .float32) {
                        self.number_sum = try addRealSum(self.number_sum, number);
                    } else {
                        try addFloatSum(&self.number_sum, &self.compensation, number);
                    }
                } else {
                    const n: f64 = @floatFromInt(self.count + 1);
                    const delta = number - self.mean;
                    if (!std.math.isFinite(self.mean) or !std.math.isFinite(number)) {
                        self.mean += number;
                    } else {
                        self.mean = if (std.math.isFinite(delta)) self.mean + delta / n else self.mean * ((n - 1) / n) + number / n;
                        if (!std.math.isFinite(self.mean)) return error.SqlNumericOutOfRange;
                    }
                }
            },
            .bool_and, .bool_or => {
                if (value.value != .bool) return error.SqlTypeMismatch;
                self.boolean = if (self.kind == .bool_and) self.boolean and value.value.bool else self.boolean or value.value.bool;
            },
            .min, .max => {
                if (self.selected) |current| {
                    const order = try scalar.compareDatums(value, current.row.values[0]);
                    if (order != (if (self.kind == .min) std.math.Order.lt else .gt)) {
                        self.count += 1;
                        return;
                    }
                }
                const selected = try OwnedRow.init(self.alloc, .{ .values = &.{value}, .keys = &.{}, .ordinal = 0 });
                if (self.selected) |*old| old.deinit();
                self.selected = selected;
            },
            .pattern_set => {
                // DISTINCT owns the string; the JSON array borrows that stable
                // copy until the grouped operator releases both together.
                if (!self.distinct) return error.InvalidSqlBackendResponse;
                try self.patterns.?.values.append(self.distinct_values.items[self.distinct_values.items.len - 1].row.row.values[0].value);
            },
        }
        self.count += 1;
    }

    /// PostgreSQL float4 SUM rounds each transition and partial combination.
    /// Finite inputs must not overflow the float4 transition state.
    pub fn addRealSum(left: f64, right: f64) !f64 {
        const a: f32 = @floatCast(left);
        const b: f32 = @floatCast(right);
        const sum: f32 = a + b;
        if (std.math.isFinite(left) and std.math.isFinite(right) and !std.math.isFinite(sum)) return error.SqlNumericOutOfRange;
        return sum;
    }

    /// Compensation applies only to finite transitions. Special inputs retain
    /// IEEE sums; finite inputs that overflow still raise a range error.
    pub fn addFloatSum(sum: *f64, compensation: *f64, value: f64) !void {
        if (!std.math.isFinite(sum.*) or !std.math.isFinite(value)) {
            sum.* += value;
            compensation.* = 0;
            return;
        }
        const adjusted = value - compensation.*;
        const total = sum.* + adjusted;
        if (!std.math.isFinite(total)) return error.SqlNumericOutOfRange;
        compensation.* = (total - sum.*) - adjusted;
        sum.* = total;
    }

    pub fn finish(self: *const Aggregate) !Datum {
        if (self.kind == .count) return Datum.json(.{ .integer = std.math.cast(i64, self.count) orelse return error.SqlNumericOutOfRange });
        if (self.kind == .pattern_set) return Datum.json(.{ .array = self.patterns.?.values });
        if (self.count == 0) return .{};
        if (self.numeric) |reducer| {
            if (reducer.state.count != self.count) return error.InvalidSqlBackendResponse;
            return Datum.typedNumeric((try reducer.finish(self.kind == .avg)) orelse return error.InvalidSqlBackendResponse);
        }
        return switch (self.kind) {
            .count => unreachable,
            .sum => Datum.json(if (self.input_type == .integer) .{ .integer = std.math.cast(i64, self.integer_sum) orelse return error.SqlNumericOutOfRange } else .{ .float = self.number_sum }),
            .avg => Datum.json(.{ .float = self.mean }),
            .bool_and, .bool_or => Datum.json(.{ .bool = self.boolean }),
            .min, .max => self.selected.?.row.values[0],
            .pattern_set => unreachable,
        };
    }
};

pub const AggregateSpec = struct { kind: Aggregate.Kind, input_type: ?ast.ColumnType = null, input_element: ?@import("array_value.zig").ElementType = null, distinct: bool = false };

/// Associative exact states may use worker and hash-partition reduction.
/// This is not primitive SIMD eligibility: NUMERIC has its own flat lane.
pub fn exactMergeable(spec: AggregateSpec) bool {
    if (spec.distinct) return false;
    // These extrema have a unique representation for equal values. Floating
    // signed zero, heterogeneous numbers and dynamic types retain source order.
    if (spec.kind == .min or spec.kind == .max) return switch (spec.input_type orelse .json) {
        .integer => spec.input_element == null or spec.input_element == .int16 or spec.input_element == .int32 or spec.input_element == .int64,
        .boolean => spec.input_element == null or spec.input_element == .boolean,
        .string => spec.input_element == null or spec.input_element == .text,
        else => false,
    };
    return spec.kind == .count or spec.kind == .bool_and or spec.kind == .bool_or or
        (spec.kind == .sum and spec.input_type == .integer) or
        ((spec.kind == .sum or spec.kind == .avg) and spec.input_type == .number and spec.input_element == .numeric);
}
pub const GroupBatch = struct {
    keys: @import("execution_batch.zig").Batch,
    aggregates: @import("execution_batch.zig").Batch,
    ordinals: []const u64,
};
pub const GroupResult = struct {
    /// Compact in-process partials materialize one reducer at a time. The
    /// reader and its packed inputs live until the provider's next page;
    /// allocations made by read belong to the consumer's per-partial scratch.
    pub const StateReader = struct {
        ptr: *const anyopaque,
        count: usize,
        read: *const fn (*const anyopaque, Allocator, usize) anyerror!Aggregate,
    };
    /// In-process partials borrow typed reducers from the page arena. Durable
    /// and backend interchange continues to use versioned aggregate cells.
    typed_states: ?[]const Aggregate = null,
    state_reader: ?StateReader = null,
    keys: []const Datum,
    aggregates: []const Datum,
    ordinal: u64,
    /// Partial providers may supply selected reducer slots. Final results and
    /// dense partials leave this null; slot identities never depend on aliases.
    aggregate_slots: ?[]const u16 = null,
};

/// Build-side ownership for a streaming hash join. Probe rows remain borrowed
/// from one native page; only the build relation occupies retained memory.
pub const HashJoin = struct {
    const PayloadCache = struct { block: *@import("spill.zig").Sequential.OwnedBlock, offset: u64 };

    pub const Limits = struct { rows: usize = 100000, bytes: usize = 8 * 1024 * 1024, spill: ?*@import("spill.zig").Manager = null };
    const Entry = struct { next: ?usize, width: usize, matched: bool = false };
    pub const Match = struct {
        index: usize,
        values: []const Datum = &.{},
        keys: []const Datum = &.{},
        transient: bool = false,
        owner: ?*HashJoin = null,
        native: ?NativeMatch = null,
        pub const NativeMatch = struct {
            join: *HashJoin,
            offset: u64,
            width: usize,
            row: usize = 0,
            pub fn block(self: @This()) !*@import("spill.zig").Sequential.OwnedBlock {
                const result = try self.join.diskBlock(self.offset);
                if (result.width() != self.width or self.row >= result.count()) return error.InvalidSqlSpill;
                return result;
            }
        };

        /// Reconstruct row-boundary values from stable typed columns. Disk
        /// candidates are copied before the next probe reuses decode storage.
        pub fn materializeKeys(self: Match, alloc: Allocator) ![]const Datum {
            return if (self.owner) |owner| owner.key_columns.row(alloc, self.index) else self.keys;
        }
        /// Keep build payloads encoded when transferring into another operator.
        pub fn valuesBatch(self: Match, alloc: Allocator) !@import("execution_batch.zig").Batch {
            if (self.native) |native| {
                const source = try alloc.create(@import("execution_batch.zig").Batch);
                source.* = try (try native.block()).batch(alloc, false);
                return source.select(alloc, try alloc.dupe(usize, &.{native.row}));
            }
            if (self.owner) |owner| if (owner.entries.items[self.index].width == owner.value_columns.columns.len)
                return .{ .retained = .{ .store = &owner.value_columns, .begin = self.index, .count = 1 } };
            const rows = try alloc.alloc([]const Datum, 1);
            rows[0] = try self.materializeValues(alloc);
            return .{ .rows = rows };
        }
        pub fn materializeValues(self: Match, alloc: Allocator) ![]const Datum {
            if (self.native) |native| {
                const block = try native.block();
                const values = try alloc.alloc(Datum, block.width());
                for (values, 0..) |*value, column| value.* = try cloneDatum(alloc, try block.cell(native.row, column));
                return values;
            }
            if (self.owner) |owner| return owner.value_columns.rowWidth(alloc, self.index, owner.entries.items[self.index].width);
            const result = try alloc.dupe(Datum, self.values);
            if (self.transient) {
                for (result) |*value| value.* = try cloneDatum(alloc, value.*);
            }
            return result;
        }
    };
    pub const Probe = struct {
        owner: *HashJoin,
        keys: []const Datum,
        cursor: ?usize,
        first: ?Match = null,

        pub fn next(self: *Probe) !?Match {
            if (self.first) |match| {
                self.first = null;
                return match;
            }
            if (self.owner.disk) |*file| while (self.cursor) |index| {
                const decoded = try self.owner.readDiskKey(file, index);
                self.cursor = if (decoded.next == @import("spill.zig").none) null else @intCast(decoded.next);
                self.owner.probes += 1;
                if (decoded.row.keys.len != self.keys.len) return error.InvalidSqlSpill;
                var equal = true;
                for (decoded.row.keys, self.keys) |left, right| if (left.sql_null or right.sql_null or (try scalar.compareDatums(left, right)) != .eq) {
                    equal = false;
                    break;
                };
                if (equal) return try self.owner.diskMatch(index, decoded);
            };
            if (self.owner.disk != null) return null;
            while (self.cursor) |index| {
                const entry = &self.owner.entries.items[index];
                self.cursor = entry.next;
                self.owner.probes += 1;
                if (try self.owner.key_columns.equal(self.owner.backing, index, self.keys, false)) return .{ .index = index, .owner = self.owner };
            }
            return null;
        }
    };

    backing: Allocator,
    budget: MemoryBudget,
    limits: Limits,
    // Primitive payload/key columns retain typed vectors and packed validity;
    // only variable-width bytes and native JSON need owned arena payloads.
    value_columns: @import("typed_store.zig").Store,
    key_columns: @import("typed_store.zig").Store,
    entries: std.ArrayList(Entry) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    key_count: ?usize = null,
    probes: u64 = 0,
    sealed: bool = false,

    disk: ?@import("spill.zig").File = null,
    disk_heads: []u64 = &.{},
    disk_values: ?*@import("spill.zig").JoinPayloads = null,
    disk_payloads: [4]?PayloadCache = @splat(null),
    next_payload: usize = 0,
    payload_reads: usize = 0,
    disk_arena: std.heap.ArenaAllocator = undefined,
    row_count: usize = 0,
    fn clearPayloadCache(self: *HashJoin) void {
        for (&self.disk_payloads) |*entry| {
            if (entry.*) |cached| cached.block.release();
            entry.* = null;
        }
    }
    fn trimPayloadCache(self: *HashJoin, keep: u64) void {
        var live: usize = 0;
        for (self.disk_payloads) |entry| if (entry) |cached| {
            live +|= cached.block.retainedBytes();
        };
        for (0..self.disk_payloads.len) |step| {
            const entry = &self.disk_payloads[(self.next_payload + step) % self.disk_payloads.len];
            if (entry.*) |cached| if (cached.offset != keep and live > 64 * 1024) {
                live -|= cached.block.retainedBytes();
                cached.block.release();
                entry.* = null;
            };
        }
    }
    fn evictPayload(raw: *anyopaque) bool {
        const self: *HashJoin = @ptrCast(@alignCast(raw));
        // Build-side dedup entries are colder than decoded probe pages.
        if (self.disk_values) |payloads| if (payloads.evictOne()) return true;
        for (0..self.disk_payloads.len) |step| {
            const entry = &self.disk_payloads[(self.next_payload + step) % self.disk_payloads.len];
            if (entry.*) |cached| {
                cached.block.release();
                entry.* = null;
                return true;
            }
        }
        return false;
    }
    fn diskBlock(self: *HashJoin, offset: u64) !*@import("spill.zig").Sequential.OwnedBlock {
        self.trimPayloadCache(offset);
        for (self.disk_payloads) |entry| if (entry) |cached| {
            if (cached.offset == offset) return cached.block;
        };
        // Retain at most four small pages (64 KiB including lazy caches), or one
        // oversized page. Output leases may pin evicted blocks independently.
        const payloads = self.disk_values orelse return error.InvalidSqlSpill;
        var pressure: @import("spill.zig").CachePressure = .{ .manager = payloads.file.manager, .context = self, .evict = evictPayload };
        const block = try payloads.readWithPressure(offset, &pressure);
        const bytes = block.retainedBytes();
        var live: usize = bytes;
        for (self.disk_payloads) |entry| if (entry) |cached| {
            live +|= cached.block.retainedBytes();
        };
        if (live > 64 * 1024) self.clearPayloadCache();
        const slot = self.next_payload;
        if (self.disk_payloads[slot]) |old| old.block.release();
        self.disk_payloads[slot] = .{ .block = block, .offset = offset };
        self.next_payload = (slot + 1) % self.disk_payloads.len;
        self.payload_reads += 1;
        return block;
    }
    // Spill files are ephemeral. The high ordinal bit distinguishes a native
    // payload offset from a small inline row's original build ordinal.
    const native_payload_bit: u64 = @as(u64, 1) << 63;
    fn diskMatch(self: *HashJoin, index: usize, decoded: @import("spill.zig").Decoded) !Match {
        if (decoded.row.ordinal & native_payload_bit == 0)
            return .{ .index = index, .values = decoded.row.values, .keys = decoded.row.keys, .transient = true };
        if (decoded.row.values.len != 2 or decoded.row.values[0].sql_null or decoded.row.values[0].value != .integer or decoded.row.values[1].sql_null or decoded.row.values[1].value != .integer) return error.InvalidSqlSpill;
        const width = std.math.cast(usize, decoded.row.values[0].value.integer) orelse return error.InvalidSqlSpill;
        const row = std.math.cast(usize, decoded.row.values[1].value.integer) orelse return error.InvalidSqlSpill;
        if (width > 1024 or row >= @import("spill.zig").JoinPayloads.page_rows) return error.InvalidSqlSpill;
        return .{ .index = index, .keys = decoded.row.keys, .transient = true, .native = .{ .join = self, .offset = decoded.row.ordinal & ~native_payload_bit, .width = width, .row = row } };
    }
    /// Tiny scalar rows retain the single-record path. Native JSON and larger
    /// payloads split from the key chain, preserving encoded source capabilities
    /// and avoiding a second file / extra reads for ordinary narrow joins.
    fn writeDiskRecord(manager: *@import("spill.zig").Manager, file: *@import("spill.zig").File, payloads: *?*@import("spill.zig").JoinPayloads, values: @import("execution_batch.zig").Batch, keys: []const Datum, ordinal: u64, link: u64, inline_budget: usize) !u64 {
        if (ordinal >= native_payload_bit) return error.SqlProgramLimitExceeded;
        var pressure = @import("spill.zig").CachePressure.forPayloads(manager, payloads);
        var arena = std.heap.ArenaAllocator.init(pressure.allocator());
        defer arena.deinit();
        if (try nativeRowBytes(values, 0, keys, inline_budget, arena.allocator()) == null)
            return file.appendWithPressure(.{ .values = try values.row(arena.allocator(), 0), .keys = keys, .ordinal = ordinal }, link, &pressure);
        if (payloads.* == null) {
            const owner = try manager.allocator().create(@import("spill.zig").JoinPayloads);
            errdefer manager.allocator().destroy(owner);
            owner.* = try @import("spill.zig").JoinPayloads.init(manager);
            payloads.* = owner;
        }
        const payload = try payloads.*.?.appendBatch(values);
        if (payload >= native_payload_bit) return error.SqlProgramLimitExceeded;
        return file.appendWithPressure(.{ .values = &.{ Datum.json(.{ .integer = @intCast(values.width()) }), Datum.json(.{ .integer = 0 }) }, .keys = keys, .ordinal = payload | native_payload_bit }, link, &pressure);
    }
    /// Include boxed decoding, wire storage and arena growth in the inline
    /// choice. Payloads that exceed that allowance use the manager-owned lane.
    const PayloadEstimate = struct {
        native: bool = false,
        retained: usize = 0,
        decoded: usize = 0,
        fn init(values: @import("execution_batch.zig").Batch, row: usize, a: Allocator) !PayloadEstimate {
            var result: PayloadEstimate = .{};
            for (0..values.width()) |column| {
                if (try values.frozenJson(row, column)) |frozen| {
                    result.native = true;
                    result.retained +|= frozen.bytes.len +| 32;
                } else if (try values.encodedJson(row, column)) |text| {
                    result.native = true;
                    result.retained +|= text.len +| 32;
                } else {
                    const cell = try values.cell(a, row, column);
                    const size = try datumBytes(cell);
                    result.decoded +|= size;
                    result.retained +|= if (cell.array != null or cell.numeric != null or cell.patterns != null) size else try @import("typed_store.zig").retainedCellBytes(cell);
                }
            }
            return result;
        }
        fn recordBytes(self: PayloadEstimate, keys: []const Datum, inline_budget: usize) !?usize {
            var decoded = self.decoded;
            for (keys) |key| decoded +|= try datumBytes(key);
            return if (self.native or self.retained > 1024 or decoded *| 2 +| 2048 > inline_budget) self.retained +| 64 else null;
        }
    };
    fn nativeRowBytes(values: @import("execution_batch.zig").Batch, row: usize, keys: []const Datum, inline_budget: usize, a: Allocator) !?usize {
        return (try PayloadEstimate.init(values, row, a)).recordBytes(keys, inline_budget);
    }
    const PayloadIdentity = [16]struct { kind: u64 = 0, first: u64 = 0, second: u64 = 0 };
    const PayloadDescription = struct { identity: ?PayloadIdentity, estimate: ?PayloadEstimate };
    /// Inspect cheap cells once while collecting lease-local identities. Array
    /// walks wait until a memo miss; ordinary inline rows need no second read.
    /// Unsupported DOMs still receive normal sizing without an identity key.
    fn describePayload(values: @import("execution_batch.zig").Batch, row: usize, a: Allocator) !PayloadDescription {
        if (values.width() > 16) return .{ .identity = null, .estimate = try PayloadEstimate.init(values, row, a) };
        var result: PayloadIdentity = @splat(.{});
        var identifiable = true;
        var estimate: ?PayloadEstimate = .{};
        for (result[0..values.width()], 0..) |*part, column| {
            if (try values.frozenJson(row, column)) |frozen| {
                part.* = .{ .kind = if (frozen.lossless_text) 1 else 2, .first = @intFromPtr(frozen.bytes.ptr), .second = frozen.bytes.len };
                if (estimate) |*size| {
                    size.native = true;
                    size.retained +|= frozen.bytes.len +| 32;
                }
            } else if (try values.encodedJson(row, column)) |text| {
                part.* = .{ .kind = 3, .first = @intFromPtr(text.ptr), .second = text.len };
                if (estimate) |*size| {
                    size.native = true;
                    size.retained +|= text.len +| 32;
                }
            } else {
                const cell = try values.cell(a, row, column);
                if (cell.array != null) {
                    estimate = null;
                } else if (estimate) |*size| {
                    const decoded = try datumBytes(cell);
                    size.decoded +|= decoded;
                    size.retained +|= if (cell.numeric != null or cell.patterns != null) decoded else try @import("typed_store.zig").retainedCellBytes(cell);
                }
                if (cell.sql_null) {
                    if (cell.value != .null or cell.array != null or cell.numeric != null or cell.patterns != null) identifiable = false;
                    part.kind = 4;
                } else if (cell.array) |value| {
                    if (cell.value != .null or cell.numeric != null or cell.patterns != null) identifiable = false;
                    part.* = .{ .kind = 5, .first = @intFromPtr(value) };
                } else if (cell.numeric) |value| {
                    if (cell.value != .null or cell.patterns != null) identifiable = false;
                    part.* = .{ .kind = 6, .first = @intFromPtr(value) };
                } else if (cell.patterns) |value| {
                    if (cell.value != .null) identifiable = false;
                    part.* = .{ .kind = 7, .first = @intFromPtr(value) };
                } else {
                    part.* = switch (cell.value) {
                        .null => .{ .kind = 8 },
                        .bool => |value| .{ .kind = 9, .first = @intFromBool(value) },
                        .integer => |value| .{ .kind = 10, .first = @bitCast(value) },
                        .float => |value| .{ .kind = 11, .first = @bitCast(value) },
                        .string => |value| .{ .kind = 12, .first = @intFromPtr(value.ptr), .second = value.len },
                        .number_string => |value| .{ .kind = 13, .first = @intFromPtr(value.ptr), .second = value.len },
                        else => blk: {
                            identifiable = false;
                            break :blk .{};
                        },
                    };
                }
            }
        }
        return .{ .identity = if (identifiable) result else null, .estimate = estimate };
    }
    /// One bounded producer cohort. Native rows share immutable payload pages;
    /// narrow rows keep the single-file path. Only page offsets and lane IDs
    /// enter key records, so source identities retire before the next pull.
    fn writeDiskCohort(manager: *@import("spill.zig").Manager, file: *@import("spill.zig").File, payloads: *?*@import("spill.zig").JoinPayloads, heads: []u64, values: @import("execution_batch.zig").Batch, keys: []const []const Datum, ordinal: usize, inline_budget: usize) !void {
        const Payloads = @import("spill.zig").JoinPayloads;
        if (values.len() == 0 or values.len() > Payloads.page_rows or keys.len != values.len()) return error.InvalidSqlBackendResponse;
        if (ordinal >= native_payload_bit or values.len() > native_payload_bit - ordinal) return error.SqlProgramLimitExceeded;
        var pressure = @import("spill.zig").CachePressure.forPayloads(manager, payloads);
        var arena = std.heap.ArenaAllocator.init(pressure.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        const Memo = struct { estimate: PayloadEstimate, native: ?usize = null };
        var identities: std.AutoHashMapUnmanaged(PayloadIdentity, Memo) = .empty;
        var native: [Payloads.page_rows]?usize = @splat(null);
        var positions: [Payloads.page_rows]usize = undefined;
        var ordinary_bytes: [Payloads.page_rows]usize = undefined;
        var refs: [Payloads.page_rows]@import("spill.zig").PayloadReader.Reference = undefined;
        var unique: usize = 0;
        for (keys, 0..) |key, row| {
            if (key.len > 256 or key.len != keys[0].len) return error.InvalidSqlBackendResponse;
            const description = try describePayload(values, row, a);
            const identity = description.identity;
            const memo = if (identity) |id| identities.get(id) else null;
            const estimate = if (memo) |entry| entry.estimate else description.estimate orelse try PayloadEstimate.init(values, row, a);
            if (try estimate.recordBytes(key, inline_budget)) |_| {
                if (identity) |id| {
                    const entry = try identities.getOrPut(a, id);
                    if (!entry.found_existing) entry.value_ptr.* = .{ .estimate = estimate };
                    if (entry.value_ptr.native) |prior| {
                        native[row] = prior;
                        continue;
                    }
                    entry.value_ptr.native = unique;
                }
                positions[unique] = row;
                ordinary_bytes[unique] = estimate.decoded;
                native[row] = unique;
                unique += 1;
            }
        }
        if (unique != 0 and payloads.* == null) {
            const owner = try manager.allocator().create(Payloads);
            errdefer manager.allocator().destroy(owner);
            owner.* = try Payloads.init(manager);
            payloads.* = owner;
        }
        var body_reuse: Payloads.BodyReuse = .{};
        if (unique != 0) try payloads.*.?.appendCohort(values, positions[0..unique], refs[0..unique], &body_reuse, Payloads.page_bytes, ordinary_bytes[0..unique]);
        // Only physical page/lane refs survive into key emission. Retire the
        // identity map and selection views before allocating key wire buffers.
        _ = arena.reset(.free_all);
        var row_scratch = std.heap.ArenaAllocator.init(pressure.allocator());
        defer row_scratch.deinit();
        for (keys, 0..) |key, row| {
            _ = row_scratch.reset(.{ .retain_with_limit = 8192 });
            const hashed = try keyHash(key);
            const bucket = if (hashed) |hash| hash & (heads.len - 1) else 0;
            const link = if (hashed != null) heads[bucket] else @import("spill.zig").none;
            const offset = if (native[row]) |id| blk: {
                const ref = refs[id];
                break :blk try file.appendWithPressure(.{ .values = &.{ Datum.json(.{ .integer = @intCast(values.width()) }), Datum.json(.{ .integer = @intCast(ref.index) }) }, .keys = key, .ordinal = ref.offset | native_payload_bit }, link, &pressure);
            } else try file.appendWithPressure(.{ .values = try values.row(row_scratch.allocator(), row), .keys = key, .ordinal = ordinal + row }, link, &pressure);
            if (hashed != null) heads[bucket] = offset;
        }
    }
    fn startSpill(self: *HashJoin) !void {
        const manager = self.limits.spill orelse return error.SqlProgramLimitExceeded;
        const alloc = self.budget.allocator();
        const slots = std.math.floorPowerOfTwo(usize, @max(@as(usize, 16), self.limits.bytes / 64));
        const heads = try alloc.alloc(u64, slots);
        errdefer alloc.free(heads);
        @memset(heads, @import("spill.zig").none);
        var file = try manager.create();
        errdefer file.close();
        var payloads: ?*@import("spill.zig").JoinPayloads = null;
        errdefer if (payloads) |owner| {
            owner.close();
            manager.allocator().destroy(owner);
        };
        var ordinal: usize = 0;
        while (ordinal < self.entries.items.len) {
            var arena = std.heap.ArenaAllocator.init(self.backing);
            defer arena.deinit();
            const a = arena.allocator();
            if (self.entries.items[ordinal].width != self.value_columns.columns.len) {
                const values = try (Match{ .owner = self, .index = ordinal }).valuesBatch(a);
                const keys = try self.key_columns.row(a, ordinal);
                try writeDiskCohort(manager, &file, &payloads, heads, values, &.{keys}, ordinal, self.budget.limit -| heads.len * @sizeOf(u64));
                ordinal += 1;
                continue;
            }
            var end = ordinal;
            while (end < self.entries.items.len and end - ordinal < @import("spill.zig").JoinPayloads.page_rows and self.entries.items[end].width == self.value_columns.columns.len) : (end += 1) {}
            const keys = try a.alloc([]const Datum, end - ordinal);
            for (keys, ordinal..) |*key, index| key.* = try self.key_columns.row(a, index);
            const values: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &self.value_columns, .begin = ordinal, .count = end - ordinal } };
            try writeDiskCohort(manager, &file, &payloads, heads, values, keys, ordinal, self.budget.limit -| heads.len * @sizeOf(u64));
            ordinal = end;
        }
        self.value_columns.deinit();
        self.key_columns.deinit();
        self.value_columns = .init(alloc);
        self.key_columns = .init(alloc);
        self.entries.clearAndFree(alloc);
        self.heads.clearAndFree(alloc);
        self.disk_heads = heads;
        self.disk = file;
        self.disk_values = payloads;
    }
    fn appendDisk(self: *HashJoin, values: []const Datum, keys: []const Datum) !void {
        return self.appendDiskBatch(.{ .rows = &.{values} }, keys);
    }
    fn appendDiskBatch(self: *HashJoin, values: @import("execution_batch.zig").Batch, keys: []const Datum) !void {
        if (keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        const hashed = try keyHash(keys);
        const bucket = if (hashed) |value| value & (self.disk_heads.len - 1) else 0;
        const offset = try writeDiskRecord(self.limits.spill.?, &self.disk.?, &self.disk_values, values, keys, self.row_count, if (hashed != null) self.disk_heads[bucket] else @import("spill.zig").none, self.budget.limit -| self.disk_heads.len * @sizeOf(u64));
        if (hashed != null) self.disk_heads[bucket] = offset;
        self.key_count = keys.len;
    }
    pub fn create(alloc: Allocator, limits: Limits) !*HashJoin {
        if (limits.bytes < @sizeOf(HashJoin)) return error.SqlProgramLimitExceeded;
        const self = try alloc.create(HashJoin);
        self.* = .{ .backing = alloc, .budget = .{ .backing = alloc, .limit = limits.bytes - @sizeOf(HashJoin) }, .limits = limits, .value_columns = undefined, .key_columns = undefined };
        self.value_columns = .init(self.budget.allocator());
        self.key_columns = .init(self.budget.allocator());
        self.disk_arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        return self;
    }
    pub fn deinit(self: *HashJoin) void {
        const alloc = self.budget.allocator();
        self.clearPayloadCache();
        if (self.disk_values) |file| {
            file.close();
            self.limits.spill.?.allocator().destroy(file);
        }
        if (self.disk) |*file| file.close();
        self.disk_arena.deinit();
        alloc.free(self.disk_heads);
        self.value_columns.deinit();
        self.key_columns.deinit();
        self.entries.deinit(alloc);
        self.heads.deinit(alloc);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
    pub fn keyHash(keys: []const Datum) !?u64 {
        var hasher = std.hash.Wyhash.init(0);
        for (keys) |key| {
            if (key.sql_null) return null;
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, try scalar.semanticHashDatum(key), .little);
            hasher.update(&bytes);
        }
        return hasher.final();
    }
    pub fn add(self: *HashJoin, values: []const Datum, keys: []const Datum) !void {
        if (self.sealed or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        if (self.row_count >= self.limits.rows) return error.SqlProgramLimitExceeded;
        self.key_count = keys.len;
        if (self.disk == null and self.limits.spill != null) {
            var estimate: usize = 4096;
            estimate +|= (try self.value_columns.appendBytes(values)) *| 4;
            estimate +|= (try self.key_columns.appendBytes(keys)) *| 4;
            if (self.budget.live > self.budget.limit / 2 or estimate > self.budget.limit -| self.budget.live) try self.startSpill();
        }
        if (self.disk != null) {
            try self.appendDisk(values, keys);
            self.row_count += 1;
            return;
        }
        self.addInner(values, keys) catch |err| return if (err == error.OutOfMemory and self.budget.exhausted) error.SqlProgramLimitExceeded else err;
        self.row_count += 1;
    }
    fn admitBatch(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum) !bool {
        var payloads = @import("typed_store.zig").Store.JsonLease.init(a);
        defer payloads.deinit();
        return self.admitBatchLeased(a, batch, keys, &payloads);
    }
    fn admitBatchLeased(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, payloads: *@import("typed_store.zig").Store.JsonLease) !bool {
        if (self.disk != null) return false;
        if (self.sealed or batch.len() != keys.len) return error.InvalidSqlBackendResponse;
        if (keys.len > self.limits.rows -| self.row_count) return error.SqlProgramLimitExceeded;
        var needed: usize = 4096 +| batch.len() *| (128 +| batch.width() *| 64);
        for (keys) |row| {
            if (row.len > 256 or (self.key_count != null and row.len != self.key_count.?)) return error.InvalidSqlBackendResponse;
            for (row) |value| needed +|= (try datumBytes(value)) *| 4;
        }
        needed +|= self.value_columns.dictionaryGrowthBytes(batch.len()) *| 2;
        needed +|= self.key_columns.dictionaryGrowthBytes(keys.len) *| 2;
        // A final single row may cross the soft halfway mark, as scalar
        // admission historically did. The allocator retains the hard ceiling.
        const ceiling = if (keys.len == 1 and self.budget.live <= self.budget.limit / 2) self.budget.limit else self.budget.limit / 2;
        const available = ceiling -| self.budget.live;
        if (needed > available) return false;
        needed +|= (try self.value_columns.batchAppendBytesLeased(a, batch, (available - needed) / 4, payloads)) *| 4;
        if (needed > available) return false;
        const hashes = try @import("batch_hash.zig").rows(a, keys, false);
        defer a.free(hashes);
        const retained = self.budget.allocator();
        try self.entries.ensureUnusedCapacity(retained, keys.len);
        try self.heads.ensureUnusedCapacity(retained, @intCast(keys.len));
        try self.value_columns.appendBatchLeased(batch, payloads);
        for (keys, hashes) |row, hash| {
            const index = self.entries.items.len;
            _ = try self.key_columns.append(row);
            self.entries.appendAssumeCapacity(.{ .width = batch.width(), .next = if (hash) |h| self.heads.get(h) else null });
            if (hash) |h| self.heads.putAssumeCapacity(h, index);
            self.key_count = row.len;
        }
        self.row_count += keys.len;
        return true;
    }
    /// Find a fitting native prefix without decoding rejected payloads. Each
    /// failed reservation leaves retained state unchanged; descriptors are
    /// reclaimed between attempts and source leases remain pinned by callers.
    fn admitPrefix(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, begin: usize) !usize {
        var payloads = @import("typed_store.zig").Store.JsonLease.init(a);
        defer payloads.deinit();
        return self.admitPrefixLeased(a, batch, keys, begin, &payloads);
    }
    fn admitPrefixLeased(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, begin: usize, payloads: *@import("typed_store.zig").Store.JsonLease) !usize {
        if (self.disk != null) return 0;
        var count = keys.len - begin;
        var scratch = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
        defer scratch.deinit();
        while (count != 0) : (count /= 2) {
            _ = scratch.reset(.retain_capacity);
            const work = scratch.allocator();
            const positions = try work.alloc(usize, count);
            for (positions, begin..) |*position, index| position.* = index;
            const selected = try batch.select(work, positions);
            if (try self.admitBatchLeased(work, selected, keys[begin..][0..count], payloads)) return count;
        }
        return 0;
    }
    /// Keep encoded columns through partial memory admission. Only the legacy
    /// row-oriented disk chain or a genuinely oversized row needs scalar input.
    pub fn addBatch(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum) !void {
        if (batch.len() != keys.len or self.sealed) return error.InvalidSqlBackendResponse;
        if (keys.len > self.limits.rows -| self.row_count) return error.SqlProgramLimitExceeded;
        var payloads = @import("typed_store.zig").Store.JsonLease.init(a);
        defer payloads.deinit();
        var begin: usize = 0;
        while (begin != keys.len) {
            const count = try self.admitPrefixLeased(a, batch, keys, begin, &payloads);
            if (count == 0) break;
            begin += count;
        }
        if (begin == keys.len) return;
        if (self.limits.spill != null) {
            if (self.disk == null) try self.startSpill();
            var region = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
            defer region.deinit();
            while (begin < keys.len) {
                _ = region.reset(.retain_capacity);
                const end = @min(keys.len, begin + @import("spill.zig").JoinPayloads.page_rows);
                const positions = try region.allocator().alloc(usize, end - begin);
                for (positions, begin..) |*position, index| position.* = index;
                const selected = try batch.select(region.allocator(), positions);
                if (self.key_count != null and keys[begin].len != self.key_count.?) return error.InvalidSqlBackendResponse;
                try writeDiskCohort(self.limits.spill.?, &self.disk.?, &self.disk_values, self.disk_heads, selected, keys[begin..end], self.row_count, self.budget.limit -| self.disk_heads.len * @sizeOf(u64));
                self.key_count = keys[begin].len;
                self.row_count += end - begin;
                begin = end;
            }
            return;
        }
        const values = try a.alloc(Datum, batch.width());
        defer a.free(values);
        var scratch = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
        defer scratch.deinit();
        for (keys[begin..], begin..) |key, index| {
            _ = scratch.reset(.retain_capacity);
            for (values, 0..) |*value, column| value.* = try batch.cell(scratch.allocator(), index, column);
            try self.add(values, key);
        }
    }
    /// Retain the largest affordable native prefix before transferring the
    /// remainder to Grace partitions. Failed sizing never constructs a DOM.
    pub fn addBatchUntilFull(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum) !usize {
        if (batch.len() != keys.len or self.disk != null or self.sealed) return error.InvalidSqlBackendResponse;
        if (keys.len > self.limits.rows -| self.row_count) return error.SqlProgramLimitExceeded;
        var payloads = @import("typed_store.zig").Store.JsonLease.init(a);
        defer payloads.deinit();
        var begin: usize = 0;
        while (begin != keys.len) {
            const count = try self.admitPrefixLeased(a, batch, keys, begin, &payloads);
            if (count == 0) break;
            begin += count;
        }
        return begin;
    }
    fn addInner(self: *HashJoin, values: []const Datum, keys: []const Datum) !void {
        if (self.sealed or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        if (self.entries.items.len >= self.limits.rows) return error.SqlProgramLimitExceeded;
        self.key_count = keys.len;
        const key_hash = try keyHash(keys);
        const alloc = self.budget.allocator();
        try self.entries.ensureUnusedCapacity(alloc, 1);
        if (key_hash != null) try self.heads.ensureUnusedCapacity(alloc, 1);
        const index = self.entries.items.len;
        _ = try self.value_columns.appendRagged(values);
        _ = try self.key_columns.append(keys);
        self.entries.appendAssumeCapacity(.{ .width = values.len, .next = if (key_hash) |hashed| self.heads.get(hashed) else null });
        if (key_hash) |hashed| self.heads.putAssumeCapacity(hashed, index);
    }
    pub fn probe(self: *HashJoin, keys: []const Datum) !Probe {
        self.sealed = true;
        if (self.key_count != null and self.key_count.? != keys.len) return error.InvalidSqlBackendResponse;
        if (self.disk != null) {
            const bucket = if (try keyHash(keys)) |hashed| self.disk_heads[hashed & (self.disk_heads.len - 1)] else @import("spill.zig").none;
            return .{ .owner = self, .keys = keys, .cursor = if (bucket == @import("spill.zig").none) null else @intCast(bucket) };
        }
        return .{ .owner = self, .keys = keys, .cursor = if (try keyHash(keys)) |hashed| self.heads.get(hashed) else null };
    }
    /// Seed probe chains for a whole batch before traversing candidates. Hash
    /// each key column together and keep per-lane NULL semantics. Returned
    /// probes borrow their batch keys until all lanes have been consumed.
    pub fn probeBatch(self: *HashJoin, a: Allocator, keys: []const []const Datum) ![]Probe {
        self.sealed = true;
        const probes = try a.alloc(Probe, keys.len);
        errdefer a.free(probes);
        const hashes = try @import("batch_hash.zig").rows(a, keys, false);
        defer a.free(hashes);
        for (keys, probes, hashes) |row, *probe_, hash| {
            if (self.key_count != null and row.len != self.key_count.?) return error.InvalidSqlBackendResponse;
            probe_.* = .{ .owner = self, .keys = row, .cursor = null };
            if (hash) |value| {
                if (self.disk != null) {
                    const head = self.disk_heads[value & (self.disk_heads.len - 1)];
                    probe_.cursor = if (head == @import("spill.zig").none) null else @intCast(head);
                } else probe_.cursor = self.heads.get(value);
            }
        }
        if (self.disk == null) {
            // Interleave chain traversal across lanes. Keep the first exact
            // match in the probe and leave remaining duplicates lazy.
            var pending = true;
            while (pending) {
                pending = false;
                for (probes) |*probe_| {
                    if (probe_.first != null) continue;
                    const index = probe_.cursor orelse continue;
                    probe_.cursor = self.entries.items[index].next;
                    self.probes += 1;
                    if (try self.key_columns.equal(self.backing, index, probe_.keys, false)) {
                        probe_.first = .{ .index = index, .owner = self };
                    } else if (probe_.cursor != null) pending = true;
                }
            }
        }
        return probes;
    }
    /// Mark only after the complete ON residual accepts this candidate.
    pub fn markMatched(self: *HashJoin, index: usize) !void {
        if (self.disk) |*file| {
            try file.match(index);
            return;
        }
        self.entries.items[index].matched = true;
    }
    fn readDiskKey(self: *HashJoin, file: *@import("spill.zig").File, offset: u64) !@import("spill.zig").Decoded {
        _ = self.disk_arena.reset(.free_all);
        const stable = self.budget.allocator();
        var pressure: @import("spill.zig").CachePressure = .{ .manager = file.manager, .context = self, .evict = evictPayload, .backing = stable };
        if (file.read_buffer.len == 0) pressure.prepare(@max(1, file.buffer_bytes orelse file.manager.buffer_bytes));
        self.disk_arena.child_allocator = pressure.allocator();
        defer self.disk_arena.child_allocator = stable;
        return file.read(self.disk_arena.allocator(), offset);
    }
    pub fn unmatched(self: *HashJoin, next: *usize) !?Match {
        if (self.disk) |*file| {
            while (next.* < file.size) {
                const offset = next.*;
                const row = try self.readDiskKey(file, offset);
                next.* = @intCast(row.following);
                if (!row.matched) return try self.diskMatch(offset, row);
            }
            return null;
        }
        while (next.* < self.entries.items.len) {
            const index = next.*;
            next.* += 1;
            const entry = self.entries.items[index];
            if (!entry.matched) return .{ .index = index, .owner = self };
        }
        return null;
    }
};

/// Bounded streaming hash aggregation. Owns only distinct keys and aggregate
/// states, never the scanned rows. A stable heap owner keeps quota allocator
/// pointers valid through map growth and result handoff.
pub const Grouped = struct {
    pub const Limits = struct { groups: usize = 10000, bytes: usize = 8 * 1024 * 1024, spill: ?*@import("spill.zig").Manager = null };
    pub const Reducers = struct { ptr: *anyopaque, groups: []const *Grouped, release: *const fn (*anyopaque) void };
    /// Already unique, fully reduced keys. Pulls own only a bounded output
    /// page; no hash table or second grouping/spill pass is required.
    pub const Finalized = struct {
        ptr: *anyopaque,
        count: usize,
        next: *const fn (*anyopaque, Allocator, usize, bool) anyerror!?GroupBatch,
        close: *const fn (*anyopaque) void,
    };
    const Group = struct { next: ?usize, ordinal: u64 };
    backing: Allocator,
    budget: MemoryBudget,
    specs: []AggregateSpec,
    limits: Limits,
    groups: std.ArrayList(Group) = .empty,
    key_columns: @import("typed_store.zig").Store,
    state_columns: []@import("aggregate_state.zig").Column,
    results: std.heap.ArenaAllocator,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    rows_seen: u64 = 0,
    source_ordinal: ?u64 = null,
    hash_probes: u64 = 0,
    last_group: ?usize = null,
    failed: bool = false,
    finished: bool = false,
    key_count: ?usize = null,

    finalized: ?Finalized = null,
    finalized_pages: usize = 0,
    finalized_page_limit: usize = 0,
    external: ?*@import("spill_grouped.zig").Grouped = null,
    result_cursor: usize = 0,
    reducers: ?Reducers = null,
    reducer_cursor: usize = 0,
    output_count: usize = 0,
    pub fn adoptReducers(self: *Grouped, reducers: Reducers, rows: u64) !void {
        if (self.finished or self.failed or self.external != null or self.reducers != null or self.groups.items.len != 0) return error.InvalidSqlBackendResponse;
        self.reducers = reducers;
        self.rows_seen = rows;
        self.finished = true; // Sealed grouping; only result pulls remain valid.
    }
    pub fn adoptFinalized(self: *Grouped, source: Finalized, page_limit: usize) !void {
        if (self.failed or self.finished or self.finalized != null or self.external != null or self.reducers != null or self.rows_seen != 0 or self.groups.items.len > 1 or (self.groups.items.len == 1 and self.key_count != 0)) return error.InvalidSqlBackendResponse;
        if (source.count > self.limits.groups) return error.SqlProgramLimitExceeded;
        self.clearGroups(); // Retire the initial empty global group, if present.
        self.finalized = source;
        self.finalized_page_limit = page_limit;
        self.finished = true;
    }
    fn nextFinalized(self: *Grouped, a: Allocator, maximum: usize, exact: bool) !?GroupBatch {
        const source = self.finalized.?;
        const result = try source.next(source.ptr, a, maximum, exact);
        if (result) |batch| {
            if (batch.ordinals.len == 0 or batch.ordinals.len > maximum or batch.keys.len() != batch.ordinals.len or batch.aggregates.len() != batch.ordinals.len or batch.aggregates.width() != self.specs.len) return error.InvalidSqlBackendResponse;
            self.finalized_pages += 1;
            if (self.finalized_pages > self.finalized_page_limit or batch.ordinals.len > source.count -| self.output_count) return error.SqlProgramLimitExceeded;
            self.output_count += batch.ordinals.len;
        } else if (self.output_count != source.count) return error.InvalidSqlBackendResponse;
        return result;
    }
    fn clearGroups(self: *Grouped) void {
        const alloc = self.budget.allocator();
        for (self.state_columns) |*column| {
            column.clear(alloc);
        }
        self.key_columns.deinit();
        self.key_columns = .init(alloc);
        _ = self.results.reset(.free_all);
        self.groups.clearAndFree(alloc);
        self.heads.clearAndFree(alloc);
    }
    fn startSpill(self: *Grouped) !void {
        const manager = self.limits.spill orelse return error.SqlProgramLimitExceeded;
        const external = try self.backing.create(@import("spill_grouped.zig").Grouped);
        errdefer self.backing.destroy(external);
        external.* = try @import("spill_grouped.zig").Grouped.init(self.backing, manager, self.specs, self.key_count orelse 0, self.limits.bytes);
        errdefer external.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.backing);
        defer scratch.deinit();
        for (0..self.groups.items.len) |index| {
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const states = try a.alloc(Aggregate, self.specs.len);
            for (self.state_columns, states) |column, *state| state.* = try column.snapshot(a, index);
            try external.partial(try self.key_columns.row(a, index), states, self.groups.items[index].ordinal);
        }
        self.clearGroups();
        self.external = external;
    }
    /// Borrowed columns remain valid until the next result pull. Retaining
    /// consumers admit them directly; scalar consumers materialize at the edge.
    pub fn nextResultBatch(self: *Grouped, a: Allocator, maximum: usize, exact: bool) !?GroupBatch {
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.failed) return error.InvalidSqlBackendResponse;
        if (self.finalized != null) return self.nextFinalized(a, maximum, exact);
        if (self.reducers) |reducers| {
            while (self.reducer_cursor < reducers.groups.len) {
                const result = try reducers.groups[self.reducer_cursor].nextResultBatch(a, maximum, exact);
                if (result) |batch| {
                    self.output_count = try std.math.add(usize, self.output_count, batch.ordinals.len);
                    if (self.output_count > self.limits.groups) return error.SqlProgramLimitExceeded;
                    self.finished = true;
                    return batch;
                }
                self.reducer_cursor += 1;
            }
            return null;
        }
        if (self.external) |external| {
            const result = try external.nextBatch(a, maximum, exact);
            if (result != null and external.output_count > self.limits.groups) return error.SqlProgramLimitExceeded;
            self.finished = true;
            return result;
        }
        if (self.result_cursor == self.groups.items.len) return null;
        const begin = self.result_cursor;
        const count = @min(maximum, self.groups.items.len - begin);
        const ordinals = try a.alloc(u64, count);
        for (self.groups.items[begin..][0..count], ordinals) |group, *ordinal| ordinal.* = group.ordinal;
        const Reader = struct {
            group: *Grouped,
            begin: usize,
            exact: bool,
            fn cell(raw: *anyopaque, alloc: Allocator, row: usize, column: usize) anyerror!Datum {
                const reader: *@This() = @ptrCast(@alignCast(raw));
                const index = reader.begin + row;
                if (!reader.exact) return reader.group.state_columns[column].finish(index);
                const state = try reader.group.state_columns[column].snapshot(alloc, index);
                return @import("aggregate_partial.zig").cell(alloc, state);
            }
        };
        const reader = try a.create(Reader);
        reader.* = .{ .group = self, .begin = begin, .exact = exact };
        self.result_cursor += count;
        self.finished = true;
        return .{
            .keys = .{ .retained = .{ .store = &self.key_columns, .begin = begin, .count = count } },
            .aggregates = .{ .reader = .{ .ptr = reader, .read = Reader.cell, .count = count, .width = self.specs.len } },
            .ordinals = ordinals,
        };
    }
    pub fn nextResult(self: *Grouped, alloc: Allocator) !?GroupResult {
        if (self.finalized != null) {
            const batch = (try self.nextFinalized(alloc, 1, false)) orelse return null;
            return .{ .keys = try batch.keys.row(alloc, 0), .aggregates = try batch.aggregates.row(alloc, 0), .ordinal = batch.ordinals[0] };
        }
        if (self.reducers) |reducers| {
            while (self.reducer_cursor < reducers.groups.len) {
                if (try reducers.groups[self.reducer_cursor].nextResult(alloc)) |result| {
                    self.output_count = try std.math.add(usize, self.output_count, 1);
                    if (self.output_count > self.limits.groups) return error.SqlProgramLimitExceeded;
                    self.finished = true;
                    return result;
                }
                self.reducer_cursor += 1;
            }
            return null;
        }
        if (self.external) |external| {
            const result = try external.next(alloc);
            if (result != null and external.output_count > self.limits.groups) return error.SqlProgramLimitExceeded;
            self.finished = true;
            return result;
        }
        if (self.result_cursor == self.groups.items.len) return null;
        const result = try self.resultAt(alloc, self.result_cursor);
        self.result_cursor += 1;
        return result;
    }
    pub fn nextTypedPartialResult(self: *Grouped, a: Allocator) !?GroupResult {
        if (self.finalized != null) return self.nextPartialResult(a);
        if (self.external) |external| {
            // Decode once at the spill boundary, before any compact join pair.
            const result = try external.nextPartial(a) orelse return null;
            const states = try a.alloc(Aggregate, result.aggregates.len);
            for (states, result.aggregates, self.specs) |*state, cell, spec| state.* = try @import("aggregate_partial.zig").decode(a, cell, spec);
            return .{ .keys = result.keys, .aggregates = &.{}, .typed_states = states, .ordinal = result.ordinal };
        }
        if (self.result_cursor == self.groups.items.len) return null;
        const index = self.result_cursor;
        self.result_cursor += 1;
        const states = try a.alloc(Aggregate, self.specs.len);
        for (self.state_columns, states) |column, *state| state.* = try column.snapshot(a, index);
        return .{ .keys = try self.key_columns.row(a, index), .aggregates = &.{}, .typed_states = states, .ordinal = self.groups.items[index].ordinal };
    }
    pub fn nextPartialResult(self: *Grouped, a: Allocator) !?GroupResult {
        if (self.finalized != null) {
            const batch = (try self.nextFinalized(a, 1, true)) orelse return null;
            return .{ .keys = try batch.keys.row(a, 0), .aggregates = try batch.aggregates.row(a, 0), .ordinal = batch.ordinals[0] };
        }
        if (self.external) |external| return external.nextPartial(a);
        if (self.result_cursor == self.groups.items.len) return null;
        const index = self.result_cursor;
        self.result_cursor += 1;
        const states = try a.alloc(Aggregate, self.specs.len);
        for (self.state_columns, states) |column, *state| state.* = try column.snapshot(a, index);
        return .{ .keys = try self.key_columns.row(a, index), .aggregates = try @import("aggregate_partial.zig").encode(a, states), .ordinal = self.groups.items[index].ordinal };
    }
    pub fn create(alloc: Allocator, specs: []const AggregateSpec, limits: Limits) !*Grouped {
        if (limits.bytes < @sizeOf(Grouped) or specs.len > 256) return error.SqlProgramLimitExceeded;
        const self = try alloc.create(Grouped);
        errdefer alloc.destroy(self);
        self.* = .{ .backing = alloc, .budget = .{ .backing = alloc, .limit = limits.bytes - @sizeOf(Grouped) }, .specs = undefined, .limits = limits, .key_columns = undefined, .state_columns = &.{}, .results = .init(alloc) };
        self.key_columns = .init(self.budget.allocator());
        self.specs = self.budget.allocator().dupe(AggregateSpec, specs) catch |err| return if (self.budget.exhausted) error.SqlProgramLimitExceeded else err;
        errdefer self.budget.allocator().free(self.specs);
        self.state_columns = try self.budget.allocator().alloc(@import("aggregate_state.zig").Column, specs.len);
        for (self.state_columns, specs) |*column, spec| column.* = .init(spec);
        return self;
    }

    pub fn deinit(self: *Grouped) void {
        if (self.finalized) |source| source.close(source.ptr);
        if (self.reducers) |reducers| reducers.release(reducers.ptr);
        if (self.external) |external| {
            external.deinit();
            self.backing.destroy(external);
        }
        const alloc = self.budget.allocator();
        self.clearGroups();
        self.key_columns.deinit();
        self.results.deinit();
        alloc.free(self.state_columns);
        alloc.free(self.specs);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }

    pub fn add(self: *Grouped, keys: []const Datum, inputs: []const Datum) !void {
        if (self.failed or self.finished) return error.InvalidSqlBackendResponse;
        if (inputs.len != self.specs.len or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        self.key_count = keys.len;
        const disk_patterns = for (self.specs) |spec| {
            if (spec.kind == .pattern_set) break true;
        } else false;
        if (self.external == null and self.limits.spill != null) {
            if (disk_patterns) try self.startSpill();
        }
        if (self.external == null and self.limits.spill != null) {
            if (!try self.canRetain(keys, inputs)) try self.startSpill();
        }
        if (self.external) |external| {
            try external.add(keys, inputs, self.source_ordinal orelse self.rows_seen);
            self.rows_seen = std.math.add(u64, self.rows_seen, 1) catch return error.SqlNumericOutOfRange;
            return;
        }
        self.update(keys, inputs) catch |err| {
            self.failed = true;
            return if (err == error.OutOfMemory and self.budget.exhausted) error.SqlProgramLimitExceeded else err;
        };
    }

    /// Resolve a vector of group IDs once, then update state by aggregate
    /// column. No per-row Datum matrix is constructed. Admission is checked
    /// before mutating groups; spill paths retain their ordered row fallback.
    pub fn addColumns(self: *Grouped, keys: []const []const Datum, inputs: []const []const Datum, count: usize) !void {
        if (self.failed or self.finished or inputs.len != self.specs.len or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        for (keys) |column| if (column.len != count) return error.InvalidSqlBackendResponse;
        for (inputs) |column| if (column.len != count) return error.InvalidSqlBackendResponse;
        if (count == 0) return;
        const a = self.backing;
        const row_keys = try a.alloc(Datum, keys.len);
        defer a.free(row_keys);
        const row_inputs = try a.alloc(Datum, inputs.len);
        defer a.free(row_inputs);
        var needed: usize = 4096 + count *| (512 + self.specs.len *| @sizeOf(Aggregate) *| 2);
        for (keys) |column| for (column) |value| {
            needed +|= (try datumBytes(value)) *| 4;
        };
        for (self.specs, inputs) |spec, column| if (spec.kind == .min or spec.kind == .max) {
            for (column) |value| needed +|= (try datumBytes(value)) *| 4;
        };
        var fast = self.external == null and needed <= self.budget.limit -| self.budget.live and self.budget.live <= self.budget.limit / 2;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or spec.input_element == .numeric) {
            fast = false;
        };
        if (!fast) {
            for (0..count) |index| {
                for (keys, row_keys) |column, *cell| cell.* = column[index];
                for (inputs, row_inputs) |column, *cell| cell.* = column[index];
                try self.add(row_keys, row_inputs);
            }
            return;
        }
        const ids = try a.alloc(usize, count);
        defer a.free(ids);
        self.key_count = keys.len;
        errdefer self.failed = true;
        const hashes = try @import("batch_hash.zig").columns(a, keys, count, true);
        defer a.free(hashes);
        for (ids, 0..) |*id, index| {
            for (keys, row_keys) |column, *cell| cell.* = column[index];
            id.* = try self.resolveGroupHashed(row_keys, hashes[index].?);
            self.rows_seen = std.math.add(u64, self.rows_seen, 1) catch return error.SqlNumericOutOfRange;
        }
        for (self.state_columns, inputs) |*column, values| try column.updateBatch(self.budget.allocator(), ids, values);
    }

    /// Keep encoded expression columns intact through group ID resolution.
    /// Admission precedes mutation; disk, DISTINCT and pattern reducers use
    /// the same ordered fallback as row input.
    pub fn addEncodedColumns(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize) !void {
        return self.addEncodedColumnsIdentified(keys, inputs, count, null);
    }
    /// Source identity lets disjoint key partitions retain columnar state
    /// without rehashing or losing first-occurrence ordinals after selection.
    pub const InputIdentity = struct { ordinals: []const u64, hashes: []const ?u64 };
    const EncodedGroups = struct {
        const Representative = struct { row: usize, hash: u64, existing: ?usize, next: ?usize, group: usize = 0 };
        rows: []usize,
        representatives: []Representative,
        needed: usize,
    };
    /// A bounded census admits distinct new state rather than input rows.
    /// Dictionary tuples are representation identities only; differing tuples
    /// still meet semantic hashing and exact comparison (NaN, zero, SQL NULL).
    fn encodedGroups(self: *Grouped, a: Allocator, keys: []const @import("execution_batch.zig").Batch, physical: []const ?@import("execution_batch.zig").PhysicalColumn, count: usize, hashes: []const ?u64) !?EncodedGroups {
        const maximum = @min(@as(usize, 4096), @as(usize, 1024 * 1024) / @max(@as(usize, 1), keys.len * @sizeOf(u64)));
        var heads: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        var tuples: std.StringHashMapUnmanaged(usize) = .empty;
        var representatives: std.ArrayList(EncodedGroups.Representative) = .empty;
        const rows = try a.alloc(usize, count);
        const tuple = try a.alloc(u64, keys.len);
        const cells = try a.alloc(Datum, keys.len);
        var numeric_keys = try @import("execution_batch.zig").NumericScratch.init(a, keys);
        var candidate_keys = try @import("execution_batch.zig").NumericScratch.init(a, keys);
        var needed: usize = 0;
        for (rows, 0..) |*representative, row_index| {
            var encoded = true;
            for (keys, physical, tuple) |column, native, *id| {
                const value = if (native) |view| view.dictionaryIdentity(row_index) else try column.dictionaryIdentity(row_index, 0);
                if (value) |identity_| id.* = identity_ else {
                    encoded = false;
                    break;
                }
            }
            const tuple_bytes = std.mem.sliceAsBytes(tuple);
            if (encoded) if (tuples.get(tuple_bytes)) |previous| {
                representative.* = previous;
                continue;
            };
            for (physical, cells, 0..) |native, *cell, col| cell.* = if (native) |view| try view.cell(row_index) else try numeric_keys.cell(a, keys, row_index, col);
            const hash = hashes[row_index] orelse return error.InvalidSqlBackendResponse;
            var previous = heads.get(hash);
            while (previous) |index| {
                const candidate = representatives.items[index];
                var equal_ = true;
                for (physical, cells, 0..) |native, cell, col| {
                    const before = if (native) |view| try view.cell(candidate.row) else try candidate_keys.cell(a, keys, candidate.row, col);
                    if (try scalar.compareDatums(before, cell) != .eq) {
                        equal_ = false;
                        break;
                    }
                }
                if (equal_) break;
                previous = candidate.next;
            }
            if (previous) |index| {
                representative.* = index;
            } else {
                if (representatives.items.len == maximum) return null;
                var existing = self.heads.get(hash);
                while (existing) |index| {
                    self.hash_probes += 1;
                    if (try self.key_columns.equal(a, index, cells, true)) break;
                    existing = self.groups.items[index].next;
                }
                if (existing == null) {
                    needed +|= 512 +| self.specs.len *| @sizeOf(Aggregate) *| 2;
                    needed +|= (try self.key_columns.appendBytes(cells)) *| 4;
                }
                representative.* = representatives.items.len;
                try representatives.append(a, .{ .row = row_index, .hash = hash, .existing = existing, .next = heads.get(hash) });
                try heads.put(a, hash, representative.*);
            }
            if (encoded and tuples.count() < maximum) try tuples.put(a, try a.dupe(u8, tuple_bytes), representative.*);
        }
        return .{ .rows = rows, .representatives = representatives.items, .needed = needed };
    }
    pub fn addEncodedColumnsIdentified(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, identity: ?InputIdentity) !void {
        if (try self.addEncodedWindow(keys, inputs, count, identity)) return;
        var begin: usize = 0;
        var length = @max(@as(usize, 1), count / 2);
        while (begin < count) {
            length = @min(length, count - begin);
            if (try self.retryEncodedWindow(keys, inputs, begin, length, identity)) {
                begin += length;
                length *|= 2;
            } else length = @max(@as(usize, 1), length / 2);
        }
    }
    /// Selection construction is optional admission too. Release the whole
    /// retry view before reducing its size; never pin a failed large prefix.
    fn retryEncodedWindow(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, begin: usize, length: usize, identity: ?InputIdentity) !bool {
        // Shrinking rows cannot shrink per-column view metadata. A singleton
        // reads the original lane directly, without optional selection/views.
        if (length == 1) {
            var scratch = std.heap.ArenaAllocator.init(self.backing);
            defer scratch.deinit();
            const a = scratch.allocator();
            var key_cells: [256]Datum = undefined;
            const input_cells = try a.alloc(Datum, inputs.len);
            // Validate optional provider evidence one column at a time. Its
            // metadata must not accumulate with the grouping-key width.
            for ([_][]const @import("execution_batch.zig").Batch{ keys, inputs }) |columns| for (columns) |column| {
                var admission: MemoryBudget = .{ .backing = self.backing, .limit = (MemoryBudget.headroom(self.backing) orelse 4 * 1024 * 1024) / 2 };
                var validation = std.heap.ArenaAllocator.init(admission.allocator());
                defer validation.deinit();
                const view = column.physicalColumn(validation.allocator(), 0) catch |err| {
                    if (err == error.OutOfMemory and admission.isLocallyExhausted()) continue;
                    return err;
                };
                if (view) |physical| try physical.validate();
            };
            for (keys, key_cells[0..keys.len]) |column, *cell| cell.* = try column.cell(a, begin, 0);
            for (inputs, input_cells) |column, *cell| cell.* = try column.cell(a, begin, 0);
            if (identity) |ids| try self.addOrdered(key_cells[0..keys.len], input_cells, ids.ordinals[begin]) else try self.add(key_cells[0..keys.len], input_cells);
            return true;
        }
        var workspace: MemoryBudget = .{ .backing = self.backing, .limit = @min(2 * 1024 * 1024, (MemoryBudget.headroom(self.backing) orelse 4 * 1024 * 1024) / 2) };
        var scratch = std.heap.ArenaAllocator.init(workspace.allocator());
        defer scratch.deinit();
        const a = scratch.allocator();
        const Parts = struct {
            fn build(alloc: Allocator, columns: []const @import("execution_batch.zig").Batch, selected: []const usize) ![]@import("execution_batch.zig").Batch {
                const parts = try alloc.alloc(@import("execution_batch.zig").Batch, columns.len);
                for (columns, parts) |*column, *part| part.* = try column.select(alloc, selected);
                return parts;
            }
        };
        const selection = a.alloc(usize, length) catch |err| {
            if (workspace.isExhausted() and length > 1) return false;
            return err;
        };
        for (selection, begin..) |*row, i| row.* = i;
        const key_parts = Parts.build(a, keys, selection) catch |err| {
            if (err == error.OutOfMemory and workspace.isExhausted() and length > 1) return false;
            return err;
        };
        const input_parts = Parts.build(a, inputs, selection) catch |err| {
            if (err == error.OutOfMemory and workspace.isExhausted() and length > 1) return false;
            return err;
        };
        const ids: ?InputIdentity = if (identity) |value| .{ .ordinals = value.ordinals[begin..][0..length], .hashes = value.hashes[begin..][0..length] } else null;
        return self.addEncodedWindow(key_parts, input_parts, length, ids);
    }
    /// Atomic admission probe for a native reducer page. False consumes no
    /// rows or retained groups, allowing an owning spill reader to switch modes.
    pub fn tryAddEncodedColumns(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, identity: InputIdentity) !bool {
        if (count < 2 or keys.len == 0 or self.external != null) return false;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or (spec.input_element == .numeric and !exactMergeable(spec))) return false;
        return self.addEncodedWindow(keys, inputs, count, identity);
    }
    /// Validate and admit a bounded columnar checkpoint window atomically.
    /// Declining preserves every source lane for repartition or scalar restore.
    pub fn tryImportPartialBatch(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, ordinals: []const u64) !bool {
        const partial = @import("aggregate_partial.zig");
        if (count == 0 or count > 64 or self.external != null) return false;
        if (self.failed or self.finished or keys.len > 256 or inputs.len != self.specs.len or ordinals.len != count or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        for (self.specs, self.state_columns) |spec, column| {
            if (!partial.fixedEligible(spec)) return false;
            switch (column.values) {
                .counts, .integers, .booleans, .bytes, .exact, .fixed => {},
                else => return false,
            }
        }
        for (keys) |column| if (column.len() != count or column.width() != 1) return error.InvalidSqlBackendResponse;
        for (inputs) |column| if (column.len() != count or column.width() != 1) return error.InvalidSqlBackendResponse;
        var workspace: MemoryBudget = .{ .backing = self.backing, .limit = @min(2 * 1024 * 1024, (MemoryBudget.headroom(self.backing) orelse 4 * 1024 * 1024) / 2) };
        var arena = std.heap.ArenaAllocator.init(workspace.allocator());
        defer arena.deinit();
        return self.importPartialWindow(arena.allocator(), keys, inputs, count, ordinals) catch |err| {
            if (err == error.OutOfMemory and workspace.isExhausted() and !self.failed) return false;
            return err;
        };
    }
    fn importPartialWindow(self: *Grouped, a: Allocator, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, ordinals: []const u64) !bool {
        const partial = @import("aggregate_partial.zig");
        const states = try a.alloc([]partial.Fixed, inputs.len);
        for (inputs, self.specs, states) |column, spec, *values| {
            values.* = try a.alloc(partial.Fixed, count);
            for (values.*, 0..) |*state, row| state.* = (try partial.decodeFixed(a, try column.cell(a, row, 0), spec)) orelse return false;
        }
        const physical = try a.alloc(?@import("execution_batch.zig").PhysicalColumn, keys.len);
        for (keys, physical) |column, *view| {
            view.* = try column.physicalColumn(a, 0);
            if (view.*) |native| try native.validate();
        }
        const hashes = try @import("batch_hash.zig").encodedColumnsWithPhysical(a, keys, physical, count, true);
        const census = (try self.encodedGroups(a, keys, physical, count, hashes)) orelse return false;
        var needed = census.needed;
        var additional: usize = 0;
        for (census.representatives) |entry| if (entry.existing == null) {
            additional += 1;
        };
        const required = self.groups.items.len +| additional;
        if (required > self.groups.capacity) needed +|= (required *| 2 +| 8) *| @sizeOf(Group);
        if (additional > self.heads.available) {
            const entries = (self.heads.count() +| additional) *| 100 / std.hash_map.default_max_load_percentage +| 1;
            const capacity = std.math.ceilPowerOfTwo(usize, @max(8, entries)) catch return error.SqlProgramLimitExceeded;
            needed +|= capacity *| 32 +| 64;
        }
        needed +|= self.key_columns.appendGrowthBytes(additional);
        for (self.state_columns, states) |*column, values| {
            needed +|= column.appendGrowth(additional);
            if (column.values == .exact) {
                // Admit the union of all coefficient exponents per destination,
                // including multiple checkpoints for the same new group.
                var high: [64]?i32 = @splat(null);
                var low: [64]i64 = undefined;
                for (values, census.rows) |state, representative| {
                    const exact = state.numeric orelse return error.InvalidSqlSpill;
                    if (exact.buckets.items.len == 0) continue;
                    const bottom = @as(i64, exact.weight) - @as(i64, @intCast(exact.buckets.items.len)) + 1;
                    if (high[representative]) |previous| {
                        high[representative] = @max(previous, exact.weight);
                        low[representative] = @min(low[representative], bottom);
                    } else {
                        high[representative] = exact.weight;
                        low[representative] = bottom;
                    }
                }
                for (census.representatives, 0..) |entry, index| if (high[index]) |weight| {
                    const length: usize = @intCast(@as(i64, weight) - low[index] + 1);
                    needed +|= column.numericGrowth(entry.existing, weight, length);
                };
            }
            if (column.values == .bytes) {
                const maxima = try a.alloc(usize, census.representatives.len);
                @memset(maxima, 0);
                for (values, census.rows) |state, representative| if (!state.selected.sql_null) {
                    maxima[representative] = @max(maxima[representative], state.selected.value.string.len);
                };
                for (maxima) |size| needed +|= size *| 4 +| 64;
            }
        }
        if (needed > @min(self.budget.limit -| self.budget.live, MemoryBudget.headroom(self.backing) orelse std.math.maxInt(usize))) return false;
        const row_keys = try a.alloc(Datum, keys.len);
        var ids: [64]usize = undefined;
        self.key_count = keys.len;
        errdefer self.failed = true;
        for (census.representatives) |*entry| {
            for (keys, physical, row_keys) |column, native, *cell| cell.* = if (native) |view| try view.cell(entry.row) else try column.cell(a, entry.row, 0);
            const saved = self.source_ordinal;
            self.source_ordinal = ordinals[entry.row];
            defer self.source_ordinal = saved;
            entry.group = entry.existing orelse try self.appendGroup(row_keys, entry.hash);
        }
        for (census.rows, ordinals, ids[0..count]) |representative, ordinal, *id| {
            id.* = census.representatives[representative].group;
            self.groups.items[id.*].ordinal = @min(self.groups.items[id.*].ordinal, ordinal);
        }
        for (self.state_columns, states) |*column, values| try column.mergeFixedBatch(self.budget.allocator(), ids[0..count], values);
        return true;
    }
    fn prepareColumns(a: Allocator, columns: []const @import("execution_batch.zig").Batch) ![]?@import("execution_batch.zig").PhysicalColumn {
        const views = try a.alloc(?@import("execution_batch.zig").PhysicalColumn, columns.len);
        for (columns, views) |column, *view| {
            view.* = try column.physicalColumn(a, 0);
            if (view.*) |native| try native.validate();
        }
        return views;
    }
    fn addEncodedWindow(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, identity: ?InputIdentity) !bool {
        const previous_ordinal = self.source_ordinal;
        defer self.source_ordinal = previous_ordinal;
        if (identity) |ids| {
            if (ids.ordinals.len != count or ids.hashes.len != count) return error.InvalidSqlBackendResponse;
            for (ids.hashes) |hash| if (hash == null) return error.InvalidSqlBackendResponse;
        }
        if (self.failed or self.finished or inputs.len != self.specs.len or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        for (keys) |column| if (column.len() != count or column.width() != 1) return error.InvalidSqlBackendResponse;
        for (inputs) |column| if (column.len() != count or column.width() != 1) return error.InvalidSqlBackendResponse;
        if (count == 0) return true;
        self.key_count = keys.len;
        if (count == 1) return self.retryEncodedWindow(keys, inputs, 0, 1, identity);
        var scratch = std.heap.ArenaAllocator.init(self.backing);
        defer scratch.deinit();
        const a = scratch.allocator();
        const row_keys = try a.alloc(Datum, keys.len);
        const row_inputs = try a.alloc(Datum, inputs.len);
        // Optional physical normalization is itself bounded, before census
        // admission. REAL or coercing readers can need lane storage even when
        // retained integer/string views are allocation-free.
        var preparation: MemoryBudget = .{ .backing = self.backing, .limit = @min(2 * 1024 * 1024, (MemoryBudget.headroom(self.backing) orelse 4 * 1024 * 1024) / 2) };
        var physical_arena = std.heap.ArenaAllocator.init(preparation.allocator());
        defer physical_arena.deinit();
        var numeric_keys = @import("execution_batch.zig").NumericScratch.init(physical_arena.allocator(), keys) catch |err| {
            if (err == error.OutOfMemory and preparation.isExhausted()) return false;
            return err;
        };
        var numeric_inputs = @import("execution_batch.zig").NumericScratch.init(physical_arena.allocator(), inputs) catch |err| {
            if (err == error.OutOfMemory and preparation.isExhausted()) return false;
            return err;
        };
        const physical_keys = prepareColumns(physical_arena.allocator(), keys) catch |err| {
            if (err == error.OutOfMemory and preparation.isExhausted() and count > 1) return false;
            return err;
        };
        const physical_inputs = prepareColumns(physical_arena.allocator(), inputs) catch |err| {
            if (err == error.OutOfMemory and preparation.isExhausted() and count > 1) return false;
            return err;
        };

        // Spilling keeps the same physical validation contract as retaining;
        // supplied identity hashes do not bypass selected-lane validation.
        if (try self.spillEncodedColumns(keys, inputs, count, identity)) return true;

        // A global reducer retains one group, independently of batch length.
        // Admit its state once and use bounded scratch windows. Variable-width
        // MIN/MAX reserve the largest possible winner rather than every row.
        var global = keys.len == 0 and self.external == null;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or spec.input_element == .numeric) {
            global = false;
        };
        if (global) {
            for (inputs, row_inputs) |column, *cell| cell.* = try column.cell(a, 0, 0);
            var growth: usize = 0;
            for (self.specs, inputs, physical_inputs) |spec, column, native| if (spec.kind == .min or spec.kind == .max) {
                var maximum: usize = 0;
                if (column == .reader and column.reader.cohort != null) {
                    const source = column.reader.cohort.?.source.*;
                    for (0..source.len()) |index| maximum = @max(maximum, try datumBytes(try source.cell(a, index, 0)));
                } else for (0..count) |index| maximum = @max(maximum, if (native) |view| try view.bytes(index) else try datumBytes(try column.cell(a, index, 0)));
                if (maximum > @sizeOf(Datum)) growth +|= maximum *| 4;
            };
            for (self.specs, self.state_columns, inputs, physical_inputs) |spec, *state, column, native| if (spec.kind == .min or spec.kind == .max) {
                var promotion: usize = 0;
                if (native) |view| {
                    promotion = try state.physicalUpdateGrowth(view, 1);
                } else {
                    for (0..count) |index| promotion = @max(promotion, state.updateGrowth(try column.cell(a, index, 0), 1));
                }
                growth +|= promotion;
            };
            if ((self.groups.items.len != 0 and growth == 0) or try self.canRetainAdditional(&.{}, row_inputs, growth)) {
                self.key_count = 0;
                errdefer self.failed = true;
                try self.ensureGlobalGroup();
                var chunk = std.heap.ArenaAllocator.init(self.backing);
                defer chunk.deinit();
                const ids: [256]usize = @splat(0);
                var selected: [256]usize = undefined;
                for (self.state_columns, inputs, physical_inputs) |*column, original, original_native| {
                    var values = original;
                    var native = original_native;
                    if (original == .reader) if (original.reader.cohort) |cohort| {
                        values = cohort.source.*;
                        native = try values.physicalColumn(a, 0);
                    };
                    var begin: usize = 0;
                    while (begin < values.len()) {
                        _ = chunk.reset(.retain_capacity);
                        const length = @min(ids.len, values.len() - begin);
                        for (selected[0..length], 0..) |*row, offset| row.* = begin + offset;
                        if (native) |physical| {
                            const view = physical.slice(begin, begin + length);
                            if (try column.updatePhysical(self.budget.allocator(), ids[0..length], view)) {
                                begin += length;
                                continue;
                            }
                        }
                        const view = try values.select(chunk.allocator(), selected[0..length]);
                        try column.updateEncoded(self.budget.allocator(), ids[0..length], view);
                        begin += length;
                    }
                }
                self.rows_seen = std.math.add(u64, self.rows_seen, count) catch return error.SqlNumericOutOfRange;
                return true;
            }
        }
        var fast = self.external == null and count > 1;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or (spec.input_element == .numeric and !exactMergeable(spec))) {
            fast = false;
        };
        // Optional census workspace is independently reclaimable. A declined
        // window returns before any retained mutation; retries never pin an
        // ancestor's hashes, representatives or selections.
        var census_budget: MemoryBudget = .{ .backing = self.backing, .limit = @min(2 * 1024 * 1024, (MemoryBudget.headroom(self.backing) orelse 4 * 1024 * 1024) / 2) };
        var census_arena = std.heap.ArenaAllocator.init(census_budget.allocator());
        defer census_arena.deinit();
        const ca = census_arena.allocator();
        const hashes = if (fast) (if (identity) |ids_| ids_.hashes else @import("batch_hash.zig").encodedColumnsWithPhysical(ca, keys, physical_keys, count, true) catch |err| {
            if (err == error.OutOfMemory and census_budget.isExhausted() and count > 1) return false;
            return err;
        }) else null;
        const census = if (hashes) |values| self.encodedGroups(ca, keys, physical_keys, count, values) catch |err| {
            if (err == error.OutOfMemory and census_budget.isExhausted() and count > 1) return false;
            return err;
        } else null;
        var needed = if (census) |groups_| groups_.needed else std.math.maxInt(usize);
        if (census) |groups_| {
            // Variable-width winners can grow once per distinct group. Reserve
            // each group's largest candidate, including sparse FILTER cohorts.
            const maxima = try a.alloc(usize, groups_.representatives.len);
            for (self.specs, inputs, physical_inputs) |spec, column, native| if (spec.kind == .min or spec.kind == .max) {
                @memset(maxima, 0);
                if (column == .reader and column.reader.cohort != null) {
                    const cohort = column.reader.cohort.?;
                    for (cohort.positions, 0..) |position, index| {
                        const group = groups_.rows[position];
                        maxima[group] = @max(maxima[group], try datumBytes(try cohort.source.cell(a, index, 0)));
                    }
                } else for (groups_.rows, 0..) |group, index| maxima[group] = @max(maxima[group], if (native) |view| try view.bytes(index) else try datumBytes(try column.cell(a, index, 0)));
                for (maxima) |bytes| if (bytes > @sizeOf(Datum)) {
                    needed +|= bytes *| 4;
                };
            };
        }
        if (census) |groups_| {
            var additional: usize = 0;
            for (groups_.representatives) |representative| if (representative.existing == null) {
                additional += 1;
            };
            if (additional != 0) {
                const required = self.groups.items.len +| additional;
                if (required > self.groups.capacity) needed +|= (required *| 2 +| 8) *| @sizeOf(Group);
                if (additional > self.heads.available) {
                    const entries = (self.heads.count() +| additional) *| 100 / std.hash_map.default_max_load_percentage +| 1;
                    const capacity = std.math.ceilPowerOfTwo(usize, @max(8, entries)) catch return error.SqlProgramLimitExceeded;
                    needed +|= capacity *| 32 +| 64;
                }
                needed +|= self.key_columns.appendGrowthBytes(additional);
                for (self.state_columns) |*state| needed +|= state.appendGrowth(additional);
            }
            for (self.state_columns, inputs, 0..) |*state, _, input_column| if (state.values == .exact) {
                const high = ca.alloc(?i32, groups_.representatives.len) catch |err| {
                    if (err == error.OutOfMemory and census_budget.isExhausted() and count > 1) return false;
                    return err;
                };
                const low = ca.alloc(i64, groups_.representatives.len) catch |err| {
                    if (err == error.OutOfMemory and census_budget.isExhausted() and count > 1) return false;
                    return err;
                };
                @memset(high, null);
                for (groups_.rows, 0..) |representative, row| {
                    const input = numeric_inputs.cell(ca, inputs, row, input_column) catch |err| {
                        if (err == error.OutOfMemory and census_budget.isExhausted() and count > 1) return false;
                        return err;
                    };
                    if (input.sql_null) continue;
                    const number = input.numeric orelse return error.SqlTypeMismatch;
                    if (number.kind != .finite or number.digits.len == 0) continue;
                    const bottom = @as(i64, number.weight) - @as(i64, @intCast(number.digits.len)) + 1;
                    if (high[representative]) |previous| {
                        high[representative] = @max(previous, number.weight);
                        low[representative] = @min(low[representative], bottom);
                    } else {
                        high[representative] = number.weight;
                        low[representative] = bottom;
                    }
                }
                for (groups_.representatives, 0..) |representative, index| if (high[index]) |weight| {
                    needed +|= state.numericGrowth(representative.existing, weight, @intCast(@as(i64, weight) - low[index] + 1));
                };
            };
            for (self.specs, self.state_columns, inputs, physical_inputs) |spec, *state, column, native| if (spec.kind == .min or spec.kind == .max) {
                var growth: usize = 0;
                if (native) |view| {
                    growth = try state.physicalUpdateGrowth(view, additional);
                } else {
                    for (0..count) |index| growth = @max(growth, state.updateGrowth(try column.cell(a, index, 0), additional));
                }
                needed +|= growth;
                if (state.values == .dynamic) needed +|= groups_.representatives.len *| 512;
            };
        }
        const headroom = @min(self.budget.limit -| self.budget.live, MemoryBudget.headroom(self.backing) orelse std.math.maxInt(usize));
        fast = fast and census != null and needed <= headroom;
        if (!fast) {
            // A large census or a new-state reservation must not deopt an
            // entire page. Admit bounded prefixes before spilling the suffix.
            // Existing fixed-width groups need no new retained allocation.
            var typed = self.external == null;
            for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or (spec.input_element == .numeric and !exactMergeable(spec))) {
                typed = false;
            };
            if (typed and count > 1) return false;
            for (0..count) |index| {
                if (self.external != null and self.external.?.partitioned) {
                    const selected = try a.alloc(usize, count - index);
                    for (selected, index..) |*row, i| row.* = i;
                    const key_parts = try a.alloc(@import("execution_batch.zig").Batch, keys.len);
                    const input_parts = try a.alloc(@import("execution_batch.zig").Batch, inputs.len);
                    for (keys, key_parts) |*column, *part| part.* = try column.select(a, selected);
                    for (inputs, input_parts) |*column, *part| part.* = try column.select(a, selected);
                    const ids_: ?InputIdentity = if (identity) |ids_| .{ .ordinals = ids_.ordinals[index..], .hashes = ids_.hashes[index..] } else null;
                    if (try self.spillEncodedColumns(key_parts, input_parts, selected.len, ids_)) return true;
                }
                for (physical_keys, row_keys, 0..) |native, *cell, col| cell.* = if (native) |view| try view.cell(index) else try numeric_keys.cell(a, keys, index, col);
                for (row_inputs, 0..) |*cell, col| cell.* = try numeric_inputs.cell(a, inputs, index, col);
                if (identity) |ids| self.source_ordinal = ids.ordinals[index];
                try self.add(row_keys, row_inputs);
            }
            return true;
        }
        const ids = try a.alloc(usize, count);
        self.key_count = keys.len;
        errdefer self.failed = true;
        if (keys.len == 0) {
            try self.ensureGlobalGroup();
            @memset(ids, 0);
            for (self.state_columns, inputs, physical_inputs) |*column, values, native| {
                if (native) |physical| if (try column.updatePhysical(self.budget.allocator(), ids, physical)) continue;
                try column.updateEncoded(self.budget.allocator(), ids, values);
            }
            self.rows_seen = std.math.add(u64, self.rows_seen, count) catch return error.SqlNumericOutOfRange;
            return true;
        }
        const groups_ = census.?;
        for (groups_.representatives) |*representative| {
            for (physical_keys, row_keys, 0..) |native, *cell, col| cell.* = if (native) |view| try view.cell(representative.row) else try numeric_keys.cell(a, keys, representative.row, col);
            self.source_ordinal = if (identity) |identity_| identity_.ordinals[representative.row] else previous_ordinal orelse (std.math.add(u64, self.rows_seen, representative.row) catch return error.SqlNumericOutOfRange);
            representative.group = representative.existing orelse try self.appendGroup(row_keys, representative.hash);
        }
        for (ids, groups_.rows) |*id, representative| id.* = groups_.representatives[representative].group;
        self.rows_seen = std.math.add(u64, self.rows_seen, count) catch return error.SqlNumericOutOfRange;
        for (self.state_columns, inputs, physical_inputs) |*column, values, native| {
            if (native) |physical| if (try column.updatePhysical(self.budget.allocator(), ids, physical)) continue;
            try column.updateEncoded(self.budget.allocator(), ids, values);
        }
        return true;
    }

    fn spillEncodedColumns(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, identity: ?InputIdentity) !bool {
        const external = self.external orelse return false;
        if (!external.partitioned) return false;
        var scratch = std.heap.ArenaAllocator.init(self.backing);
        defer scratch.deinit();
        const a = scratch.allocator();
        const hashes = if (identity) |ids| ids.hashes else try @import("batch_hash.zig").encodedColumns(a, keys, count, true);
        const ordinals = if (identity) |ids| ids.ordinals else blk: {
            const values = try a.alloc(u64, count);
            for (values, 0..) |*value, i| value.* = self.source_ordinal orelse (std.math.add(u64, self.rows_seen, i) catch return error.SqlNumericOutOfRange);
            break :blk values;
        };
        const seen = std.math.add(u64, self.rows_seen, count) catch return error.SqlNumericOutOfRange;
        errdefer self.failed = true;
        try external.addBatch(keys, inputs, hashes, ordinals);
        self.rows_seen = seen;
        return true;
    }

    /// Global COUNT, integer SUM and boolean reductions do not need a hash
    /// probe per row. Validate the batch before changing state; unsupported
    /// kinds, DISTINCT, mixed values and disk groups retain ordered updates.
    pub fn canRetain(self: *const Grouped, keys: []const Datum, inputs: []const Datum) !bool {
        return self.canRetainAdditional(keys, inputs, 0);
    }
    fn canRetainAdditional(self: *const Grouped, keys: []const Datum, inputs: []const Datum, extra: usize) !bool {
        if (inputs.len != self.state_columns.len) return error.InvalidSqlBackendResponse;
        var needed: usize = extra +| (4096 + self.specs.len * @sizeOf(Aggregate) * 2);
        needed +|= (try self.key_columns.appendBytes(keys)) *| 4;
        for (inputs) |input| needed +|= (try datumBytes(input)) *| 4;
        needed +|= try self.numericGrowth(keys, inputs);
        for (self.state_columns, inputs) |*column, input| needed +|= column.updateGrowth(input, 1);
        return self.budget.live <= self.budget.limit / 2 and needed <= self.budget.limit -| self.budget.live;
    }
    fn numericGrowth(self: *const Grouped, keys: []const Datum, inputs: []const Datum) !usize {
        if (inputs.len == 0) return 0;
        const present = for (self.specs) |spec| {
            if (spec.input_element == .numeric and (spec.kind == .sum or spec.kind == .avg)) break true;
        } else false;
        if (!present) return 0;
        const slot = try self.findGroup(keys);
        var bytes: usize = 0;
        for (self.specs, self.state_columns, inputs) |spec, column, input| {
            if (input.sql_null or spec.input_element != .numeric or (spec.kind != .sum and spec.kind != .avg)) continue;
            const value = input.numeric orelse return error.SqlTypeMismatch;
            if (value.kind == .finite) bytes +|= column.numericGrowth(slot, value.weight, value.digits.len);
        }
        return bytes;
    }
    fn numericStateGrowth(self: *const Grouped, keys: []const Datum, states: []const Aggregate, slots: ?[]const u16) !usize {
        const present = for (states) |state| {
            if (state.numeric != null) break true;
        } else false;
        if (!present) return 0;
        const slot = try self.findGroup(keys);
        var bytes: usize = 0;
        for (states, 0..) |state, index| if (state.numeric) |reducer| {
            const column = if (slots) |mapping| mapping[index] else index;
            bytes +|= self.state_columns[column].numericGrowth(slot, reducer.state.weight, reducer.state.buckets.items.len);
        };
        return bytes;
    }
    pub fn addOrdered(self: *Grouped, keys: []const Datum, inputs: []const Datum, ordinal: u64) !void {
        const prior_groups = self.groups.items.len;
        try self.add(keys, inputs);
        const index = self.last_group orelse return error.InvalidSqlBackendResponse;
        self.groups.items[index].ordinal = if (index >= prior_groups) ordinal else @min(self.groups.items[index].ordinal, ordinal);
    }
    /// Partition immutable in-memory partials by their existing semantic hash.
    /// Each source group has exactly one owner; no keys or states are copied.
    pub fn partitionGroups(self: *const Grouped, a: Allocator, count: usize) ![][]usize {
        if (count == 0 or count > 4 or self.external != null or self.reducers != null) return error.InvalidSqlBackendResponse;
        const partitions = try a.alloc([]usize, count);
        errdefer a.free(partitions);
        var sizes: [4]usize = @splat(0);
        var heads = self.heads.iterator();
        while (heads.next()) |entry| {
            const owner = entry.key_ptr.* % count;
            var next: ?usize = entry.value_ptr.*;
            while (next) |index| : (next = self.groups.items[index].next) sizes[owner] += 1;
        }
        var initialized: usize = 0;
        errdefer for (partitions[0..initialized]) |indices| a.free(indices);
        for (partitions, sizes[0..count]) |*indices, size| {
            indices.* = try a.alloc(usize, size);
            initialized += 1;
        }
        sizes = @splat(0);
        heads = self.heads.iterator();
        while (heads.next()) |entry| {
            const owner = entry.key_ptr.* % count;
            var next: ?usize = entry.value_ptr.*;
            while (next) |index| : (next = self.groups.items[index].next) {
                partitions[owner][sizes[owner]] = index;
                sizes[owner] += 1;
            }
        }
        return partitions;
    }
    /// Merge exact local reducers, including spilled states, without narrowing
    /// partial sums. Worker-local ordinals occupy disjoint ranges; consumers
    /// requiring source-order numeric semantics never use this reduction.
    pub fn mergeExact(self: *Grouped, source: *Grouped, ordinal_base: u64) !void {
        return self.mergeExactSelected(source, ordinal_base, null);
    }
    pub fn mergeExactSelected(self: *Grouped, source: *Grouped, ordinal_base: u64, selection: ?[]const usize) !void {
        if (selection != null and source.external != null) return error.InvalidSqlBackendResponse;
        if (self.failed or self.finished or self.specs.len != source.specs.len) return error.InvalidSqlBackendResponse;
        var arena = std.heap.ArenaAllocator.init(self.backing);
        defer arena.deinit();
        if (source.external != null) {
            const prior_rows = self.rows_seen;
            while (true) {
                _ = arena.reset(.retain_capacity);
                const partial = (try source.nextPartialResult(arena.allocator())) orelse break;
                try self.importPartial(partial.keys, partial.aggregates, try std.math.add(u64, ordinal_base, partial.ordinal));
            }
            self.rows_seen = try std.math.add(u64, prior_rows, source.rows_seen);
            return;
        }
        var spill_scratch = std.heap.ArenaAllocator.init(self.backing);
        defer spill_scratch.deinit();
        const states = try self.backing.alloc(Aggregate, self.specs.len);
        defer self.backing.free(states);
        const empty = try self.backing.alloc(Datum, self.specs.len);
        defer self.backing.free(empty);
        @memset(empty, .{});
        const length = if (selection) |selected| selected.len else source.groups.items.len;
        var begin: usize = 0;
        while (begin < length) {
            _ = arena.reset(.retain_capacity);
            const a = arena.allocator();
            const count = @min(64, length - begin);
            // Fixed-width partials admit a whole batch before changing groups.
            // Variable-width and exponent-gap states use precise per-group
            // admission and merge borrowed typed state directly.
            var fixed = self.external == null;
            for (source.state_columns) |column| switch (column.values) {
                .counts, .integers, .booleans, .fixed => {},
                else => fixed = false,
            };
            const rows = try a.alloc([]const Datum, count);
            const width = source.key_columns.columns.len;
            const cells = try a.alloc(Datum, count * width);
            var needed: usize = count *| (4096 + self.specs.len *| @sizeOf(Aggregate) *| 2);
            for (rows, 0..) |*keys, item| {
                const index = if (selection) |selected| selected[begin + item] else begin + item;
                const offset = item * width;
                keys.* = cells[offset..][0..width];
                for (cells[offset..][0..width], 0..) |*cell, column| cell.* = try source.key_columns.cell(a, index, column);
                needed +|= (try self.key_columns.appendBytes(keys.*)) *| 4;
            }
            if (fixed and self.budget.live <= self.budget.limit / 2 and needed <= self.budget.limit -| self.budget.live) {
                var ids: [64]usize = undefined;
                for (rows, ids[0..count], 0..) |keys, *id, item| {
                    const index = if (selection) |selected| selected[begin + item] else begin + item;
                    self.key_count = keys.len;
                    const previous = self.groups.items.len;
                    id.* = try self.resolveGroup(keys);
                    const ordinal = std.math.add(u64, ordinal_base, source.groups.items[index].ordinal) catch return error.SqlNumericOutOfRange;
                    self.groups.items[id.*].ordinal = if (id.* >= previous) ordinal else @min(self.groups.items[id.*].ordinal, ordinal);
                }
                for (self.state_columns, source.state_columns) |*column, *from| try column.mergeSelectedBatch(self.budget.allocator(), ids[0..count], from, begin, if (selection) |selected| selected[begin..][0..count] else null);
            } else for (rows, 0..) |keys, item| {
                const index = if (selection) |selected| selected[begin + item] else begin + item;
                const ordinal = std.math.add(u64, ordinal_base, source.groups.items[index].ordinal) catch return error.SqlNumericOutOfRange;
                self.key_count = keys.len;
                if (self.external == null and self.limits.spill != null) {
                    const slot = try self.findGroup(keys);
                    var bytes: usize = 0;
                    for (self.state_columns, source.state_columns) |*column, *from| bytes +|= try column.mergeGrowth(slot, from, index);
                    if (!try self.canRetainAdditional(keys, empty, bytes)) try self.startSpill();
                }
                if (self.external) |external| {
                    _ = spill_scratch.reset(.retain_capacity);
                    for (source.state_columns, states) |*column, *state| state.* = try column.snapshot(spill_scratch.allocator(), index);
                    try external.partial(keys, states, ordinal);
                } else {
                    const previous = self.groups.items.len;
                    const slot = try self.resolveGroup(keys);
                    self.groups.items[slot].ordinal = if (slot >= previous) ordinal else @min(self.groups.items[slot].ordinal, ordinal);
                    for (self.state_columns, source.state_columns) |*column, *from| try column.mergeBatch(self.budget.allocator(), &.{slot}, from, index);
                }
            }
            begin += count;
        }
        self.rows_seen = std.math.add(u64, self.rows_seen, source.rows_seen) catch return error.SqlNumericOutOfRange;
    }
    pub fn importPartial(self: *Grouped, keys: []const Datum, cells: []const Datum, ordinal: u64) !void {
        return self.importPartialMapped(keys, cells, null, ordinal);
    }
    pub fn importPartialMapped(self: *Grouped, keys: []const Datum, cells: []const Datum, slots: ?[]const u16, ordinal: u64) !void {
        return self.importPartialMappedAdmitted(keys, cells, slots, ordinal, false);
    }
    /// Decode once and prove the entire merge allocation before changing any
    /// group. Partition reducers may spill a declined checkpoint unchanged.
    pub fn tryImportPartial(self: *Grouped, keys: []const Datum, cells: []const Datum, ordinal: u64) !bool {
        self.importPartialMappedAdmitted(keys, cells, null, ordinal, true) catch |err| return switch (err) {
            error.SqlAggregateAdmissionExceeded => false,
            else => err,
        };
        return true;
    }
    fn importPartialMappedAdmitted(self: *Grouped, keys: []const Datum, cells: []const Datum, slots: ?[]const u16, ordinal: u64, require_fit: bool) !void {
        if (slots) |mapping| {
            if (mapping.len == 0 or mapping.len != cells.len or mapping.len > self.specs.len) return error.InvalidSqlSpill;
            for (mapping, 0..) |slot, i| {
                if (slot >= self.specs.len) return error.InvalidSqlSpill;
                for (mapping[0..i]) |prior| if (prior == slot) return error.InvalidSqlSpill;
            }
        } else if (cells.len != self.specs.len) return error.InvalidSqlSpill;
        var arena = std.heap.ArenaAllocator.init(self.backing);
        defer arena.deinit();
        const a = arena.allocator();
        const states = try a.alloc(Aggregate, cells.len);
        var initialized: usize = 0;
        defer for (states[0..initialized]) |*state| state.deinit();
        // Decode and validate every signature before mutating destination state.
        for (states, cells, 0..) |*state, value, i| {
            const slot = if (slots) |mapping| mapping[i] else i;
            state.* = try @import("aggregate_partial.zig").decode(a, value, self.specs[slot]);
            initialized += 1;
        }
        return self.importTypedPartialMappedAdmitted(keys, states, slots, ordinal, require_fit);
    }
    /// Import an entire provider page without reboxing typed reducer payloads.
    pub fn importPartialBatch(self: *Grouped, rows: []const GroupResult) !void {
        for (rows) |row| {
            const count = try @import("aggregate_partial.zig").partialCount(row);
            if (row.typed_states) |states| {
                try self.importTypedPartialMapped(row.keys, states, row.aggregate_slots, row.ordinal);
            } else if (row.state_reader != null) {
                var arena = std.heap.ArenaAllocator.init(self.backing);
                defer arena.deinit();
                const states = try arena.allocator().alloc(Aggregate, count);
                for (states, 0..) |*state, index| {
                    const slot = if (row.aggregate_slots) |slots| slots[index] else index;
                    if (slot >= self.specs.len) return error.InvalidSqlBackendResponse;
                    state.* = try @import("aggregate_partial.zig").read(arena.allocator(), row, index, self.specs[slot]);
                }
                try self.importTypedPartialMapped(row.keys, states, row.aggregate_slots, row.ordinal);
            } else try self.importPartialMapped(row.keys, row.aggregates, row.aggregate_slots, row.ordinal);
        }
    }
    pub fn importTypedPartialMapped(self: *Grouped, keys: []const Datum, states: []const Aggregate, slots: ?[]const u16, ordinal: u64) !void {
        return self.importTypedPartialMappedAdmitted(keys, states, slots, ordinal, false);
    }
    fn importTypedPartialMappedAdmitted(self: *Grouped, keys: []const Datum, states: []const Aggregate, slots: ?[]const u16, ordinal: u64, require_fit: bool) !void {
        if (slots) |mapping| {
            if (mapping.len == 0 or mapping.len != states.len or mapping.len > self.specs.len) return error.InvalidSqlSpill;
            for (mapping, 0..) |slot, i| {
                if (slot >= self.specs.len) return error.InvalidSqlSpill;
                for (mapping[0..i]) |prior| if (prior == slot) return error.InvalidSqlSpill;
            }
        } else if (states.len != self.specs.len) return error.InvalidSqlSpill;
        for (states, 0..) |state, i| {
            const spec = self.specs[if (slots) |mapping| mapping[i] else i];
            if (state.kind != spec.kind or state.input_type != spec.input_type or state.input_element != spec.input_element or state.distinct != spec.distinct) return error.InvalidSqlSpill;
        }
        var arena = std.heap.ArenaAllocator.init(self.backing);
        defer arena.deinit();
        const a = arena.allocator();
        var input_storage: [256]Datum = undefined;
        const inputs = input_storage[0..self.specs.len];
        @memset(inputs, .{});
        self.key_count = keys.len;
        var state_bytes: usize = 0;
        for (states) |state| {
            if (state.numeric) |reducer| state_bytes +|= (reducer.state.buckets.items.len *| @sizeOf(i128) +| @sizeOf(@import("numeric_aggregate.zig").Reducer)) *| 4;
            if (state.selected) |selected| state_bytes +|= (try datumBytes(selected.row.values[0])) *| 4;
            for (state.distinct_values.items) |entry| state_bytes +|= (try datumBytes(entry.row.row.values[0]) +| @sizeOf(Aggregate)) *| 4;
        }
        state_bytes +|= try self.numericStateGrowth(keys, states, slots);
        if (self.external == null and !try self.canRetainAdditional(keys, inputs, state_bytes)) {
            if (self.limits.spill != null) try self.startSpill() else if (require_fit) return error.SqlAggregateAdmissionExceeded;
        }
        if (self.external) |external| {
            if (slots) |mapping| {
                // Existing spill frames are dense. Expand only at this durable
                // boundary, while the in-memory typed columns touch used slots.
                const dense = try a.alloc(Aggregate, self.specs.len);
                var initialized_dense: usize = 0;
                defer for (dense[0..initialized_dense]) |*state| state.deinit();
                for (dense, self.specs) |*state, spec| {
                    state.* = try Aggregate.initTyped(a, spec.kind, spec.input_type, spec.input_element);
                    state.distinct = spec.distinct;
                    initialized_dense += 1;
                }
                for (mapping, states) |slot, value| {
                    // A separate decode gives the dense frame its own payload
                    // ownership and keeps failure cleanup independent.
                    const replacement = try @import("aggregate_partial.zig").decode(a, try @import("aggregate_partial.zig").cell(a, value), self.specs[slot]);
                    dense[slot].deinit();
                    dense[slot] = replacement;
                }
                return external.partial(keys, dense, ordinal);
            }
            return external.partial(keys, states, ordinal);
        }
        errdefer self.failed = true;
        const previous = self.groups.items.len;
        const index = try self.resolveGroup(keys);
        self.groups.items[index].ordinal = if (index >= previous) ordinal else @min(self.groups.items[index].ordinal, ordinal);
        for (states, 0..) |state, i| {
            const slot = if (slots) |mapping| mapping[i] else i;
            try self.state_columns[slot].mergeExact(self.budget.allocator(), index, state);
        }
    }
    pub fn exportPartial(self: *Grouped, external: *@import("spill_grouped.zig").Grouped) !void {
        return self.exportPartialAt(external, 0);
    }
    pub fn exportPartialAt(self: *Grouped, external: *@import("spill_grouped.zig").Grouped, ordinal_base: u64) !void {
        var scratch = std.heap.ArenaAllocator.init(self.backing);
        defer scratch.deinit();
        for (self.groups.items, 0..) |group, index| {
            _ = scratch.reset(.free_all);
            const a = scratch.allocator();
            const states = try a.alloc(Aggregate, self.specs.len);
            for (self.state_columns, states) |column, *state| state.* = try column.snapshot(a, index);
            try external.partial(try self.key_columns.row(a, index), states, try std.math.add(u64, ordinal_base, group.ordinal));
        }
    }

    pub fn addGlobalBatch(self: *Grouped, inputs: []const []const Datum) !void {
        if (self.failed or self.finished or (self.key_count != null and self.key_count.? != 0)) return error.InvalidSqlBackendResponse;
        var supported = self.external == null;
        for (self.specs) |spec| supported = supported and !spec.distinct and (spec.kind == .count or spec.kind == .bool_and or spec.kind == .bool_or or (spec.kind == .sum and spec.input_type == .integer));
        for (inputs) |row| {
            if (row.len != self.specs.len) return error.InvalidSqlBackendResponse;
            for (row, self.specs) |value, spec| {
                if (value.sql_null or spec.kind == .count) continue;
                if ((spec.kind == .sum and value.value != .integer) or ((spec.kind == .bool_and or spec.kind == .bool_or) and value.value != .bool)) supported = false;
            }
        }
        if (!supported) {
            for (inputs) |row| try self.add(&.{}, row);
            return;
        }
        if (inputs.len == 0) return;
        // First-row admission retains the usual spill/budget decision.
        try self.add(&.{}, inputs[0]);
        if (self.external != null) {
            for (inputs[1..]) |row| try self.add(&.{}, row);
            return;
        }
        errdefer self.failed = true;
        const remaining = inputs[1..];
        for (self.state_columns, 0..) |*states, column| {
            var snapshot = try states.snapshot(self.backing, 0);
            const state = &snapshot;
            var index: usize = 0;
            while (index < remaining.len) {
                var counts: [4]u64 = @splat(0);
                var integers: [4]i128 = @splat(0);
                var booleans: [4]bool = @splat(state.kind == .bool_and);
                const count = @min(@as(usize, 4), remaining.len - index);
                for (0..count) |lane| {
                    const value = remaining[index + lane][column];
                    if (value.sql_null) continue;
                    counts[lane] = 1;
                    if (state.kind == .sum) integers[lane] = value.value.integer;
                    if (state.kind == .bool_and or state.kind == .bool_or) booleans[lane] = value.value.bool;
                }
                const added = @reduce(.Add, @as(@Vector(4, u64), counts));
                if (added > @as(u64, std.math.maxInt(i64)) - state.count) return error.SqlNumericOutOfRange;
                state.count += added;
                if (state.kind == .sum) state.integer_sum = std.math.add(i128, state.integer_sum, @reduce(.Add, @as(@Vector(4, i128), integers))) catch return error.SqlNumericOutOfRange;
                if (state.kind == .bool_and) state.boolean = state.boolean and @reduce(.And, @as(@Vector(4, bool), booleans));
                if (state.kind == .bool_or) state.boolean = state.boolean or @reduce(.Or, @as(@Vector(4, bool), booleans));
                index += count;
            }
            states.set(0, snapshot);
        }
        self.rows_seen = std.math.add(u64, self.rows_seen, remaining.len) catch return error.SqlNumericOutOfRange;
    }

    pub fn addGlobalColumns(self: *Grouped, inputs: []const []const Datum, row_count: usize) !void {
        if (self.failed or self.finished or (self.key_count != null and self.key_count.? != 0)) return error.InvalidSqlBackendResponse;
        var supported = self.external == null;
        for (self.specs) |spec| supported = supported and !spec.distinct and (spec.kind == .count or spec.kind == .bool_and or spec.kind == .bool_or or (spec.kind == .sum and spec.input_type == .integer));
        if (inputs.len != self.specs.len) return error.InvalidSqlBackendResponse;
        for (inputs, self.specs) |column, spec| {
            if (column.len != row_count) return error.InvalidSqlBackendResponse;
            for (column) |value| {
                if (value.sql_null or spec.kind == .count) continue;
                if ((spec.kind == .sum and value.value != .integer) or ((spec.kind == .bool_and or spec.kind == .bool_or) and value.value != .bool)) supported = false;
            }
        }
        const row = try self.backing.alloc(Datum, inputs.len);
        defer self.backing.free(row);
        if (!supported) {
            for (0..row_count) |index| {
                for (inputs, row) |column, *cell| cell.* = column[index];
                try self.add(&.{}, row);
            }
            return;
        }
        if (row_count == 0) return;
        // First-row admission retains the usual spill/budget decision.
        for (inputs, row) |column, *cell| cell.* = column[0];
        try self.add(&.{}, row);
        if (self.external != null) {
            for (1..row_count) |index| {
                for (inputs, row) |column, *cell| cell.* = column[index];
                try self.add(&.{}, row);
            }
            return;
        }
        errdefer self.failed = true;
        const remaining = row_count - 1;
        for (self.state_columns, 0..) |*states, column| {
            var snapshot = try states.snapshot(self.backing, 0);
            const state = &snapshot;
            var index: usize = 0;
            while (index < remaining) {
                var counts: [4]u64 = @splat(0);
                var integers: [4]i128 = @splat(0);
                var booleans: [4]bool = @splat(state.kind == .bool_and);
                const count = @min(@as(usize, 4), remaining - index);
                for (0..count) |lane| {
                    const value = inputs[column][1 + index + lane];
                    if (value.sql_null) continue;
                    counts[lane] = 1;
                    if (state.kind == .sum) integers[lane] = value.value.integer;
                    if (state.kind == .bool_and or state.kind == .bool_or) booleans[lane] = value.value.bool;
                }
                const added = @reduce(.Add, @as(@Vector(4, u64), counts));
                if (added > @as(u64, std.math.maxInt(i64)) - state.count) return error.SqlNumericOutOfRange;
                state.count += added;
                if (state.kind == .sum) state.integer_sum = std.math.add(i128, state.integer_sum, @reduce(.Add, @as(@Vector(4, i128), integers))) catch return error.SqlNumericOutOfRange;
                if (state.kind == .bool_and) state.boolean = state.boolean and @reduce(.And, @as(@Vector(4, bool), booleans));
                if (state.kind == .bool_or) state.boolean = state.boolean or @reduce(.Or, @as(@Vector(4, bool), booleans));
                index += count;
            }
            states.set(0, snapshot);
        }
        self.rows_seen = std.math.add(u64, self.rows_seen, remaining) catch return error.SqlNumericOutOfRange;
    }

    pub fn ensureGlobalGroup(self: *Grouped) !void {
        if (self.rows_seen != 0 or self.groups.items.len != 0) return;
        self.key_count = 0;
        var hasher = std.hash.Wyhash.init(0);
        _ = try self.appendGroup(&.{}, hasher.final());
    }

    /// Seed exact COUNT(*) states supplied by an authorized snapshot provider.
    /// Other aggregates still consume typed input pages through add().
    pub fn addGlobalCount(self: *Grouped, count: u64) !void {
        if (self.failed or self.finished or self.groups.items.len != 1 or self.key_count != 0 or self.rows_seen != 0) return error.InvalidSqlBackendResponse;
        for (self.specs) |spec| if (spec.kind != .count or spec.distinct) return error.InvalidSqlBackendResponse;
        if (count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        for (self.state_columns) |*column| column.values.counts.items[0] = count;
    }

    pub fn groupCount(self: *const Grouped) usize {
        if (self.finalized) |source| return source.count;
        if (self.reducers) |reducers| {
            var count: usize = 0;
            for (reducers.groups) |group| count +|= group.groupCount();
            return count;
        }
        return if (self.external != null) @intCast(self.rows_seen) else self.groups.items.len;
    }

    pub fn resultAt(self: *Grouped, alloc: Allocator, index: usize) !GroupResult {
        if (self.failed or index >= self.groups.items.len) return error.InvalidSqlBackendResponse;
        self.finished = true;
        const values = try alloc.alloc(Datum, self.specs.len);
        errdefer alloc.free(values);
        for (self.state_columns, values) |*column, *value| value.* = try column.finish(index);
        return .{ .keys = try self.key_columns.row(self.results.allocator(), index), .aggregates = values, .ordinal = self.groups.items[index].ordinal };
    }

    fn update(self: *Grouped, keys: []const Datum, inputs: []const Datum) !void {
        if (inputs.len != self.specs.len or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        self.key_count = keys.len;
        const slot = try self.resolveGroup(keys);
        for (self.state_columns, inputs) |*column, value| try column.update(self.budget.allocator(), slot, value);
        self.rows_seen = std.math.add(u64, self.rows_seen, 1) catch return error.SqlNumericOutOfRange;
    }

    fn groupHash(keys: []const Datum) !u64 {
        var hasher = std.hash.Wyhash.init(0);
        for (keys) |key| {
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(key.sql_null);
            std.mem.writeInt(u64, bytes[1..9], if (key.sql_null) 0 else try scalar.semanticHashDatum(key), .little);
            hasher.update(&bytes);
        }
        return hasher.final();
    }
    fn findGroup(self: *const Grouped, keys: []const Datum) !?usize {
        var slot = self.heads.get(try groupHash(keys));
        while (slot) |index| {
            if (try self.key_columns.equal(self.backing, index, keys, true)) return index;
            slot = self.groups.items[index].next;
        }
        return null;
    }
    fn resolveGroup(self: *Grouped, keys: []const Datum) !usize {
        return self.resolveGroupHashed(keys, try groupHash(keys));
    }
    fn resolveGroupHashed(self: *Grouped, keys: []const Datum, hash: u64) !usize {
        var slot = self.heads.get(hash);
        while (slot) |index| {
            self.hash_probes += 1;
            const group = &self.groups.items[index];
            if (try self.key_columns.equal(self.backing, index, keys, true)) break;
            slot = group.next;
        }
        if (slot == null) slot = try self.appendGroup(keys, hash);
        self.last_group = slot;
        return slot.?;
    }

    fn appendGroup(self: *Grouped, keys: []const Datum, hash: u64) !usize {
        if (self.groups.items.len >= self.limits.groups) return error.SqlProgramLimitExceeded;
        const alloc = self.budget.allocator();
        try self.groups.ensureUnusedCapacity(alloc, 1);
        try self.heads.ensureUnusedCapacity(alloc, 1);
        _ = try self.key_columns.append(keys);
        for (self.state_columns) |*column| try column.append(alloc);
        const index = self.groups.items.len;
        self.groups.appendAssumeCapacity(.{ .next = self.heads.get(hash), .ordinal = self.source_ordinal orelse self.rows_seen });
        self.heads.putAssumeCapacity(hash, index);
        return index;
    }

    /// Returned arrays belong to alloc; nested values remain borrowed from this
    /// operator. Use a caller arena or individually free each aggregates slice.
    pub fn finish(self: *Grouped, alloc: Allocator) ![]GroupResult {
        if (self.failed) return error.InvalidSqlBackendResponse;
        if (self.finalized != null) {
            var rows: std.ArrayList(GroupResult) = .empty;
            errdefer rows.deinit(alloc);
            while (try self.nextResult(alloc)) |row| try rows.append(alloc, row);
            return rows.toOwnedSlice(alloc);
        }
        self.finished = true;
        const result = try alloc.alloc(GroupResult, self.groups.items.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |group| alloc.free(group.aggregates);
            alloc.free(result);
        }
        for (result, 0..) |*output, i| {
            output.* = try self.resultAt(alloc, i);
            initialized += 1;
        }
        return result;
    }
};

pub fn cloneDatum(alloc: Allocator, value: Datum) anyerror!Datum {
    if (value.numeric) |number| {
        if (value.sql_null or value.array != null or value.patterns != null or value.value != .null) return error.SqlTypeMismatch;
        const exact = @import("numeric_value.zig");
        var ctx: exact.Context = .{ .alloc = alloc };
        try exact.validateCanonical(&ctx, number.*);
        const owned = try alloc.create(exact.Value);
        errdefer alloc.destroy(owned);
        owned.* = number.*;
        owned.digits = try alloc.dupe(u16, number.digits);
        return Datum.typedNumeric(owned);
    }
    if (value.array) |array| {
        if (value.sql_null or value.patterns != null or value.value != .null) return error.SqlTypeMismatch;
        const owned = try alloc.create(@import("array_value.zig").Value);
        const dimensions = try alloc.dupe(@import("array_value.zig").Dimension, array.dimensions);
        if (array.stored) |stored| {
            const copy = try alloc.create(@import("../common/sql_array_layout.zig").View);
            copy.* = stored.*;
            copy.bytes = try alloc.dupe(u8, stored.bytes);
            owned.* = .{ .element_type = array.element_type, .dimensions = dimensions, .stored = copy };
            return Datum.typedArray(owned);
        }
        const elements = try alloc.alloc(Datum, array.cardinality());
        var footprint: usize = @sizeOf(@import("array_value.zig").Value) + dimensions.len * @sizeOf(@import("array_value.zig").Dimension);
        for (0..array.cardinality(), elements) |array_cell_0, *out| {
            const element = array.flatCell(array_cell_0);
            if (element.array != null) return error.SqlTypeMismatch;
            out.* = try cloneDatum(alloc, element);
            if (array.prepared_footprint == null) footprint +|= try datumBytes(out.*);
        }
        owned.* = .{ .element_type = array.element_type, .dimensions = dimensions, .elements = elements, .prepared_footprint = array.prepared_footprint orelse footprint };
        return Datum.typedArray(owned);
    }
    return .{ .value = try cloneJson(alloc, value.value, 0), .sql_null = value.sql_null, .patterns = value.patterns };
}

fn cloneJson(alloc: Allocator, value: Json, depth: usize) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try alloc.dupe(u8, text) },
        .array => |array| blk: {
            var result: std.array_list.Managed(Json) = .init(alloc);
            try result.ensureTotalCapacity(array.items.len);
            for (array.items) |item| result.appendAssumeCapacity(try cloneJson(alloc, item, depth + 1));
            break :blk .{ .array = result };
        },
        .object => |object| blk: {
            var result: std.json.ObjectMap = .empty;
            try result.ensureTotalCapacity(alloc, @intCast(object.count()));
            for (object.keys(), object.values()) |key, item| try result.put(alloc, try alloc.dupe(u8, key), try cloneJson(alloc, item, depth + 1));
            break :blk .{ .object = result };
        },
        else => value,
    };
}
pub fn datumBytes(value: Datum) anyerror!usize {
    if (value.numeric) |number| return @sizeOf(Datum) + @sizeOf(@import("numeric_value.zig").Value) + number.digits.len * 2;
    if (value.array) |array| return @sizeOf(Datum) +| try array.retainedBytes();
    return jsonBytes(value.value, 0);
}
fn jsonBytes(value: Json, depth: usize) error{SqlProgramLimitExceeded}!usize {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    var size: usize = @sizeOf(Datum);
    switch (value) {
        .string, .number_string => |text| size +|= text.len,
        .array => |array| for (array.items) |item| {
            size +|= try jsonBytes(item, depth + 1);
        },
        .object => |object| for (object.keys(), object.values()) |key, item| {
            size +|= key.len +| try jsonBytes(item, depth + 1);
        },
        else => {},
    }
    return size;
}

test "SQL typed array physical keys preserve ordering hashes join equality and copied ownership" {
    const arrays = @import("array_value.zig");
    const a = std.testing.allocator;
    const elements = &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = 2 }) };
    const first = try arrays.Value.init(.int32, &.{.{ .length = 2, .lower = 0 }}, elements, .{});
    const second = try arrays.Value.init(.int32, &.{.{ .length = 2, .lower = 1 }}, elements, .{});
    const first_cell = Datum.typedArray(&first);
    const second_cell = Datum.typedArray(&second);
    try std.testing.expectEqual(std.math.Order.lt, try scalar.compareDatums(first_cell, second_cell));
    try std.testing.expect(try scalar.semanticHashDatum(first_cell) != try scalar.semanticHashDatum(second_cell));
    try std.testing.expect(@import("sort_key.zig").encode(&.{first_cell}, &[_]Order{.{}}) == null);
    var top = try TopK.init(a, 2, &.{.{}}, 128 * 1024);
    defer top.deinit();
    try top.add(.{ .values = &.{second_cell}, .keys = &.{second_cell}, .ordinal = 0 });
    try top.add(.{ .values = &.{first_cell}, .keys = &.{first_cell}, .ordinal = 1 });
    const ordered = try top.finish(a);
    defer a.free(ordered);
    try std.testing.expectEqual(@as(u64, 1), ordered[0].ordinal);
    try std.testing.expect(ordered[0].values[0].array.? != &first);
    const join = try HashJoin.create(a, .{});
    defer join.deinit();
    try join.add(&.{first_cell}, &.{first_cell});
    try join.add(&.{second_cell}, &.{second_cell});
    var probe = try join.probe(&.{first_cell});
    const matched = (try probe.next()).?;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const values = try matched.materializeValues(arena.allocator());
    try std.testing.expectEqual(@as(i32, 0), values[0].array.?.dimensions[0].lower);
    try std.testing.expect((try probe.next()) == null);
}

test "SQL top K retains bounded competitive rows with stable null ordering" {
    var top = try TopK.init(std.testing.allocator, 5, &.{.{}}, 128 * 1024);
    defer top.deinit();
    for (0..10000) |i| {
        const value = Datum.json(.{ .integer = @intCast(i) });
        try top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = i });
    }
    const result = try top.finish(std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(usize, 5), top.copied_rows);
    try std.testing.expectEqual(@as(usize, 5), result.len);
    for (result, 0..) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i)), row.values[0].value.integer);
    try std.testing.expect(top.comparisons < 10100);
    std.debug.print("SQL topK: input=10000 k=5 copied={} retained_bytes={} comparisons={}\n", .{ top.copied_rows, top.retained_bytes, top.comparisons });
    var reverse = try TopK.init(std.testing.allocator, 3, &.{.{ .descending = true, .nulls_first = false }}, 128 * 1024);
    defer reverse.deinit();
    for ([_]Datum{ .{}, Datum.json(.{ .integer = 3 }), Datum.json(.{ .integer = 3 }), Datum.json(.{ .integer = 7 }) }, 0..) |value, i| try reverse.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = i });
    const descending = try reverse.finish(std.testing.allocator);
    defer std.testing.allocator.free(descending);
    try std.testing.expectEqual(@as(u64, 3), descending[0].ordinal);
    try std.testing.expectEqual(@as(u64, 1), descending[1].ordinal);
    try std.testing.expectEqual(@as(u64, 2), descending[2].ordinal);
}

test "SQL top K sparse logical ceilings reserve only admitted heap capacity" {
    var top = try TopK.init(std.testing.allocator, 10_000_000, &.{.{}}, 64 * 1024);
    defer top.deinit();
    try std.testing.expectEqual(@as(usize, 0), top.entries.len);
    for ([_]i64{ 2, 1 }, 0..) |number, ordinal| {
        const value = Datum.json(.{ .integer = number });
        try top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = ordinal });
    }
    const rows = try top.finish(std.testing.allocator);
    defer std.testing.allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0].values[0].value.integer);
    try std.testing.expectEqual(@as(i64, 2), rows[1].values[0].value.integer);
    try std.testing.expect(top.retained_bytes < 16 * 1024);
    std.debug.print("SQL sparse topK: logical_k=10000000 rows=2 heap_slots={d} retained_bytes={d}\n", .{ top.entries.len, top.retained_bytes });
}

test "SQL top K growing heaps preserve the logical limit under allocation faults" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            var top = try TopK.init(alloc, 33, &.{.{}}, 256 * 1024);
            defer top.deinit();
            for (0..70) |ordinal| {
                const value = Datum.json(.{ .integer = @intCast(69 - ordinal) });
                try top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = ordinal });
            }
            const rows = try top.finish(alloc);
            defer alloc.free(rows);
            try std.testing.expectEqual(@as(usize, 33), rows.len);
            for (rows, 0..) |row, ordinal| try std.testing.expectEqual(@as(i64, @intCast(ordinal)), row.values[0].value.integer);
            try std.testing.expectEqual(@as(usize, 33), top.entries.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL top K growing heaps retain an intact prefix when the byte budget is exhausted" {
    var top = try TopK.init(std.testing.allocator, 10_000_000, &.{.{}}, 8 * 1024);
    defer top.deinit();
    var admitted: usize = 0;
    for (0..100) |ordinal| {
        const value = Datum.json(.{ .integer = @intCast(ordinal) });
        top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = ordinal }) catch |err| {
            try std.testing.expectEqual(error.SqlProgramLimitExceeded, err);
            break;
        };
        admitted += 1;
    }
    try std.testing.expect(admitted > 0 and admitted < 100);
    try std.testing.expect(top.retained_bytes <= top.max_bytes);
    const rows = try top.finish(std.testing.allocator);
    defer std.testing.allocator.free(rows);
    try std.testing.expectEqual(admitted, rows.len);
    for (rows, 0..) |row, ordinal| try std.testing.expectEqual(@as(i64, @intCast(ordinal)), row.values[0].value.integer);
}

test "SQL top K encoding releases retained rows incrementally" {
    const alloc = std.testing.allocator;
    var top = try TopK.init(alloc, 2, &.{.{}}, 128 * 1024);
    defer top.deinit();
    const text = @as([16384]u8, @splat('x'));
    for (0..2) |i| try top.add(.{ .values = &.{Datum.json(.{ .string = &text })}, .keys = &.{Datum.json(.{ .integer = @intCast(i) })}, .ordinal = i });
    const result = try top.finish(alloc);
    defer alloc.free(result);
    const before = top.retained_bytes;
    top.releaseFinishedRow(0);
    try std.testing.expect(top.retained_bytes < before);
    try std.testing.expectEqualStrings(&text, result[1].values[0].value.string);
    try std.testing.expectError(error.InvalidSqlBackendResponse, top.finish(alloc));
    top.releaseFinishedRow(1);
    try std.testing.expect(top.retained_bytes < 1024);
}

test "SQL aggregates preserve empty null exact integer and large average semantics" {
    var sum = try Aggregate.init(std.testing.allocator, .sum, .integer);
    defer sum.deinit();
    try std.testing.expect((try sum.finish()).sql_null);
    try sum.update(.{});
    try sum.update(Datum.json(.{ .integer = std.math.maxInt(i64) }));
    try sum.update(Datum.json(.{ .integer = 1 }));
    try std.testing.expectError(error.SqlNumericOutOfRange, sum.finish());
    try sum.update(Datum.json(.{ .integer = -1 }));
    try std.testing.expectEqual(std.math.maxInt(i64), (try sum.finish()).value.integer);
    var average = try Aggregate.init(std.testing.allocator, .avg, .number);
    defer average.deinit();
    try average.update(Datum.json(.{ .float = 1e308 }));
    try average.update(Datum.json(.{ .float = 1e308 }));
    try std.testing.expectEqual(@as(f64, 1e308), (try average.finish()).value.float);
    var count = try Aggregate.init(std.testing.allocator, .count, .json);
    defer count.deinit();
    try count.update(.{});
    try count.update(Datum.json(.null));
    try std.testing.expectEqual(@as(i64, 1), (try count.finish()).value.integer);
}

test "SQL top K and aggregate ownership survives every allocation failure" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            var top = try TopK.init(alloc, 2, &.{.{}}, 128 * 1024);
            defer top.deinit();
            for ([_][]const u8{ "zebra", "yak", "apple", "banana", "zebra" }, 0..) |word, ordinal| {
                const datum = Datum.json(.{ .string = word });
                try top.add(.{ .values = &.{datum}, .keys = &.{datum}, .ordinal = ordinal });
            }
            const rows = try top.finish(alloc);
            defer alloc.free(rows);
            try std.testing.expectEqualStrings("apple", rows[0].values[0].value.string);
            try std.testing.expectEqualStrings("banana", rows[1].values[0].value.string);
            var minimum = try Aggregate.init(alloc, .min, .string);
            defer minimum.deinit();
            try minimum.update(Datum.json(.{ .string = "zebra" }));
            try minimum.update(Datum.json(.{ .string = "apple" }));
            try std.testing.expectEqualStrings("apple", (try minimum.finish()).value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL grouped aggregation streams duplicate JSON keys within a hard quota" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = try std.json.parseFromSliceLeaky(Json, arena.allocator(), "{\"a\":1,\"b\":2}", .{ .parse_numbers = false });
    const b = try std.json.parseFromSliceLeaky(Json, arena.allocator(), "{\"b\":2.0,\"a\":1e0}", .{ .parse_numbers = false });
    const grouped = try Grouped.create(std.testing.allocator, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, .{ .groups = 8, .bytes = 16384 });
    defer grouped.deinit();
    for (0..10000) |i| try grouped.add(&.{Datum.json(if (i % 2 == 0) a else b)}, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = 2 }) });
    const result = try grouped.finish(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(i64, 10000), result[0].aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 20000), result[0].aggregates[1].value.integer);
    try std.testing.expect(grouped.budget.peak < 16384);
    std.debug.print("SQL groups: input={} groups={} probes={} peak_bytes={}\n", .{ grouped.rows_seen, result.len, grouped.hash_probes, grouped.budget.peak + @sizeOf(Grouped) });
}

test "SQL grouped aggregation quota and allocation failures retain no state" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const grouped = try Grouped.create(alloc, &.{.{ .kind = .min, .input_type = .string }}, .{});
            defer grouped.deinit();
            try grouped.add(&.{Datum.json(.{ .string = "key" })}, &.{Datum.json(.{ .string = "zebra" })});
            try grouped.add(&.{Datum.json(.{ .string = "key" })}, &.{Datum.json(.{ .string = "apple" })});
            const result = try grouped.finish(alloc);
            defer {
                for (result) |group| alloc.free(group.aggregates);
                alloc.free(result);
            }
            try std.testing.expectEqualStrings("apple", result[0].aggregates[0].value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const grouped = try Grouped.create(std.testing.allocator, &.{.{ .kind = .count }}, .{ .groups = 1, .bytes = 8192 });
    defer grouped.deinit();
    try grouped.add(&.{Datum.json(.{ .integer = 1 })}, &.{Datum.json(.{ .integer = 1 })});
    try std.testing.expectError(error.SqlProgramLimitExceeded, grouped.add(&.{Datum.json(.{ .integer = 2 })}, &.{Datum.json(.{ .integer = 1 })}));
}

test "SQL pattern aggregate retains distinct nullable patterns within quota" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const empty_grouped = try Grouped.create(alloc, &.{.{ .kind = .pattern_set, .input_type = .string, .distinct = true }}, .{ .groups = 1, .bytes = 8192 });
            defer empty_grouped.deinit();
            try empty_grouped.ensureGlobalGroup();
            const empty = try empty_grouped.resultAt(alloc, 0);
            defer alloc.free(empty.aggregates);
            try std.testing.expectEqual(@as(usize, 0), empty.aggregates[0].value.array.items.len);
            const grouped = try Grouped.create(alloc, &.{.{ .kind = .pattern_set, .input_type = .string, .distinct = true }}, .{ .groups = 1, .bytes = 8192 });
            defer grouped.deinit();
            try grouped.add(&.{}, &.{Datum.json(.{ .string = "op%" })});
            try grouped.add(&.{}, &.{Datum.json(.{ .string = "op%" })});
            try grouped.add(&.{}, &.{.{}});
            const result = try grouped.finish(alloc);
            defer {
                for (result) |group| alloc.free(group.aggregates);
                alloc.free(result);
            }
            try std.testing.expectEqual(@as(usize, 2), result[0].aggregates[0].value.array.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const grouped = try Grouped.create(std.testing.allocator, &.{.{ .kind = .pattern_set, .input_type = .string, .distinct = true }}, .{ .groups = 1, .bytes = 2048 });
    defer grouped.deinit();
    var exhausted = false;
    for (0..1000) |index| {
        var buffer: [32]u8 = undefined;
        const pattern = try std.fmt.bufPrint(&buffer, "pattern-{d}%", .{index});
        grouped.add(&.{}, &.{Datum.json(.{ .string = pattern })}) catch |err| {
            try std.testing.expectEqual(error.SqlProgramLimitExceeded, err);
            exhausted = true;
            break;
        };
    }
    try std.testing.expect(exhausted);
}

test "SQL top K admits sparse input under a large logical row limit" {
    var quota: MemoryBudget = .{ .backing = std.testing.allocator, .limit = 8192 };
    var top = try TopK.init(quota.allocator(), 10_000_000, &.{.{}}, 8192);
    defer top.deinit();
    try std.testing.expectEqual(@as(usize, 0), top.entries.len);
    for (0..20) |i| {
        const value = Datum.json(.{ .integer = @intCast(20 - i) });
        try top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = i });
    }
    const result = try top.finish(std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(usize, 20), result.len);
    for (result, 0..) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.values[0].value.integer);
    try std.testing.expect(quota.peak <= quota.limit);
}

test "SQL top K replacement churn stays bounded by actual allocation quota" {
    var quota: MemoryBudget = .{ .backing = std.testing.allocator, .limit = 8192 };
    var top = try TopK.init(quota.allocator(), 5, &.{.{}}, 8192);
    defer top.deinit();
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |i| {
        const value = Datum.json(.{ .integer = @intCast(10000 - i) });
        try top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = i });
    }
    const result = try top.finish(std.testing.allocator);
    defer std.testing.allocator.free(result);
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    try std.testing.expectEqual(@as(usize, 10000), top.copied_rows);
    for (result, 0..) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.values[0].value.integer);
    try std.testing.expectEqual(quota.live, top.retained_bytes + if (top.scratch) |*scratch| arenaBytes(scratch) else @as(usize, 0));
    try std.testing.expect(quota.peak <= quota.limit);
    std.debug.print("SQL topK churn: input=10000 k=5 copied={} retained_bytes={} peak_bytes={} comparisons={} elapsed_ns={}\n", .{ top.copied_rows, top.retained_bytes, quota.peak, top.comparisons, elapsed });
}

test "SQL hash join streams duplicate matches and preserves unmatched SQL NULL rows" {
    const join = try HashJoin.create(std.testing.allocator, .{ .rows = 8, .bytes = 16384 });
    defer join.deinit();
    try join.add(&.{Datum.json(.{ .string = "a" })}, &.{Datum.json(.{ .integer = 1 })});
    try join.add(&.{Datum.json(.{ .string = "b" })}, &.{Datum.json(.{ .float = 1.0 })});
    try join.add(&.{Datum.json(.{ .string = "null" })}, &.{.{}});
    var probe = try join.probe(&.{Datum.json(.{ .number_string = "1.00" })});
    var matches: usize = 0;
    while (try probe.next()) |match| {
        try join.markMatched(match.index);
        matches += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), matches);
    var null_probe = try join.probe(&.{.{}});
    try std.testing.expect((try null_probe.next()) == null);
    var cursor: usize = 0;
    const remaining = try (try join.unmatched(&cursor)).?.materializeValues(std.testing.allocator);
    defer std.testing.allocator.free(remaining);
    try std.testing.expectEqualStrings("null", remaining[0].value.string);
    try std.testing.expect((try join.unmatched(&cursor)) == null);
}

test "SQL hash join packs sparse five thousand row build under the default quota" {
    const join = try HashJoin.create(std.testing.allocator, .{});
    defer join.deinit();
    var values = @as([12]Datum, @splat(Datum{}));
    for (0..5000) |i| {
        values[0] = Datum.json(.{ .integer = @intCast(i) });
        try join.add(&values, values[0..1]);
    }
    var probe = try join.probe(&.{Datum.json(.{ .integer = 4999 })});
    const match = (try probe.next()).?;
    const expanded = try match.materializeValues(std.testing.allocator);
    defer std.testing.allocator.free(expanded);
    try std.testing.expectEqual(@as(usize, 12), expanded.len);
    try std.testing.expectEqual(@as(i64, 4999), expanded[0].value.integer);
    try std.testing.expect(expanded[11].sql_null);
    try std.testing.expect(join.budget.peak < 2 * 1024 * 1024);
    try std.testing.expect((try probe.next()) == null);
}

test "SQL typed hash join preserves SQL NULL and JSON null through allocation failures" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const join = try HashJoin.create(alloc, .{});
            defer join.deinit();
            try join.add(&.{ .{}, Datum.json(.null), Datum.json(.{ .string = "owned" }), .{} }, &.{Datum.json(.{ .integer = 1 })});
            try join.add(&.{ .{}, .{}, .{} }, &.{.{}});
            var probe = try join.probe(&.{Datum.json(.{ .integer = 1 })});
            const match = (try probe.next()).?;
            const values = try match.materializeValues(alloc);
            defer alloc.free(values);
            try std.testing.expect(values[0].sql_null and values[3].sql_null);
            try std.testing.expect(!values[1].sql_null and values[1].value == .null);
            try std.testing.expectEqualStrings("owned", values[2].value.string);
            try join.markMatched(match.index);
            var cursor: usize = 0;
            const unmatched = try (try join.unmatched(&cursor)).?.materializeValues(alloc);
            defer alloc.free(unmatched);
            try std.testing.expectEqual(@as(usize, 3), unmatched.len);
            for (unmatched) |value| try std.testing.expect(value.sql_null);
            try std.testing.expect((try join.unmatched(&cursor)) == null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL disk spilling sorts offset pages aggregates distinct keys and outer join matches" {
    var quota: MemoryBudget = .{ .backing = std.heap.page_allocator, .limit = 256 * 1024 };
    defer std.debug.assert(quota.live == 0);
    const a = quota.allocator();
    const spill = @import("spill.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    {
        var top = try TopK.initWithSpill(a, 501, &.{.{}}, 32 * 1024, &manager);
        defer top.deinit();
        for (0..1000) |i| {
            const value = Datum.json(.{ .integer = @intCast(999 - i) });
            try top.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = i });
        }
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const page = try top.finishPage(arena.allocator(), 499, 2);
        try std.testing.expectEqual(@as(usize, 2), page.len);
        try std.testing.expectEqual(@as(i64, 499), page[0].values[0].value.integer);
        try std.testing.expectEqual(@as(i64, 500), page[1].values[0].value.integer);
        try std.testing.expect(top.external != null);
    }
    {
        const group = try Grouped.create(a, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count, .distinct = true } }, .{ .groups = 200, .bytes = 64 * 1024, .spill = &manager });
        defer group.deinit();
        for (0..2000) |i| {
            const key = Datum.json(.{ .integer = @intCast(i % 200) });
            const value = Datum.json(.{ .integer = 9007199254740 });
            try group.add(&.{key}, &.{ value, value, value });
        }
        try std.testing.expect(group.external != null);
        var count: usize = 0;
        while (true) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const row = (try group.nextResult(arena.allocator())) orelse break;
            try std.testing.expectEqual(@as(i64, 10), row.aggregates[0].value.integer);
            try std.testing.expectEqual(@as(i64, 90071992547400), row.aggregates[1].value.integer);
            try std.testing.expectEqual(@as(i64, 1), row.aggregates[2].value.integer);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 200), count);
    }
    {
        const join = try HashJoin.create(a, .{ .rows = 2000, .bytes = 64 * 1024, .spill = &manager });
        defer join.deinit();
        for (0..1000) |i| {
            const key = Datum.json(.{ .integer = @intCast(i % 100) });
            try join.add(&.{ Datum.json(.{ .integer = @intCast(i) }), Datum.json(.null), .{} }, &.{key});
        }
        try std.testing.expect(join.disk != null);
        var probe = try join.probe(&.{Datum.json(.{ .integer = 42 })});
        var found: usize = 0;
        while (try probe.next()) |match| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const values = try match.materializeValues(arena.allocator());
            try std.testing.expectEqual(@as(i64, 42), @mod(values[0].value.integer, 100));
            try std.testing.expect(!values[1].sql_null and values[2].sql_null);
            try join.markMatched(match.index);
            found += 1;
        }
        try std.testing.expectEqual(@as(usize, 10), found);
        var position: usize = 0;
        var unmatched_count: usize = 0;
        while (try join.unmatched(&position)) |_| unmatched_count += 1;
        try std.testing.expectEqual(@as(usize, 990), unmatched_count);
    }
    try std.testing.expect(manager.merges > 0);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL spilled aggregate partials merge averages extrema booleans and high cardinality distinct" {
    var quota: MemoryBudget = .{ .backing = std.heap.page_allocator, .limit = 256 * 1024 };
    defer std.debug.assert(quota.live == 0);
    const a = quota.allocator();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const group = try Grouped.create(a, &.{
        .{ .kind = .count },                                         .{ .kind = .sum, .input_type = .integer },
        .{ .kind = .avg, .input_type = .integer },                   .{ .kind = .min, .input_type = .integer },
        .{ .kind = .max, .input_type = .integer },                   .{ .kind = .bool_and, .input_type = .boolean },
        .{ .kind = .bool_or, .input_type = .boolean },               .{ .kind = .count, .distinct = true, .input_type = .integer },
        .{ .kind = .sum, .distinct = true, .input_type = .integer },
    }, .{ .bytes = 64 * 1024, .spill = &manager });
    defer group.deinit();
    for (0..1000) |i| {
        const n = Datum.json(.{ .integer = @intCast(i % 500) });
        const b = Datum.json(.{ .bool = i % 2 == 0 });
        try group.add(&.{Datum.json(.{ .integer = 1 })}, &.{ n, n, n, n, n, b, b, n, n });
    }
    try std.testing.expect(group.external != null);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const result = (try group.nextResult(arena.allocator())).?;
    try std.testing.expectEqual(@as(i64, 1000), result.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 249500), result.aggregates[1].value.integer);
    try std.testing.expectApproxEqAbs(@as(f64, 249.5), result.aggregates[2].value.float, 0.0000001);
    try std.testing.expectEqual(@as(i64, 0), result.aggregates[3].value.integer);
    try std.testing.expectEqual(@as(i64, 499), result.aggregates[4].value.integer);
    try std.testing.expect(!result.aggregates[5].value.bool and result.aggregates[6].value.bool);
    try std.testing.expectEqual(@as(i64, 500), result.aggregates[7].value.integer);
    try std.testing.expectEqual(@as(i64, 124750), result.aggregates[8].value.integer);
    try std.testing.expect((try group.nextResult(arena.allocator())) == null);
}

test "SQL NUMERIC aggregate exponent gaps spill before row worker and checkpoint mutation" {
    const numeric = @import("numeric_value.zig");
    const a = std.testing.allocator;
    var context: numeric.Context = .{ .alloc = a };
    var high = try numeric.parse(&context, "1e10000");
    defer high.deinit();
    var low = try numeric.parse(&context, "1e-1000");
    defer low.deinit();
    var expected = try numeric.add(&context, high.value, low.value);
    defer expected.deinit();
    const Check = struct {
        fn call(_: *anyopaque) anyerror!void {}
    };
    const spec: AggregateSpec = .{ .kind = .sum, .input_type = .number, .input_element = .numeric };
    var token: u8 = 0;
    for (0..3) |path| {
        var quota: MemoryBudget = .{ .backing = std.heap.page_allocator, .limit = 512 * 1024 };
        defer std.debug.assert(quota.live == 0);
        const alloc = quota.allocator();
        var manager: @import("spill.zig").Manager = .{ .alloc = alloc, .io = std.testing.io, .context = &token, .checkpoint = Check.call };
        defer manager.deinit();
        const group = try Grouped.create(alloc, &.{spec}, .{ .bytes = 64 * 1024, .spill = &manager });
        defer group.deinit();
        try group.add(&.{}, &.{Datum.typedNumeric(&low.value)});
        try std.testing.expect(group.external == null);
        switch (path) {
            0 => try group.add(&.{}, &.{Datum.typedNumeric(&high.value)}),
            1 => {
                const worker = try Grouped.create(alloc, &.{spec}, .{});
                defer worker.deinit();
                try worker.add(&.{}, &.{Datum.typedNumeric(&high.value)});
                try group.mergeExact(worker, 1);
            },
            else => {
                var partial = try Aggregate.initTyped(alloc, .sum, .number, .numeric);
                defer partial.deinit();
                try partial.update(Datum.typedNumeric(&high.value));
                const encoded = try @import("aggregate_partial.zig").cell(alloc, partial);
                defer alloc.free(encoded.value.string);
                try group.importPartial(&.{}, &.{encoded}, 1);
            },
        }
        try std.testing.expect(group.external != null);
        try std.testing.expect(!group.budget.exhausted);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const result = (try group.nextResult(arena.allocator())).?;
        try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&context, expected.value, result.aggregates[0].numeric.?.*));
        try std.testing.expect((try group.nextResult(arena.allocator())) == null);
        try std.testing.expect(quota.peak <= quota.limit);
    }
}

test "SQL pattern sets spill distinct state and preserve reusable quantified null semantics" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = std.heap.page_allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const group = try Grouped.create(std.heap.page_allocator, &.{ .{ .kind = .pattern_set, .input_type = .string, .distinct = true }, .{ .kind = .pattern_set, .input_type = .string, .distinct = true } }, .{ .bytes = 128 * 1024, .spill = &manager });
    defer group.deinit();
    for (0..4000) |index| {
        var bytes: [32]u8 = undefined;
        const value = Datum.json(.{ .string = try std.fmt.bufPrint(&bytes, "pattern-{d}", .{index % 2000}) });
        try group.add(&.{}, &.{ value, value });
    }
    try group.add(&.{}, &.{ .{}, Datum.json(.{ .string = "pattern-0" }) });
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const result = (try group.nextResult(arena.allocator())).?;
    try std.testing.expectEqual(@as(usize, 2001), result.aggregates[0].patterns.?.count);
    try std.testing.expectEqual(@as(usize, 2000), result.aggregates[1].patterns.?.count);
    const operand: ast.Scalar = .{ .literal = .{ .string = "absent" } };
    const patterns: ast.Scalar = .{ .column = "p" };
    const no: ast.Scalar = .{ .literal = .{ .boolean = false } };
    const expression: ast.Scalar = .{ .call = .{ .name = "$pattern_quantified", .args = &.{ &operand, &patterns, &no, &no, &no } } };
    var program = try scalar.bind(std.testing.allocator, &expression, &.{.{ .name = "p", .type = .json }}, &.{}, .{});
    defer program.deinit();
    for (0..2) |_| {
        const nullable = try program.evaluate(arena.allocator(), &.{result.aggregates[0]}, &.{}, .{});
        try std.testing.expect(nullable.sql_null);
        const nonnullable = try program.evaluate(arena.allocator(), &.{result.aggregates[1]}, &.{}, .{});
        try std.testing.expect(!nonnullable.sql_null and !nonnullable.value.bool);
    }
    try std.testing.expect(manager.written_bytes > 128 * 1024);
}

test "SQL global batch reductions preserve scalar null counts booleans and exact sums" {
    const a = std.testing.allocator;
    const specs = [_]AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .bool_and }, .{ .kind = .bool_or } };
    var batched = try Grouped.create(a, &specs, .{});
    defer batched.deinit();
    var baseline = try Grouped.create(a, &specs, .{});
    defer baseline.deinit();
    const rows = [_][]const Datum{
        &.{ Datum.json(.null), Datum.json(.{ .integer = std.math.maxInt(i64) }), Datum.json(.{ .bool = true }), .{} },
        &.{ .{}, Datum.json(.{ .integer = 1 }), .{}, Datum.json(.{ .bool = false }) },
        &.{ Datum.json(.{ .integer = 2 }), Datum.json(.{ .integer = -1 }), Datum.json(.{ .bool = false }), Datum.json(.{ .bool = true }) },
        &.{ .{}, .{}, .{}, .{} },
        &.{ .{}, .{}, Datum.json(.{ .bool = true }), .{} },
        &.{ .{}, .{}, .{}, .{} },
    };
    try batched.addGlobalBatch(&rows);
    for (rows) |row_value| try baseline.add(&.{}, row_value);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const actual = try batched.resultAt(arena.allocator(), 0);
    const expected = try baseline.resultAt(arena.allocator(), 0);
    try std.testing.expectEqualDeep(expected.aggregates, actual.aggregates);
    try std.testing.expectEqual(@as(u64, 0), batched.hash_probes);
    try std.testing.expectEqual(@as(u64, rows.len), batched.rows_seen);
}

test "SQL spilled worker partials preserve wide sums until final merge" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const specs = [_]AggregateSpec{.{ .kind = .sum, .input_type = .integer }};
    const positive = try Grouped.create(a, &specs, .{ .bytes = 64 * 1024, .groups = 4096, .spill = &manager });
    defer positive.deinit();
    const negative = try Grouped.create(a, &specs, .{ .bytes = 64 * 1024, .groups = 4096, .spill = &manager });
    defer negative.deinit();
    const merged = try Grouped.create(a, &specs, .{ .bytes = 64 * 1024, .groups = 4096, .spill = &manager });
    defer merged.deinit();
    for (0..2) |_| for (0..1024) |key| {
        const keys = [_]Datum{Datum.json(.{ .integer = @intCast(key) })};
        try positive.add(&keys, &.{Datum.json(.{ .integer = std.math.maxInt(i64) })});
        try negative.add(&keys, &.{Datum.json(.{ .integer = -std.math.maxInt(i64) })});
    };
    try std.testing.expect(positive.external != null and negative.external != null);
    try merged.mergeExact(positive, 0);
    try merged.mergeExact(negative, positive.rows_seen);
    try std.testing.expectEqual(@as(u64, 4096), merged.rows_seen);
    var seen: [1024]bool = @splat(false);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    while (true) {
        _ = arena.reset(.retain_capacity);
        const row = (try merged.nextResult(arena.allocator())) orelse break;
        const key: usize = @intCast(row.keys[0].value.integer);
        try std.testing.expect(!seen[key]);
        seen[key] = true;
        try std.testing.expectEqual(@as(i64, 0), row.aggregates[0].value.integer);
    }
    for (seen) |found| try std.testing.expect(found);
}

test "SQL bounded native join admission transfers a prefix without a chain spool" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const count = 256;
    var integers: [count]Datum = undefined;
    var rows: [count][]const Datum = undefined;
    var keys: [count][]const Datum = undefined;
    for (&integers, &rows, &keys, 0..) |*value, *row, *key, index| {
        value.* = Datum.json(.{ .integer = @intCast(index) });
        row.* = integers[index..][0..1];
        key.* = row.*;
    }
    const hash = try HashJoin.create(a, .{ .bytes = 32 * 1024, .rows = count, .spill = &manager });
    defer hash.deinit();
    const consumed = try hash.addBatchUntilFull(a, .{ .rows = &rows }, &keys);
    try std.testing.expect(consumed > 0 and consumed < count);
    try std.testing.expectEqual(consumed, hash.row_count);
    try std.testing.expect(hash.disk == null);
    try std.testing.expectEqual(@as(u64, 0), manager.written_bytes);
    const grace = try @import("partition_join.zig").Join.create(a, &manager, 32 * 1024, count, 0, false, false);
    defer grace.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var index: usize = 0;
    while (try hash.unmatched(&index)) |match| {
        _ = arena.reset(.retain_capacity);
        try grace.add(true, try match.materializeValues(arena.allocator()), try match.materializeKeys(arena.allocator()), match.index);
    }
    try grace.addBatch(true, .{ .rows = &rows }, &keys, consumed);
    try grace.addBatch(false, .{ .rows = &rows }, &keys, 0);
    try std.testing.expectEqual(@as(usize, count), grace.rows[1]);
    var matches: usize = 0;
    while (try grace.next()) |pair| {
        try std.testing.expectEqual(pair.left.?[0].value.integer, pair.right.?[0].value.integer);
        if (pair.match) |matched| try grace.accept(matched);
        matches += 1;
    }
    try std.testing.expectEqual(@as(usize, count), matches);
}

fn groupBatchScenario(a: Allocator) !void {
    const specs = [_]AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .bool_or, .input_type = .boolean } };
    const grouped = try Grouped.create(a, &specs, .{});
    defer grouped.deinit();
    for (0..128) |row| try grouped.add(&.{ Datum.json(.{ .integer = @intCast(row % 8) }), if (row % 2 == 0) .{} else Datum.json(.null) }, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = @intCast(row) }), Datum.json(.{ .bool = row % 2 == 0 }) });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var seen: usize = 0;
    while (true) {
        _ = arena.reset(.free_all);
        const batch = (try grouped.nextResultBatch(arena.allocator(), 3, false)) orelse break;
        for (0..batch.ordinals.len) |row| {
            const key = (try batch.keys.cell(a, row, 0)).value.integer;
            const null_ = try batch.keys.cell(a, row, 1);
            try std.testing.expectEqual(@mod(key, 2) == 0, null_.sql_null);
            try std.testing.expectEqual(@as(i64, 16), (try batch.aggregates.cell(a, row, 0)).value.integer);
            try std.testing.expectEqual(16 * key + 960, (try batch.aggregates.cell(a, row, 1)).value.integer);
            try std.testing.expectEqual(@mod(key, 2) == 0, (try batch.aggregates.cell(a, row, 2)).value.bool);
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 8), seen);
}
test "SQL grouped result batches preserve null domains and unwind allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, groupBatchScenario, .{});
}

test "SQL typed aggregate partial batches preserve i128 sums through cancellation" {
    const a = std.testing.allocator;
    const specs = [_]AggregateSpec{.{ .kind = .sum, .input_type = .integer }};
    const positive = try Grouped.create(a, &specs, .{});
    defer positive.deinit();
    const merged = try Grouped.create(a, &specs, .{});
    defer merged.deinit();
    for (0..3) |_| try positive.add(&.{Datum.json(.{ .integer = 7 })}, &.{Datum.json(.{ .integer = std.math.maxInt(i64) })});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = (try positive.nextResultBatch(arena.allocator(), 64, true)).?;
    try std.testing.expectEqual(@as(usize, 1), batch.aggregates.width());
    var sum = try @import("aggregate_partial.zig").decode(arena.allocator(), try batch.aggregates.cell(arena.allocator(), 0, 0), specs[0]);
    defer sum.deinit();
    try std.testing.expectEqual(@as(i128, 27670116110564327421), sum.integer_sum);
    try merged.importPartial(try batch.keys.row(arena.allocator(), 0), try batch.aggregates.row(arena.allocator(), 0), batch.ordinals[0]);
    for (0..3) |_| try merged.add(&.{Datum.json(.{ .integer = 7 })}, &.{Datum.json(.{ .integer = -std.math.maxInt(i64) })});
    const result = (try merged.nextResultBatch(arena.allocator(), 64, false)).?;
    try std.testing.expectEqual(@as(i64, 0), (try result.aggregates.cell(a, 0, 0)).value.integer);
}

test "SQL binary partial import composes typed extrema floats distinct and pattern domains" {
    const a = std.testing.allocator;
    const specs = [_]AggregateSpec{
        .{ .kind = .count },
        .{ .kind = .sum, .input_type = .integer },
        .{ .kind = .sum, .input_type = .number },
        .{ .kind = .avg, .input_type = .number },
        .{ .kind = .min, .input_type = .integer },
        .{ .kind = .max, .input_type = .number },
        .{ .kind = .min, .input_type = .string },
        .{ .kind = .max, .input_type = .boolean },
        .{ .kind = .min, .input_type = .json },
        .{ .kind = .sum, .input_type = .integer, .distinct = true },
        .{ .kind = .pattern_set, .input_type = .string, .distinct = true },
    };
    const target = try Grouped.create(a, &specs, .{});
    defer target.deinit();
    const reference = try Grouped.create(a, &specs, .{});
    defer reference.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..2) |partition| {
        const source = try Grouped.create(a, &specs, .{});
        defer source.deinit();
        for (0..3) |i| {
            const n: i64 = @intCast(i + partition);
            const row = [_]Datum{
                Datum.json(.{ .integer = 1 }),                               Datum.json(.{ .integer = n }),
                Datum.json(.{ .float = @floatFromInt(n) }),                  Datum.json(.{ .float = @floatFromInt(n) }),
                Datum.json(.{ .integer = n }),                               Datum.json(.{ .float = @floatFromInt(n) }),
                Datum.json(.{ .string = if (partition == 0) "z" else "a" }), Datum.json(.{ .bool = partition == 1 }),
                Datum.json(.null),                                           Datum.json(.{ .integer = n }),
                if (i == 0) Datum{} else Datum.json(.{ .string = "%cat%" }),
            };
            try source.add(&.{}, &row);
            try reference.add(&.{}, &row);
        }
        const partial = (try source.nextResultBatch(arena.allocator(), 1, true)).?;
        try target.importPartial(&.{}, try partial.aggregates.row(arena.allocator(), 0), @intCast(partition));
    }
    const actual = try target.resultAt(arena.allocator(), 0);
    const expected = try reference.resultAt(arena.allocator(), 0);
    for (actual.aggregates, expected.aggregates) |got, wanted| {
        try std.testing.expectEqual(wanted.sql_null, got.sql_null);
        const left = try std.json.Stringify.valueAlloc(arena.allocator(), got.value, .{});
        const right = try std.json.Stringify.valueAlloc(arena.allocator(), wanted.value, .{});
        try std.testing.expectEqualStrings(right, left);
    }
    try std.testing.expect(!actual.aggregates[8].sql_null and actual.aggregates[8].value == .null);
}

fn mixedExtremaPartialScenario(a: Allocator) !void {
    for ([_]Aggregate.Kind{ .min, .max }) |kind| {
        const specs = [_]AggregateSpec{.{ .kind = kind, .input_type = .number }};
        const source = try Grouped.create(a, &specs, .{});
        defer source.deinit();
        const target = try Grouped.create(a, &specs, .{});
        defer target.deinit();
        const exact: i64 = if (kind == .min) -9007199254740993 else 9007199254740993;
        for (0..3) |_| try source.add(&.{}, &.{Datum.json(.{ .integer = exact })});
        try target.add(&.{}, &.{Datum.json(.{ .float = 2.5 })});
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const partial = (try source.nextResultBatch(arena.allocator(), 1, true)).?;
        try target.importPartial(&.{}, try partial.aggregates.row(arena.allocator(), 0), 0);
        const state = try target.state_columns[0].snapshot(arena.allocator(), 0);
        try std.testing.expectEqual(@as(u64, 4), state.count);
        const result = (try target.nextResultBatch(arena.allocator(), 1, false)).?;
        try std.testing.expectEqual(exact, (try result.aggregates.cell(a, 0, 0)).value.integer);
    }
}
test "SQL mixed numeric extrema partials promote without narrowing or losing counts" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, mixedExtremaPartialScenario, .{});
}

fn encodedGroupScenario(a: Allocator) !void {
    const Batch = @import("execution_batch.zig").Batch;
    const specs = [_]AggregateSpec{ .{ .kind = .count, .input_type = .integer }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .sum, .input_type = .number } };
    const encoded = try Grouped.create(a, &specs, .{});
    defer encoded.deinit();
    const reference = try Grouped.create(a, &specs, .{});
    defer reference.deinit();
    const indices = [_]u32{ 0, 1, 0, 2, 1, 2, 0, 1 };
    const keys: Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .string = "repeated" }), .{}, Datum.json(.null) }, .indices = &indices } };
    const integers: Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.{ .integer = -9007199254740993 }) }, .indices = &indices } };
    const numbers: Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .float = 1e16 }), Datum.json(.{ .float = -1e16 }), Datum.json(.{ .float = 1.5 }) }, .indices = &indices } };
    try encoded.addEncodedColumns(&.{keys}, &.{ integers, integers, numbers }, indices.len);
    for (0..indices.len) |index| try reference.add(&.{try keys.cell(a, index, 0)}, &.{ try integers.cell(a, index, 0), try integers.cell(a, index, 0), try numbers.cell(a, index, 0) });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectEqualDeep(try reference.finish(arena.allocator()), try encoded.finish(arena.allocator()));
}
test "SQL encoded grouping preserves exact integers floating lane order and distinct NULL keys" {
    try encodedGroupScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, encodedGroupScenario, .{});
}

test "SQL exact partial restoration preserves compensated floating sum bits" {
    const a = std.testing.allocator;
    const specs = [_]AggregateSpec{.{ .kind = .sum, .input_type = .number }};
    for ([_]f64{ 9007199254740994.0, -9007199254740994.0 }) |sum| {
        for ([_]f64{ 1.0, -1.0 }) |compensation| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const state: Aggregate = .{ .alloc = scratch, .kind = .sum, .input_type = .number, .count = 3, .number_sum = sum, .compensation = compensation };
            const encoded = try @import("aggregate_partial.zig").cell(scratch, state);
            const target = try Grouped.create(a, &specs, .{});
            defer target.deinit();
            try target.importPartial(&.{}, &.{encoded}, 0);
            const result = try target.resultAt(scratch, 0);
            try std.testing.expectEqual(@as(u64, @bitCast(sum)), @as(u64, @bitCast(result.aggregates[0].value.float)));
            const restored = try target.state_columns[0].snapshot(scratch, 0);
            try std.testing.expectEqual(@as(u64, @bitCast(compensation)), @as(u64, @bitCast(restored.compensation)));
            try std.testing.expectEqual(@as(u64, 3), restored.count);
        }
    }
}

test "SQL sparse partial slots validate before mutation and retain untouched floating state" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    const specs = [_]AggregateSpec{ .{ .kind = .sum, .input_type = .number }, .{ .kind = .count } };
    const target = try Grouped.create(a, &specs, .{});
    defer target.deinit();
    const sum: Aggregate = .{ .alloc = pa, .kind = .sum, .input_type = .number, .count = 3, .number_sum = 9007199254740994.0, .compensation = 1.0 };
    const sum_cell = try @import("aggregate_partial.zig").cell(pa, sum);
    const count_cell = try @import("aggregate_partial.zig").cell(pa, .{ .alloc = pa, .kind = .count, .input_type = null, .count = 7 });
    try std.testing.expectError(error.InvalidSqlSpill, target.importPartialMapped(&.{}, &.{count_cell}, &.{2}, 0));
    try std.testing.expectError(error.InvalidSqlSpill, target.importPartialMapped(&.{}, &.{ sum_cell, sum_cell }, &.{ 0, 0 }, 0));
    try std.testing.expectEqual(@as(usize, 0), target.groups.items.len);
    try target.importPartialMapped(&.{}, &.{sum_cell}, &.{0}, 0);
    try target.importPartialMapped(&.{}, &.{count_cell}, &.{1}, 0);
    const restored = try target.state_columns[0].snapshot(pa, 0);
    try std.testing.expectEqual(@as(u64, @bitCast(sum.number_sum)), @as(u64, @bitCast(restored.number_sum)));
    try std.testing.expectEqual(@as(u64, @bitCast(sum.compensation)), @as(u64, @bitCast(restored.compensation)));
    const result = try target.resultAt(pa, 0);
    try std.testing.expectEqual(@as(i64, 7), result.aggregates[1].value.integer);
}

test "SQL sparse partial slots survive native group spilling" {
    const a = std.testing.allocator;
    var context: u8 = 0;
    var spill: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer spill.deinit();
    const specs = [_]AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    const target = try Grouped.create(a, &specs, .{ .bytes = 16 * 1024, .spill = &spill });
    defer target.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    const sum = try @import("aggregate_partial.zig").cell(pa, .{ .alloc = pa, .kind = .sum, .input_type = .integer, .count = 1, .integer_sum = 9007199254740993 });
    const count = try @import("aggregate_partial.zig").cell(pa, .{ .alloc = pa, .kind = .count, .input_type = null, .count = 1 });
    for (0..600) |key| try target.importPartialMapped(&.{Datum.json(.{ .integer = @intCast(key) })}, &.{sum}, &.{0}, key);
    for (0..600) |key| try target.importPartialMapped(&.{Datum.json(.{ .integer = @intCast(599 - key) })}, &.{count}, &.{1}, 599 - key);
    try std.testing.expect(target.external != null);
    var seen: usize = 0;
    while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const result = (try target.nextResult(page.allocator())) orelse break;
        try std.testing.expectEqual(@as(i64, 9007199254740993), result.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, 1), result.aggregates[1].value.integer);
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, 600), seen);
}

test "SQL variable extrema batch admission spills before retaining payloads" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const bytes = try a.alloc(u8, 16384);
    defer a.free(bytes);
    @memset(bytes, 'x');
    var keys: [32]Datum = undefined;
    var values: [32]Datum = undefined;
    for (&keys, &values, 0..) |*key, *value, index| {
        key.* = Datum.json(.{ .integer = @intCast(index) });
        value.* = Datum.json(.{ .string = bytes });
    }
    for ([_]bool{ false, true }) |encoded| {
        const group = try Grouped.create(a, &.{.{ .kind = .min, .input_type = .string }}, .{ .groups = 64, .bytes = 256 * 1024, .spill = &manager });
        defer group.deinit();
        if (encoded) {
            const Batch = @import("execution_batch.zig").Batch;
            try group.addEncodedColumns(&.{Batch{ .vectors = .{ .values = &.{&keys}, .count = keys.len } }}, &.{Batch{ .vectors = .{ .values = &.{&values}, .count = values.len } }}, keys.len);
        } else try group.addColumns(&.{&keys}, &.{&values}, keys.len);
        try std.testing.expect(group.external != null);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count: usize = 0;
        while (try group.nextResult(arena.allocator())) |result| {
            try std.testing.expectEqualStrings(bytes, result.aggregates[0].value.string);
            count += 1;
        }
        try std.testing.expectEqual(keys.len, count);
    }
}

test "external lake SQL identified group columns separate source ordinals from input cardinality" {
    const a = std.testing.allocator;
    const Batch = @import("execution_batch.zig").Batch;
    const values = [_]Datum{ Datum.json(.{ .integer = 11 }), Datum.json(.{ .integer = 22 }) };
    const indices = [_]u32{ 1, 0, 1 };
    const columns = [_]Batch{.{ .dictionary = .{ .values = &values, .indices = &indices } }};
    const hashes = try @import("batch_hash.zig").encodedColumns(a, &columns, indices.len, true);
    defer a.free(hashes);
    var context: u8 = 0;
    for ([_]bool{ false, true }) |disk| {
        var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
            fn check(_: *anyopaque) !void {}
        }.check };
        defer manager.deinit();
        const grouped = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .bytes = 1024 * 1024, .spill = &manager });
        defer grouped.deinit();
        if (disk) {
            grouped.key_count = columns.len;
            try grouped.startSpill();
        }
        try grouped.addEncodedColumnsIdentified(&columns, &columns, indices.len, .{ .ordinals = &.{ 100, 900, 5000 }, .hashes = hashes });
        try std.testing.expectEqual(@as(u64, 3), grouped.rows_seen);
        try std.testing.expect(grouped.source_ordinal == null);
        var count: usize = 0;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        while (try grouped.nextTypedPartialResult(scratch.allocator())) |group| {
            const is_eleven = group.keys[0].value.integer == 11;
            try std.testing.expectEqual(@as(u64, if (is_eleven) 900 else 100), group.ordinal);
            try std.testing.expectEqual(@as(u64, if (is_eleven) 1 else 2), group.typed_states.?[0].count);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 2), count);
    }
}

test "SQL typed grouped partial batches preserve overlapping groups null extrema and ordinals" {
    const a = std.testing.allocator;
    const specs = [_]AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .min, .input_type = .integer }, .{ .kind = .max, .input_type = .integer } };
    const merged = try Grouped.create(a, &specs, .{});
    defer merged.deinit();
    const reference = try Grouped.create(a, &specs, .{});
    defer reference.deinit();
    for (0..3) |worker_index| {
        const worker = try Grouped.create(a, &specs, .{});
        defer worker.deinit();
        for (0..257) |index| {
            const key = Datum.json(.{ .integer = @intCast(index) });
            const value: Datum = if (index % 17 == 0) .{} else Datum.json(.{ .integer = @as(i64, @intCast(worker_index)) - @as(i64, @intCast(index)) });
            try worker.add(&.{key}, &.{ value, value, value, value });
            try reference.add(&.{key}, &.{ value, value, value, value });
        }
        try merged.mergeExact(worker, @intCast(worker_index * 257));
    }
    try std.testing.expectEqual(reference.rows_seen, merged.rows_seen);
    try std.testing.expectEqual(reference.groups.items.len, merged.groups.items.len);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..257) |index| {
        _ = arena.reset(.retain_capacity);
        const actual = try merged.resultAt(arena.allocator(), index);
        const expected = try reference.resultAt(arena.allocator(), index);
        try std.testing.expectEqualDeep(expected.keys, actual.keys);
        try std.testing.expectEqualDeep(expected.aggregates, actual.aggregates);
        try std.testing.expectEqual(expected.ordinal, actual.ordinal);
    }
}

fn selectedGroupReduction(a: Allocator, group_limit: usize) !void {
    const Owner = struct {
        a: Allocator,
        groups: [2]*Grouped = undefined,
        initialized: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (self.groups[0..self.initialized]) |group| group.deinit();
            self.a.destroy(self);
        }
    };
    const specs = [_]AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .min, .input_type = .string } };
    const parent = try Grouped.create(a, &specs, .{ .groups = group_limit });
    defer parent.deinit();
    const owner = try a.create(Owner);
    owner.* = .{ .a = a };
    var adopted = false;
    defer if (!adopted) Owner.release(owner);
    for (&owner.groups) |*group| {
        group.* = try Grouped.create(a, &specs, .{});
        owner.initialized += 1;
    }
    for (0..3) |worker| {
        const source = try Grouped.create(a, &specs, .{});
        defer source.deinit();
        for (0..67) |index| try source.add(&.{Datum.json(.{ .integer = @intCast(index) })}, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = @intCast(worker + index) }), Datum.json(.{ .string = if (worker == 1) "alpha" else "zeta" }) });
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const selections = try source.partitionGroups(scratch.allocator(), 2);
        try std.testing.expectEqual(@as(usize, 67), selections[0].len + selections[1].len);
        for (owner.groups, selections) |group, selection| try group.mergeExactSelected(source, worker * 67, selection);
    }
    try parent.adoptReducers(.{ .ptr = owner, .groups = &owner.groups, .release = Owner.release }, 201);
    adopted = true;
    try std.testing.expectError(error.InvalidSqlBackendResponse, parent.add(&.{}, &.{}));
    try std.testing.expectEqual(@as(usize, 67), parent.groupCount());
    var seen: [67]bool = @splat(false);
    while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const batch = (try parent.nextResultBatch(page.allocator(), 7, false)) orelse break;
        for (batch.ordinals, 0..) |ordinal, row| {
            const key: usize = @intCast((try batch.keys.cell(page.allocator(), row, 0)).value.integer);
            try std.testing.expect(!seen[key]);
            seen[key] = true;
            try std.testing.expectEqual(@as(u64, @intCast(key)), ordinal);
            try std.testing.expectEqual(@as(i64, 3), (try batch.aggregates.cell(a, row, 0)).value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(3 * key + 3)), (try batch.aggregates.cell(a, row, 1)).value.integer);
            try std.testing.expectEqualStrings("alpha", (try batch.aggregates.cell(a, row, 2)).value.string);
        }
    }
    for (seen) |present| try std.testing.expect(present);
}
test "SQL hash owned final reducers preserve typed state ordinals and lifetime" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectedGroupReduction, .{67});
    try std.testing.expectError(error.SqlProgramLimitExceeded, selectedGroupReduction(std.testing.allocator, 66));
}

test "SQL global encoded aggregates consume physical vectors without hash probes" {
    const a = std.testing.allocator;
    const Batch = @import("execution_batch.zig").Batch;
    const definitions = [_]@import("scalar.zig").Column{.{ .name = "n", .type = .integer }};
    const batch: Batch = .{ .columns = .{ .definitions = &definitions, .page = .{
        .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" } }, .columns = &.{.{ .name = "n", .values = .{ .i64 = &.{ 9, 0, -2 } }, .nulls = .{ .bytes = &.{ 0, 1, 0 } } }} },
        .selection = &.{ 2, 1, 0, 2 },
    } } };
    const group = try Grouped.create(a, &.{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count }, .{ .kind = .min, .input_type = .integer } }, .{});
    defer group.deinit();
    try group.addEncodedColumns(&.{}, &.{ batch, batch, batch }, 4);
    try std.testing.expectEqual(@as(usize, 0), group.hash_probes);
    try std.testing.expectEqual(@as(u64, 4), group.rows_seen);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const result = (try group.nextResult(scratch.allocator())).?;
    try std.testing.expectEqual(@as(i64, 5), result.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 3), result.aggregates[1].value.integer);
    try std.testing.expectEqual(@as(i64, -2), result.aggregates[2].value.integer);
}

test "SQL global encoded wide batches admit one group under a small budget" {
    const a = std.testing.allocator;
    const specs: [12]AggregateSpec = @splat(.{ .kind = .count });
    const values = [_]Datum{Datum.json(.{ .integer = 1 })};
    const indices: [1024]u32 = @splat(0);
    const batch: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &values, .indices = &indices } };
    const inputs: [12]@import("execution_batch.zig").Batch = @splat(batch);
    var grouped = try Grouped.create(a, &specs, .{ .bytes = 64 * 1024 });
    defer grouped.deinit();
    try grouped.addEncodedColumns(&.{}, &inputs, indices.len);
    try std.testing.expectEqual(@as(usize, 0), grouped.hash_probes);
    for (grouped.state_columns) |column| try std.testing.expectEqual(@as(u64, indices.len), column.values.counts.items[0]);
}

test "SQL encoded grouping admits distinct dictionary tuples and sparse cohorts under a small quota" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const Batch = @import("execution_batch.zig").Batch;
    const count = 10000;
    const indices = try scratch.alloc(u32, count);
    for (indices, 0..) |*id, i| id.* = @intCast(i % 3);
    // Two representations of one semantic key must resolve to one group.
    const keys: Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .integer = 7 }), Datum.json(.{ .integer = 7 }), Datum.json(.{ .integer = 9 }) }, .indices = indices } };
    const ones = try scratch.alloc(u32, count);
    @memset(ones, 0);
    const input: Batch = .{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = ones } };
    const positions = [_]usize{ 0, 2, 9999 };
    const subset: Batch = .{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 11 })}, .indices = &.{ 0, 0, 0 } } };
    const filtered = try subset.mask(scratch, &positions, count);
    const grouped = try Grouped.create(a, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, .{ .bytes = 64 * 1024, .groups = 2 });
    defer grouped.deinit();
    try grouped.addEncodedColumns(&.{keys}, &.{ input, filtered }, count);
    try grouped.addEncodedColumns(&.{keys}, &.{ input, filtered }, count);
    try std.testing.expectEqual(@as(usize, 2), grouped.groupCount());
    try std.testing.expectEqual(@as(u64, 2 * count), grouped.rows_seen);
    // One exact lookup per distinct key in the second batch, rather than rows.
    try std.testing.expect(grouped.hash_probes <= 2);
    try std.testing.expect(grouped.external == null);
    const first = try grouped.resultAt(scratch, 0);
    const second = try grouped.resultAt(scratch, 1);
    try std.testing.expectEqual(@as(i64, 13334), first.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 44), first.aggregates[1].value.integer);
    try std.testing.expectEqual(@as(u64, 0), first.ordinal);
    try std.testing.expectEqual(@as(i64, 6666), second.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 22), second.aggregates[1].value.integer);
    try std.testing.expectEqual(@as(u64, 2), second.ordinal);
    const global = try Grouped.create(a, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, .{ .bytes = 64 * 1024 });
    defer global.deinit();
    try global.addEncodedColumns(&.{}, &.{ filtered, filtered }, count);
    const result = try global.resultAt(scratch, 0);
    try std.testing.expectEqual(@as(i64, 3), result.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 33), result.aggregates[1].value.integer);
}

test "SQL encoded existing groups remain batched above half quota" {
    const a = std.testing.allocator;
    const Batch = @import("execution_batch.zig").Batch;
    const group = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .bytes = 256 * 1024 });
    defer group.deinit();
    const keys: Batch = .{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = &.{ 0, 0, 0, 0 } } };
    try group.addEncodedColumns(&.{keys}, &.{keys}, 4);
    // Account unrelated retained state against the same operator budget.
    const reservation = try group.budget.allocator().alloc(u8, 140 * 1024);
    defer group.budget.allocator().free(reservation);
    try std.testing.expect(group.budget.live > group.budget.limit / 2);
    const probes = group.hash_probes;
    try group.addEncodedColumns(&.{keys}, &.{keys}, 4);
    try std.testing.expectEqual(probes + 1, group.hash_probes);
    try std.testing.expectEqual(@as(u64, 8), group.state_columns[0].values.counts.items[0]);
}
fn encodedPartitionScenario(a: Allocator, count: usize) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const Batch = @import("execution_batch.zig").Batch;
    var raw: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &raw, .checkpoint = Hook.check, .async_writes = false, .compression = .none };
    defer manager.deinit();
    const specs = [_]AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } };
    const group = try Grouped.create(a, &specs, .{ .bytes = 128 * 1024, .spill = &manager });
    defer group.deinit();
    group.key_count = 1;
    try group.startSpill();
    group.external.?.parallel = false;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const indices = try alloc.alloc(u32, count);
    const ordinals = try alloc.alloc(u64, indices.len);
    for (indices, ordinals, 0..) |*id, *ordinal, index| {
        id.* = @intCast(index % 2);
        ordinal.* = 100 + index * 3;
    }
    const keys: Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .integer = 7 }), .{} }, .indices = indices } };
    const values: Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .integer = 3 }), Datum.json(.{ .integer = 5 }) }, .indices = indices } };
    const hashes = try @import("batch_hash.zig").encodedColumns(alloc, &.{keys}, indices.len, true);
    try group.addEncodedColumnsIdentified(&.{keys}, &.{ values, values }, indices.len, .{ .hashes = hashes, .ordinals = ordinals });
    var seen: usize = 0;
    while (try group.nextResultBatch(alloc, 8, false)) |batch| {
        for (batch.ordinals, 0..) |ordinal, index| {
            const key = try batch.keys.cell(alloc, index, 0);
            try std.testing.expectEqual(@as(i64, @intCast(count / 2)), (try batch.aggregates.cell(alloc, index, 0)).value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(count / 2 * @as(usize, if (key.sql_null) 5 else 3))), (try batch.aggregates.cell(alloc, index, 1)).value.integer);
            try std.testing.expectEqual(@as(u64, if (key.sql_null) 103 else 100), ordinal);
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), seen);
    try std.testing.expect(group.external.?.native_reduced_rows != 0);
}
test "SQL encoded partition spill preserves native input blocks null keys and ordinals" {
    try encodedPartitionScenario(std.testing.allocator, 1024);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, encodedPartitionScenario, .{@as(usize, 8)});
}

test "SQL encoded incremental admission spills a high cardinality suffix" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const Batch = @import("execution_batch.zig").Batch;
    const a = std.testing.allocator;
    var raw: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &raw, .checkpoint = Hook.check, .async_writes = false, .compression = .none };
    defer manager.deinit();
    const group = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .bytes = 64 * 1024, .spill = &manager });
    defer group.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const values = try alloc.alloc(Datum, 1024);
    const indices = try alloc.alloc(u32, values.len);
    for (values, indices, 0..) |*value, *index, i| {
        value.* = Datum.json(.{ .integer = @intCast(i) });
        index.* = @intCast(i);
    }
    const keys: Batch = .{ .dictionary = .{ .values = values, .indices = indices } };
    try group.addEncodedColumns(&.{keys}, &.{keys}, values.len);
    try std.testing.expect(group.external != null);
    try std.testing.expectEqual(@as(u64, values.len), group.rows_seen);
    var seen: [1024]bool = @splat(false);
    while (try group.nextResultBatch(alloc, 32, false)) |batch| for (batch.ordinals, 0..) |ordinal, lane| {
        const key: usize = @intCast((try batch.keys.cell(alloc, lane, 0)).value.integer);
        try std.testing.expect(!seen[key]);
        seen[key] = true;
        try std.testing.expectEqual(@as(u64, key), ordinal);
        try std.testing.expectEqual(@as(i64, 1), (try batch.aggregates.cell(alloc, lane, 0)).value.integer);
    };
    for (seen) |present| try std.testing.expect(present);
}

test "SQL encoded spill validates physical inputs before writing scalar blocks" {
    const Batch = @import("execution_batch.zig").Batch;
    const Reader = struct {
        fn check(_: *anyopaque) !void {}
        fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
            return Datum.json(.{ .integer = 1 });
        }
        fn physical(_: *anyopaque, _: Allocator, _: usize) !?@import("execution_batch.zig").PhysicalColumn {
            return .{ .vector = .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{1}, .indices = &.{2} } } }, .selection = &.{0}, .kind = .integer };
        }
    };
    const a = std.testing.allocator;
    var raw: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &raw, .checkpoint = Reader.check, .async_writes = false };
    defer manager.deinit();
    const group = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .bytes = 64 * 1024, .spill = &manager });
    defer group.deinit();
    group.key_count = 1;
    try group.startSpill();
    const key: Batch = .{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 7 })}, .indices = &.{0} } };
    const input: Batch = .{ .reader = .{ .ptr = &raw, .read = Reader.cell, .read_physical = Reader.physical, .count = 1, .width = 1 } };
    try std.testing.expectError(error.InvalidSqlBackendResponse, group.addEncodedColumnsIdentified(&.{key}, &.{input}, 1, .{ .hashes = &.{0}, .ordinals = &.{0} }));
    try std.testing.expectEqual(@as(u64, 0), group.rows_seen);
    try std.testing.expectEqual(@as(u64, 0), manager.written_bytes);
}

test "SQL encoded admission reclaims declined census under shared pressure" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const Batch = @import("execution_batch.zig").Batch;
    const base = std.testing.allocator;
    var values: [64]Datum = undefined;
    var indices: [64]u32 = undefined;
    for (&values, &indices, 0..) |*value, *index, i| {
        value.* = Datum.json(.{ .integer = @intCast(i) });
        index.* = @intCast(i);
    }
    const keys: Batch = .{ .dictionary = .{ .values = &values, .indices = &indices } };
    const hashes = try @import("batch_hash.zig").encodedColumns(base, &.{keys}, values.len, true);
    defer base.free(hashes);
    var ordinals: [64]u64 = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = i;
    for ([_]usize{ 800, 850, 875, 900, 925, 950, 975, 985 }) |kib| {
        for ([_]bool{ true, false }) |batch| for ([_]bool{ false, true }) |wrapped| {
            var quota: MemoryBudget = .{ .backing = base, .limit = 1024 * 1024 };
            const a = quota.allocator();
            const reservation = try a.alloc(u8, kib * 1024);
            defer a.free(reservation);
            var raw: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &raw, .checkpoint = Hook.check, .async_writes = false, .compression = .none };
            defer manager.deinit();
            var locked: @import("parallel_scheduler.zig").LockedAllocator = .{ .backing = a };
            const group = try Grouped.create(if (wrapped) locked.allocator() else a, &.{.{ .kind = .count }}, .{ .bytes = 64 * 1024, .spill = &manager });
            defer group.deinit();
            const outcome: ?anyerror = blk: {
                if (batch) {
                    group.addEncodedColumnsIdentified(&.{keys}, &.{keys}, values.len, .{ .hashes = hashes, .ordinals = &ordinals }) catch |err| break :blk err;
                } else {
                    for (values) |value| group.add(&.{value}, &.{value}) catch |err| break :blk err;
                }
                break :blk null;
            };
            try std.testing.expect(outcome == null);
            try std.testing.expectEqual(@as(u64, values.len), group.rows_seen);
        };
    }
}

test "SQL compact retained physical admission under shared pressure" {
    const a = std.testing.allocator;
    const Batch = @import("execution_batch.zig").Batch;
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    for (0..4096) |_| _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
    const keys: Batch = .{ .retained = .{ .store = &store, .count = 4096 } };
    for ([_]usize{ 960, 1000, 1008 }) |reserved| for ([_]bool{ false, true }) |batched| for ([_]bool{ false, true }) |wrapped| {
        var quota: MemoryBudget = .{ .backing = a, .limit = 1024 * 1024 };
        const qa = quota.allocator();
        const reservation = try qa.alloc(u8, reserved * 1024);
        defer qa.free(reservation);
        var locked: @import("parallel_scheduler.zig").LockedAllocator = .{ .backing = qa };
        const group = try Grouped.create(if (wrapped) locked.allocator() else qa, &.{.{ .kind = .count }}, .{ .bytes = 64 * 1024 });
        defer group.deinit();
        if (batched) try group.addEncodedColumns(&.{keys}, &.{keys}, 4096) else for (0..4096) |_| try group.add(&.{Datum.json(.{ .integer = 7 })}, &.{Datum.json(.{ .integer = 7 })});
        try std.testing.expectEqual(@as(u64, 4096), group.rows_seen);
    };
}

test "SQL exact NUMERIC checkpoint columns admit exponent unions and preserve SUM AVG" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    const specs = [_]AggregateSpec{ .{ .kind = .sum, .input_type = .number, .input_element = .numeric }, .{ .kind = .avg, .input_type = .number, .input_element = .numeric } };
    var keys: [32]Datum = undefined;
    var sums: [32]Datum = undefined;
    var averages: [32]Datum = undefined;
    var ordinals: [32]u64 = undefined;
    var work: @import("array_value.zig").Budget = .{};
    for (&keys, &sums, &averages, &ordinals, 0..) |*key, *sum, *average, *ordinal, i| {
        key.* = Datum.json(.{ .integer = @intCast(i % 2) });
        ordinal.* = 100 - i;
        const input = try scalar.numericTextLeaky(pa, if (i % 4 < 2) "10000.0000" else "0.0001", &work);
        var sum_state = try Aggregate.initTyped(a, .sum, .number, .numeric);
        defer sum_state.deinit();
        var avg_state = try Aggregate.initTyped(a, .avg, .number, .numeric);
        defer avg_state.deinit();
        try sum_state.update(input);
        try avg_state.update(input);
        sum.* = try @import("aggregate_partial.zig").cell(pa, sum_state);
        average.* = try @import("aggregate_partial.zig").cell(pa, avg_state);
    }
    const Batch = @import("execution_batch.zig").Batch;
    const grouped = try Grouped.create(a, &specs, .{ .bytes = 128 * 1024 });
    defer grouped.deinit();
    const key_batch: Batch = .{ .vectors = .{ .values = &.{&keys}, .count = keys.len } };
    const sum_batch: Batch = .{ .vectors = .{ .values = &.{&sums}, .count = sums.len } };
    const avg_batch: Batch = .{ .vectors = .{ .values = &.{&averages}, .count = averages.len } };
    try std.testing.expect(try grouped.tryImportPartialBatch(&.{key_batch}, &.{ sum_batch, avg_batch }, 32, &ordinals));
    var count: usize = 0;
    while (try grouped.nextResult(pa)) |result| {
        const expected_sum = try scalar.numericTextLeaky(pa, "80000.0008", &work);
        const expected_avg = try scalar.numericTextLeaky(pa, "5000.00005", &work);
        try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(expected_sum, result.aggregates[0]));
        try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(expected_avg, result.aggregates[1]));
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "SQL wide encoded grouping falls back without optional views under parent pressure" {
    const a = std.testing.allocator;
    const Batch = @import("execution_batch.zig").Batch;
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    for (0..4096) |_| _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
    for ([_]usize{ 32, 64 }) |width| for ([_]usize{ 1, 4096 }) |row_count| {
        const key: Batch = .{ .retained = .{ .store = &store, .count = row_count } };
        const columns: [64]Batch = @splat(key);
        var parent: MemoryBudget = .{ .backing = a, .limit = 1024 * 1024 };
        const alloc = parent.allocator();
        const occupied = try alloc.alloc(u8, (if (width == 32) @as(usize, 992) else 960) * 1024);
        defer alloc.free(occupied);
        const group = try Grouped.create(alloc, &.{.{ .kind = .count }}, .{ .bytes = 128 * 1024 });
        defer group.deinit();
        try group.addEncodedColumns(columns[0..width], &.{key}, row_count);
        var result = std.heap.ArenaAllocator.init(a);
        defer result.deinit();
        const row = try group.resultAt(result.allocator(), 0);
        try std.testing.expectEqual(@as(u64, @intCast(row_count)), group.rows_seen);
        try std.testing.expectEqual(@as(i64, @intCast(row_count)), row.aggregates[0].value.integer);
        try std.testing.expect(group.source_ordinal == null);
    };
}

test "SQL encoded JSON hash join admission and repeated matches reuse retained payloads" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            const Probe = struct {
                fn json(_: *anyopaque, _: usize, _: usize) !?[]const u8 {
                    return "{\"x\":[1,2,3]}";
                }
                fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
                    return error.UnexpectedScalarMaterialization;
                }
            };
            var raw: u8 = 0;
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const alloc = scratch.allocator();
            const join = try HashJoin.create(a, .{ .bytes = 1 << 20 });
            defer join.deinit();
            try join.addBatch(alloc, .{ .reader = .{ .ptr = &raw, .read = Probe.cell, .read_json = Probe.json, .count = 1, .width = 1 } }, &.{&.{Datum.json(.{ .integer = 7 })}});
            var first_tree: ?[*]std.json.Value = null;
            for (0..16) |_| {
                var probe = try join.probe(&.{Datum.json(.{ .integer = 7 })});
                const match = (try probe.next()).?;
                const batch = try match.valuesBatch(alloc);
                try std.testing.expectEqualStrings("{\"x\":[1,2,3]}", (try batch.encodedJson(0, 0)).?);
                const values = try match.materializeValues(alloc);
                const tree = values[0].value.object.values().ptr;
                if (first_tree) |prior| try std.testing.expect(prior == tree) else first_tree = tree;
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

const EncodedAdmissionFixture = struct {
    bytes: []const u8,
    scalar_reads: usize = 0,
    allow_scalar: bool = false,
    fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
        return @as(*@This(), @ptrCast(@alignCast(raw))).bytes;
    }
    fn cell(raw: *anyopaque, a: Allocator, _: usize, _: usize) !Datum {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.scalar_reads += 1;
        if (!self.allow_scalar) return error.UnexpectedScalarMaterialization;
        return Datum.json(try std.json.parseFromSliceLeaky(Json, a, self.bytes, .{ .parse_numbers = false }));
    }
    const Measurement = struct { count: usize, scalar_reads: usize, peak: usize, retained: usize };
    fn run(a: Allocator, bytes: []const u8, limit: usize, count: usize, baseline: bool) !Measurement {
        var budget: MemoryBudget = .{ .backing = a, .limit = 128 << 20 };
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        const work = scratch.allocator();
        const join = try HashJoin.create(budget.allocator(), .{ .bytes = limit });
        defer join.deinit();
        var source: @This() = .{ .bytes = bytes, .allow_scalar = baseline };
        const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &source, .read = cell, .read_json = json, .count = count, .width = 1 } };
        const values = try work.alloc(Datum, count);
        const keys = try work.alloc([]const Datum, count);
        for (values, keys, 0..) |*value, *key, index| {
            value.* = Datum.json(.{ .integer = @intCast(index) });
            key.* = @as(*[1]Datum, @ptrCast(value));
        }
        var admitted: usize = 0;
        if (baseline) {
            // Keep the reference independent of native payload sharing in
            // production admission: the former estimate charged every body.
            var former_needed: usize = 4096 +| count *| (128 +| batch.width() *| 64);
            former_needed +|= bytes.len *| count *| 4;
            for (keys) |row| for (row) |value| {
                former_needed +|= (try datumBytes(value)) *| 4;
            };
            if (former_needed <= limit / 2 and try join.admitBatch(work, batch, keys)) admitted = count else {
                var row_scratch = std.heap.ArenaAllocator.init(budget.allocator());
                defer row_scratch.deinit();
                for (keys, 0..) |key, index| {
                    _ = row_scratch.reset(.retain_capacity);
                    const value = try batch.cell(row_scratch.allocator(), index, 0);
                    const estimate = 4096 +| (try join.value_columns.appendBytes(&.{value})) *| 4 +| (try join.key_columns.appendBytes(key)) *| 4;
                    if (join.budget.live > join.budget.limit / 2 or estimate > join.budget.limit -| join.budget.live) break;
                    try join.add(&.{value}, key);
                    admitted += 1;
                }
            }
        } else admitted = try join.addBatchUntilFull(work, batch, keys);
        if (!baseline) {
            try std.testing.expectEqual(@as(usize, 0), source.scalar_reads);
            try std.testing.expect(admitted > 0);
            for (0..admitted) |index| try std.testing.expectEqualStrings(bytes, (try join.value_columns.encodedJson(index, 0)).?);
        }
        return .{ .count = admitted, .scalar_reads = source.scalar_reads, .peak = budget.peak, .retained = join.budget.peak };
    }
};
test "SQL encoded JSON join partial admission retains native prefixes through quota and allocation failures" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            const result = try EncodedAdmissionFixture.run(a, "[1,2,3]", 64 << 10, 256, false);
            try std.testing.expect(result.count < 256);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
test "native projection refinements benchmark encoded join prefix admission" {
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    var text: std.Io.Writer.Allocating = .init(source.allocator());
    try text.writer.writeByte('[');
    for (0..1024) |i| {
        if (i != 0) try text.writer.writeByte(',');
        try text.writer.writeAll("{\"x\":1,\"s\":\"nested value\"}");
    }
    try text.writer.writeByte(']');
    const before = try EncodedAdmissionFixture.run(std.testing.allocator, text.written(), 8 << 20, 64, true);
    const after = try EncodedAdmissionFixture.run(std.testing.allocator, text.written(), 8 << 20, 64, false);
    try std.testing.expectEqual(@as(usize, 64), after.count);
    try std.testing.expect(before.count < after.count and before.retained > after.retained);
    std.debug.print("JOIN_PREFIX before_rows={d} after_rows={d} before_scalar_reads={d} after_scalar_reads={d} before_retained_peak={d} after_retained_peak={d} before_backing_peak={d} after_backing_peak={d} wire_per_row={d}\n", .{ before.count, after.count, before.scalar_reads, after.scalar_reads, before.retained, after.retained, before.peak, after.peak, text.written().len });
}

test "SQL shared payload admission fits duplicate JSON within the hard budget" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            var text: std.Io.Writer.Allocating = .init(source.allocator());
            try text.writer.writeAll("{\"payload\":\"");
            try text.writer.splatByteAll('x', 12000);
            try text.writer.writeAll("\"}");
            const result = try EncodedAdmissionFixture.run(a, text.written(), 64 << 10, 64, false);
            try std.testing.expectEqual(@as(usize, 64), result.count);
            try std.testing.expect(result.retained < 64 << 10);
            std.debug.print("SHARED_ADMISSION rows={d} retained_peak={d} backing_peak={d}\n", .{ result.count, result.retained, result.peak });
        }
    };
    try Fixture.run(std.testing.allocator);
}

test "SQL native prefix retries reuse content hashes and restart counting" {
    const Fixture = struct {
        fn run(a: Allocator, length: usize, measure: bool) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const prefix = "{\"payload\":\"";
            const text = try arena.allocator().alloc(u8, prefix.len + length + 2);
            @memcpy(text[0..prefix.len], prefix);
            @memset(text[prefix.len..][0..length], 'x');
            @memcpy(text[text.len - 2 ..], "\"}");
            const join = try HashJoin.create(a, .{ .bytes = 64 << 10 });
            defer join.deinit();
            var fixture: EncodedAdmissionFixture = .{ .bytes = text };
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &fixture, .read = EncodedAdmissionFixture.cell, .read_json = EncodedAdmissionFixture.json, .count = 64, .width = 1 } };
            const key = Datum.json(.{ .integer = 1 });
            const keys: [64][]const Datum = @splat(&.{key});
            var memo = @import("typed_store.zig").Store.JsonLease.init(a);
            defer memo.deinit();
            try std.testing.expectEqual(@as(usize, 0), try join.admitPrefixLeased(a, batch, &keys, 0, &memo));
            try std.testing.expectEqual(text.len, memo.hashed_bytes);
            try std.testing.expectEqual(@as(usize, 0), join.row_count);
            try std.testing.expectEqual(@as(usize, 0), fixture.scalar_reads);
            if (measure) std.debug.print("PREFIX_HASH_REUSE rows=64 payload={d} hash_input={d} admitted=0\n", .{ text.len, memo.hashed_bytes });
        }
    };
    try Fixture.run(std.testing.allocator, 1 << 20, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ @as(usize, 16384), false });
}

test "SQL native disk chains filter keys before reading encoded payloads" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, frozen: bool, compressed: bool, count: usize) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const prefix = "{\"x\":9007199254740993,\"payload\":\"";
            const text = try sa.alloc(u8, prefix.len + 8192 + 2);
            @memcpy(text[0..prefix.len], prefix);
            @memset(text[prefix.len..][0..8192], 'x');
            @memcpy(text[text.len - 2 ..], "\"}");
            const Reader = struct {
                text: []const u8,
                frozen: ?@import("execution_batch.zig").FrozenJson = null,
                fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
                    return error.UnexpectedScalarMaterialization;
                }
                fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    return if (self.frozen == null) self.text else null;
                }
                fn tokens(raw: *anyopaque, _: usize, _: usize) !?@import("execution_batch.zig").FrozenJson {
                    return @as(*@This(), @ptrCast(@alignCast(raw))).frozen;
                }
            };
            var reader: Reader = .{ .text = text };
            if (frozen) {
                const root = try std.json.parseFromSliceLeaky(Json, sa, text, .{});
                const wire = try @import("spill.zig").encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const decoded = try @import("spill.zig").decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
                defer decoded.deinit();
                reader.frozen = (try decoded.frozenJson(0, 0, false)).?;
            }
            var dummy: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false };
            defer manager.deinit();
            const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
            defer join.deinit();
            const key = Datum.json(.{ .integer = 1 });
            const keys: [64][]const Datum = @splat(&.{key});
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_json = Reader.json, .read_frozen = Reader.tokens, .count = count, .width = 1 } };
            try join.addBatch(sa, batch, keys[0..count]);
            try std.testing.expect(join.disk != null);
            const bucket = (try HashJoin.keyHash(&.{key})).? & (join.disk_heads.len - 1);
            var wrong: i64 = 2;
            while (((try HashJoin.keyHash(&.{Datum.json(.{ .integer = wrong })})).? & (join.disk_heads.len - 1)) != bucket) : (wrong += 1) {}
            var misses = try join.probe(&.{Datum.json(.{ .integer = wrong })});
            try std.testing.expect((try misses.next()) == null);
            try std.testing.expectEqual(@as(usize, 0), join.payload_reads);
            try std.testing.expectEqual(@as(usize, 0), join.disk_values.?.file.read_buffer.len);
            try std.testing.expectEqual(@as(u64, count), join.probes);
            if (count == 64 and !frozen and !compressed) std.debug.print("NATIVE_DISK_KEYS candidates={d} payload_reads={d} payload_bytes={d}\n", .{ count, join.payload_reads, reader.text.len });
            var probe = try join.probe(&.{key});
            const match = (try probe.next()).?;
            try std.testing.expectEqual(@as(usize, 0), join.payload_reads);
            const values = try match.valuesBatch(sa);
            if (frozen) try std.testing.expectEqualSlices(u8, reader.frozen.?.bytes, (try values.frozenJson(0, 0)).?.bytes) else try std.testing.expectEqualStrings(reader.text, (try values.encodedJson(0, 0)).?);
            try std.testing.expectEqual(@as(usize, 1), join.payload_reads);
            try join.markMatched(match.index);
            var next: usize = 0;
            var unmatched: usize = 0;
            while (try join.unmatched(&next)) |_| unmatched += 1;
            try std.testing.expectEqual(count - 1, unmatched);
            try std.testing.expectEqual(@as(usize, 1), join.payload_reads);
            const scalar_values = try match.materializeValues(sa);
            const number = scalar_values[0].value.object.get("x").?;
            if (frozen) try std.testing.expectEqual(@as(i64, 9007199254740993), number.integer) else try std.testing.expectEqualStrings("9007199254740993", number.number_string);
        }
    };
    for ([_]bool{ false, true }) |frozen| for ([_]bool{ false, true }) |compressed| try Fixture.run(std.testing.allocator, frozen, compressed, 64);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ false, true, @as(usize, 3) });
}

test "SQL native chains keep narrow rows inline and mix native payloads" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
    defer join.deinit();
    try join.startSpill();
    const key = Datum.json(.{ .integer = 5 });
    const narrow: [8]Datum = @splat(Datum.json(.{ .integer = 42 }));
    try join.add(&narrow, &.{key});
    try std.testing.expect(join.disk_values == null);
    const text: [4096]u8 = @splat('x');
    try join.add(&.{Datum.json(.{ .string = &text })}, &.{key});
    try std.testing.expect(join.disk_values != null);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var probe = try join.probe(&.{key});
    const large = (try probe.next()).?;
    try std.testing.expect(large.native != null);
    try std.testing.expectEqualStrings(&text, (try large.materializeValues(arena.allocator()))[0].value.string);
    const small = (try probe.next()).?;
    try std.testing.expect(small.native == null);
    const small_values = try small.materializeValues(arena.allocator());
    try std.testing.expectEqual(@as(usize, 8), small_values.len);
    try std.testing.expectEqual(@as(i64, 42), small_values[0].value.integer);
    try std.testing.expect((try probe.next()) == null);
}

test "SQL native spill pages share leased payloads pack distinct lanes and retain evicted blocks" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, repeated: bool, scalar_input: bool, count: usize, measure: bool) !void {
            const spill = @import("spill.zig");
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
            var live = true;
            defer if (live) join.deinit();
            try join.startSpill();
            const key = Datum.json(.{ .integer = 1 });
            {
                var source = std.heap.ArenaAllocator.init(a);
                defer source.deinit();
                const sa = source.allocator();
                const rows = try sa.alloc([]const Datum, count);
                const keys = try sa.alloc([]const Datum, count);
                const body_len: usize = if (repeated) 8192 else 1536;
                const body = try sa.alloc(u8, body_len);
                @memset(body, 'x');
                for (rows, keys, 0..) |*row, *keys_, index| {
                    const bytes = if (repeated) body else try sa.dupe(u8, body);
                    if (!repeated) bytes[0] = @intCast('a' + index);
                    const cells = try sa.alloc(Datum, 1);
                    cells[0] = Datum.json(.{ .string = bytes });
                    row.* = cells;
                    keys_.* = &.{key};
                    if (scalar_input) try join.add(cells, keys_.*);
                }
                if (!scalar_input) try join.addBatch(sa, .{ .rows = rows }, keys);
                // Neither identity keys nor payload bytes may borrow this arena.
                @memset(body, 'z');
            }
            var pinned: ?*spill.Sequential.OwnedBlock = null;
            var pinned_row: usize = 0;
            defer if (pinned) |block| block.release();
            for (0..2) |_| {
                var probe = try join.probe(&.{key});
                var seen: usize = 0;
                while (try probe.next()) |match| {
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const batch = try match.valuesBatch(arena.allocator());
                    try std.testing.expectEqual(@as(usize, 1), batch.len());
                    const cell = try batch.cell(arena.allocator(), 0, 0);
                    try std.testing.expectEqual(@as(u8, if (repeated) 'x' else @intCast('a' + count - seen - 1)), cell.value.string[0]);
                    if (pinned == null) {
                        pinned_row = match.native.?.row;
                        pinned = try match.native.?.block();
                        pinned.?.retain();
                    }
                    var cached_bytes: usize = 0;
                    var cached_pages: usize = 0;
                    for (join.disk_payloads) |entry| if (entry) |cached| {
                        cached_pages += 1;
                        cached_bytes += cached.block.retainedBytes();
                    };
                    try std.testing.expect(cached_pages <= 4);
                    try std.testing.expect(cached_bytes <= 64 * 1024 or cached_pages == 1);
                    seen += 1;
                }
                try std.testing.expectEqual(count, seen);
            }
            if (repeated or !scalar_input) try std.testing.expectEqual(@as(usize, 1), join.payload_reads);
            if (repeated) {
                try std.testing.expect(join.disk_values.?.file.size < 9000);
                try std.testing.expectEqual(@as(usize, 1), join.disk_values.?.encoded_pages);
            }
            if (measure) std.debug.print("SHARED_SPILL rows={d} repeated={} scalar={} payload_bytes={d} payload_reads={d}\n", .{ count, repeated, scalar_input, join.disk_values.?.file.size, join.payload_reads });
            // Force eviction; the accepted output lease must still own its page.
            for (0..6) |index| {
                var body: [4096]u8 = @splat('q');
                body[0] = @intCast('A' + index);
                const offset = try join.disk_values.?.appendBatch(.{ .rows = &.{&.{Datum.json(.{ .string = &body })}} });
                _ = try join.diskBlock(offset);
            }
            join.deinit();
            live = false;
            try std.testing.expectEqual(@as(u8, if (repeated) 'x' else @intCast('a' + count - 1)), (try pinned.?.cell(pinned_row, 0)).value.string[0]);
        }
    };
    for ([_]bool{ false, true }) |repeated| for ([_]bool{ false, true }) |scalar_input| {
        try Fixture.run(std.testing.allocator, repeated, scalar_input, if (repeated) 64 else 8, true);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ repeated, scalar_input, @as(usize, 3), false });
    };
}

test "SQL native spill preserves owner bound patterns and wide rows under tiny budgets" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, width: usize, patterns: bool, compressed: bool) !void {
            var dummy: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false };
            defer manager.deinit();
            const pattern = if (patterns) try @import("pattern_spill.zig").Set.create(&manager) else null;
            if (pattern) |set| try set.append(Datum.json(.{ .string = "%cat%" }));
            const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
            defer join.deinit();
            try join.startSpill();
            const key = Datum.json(.{ .integer = 1 });
            const values = try a.alloc(Datum, width);
            defer a.free(values);
            @memset(values, Datum.json(.{ .integer = 42 }));
            const body: [4096]u8 = @splat('x');
            if (pattern) |set| {
                values[0] = .{ .sql_null = false, .patterns = &set.interface };
                values[1] = Datum.json(.{ .string = &body });
            }
            try join.add(values, &.{key});
            var probe = try join.probe(&.{key});
            const match = (try probe.next()).?;
            var output = std.heap.ArenaAllocator.init(a);
            defer output.deinit();
            const result = try match.materializeValues(output.allocator());
            try std.testing.expectEqual(width, result.len);
            if (pattern) |set| {
                try std.testing.expect(result[0].patterns == &set.interface);
                var offset: u64 = 0;
                const item = (try result[0].patterns.?.next(result[0].patterns.?.ptr, output.allocator(), &offset)).?;
                try std.testing.expectEqualStrings("%cat%", item.value.string);
                try std.testing.expectEqualStrings(&body, result[1].value.string);
            } else for (result) |cell| try std.testing.expectEqual(@as(i64, 42), cell.value.integer);
            try std.testing.expect((try probe.next()) == null);
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        for ([_]usize{ 8, 32, 64, 100, 128 }) |width| try Fixture.run(std.testing.allocator, width, false, compressed);
        try Fixture.run(std.testing.allocator, 2, true, compressed);
    }
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ @as(usize, 64), false, false });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ @as(usize, 2), true, true });
}

test "SQL native payload cache eviction includes lazy JSON memory and preserves output leases" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
    defer manager.deinit();
    const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
    defer join.deinit();
    try join.startSpill();
    {
        var source = std.heap.ArenaAllocator.init(a);
        defer source.deinit();
        const items = try source.allocator().alloc(Json, 2048);
        for (items, 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
        try join.add(&.{Datum.json(.{ .array = std.json.Array.fromOwnedSlice(source.allocator(), items) })}, &.{});
    }
    var probe = try join.probe(&.{});
    const match = (try probe.next()).?;
    const block = try match.native.?.block();
    block.retain();
    defer block.release();
    const cold = block.retainedBytes();
    const values = try block.cell(0, 0);
    try std.testing.expectEqual(@as(usize, 2048), values.value.array.items.len);
    try std.testing.expect(block.retainedBytes() > cold);
    try std.testing.expect(block.retainedBytes() > 64 * 1024);
    const next = try join.disk_values.?.appendBatch(.{ .rows = &.{&.{Datum.json(.{ .integer = 1 })}} });
    _ = try join.diskBlock(next);
    for (join.disk_payloads) |entry| if (entry) |cached| try std.testing.expect(cached.offset != match.native.?.offset);
    // Eviction drops only the cache reference, leaving accepted output intact.
    try std.testing.expectEqual(@as(i64, 2047), (try block.cell(0, 0)).value.array.items[2047].integer);
}

test "SQL native payload read cache uses actual allocations under statement sized record ceilings" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, quota: usize, compressed: bool, pressure: bool, measure: bool) !void {
            const spill = @import("spill.zig");
            const Budget = @import("memory_budget.zig");
            var budget: Budget = .{ .backing = a, .limit = quota };
            defer std.debug.assert(budget.live == 0);
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = check, .max_record_bytes = quota, .compression = if (compressed) .snappy else .none, .async_writes = false };
            defer manager.deinit();
            const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
            var live = true;
            defer if (live) join.deinit();
            try join.startSpill();
            var body: [8192]u8 = @splat('x');
            const key = Datum.json(.{ .integer = 1 });
            try join.add(&.{Datum.json(.{ .string = &body })}, &.{key});
            body[0] = 'y';
            try join.add(&.{Datum.json(.{ .string = &body })}, &.{key});
            for (0..2) |_| {
                var probe = try join.probe(&.{key});
                var count: usize = 0;
                while (try probe.next()) |match| {
                    const block = try match.native.?.block();
                    try std.testing.expectEqual(@as(u8, if (count == 0) 'y' else 'x'), (try block.cell(0, 0)).value.string[0]);
                    count += 1;
                }
                try std.testing.expectEqual(@as(usize, 2), count);
            }
            try std.testing.expectEqual(@as(usize, 2), join.payload_reads);
            if (measure) std.debug.print("SPILL_READ_QUOTA compressed={} quota={d} reads={d} peak={d}\n", .{ compressed, quota, join.payload_reads, budget.peak });
            if (pressure) {
                // Pin an output page, then force admission to drop cache refs.
                const first = join.disk_payloads[0].?.block;
                first.retain();
                defer first.release();
                const first_offset = join.disk_payloads[0].?.offset;
                const second_offset = join.disk_payloads[1].?.offset;
                while (join.disk_values.?.evictOne()) {}
                const incoming = first.retainedBytes();
                join.clearPayloadCache();
                const available = Budget.headroom(manager.alloc).?;
                const ballast = try manager.allocator().alloc(u8, available - incoming * 2);
                defer manager.allocator().free(ballast);
                // Both pinned and cached pages fit, but a third decoded page
                // requires eviction of the unpinned cache entry.
                _ = try join.diskBlock(second_offset);
                _ = try join.diskBlock(first_offset);
                var cached_pages: usize = 0;
                for (join.disk_payloads) |entry| if (entry != null) {
                    cached_pages += 1;
                };
                try std.testing.expectEqual(@as(usize, 1), cached_pages);
                try std.testing.expect(!budget.isExhausted());
                join.deinit();
                live = false;
                try std.testing.expectEqual(@as(u8, 'y'), (try first.cell(0, 0)).value.string[0]);
            }
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        for ([_]usize{ 2 * 1024 * 1024, 32 * 1024 * 1024 }) |quota|
            try Fixture.run(std.testing.allocator, quota, compressed, true, true);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ @as(usize, 2 * 1024 * 1024), compressed, false, false });
    }
}

test "SQL native spill keys share cache pressure across scalar and cohort writes" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, batch: bool, key_bytes: usize, count: usize, measure: bool) !void {
            var budget: MemoryBudget = .{ .backing = a, .limit = 65536 };
            defer std.debug.assert(budget.live == 0);
            var dummy: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const join = try HashJoin.create(budget.allocator(), .{ .bytes = 8192, .spill = &manager });
            defer join.deinit();
            try join.startSpill();
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const rows = try sa.alloc([]const Datum, count);
            const keys = try sa.alloc([]const Datum, count);
            const key = try sa.alloc(u8, key_bytes);
            @memset(key, 'k');
            for (rows, keys, 0..) |*row, *keys_, index| {
                const body = try sa.alloc(u8, 12000);
                @memset(body, 'x');
                body[0] = @intCast('a' + index);
                const cells = try sa.alloc(Datum, 1);
                cells[0] = Datum.json(.{ .string = body });
                row.* = cells;
                const key_cells = try sa.alloc(Datum, 1);
                key_cells[0] = Datum.json(.{ .string = key });
                keys_.* = key_cells;
                if (!batch) try join.add(cells, key_cells);
            }
            if (batch) try join.addBatch(sa, .{ .rows = rows }, keys);
            try std.testing.expectEqual(count, join.row_count);
            try std.testing.expect(!budget.isExhausted());
            if (measure) std.debug.print("SPILL_KEY_ADMISSION batch={} rows={d} key={d} limit={d} peak={d}\n", .{ batch, count, key_bytes, budget.limit, budget.peak });
        }
    };
    for ([_]bool{ false, true }) |batch| {
        for ([_]usize{ 16384, 24000 }) |key_bytes| try Fixture.run(std.testing.allocator, batch, key_bytes, 10, true);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ batch, @as(usize, 16384), @as(usize, 3), false });
    }
}

test "SQL native cohort sizing reuses immutable array estimates and validates each key" {
    const Fixture = struct {
        array: *@import("array_value.zig").Value,
        reads: usize = 0,
        fn read(raw: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return Datum.typedArray(self.array);
        }
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, count: usize, cardinality: usize, measure: bool) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const elements = try sa.alloc(Datum, cardinality);
            for (elements, 0..) |*element, index| element.* = Datum.json(.{ .integer = @intCast(index) });
            var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = @intCast(cardinality) }}, elements, .{});
            var fixture: @This() = .{ .array = &array };
            var dummy: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
            defer join.deinit();
            try join.startSpill();
            const keys = try sa.alloc([]const Datum, count);
            for (keys) |*key| key.* = &.{Datum.json(.{ .integer = 1 })};
            try join.addBatch(sa, .{ .reader = .{ .ptr = &fixture, .read = read, .count = count, .width = 1 } }, keys);
            try std.testing.expectEqual(count + 2, fixture.reads);
            try std.testing.expectEqual(@as(usize, 1), join.disk_values.?.encoded_pages);
            if (measure) std.debug.print("SPILL_ARRAY_SIZING rows={d} elements={d} cell_reads={d} encoded_pages=1\n", .{ count, cardinality, fixture.reads });
            // Payload memoization cannot skip per-row key admission.
            const estimate = try HashJoin.PayloadEstimate.init(.{ .rows = &.{&.{Datum.json(.{ .integer = 1 })}} }, 0, sa);
            try std.testing.expect(try estimate.recordBytes(&.{Datum.json(.{ .integer = 1 })}, 8192) == null);
            var body: [8192]u8 = @splat('k');
            try std.testing.expect(try estimate.recordBytes(&.{Datum.json(.{ .string = &body })}, 8192) != null);
        }
    };
    try Fixture.run(std.testing.allocator, 64, 8192, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ @as(usize, 3), @as(usize, 128), false });
}

test "SQL native spill classification keeps ordinary inline rows on one sizing read" {
    const Reader = struct {
        reads: usize = 0,
        fn cell(raw: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return Datum.json(.{ .integer = 7 });
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Reader.check, .async_writes = false };
    defer manager.deinit();
    const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
    defer join.deinit();
    try join.startSpill();
    var reader: Reader = .{};
    var keys: [64][]const Datum = undefined;
    for (&keys) |*key| key.* = &.{Datum.json(.{ .integer = 1 })};
    try join.addBatch(source.allocator(), .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .count = 64, .width = 1 } }, &keys);
    try std.testing.expect(join.disk_values == null);
    try std.testing.expectEqual(@as(usize, 128), reader.reads);
}

test "SQL native key reads evict optional caches before parent admission" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator) !void {
            var budget: MemoryBudget = .{ .backing = a, .limit = 98304 };
            defer std.debug.assert(budget.live == 0);
            var dummy: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const join = try HashJoin.create(budget.allocator(), .{ .bytes = budget.limit, .spill = &manager });
            defer join.deinit();
            try join.startSpill();
            var body: [12000]u8 = @splat('x');
            const key: [8192]u8 = @splat('k');
            for (0..10) |index| {
                body[0] = @intCast('a' + index);
                try join.add(&.{Datum.json(.{ .string = &body })}, &.{Datum.json(.{ .string = &key })});
            }
            var probe = try join.probe(&.{Datum.json(.{ .string = &key })});
            var found: usize = 0;
            while (try probe.next()) |match| {
                const block = try match.native.?.block();
                try std.testing.expectEqual(@as(u8, @intCast('j' - found)), (try block.cell(0, 0)).value.string[0]);
                found += 1;
            }
            try std.testing.expectEqual(@as(usize, 10), found);
            var offset: usize = 0;
            var unmatched_count: usize = 0;
            while (try join.unmatched(&offset)) |match| {
                const block = try match.native.?.block();
                _ = try block.cell(0, 0);
                unmatched_count += 1;
            }
            try std.testing.expectEqual(@as(usize, 10), unmatched_count);
            try std.testing.expect(!budget.isExhausted());
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL native small limit rejects batch losers without reading payloads" {
    const Reader = struct {
        reads: usize = 0,
        fn cell(raw: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return error.UnexpectedPayloadMaterialization;
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var top = try TopK.init(a, 1, &.{.{}}, 64 * 1024);
    defer top.deinit();
    try top.add(.{ .values = &.{Datum.json(.{ .integer = 99 })}, .keys = &.{Datum.json(.{ .integer = 0 })}, .ordinal = 0 });
    var reader: Reader = .{};
    const loser = [_]Datum{Datum.json(.{ .integer = 1 })};
    try top.addBatch(arena.allocator(), .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .count = 3, .width = 1 } }, &.{ &loser, &loser, &loser }, &.{ 1, 2, 3 });
    try std.testing.expectEqual(@as(usize, 0), reader.reads);
    try std.testing.expectEqual(@as(usize, 3), top.comparisons);
    const result = try top.finish(arena.allocator());
    try std.testing.expectEqual(@as(i64, 99), result[0].values[0].value.integer);
}

test "SQL native small heap retains frozen JSON within parent quotas" {
    const Reader = struct {
        block: *@import("spill.zig").ColumnarBlock,
        fn read(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.block.cell(row, column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.block.frozenJson(row, column, false);
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]usize{ 131072, 262144 }) |quota| {
        const elements: usize = 4096;
        var input = std.heap.ArenaAllocator.init(a);
        defer input.deinit();
        const ia = input.allocator();
        var array = std.json.Array.init(ia);
        for (0..elements) |i| try array.append(.{ .integer = @intCast(i) });
        const spill = @import("spill.zig");
        const wire = try spill.encodeColumnarBlockVersionAlloc(ia, &.{.{ .values = &.{Datum.json(.{ .array = array })}, .keys = &.{}, .ordinal = 0 }}, 8 << 20, .v2);
        var budget: MemoryBudget = .{ .backing = a, .limit = quota };
        defer std.debug.assert(budget.live == 0);
        var block = try spill.decodeColumnarBlockForForwarding(ia, budget.allocator(), wire, 8 << 20);
        defer block.deinit();
        var reader: Reader = .{ .block = &block };
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128, .max_record_bytes = quota };
        defer manager.deinit();
        var top = try TopK.initWithSpill(budget.allocator(), 1, &.{.{}}, quota, &manager);
        defer top.deinit();
        var work = std.heap.ArenaAllocator.init(budget.allocator());
        defer work.deinit();
        for (0..12) |i| {
            _ = work.reset(.free_all);
            const key = Datum.json(.{ .integer = @intCast(12 - i) });
            try top.addBatch(work.allocator(), .{ .reader = .{ .ptr = &reader, .read = Reader.read, .read_frozen = Reader.frozen, .count = 1, .width = 1 } }, &.{&.{key}}, &.{i});
        }
        try std.testing.expect(top.external == null);
        const rows = try top.finishNative(work.allocator());
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqual(@as(i64, 1), rows[0].keys[0].value.integer);
        const frozen = (try top.nativeStore(0).?.frozenJson(0, 0)).?;
        _ = frozen;
        try std.testing.expectEqual(@as(u64, 0), manager.written_bytes);
        // An empty selected window must not expand its skipped winner.
        const empty = try top.finishPage(work.allocator(), 1, 0);
        try std.testing.expectEqual(@as(usize, 0), empty.len);
        std.debug.print("NATIVE_TOPK quota={d} peak={d} wire={d} replacements=11\n", .{ quota, budget.peak, wire.len });
    }
}

test "SQL native reference replay stays frozen under tight quotas and unwinds every allocation" {
    const Reader = struct {
        block: *@import("spill.zig").ColumnarBlock,
        fn read(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.block.cell(row, column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.block.frozenJson(row, column, false);
        }
        fn check(_: *anyopaque) !void {}
    };
    const Harness = struct {
        fn run(a: Allocator, quota: usize, elements: usize, replay: bool, measure: bool) !void {
            var input = std.heap.ArenaAllocator.init(a);
            defer input.deinit();
            const ia = input.allocator();
            var array = std.json.Array.init(ia);
            for (0..elements) |i| try array.append(.{ .integer = @intCast(i) });
            const spill = @import("spill.zig");
            const wire = try spill.encodeColumnarBlockVersionAlloc(ia, &.{.{ .values = &.{Datum.json(.{ .array = array })}, .keys = &.{}, .ordinal = 0 }}, 8 << 20, .v2);
            var budget: MemoryBudget = .{ .backing = a, .limit = quota };
            defer std.debug.assert(budget.live == 0);
            var block = try spill.decodeColumnarBlockForForwarding(ia, budget.allocator(), wire, 8 << 20);
            defer block.deinit();
            var reader: Reader = .{ .block = &block };
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128, .max_record_bytes = quota };
            defer manager.deinit();
            var top = try TopK.initWithSpill(budget.allocator(), 1, &.{.{}}, quota, &manager);
            defer top.deinit();
            var work = std.heap.ArenaAllocator.init(budget.allocator());
            defer work.deinit();
            try top.startSpill();
            const Cursor = @import("result_cursor.zig").Cursor;
            const cursor = try Cursor.create(budget.allocator(), &manager, 1);
            defer cursor.close();

            top.external.?.memory_bytes = 4096;
            try top.addBatch(work.allocator(), .{ .reader = .{ .ptr = &reader, .read = Reader.read, .read_frozen = Reader.frozen, .count = 1, .width = 1 } }, &.{&.{Datum.json(.{ .integer = 1 })}}, &.{0});
            try Cursor.takeSorted(cursor, &top, 0, 1, false);
            if (replay) {
                {
                    const first = try cursor.openReplayReader();
                    defer first.close();
                    const second = try cursor.openReplayReader();
                    defer second.close();
                    try std.testing.expect(cursor.payloads != null);
                    for (0..3) |_| {
                        first.rewind();
                        second.rewind();
                        const page = try first.nextBatch(work.allocator(), 1);
                        try std.testing.expect((try page.frozenJson(0, 0)) != null);
                        const independent = try second.nextBatch(work.allocator(), 1);
                        try std.testing.expect((try independent.frozenJson(0, 0)) != null);
                        try std.testing.expectEqual(@as(usize, 0), (try first.nextBatch(work.allocator(), 1)).len());
                        try std.testing.expect((try independent.frozenJson(0, 0)) != null);
                    }
                }
                const lease = try cursor.nextLease(work.allocator(), 1, quota);
                defer lease.deinit();
                try std.testing.expect((try lease.values.frozenJson(0, 0)) != null);
            } else {
                const lease = try cursor.nextLease(work.allocator(), 1, quota);
                defer lease.deinit();
                try std.testing.expect((try lease.values.frozenJson(0, 0)) != null);
            }
            if (measure) std.debug.print("NATIVE_REFERENCE_REPLAY quota={d} replay={} peak={d}\n", .{ quota, replay, budget.peak });
        }
    };
    for ([_]usize{ 131072, 262144 }) |quota| for ([_]bool{ false, true }) |replay| try Harness.run(std.testing.allocator, quota, 4096, replay, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ @as(usize, 131072), @as(usize, 256), true, false });
}

test "SQL native heap winners share bodies and scalar caches across distinct rows and retirement" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, count: usize, elements: usize, measure: bool) !void {
            var budget: MemoryBudget = .{ .backing = a, .limit = 1024 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var token: u8 = 0;
            const spill = @import("spill.zig");
            var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = check };
            defer manager.deinit();
            var top = try TopK.initWithSpill(budget.allocator(), count, &.{.{}}, 1024 * 1024, &manager);
            defer top.deinit();
            var work = std.heap.ArenaAllocator.init(budget.allocator());
            defer work.deinit();
            var wire_bytes: usize = 0;
            {
                var source = std.heap.ArenaAllocator.init(a);
                defer source.deinit();
                const sa = source.allocator();
                var array = std.json.Array.init(sa);
                for (0..elements) |i| try array.append(.{ .integer = @intCast(i) });
                const wire = try spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .array = array })}, .keys = &.{}, .ordinal = 0 }}, 1024 * 1024, .v2);
                wire_bytes = wire.len;
                var block = try spill.decodeColumnarBlockForForwarding(sa, a, wire, 1024 * 1024);
                defer block.deinit();
                const Reader = struct {
                    frozen: @import("execution_batch.zig").FrozenJson,
                    fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
                        if (column == 0) return error.UnexpectedMaterialization;
                        return Datum.json(.{ .integer = @intCast(row) });
                    }
                    fn tokens(raw: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                        return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).frozen else null;
                    }
                };
                var reader: Reader = .{ .frozen = (try block.frozenJson(0, 0, false)).? };
                var key_values: [64]Datum = undefined;
                var keys: [64][]const Datum = undefined;
                var ordinals: [64]u64 = undefined;
                for (0..count) |i| {
                    key_values[i] = Datum.json(.{ .integer = @intCast(count - i) });
                    keys[i] = key_values[i..][0..1];
                    ordinals[i] = i;
                }
                try top.addBatch(work.allocator(), .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.tokens, .count = count, .width = 2 } }, keys[0..count], ordinals[0..count]);
            }
            try std.testing.expect(top.external == null);
            try std.testing.expectEqual(@as(usize, 1), top.json_pool.?.bodies.count());
            try std.testing.expect(top.retained_bytes < 384 * 1024);
            if (measure) std.debug.print("NATIVE_HEAP_SHARED rows={d} frozen={d} retained={d} peak={d}\n", .{ count, wire_bytes, top.retained_bytes, budget.peak });
            const rows = try top.finishNative(work.allocator());
            for (rows, 0..) |row, i| {
                try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.keys[0].value.integer);
                try std.testing.expectEqual(@as(i64, @intCast(count - i - 1)), (try top.nativeStore(i).?.cell(work.allocator(), 0, 1)).value.integer);
            }
            const first = try top.scalarRow(0);
            const last = try top.scalarRow(count - 1);
            try std.testing.expect(first.values[0].value.array.items.ptr == last.values[0].value.array.items.ptr);
            try std.testing.expectEqual(elements, last.values[0].value.array.items.len);
            for (0..count) |i| top.releaseFinishedRow(i);
            try std.testing.expectEqual(@as(usize, 0), top.json_pool.?.bodies.count());
            try std.testing.expectEqual(@as(usize, 0), top.json_pool.?.cache_budget.live);
        }
    };
    try Harness.run(std.testing.allocator, 64, 4096, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ @as(usize, 4), @as(usize, 64), false });
}

test "SQL native heap pooled bodies reclaim cached values during competitive churn" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, iterations: usize, body_bytes: usize) !void {
            var budget: MemoryBudget = .{ .backing = a, .limit = if (body_bytes > 1024) 128 * 1024 else 64 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var token: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = check };
            defer manager.deinit();
            var top = try TopK.initWithSpill(budget.allocator(), 1, &.{.{}}, 64 * 1024, &manager);
            defer top.deinit();
            const Reader = struct {
                bytes: []const u8,
                fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
                    return error.UnexpectedMaterialization;
                }
                fn text(raw: *anyopaque, _: usize, _: usize) anyerror!?[]const u8 {
                    return @as(*@This(), @ptrCast(@alignCast(raw))).bytes;
                }
            };
            var bytes: [32768]u8 = @splat('x');
            bytes[0] = '"';
            bytes[body_bytes - 1] = '"';
            var reader: Reader = .{ .bytes = bytes[0..body_bytes] };
            var scratch = std.heap.ArenaAllocator.init(budget.allocator());
            defer scratch.deinit();
            for (0..iterations) |i| {
                _ = scratch.reset(.free_all);
                _ = try std.fmt.bufPrint(bytes[1..9], "{d:0>8}", .{i});
                const key = Datum.json(.{ .integer = @intCast(iterations - i) });
                try top.addBatch(scratch.allocator(), .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_json = Reader.text, .count = 1, .width = 1 } }, &.{&.{key}}, &.{i});
                try std.testing.expect(top.external == null);
                try std.testing.expectEqual(@as(usize, 1), top.json_pool.?.bodies.count());
                if (body_bytes <= 1024) {
                    const value = try top.nativeStore(0).?.cell(scratch.allocator(), 0, 0);
                    try std.testing.expectEqualStrings(bytes[1 .. body_bytes - 1], value.value.string);
                } else {
                    const text = (try top.nativeStore(0).?.encodedJson(0, 0)).?;
                    try std.testing.expectEqualStrings(bytes[0..body_bytes], text);
                }
                try std.testing.expect(top.json_pool.?.cache_budget.live < 8 * 1024);
            }
            try std.testing.expect(budget.peak < budget.limit);
        }
    };
    try Harness.run(std.testing.allocator, 256, 1024);
    try Harness.run(std.testing.allocator, 16, 32768);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ @as(usize, 8), @as(usize, 1024) });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ @as(usize, 4), @as(usize, 32768) });
}

test "SQL native hash join shares producer body preparation across physical pages" {
    const spill = @import("spill.zig");
    const Frozen = @import("execution_batch.zig").FrozenJson;
    const Reader = struct {
        value: Frozen,
        fn check(_: *anyopaque) !void {}
        fn frozen(raw: *anyopaque, _: usize, column: usize) anyerror!?Frozen {
            return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).value else null;
        }
        fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 0) return error.UnexpectedMaterialization;
            return Datum.json(.{ .integer = @intCast(row) });
        }
    };
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    const sa = source.allocator();
    const body = try sa.alloc(u8, 65536);
    @memset(body, 'x');
    var object: std.json.ObjectMap = .empty;
    try object.put(sa, "body", .{ .string = body });
    const wire = try spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
    const block = try spill.decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
    defer block.deinit();
    var view: Reader = .{ .value = (try block.frozenJson(0, 0, false)).? };
    var token: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
    defer manager.deinit();
    const join = try HashJoin.create(a, .{ .bytes = 8192, .spill = &manager });
    defer join.deinit();
    try join.startSpill();
    const key = Datum.json(.{ .integer = 1 });
    const keys: [64][]const Datum = @splat(&.{key});
    try join.addBatch(sa, .{ .reader = .{ .ptr = &view, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 64, .width = 2 } }, &keys);
    try std.testing.expectEqual(@as(usize, 2), join.disk_values.?.encoded_pages);
    try std.testing.expect(manager.read_bytes < 8192);
    std.debug.print("JOIN_BODY_PREPARATION frozen={d} encode_attempts={d} read_bytes={d} read_calls={d} written={d}\n", .{ view.value.bytes.len, join.disk_values.?.encoded_pages, manager.read_bytes, manager.read_calls, manager.written_bytes });
    var seen: usize = 0;
    var probe = try join.probe(&.{key});
    while (try probe.next()) |match| {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const batch = try match.valuesBatch(scratch.allocator());
        try std.testing.expectEqual(@as(i64, @intCast(63 - seen)), (try batch.cell(scratch.allocator(), 0, 1)).value.integer);
        try std.testing.expectEqualSlices(u8, view.value.bytes, (try batch.frozenJson(0, 0)).?.bytes);
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, 64), seen);
}

test "SQL finalized aggregate streams bypass hash retention and fence ownership and quotas" {
    const a = std.testing.allocator;
    const Source = struct {
        count: usize,
        position: usize = 0,
        closes: usize = 0,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
        fn next(raw: *anyopaque, out: Allocator, maximum: usize, _: bool) !?GroupBatch {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.position == self.count) return null;
            const count = @min(maximum, self.count - self.position);
            const keys = try out.alloc([]const Datum, count);
            const values = try out.alloc([]const Datum, count);
            const ordinals = try out.alloc(u64, count);
            for (keys, values, ordinals, self.position..) |*key, *value, *ordinal, row| {
                key.* = try out.dupe(Datum, &.{Datum.json(.{ .integer = @intCast(row) })});
                value.* = try out.dupe(Datum, &.{Datum.json(.{ .integer = @intCast(row + 1) })});
                ordinal.* = row;
            }
            self.position += count;
            return .{ .keys = .{ .rows = keys }, .aggregates = .{ .rows = values }, .ordinals = ordinals };
        }
    };
    var source: Source = .{ .count = 10000 };
    {
        const group = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .groups = 10000, .bytes = 16 * 1024 });
        defer group.deinit();
        try group.adoptFinalized(.{ .ptr = &source, .count = source.count, .next = Source.next, .close = Source.close }, 1000);
        try std.testing.expectEqual(@as(usize, 10000), group.groupCount());
        var count: usize = 0;
        while (true) {
            var page = std.heap.ArenaAllocator.init(a);
            defer page.deinit();
            const batch = (try group.nextResultBatch(page.allocator(), 17, false)) orelse break;
            for (batch.ordinals, count..) |ordinal, row| {
                try std.testing.expectEqual(row, ordinal);
                try std.testing.expectEqual(@as(i64, @intCast(row + 1)), (try batch.aggregates.cell(page.allocator(), row - count, 0)).value.integer);
            }
            count += batch.ordinals.len;
        }
        try std.testing.expectEqual(source.count, count);
        try std.testing.expectEqual(@as(usize, 0), group.groups.items.len);
        try std.testing.expectEqual(@as(usize, 0), group.heads.count());
        try std.testing.expectEqual(@as(u64, 0), group.hash_probes);
    }
    try std.testing.expectEqual(@as(usize, 1), source.closes);
    source = .{ .count = 10 };
    {
        const group = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .groups = 10, .bytes = 16 * 1024 });
        defer group.deinit();
        try group.ensureGlobalGroup();
        try std.testing.expectError(error.SqlProgramLimitExceeded, group.adoptFinalized(.{ .ptr = &source, .count = 11, .next = Source.next, .close = Source.close }, 1));
        try std.testing.expectEqual(@as(usize, 0), source.closes);
        try group.adoptFinalized(.{ .ptr = &source, .count = 10, .next = Source.next, .close = Source.close }, 1);
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        _ = try group.nextResultBatch(page.allocator(), 2, false);
        try std.testing.expectError(error.SqlProgramLimitExceeded, group.nextResultBatch(page.allocator(), 2, false));
    }
    try std.testing.expectEqual(@as(usize, 1), source.closes);
}
