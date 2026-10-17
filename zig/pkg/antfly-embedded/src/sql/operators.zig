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

const OwnedRow = struct {
    arena: std.heap.ArenaAllocator,
    row: Row,

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
        self.arena.deinit();
    }

    fn allocatedBytes(self: OwnedRow) usize {
        return arenaBytes(&self.arena);
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
            try sort.add(entry.row);
            self.retained_bytes -= entry.allocatedBytes();
            entry.deinit();
            self.released += 1;
        }
        self.released = 0;
        if (self.scratch) |*arena| arena.deinit();
        self.scratch = null;
        self.alloc.free(self.entries);
        self.entries = &.{};
        self.count = 0;
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
                const row = (try sort.next(arena.allocator())) orelse break;
                if (index < offset) continue;
                if (emitted == limit) {
                    if (implicit) return error.SqlResultTooLarge;
                    break;
                }
                try sink.append(sink.ptr, row.values);
                emitted += 1;
            }
        } else {
            const rows = try self.finish(arena.allocator());
            const begin = @min(offset, rows.len);
            if (implicit and rows.len - begin > limit) return error.SqlResultTooLarge;
            for (rows[begin..][0..@min(limit, rows.len - begin)]) |row| try sink.append(sink.ptr, row.values);
        }
    }
    pub fn finishPage(self: *TopK, alloc: Allocator, offset: usize, limit: usize) ![]const Row {
        if (self.external) |sort| {
            self.finished = true;
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            for (0..@min(offset, self.capacity)) |_| {
                _ = arena.reset(.free_all);
                if ((try sort.next(arena.allocator())) == null) return &.{};
            }
            var rows: std.ArrayList(Row) = .empty;
            for (0..@min(limit, self.capacity -| offset)) |_| {
                const row = try sort.next(alloc) orelse break;
                try rows.append(alloc, row);
            }
            return rows.toOwnedSlice(alloc);
        }
        const rows = try self.finish(alloc);
        const begin = @min(offset, rows.len);
        for (0..begin) |index| self.releaseFinishedRow(index);
        return rows[begin..][0..@min(limit, rows.len - begin)];
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
        self.addInMemory(row) catch |err| {
            if (self.spill_manager == null or (err != error.SqlProgramLimitExceeded and err != error.OutOfMemory)) return err;
            try self.startSpill();
            try self.external.?.add(row);
        };
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
        const bytes = owned.allocatedBytes();
        if (bytes > self.max_bytes -| self.retained_bytes) return error.SqlProgramLimitExceeded;
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
        self.released += 1;
    }

    /// Returns rows in requested order, with original ordinal as a stable tie
    /// breaker. Returned cell data remains owned by this operator until deinit.
    pub fn finish(self: *TopK, alloc: Allocator) ![]const Row {
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
    pub const Limits = struct { rows: usize = 100000, bytes: usize = 8 * 1024 * 1024, spill: ?*@import("spill.zig").Manager = null };
    const Entry = struct { next: ?usize, width: usize, matched: bool = false };
    pub const Match = struct {
        index: usize,
        values: []const Datum = &.{},
        keys: []const Datum = &.{},
        transient: bool = false,
        owner: ?*HashJoin = null,

        /// Reconstruct row-boundary values from stable typed columns. Disk
        /// candidates are copied before the next probe reuses decode storage.
        pub fn materializeKeys(self: Match, alloc: Allocator) ![]const Datum {
            return if (self.owner) |owner| owner.key_columns.row(alloc, self.index) else self.keys;
        }
        pub fn materializeValues(self: Match, alloc: Allocator) ![]const Datum {
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
                _ = self.owner.disk_arena.reset(.free_all);
                const decoded = try file.read(self.owner.disk_arena.allocator(), index);
                self.cursor = if (decoded.next == @import("spill.zig").none) null else @intCast(decoded.next);
                self.owner.probes += 1;
                if (decoded.row.keys.len != self.keys.len) return error.InvalidSqlSpill;
                var equal = true;
                for (decoded.row.keys, self.keys) |left, right| if (left.sql_null or right.sql_null or (try scalar.compareDatums(left, right)) != .eq) {
                    equal = false;
                    break;
                };
                if (equal) return .{ .index = index, .values = decoded.row.values, .keys = decoded.row.keys, .transient = true };
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
    disk_arena: std.heap.ArenaAllocator = undefined,
    row_count: usize = 0,
    fn startSpill(self: *HashJoin) !void {
        const manager = self.limits.spill orelse return error.SqlProgramLimitExceeded;
        const alloc = self.budget.allocator();
        const slots = std.math.floorPowerOfTwo(usize, @max(@as(usize, 16), self.limits.bytes / 64));
        const heads = try alloc.alloc(u64, slots);
        errdefer alloc.free(heads);
        @memset(heads, @import("spill.zig").none);
        var file = try manager.create();
        errdefer file.close();
        for (self.entries.items, 0..) |_, ordinal| {
            var arena = std.heap.ArenaAllocator.init(self.backing);
            defer arena.deinit();
            const values = try self.value_columns.rowWidth(arena.allocator(), ordinal, self.entries.items[ordinal].width);
            const keys = try self.key_columns.row(arena.allocator(), ordinal);
            const hashed = try keyHash(keys);
            const bucket = if (hashed) |value| value & (heads.len - 1) else 0;
            const offset = try file.append(.{ .values = values, .keys = keys, .ordinal = ordinal }, if (hashed != null) heads[bucket] else @import("spill.zig").none);
            if (hashed != null) heads[bucket] = offset;
        }
        self.value_columns.deinit();
        self.key_columns.deinit();
        self.value_columns = .init(alloc);
        self.key_columns = .init(alloc);
        self.entries.clearAndFree(alloc);
        self.heads.clearAndFree(alloc);
        self.disk_heads = heads;
        self.disk = file;
    }
    fn appendDisk(self: *HashJoin, values: []const Datum, keys: []const Datum) !void {
        const hashed = try keyHash(keys);
        const bucket = if (hashed) |value| value & (self.disk_heads.len - 1) else 0;
        const offset = try self.disk.?.append(.{ .values = values, .keys = keys, .ordinal = self.row_count }, if (hashed != null) self.disk_heads[bucket] else @import("spill.zig").none);
        if (hashed != null) self.disk_heads[bucket] = offset;
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
        if (self.disk != null) return false;
        if (self.sealed or batch.len() != keys.len) return error.InvalidSqlBackendResponse;
        if (keys.len > self.limits.rows -| self.row_count) return error.SqlProgramLimitExceeded;
        var needed: usize = 4096 +| batch.len() *| (128 +| batch.width() *| 64);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (0..batch.width()) |column| for (0..batch.len()) |index| {
            _ = scratch.reset(.retain_capacity);
            needed +|= (try datumBytes(try batch.cell(scratch.allocator(), index, column))) *| 4;
        };
        for (keys) |row| {
            if (row.len > 256 or (self.key_count != null and row.len != self.key_count.?)) return error.InvalidSqlBackendResponse;
            for (row) |value| needed +|= (try datumBytes(value)) *| 4;
        }
        needed +|= self.value_columns.dictionaryGrowthBytes(batch.len()) *| 2;
        needed +|= self.key_columns.dictionaryGrowthBytes(keys.len) *| 2;
        if (needed > self.budget.limit / 2 -| self.budget.live) return false;
        const hashes = try @import("batch_hash.zig").rows(a, keys, false);
        defer a.free(hashes);
        const retained = self.budget.allocator();
        try self.entries.ensureUnusedCapacity(retained, keys.len);
        try self.heads.ensureUnusedCapacity(retained, @intCast(keys.len));
        try self.value_columns.appendBatch(batch);
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
    /// Gather each build row into reusable scratch and admit it directly into
    /// retained typed state. Native payloads are copied exactly once.
    pub fn addBatch(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum) !void {
        if (batch.len() != keys.len) return error.InvalidSqlBackendResponse;
        if (try self.admitBatch(a, batch, keys)) return;
        const values = try a.alloc(Datum, batch.width());
        defer a.free(values);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (keys, 0..) |key, index| {
            _ = scratch.reset(.retain_capacity);
            for (values, 0..) |*value, column| value.* = try batch.cell(scratch.allocator(), index, column);
            try self.add(values, key);
        }
    }
    /// Retain a bounded prefix without creating an intermediate disk chain.
    /// The caller transfers this prefix once to its Grace partition sink.
    pub fn addBatchUntilFull(self: *HashJoin, a: Allocator, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum) !usize {
        if (batch.len() != keys.len or self.disk != null) return error.InvalidSqlBackendResponse;
        if (try self.admitBatch(a, batch, keys)) return keys.len;
        const values = try a.alloc(Datum, batch.width());
        defer a.free(values);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (keys, 0..) |key, index| {
            _ = scratch.reset(.retain_capacity);
            for (values, 0..) |*value, column| value.* = try batch.cell(scratch.allocator(), index, column);
            const estimate = 4096 +| (try self.value_columns.appendBytes(values)) *| 4 +| (try self.key_columns.appendBytes(key)) *| 4;
            if (self.budget.live > self.budget.limit / 2 or estimate > self.budget.limit -| self.budget.live) return index;
            try self.add(values, key);
        }
        return keys.len;
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
    pub fn unmatched(self: *HashJoin, next: *usize) !?Match {
        if (self.disk) |*file| {
            while (next.* < file.size) {
                const offset = next.*;
                _ = self.disk_arena.reset(.free_all);
                const row = try file.read(self.disk_arena.allocator(), offset);
                next.* = @intCast(row.following);
                if (!row.matched) return .{ .index = offset, .values = row.row.values, .keys = row.row.keys, .transient = true };
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
            for (keys, physical, cells) |column, native, *cell| cell.* = if (native) |view| try view.cell(row_index) else try column.cell(a, row_index, 0);
            const hash = hashes[row_index] orelse return error.InvalidSqlBackendResponse;
            var previous = heads.get(hash);
            while (previous) |index| {
                const candidate = representatives.items[index];
                var equal_ = true;
                for (keys, physical, cells) |column, native, cell| {
                    const before = if (native) |view| try view.cell(candidate.row) else try column.cell(a, candidate.row, 0);
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
        const Batch = @import("execution_batch.zig").Batch;
        var scratch = std.heap.ArenaAllocator.init(self.backing);
        defer scratch.deinit();
        var begin: usize = 0;
        var length = @max(@as(usize, 1), count / 2);
        while (begin < count) {
            _ = scratch.reset(.free_all);
            const a = scratch.allocator();
            length = @min(length, count - begin);
            const selection = try a.alloc(usize, length);
            for (selection, begin..) |*row, i| row.* = i;
            const key_parts = try a.alloc(Batch, keys.len);
            const input_parts = try a.alloc(Batch, inputs.len);
            for (keys, key_parts) |*column, *part| part.* = try column.select(a, selection);
            for (inputs, input_parts) |*column, *part| part.* = try column.select(a, selection);
            const ids: ?InputIdentity = if (identity) |value| .{ .ordinals = value.ordinals[begin..][0..length], .hashes = value.hashes[begin..][0..length] } else null;
            if (try self.addEncodedWindow(key_parts, input_parts, length, ids)) {
                begin += length;
                length *|= 2;
            } else length = @max(@as(usize, 1), length / 2);
        }
    }
    /// Atomic admission probe for a native reducer page. False consumes no
    /// rows or retained groups, allowing an owning spill reader to switch modes.
    pub fn tryAddEncodedColumns(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, count: usize, identity: InputIdentity) !bool {
        if (count < 2 or keys.len == 0 or self.external != null) return false;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or spec.input_element == .numeric) return false;
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
                .counts, .integers, .booleans, .bytes => {},
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
            for (values.*, 0..) |*state, row| state.* = (try partial.decodeFixed(try column.cell(a, row, 0), spec)) orelse return false;
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
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or spec.input_element == .numeric) {
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
            for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or spec.input_element == .numeric) {
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
                for (keys, physical_keys, row_keys) |column, native, *cell| cell.* = if (native) |view| try view.cell(index) else try column.cell(a, index, 0);
                for (inputs, row_inputs) |column, *cell| cell.* = try column.cell(a, index, 0);
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
            for (keys, physical_keys, row_keys) |column, native, *cell| cell.* = if (native) |view| try view.cell(representative.row) else try column.cell(a, representative.row, 0);
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
                .counts, .integers, .booleans => {},
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
        for (0..array.cardinality(), elements) |array_cell_0, *out| {
            const element = array.flatCell(array_cell_0);
            if (element.array != null) return error.SqlTypeMismatch;
            out.* = try cloneDatum(alloc, element);
        }
        owned.* = .{ .element_type = array.element_type, .dimensions = dimensions, .elements = elements };
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
    if (value.array) |array| {
        if (array.stored) |stored| return @sizeOf(Datum) + @sizeOf(@import("array_value.zig").Value) + array.dimensions.len * @sizeOf(@import("array_value.zig").Dimension) + @sizeOf(@import("../common/sql_array_layout.zig").View) + stored.bytes.len;
        var bytes: usize = @sizeOf(Datum) + @sizeOf(@import("array_value.zig").Value) + array.dimensions.len * @sizeOf(@import("array_value.zig").Dimension);
        for (0..array.cardinality()) |array_cell_0| {
            const element = array.flatCell(array_cell_0);
            if (element.array != null) return error.SqlTypeMismatch;
            bytes +|= try datumBytes(element);
        }
        return bytes;
    }
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
    for ([_]bool{ false, true }) |batched| for ([_]bool{ false, true }) |wrapped| {
        var quota: MemoryBudget = .{ .backing = a, .limit = 1024 * 1024 };
        const qa = quota.allocator();
        const reservation = try qa.alloc(u8, 960 * 1024);
        defer qa.free(reservation);
        var locked: @import("parallel_scheduler.zig").LockedAllocator = .{ .backing = qa };
        const group = try Grouped.create(if (wrapped) locked.allocator() else qa, &.{.{ .kind = .count }}, .{ .bytes = 64 * 1024 });
        defer group.deinit();
        if (batched) try group.addEncodedColumns(&.{keys}, &.{keys}, 4096) else for (0..4096) |_| try group.add(&.{Datum.json(.{ .integer = 7 })}, &.{Datum.json(.{ .integer = 7 })});
        try std.testing.expectEqual(@as(u64, 4096), group.rows_seen);
    };
}
