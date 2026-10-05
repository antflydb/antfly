// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded physical SQL operators shared by local and distributed execution.
//! Inputs may borrow a scan page. Competitive top-K rows and aggregate extrema
//! take ownership before that page closes; discarded rows allocate nothing.
const std = @import("std");
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
        const comparison = try scalar.compare(a.value, b.value);
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
        const bytes = std.math.add(usize, std.math.mul(usize, k, @sizeOf(OwnedRow)) catch return error.SqlProgramLimitExceeded, std.math.mul(usize, orders.len, @sizeOf(Order)) catch return error.SqlProgramLimitExceeded) catch return error.SqlProgramLimitExceeded;
        if (bytes > max_bytes or orders.len > 256) return error.SqlProgramLimitExceeded;
        const entries = try alloc.alloc(OwnedRow, k);
        errdefer alloc.free(entries);
        const owned_orders = try alloc.dupe(Order, orders);
        return .{ .alloc = alloc, .entries = entries, .orders = owned_orders, .max_bytes = max_bytes, .retained_bytes = bytes, .capacity = k };
    }

    pub fn initWithSpill(alloc: Allocator, k: usize, orders: []const Order, max_bytes: usize, manager: ?*@import("spill.zig").Manager) !TopK {
        if (manager != null and k > (max_bytes / 4) / @sizeOf(OwnedRow)) {
            if (orders.len > 256) return error.SqlProgramLimitExceeded;
            var top: TopK = .{ .alloc = alloc, .entries = &.{}, .orders = try alloc.dupe(Order, orders), .max_bytes = max_bytes, .retained_bytes = 0, .capacity = k, .spill_manager = manager };
            errdefer alloc.free(top.orders);
            try top.startSpill();
            return top;
        }
        var top = try init(alloc, k, orders, if (manager != null) max_bytes - max_bytes / 4 else max_bytes);
        top.spill_manager = manager;
        return top;
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
        if (self.entries.len == 0) return;
        // Validate values before mutation, including when there is no prior
        // row to compare; malformed input cannot poison a partly sorted heap.
        for (row.keys) |key| if (!key.sql_null) {
            _ = try scalar.compare(key.value, key.value);
        };
        if (self.count == self.entries.len and (try self.compare(row, self.entries[0].row)) != .lt) return;
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
        try validate(kind, input_type);
        var result: Aggregate = .{ .alloc = alloc, .kind = kind, .input_type = input_type, .boolean = kind == .bool_and };
        if (kind == .pattern_set) {
            const state = try alloc.create(PatternState);
            state.* = .{ .values = .init(alloc) };
            result.patterns = state;
        }
        return result;
    }

    pub fn deinit(self: *Aggregate) void {
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
            const hash = try scalar.semanticHash(value.value);
            var slot = self.distinct_heads.get(hash);
            while (slot) |index| {
                const entry = self.distinct_values.items[index];
                if ((try scalar.compare(value.value, entry.row.row.values[0].value)) == .eq) return;
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
            .sum, .avg => {
                const number: f64 = switch (value.value) {
                    .integer => |v| @floatFromInt(v),
                    .float => |v| if (std.math.isFinite(v)) v else return error.SqlNumericOutOfRange,
                    else => return error.SqlTypeMismatch,
                };
                if (self.kind == .sum) {
                    if (self.input_type == .integer) {
                        if (value.value != .integer) return error.SqlTypeMismatch;
                        self.integer_sum = std.math.add(i128, self.integer_sum, value.value.integer) catch return error.SqlNumericOutOfRange;
                    } else {
                        const adjusted = number - self.compensation;
                        const total = self.number_sum + adjusted;
                        if (!std.math.isFinite(total)) return error.SqlNumericOutOfRange;
                        self.compensation = (total - self.number_sum) - adjusted;
                        self.number_sum = total;
                    }
                } else {
                    const n: f64 = @floatFromInt(self.count + 1);
                    const delta = number - self.mean;
                    self.mean = if (std.math.isFinite(delta)) self.mean + delta / n else self.mean * ((n - 1) / n) + number / n;
                    if (!std.math.isFinite(self.mean)) return error.SqlNumericOutOfRange;
                }
            },
            .bool_and, .bool_or => {
                if (value.value != .bool) return error.SqlTypeMismatch;
                self.boolean = if (self.kind == .bool_and) self.boolean and value.value.bool else self.boolean or value.value.bool;
            },
            .min, .max => {
                if (self.selected) |current| {
                    const order = try scalar.compare(value.value, current.row.values[0].value);
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

    pub fn finish(self: *const Aggregate) !Datum {
        if (self.kind == .count) return Datum.json(.{ .integer = std.math.cast(i64, self.count) orelse return error.SqlNumericOutOfRange });
        if (self.kind == .pattern_set) return Datum.json(.{ .array = self.patterns.?.values });
        if (self.count == 0) return .{};
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

pub const AggregateSpec = struct { kind: Aggregate.Kind, input_type: ?ast.ColumnType = null, distinct: bool = false };
pub const GroupBatch = struct {
    keys: @import("execution_batch.zig").Batch,
    aggregates: @import("execution_batch.zig").Batch,
    ordinals: []const u64,
};
pub const GroupResult = struct { keys: []const Datum, aggregates: []const Datum, ordinal: u64 };

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
                for (decoded.row.keys, self.keys) |left, right| if (left.sql_null or right.sql_null or (try scalar.compare(left.value, right.value)) != .eq) {
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
            std.mem.writeInt(u64, &bytes, try scalar.semanticHash(key.value), .little);
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
    hash_probes: u64 = 0,
    last_group: ?usize = null,
    failed: bool = false,
    finished: bool = false,
    key_count: ?usize = null,

    external: ?*@import("spill_grouped.zig").Grouped = null,
    result_cursor: usize = 0,
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
                const state = try reader.group.state_columns[column / 7].snapshot(alloc, index);
                return @import("aggregate_partial.zig").cell(alloc, state, column % 7);
            }
        };
        const reader = try a.create(Reader);
        reader.* = .{ .group = self, .begin = begin, .exact = exact };
        self.result_cursor += count;
        self.finished = true;
        return .{
            .keys = .{ .retained = .{ .store = &self.key_columns, .begin = begin, .count = count } },
            .aggregates = .{ .reader = .{ .ptr = reader, .read = Reader.cell, .count = count, .width = self.specs.len * (if (exact) @as(usize, 7) else 1) } },
            .ordinals = ordinals,
        };
    }
    pub fn nextResult(self: *Grouped, alloc: Allocator) !?GroupResult {
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
            var needed: usize = 4096 + self.specs.len * @sizeOf(Aggregate) * 2;
            for (keys) |key| needed +|= (try datumBytes(key)) *| 4;
            for (inputs) |input| needed +|= (try datumBytes(input)) *| 4;
            if (self.budget.live > self.budget.limit / 2 or needed > self.budget.limit -| self.budget.live) try self.startSpill();
        }
        if (self.external) |external| {
            try external.add(keys, inputs, self.rows_seen);
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
        var fast = self.external == null and needed <= self.budget.limit -| self.budget.live and self.budget.live <= self.budget.limit / 2;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set) {
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

    /// Global COUNT, integer SUM and boolean reductions do not need a hash
    /// probe per row. Validate the batch before changing state; unsupported
    /// kinds, DISTINCT, mixed values and disk groups retain ordered updates.
    pub fn canRetain(self: *const Grouped, keys: []const Datum, inputs: []const Datum) !bool {
        var needed: usize = 4096 + self.specs.len * @sizeOf(Aggregate) * 2;
        needed +|= (try self.key_columns.appendBytes(keys)) *| 4;
        for (inputs) |input| needed +|= (try datumBytes(input)) *| 4;
        return self.budget.live <= self.budget.limit / 2 and needed <= self.budget.limit -| self.budget.live;
    }
    pub fn addOrdered(self: *Grouped, keys: []const Datum, inputs: []const Datum, ordinal: u64) !void {
        const prior_groups = self.groups.items.len;
        try self.add(keys, inputs);
        const index = self.last_group orelse return error.InvalidSqlBackendResponse;
        self.groups.items[index].ordinal = if (index >= prior_groups) ordinal else @min(self.groups.items[index].ordinal, ordinal);
    }
    /// Merge exact local reducers, including spilled states, without narrowing
    /// partial sums. Worker-local ordinals occupy disjoint ranges; consumers
    /// requiring source-order numeric semantics never use this reduction.
    pub fn mergeExact(self: *Grouped, source: *Grouped, ordinal_base: u64) !void {
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
        const states = try self.backing.alloc(Aggregate, self.specs.len);
        defer self.backing.free(states);
        const empty = try self.backing.alloc(Datum, self.specs.len);
        defer self.backing.free(empty);
        @memset(empty, .{});
        for (source.groups.items, 0..) |group, index| {
            _ = arena.reset(.free_all);
            const keys = try source.key_columns.row(arena.allocator(), index);
            const ordinal = std.math.add(u64, ordinal_base, group.ordinal) catch return error.SqlNumericOutOfRange;
            for (source.state_columns, states) |*column, *state| state.* = try column.snapshot(arena.allocator(), index);
            self.key_count = keys.len;
            if (self.external == null and self.limits.spill != null and !try self.canRetain(keys, empty)) try self.startSpill();
            if (self.external) |external| {
                try external.partial(keys, states, ordinal);
            } else {
                const previous = self.groups.items.len;
                const slot = try self.resolveGroup(keys);
                self.groups.items[slot].ordinal = if (slot >= previous) ordinal else @min(self.groups.items[slot].ordinal, ordinal);
                for (self.state_columns, states) |*column, state| try column.mergeExact(slot, state);
            }
        }
        self.rows_seen = std.math.add(u64, self.rows_seen, source.rows_seen) catch return error.SqlNumericOutOfRange;
    }
    pub fn importPartial(self: *Grouped, keys: []const Datum, cells: []const Datum, ordinal: u64) !void {
        if (cells.len != self.specs.len * 7) return error.InvalidSqlSpill;
        const inputs = try self.backing.alloc(Datum, self.specs.len);
        defer self.backing.free(inputs);
        @memset(inputs, .{});
        self.key_count = keys.len;
        if (self.external == null and self.limits.spill != null and !try self.canRetain(keys, inputs)) try self.startSpill();
        if (self.external) |external| return external.appendPartial(keys, cells, ordinal);
        try self.addOrdered(keys, inputs, ordinal);
        const index = self.last_group.?;
        for (self.state_columns, 0..) |*column, slot| {
            var state = try column.snapshot(self.backing, index);
            try @import("spill_grouped.zig").Grouped.merge(&state, cells[slot * 7 ..][0..7]);
            column.set(index, state);
        }
    }
    pub fn exportPartial(self: *Grouped, external: *@import("spill_grouped.zig").Grouped) !void {
        var scratch = std.heap.ArenaAllocator.init(self.backing);
        defer scratch.deinit();
        for (self.groups.items, 0..) |group, index| {
            _ = scratch.reset(.free_all);
            const a = scratch.allocator();
            const states = try a.alloc(Aggregate, self.specs.len);
            for (self.state_columns, states) |column, *state| state.* = try column.snapshot(a, index);
            try external.partial(try self.key_columns.row(a, index), states, group.ordinal);
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

    fn resolveGroup(self: *Grouped, keys: []const Datum) !usize {
        var hasher = std.hash.Wyhash.init(0);
        for (keys) |key| {
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(key.sql_null);
            std.mem.writeInt(u64, bytes[1..9], if (key.sql_null) 0 else try scalar.semanticHash(key.value), .little);
            hasher.update(&bytes);
        }
        return self.resolveGroupHashed(keys, hasher.final());
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
        self.groups.appendAssumeCapacity(.{ .next = self.heads.get(hash), .ordinal = self.rows_seen });
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

pub fn cloneDatum(alloc: Allocator, value: Datum) !Datum {
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
pub fn datumBytes(value: Datum) !usize {
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
    try std.testing.expectEqual(@as(usize, 7), batch.aggregates.width());
    const sum = try batch.aggregates.cell(arena.allocator(), 0, 1);
    try std.testing.expectEqualStrings("27670116110564327421", sum.value.number_string);
    try merged.importPartial(try batch.keys.row(arena.allocator(), 0), try batch.aggregates.row(arena.allocator(), 0), batch.ordinals[0]);
    for (0..3) |_| try merged.add(&.{Datum.json(.{ .integer = 7 })}, &.{Datum.json(.{ .integer = -std.math.maxInt(i64) })});
    const result = (try merged.nextResultBatch(arena.allocator(), 64, false)).?;
    try std.testing.expectEqual(@as(i64, 0), (try result.aggregates.cell(a, 0, 0)).value.integer);
}
