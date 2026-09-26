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
pub const Row = struct { values: []const Datum, keys: []const Datum, ordinal: u64 };

pub fn compareRows(left: Row, right: Row, orders: []const Order) !std.math.Order {
    if (left.keys.len != orders.len or right.keys.len != orders.len) return error.InvalidSqlBackendResponse;
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
        return .{ .values = values, .keys = keys, .ordinal = row.ordinal };
    }

    fn init(alloc: Allocator, row: Row) !OwnedRow {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const cloned = try cloneInto(&arena, row);
        return .{ .arena = arena, .row = cloned };
    }

    fn deinit(self: *OwnedRow) void {
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
    // Replacement exchanges a candidate with the displaced root. Retaining
    // that root's arena avoids one allocator round-trip per competitive row.
    scratch: ?std.heap.ArenaAllocator = null,

    pub fn init(alloc: Allocator, k: usize, orders: []const Order, max_bytes: usize) !TopK {
        const bytes = std.math.add(usize, std.math.mul(usize, k, @sizeOf(OwnedRow)) catch return error.SqlProgramLimitExceeded, std.math.mul(usize, orders.len, @sizeOf(Order)) catch return error.SqlProgramLimitExceeded) catch return error.SqlProgramLimitExceeded;
        if (bytes > max_bytes or orders.len > 256) return error.SqlProgramLimitExceeded;
        const entries = try alloc.alloc(OwnedRow, k);
        errdefer alloc.free(entries);
        const owned_orders = try alloc.dupe(Order, orders);
        return .{ .alloc = alloc, .entries = entries, .orders = owned_orders, .max_bytes = max_bytes, .retained_bytes = bytes };
    }

    pub fn deinit(self: *TopK) void {
        for (self.entries[0..self.count]) |*entry| entry.deinit();
        if (self.scratch) |*scratch| scratch.deinit();
        self.alloc.free(self.entries);
        self.alloc.free(self.orders);
        self.* = undefined;
    }

    fn compare(self: *TopK, a: Row, b: Row) !std.math.Order {
        self.comparisons += 1;
        return compareRows(a, b, self.orders);
    }

    pub fn add(self: *TopK, row: Row) !void {
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

    /// Returns rows in requested order, with original ordinal as a stable tie
    /// breaker. Returned cell data remains owned by this operator until deinit.
    pub fn finish(self: *TopK, alloc: Allocator) ![]const Row {
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
        if (self.kind == .count) return Datum.json(.{ .integer = @intCast(self.count) });
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
pub const GroupResult = struct { keys: []const Datum, aggregates: []const Datum, ordinal: u64 };

/// Build-side ownership for a streaming hash join. Probe rows remain borrowed
/// from one native page; only the build relation occupies retained memory.
pub const HashJoin = struct {
    pub const Limits = struct { rows: usize = 100000, bytes: usize = 8 * 1024 * 1024 };
    const Entry = struct { row: OwnedRow, next: ?usize, matched: bool = false };
    pub const Match = struct { index: usize, values: []const Datum };
    pub const Probe = struct {
        owner: *HashJoin,
        keys: []const Datum,
        cursor: ?usize,

        pub fn next(self: *Probe) !?Match {
            while (self.cursor) |index| {
                const entry = &self.owner.entries.items[index];
                self.cursor = entry.next;
                self.owner.probes += 1;
                var equal = true;
                for (entry.row.row.keys, self.keys) |left, right| if ((try scalar.compare(left.value, right.value)) != .eq) {
                    equal = false;
                    break;
                };
                if (equal) return .{ .index = index, .values = entry.row.row.values };
            }
            return null;
        }
    };

    backing: Allocator,
    budget: MemoryBudget,
    limits: Limits,
    entries: std.ArrayList(Entry) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    key_count: ?usize = null,
    probes: u64 = 0,
    sealed: bool = false,

    pub fn create(alloc: Allocator, limits: Limits) !*HashJoin {
        if (limits.bytes < @sizeOf(HashJoin)) return error.SqlProgramLimitExceeded;
        const self = try alloc.create(HashJoin);
        self.* = .{ .backing = alloc, .budget = .{ .backing = alloc, .limit = limits.bytes - @sizeOf(HashJoin) }, .limits = limits };
        return self;
    }
    pub fn deinit(self: *HashJoin) void {
        const alloc = self.budget.allocator();
        for (self.entries.items) |*entry| entry.row.deinit();
        self.entries.deinit(alloc);
        self.heads.deinit(alloc);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
    fn hash(keys: []const Datum) !?u64 {
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
        self.addInner(values, keys) catch |err| return if (err == error.OutOfMemory and self.budget.exhausted) error.SqlProgramLimitExceeded else err;
    }
    fn addInner(self: *HashJoin, values: []const Datum, keys: []const Datum) !void {
        if (self.sealed or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        if (self.entries.items.len >= self.limits.rows) return error.SqlProgramLimitExceeded;
        self.key_count = keys.len;
        const key_hash = try hash(keys);
        const alloc = self.budget.allocator();
        try self.entries.ensureUnusedCapacity(alloc, 1);
        if (key_hash != null) try self.heads.ensureUnusedCapacity(alloc, 1);
        const index = self.entries.items.len;
        const owned = try OwnedRow.init(alloc, .{ .values = values, .keys = keys, .ordinal = index });
        self.entries.appendAssumeCapacity(.{ .row = owned, .next = if (key_hash) |hashed| self.heads.get(hashed) else null });
        if (key_hash) |hashed| self.heads.putAssumeCapacity(hashed, index);
    }
    pub fn probe(self: *HashJoin, keys: []const Datum) !Probe {
        self.sealed = true;
        if (self.key_count != null and self.key_count.? != keys.len) return error.InvalidSqlBackendResponse;
        return .{ .owner = self, .keys = keys, .cursor = if (try hash(keys)) |hashed| self.heads.get(hashed) else null };
    }
    /// Mark only after the complete ON residual accepts this candidate.
    pub fn markMatched(self: *HashJoin, index: usize) void {
        self.entries.items[index].matched = true;
    }
    pub fn unmatched(self: *HashJoin, next: *usize) ?Match {
        while (next.* < self.entries.items.len) {
            const index = next.*;
            next.* += 1;
            const entry = self.entries.items[index];
            if (!entry.matched) return .{ .index = index, .values = entry.row.row.values };
        }
        return null;
    }
};

/// Bounded streaming hash aggregation. Owns only distinct keys and aggregate
/// states, never the scanned rows. A stable heap owner keeps quota allocator
/// pointers valid through map growth and result handoff.
pub const Grouped = struct {
    pub const Limits = struct { groups: usize = 10000, bytes: usize = 8 * 1024 * 1024 };
    const Group = struct { keys: OwnedRow, states: []Aggregate, next: ?usize };
    backing: Allocator,
    budget: MemoryBudget,
    specs: []AggregateSpec,
    limits: Limits,
    groups: std.ArrayList(Group) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    rows_seen: u64 = 0,
    hash_probes: u64 = 0,
    failed: bool = false,
    finished: bool = false,
    key_count: ?usize = null,

    pub fn create(alloc: Allocator, specs: []const AggregateSpec, limits: Limits) !*Grouped {
        if (limits.bytes < @sizeOf(Grouped) or specs.len > 256) return error.SqlProgramLimitExceeded;
        const self = try alloc.create(Grouped);
        errdefer alloc.destroy(self);
        self.* = .{ .backing = alloc, .budget = .{ .backing = alloc, .limit = limits.bytes - @sizeOf(Grouped) }, .specs = undefined, .limits = limits };
        self.specs = self.budget.allocator().dupe(AggregateSpec, specs) catch |err| return if (self.budget.exhausted) error.SqlProgramLimitExceeded else err;
        return self;
    }

    pub fn deinit(self: *Grouped) void {
        const alloc = self.budget.allocator();
        for (self.groups.items) |*group| {
            group.keys.deinit();
            for (group.states) |*state| state.deinit();
            alloc.free(group.states);
        }
        self.groups.deinit(alloc);
        self.heads.deinit(alloc);
        alloc.free(self.specs);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }

    pub fn add(self: *Grouped, keys: []const Datum, inputs: []const Datum) !void {
        if (self.failed or self.finished) return error.InvalidSqlBackendResponse;
        self.update(keys, inputs) catch |err| {
            self.failed = true;
            return if (err == error.OutOfMemory and self.budget.exhausted) error.SqlProgramLimitExceeded else err;
        };
    }

    pub fn ensureGlobalGroup(self: *Grouped) !void {
        if (self.rows_seen != 0 or self.groups.items.len != 0) return;
        self.key_count = 0;
        var hasher = std.hash.Wyhash.init(0);
        _ = try self.appendGroup(&.{}, hasher.final());
    }

    pub fn groupCount(self: *const Grouped) usize {
        return self.groups.items.len;
    }

    pub fn resultAt(self: *Grouped, alloc: Allocator, index: usize) !GroupResult {
        if (self.failed or index >= self.groups.items.len) return error.InvalidSqlBackendResponse;
        self.finished = true;
        const group = &self.groups.items[index];
        const values = try alloc.alloc(Datum, group.states.len);
        errdefer alloc.free(values);
        for (group.states, values) |*state, *value| value.* = try state.finish();
        return .{ .keys = group.keys.row.values, .aggregates = values, .ordinal = index };
    }

    fn update(self: *Grouped, keys: []const Datum, inputs: []const Datum) !void {
        if (inputs.len != self.specs.len or keys.len > 256 or (self.key_count != null and self.key_count.? != keys.len)) return error.InvalidSqlBackendResponse;
        self.key_count = keys.len;
        var hasher = std.hash.Wyhash.init(0);
        for (keys) |key| {
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(key.sql_null);
            std.mem.writeInt(u64, bytes[1..9], if (key.sql_null) 0 else try scalar.semanticHash(key.value), .little);
            hasher.update(&bytes);
        }
        const hash = hasher.final();
        var slot = self.heads.get(hash);
        while (slot) |index| {
            self.hash_probes += 1;
            const group = &self.groups.items[index];
            var equal = true;
            for (group.keys.row.values, keys) |stored, key| {
                if (stored.sql_null != key.sql_null or (!key.sql_null and (try scalar.compare(stored.value, key.value)) != .eq)) {
                    equal = false;
                    break;
                }
            }
            if (equal) break;
            slot = group.next;
        }
        if (slot == null) slot = try self.appendGroup(keys, hash);
        for (self.groups.items[slot.?].states, inputs) |*state, value| try state.update(value);
        self.rows_seen = std.math.add(u64, self.rows_seen, 1) catch return error.SqlNumericOutOfRange;
    }

    fn appendGroup(self: *Grouped, keys: []const Datum, hash: u64) !usize {
        if (self.groups.items.len >= self.limits.groups) return error.SqlProgramLimitExceeded;
        const alloc = self.budget.allocator();
        try self.groups.ensureUnusedCapacity(alloc, 1);
        try self.heads.ensureUnusedCapacity(alloc, 1);
        var owned = try OwnedRow.init(alloc, .{ .values = keys, .keys = &.{}, .ordinal = self.groups.items.len });
        errdefer owned.deinit();
        const states = try alloc.alloc(Aggregate, self.specs.len);
        errdefer alloc.free(states);
        var initialized: usize = 0;
        errdefer for (states[0..initialized]) |*state| state.deinit();
        for (states, self.specs) |*state, spec| {
            state.* = try Aggregate.init(alloc, spec.kind, spec.input_type);
            state.distinct = spec.distinct;
            initialized += 1;
        }
        const index = self.groups.items.len;
        self.groups.appendAssumeCapacity(.{ .keys = owned, .states = states, .next = self.heads.get(hash) });
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
        for (self.groups.items, result, 0..) |*group, *output, i| {
            const values = try alloc.alloc(Datum, group.states.len);
            output.* = .{ .keys = group.keys.row.values, .aggregates = values, .ordinal = i };
            initialized += 1;
            for (group.states, values) |*state, *value| value.* = try state.finish();
        }
        return result;
    }
};

pub fn cloneDatum(alloc: Allocator, value: Datum) !Datum {
    return .{ .value = try cloneJson(alloc, value.value, 0), .sql_null = value.sql_null };
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
fn datumBytes(value: Datum) !usize {
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
        join.markMatched(match.index);
        matches += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), matches);
    var null_probe = try join.probe(&.{.{}});
    try std.testing.expect((try null_probe.next()) == null);
    var cursor: usize = 0;
    try std.testing.expectEqualStrings("null", join.unmatched(&cursor).?.values[0].value.string);
    try std.testing.expect(join.unmatched(&cursor) == null);
}
