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

//! Sorted partial aggregation: spill mergeable states, then reduce one key at
//! a time. Raw updates retain arrival ordinals within each key's merge stream.
const std = @import("std");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
const Datum = scalar.Datum;
const Allocator = std.mem.Allocator;
pub const Grouped = struct {
    a: Allocator,
    sort: spill.Sort,
    specs: []const operators.AggregateSpec,
    state_arena: std.heap.ArenaAllocator,
    read_arena: std.heap.ArenaAllocator,
    pending: ?operators.Row = null,
    output_count: usize = 0,
    pub fn init(a: Allocator, manager: *spill.Manager, specs: []const operators.AggregateSpec, key_count: usize, bytes: usize) !Grouped {
        const orders = try a.alloc(operators.Order, key_count);
        @memset(orders, .{});
        return .{ .a = a, .sort = spill.Sort.init(a, manager, orders, bytes / 4), .specs = specs, .state_arena = std.heap.ArenaAllocator.init(a), .read_arena = std.heap.ArenaAllocator.init(a) };
    }
    pub fn deinit(self: *Grouped) void {
        self.a.free(self.sort.orders);
        self.sort.deinit();
        self.state_arena.deinit();
        self.read_arena.deinit();
    }
    pub fn partial(self: *Grouped, keys: []const Datum, states: []const operators.Aggregate, ordinal: u64) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const values = try a.alloc(Datum, 1 + states.len * 7);
        values[0] = Datum.json(.{ .bool = true });
        for (states, 0..) |state, i| {
            const cells = values[1 + i * 7 ..][0..7];
            cells[0] = Datum.json(.{ .integer = if (state.distinct) 0 else @intCast(state.count) });
            cells[1] = Datum.json(.{ .number_string = try std.fmt.allocPrint(a, "{d}", .{state.integer_sum}) });
            cells[2] = Datum.json(.{ .float = state.number_sum });
            cells[3] = Datum.json(.{ .float = state.compensation });
            cells[4] = Datum.json(.{ .float = state.mean });
            cells[5] = Datum.json(.{ .bool = state.boolean });
            cells[6] = if (state.selected) |selected| selected.row.values[0] else .{};
        }
        try self.sort.add(.{ .keys = keys, .values = values, .ordinal = ordinal });
        for (states, 0..) |state, slot| if (state.distinct) {
            for (state.distinct_values.items) |entry| {
                try self.sort.add(.{ .keys = keys, .values = &.{ Datum.json(.{ .integer = @intCast(slot) }), entry.row.row.values[0] }, .ordinal = ordinal });
            }
            if (state.kind == .pattern_set and state.patterns.?.has_null) try self.sort.add(.{ .keys = keys, .values = &.{ Datum.json(.{ .integer = @intCast(slot) }), .{} }, .ordinal = ordinal });
        };
    }
    pub fn add(self: *Grouped, keys: []const Datum, inputs: []const Datum, ordinal: u64) !void {
        const values = try self.a.alloc(Datum, inputs.len + 1);
        defer self.a.free(values);
        values[0] = Datum.json(.{ .bool = false });
        @memcpy(values[1..], inputs);
        try self.sort.add(.{ .keys = keys, .values = values, .ordinal = ordinal });
    }
    fn same(left: []const Datum, right: []const Datum) !bool {
        if (left.len != right.len) return error.InvalidSqlSpill;
        for (left, right) |a, b| if (a.sql_null != b.sql_null or (!a.sql_null and (try scalar.compare(a.value, b.value)) != .eq)) return false;
        return true;
    }
    pub fn next(self: *Grouped, out: Allocator) !?operators.GroupResult {
        _ = self.state_arena.reset(.free_all);
        const a = self.state_arena.allocator();
        var row = self.pending orelse (try self.sort.next(self.read_arena.allocator())) orelse return null;
        self.pending = null;
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.keys, keys) |key, *copy| copy.* = try operators.cloneDatum(a, key);
        var ordinal = row.ordinal;
        const states = try a.alloc(operators.Aggregate, self.specs.len);
        var initialized: usize = 0;
        defer for (states[0..initialized]) |*state| state.deinit();
        for (states, self.specs) |*state, spec| {
            state.* = try operators.Aggregate.init(self.a, spec.kind, spec.input_type);
            initialized += 1;
        }
        const pattern_sets = try a.alloc(?*@import("pattern_spill.zig").Set, states.len);
        for (self.specs, pattern_sets) |spec, *set| set.* = if (spec.kind == .pattern_set) try @import("pattern_spill.zig").Set.create(self.sort.manager) else null;
        var distinct = spill.Sort.init(self.a, self.sort.manager, &.{ .{}, .{} }, self.sort.memory_bytes);
        defer distinct.deinit();
        var distinct_ordinal: u64 = 0;
        while (true) {
            ordinal = @min(ordinal, row.ordinal);
            if (row.values.len == 0) return error.InvalidSqlSpill;
            if (row.values[0].value == .integer) {
                if (row.values.len != 2) return error.InvalidSqlSpill;
                const slot = std.math.cast(usize, row.values[0].value.integer) orelse return error.InvalidSqlSpill;
                if (slot >= states.len) return error.InvalidSqlSpill;
                try distinct.add(.{ .keys = &.{ row.values[0], row.values[1] }, .values = &.{row.values[1]}, .ordinal = distinct_ordinal });
                distinct_ordinal += 1;
            } else if (row.values[0].value != .bool) return error.InvalidSqlSpill else if (row.values[0].value.bool) {
                if (row.values.len != 1 + states.len * 7) return error.InvalidSqlSpill;
                for (states, 0..) |*state, i| try merge(state, row.values[1 + i * 7 ..][0..7]);
            } else {
                if (row.values.len != states.len + 1) return error.InvalidSqlSpill;
                for (states, row.values[1..], self.specs, 0..) |*state, input, spec, slot| {
                    if (spec.distinct) {
                        if (!input.sql_null or spec.kind == .pattern_set) {
                            try distinct.add(.{ .keys = &.{ Datum.json(.{ .integer = @intCast(slot) }), input }, .values = &.{input}, .ordinal = distinct_ordinal });
                            distinct_ordinal += 1;
                        }
                    } else try state.update(input);
                }
            }
            _ = self.read_arena.reset(.free_all);
            row = (try self.sort.next(self.read_arena.allocator())) orelse break;
            if (!try same(keys, row.keys)) {
                self.pending = row;
                break;
            }
        }
        if (distinct_ordinal != 0) {
            var previous = std.heap.ArenaAllocator.init(self.a);
            defer previous.deinit();
            var current = std.heap.ArenaAllocator.init(self.a);
            defer current.deinit();
            var last: ?[]const Datum = null;
            while (true) {
                _ = current.reset(.free_all);
                const record = (try distinct.next(current.allocator())) orelse break;
                if (last) |prior| if (try same(prior, record.keys)) continue;
                const slot = std.math.cast(usize, record.keys[0].value.integer) orelse return error.InvalidSqlSpill;
                if (slot >= states.len) return error.InvalidSqlSpill;
                if (pattern_sets[slot]) |set| try set.append(record.values[0]) else try states[slot].update(record.values[0]);
                _ = previous.reset(.free_all);
                const copy = try previous.allocator().alloc(Datum, 2);
                for (record.keys, copy) |v, *cell| cell.* = try operators.cloneDatum(previous.allocator(), v);
                last = copy;
            }
        }
        const output_keys = try out.alloc(Datum, keys.len);
        for (keys, output_keys) |key, *copy| copy.* = try operators.cloneDatum(out, key);
        const aggregates = try out.alloc(Datum, states.len);
        for (states, aggregates, pattern_sets) |*state, *copy, set| copy.* = if (set) |patterns| .{ .patterns = &patterns.interface, .sql_null = false } else try operators.cloneDatum(out, try state.finish());
        self.output_count += 1;
        return .{ .keys = output_keys, .aggregates = aggregates, .ordinal = ordinal };
    }
    fn merge(state: *operators.Aggregate, cells: []const Datum) !void {
        const count = std.math.cast(u64, cells[0].value.integer) orelse return error.InvalidSqlSpill;
        if (count == 0) return;
        const total = std.math.add(u64, state.count, count) catch return error.SqlNumericOutOfRange;
        if (total > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        switch (state.kind) {
            .count => {},
            .sum => if (state.input_type == .integer) {
                const sum = try std.fmt.parseInt(i128, cells[1].value.number_string, 10);
                state.integer_sum = std.math.add(i128, state.integer_sum, sum) catch return error.SqlNumericOutOfRange;
            } else {
                const adjusted = cells[2].value.float - state.compensation;
                const sum = state.number_sum + adjusted;
                if (!std.math.isFinite(sum)) return error.SqlNumericOutOfRange;
                state.compensation = (sum - state.number_sum) - adjusted;
                state.number_sum = sum;
            },
            .avg => {
                const incoming = cells[4].value.float;
                const fraction = @as(f64, @floatFromInt(count)) / @as(f64, @floatFromInt(total));
                const delta = incoming - state.mean;
                state.mean = if (state.count == 0) incoming else if (std.math.isFinite(delta)) state.mean + delta * fraction else state.mean * (1 - fraction) + incoming * fraction;
                if (!std.math.isFinite(state.mean)) return error.SqlNumericOutOfRange;
            },
            .bool_and => state.boolean = state.boolean and cells[5].value.bool,
            .bool_or => state.boolean = state.boolean or cells[5].value.bool,
            .min, .max => {
                try state.update(cells[6]);
            },
            .pattern_set => return error.UnsupportedSqlExecution,
        }
        state.count = total;
    }
};
