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

//! Per-aggregate typed vectors indexed by group ID. Dynamic DISTINCT/JSON
//! state remains explicit; common aggregates carry no allocator/map/Datum tags.
const std = @import("std");
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;
pub const Column = struct {
    const Integer = struct { count: u64 = 0, sum: i128 = 0, selected: i64 = 0 };
    const Number = struct { count: u64 = 0, sum: f64 = 0, compensation: f64 = 0, mean: f64 = 0, selected: f64 = 0 };
    const Boolean = struct { count: u64 = 0, value: bool = false };
    const Bytes = struct { count: u64 = 0, value: ?[]u8 = null };
    const Values = union(enum) {
        counts: std.ArrayList(u64),
        integers: std.ArrayList(Integer),
        numbers: std.ArrayList(Number),
        exact: std.ArrayList(@import("numeric_aggregate.zig").Reducer),
        booleans: std.ArrayList(Boolean),
        bytes: std.ArrayList(Bytes),
        dynamic: std.ArrayList(operators.Aggregate),
    };
    spec: operators.AggregateSpec,
    values: Values,
    pub fn init(spec: operators.AggregateSpec) Column {
        return .{ .spec = spec, .values = if (spec.distinct or spec.kind == .pattern_set) .{ .dynamic = .empty } else if (spec.input_element == .numeric and (spec.kind == .sum or spec.kind == .avg)) .{ .exact = .empty } else switch (spec.kind) {
            .count => .{ .counts = .empty },
            .sum => if (spec.input_type == .integer) .{ .integers = .empty } else .{ .numbers = .empty },
            .avg => .{ .numbers = .empty },
            .bool_and, .bool_or => .{ .booleans = .empty },
            .min, .max => switch (spec.input_type orelse .json) {
                .integer => .{ .integers = .empty },
                .number => .{ .numbers = .empty },
                .boolean => .{ .booleans = .empty },
                .string => .{ .bytes = .empty },
                else => .{ .dynamic = .empty },
            },
            .pattern_set => unreachable,
        } };
    }
    pub fn clear(self: *Column, a: A) void {
        switch (self.values) {
            .bytes => |values| for (values.items) |value| if (value.value) |bytes| a.free(bytes),
            .dynamic => |values| for (values.items) |*state| state.deinit(),
            .exact => |values| for (values.items) |*state| state.deinit(),
            else => {},
        }
        switch (self.values) {
            inline else => |*values| values.clearAndFree(a),
        }
    }
    pub fn append(self: *Column, a: A) !void {
        try operators.Aggregate.validate(self.spec.kind, self.spec.input_type);
        switch (self.values) {
            .counts => |*values| try values.append(a, 0),
            .integers => |*values| try values.append(a, .{}),
            .numbers => |*values| try values.append(a, .{}),
            .exact => |*values| try values.append(a, .{ .alloc = a }),
            .booleans => |*values| try values.append(a, .{ .value = self.spec.kind == .bool_and }),
            .bytes => |*values| try values.append(a, .{}),
            .dynamic => |*values| {
                var state = try operators.Aggregate.initTyped(a, self.spec.kind, self.spec.input_type, self.spec.input_element);
                errdefer state.deinit();
                state.distinct = self.spec.distinct;
                try values.append(a, state);
            },
        }
    }
    pub fn snapshot(self: *const Column, a: A, index: usize) !operators.Aggregate {
        var state: operators.Aggregate = .{ .alloc = a, .kind = self.spec.kind, .input_type = self.spec.input_type, .input_element = self.spec.input_element, .boolean = self.spec.kind == .bool_and };
        var selected: ?Datum = null;
        switch (self.values) {
            .counts => |v| state.count = v.items[index],
            .exact => |v| {
                const reducer = try a.create(@import("numeric_aggregate.zig").Reducer);
                errdefer a.destroy(reducer);
                reducer.* = try v.items[index].clone(a);
                state.numeric = reducer;
                state.count = reducer.state.count;
            },
            .integers => |v| {
                state.count = v.items[index].count;
                state.integer_sum = v.items[index].sum;
                selected = Datum.json(.{ .integer = v.items[index].selected });
            },
            .numbers => |v| {
                state.count = v.items[index].count;
                state.number_sum = v.items[index].sum;
                state.compensation = v.items[index].compensation;
                state.mean = v.items[index].mean;
                selected = Datum.json(.{ .float = v.items[index].selected });
            },
            .booleans => |v| {
                state.count = v.items[index].count;
                state.boolean = v.items[index].value;
                selected = Datum.json(.{ .bool = v.items[index].value });
            },
            .bytes => |v| {
                state.count = v.items[index].count;
                if (v.items[index].value) |bytes| selected = Datum.json(.{ .string = bytes });
            },
            .dynamic => |v| return v.items[index],
        }
        if ((self.spec.kind == .min or self.spec.kind == .max) and state.count != 0) {
            // Snapshot ownership belongs to the caller's arena, not live state.
            const RowOwner = @TypeOf(state.selected.?);
            state.selected = try RowOwner.init(a, .{ .values = &.{selected.?}, .keys = &.{}, .ordinal = 0 });
        }
        return state;
    }
    pub fn set(self: *Column, index: usize, state: operators.Aggregate) void {
        switch (self.values) {
            .counts => |*v| v.items[index] = state.count,
            .integers => |*v| {
                v.items[index].count = state.count;
                v.items[index].sum = state.integer_sum;
            },
            .numbers => |*v| {
                v.items[index].count = state.count;
                v.items[index].sum = state.number_sum;
                v.items[index].compensation = state.compensation;
                v.items[index].mean = state.mean;
            },
            .booleans => |*v| v.items[index] = .{ .count = state.count, .value = state.boolean },
            .bytes, .dynamic, .exact => unreachable,
        }
    }
    fn promote(self: *Column, a: A) !void {
        var states: std.ArrayList(operators.Aggregate) = .empty;
        errdefer {
            for (states.items) |*state| state.deinit();
            states.deinit(a);
        }
        const count = switch (self.values) {
            inline else => |values| values.items.len,
        };
        try states.ensureTotalCapacity(a, count);
        for (0..count) |index| states.appendAssumeCapacity(try self.snapshot(a, index));
        self.clear(a);
        self.values = .{ .dynamic = states };
    }
    pub fn numericGrowth(self: *const Column, index: ?usize, weight: i32, length: usize) usize {
        var state: @import("numeric_aggregate.zig").State = .{};
        if (index) |slot| switch (self.values) {
            .exact => |values| state = values.items[slot].state,
            .dynamic => |values| if (values.items[slot].numeric) |reducer| {
                state = reducer.state;
            },
            else => return 0,
        };
        return state.growthBytes(weight, length);
    }

    pub fn update(self: *Column, a: A, index: usize, value: Datum) !void {
        if (self.values == .dynamic) return self.values.dynamic.items[index].update(value);
        if (value.sql_null) return;
        if (self.values == .exact) return self.values.exact.items[index].add((value.numeric orelse return error.SqlTypeMismatch).*);
        if (self.spec.kind == .min or self.spec.kind == .max) {
            const compatible = switch (self.values) {
                .integers => value.value == .integer,
                .numbers => value.value == .float,
                .booleans => value.value == .bool,
                .bytes => value.value == .string,
                else => unreachable,
            };
            if (!compatible) {
                try self.promote(a);
                return self.values.dynamic.items[index].update(value);
            }
            var count: u64 = 0;
            var prior: ?Datum = null;
            switch (self.values) {
                .integers => |v| {
                    count = v.items[index].count;
                    prior = Datum.json(.{ .integer = v.items[index].selected });
                    if (value.value != .integer) return error.SqlTypeMismatch;
                },
                .numbers => |v| {
                    count = v.items[index].count;
                    prior = Datum.json(.{ .float = v.items[index].selected });
                    if (value.value != .float) return error.SqlTypeMismatch;
                },
                .booleans => |v| {
                    count = v.items[index].count;
                    prior = Datum.json(.{ .bool = v.items[index].value });
                    if (value.value != .bool) return error.SqlTypeMismatch;
                },
                .bytes => |v| {
                    count = v.items[index].count;
                    if (v.items[index].value) |bytes| prior = Datum.json(.{ .string = bytes });
                    if (value.value != .string) return error.SqlTypeMismatch;
                },
                else => unreachable,
            }
            if (count == std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            const replace = count == 0 or (try @import("scalar.zig").compareDatums(value, prior.?)) == (if (self.spec.kind == .min) std.math.Order.lt else .gt);
            switch (self.values) {
                .integers => |*v| {
                    v.items[index].count += 1;
                    if (replace) v.items[index].selected = value.value.integer;
                },
                .numbers => |*v| {
                    v.items[index].count += 1;
                    if (replace) v.items[index].selected = value.value.float;
                },
                .booleans => |*v| {
                    v.items[index].count += 1;
                    if (replace) v.items[index].value = value.value.bool;
                },
                .bytes => |*v| {
                    if (replace) {
                        const owned = try a.dupe(u8, value.value.string);
                        if (v.items[index].value) |bytes| a.free(bytes);
                        v.items[index].value = owned;
                    }
                    v.items[index].count += 1;
                },
                else => unreachable,
            }
            return;
        }
        var state = try self.snapshot(a, index);
        try state.update(value);
        self.set(index, state);
    }
    /// Dispatch once per aggregate vector. Each group's lane order remains
    /// stable, including compensated floating reductions in the fallback.
    pub fn updateBatch(self: *Column, a: A, ids: []const usize, values: []const Datum) !void {
        if (ids.len != values.len) return error.InvalidSqlBackendResponse;
        switch (self.values) {
            .counts => |*counts| for (ids, values) |id, value| {
                if (value.sql_null) continue;
                counts.items[id] = std.math.add(u64, counts.items[id], 1) catch return error.SqlNumericOutOfRange;
                if (counts.items[id] > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            },
            .integers => |*integers| {
                if (self.spec.kind != .sum) {
                    for (ids, values) |id, value| try self.update(a, id, value);
                    return;
                }
                for (values) |value| if (!value.sql_null and value.value != .integer) {
                    for (ids, values) |id, cell| try self.update(a, id, cell);
                    return;
                };
                for (ids, values) |id, value| {
                    if (value.sql_null) continue;
                    const state = &integers.items[id];
                    state.sum = std.math.add(i128, state.sum, value.value.integer) catch return error.SqlNumericOutOfRange;
                    state.count = std.math.add(u64, state.count, 1) catch return error.SqlNumericOutOfRange;
                    if (state.count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
                }
            },
            .booleans => |*booleans| {
                if (self.spec.kind != .bool_and and self.spec.kind != .bool_or) {
                    for (ids, values) |id, value| try self.update(a, id, value);
                    return;
                }
                for (values) |value| if (!value.sql_null and value.value != .bool) return error.SqlTypeMismatch;
                for (ids, values) |id, value| {
                    if (value.sql_null) continue;
                    const state = &booleans.items[id];
                    state.value = if (self.spec.kind == .bool_and) state.value and value.value.bool else state.value or value.value.bool;
                    state.count = std.math.add(u64, state.count, 1) catch return error.SqlNumericOutOfRange;
                    if (state.count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
                }
            },
            else => for (ids, values) |id, value| try self.update(a, id, value),
        }
    }
    /// Consume a borrowed physical vector without expanding dictionary IDs.
    /// Iterate source lanes in order so floating and dynamic reductions retain
    /// exactly the same update order as the scalar path.
    pub fn updateEncoded(self: *Column, a: A, ids: []const usize, batch: @import("execution_batch.zig").Batch) !void {
        if (batch.width() != 1 or batch.len() != ids.len) return error.InvalidSqlBackendResponse;
        switch (batch) {
            .vectors => |vectors| return self.updateBatch(a, ids, vectors.values[0]),
            .dictionary => |dictionary| for (ids, dictionary.indices) |id, index| {
                if (index >= dictionary.values.len) return error.InvalidSqlBackendResponse;
                try self.update(a, id, dictionary.values[index]);
            },
            else => for (ids, 0..) |id, index| try self.update(a, id, try batch.cell(a, index, 0)),
        }
    }
    /// Merge compact partial vectors directly after resolving destination IDs.
    /// Typed states need no Aggregate snapshots, cloned reducers or OwnedRows.
    pub fn mergeBatch(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize) !void {
        return self.mergeSelectedBatch(a, ids, source, begin, null);
    }
    /// Selection vectors keep partial states typed across hash-owner exchanges.
    pub fn mergeSelectedBatch(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize, indices: ?[]const usize) !void {
        if (indices) |selected| if (selected.len != ids.len) return error.InvalidSqlBackendResponse;
        if (!std.meta.eql(self.spec, source.spec)) return error.InvalidSqlBackendResponse;
        if (std.meta.activeTag(self.values) != std.meta.activeTag(source.values)) {
            if (self.values != .dynamic) try self.promote(a);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                _ = arena.reset(.retain_capacity);
                try @import("aggregate_partial.zig").merge(&self.values.dynamic.items[id], try source.snapshot(arena.allocator(), index));
            }
            return;
        }
        switch (self.values) {
            .counts => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                target.items[id] = try mergedCount(target.items[id], source.values.counts.items[index]);
            },
            .exact => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                try target.items[id].merge(source.values.exact.items[index].state);
            },
            .integers => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                const from = source.values.integers.items[index];
                const to = &target.items[id];
                const count = try mergedCount(to.count, from.count);
                if (self.spec.kind == .sum) {
                    to.sum = std.math.add(i128, to.sum, from.sum) catch return error.SqlNumericOutOfRange;
                } else if (from.count != 0 and (to.count == 0 or (if (self.spec.kind == .min) from.selected < to.selected else from.selected > to.selected))) {
                    to.selected = from.selected;
                }
                to.count = count;
            },
            .booleans => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                const from = source.values.booleans.items[index];
                const to = &target.items[id];
                const count = try mergedCount(to.count, from.count);
                if (from.count != 0) to.value = if (to.count == 0) from.value else if (self.spec.kind == .bool_and or self.spec.kind == .min) to.value and from.value else to.value or from.value;
                to.count = count;
            },
            .bytes => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                const from = source.values.bytes.items[index];
                const to = &target.items[id];
                const count = try mergedCount(to.count, from.count);
                if (from.count != 0 and (to.count == 0 or std.mem.order(u8, from.value.?, to.value.?) == (if (self.spec.kind == .min) std.math.Order.lt else .gt))) {
                    const bytes = try a.dupe(u8, from.value.?);
                    if (to.value) |old| a.free(old);
                    to.value = bytes;
                }
                to.count = count;
            },
            .dynamic => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                try @import("aggregate_partial.zig").merge(&target.items[id], source.values.dynamic.items[index]);
            },
            .numbers => {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                for (ids, 0..) |id, offset| {
                    const index = if (indices) |selected| selected[offset] else begin + offset;
                    _ = arena.reset(.retain_capacity);
                    try self.mergeExact(a, id, try source.snapshot(arena.allocator(), index));
                }
            },
        }
    }
    fn mergedCount(left: u64, right: u64) !u64 {
        const count = std.math.add(u64, left, right) catch return error.SqlNumericOutOfRange;
        if (count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        return count;
    }
    pub fn mergeGrowth(self: *const Column, index: ?usize, source: *const Column, from: usize) !usize {
        switch (source.values) {
            .exact => |values| {
                const state = values.items[from].state;
                return self.numericGrowth(index, state.weight, state.buckets.items.len);
            },
            .dynamic => |values| {
                if (values.items[from].numeric) |reducer| return self.numericGrowth(index, reducer.state.weight, reducer.state.buckets.items.len);
                if (values.items[from].selected) |selected| return (try operators.datumBytes(selected.row.values[0])) *| 4;
            },
            .bytes => |values| return if (values.items[from].value) |bytes| bytes.len +| 64 else 0,
            else => {},
        }
        return 0;
    }
    pub fn mergeExact(self: *Column, a: A, index: usize, state: operators.Aggregate) !void {
        if (state.kind != self.spec.kind or state.input_type != self.spec.input_type or state.input_element != self.spec.input_element or state.distinct != self.spec.distinct) return error.InvalidSqlBackendResponse;
        if (self.values == .exact) {
            const reducer = state.numeric orelse return error.InvalidSqlBackendResponse;
            if (reducer.state.count != state.count) return error.InvalidSqlBackendResponse;
            return self.values.exact.items[index].merge(reducer.state);
        }
        if (self.values == .dynamic) return @import("aggregate_partial.zig").merge(&self.values.dynamic.items[index], state);
        if (self.spec.kind == .min or self.spec.kind == .max) {
            if (state.count == 0) return;
            const prior = switch (self.values) {
                .counts, .dynamic, .exact => unreachable,
                inline else => |values| values.items[index].count,
            };
            const total = std.math.add(u64, prior, state.count) catch return error.SqlNumericOutOfRange;
            if (total > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            try self.update(a, index, state.selected.?.row.values[0]);
            switch (self.values) {
                .counts => unreachable,
                .exact => unreachable,
                .dynamic => |*values| values.items[index].count = total,
                inline else => |*values| values.items[index].count = total,
            }
            return;
        }
        if (self.values == .numbers) {
            var target = try self.snapshot(a, index);
            try @import("aggregate_partial.zig").merge(&target, state);
            self.set(index, target);
            return;
        }
        switch (self.values) {
            .counts => |*values| {
                values.items[index] = std.math.add(u64, values.items[index], state.count) catch return error.SqlNumericOutOfRange;
                if (values.items[index] > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            },
            .integers => |*values| {
                if (self.spec.kind != .sum) return error.InvalidSqlBackendResponse;
                const target = &values.items[index];
                target.sum = std.math.add(i128, target.sum, state.integer_sum) catch return error.SqlNumericOutOfRange;
                target.count = std.math.add(u64, target.count, state.count) catch return error.SqlNumericOutOfRange;
                if (target.count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            },
            .booleans => |*values| {
                if (self.spec.kind != .bool_and and self.spec.kind != .bool_or) return error.InvalidSqlBackendResponse;
                const target = &values.items[index];
                target.value = if (self.spec.kind == .bool_and) target.value and state.boolean else target.value or state.boolean;
                target.count = std.math.add(u64, target.count, state.count) catch return error.SqlNumericOutOfRange;
                if (target.count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            },
            else => return error.InvalidSqlBackendResponse,
        }
    }
    pub fn finish(self: *const Column, index: usize) !Datum {
        if (self.values == .exact) return if (try self.values.exact.items[index].finish(self.spec.kind == .avg)) |value| Datum.typedNumeric(value) else .{};
        if (self.values == .dynamic) return self.values.dynamic.items[index].finish();
        if (self.spec.kind == .min or self.spec.kind == .max) return switch (self.values) {
            .integers => |v| if (v.items[index].count == 0) .{} else Datum.json(.{ .integer = v.items[index].selected }),
            .numbers => |v| if (v.items[index].count == 0) .{} else Datum.json(.{ .float = v.items[index].selected }),
            .booleans => |v| if (v.items[index].count == 0) .{} else Datum.json(.{ .bool = v.items[index].value }),
            .bytes => |v| if (v.items[index].value) |bytes| Datum.json(.{ .string = bytes }) else .{},
            else => unreachable,
        };
        return (try self.snapshot(std.heap.page_allocator, index)).finish();
    }
};

test "SQL typed aggregate vectors match scalar states for nulls mixed numerics and extrema" {
    const a = std.testing.allocator;
    const kinds = [_]operators.Aggregate.Kind{ .count, .sum, .avg, .min, .max, .bool_and, .bool_or };
    for (kinds) |kind| for ([_]@import("ast.zig").ColumnType{ .integer, .number, .string, .boolean }) |type_| {
        operators.Aggregate.validate(kind, type_) catch continue;
        const spec: operators.AggregateSpec = .{ .kind = kind, .input_type = type_ };
        var column = Column.init(spec);
        defer column.clear(a);
        try column.append(a);
        var reference = try operators.Aggregate.init(a, kind, type_);
        defer reference.deinit();
        for (0..131) |i| {
            const value: Datum = if (i % 7 == 0) .{} else Datum.json(switch (type_) {
                .integer => .{ .integer = @as(i64, @intCast(i % 13)) - 8 },
                .number => if (i % 2 == 0) .{ .float = @as(f64, @floatFromInt(i % 17)) - 3.5 } else .{ .integer = @intCast(i % 13) },
                .string => .{ .string = if (i % 2 == 0) "zebra" else "apple" },
                .boolean => .{ .bool = i % 3 == 0 },
                else => unreachable,
            });
            try reference.update(value);
            try column.update(a, 0, value);
            try std.testing.expectEqualDeep(try reference.finish(), try column.finish(0));
        }
    };
}

test "SQL typed partial merge batches allocate no primitive snapshots and own strings" {
    const a = std.testing.allocator;
    for ([_]operators.AggregateSpec{
        .{ .kind = .count },                            .{ .kind = .sum, .input_type = .integer },
        .{ .kind = .min, .input_type = .integer },      .{ .kind = .max, .input_type = .integer },
        .{ .kind = .min, .input_type = .boolean },      .{ .kind = .max, .input_type = .boolean },
        .{ .kind = .bool_and, .input_type = .boolean }, .{ .kind = .bool_or, .input_type = .boolean },
    }) |spec| {
        try std.testing.expect(operators.exactMergeable(spec));
        var source = Column.init(spec);
        defer source.clear(a);
        var target = Column.init(spec);
        defer target.clear(a);
        var reference = try operators.Aggregate.init(a, spec.kind, spec.input_type);
        defer reference.deinit();
        try target.append(a);
        for (0..129) |index| {
            try source.append(a);
            const value: Datum = if (index % 7 == 0) .{} else if (spec.input_type == .boolean) Datum.json(.{ .bool = index % 3 == 0 }) else Datum.json(.{ .integer = @as(i64, @intCast(index)) - 64 });
            try source.update(a, index, value);
            try reference.update(value);
        }
        var none = std.heap.FixedBufferAllocator.init(&.{});
        const ids: [129]usize = @splat(0);
        try target.mergeBatch(none.allocator(), &ids, &source, 0);
        try std.testing.expectEqualDeep(try reference.finish(), try target.finish(0));
    }
    for ([_]operators.Aggregate.Kind{ .min, .max }) |kind| {
        var source = Column.init(.{ .kind = kind, .input_type = .string });
        errdefer source.clear(a);
        var target = Column.init(source.spec);
        defer target.clear(a);
        try target.append(a);
        for ([_][]const u8{ "zebra", "apple", "apple" }, 0..) |text, index| {
            try source.append(a);
            try source.update(a, index, Datum.json(.{ .string = text }));
        }
        try target.mergeBatch(a, &.{ 0, 0, 0 }, &source, 0);
        source.clear(a);
        try std.testing.expectEqualStrings(if (kind == .min) "apple" else "zebra", (try target.finish(0)).value.string);
    }
    try std.testing.expect(!operators.exactMergeable(.{ .kind = .min, .input_type = .number }));
    try std.testing.expect(operators.exactMergeable(.{ .kind = .min, .input_type = .integer, .input_element = .int32 }));
    try std.testing.expect(operators.exactMergeable(.{ .kind = .max, .input_type = .string, .input_element = .text }));
}

test "SQL typed partial merge batches select exact NUMERIC lanes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var work: @import("array_value.zig").Budget = .{};
    const scalar = @import("scalar.zig");
    const low = try scalar.numericTextLeaky(arena.allocator(), "1.1", &work);
    const high = try scalar.numericTextLeaky(arena.allocator(), "2.2", &work);
    const expected = try scalar.numericTextLeaky(arena.allocator(), "5.5", &work);
    var source = Column.init(.{ .kind = .sum, .input_type = .number, .input_element = .numeric });
    defer source.clear(a);
    var target = Column.init(source.spec);
    defer target.clear(a);
    try source.append(a);
    try source.append(a);
    try source.update(a, 0, low);
    try source.update(a, 1, high);
    try target.append(a);
    try target.mergeSelectedBatch(a, &.{ 0, 0, 0 }, &source, 0, &.{ 1, 0, 1 });
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(expected, try target.finish(0)));
}
