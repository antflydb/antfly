// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
        booleans: std.ArrayList(Boolean),
        bytes: std.ArrayList(Bytes),
        dynamic: std.ArrayList(operators.Aggregate),
    };
    spec: operators.AggregateSpec,
    values: Values,
    pub fn init(spec: operators.AggregateSpec) Column {
        return .{ .spec = spec, .values = if (spec.distinct or spec.kind == .pattern_set) .{ .dynamic = .empty } else switch (spec.kind) {
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
            .booleans => |*values| try values.append(a, .{ .value = self.spec.kind == .bool_and }),
            .bytes => |*values| try values.append(a, .{}),
            .dynamic => |*values| {
                var state = try operators.Aggregate.init(a, self.spec.kind, self.spec.input_type);
                errdefer state.deinit();
                state.distinct = self.spec.distinct;
                try values.append(a, state);
            },
        }
    }
    pub fn snapshot(self: *const Column, a: A, index: usize) !operators.Aggregate {
        var state: operators.Aggregate = .{ .alloc = a, .kind = self.spec.kind, .input_type = self.spec.input_type, .boolean = self.spec.kind == .bool_and };
        var selected: ?Datum = null;
        switch (self.values) {
            .counts => |v| state.count = v.items[index],
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
            .bytes, .dynamic => unreachable,
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
    pub fn update(self: *Column, a: A, index: usize, value: Datum) !void {
        if (self.values == .dynamic) return self.values.dynamic.items[index].update(value);
        if (value.sql_null) return;
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
            const replace = count == 0 or (try @import("scalar.zig").compare(value.value, prior.?.value)) == (if (self.spec.kind == .min) std.math.Order.lt else .gt);
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
    pub fn finish(self: *const Column, index: usize) !Datum {
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
