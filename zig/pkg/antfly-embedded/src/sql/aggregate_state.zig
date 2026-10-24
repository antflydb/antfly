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
const Work = @import("operator_work.zig");
const Access = @import("execution_pager.zig").Access;
pub const Column = struct {
    const Integer = struct { count: u64 = 0, sum: i128 = 0, selected: i64 = 0 };
    const Number = struct { count: u64 = 0, sum: f64 = 0, compensation: f64 = 0, mean: f64 = 0, selected: f64 = 0 };
    const Boolean = struct { count: u64 = 0, value: bool = false };
    const Bytes = struct { count: u64 = 0, value: ?[]u8 = null };
    const Fixed = struct {
        alloc: A,
        count: u64 = 0,
        coefficient: i128 = 0,
        scale: u16 = 0,
        canonical: ?@import("numeric_value.zig").Owned = null,
        fn clear(self: *@This()) void {
            if (self.canonical) |*number| number.deinit();
            self.canonical = null;
        }
        fn add(self: *@This(), coefficient: i128, scale: u16, count: u64, minimum: bool) !void {
            if (count == 0) return;
            const total = try Column.mergedCount(self.count, count);
            if (self.count == 0 or @import("../common/decimal128.zig").order(coefficient, scale, self.coefficient, self.scale) == (if (minimum) std.math.Order.lt else .gt)) {
                self.clear();
                self.coefficient = coefficient;
                self.scale = scale;
            }
            self.count = total;
        }
        fn finish(self: *@This()) !Datum {
            if (self.count == 0) return .{};
            if (self.canonical == null) {
                var context: @import("numeric_value.zig").Context = .{ .alloc = self.alloc };
                self.canonical = try @import("numeric_value.zig").fromScaledInteger(&context, self.coefficient, self.scale);
            }
            return Datum.typedNumeric(&self.canonical.?.value);
        }
    };
    const Values = union(enum) {
        counts: std.ArrayList(u64),
        integers: std.ArrayList(Integer),
        numbers: std.ArrayList(Number),
        fixed: std.ArrayList(Fixed),
        exact: std.ArrayList(@import("numeric_aggregate.zig").Reducer),
        booleans: std.ArrayList(Boolean),
        bytes: std.ArrayList(Bytes),
        dynamic: std.ArrayList(operators.Aggregate),
    };
    const paging = @import("execution_pager.zig");
    const Paged = union(enum) {
        counts: paging.Array(u64),
        integers: paging.Array(Integer),
        numbers: paging.Array(Number),
        booleans: paging.Array(Boolean),
    };
    page_manager: ?*@import("spill.zig").Manager = null,
    paged: ?Paged = null,
    spec: operators.AggregateSpec,
    values: Values,
    pub fn init(spec: operators.AggregateSpec) Column {
        return .{ .spec = spec, .values = if (spec.distinct or spec.kind == .pattern_set) .{ .dynamic = .empty } else if (spec.input_element == .numeric and (spec.kind == .sum or spec.kind == .avg)) .{ .exact = .empty } else if (spec.input_element == .numeric and (spec.kind == .min or spec.kind == .max)) .{ .fixed = .empty } else switch (spec.kind) {
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
    const PageView = struct { column: Column, lease: paging.Pager.Lease };
    fn pageView(self: *const Column, index: usize, write: bool, access: Access) !PageView {
        switch (self.paged.?) {
            inline else => |array, tag| {
                const pinned = try array.pinFor(index, write, access);
                return .{ .lease = pinned.lease, .column = .{ .spec = self.spec, .values = @unionInit(Values, @tagName(tag), .{ .items = @as([*]@TypeOf(pinned.value.*), @ptrCast(pinned.value))[0..1], .capacity = 1, .pointer_stability = .{} }) } };
            },
        }
    }
    fn pageStart(self: *const Column, index: usize) usize {
        return switch (self.paged.?) {
            inline else => |array| array.pageStart(index),
        };
    }
    fn pageWindow(self: *const Column, index: usize, access: Access) !PageView {
        switch (self.paged.?) {
            inline else => |array, tag| {
                const window = try array.pinPageFor(index, true, access);
                return .{ .lease = window.lease, .column = .{ .spec = self.spec, .values = @unionInit(Values, @tagName(tag), .{ .items = window.values, .capacity = window.values.len, .pointer_stability = .{} }) } };
            },
        }
    }
    /// Group a bounded input tile by resident page. Each group's lanes keep
    /// their original order, including compensated floating reductions.
    fn updatePaged(self: *Column, a: A, ids: []const usize, values: ?[]const Datum, physical: ?@import("execution_batch.zig").PhysicalColumn, access: Access) anyerror!void {
        var begin: usize = 0;
        while (begin < ids.len) {
            const end = @min(ids.len, begin + 128);
            var consumed: [128]bool = @splat(false);
            for (ids[begin..end], 0..) |id, lane| {
                if (consumed[lane]) continue;
                const first = self.pageStart(id);
                var selected: [128]usize = undefined;
                var cells: [128]Datum = undefined;
                var rows: [128]usize = undefined;
                var count: usize = 0;
                for (ids[begin..end], 0..) |candidate, position| {
                    if (consumed[position] or self.pageStart(candidate) != first) continue;
                    consumed[position] = true;
                    selected[count] = candidate - first;
                    if (values) |input| cells[count] = input[begin + position];
                    if (physical) |input| rows[count] = input.rowAt(begin + position);
                    count += 1;
                }
                var view = try self.pageWindow(id, access);
                defer view.lease.release();
                if (physical) |input| {
                    var tile = input;
                    tile.range = null;
                    tile.selection = rows[0..count];
                    if (!try view.column.updatePhysicalFor(a, selected[0..count], tile, access)) {
                        for (selected[0..count], 0..) |slot, position| try view.column.updateFor(a, slot, try tile.cell(position), access);
                    }
                } else try view.column.updateBatchFor(a, selected[0..count], cells[0..count], access);
            }
            begin = end;
        }
    }
    fn startPaging(self: *Column, a: A, access: Access) !void {
        if (self.page_manager == null or self.paged != null) return;
        const manager = self.page_manager.?;
        if (manager.page_residency_bytes < paging.page_bytes) return;
        if (self.spec.kind == .min or self.spec.kind == .max) return;
        switch (self.values) {
            inline .counts, .integers, .numbers, .booleans => |*values, tag| {
                if (values.items.len < 2048) return;
                var array: @FieldType(Paged, @tagName(tag)) = .{ .pager = try manager.pagerFor(access) };
                errdefer array.deinit();
                try array.appendSliceFor(values.items, access);
                values.clearAndFree(a);
                self.paged = @unionInit(Paged, @tagName(tag), array);
            },
            else => {},
        }
    }
    pub fn clear(self: *Column, a: A) void {
        if (self.paged) |*paged| switch (paged.*) {
            inline else => |*array| array.deinit(),
        };
        self.paged = null;
        switch (self.values) {
            .fixed => |values| for (values.items) |*value| value.clear(),
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
        return self.appendControlled(a, null);
    }
    pub fn appendControlled(self: *Column, a: A, control: ?Work.Control) !void {
        return self.appendFor(a, .{ .control = control });
    }
    pub fn appendFor(self: *Column, a: A, access: Access) !void {
        const control = access.control;
        try Work.poll(control);
        try self.startPaging(a, access);
        if (self.paged) |*paged| {
            switch (paged.*) {
                .counts => |*array| try array.appendFor(0, access),
                .integers => |*array| try array.appendFor(.{}, access),
                .numbers => |*array| try array.appendFor(.{}, access),
                .booleans => |*array| try array.appendFor(.{ .value = self.spec.kind == .bool_and }, access),
            }
            return;
        }
        try operators.Aggregate.validate(self.spec.kind, self.spec.input_type);
        switch (self.values) {
            .fixed => |*values| try values.append(a, .{ .alloc = a }),
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
        return self.snapshotControlled(a, index, null);
    }
    pub fn snapshotControlled(self: *const Column, a: A, index: usize, control: ?Work.Control) !operators.Aggregate {
        return self.snapshotFor(a, index, .{ .control = control });
    }
    pub fn snapshotFor(self: *const Column, a: A, index: usize, access: Access) !operators.Aggregate {
        const control = access.control;
        try Work.poll(control);
        if (self.paged != null) {
            const view = try self.pageView(index, false, access);
            defer view.lease.release();
            return view.column.snapshotFor(a, 0, access);
        }
        var state: operators.Aggregate = .{ .alloc = a, .kind = self.spec.kind, .input_type = self.spec.input_type, .input_element = self.spec.input_element, .boolean = self.spec.kind == .bool_and };
        var selected: ?Datum = null;
        var fixed_digits: @import("numeric_value.zig").ScaledInteger = undefined;
        var fixed_number: @import("numeric_value.zig").Value = undefined;
        switch (self.values) {
            .fixed => |v| {
                const from = v.items[index];
                state.count = from.count;
                fixed_digits = try @import("numeric_value.zig").ScaledInteger.init(from.coefficient, from.scale);
                fixed_number = fixed_digits.view();
                selected = Datum.typedNumeric(&fixed_number);
            },
            .counts => |v| state.count = v.items[index],
            .exact => |v| {
                const reducer = try a.create(@import("numeric_aggregate.zig").Reducer);
                errdefer a.destroy(reducer);
                reducer.* = try v.items[index].cloneControlled(a, control);
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
            state.selected = try RowOwner.initControlled(a, .{ .values = &.{selected.?}, .keys = &.{}, .ordinal = 0 }, control);
        }
        return state;
    }
    pub fn set(self: *Column, index: usize, state: operators.Aggregate) !void {
        return self.setFor(index, state, .{});
    }
    pub fn setFor(self: *Column, index: usize, state: operators.Aggregate, access: Access) !void {
        try Work.poll(access.control);
        if (self.paged != null) {
            var view = try self.pageView(index, true, access);
            defer view.lease.release();
            return view.column.setFor(0, state, access);
        }
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
            .bytes, .dynamic, .exact, .fixed => unreachable,
        }
    }
    fn promote(self: *Column, a: A, control: ?Work.Control) !void {
        var states: std.ArrayList(operators.Aggregate) = .empty;
        errdefer {
            for (states.items) |*state| state.deinit();
            states.deinit(a);
        }
        const count = switch (self.values) {
            inline else => |values| values.items.len,
        };
        try states.ensureTotalCapacity(a, count);
        for (0..count) |index| states.appendAssumeCapacity(try self.snapshotControlled(a, index, control));
        self.clear(a);
        self.values = .{ .dynamic = states };
    }
    pub fn appendGrowth(self: *const Column, additional: usize) usize {
        if (self.paged != null) return additional *| @sizeOf(usize);

        return switch (self.values) {
            inline else => |values| blk: {
                const required = values.items.len +| additional;
                if (required <= values.capacity) break :blk 0;
                break :blk (required *| 2 +| 8) *| @sizeOf(@TypeOf(values.items[0]));
            },
        };
    }
    /// Mixed extrema promote the entire state column, including untouched
    /// groups. Admit that transient copy before changing any live state.
    pub fn updateGrowth(self: *const Column, value: Datum, additional: usize) usize {
        if (value.sql_null or (self.spec.kind != .min and self.spec.kind != .max)) return 0;
        const compatible = switch (self.values) {
            .fixed => fixedValue(value) != null,
            .integers => value.value == .integer,
            .numbers => value.value == .float,
            .booleans => value.value == .bool,
            .bytes => value.value == .string,
            .dynamic => true,
            else => false,
        };
        if (compatible) return 0;
        const count = switch (self.values) {
            inline else => |values| values.items.len,
        };
        var bytes = (count +| additional) *| (2 * @sizeOf(operators.Aggregate) + 1024);
        if (self.values == .bytes) for (self.values.bytes.items) |state| if (state.value) |text| {
            bytes +|= text.len;
        };
        return bytes;
    }
    pub fn physicalUpdateGrowth(self: *const Column, physical: @import("execution_batch.zig").PhysicalColumn, additional: usize) !usize {
        if (self.spec.kind != .min and self.spec.kind != .max) return 0;
        const compatible = switch (self.values) {
            .integers => physical.kind == .integer,
            .numbers => physical.kind == .number,
            .booleans => physical.kind == .boolean,
            .bytes => physical.kind == .string,
            .dynamic => true,
            else => false,
        };
        if (compatible) return 0;
        for (0..physical.len()) |lane| if (!physical.vector.nulls.isNull(physical.rowAt(lane))) return self.updateGrowth(try physical.cell(lane), additional);
        return 0;
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

    fn fixedValue(value: Datum) ?struct { coefficient: i128, scale: u16 } {
        const number = value.numeric orelse return null;
        if (number.kind != .finite or number.scale > 38) return null;
        const coefficient = @import("numeric_value.zig").coefficientAtScale(number.*, number.scale) orelse return null;
        @import("../common/decimal128.zig").validateCoefficient(coefficient, 38, number.scale) catch return null;
        return .{ .coefficient = coefficient, .scale = number.scale };
    }
    pub fn update(self: *Column, a: A, index: usize, value: Datum) !void {
        return self.updateControlled(a, index, value, null);
    }
    pub fn updateControlled(self: *Column, a: A, index: usize, value: Datum, control: ?Work.Control) !void {
        return self.updateFor(a, index, value, .{ .control = control });
    }
    pub fn updateFor(self: *Column, a: A, index: usize, value: Datum, access: Access) !void {
        const control = access.control;
        try Work.poll(control);
        if (self.paged != null) {
            var view = try self.pageView(index, true, access);
            defer view.lease.release();
            return view.column.updateFor(a, 0, value, access);
        }
        if (self.values == .dynamic) return self.values.dynamic.items[index].updateControlled(value, control);
        if (value.sql_null) return;
        if (self.values == .fixed) {
            if (fixedValue(value)) |fixed| return self.values.fixed.items[index].add(fixed.coefficient, fixed.scale, 1, self.spec.kind == .min);
            try self.promote(a, control);
            return self.values.dynamic.items[index].updateControlled(value, control);
        }
        if (self.values == .exact) return self.values.exact.items[index].addControlled((value.numeric orelse return error.SqlTypeMismatch).*, control);
        if (self.spec.kind == .min or self.spec.kind == .max) {
            const compatible = switch (self.values) {
                .integers => value.value == .integer,
                .numbers => value.value == .float,
                .booleans => value.value == .bool,
                .bytes => value.value == .string,
                else => unreachable,
            };
            if (!compatible) {
                try self.promote(a, control);
                return self.values.dynamic.items[index].updateControlled(value, control);
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
            const replace = count == 0 or (try @import("scalar.zig").compareDatumsControlled(value, prior.?, control)) == (if (self.spec.kind == .min) std.math.Order.lt else .gt);
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
                        const owned = try Work.dupe(u8, a, value.value.string, control);
                        if (v.items[index].value) |bytes| a.free(bytes);
                        v.items[index].value = owned;
                    }
                    v.items[index].count += 1;
                },
                else => unreachable,
            }
            return;
        }
        var state = try self.snapshotFor(a, index, access);
        try state.updateControlled(value, control);
        try self.setFor(index, state, access);
    }
    /// Dispatch once per aggregate vector. Each group's lane order remains
    /// stable, including compensated floating reductions in the fallback.
    pub fn updateBatch(self: *Column, a: A, ids: []const usize, values: []const Datum) !void {
        return self.updateBatchControlled(a, ids, values, null);
    }
    pub fn updateBatchControlled(self: *Column, a: A, ids: []const usize, values: []const Datum, control: ?Work.Control) !void {
        return self.updateBatchFor(a, ids, values, .{ .control = control });
    }
    pub fn updateBatchFor(self: *Column, a: A, ids: []const usize, values: []const Datum, access: Access) !void {
        const control = access.control;
        try Work.poll(control);
        if (self.paged != null) {
            if (ids.len != values.len) return error.InvalidSqlBackendResponse;
            return self.updatePaged(a, ids, values, null, access);
        }
        if (ids.len != values.len) return error.InvalidSqlBackendResponse;
        switch (self.values) {
            .counts => |*counts| for (ids, values) |id, value| {
                try Work.poll(control);
                if (value.sql_null) continue;
                counts.items[id] = std.math.add(u64, counts.items[id], 1) catch return error.SqlNumericOutOfRange;
                if (counts.items[id] > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            },
            .integers => |*integers| {
                if (self.spec.kind != .sum) {
                    for (ids, values) |id, value| try self.updateFor(a, id, value, access);
                    return;
                }
                for (values) |value| {
                    try Work.poll(control);
                    if (!value.sql_null and value.value != .integer) {
                        for (ids, values) |id, cell| try self.updateFor(a, id, cell, access);
                        return;
                    }
                }
                for (ids, values) |id, value| {
                    try Work.poll(control);
                    if (value.sql_null) continue;
                    const state = &integers.items[id];
                    state.sum = std.math.add(i128, state.sum, value.value.integer) catch return error.SqlNumericOutOfRange;
                    state.count = std.math.add(u64, state.count, 1) catch return error.SqlNumericOutOfRange;
                    if (state.count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
                }
            },
            .booleans => |*booleans| {
                if (self.spec.kind != .bool_and and self.spec.kind != .bool_or) {
                    for (ids, values) |id, value| try self.updateFor(a, id, value, access);
                    return;
                }
                for (values) |value| {
                    try Work.poll(control);
                    if (!value.sql_null and value.value != .bool) return error.SqlTypeMismatch;
                }
                for (ids, values) |id, value| {
                    try Work.poll(control);
                    if (value.sql_null) continue;
                    const state = &booleans.items[id];
                    state.value = if (self.spec.kind == .bool_and) state.value and value.value.bool else state.value or value.value.bool;
                    state.count = std.math.add(u64, state.count, 1) catch return error.SqlNumericOutOfRange;
                    if (state.count > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
                }
            },
            else => for (ids, values) |id, value| try self.updateFor(a, id, value, access),
        }
    }
    /// Consume a borrowed physical vector without expanding dictionary IDs.
    /// Iterate source lanes in order so floating and dynamic reductions retain
    /// exactly the same update order as the scalar path.
    pub fn updateEncoded(self: *Column, a: A, ids: []const usize, batch: @import("execution_batch.zig").Batch) !void {
        return self.updateEncodedControlled(a, ids, batch, null);
    }
    pub fn updateEncodedControlled(self: *Column, a: A, ids: []const usize, batch: @import("execution_batch.zig").Batch, control: ?Work.Control) !void {
        return self.updateEncodedFor(a, ids, batch, .{ .control = control });
    }
    pub fn updateEncodedFor(self: *Column, a: A, ids: []const usize, batch: @import("execution_batch.zig").Batch, access: Access) !void {
        const control = access.control;
        try Work.poll(control);
        if (self.paged != null) {
            if (batch.width() != 1 or batch.len() != ids.len) return error.InvalidSqlBackendResponse;
            if (batch == .reader) if (batch.reader.cohort) |cohort| return self.updateCohortFor(a, ids, cohort.source.*, cohort.positions, access);
            var scratch = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
            defer scratch.deinit();
            if (try batch.physicalColumn(scratch.allocator(), 0)) |physical| if (try self.updatePhysicalFor(a, ids, physical, access)) return;
            for (ids, 0..) |id, row| try self.updateFor(a, id, try batch.cell(scratch.allocator(), row, 0), access);
            return;
        }
        if (batch.width() != 1 or batch.len() != ids.len) return error.InvalidSqlBackendResponse;
        if (batch == .reader) if (batch.reader.cohort) |cohort| {
            return self.updateCohortFor(a, ids, cohort.source.*, cohort.positions, access);
        };
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        if (self.values == .exact or self.values == .fixed or self.values == .counts) if (try batch.decimalColumnSelected(scratch.allocator(), 0, null)) |decimal| {
            const view = decimal.physical;
            const coefficients = view.vector.values.decimal128;
            for (ids, 0..) |id, lane| {
                const row = view.rowAt(lane);
                if (view.vector.nulls.isNull(row)) continue;
                const scale = try decimal.scaleAt(lane);
                try @import("../common/decimal128.zig").validateCoefficient(coefficients.values[row], coefficients.precision, scale);
                if (self.values == .counts) {
                    self.values.counts.items[id] = try mergedCount(self.values.counts.items[id], 1);
                    continue;
                }
                if (self.values == .fixed) {
                    try self.values.fixed.items[id].add(coefficients.values[row], scale, 1, self.spec.kind == .min);
                    continue;
                }
                const number = try @import("numeric_value.zig").ScaledInteger.init(coefficients.values[row], scale);
                try self.values.exact.items[id].add(number.view());
            }
            return;
        };
        if (try batch.physicalColumn(scratch.allocator(), 0)) |physical| if (try self.updatePhysicalFor(a, ids, physical, access)) return;
        switch (batch) {
            .vectors => |vectors| return self.updateBatchFor(a, ids, vectors.values[0], access),
            .dictionary => |dictionary| {
                if (self.values == .counts) {
                    for (ids, dictionary.indices) |id, index| {
                        try Work.poll(control);
                        if (index >= dictionary.values.len) return error.InvalidSqlBackendResponse;
                        if (!dictionary.values[index].sql_null) self.values.counts.items[id] = try mergedCount(self.values.counts.items[id], 1);
                    }
                    return;
                }
                for (ids, dictionary.indices) |id, index| {
                    try Work.poll(control);
                    if (index >= dictionary.values.len) return error.InvalidSqlBackendResponse;
                    try self.updateFor(a, id, dictionary.values[index], access);
                }
            },
            else => for (ids, 0..) |id, index| try self.updateFor(a, id, try batch.cellControlled(scratch.allocator(), index, 0, control), access),
        }
    }
    pub fn updateCohort(self: *Column, a: A, ids: []const usize, source: @import("execution_batch.zig").Batch, positions: []const usize) !void {
        return self.updateCohortFor(a, ids, source, positions, .{});
    }
    pub fn updateCohortFor(self: *Column, a: A, ids: []const usize, source: @import("execution_batch.zig").Batch, positions: []const usize, access: Access) !void {
        try Work.poll(access.control);
        if (source.len() != positions.len) return error.InvalidSqlBackendResponse;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        if (self.values == .exact or self.values == .fixed or self.values == .counts) if (try source.decimalColumnSelected(scratch.allocator(), 0, null)) |decimal| {
            for (positions, 0..) |position, lane| {
                if (position >= ids.len) return error.InvalidSqlBackendResponse;
                var fixed: @import("numeric_value.zig").ScaledInteger = undefined;
                var number: @import("numeric_value.zig").Value = undefined;
                const value = try decimal.cell(lane, &fixed, &number);
                if (!value.sql_null) try self.updateFor(a, ids[position], value, access);
            }
            return;
        };
        const physical = try source.physicalColumn(scratch.allocator(), 0);
        var begin: usize = 0;
        var selected_ids: [256]usize = undefined;
        while (begin < positions.len) {
            const length = @min(selected_ids.len, positions.len - begin);
            for (positions[begin..][0..length], selected_ids[0..length]) |position, *id| {
                if (position >= ids.len) return error.InvalidSqlBackendResponse;
                id.* = ids[position];
            }
            if (physical) |native| {
                const view = native.slice(begin, begin + length);
                if (try self.updatePhysicalFor(a, selected_ids[0..length], view, access)) {
                    begin += length;
                    continue;
                }
            }
            for (selected_ids[0..length], 0..) |id, offset| try self.updateFor(a, id, try source.cell(scratch.allocator(), begin + offset, 0), access);
            begin += length;
        }
    }
    fn physicalValue(values: anytype, row: usize) switch (@typeInfo(@TypeOf(values))) {
        .pointer => |p| p.child,
        else => @TypeOf(values.values[0]),
    } {
        return switch (@typeInfo(@TypeOf(values))) {
            .pointer => values[row],
            else => values.at(row),
        };
    }
    /// Dispatch once per state/vector pair. Nulls and selections stay physical;
    /// dictionary IDs are gathered directly without creating Datum arrays.
    pub fn updatePhysical(self: *Column, a: A, ids: []const usize, physical: @import("execution_batch.zig").PhysicalColumn) !bool {
        return self.updatePhysicalFor(a, ids, physical, .{});
    }
    pub fn updatePhysicalFor(self: *Column, a: A, ids: []const usize, physical: @import("execution_batch.zig").PhysicalColumn, access: Access) !bool {
        try Work.poll(access.control);
        if (self.paged != null) {
            if (physical.len() != ids.len) return error.InvalidSqlBackendResponse;
            try physical.validate();
            try self.updatePaged(a, ids, null, physical, access);
            return true;
        }
        if (physical.len() != ids.len) return error.InvalidSqlBackendResponse;
        const compatible = switch (self.values) {
            .counts => true,
            .integers => physical.kind == .integer,
            .numbers => physical.kind == .number or (physical.kind == .integer and self.spec.kind == .avg),
            .booleans => physical.kind == .boolean,
            .bytes => physical.kind == .string,
            else => false,
        };
        if (!compatible) return false;
        try physical.validate();
        const vector = physical.vector;
        switch (self.values) {
            .counts => |*states| for (ids, 0..) |id, lane| {
                const row = physical.rowAt(lane);
                if (!vector.nulls.isNull(row)) states.items[id] = try mergedCount(states.items[id], 1);
            },
            .integers => |*states| switch (vector.values) {
                inline .i64, .dictionary_i64 => |values| for (ids, 0..) |id, lane| {
                    const row = physical.rowAt(lane);
                    if (vector.nulls.isNull(row)) continue;
                    const value = physicalValue(values, row);
                    const state = &states.items[id];
                    const count = try mergedCount(state.count, 1);
                    if (self.spec.kind == .sum) {
                        state.sum = std.math.add(i128, state.sum, value) catch return error.SqlNumericOutOfRange;
                    } else if (state.count == 0 or (if (self.spec.kind == .min) value < state.selected else value > state.selected)) state.selected = value;
                    state.count = count;
                },
                else => unreachable,
            },
            .numbers => |*states| switch (vector.values) {
                inline .i64, .dictionary_i64, .f64, .dictionary_f64 => |values| for (ids, 0..) |id, lane| {
                    const row = physical.rowAt(lane);
                    if (vector.nulls.isNull(row)) continue;
                    const native = physicalValue(values, row);
                    const value: f64 = if (@TypeOf(native) == i64) @floatFromInt(native) else native;
                    const state = &states.items[id];
                    const count = try mergedCount(state.count, 1);
                    if (self.spec.kind == .sum) {
                        if (self.spec.input_element == .float32) state.sum = try operators.Aggregate.addRealSum(state.sum, value) else try operators.Aggregate.addFloatSum(&state.sum, &state.compensation, value);
                    } else if (self.spec.kind == .min or self.spec.kind == .max) {
                        // SQL orders NaN above finite values and treats NaNs
                        // and signed zero ties as equal, retaining the first.
                        const order: std.math.Order = if (std.math.isNan(value)) (if (std.math.isNan(state.selected)) .eq else .gt) else if (std.math.isNan(state.selected)) .lt else std.math.order(value, state.selected);
                        if (state.count == 0 or order == (if (self.spec.kind == .min) std.math.Order.lt else .gt)) state.selected = value;
                    } else {
                        const n: f64 = @floatFromInt(count);
                        const delta = value - state.mean;
                        if (!std.math.isFinite(state.mean) or !std.math.isFinite(value)) state.mean += value else {
                            state.mean = if (std.math.isFinite(delta)) state.mean + delta / n else state.mean * ((n - 1) / n) + value / n;
                            if (!std.math.isFinite(state.mean)) return error.SqlNumericOutOfRange;
                        }
                    }
                    state.count = count;
                },
                else => unreachable,
            },
            .booleans => |*states| for (ids, 0..) |id, lane| {
                const row = physical.rowAt(lane);
                if (vector.nulls.isNull(row)) continue;
                const value = vector.values.bool[row];
                const state = &states.items[id];
                const count = try mergedCount(state.count, 1);
                switch (self.spec.kind) {
                    .bool_and => state.value = state.value and value,
                    .bool_or => state.value = state.value or value,
                    .min => state.value = if (state.count == 0) value else state.value and value,
                    .max => state.value = if (state.count == 0) value else state.value or value,
                    else => unreachable,
                }
                state.count = count;
            },
            .bytes => |*states| switch (vector.values) {
                inline .bytes, .dictionary_bytes => |values| for (ids, 0..) |id, lane| {
                    const row = physical.rowAt(lane);
                    if (vector.nulls.isNull(row)) continue;
                    const value = physicalValue(values, row);
                    const state = &states.items[id];
                    const count = try mergedCount(state.count, 1);
                    if (state.value == null or std.mem.order(u8, value, state.value.?) == (if (self.spec.kind == .min) std.math.Order.lt else .gt)) {
                        const owned = try a.dupe(u8, value);
                        if (state.value) |previous| a.free(previous);
                        state.value = owned;
                    }
                    state.count = count;
                },
                else => unreachable,
            },
            else => unreachable,
        }
        return true;
    }
    /// Merge one validated AGS column after a shared destination-key census.
    pub fn mergeFixedBatch(self: *Column, a: A, ids: []const usize, states: []const @import("aggregate_partial.zig").Fixed) !void {
        return self.mergeFixedBatchFor(a, ids, states, .{});
    }
    pub fn mergeFixedBatchFor(self: *Column, a: A, ids: []const usize, states: []const @import("aggregate_partial.zig").Fixed, access: Access) !void {
        try Work.poll(access.control);
        if (self.paged != null) {
            if (ids.len != states.len) return error.InvalidSqlBackendResponse;
            var begin: usize = 0;
            while (begin < ids.len) {
                const end = @min(ids.len, begin + 128);
                var done: [128]bool = @splat(false);
                for (ids[begin..end], 0..) |id, lane| {
                    if (done[lane]) continue;
                    const first = self.pageStart(id);
                    var selected: [128]usize = undefined;
                    var input: [128]@import("aggregate_partial.zig").Fixed = undefined;
                    var count: usize = 0;
                    for (ids[begin..end], 0..) |candidate, i| {
                        if (done[i] or self.pageStart(candidate) != first) continue;
                        done[i] = true;
                        selected[count] = candidate - first;
                        input[count] = states[begin + i];
                        count += 1;
                    }
                    var view = try self.pageWindow(id, access);
                    defer view.lease.release();
                    try view.column.mergeFixedBatchFor(a, selected[0..count], input[0..count], access);
                }
                begin = end;
            }
            return;
        }
        if (ids.len != states.len) return error.InvalidSqlBackendResponse;
        switch (self.values) {
            .counts => |*target| for (ids, states) |id, from| {
                target.items[id] = try mergedCount(target.items[id], from.count);
            },
            .fixed => {
                for (ids, states) |id, from| {
                    if (from.count == 0) continue;
                    if (fixedValue(from.selected)) |fixed| {
                        if (self.values == .fixed) {
                            try self.values.fixed.items[id].add(fixed.coefficient, fixed.scale, from.count, self.spec.kind == .min);
                            continue;
                        }
                    }
                    if (self.values != .dynamic) try self.promote(a, null);
                    var state = try operators.Aggregate.initTyped(a, self.spec.kind, self.spec.input_type, self.spec.input_element);
                    defer state.deinit();
                    try state.update(from.selected);
                    state.count = from.count;
                    try @import("aggregate_partial.zig").merge(&self.values.dynamic.items[id], state);
                }
            },
            .exact => |*target| for (ids, states) |id, from| {
                try target.items[id].merge(from.numeric orelse return error.InvalidSqlBackendResponse);
            },
            .integers => |*target| for (ids, states) |id, from| {
                const to = &target.items[id];
                const count = try mergedCount(to.count, from.count);
                if (self.spec.kind == .sum) to.sum = std.math.add(i128, to.sum, from.sum) catch return error.SqlNumericOutOfRange else if (from.count != 0 and (to.count == 0 or (if (self.spec.kind == .min) from.selected.value.integer < to.selected else from.selected.value.integer > to.selected))) to.selected = from.selected.value.integer;
                to.count = count;
            },
            .booleans => |*target| for (ids, states) |id, from| {
                const to = &target.items[id];
                const count = try mergedCount(to.count, from.count);
                const value = if (self.spec.kind == .min or self.spec.kind == .max) if (from.count == 0) false else from.selected.value.bool else from.boolean;
                if (from.count != 0) to.value = if (to.count == 0) value else if (self.spec.kind == .bool_and or self.spec.kind == .min) to.value and value else to.value or value;
                to.count = count;
            },
            .bytes => |*target| for (ids, states) |id, from| {
                const to = &target.items[id];
                const count = try mergedCount(to.count, from.count);
                if (from.count != 0 and (to.count == 0 or std.mem.order(u8, from.selected.value.string, to.value.?) == (if (self.spec.kind == .min) std.math.Order.lt else .gt))) {
                    const owned = try a.dupe(u8, from.selected.value.string);
                    if (to.value) |old| a.free(old);
                    to.value = owned;
                }
                to.count = count;
            },
            else => return error.InvalidSqlBackendResponse,
        }
    }
    /// Merge compact partial vectors directly after resolving destination IDs.
    /// Typed states need no Aggregate snapshots, cloned reducers or OwnedRows.
    pub fn mergeBatch(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize) !void {
        return self.mergeBatchFor(a, ids, source, begin, .{});
    }
    pub fn mergeBatchFor(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize, access: Access) !void {
        try Work.poll(access.control);
        return self.mergeSelectedBatchFor(a, ids, source, begin, null, access);
    }
    /// Selection vectors keep partial states typed across hash-owner exchanges.
    pub fn mergeSelectedBatch(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize, indices: ?[]const usize) anyerror!void {
        return self.mergeSelectedBatchFor(a, ids, source, begin, indices, .{});
    }
    pub fn mergeSelectedBatchFor(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize, indices: ?[]const usize, access: Access) anyerror!void {
        try Work.poll(access.control);
        if (!std.meta.eql(self.spec, source.spec)) return error.InvalidSqlBackendResponse;
        if (indices) |selected| if (selected.len != ids.len) return error.InvalidSqlBackendResponse;
        if (self == source and self.paged != null) {
            for (ids, 0..) |id, lane| try self.mergeExactFor(a, id, try source.snapshotFor(a, if (indices) |selected| selected[lane] else begin + lane, access), access);
            return;
        }
        if (source.paged) |paged| switch (paged) {
            inline else => |array, tag| {
                var offset: usize = 0;
                while (offset < ids.len) {
                    const end = @min(ids.len, offset + 128);
                    var selected: [128]usize = undefined;
                    for (selected[0 .. end - offset], offset..) |*index, lane| index.* = if (indices) |input| input[lane] else begin + lane;
                    const Element = switch (tag) {
                        .counts => u64,
                        .integers => Integer,
                        .numbers => Number,
                        .booleans => Boolean,
                    };
                    var values: [128]Element = undefined;
                    try array.getManyFor(selected[0 .. end - offset], values[0 .. end - offset], access);
                    const tile: Column = .{ .spec = source.spec, .values = @unionInit(Values, @tagName(tag), .{ .items = values[0 .. end - offset], .capacity = values.len, .pointer_stability = .{} }) };
                    try self.mergeDestinationTiles(a, ids[offset..end], &tile, 0, null, access);
                    offset = end;
                }
                return;
            },
        };
        if (self.paged != null) return self.mergeDestinationTiles(a, ids, source, begin, indices, access);
        if (indices) |selected| if (selected.len != ids.len) return error.InvalidSqlBackendResponse;
        if (!std.meta.eql(self.spec, source.spec)) return error.InvalidSqlBackendResponse;
        if (std.meta.activeTag(self.values) != std.meta.activeTag(source.values)) {
            if (self.values != .dynamic) try self.promote(a, null);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                _ = arena.reset(.retain_capacity);
                try @import("aggregate_partial.zig").merge(&self.values.dynamic.items[id], try source.snapshotFor(arena.allocator(), index, access));
            }
            return;
        }
        switch (self.values) {
            .counts => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected| selected[offset] else begin + offset;
                target.items[id] = try mergedCount(target.items[id], source.values.counts.items[index]);
            },
            .fixed => |*target| for (ids, 0..) |id, offset| {
                const index = if (indices) |selected_| selected_[offset] else begin + offset;
                const from = source.values.fixed.items[index];
                try target.items[id].add(from.coefficient, from.scale, from.count, self.spec.kind == .min);
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
                    try self.mergeExactFor(a, id, try source.snapshotFor(arena.allocator(), index, access), access);
                }
            },
        }
    }
    /// Source pages are gathered before destination pinning, so one-frame
    /// residency and a shared source/destination pager cannot deadlock. Lanes
    /// for each destination retain their exact partial-reduction order.
    fn mergeDestinationTiles(self: *Column, a: A, ids: []const usize, source: *const Column, begin: usize, indices: ?[]const usize, access: Access) anyerror!void {
        if (self.paged == null) return self.mergeSelectedBatchFor(a, ids, source, begin, indices, access);
        var offset: usize = 0;
        while (offset < ids.len) {
            const end = @min(ids.len, offset + 128);
            var done: [128]bool = @splat(false);
            for (ids[offset..end], 0..) |id, lane| {
                if (done[lane]) continue;
                const first = self.pageStart(id);
                var target: [128]usize = undefined;
                var input: [128]usize = undefined;
                var count: usize = 0;
                for (ids[offset..end], 0..) |candidate, position| {
                    if (done[position] or self.pageStart(candidate) != first) continue;
                    done[position] = true;
                    target[count] = candidate - first;
                    input[count] = if (indices) |selected| selected[offset + position] else begin + offset + position;
                    count += 1;
                }
                var view = try self.pageWindow(id, access);
                defer view.lease.release();
                try view.column.mergeSelectedBatchFor(a, target[0..count], source, 0, input[0..count], access);
            }
            offset = end;
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
        return self.mergeExactControlled(a, index, state, null);
    }
    pub fn mergeExactControlled(self: *Column, a: A, index: usize, state: operators.Aggregate, control: ?Work.Control) !void {
        return self.mergeExactFor(a, index, state, .{ .control = control });
    }
    pub fn mergeExactFor(self: *Column, a: A, index: usize, state: operators.Aggregate, access: Access) !void {
        const control = access.control;
        try Work.poll(control);
        if (self.paged != null) {
            var view = try self.pageView(index, true, access);
            defer view.lease.release();
            return view.column.mergeExactFor(a, 0, state, access);
        }
        if (state.kind != self.spec.kind or state.input_type != self.spec.input_type or state.input_element != self.spec.input_element or state.distinct != self.spec.distinct) return error.InvalidSqlBackendResponse;
        if (self.values == .exact) {
            const reducer = state.numeric orelse return error.InvalidSqlBackendResponse;
            if (reducer.state.count != state.count) return error.InvalidSqlBackendResponse;
            return self.values.exact.items[index].mergeControlled(reducer.state, control);
        }
        if (self.values == .dynamic) return @import("aggregate_partial.zig").mergeControlled(&self.values.dynamic.items[index], state, control);
        if (self.spec.kind == .min or self.spec.kind == .max) {
            if (state.count == 0) return;
            const prior = switch (self.values) {
                .counts, .dynamic, .exact => unreachable,
                inline else => |values| values.items[index].count,
            };
            const total = std.math.add(u64, prior, state.count) catch return error.SqlNumericOutOfRange;
            if (total > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
            try self.updateFor(a, index, state.selected.?.row.values[0], access);
            switch (self.values) {
                .counts => unreachable,
                .exact => unreachable,
                .dynamic => |*values| values.items[index].count = total,
                inline else => |*values| values.items[index].count = total,
            }
            return;
        }
        if (self.values == .numbers) {
            var target = try self.snapshotFor(a, index, access);
            try @import("aggregate_partial.zig").mergeControlled(&target, state, control);
            try self.setFor(index, target, access);
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
        return self.finishControlled(index, null);
    }
    pub fn finishControlled(self: *const Column, index: usize, control: ?Work.Control) !Datum {
        return self.finishFor(index, .{ .control = control });
    }
    pub fn finishFor(self: *const Column, index: usize, access: Access) !Datum {
        const control = access.control;
        try Work.poll(control);
        if (self.paged != null) return (try self.snapshotFor(std.heap.page_allocator, index, access)).finishControlled(control);
        if (self.values == .fixed) return self.values.fixed.items[index].finish();
        if (self.values == .exact) return if (try self.values.exact.items[index].finishControlled(self.spec.kind == .avg, control)) |value| Datum.typedNumeric(value) else .{};
        if (self.values == .dynamic) return self.values.dynamic.items[index].finishControlled(control);
        if (self.spec.kind == .min or self.spec.kind == .max) return switch (self.values) {
            .integers => |v| if (v.items[index].count == 0) .{} else Datum.json(.{ .integer = v.items[index].selected }),
            .numbers => |v| if (v.items[index].count == 0) .{} else Datum.json(.{ .float = v.items[index].selected }),
            .booleans => |v| if (v.items[index].count == 0) .{} else Datum.json(.{ .bool = v.items[index].value }),
            .bytes => |v| if (v.items[index].value) |bytes| Datum.json(.{ .string = bytes }) else .{},
            else => unreachable,
        };
        return (try self.snapshotFor(std.heap.page_allocator, index, access)).finishControlled(control);
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

fn physicalKernelScenario(a: A) !void {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const alloc = scratch.allocator();
    const Batch = @import("execution_batch.zig").Batch;
    const Vector = @import("../storage/rowsource/types.zig").ColumnVector;
    const vectors = [_]Vector{
        .{ .name = "n", .values = .{ .i64 = &.{ std.math.maxInt(i64), -7, 0, std.math.minInt(i64) } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ std.math.maxInt(i64), -7, std.math.minInt(i64) }, .indices = &.{ 0, 1, 99, 2 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        .{ .name = "n", .values = .{ .bool = &.{ true, false, true, false } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        .{ .name = "n", .values = .{ .dictionary_bytes = .{ .values = &.{ "zeta", "alpha", "beta" }, .indices = &.{ 0, 1, 99, 2 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        .{ .name = "n", .values = .{ .f64 = &.{ 9007199254740992, 1, 0, -9007199254740992 } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
    };
    for (vectors) |vector| {
        const kind: @import("ast.zig").ColumnType = switch (vector.values) {
            .i64, .dictionary_i64 => .integer,
            .bool => .boolean,
            .dictionary_bytes => .string,
            else => .number,
        };
        const definitions = [_]@import("scalar.zig").Column{.{ .name = "n", .type = kind }};
        const source: Batch = .{ .columns = .{ .definitions = &definitions, .page = .{
            .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" }, .{ .relational_key = "d" } }, .columns = &.{vector} },
            .selection = &.{ 3, 2, 0, 1, 0 },
        } } };
        const projected = try source.project(alloc, 0);
        const selected = try projected.select(alloc, &.{ 0, 1, 2, 3, 4, 2 });
        try std.testing.expect((try selected.physicalColumn(alloc, 0)) != null);
        const ids = [_]usize{ 0, 1, 0, 1, 0, 1 };
        for ([_]operators.Aggregate.Kind{ .count, .sum, .avg, .min, .max, .bool_and, .bool_or }) |operation| {
            if ((operation == .sum or operation == .avg) and kind != .integer and kind != .number) continue;
            if ((operation == .bool_and or operation == .bool_or) and kind != .boolean) continue;
            const spec: operators.AggregateSpec = .{ .kind = operation, .input_type = kind };
            var native = Column.init(spec);
            defer native.clear(alloc);
            var scalar_state = Column.init(spec);
            defer scalar_state.clear(alloc);
            for (0..2) |_| {
                try native.append(alloc);
                try scalar_state.append(alloc);
            }
            try native.updateEncoded(alloc, &ids, selected);
            for (ids, 0..) |id, index| try scalar_state.update(alloc, id, try selected.cell(alloc, index, 0));
            for (0..2) |index| {
                // Compare wide transition states before integer final narrowing.
                const actual = try native.snapshot(alloc, index);
                const expected = try scalar_state.snapshot(alloc, index);
                try std.testing.expectEqual(expected.count, actual.count);
                try std.testing.expectEqual(expected.integer_sum, actual.integer_sum);
                try std.testing.expectEqual(@as(u64, @bitCast(expected.number_sum)), @as(u64, @bitCast(actual.number_sum)));
                try std.testing.expectEqual(@as(u64, @bitCast(expected.compensation)), @as(u64, @bitCast(actual.compensation)));
                if (!(operation == .sum and kind == .integer)) try std.testing.expectEqualDeep(try expected.finish(), try actual.finish());
            }
        }
    }
}
test "SQL physical aggregate kernels preserve selections null dictionaries and exact transitions" {
    try physicalKernelScenario(std.testing.allocator);
    var column = Column.init(.{ .kind = .sum, .input_type = .integer });
    defer column.clear(std.testing.allocator);
    try column.append(std.testing.allocator);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    try std.testing.expect(try column.updatePhysical(none.allocator(), &.{ 0, 0, 0 }, .{
        .kind = .integer,
        .selection = &.{ 2, 0, 1 },
        .vector = .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 7, -3 }, .indices = &.{ 0, 1, 99 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } },
    }));
    try std.testing.expectEqual(@as(i64, 4), (try column.finish(0)).value.integer);
}

fn physicalBytesFailure(a: A) !void {
    var column = Column.init(.{ .kind = .min, .input_type = .string });
    defer column.clear(a);
    try column.append(a);
    try std.testing.expect(try column.updatePhysical(a, &.{ 0, 0, 0 }, .{
        .kind = .string,
        .selection = &.{ 0, 1, 2 },
        .vector = .{ .name = "n", .values = .{ .bytes = &.{ "zeta", "alpha", "beta" } } },
    }));
    try std.testing.expectEqualStrings("alpha", (try column.finish(0)).value.string);
}
test "SQL physical aggregate byte winners unwind allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, physicalBytesFailure, .{});
}

test "SQL physical floating extrema retain NaN ordering and signed zero ties" {
    const a = std.testing.allocator;
    for ([_]operators.Aggregate.Kind{ .min, .max }) |kind| {
        var column = Column.init(.{ .kind = kind, .input_type = .number, .input_element = .float64 });
        defer column.clear(a);
        try column.append(a);
        var none = std.heap.FixedBufferAllocator.init(&.{});
        try std.testing.expect(try column.updatePhysical(none.allocator(), &.{ 0, 0, 0, 0 }, .{
            .kind = .number,
            .element = .float64,
            .selection = &.{ 0, 1, 2, 3 },
            .vector = .{ .name = "f", .values = .{ .f64 = &.{ -0.0, 0.0, std.math.nan(f64), std.math.inf(f64) } } },
        }));
        const value = (try column.finish(0)).value.float;
        if (kind == .max) try std.testing.expect(std.math.isNan(value)) else try std.testing.expectEqual(@as(u64, @bitCast(@as(f64, -0.0))), @as(u64, @bitCast(value)));
    }
}

test "SQL decimal coefficient aggregates widen without per-lane allocations and retain null selections" {
    const a = std.testing.allocator;
    var tracked = std.testing.FailingAllocator.init(a, .{});
    const alloc = tracked.allocator();
    var selection: [1024]usize = undefined;
    for (&selection, 0..) |*row_index, index| row_index.* = if (index % 7 == 0) 2 else index % 2;
    const source: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "null" } }, .columns = &.{.{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{ 99999999999999999999999999999999999999, 99999999999999999999999999999999999999, std.math.minInt(i128) }, .precision = 38, .scale = 4 } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } }} }, .selection = &selection };
    const batch: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = source, .definitions = &.{.{ .name = "n", .type = .number, .element_type = .numeric }} } };
    var state = Column.init(.{ .kind = .sum, .input_type = .number, .input_element = .numeric });
    defer state.clear(alloc);
    try state.append(alloc);
    var warm = batch;
    warm.columns.page.selection = &.{0};
    try state.updateEncoded(alloc, &.{0}, warm);
    const before = tracked.alloc_index;
    const ids: [1024]usize = @splat(0);
    try state.updateEncoded(alloc, &ids, batch);
    try std.testing.expectEqual(before, tracked.alloc_index);
    const result = try state.finish(0);
    var context: @import("numeric_value.zig").Context = .{ .alloc = a };
    const text = try @import("numeric_value.zig").format(&context, result.numeric.?.*);
    defer a.free(text);
    try std.testing.expectEqualStrings("8779999999999999999999999999999999999.9122", text);
}

fn nativeExtremaScenario(a: A) !void {
    const batch = blk: {
        const values = try a.dupe(i128, &.{ -123, 12300, -12000, 0 });
        errdefer a.free(values);
        const nulls = try a.dupe(u8, &.{ 0, 0, 0, 1 });
        errdefer a.free(nulls);
        const scales = try a.dupe(u16, &.{ 2, 4, 3, 0 });
        errdefer a.free(scales);
        break :blk try @import("execution_batch.zig").Batch.fromDecimalScales(a, values, nulls, 2, scales);
    };
    defer batch.reader.free_owned.?(batch.reader.ptr, a);
    for ([_]operators.Aggregate.Kind{ .count, .min, .max }) |kind| {
        const spec: operators.AggregateSpec = .{ .kind = kind, .input_type = .number, .input_element = .numeric };
        var column = Column.init(spec);
        defer column.clear(a);
        try column.append(a);
        var none: [0]u8 = .{};
        var denied = std.heap.FixedBufferAllocator.init(&none);
        // Admitted native updates allocate no canonical limb cell per row.
        try column.updateEncoded(denied.allocator(), &.{ 0, 0, 0, 0 }, batch);
        if (kind == .count) {
            try std.testing.expectEqual(@as(i64, 3), (try column.finish(0)).value.integer);
            continue;
        }
        try std.testing.expect(column.values == .fixed);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const sa = arena.allocator();
        var context: @import("numeric_value.zig").Context = .{ .alloc = sa };
        try std.testing.expectEqualStrings(if (kind == .min) "-12.000" else "1.2300", try @import("numeric_value.zig").format(&context, (try column.finish(0)).numeric.?.*));
        var merged = Column.init(spec);
        defer merged.clear(a);
        try merged.append(a);
        try merged.mergeBatch(a, &.{0}, &column, 0);
        try std.testing.expect(merged.values == .fixed);
        var work: @import("array_value.zig").Budget = .{};
        try merged.update(a, 0, try @import("scalar.zig").numericTextLeaky(sa, "1e100", &work));
        try std.testing.expect(merged.values == .dynamic);
    }
}
test "lake SQL native decimal COUNT and extrema retain scales merge typed state and promote wide values" {
    try nativeExtremaScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, nativeExtremaScenario, .{});
}

test "SQL execution pager grouping preserves compact cohort lane identities" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    var column = Column.init(.{ .kind = .count });
    column.page_manager = &manager;
    defer column.clear(a);
    for (0..3000) |_| try column.append(a);
    try std.testing.expect(column.paged != null);
    var ids: [32]usize = undefined;
    for (&ids, 0..) |*id, lane| id.* = 2999 - lane;
    const source: @import("execution_batch.zig").Batch = .{ .rows = &.{ &.{Datum.json(.{ .integer = 1 })}, &.{Datum.json(.{ .integer = 1 })} } };
    try column.updateCohort(a, &ids, source, &.{ 4, 7 });
    try std.testing.expectEqual(@as(i64, 1), (try column.finish(2995)).value.integer);
    try std.testing.expectEqual(@as(i64, 1), (try column.finish(2992)).value.integer);
    try std.testing.expectEqual(@as(i64, 0), (try column.finish(2999)).value.integer);
}

test "SQL paged native kernels pin each reached state page once per tile" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    var column = Column.init(.{ .kind = .sum, .input_type = .number });
    column.page_manager = &manager;
    defer column.clear(a);
    for (0..3000) |_| try column.append(a);
    var ids: [128]usize = undefined;
    var cells: [128]Datum = undefined;
    for (&ids, &cells, 0..) |*id, *cell, lane| {
        id.* = if (lane % 2 == 0) 0 else 2999;
        cell.* = Datum.json(.{ .float = switch (lane % 3) {
            0 => 1e16,
            1 => 1,
            else => -1e16,
        } });
    }
    const pager = manager.execution_pages.?;
    const before = pager.pins;
    try column.updateBatch(a, &ids, &cells);
    try std.testing.expectEqual(@as(u64, 2), pager.pins - before);
    var expected = [_]operators.Aggregate{
        try operators.Aggregate.init(a, .sum, .number), try operators.Aggregate.init(a, .sum, .number),
    };
    defer for (&expected) |*state| state.deinit();
    for (cells, 0..) |cell, lane| try expected[lane % 2].update(cell);
    try std.testing.expectEqualDeep(try expected[0].finish(), try column.finish(0));
    try std.testing.expectEqualDeep(try expected[1].finish(), try column.finish(2999));
}

test "SQL paged partial merges gather source pages before destination kernels" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    for ([_]operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .sum, .input_type = .number }, .{ .kind = .avg, .input_type = .number }, .{ .kind = .count }, .{ .kind = .bool_or, .input_type = .boolean } }) |spec| {
        var source = Column.init(spec);
        source.page_manager = &manager;
        defer source.clear(a);
        var target = Column.init(spec);
        target.page_manager = &manager;
        defer target.clear(a);
        var reference = Column.init(spec);
        defer reference.clear(a);
        for (0..3000) |_| {
            try source.append(a);
            try target.append(a);
        }
        try reference.append(a);
        try reference.append(a);
        const pager = manager.execution_pages.?;
        pager.limit = Column.paging.page_bytes;
        const values = [_]Datum{ if (spec.input_type == .boolean) Datum.json(.{ .bool = true }) else Datum.json(.{ .integer = 17 }), if (spec.input_type == .boolean) Datum.json(.{ .bool = false }) else Datum.json(.{ .integer = -3 }) };
        try source.update(a, 0, values[0]);
        try source.update(a, 2999, values[1]);
        var ids: [128]usize = undefined;
        var selected: [128]usize = undefined;
        for (&ids, &selected, 0..) |*id, *from, i| {
            id.* = if (i % 2 == 0) 2999 else 0;
            from.* = if (i % 3 == 0) 0 else 2999;
            try reference.update(a, i % 2, values[if (i % 3 == 0) 0 else 1]);
        }
        var none = std.heap.FixedBufferAllocator.init(&.{});
        const before = pager.pins;
        try target.mergeSelectedBatch(none.allocator(), &ids, &source, 0, &selected);
        try std.testing.expectEqual(@as(u64, 4), pager.pins - before);
        try std.testing.expectEqualDeep(try reference.finish(0), try target.finish(2999));
        try std.testing.expectEqualDeep(try reference.finish(1), try target.finish(0));
        if (spec.input_type == .integer) {
            const fixed: [128]@import("aggregate_partial.zig").Fixed = @splat(.{ .count = 1, .sum = 5, .boolean = false, .selected = .{} });
            const pinned = pager.pins;
            try target.mergeFixedBatch(none.allocator(), &ids, &fixed);
            try std.testing.expectEqual(@as(u64, 2), pager.pins - pinned);
        }
    }
}
