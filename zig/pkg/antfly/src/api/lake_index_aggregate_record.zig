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

//! Schema-bound native transitions. Fixed offsets are compiled once from the
//! authenticated recipe. Only variable fields carry offsets; projecting one
//! primitive state requires no column descriptors or reducer allocations.
const std = @import("std");
const local = @import("antfly_local_sources");
const partial = local.sql_aggregate_partial;
const ops = local.sql_operators;
const Datum = local.sql_scalar.Datum;
const Recipe = local.sql_aggregate_materialization.Recipe;
const A = std.mem.Allocator;
const magic = "AGR2";
const limit = 4 * 1024 * 1024;
const Kind = enum { lanes, integer, floating, boolean, text, scalar, numeric };
const Slot = struct { offset: usize, variable: ?usize = null, kind: Kind, spec: ops.AggregateSpec };
pub const Layout = struct {
    recipe: Recipe,
    slots: []const Slot,
    fixed_bytes: usize,
    variables: usize,
    pub fn init(a: A, recipe: Recipe) !Layout {
        if (recipe.inputs.len > 256) return error.InvalidNativeAggregateArtifact;
        const slots = try a.alloc(Slot, recipe.inputs.len);
        errdefer a.free(slots);
        var fixed: usize = 0;
        var variables: usize = 1; // Original grouping key presentation.
        for (recipe.inputs, slots) |input, *slot| {
            const spec = input.spec;
            if (!partial.physicalEligible(spec)) return error.InvalidNativeAggregateArtifact;
            const kind: Kind = if (spec.kind == .min or spec.kind == .max) switch (spec.input_type orelse .json) {
                .integer => .integer,
                .boolean => .boolean,
                .string, .datetime, .uuid => .text,
                .number => if (spec.input_element == .float32 or spec.input_element == .float64) .floating else .scalar,
                else => .scalar,
            } else if (spec.input_element == .numeric and (spec.kind == .sum or spec.kind == .avg)) .numeric else .lanes;
            slot.* = .{ .offset = fixed, .kind = kind, .spec = spec };
            switch (kind) {
                .lanes => for (partial.physicalColumns(spec)) |lane| {
                    fixed += if (lane == 6) @as(usize, 1) else 8;
                },
                .integer, .floating => fixed += 16,
                .boolean => fixed += 9,
                .text, .scalar, .numeric => {
                    fixed += 8;
                    slot.variable = variables;
                    variables += 1;
                },
            }
        }
        return .{ .recipe = recipe, .slots = slots, .fixed_bytes = fixed, .variables = variables };
    }
    pub fn deinit(self: Layout, a: A) void {
        a.free(self.slots);
    }
    fn header(self: Layout) usize {
        return 4 + 4 * (self.variables + 1);
    }
};
fn offset(bytes: []u8, ordinal: usize, value: usize) void {
    std.mem.writeInt(u32, bytes[4 + ordinal * 4 ..][0..4], @intCast(value), .little);
}
fn putWord(bytes: []u8, position: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[position..][0..8], value, .little);
}
pub fn encode(a: A, group: ops.GroupResult, recipe: Recipe) ![]u8 {
    const layout = try Layout.init(a, recipe);
    defer layout.deinit(a);
    return encodeWithLayout(a, group, layout);
}
fn writeCells(a: A, bytes: *std.ArrayList(u8), slot: Slot, cells: [partial.physical_width]Datum, layout: Layout) !void {
    var position = layout.header() + slot.offset;
    switch (slot.kind) {
        .lanes => for (partial.physicalColumns(slot.spec)) |lane| {
            if (lane == 6) {
                bytes.items[position] = @intFromBool(cells[lane].value.bool);
                position += 1;
            } else {
                putWord(bytes.items, position, @bitCast(cells[lane].value.integer));
                position += 8;
            }
        },
        else => {
            putWord(bytes.items, position, @bitCast(cells[0].value.integer));
            position += 8;
            if (cells[0].value.integer != 0) switch (slot.kind) {
                .integer => putWord(bytes.items, position, @bitCast(cells[7].value.integer)),
                .floating => {
                    if (cells[7].value != .float) return error.InvalidSqlSpill;
                    putWord(bytes.items, position, @bitCast(cells[7].value.float));
                },
                .boolean => {
                    bytes.items[position] = @intFromBool(cells[7].value.bool);
                },
                .text => try bytes.appendSlice(a, cells[7].value.string),
                .scalar => {
                    const value = try local.sql_spill.encodeDatumsAlloc(a, &.{cells[7]}, limit);
                    defer a.free(value);
                    try bytes.appendSlice(a, value);
                },
                .numeric => {},
                .lanes => unreachable,
            };
            // Empty exact reducers also have a portable coefficient state.
            if (slot.kind == .numeric) try bytes.appendSlice(a, cells[8].value.string);
            if (slot.variable) |variable| {
                if (bytes.items.len > limit) return error.SqlProgramLimitExceeded;
                offset(bytes.items, variable + 1, bytes.items.len);
            }
        },
    }
    if (bytes.items.len > limit) return error.SqlProgramLimitExceeded;
}
fn finishRecord(a: A, bytes: *std.ArrayList(u8)) ![]u8 {
    // Allocators may refuse remap even when the length is unchanged. Transfer
    // an exactly sized record directly instead of allocating a second copy.
    if (bytes.items.len == bytes.capacity) {
        const result = bytes.items;
        bytes.* = .empty;
        return result;
    }
    return bytes.toOwnedSlice(a);
}
fn beginRecord(a: A, layout: Layout, keys: []const u8) !std.ArrayList(u8) {
    var bytes = std.ArrayList(u8).fromOwnedSlice(try a.alloc(u8, layout.header() + layout.fixed_bytes + keys.len));
    bytes.clearRetainingCapacity();
    errdefer bytes.deinit(a);
    try bytes.appendNTimes(a, 0, layout.header() + layout.fixed_bytes);
    @memcpy(bytes.items[0..4], magic);
    offset(bytes.items, 0, bytes.items.len);
    try bytes.appendSlice(a, keys);
    offset(bytes.items, 1, bytes.items.len);
    return bytes;
}
pub fn encodeWithLayout(a: A, group: ops.GroupResult, layout: Layout) ![]u8 {
    const recipe = layout.recipe;
    if (group.keys.len != recipe.keys.len or try partial.partialCount(group) != recipe.inputs.len) return error.InvalidNativeAggregateArtifact;
    const keys = try local.sql_spill.encodeDatumsAlloc(a, group.keys, limit);
    defer a.free(keys);
    var bytes = try beginRecord(a, layout, keys);
    errdefer bytes.deinit(a);
    var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    for (layout.slots, 0..) |slot, index| {
        _ = scratch.reset(.free_all);
        const work = scratch.allocator();
        const state = try partial.read(work, group, index, slot.spec);
        try writeCells(a, &bytes, slot, try partial.physicalCells(work, state), layout);
    }
    return finishRecord(a, &bytes);
}
/// Fixed transition kernels work in stack lanes. Variable/exact reducers retain
/// the shared validated merge semantics in one reclaimable slot workspace.
/// A fixed cohort needs only its output record allocation, regardless of width.
pub fn merge(a: A, left_bytes: []const u8, right_bytes: []const u8, layout: *const Layout) ![]u8 {
    const left = try View.init(left_bytes, layout);
    const right = try View.init(right_bytes, layout);
    var bytes = try beginRecord(a, layout.*, left.field(0));
    errdefer bytes.deinit(a);
    var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    for (layout.slots, 0..) |slot, index| {
        _ = scratch.reset(.free_all);
        const work = scratch.allocator();
        var l = try left.cells(work, index);
        const r = try right.cells(work, index);
        if (l[0].value.integer < 0 or r[0].value.integer < 0) return error.InvalidSqlSpill;
        const count = std.math.add(i64, l[0].value.integer, r[0].value.integer) catch return error.SqlNumericOutOfRange;
        const fixed = slot.kind == .lanes and (slot.spec.kind == .count or (slot.spec.kind == .sum and slot.spec.input_type == .integer) or slot.spec.kind == .bool_and or slot.spec.kind == .bool_or);
        if (fixed) {
            if (r[0].value.integer != 0) switch (slot.spec.kind) {
                .sum => {
                    const lv: i128 = @bitCast(@as(u128, @as(u64, @bitCast(l[2].value.integer))) << 64 | @as(u64, @bitCast(l[1].value.integer)));
                    const rv: i128 = @bitCast(@as(u128, @as(u64, @bitCast(r[2].value.integer))) << 64 | @as(u64, @bitCast(r[1].value.integer)));
                    const value = std.math.add(i128, lv, rv) catch return error.SqlNumericOutOfRange;
                    const bits: u128 = @bitCast(value);
                    l[1] = Datum.json(.{ .integer = @bitCast(@as(u64, @truncate(bits))) });
                    l[2] = Datum.json(.{ .integer = @bitCast(@as(u64, @truncate(bits >> 64))) });
                },
                .bool_and => l[6] = Datum.json(.{ .bool = l[6].value.bool and r[6].value.bool }),
                .bool_or => l[6] = Datum.json(.{ .bool = l[6].value.bool or r[6].value.bool }),
                .count => {},
                else => unreachable,
            };
            l[0] = Datum.json(.{ .integer = count });
        } else if (slot.kind == .integer or slot.kind == .floating or slot.kind == .boolean or slot.kind == .text) {
            if (r[0].value.integer != 0) {
                const order = if (l[0].value.integer == 0) std.math.Order.eq else try local.sql_scalar.compareDatums(r[7], l[7]);
                if (l[0].value.integer == 0 or (slot.spec.kind == .min and order == .lt) or (slot.spec.kind == .max and order == .gt)) l[7] = r[7];
            }
            l[0] = Datum.json(.{ .integer = count });
        } else {
            var state = try partial.readPhysicalCells(work, l, slot.spec);
            try partial.merge(&state, try partial.readPhysicalCells(work, r, slot.spec));
            l = try partial.physicalCells(work, state);
        }
        try writeCells(a, &bytes, slot, l, layout.*);
    }
    return finishRecord(a, &bytes);
}
pub const View = struct {
    bytes: []const u8,
    layout: *const Layout,
    pub fn init(bytes: []const u8, layout: *const Layout) !View {
        const header = layout.header();
        if (bytes.len < header + layout.fixed_bytes or bytes.len > limit or !std.mem.eql(u8, bytes[0..4], magic)) return error.InvalidNativeAggregateArtifact;
        var previous: usize = header + layout.fixed_bytes;
        for (0..layout.variables + 1) |slot| {
            const value = std.mem.readInt(u32, bytes[4 + slot * 4 ..][0..4], .little);
            if ((slot == 0 and value != previous) or value < previous or value > bytes.len) return error.InvalidNativeAggregateArtifact;
            previous = value;
        }
        if (previous != bytes.len) return error.InvalidNativeAggregateArtifact;
        return .{ .bytes = bytes, .layout = layout };
    }
    fn field(self: View, slot: usize) []const u8 {
        const begin = std.mem.readInt(u32, self.bytes[4 + slot * 4 ..][0..4], .little);
        const end = std.mem.readInt(u32, self.bytes[8 + slot * 4 ..][0..4], .little);
        return self.bytes[begin..end];
    }
    pub fn keys(self: View, a: A) ![]const Datum {
        const result = try local.sql_spill.decodeDatumsInArena(a, self.field(0), limit);
        if (result.len != self.layout.recipe.keys.len) return error.InvalidNativeAggregateArtifact;
        return result;
    }
    pub fn cells(self: View, a: A, index: usize) ![partial.physical_width]Datum {
        if (index >= self.layout.slots.len) return error.InvalidNativeAggregateArtifact;
        const slot = self.layout.slots[index];
        var result: [partial.physical_width]Datum = @splat(.{});
        @memset(result[0..6], Datum.json(.{ .integer = 0 }));
        result[6] = Datum.json(.{ .bool = false });
        var position = self.layout.header() + slot.offset;
        if (slot.kind == .lanes) {
            for (partial.physicalColumns(slot.spec)) |lane| {
                if (lane == 6) {
                    if (self.bytes[position] > 1) return error.InvalidNativeAggregateArtifact;
                    result[lane] = Datum.json(.{ .bool = self.bytes[position] == 1 });
                    position += 1;
                } else {
                    result[lane] = Datum.json(.{ .integer = @bitCast(std.mem.readInt(u64, self.bytes[position..][0..8], .little)) });
                    position += 8;
                }
            }
            return result;
        }
        result[0] = Datum.json(.{ .integer = @bitCast(std.mem.readInt(u64, self.bytes[position..][0..8], .little)) });
        position += 8;
        const variable = if (slot.variable) |ordinal| self.field(ordinal) else "";
        if (slot.kind == .numeric) {
            result[8] = Datum.json(.{ .string = variable });
            return result;
        }
        if (result[0].value.integer == 0) {
            if (variable.len != 0) return error.InvalidNativeAggregateArtifact;
            return result;
        }
        result[7] = switch (slot.kind) {
            .integer => Datum.json(.{ .integer = @bitCast(std.mem.readInt(u64, self.bytes[position..][0..8], .little)) }),
            .floating => Datum.json(.{ .float = @bitCast(std.mem.readInt(u64, self.bytes[position..][0..8], .little)) }),
            .boolean => boolean: {
                if (self.bytes[position] > 1) return error.InvalidNativeAggregateArtifact;
                break :boolean Datum.json(.{ .bool = self.bytes[position] == 1 });
            },
            .text => Datum.json(.{ .string = variable }),
            .scalar => scalar: {
                const values = try local.sql_spill.decodeDatumsInArena(a, variable, limit);
                if (values.len != 1) return error.InvalidNativeAggregateArtifact;
                break :scalar values[0];
            },
            else => unreachable,
        };
        return result;
    }
    pub fn read(self: View, a: A, slot: usize) !ops.Aggregate {
        if (slot >= self.layout.slots.len) return error.InvalidNativeAggregateArtifact;
        return partial.readPhysicalCells(a, try self.cells(a, slot), self.layout.slots[slot].spec);
    }
    pub fn finish(self: View, a: A, slot: usize, spec: ops.AggregateSpec) !Datum {
        if (slot >= self.layout.slots.len or !std.meta.eql(spec, self.layout.slots[slot].spec)) return error.InvalidSqlBackendResponse;
        return partial.finishPhysicalCells(a, try self.cells(a, slot), spec);
    }
};
test "external lake aggregate compact records share recipes and finalize selected counters without allocations" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var inputs: [64]local.sql_aggregate_materialization.Input = undefined;
    var specs: [64]ops.AggregateSpec = undefined;
    var datums: [64]Datum = undefined;
    for (&inputs, &specs, &datums) |*input, *spec, *datum| {
        spec.* = .{ .kind = .count };
        input.* = .{ .spec = spec.*, .column = null };
        datum.* = Datum.json(.{ .integer = 1 });
    }
    const recipe: Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = false }}, .inputs = &inputs };
    const group = try ops.Grouped.create(a, &specs, .{});
    defer group.deinit();
    try group.add(&.{Datum.json(.{ .integer = 42 })}, &datums);
    const source = (try group.nextTypedPartialResult(out)).?;
    const encoded = try encode(out, source, recipe);
    const layout = try Layout.init(out, recipe);
    const view = try View.init(encoded, &layout);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectEqual(@as(i64, 1), (try view.finish(failing.allocator(), 63, specs[63])).value.integer);
    var one_allocation = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    const combined = try merge(one_allocation.allocator(), encoded, encoded, &layout);
    defer one_allocation.allocator().free(combined);
    const doubled = try View.init(combined, &layout);
    for (0..64) |i| try std.testing.expectEqual(@as(i64, 2), (try doubled.finish(failing.allocator(), i, specs[i])).value.integer);
    try std.testing.expectEqual(@as(usize, 1), one_allocation.alloc_index);
    std.debug.print("compact aggregate merge: counters=64 allocations={}\n", .{one_allocation.alloc_index});
    const original = try out.alloc(Datum, inputs.len);
    @memset(original, Datum.json(.{ .integer = 1 }));
    const old = try local.sql_spill.encodeColumnarBlockAlloc(out, &.{.{ .keys = source.keys, .values = original, .ordinal = 0 }}, limit);
    try std.testing.expect(encoded.len < old.len);
    std.debug.print("compact aggregate counters: old_bytes={} native_bytes={}\n", .{ old.len, encoded.len });
    for (0..encoded.len) |end| try std.testing.expectError(error.InvalidNativeAggregateArtifact, View.init(encoded[0..end], &layout));
    const corrupt = try out.dupe(u8, encoded);
    std.mem.writeInt(u32, corrupt[8..12], 0, .little);
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, View.init(corrupt, &layout));
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, view.read(out, inputs.len));
}

test "external lake aggregate NUMERIC finalization reclaims scratch within its output page budget" {
    const a = std.testing.allocator;
    var source = try ops.Aggregate.initTyped(a, .sum, .number, .numeric);
    defer source.deinit();
    const numeric = local.sql_numeric_value;
    var context: numeric.Context = .{ .alloc = a };
    var value = try numeric.parse(&context, "9007199254740993.1200");
    defer value.deinit();
    try source.update(Datum.typedNumeric(&value.value));
    var initial = std.heap.ArenaAllocator.init(a);
    defer initial.deinit();
    const recipe: Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = .{ .kind = .sum, .input_type = .number, .input_element = .numeric }, .column = .{ .path = "n", .type = .number, .nullable = true, .element_type = .numeric } }} };
    const layout = try Layout.init(initial.allocator(), recipe);
    const encoded = try encodeWithLayout(initial.allocator(), .{ .keys = &.{}, .aggregates = &.{}, .typed_states = &.{source}, .ordinal = 0 }, layout);
    const view = try View.init(encoded, &layout);
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 4 * 1024 * 1024 };
    var page = std.heap.ArenaAllocator.init(budget.allocator());
    defer page.deinit();
    for (0..16384) |_| {
        const output = try view.finish(page.allocator(), 0, recipe.inputs[0].spec);
        try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&context, output.numeric.?.*, value.value));
    }
    try std.testing.expect(budget.peak < 2 * 1024 * 1024);
    std.debug.print("native NUMERIC output: finalized=16384 peak={}\n", .{budget.peak});
}

test "external lake aggregate compact record kernels preserve typed transitions and exact arithmetic" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const n = Datum.json(.{ .integer = -7 });
    const y = Datum.json(.{ .bool = true });
    const no = Datum.json(.{ .bool = false });
    const nan = Datum.json(.{ .float = std.math.nan(f64) });
    var numeric_work: local.sql_array_value.Budget = .{};
    const huge = try local.sql_scalar.numericTextLeaky(out, "9007199254740993.1200", &numeric_work);
    const cases = [_]struct { spec: ops.AggregateSpec, left: []const Datum, right: []const Datum }{
        .{ .spec = .{ .kind = .count }, .left = &.{ n, n }, .right = &.{ n, .{} } },
        .{ .spec = .{ .kind = .sum, .input_type = .integer }, .left = &.{ Datum.json(.{ .integer = std.math.maxInt(i64) }), Datum.json(.{ .integer = 1 }) }, .right = &.{n} },
        .{ .spec = .{ .kind = .sum, .input_type = .integer }, .left = &.{.{}}, .right = &.{n} },
        .{ .spec = .{ .kind = .bool_and, .input_type = .boolean }, .left = &.{y}, .right = &.{no} },
        .{ .spec = .{ .kind = .bool_or, .input_type = .boolean }, .left = &.{}, .right = &.{y} },
        .{ .spec = .{ .kind = .min, .input_type = .integer }, .left = &.{.{}}, .right = &.{n} },
        .{ .spec = .{ .kind = .max, .input_type = .boolean }, .left = &.{no}, .right = &.{y} },
        .{ .spec = .{ .kind = .min, .input_type = .string }, .left = &.{Datum.json(.{ .string = "z" })}, .right = &.{Datum.json(.{ .string = "a" })} },
        .{ .spec = .{ .kind = .max, .input_type = .number, .input_element = .float64 }, .left = &.{Datum.json(.{ .float = -7 })}, .right = &.{nan} },
        .{ .spec = .{ .kind = .sum, .input_type = .number, .input_element = .float32 }, .left = &.{Datum.json(.{ .float = 1.25 })}, .right = &.{Datum.json(.{ .float = -0.25 })} },
        .{ .spec = .{ .kind = .sum, .input_type = .number, .input_element = .float64 }, .left = &.{ Datum.json(.{ .float = 1e20 }), Datum.json(.{ .float = 1 }) }, .right = &.{Datum.json(.{ .float = -1e20 })} },
        .{ .spec = .{ .kind = .avg, .input_type = .number, .input_element = .float64 }, .left = &.{nan}, .right = &.{n} },
        .{ .spec = .{ .kind = .sum, .input_type = .number, .input_element = .numeric }, .left = &.{huge}, .right = &.{huge} },
        .{ .spec = .{ .kind = .avg, .input_type = .number, .input_element = .numeric }, .left = &.{huge}, .right = &.{huge} },
    };
    for (cases) |case| {
        var left = try ops.Aggregate.initTyped(a, case.spec.kind, case.spec.input_type, case.spec.input_element);
        defer left.deinit();
        var right = try ops.Aggregate.initTyped(a, case.spec.kind, case.spec.input_type, case.spec.input_element);
        defer right.deinit();
        for (case.left) |v| try left.update(v);
        for (case.right) |v| try right.update(v);
        const recipe: Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = case.spec, .column = .{ .path = "v", .type = case.spec.input_type orelse .integer, .nullable = true, .element_type = case.spec.input_element } }} };
        const layout = try Layout.init(out, recipe);
        const l = try encodeWithLayout(out, .{ .keys = &.{}, .aggregates = &.{}, .typed_states = &.{left}, .ordinal = 0 }, layout);
        const r = try encodeWithLayout(out, .{ .keys = &.{}, .aggregates = &.{}, .typed_states = &.{right}, .ordinal = 0 }, layout);
        const actual = try merge(out, l, r, &layout);
        try partial.merge(&left, right);
        const expected = try encodeWithLayout(out, .{ .keys = &.{}, .aggregates = &.{}, .typed_states = &.{left}, .ordinal = 0 }, layout);
        try std.testing.expectEqualSlices(u8, expected, actual);
        try std.testing.checkAllAllocationFailures(a, mergeFaultScenario, .{ l, r, &layout, expected });
    }
}

test "external lake compact record kernels reject count and wide integer sum overflow" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    for ([_]ops.AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }) |spec| {
        const recipe: Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = null }} };
        const layout = try Layout.init(out, recipe);
        var left = try ops.Aggregate.initTyped(out, spec.kind, spec.input_type, null);
        var right = try ops.Aggregate.initTyped(out, spec.kind, spec.input_type, null);
        left.count = if (spec.kind == .count) std.math.maxInt(i64) else 1;
        left.integer_sum = if (spec.kind == .sum) std.math.maxInt(i128) else 0;
        try right.update(Datum.json(.{ .integer = 1 }));
        const l = try encodeWithLayout(out, .{ .keys = &.{}, .aggregates = &.{}, .typed_states = &.{left}, .ordinal = 0 }, layout);
        const r = try encodeWithLayout(out, .{ .keys = &.{}, .aggregates = &.{}, .typed_states = &.{right}, .ordinal = 0 }, layout);
        try std.testing.expectError(error.SqlNumericOutOfRange, merge(a, l, r, &layout));
    }
}

fn mergeFaultScenario(a: A, left: []const u8, right: []const u8, layout: *const Layout, expected: []const u8) !void {
    const value = try merge(a, left, right, layout);
    defer a.free(value);
    try std.testing.expectEqualSlices(u8, expected, value);
}
