// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
pub fn encodeWithLayout(a: A, group: ops.GroupResult, layout: Layout) ![]u8 {
    const recipe = layout.recipe;
    if (group.keys.len != recipe.keys.len or try partial.partialCount(group) != recipe.inputs.len) return error.InvalidNativeAggregateArtifact;
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(a);
    const header = layout.header();
    try bytes.appendNTimes(a, 0, header + layout.fixed_bytes);
    @memcpy(bytes.items[0..4], magic);
    offset(bytes.items, 0, bytes.items.len);
    const keys = try local.sql_spill.encodeDatumsAlloc(a, group.keys, limit);
    defer a.free(keys);
    try bytes.appendSlice(a, keys);
    offset(bytes.items, 1, bytes.items.len);
    for (layout.slots, 0..) |slot, index| {
        const state = try partial.read(a, group, index, slot.spec);
        const cells = try partial.physicalCells(a, state);
        var position = header + slot.offset;
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
                if (state.count != 0) switch (slot.kind) {
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
    }
    if (bytes.items.len > limit) return error.SqlProgramLimitExceeded;
    return bytes.toOwnedSlice(a);
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
