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

//! Windows share one sort per partition/order specification. Arbitrary moving
//! aggregate frames use a segment tree, never a quadratic per-row rescan.
//! All input, sort, frame and output state uses the statement memory budget.
const std = @import("std");
const ast = @import("ast.zig");
const binding = @import("window_binding.zig");
const scalar = @import("scalar.zig");
const describe = @import("describe.zig");
const operators = @import("operators.zig");
const Datum = scalar.Datum;
const Json = std.json.Value;
const Allocator = std.mem.Allocator;

const disk = @import("disk_rows.zig");
const Cells = union(enum) {
    memory: []const []Datum,
    disk: *disk.Rows,
    view: *disk.View,
    fn from(value: anytype) Cells {
        return if (@TypeOf(value) == *disk.Rows) .{ .disk = value } else if (@TypeOf(value) == *disk.View) .{ .view = value } else .{ .memory = value };
    }
    fn get(self: Cells, row: usize, column: usize) !Datum {
        return switch (self) {
            .memory => |rows| rows[row][column],
            .disk => |rows| rows.cell(row, column),
            .view => |rows| rows.cell(row, column),
        };
    }
};
fn getCell(rows: anytype, row: usize, column: usize) !Datum {
    return Cells.from(rows).get(row, column);
}
fn at(values: anytype, index: usize) !usize {
    return if (@TypeOf(values) == disk.Identity or @TypeOf(values) == *disk.Integers) values.at(index) else (@as([]const usize, values))[index];
}
fn setCell(rows: anytype, row: usize, column: usize, value: Datum) !void {
    if (comptime disk.isDisk(@TypeOf(rows))) try rows.setCell(row, column, value) else rows[row][column] = value;
}

fn compare(a: Datum, b: Datum, direction: operators.Order) !std.math.Order {
    if (a.sql_null or b.sql_null) {
        if (a.sql_null == b.sql_null) return .eq;
        return if (a.sql_null == (direction.nulls_first orelse direction.descending)) .lt else .gt;
    }
    const result = try scalar.compareDatums(a, b);
    return if (direction.descending) result.invert() else result;
}
pub fn equal(cells: anytype, a: usize, b: usize, columns: []const usize) !bool {
    for (columns) |column| if (try compare(try getCell(cells, a, column), try getCell(cells, b, column), .{}) != .eq) return false;
    return true;
}
fn Sorter(comptime Context: type) type {
    return struct {
        context: Context,
        cells: []const []Datum,
        spec: binding.Sort,
        failure: ?anyerror = null,
        comparisons: usize = 0,
        fn less(self: *@This(), a: usize, b: usize) bool {
            if (self.failure != null) return a < b;
            return self.lessChecked(a, b) catch |err| {
                self.failure = err;
                return a < b;
            };
        }
        fn lessChecked(self: *@This(), a: usize, b: usize) !bool {
            self.comparisons += 1;
            if (self.comparisons % 1024 == 0) try self.context.checkpoint();
            for (self.spec.partition) |column| {
                const order = try compare(self.cells[a][column], self.cells[b][column], .{});
                if (order != .eq) return order == .lt;
            }
            for (self.spec.order, self.spec.directions) |column, direction| {
                const order = try compare(self.cells[a][column], self.cells[b][column], direction);
                if (order != .eq) return order == .lt;
            }
            return a < b;
        }
    };
}

fn integer(value: Datum) !?i64 {
    if (value.sql_null) return null;
    const converted = try describe.coerce(value.value, .integer);
    if (converted != .integer) return error.SqlTypeMismatch;
    return converted.integer;
}
fn offsetValue(context: anytype, value: ast.Value) !usize {
    const raw = if (value == .parameter) blk: {
        if (value.parameter == 0 or value.parameter > context.parameters.len) return error.InvalidSqlParameters;
        break :blk context.parameters[value.parameter - 1];
    } else try describe.bindLiteral(context.arena, value, .integer);
    const number = (try integer(Datum.fromJson(raw))) orelse return error.InvalidSqlParameters;
    if (number < 0) return error.InvalidSqlParameters;
    return std.math.cast(usize, number) orelse error.InvalidSqlParameters;
}
fn rowBoundary(context: anytype, bound: ast.Window.Bound, position: usize, count: usize, end: bool) !usize {
    const inclusive: i128 = @intFromBool(end);
    const pos: i128 = @intCast(position);
    const index: i128 = switch (bound) {
        .unbounded_preceding => 0,
        .unbounded_following => @intCast(count),
        .current => pos + inclusive,
        .preceding => |value| pos - @as(i128, @intCast(try offsetValue(context, value))) + inclusive,
        .following => |value| pos + @as(i128, @intCast(try offsetValue(context, value))) + inclusive,
    };
    return @intCast(@max(0, @min(@as(i128, @intCast(count)), index)));
}

fn rangeBoundary(context: anytype, bound: ast.Window.Bound, cells: anytype, indices: anytype, spec: binding.Sort, position: usize, peer_start: usize, peer_end: usize, end: bool) !usize {
    switch (bound) {
        .unbounded_preceding => return 0,
        .unbounded_following => return indices.len,
        .current => return if (end) peer_end else peer_start,
        else => {},
    }
    if (spec.order.len != 1) return error.UnsupportedSqlShape;
    const distance = try offsetValue(context, switch (bound) {
        .preceding, .following => |value| value,
        else => unreachable,
    });
    const column = spec.order[0];
    const direction = spec.directions[0];
    const current = try getCell(cells, try at(indices, position), column);
    if (current.sql_null) return if (end) peer_end else peer_start;
    if (current.value != .integer and current.value != .float) return error.SqlTypeMismatch;
    const subtract = (bound == .preceding) != direction.descending;
    const integer_target: i128 = if (current.value == .integer) @as(i128, current.value.integer) + (if (subtract) -@as(i128, @intCast(distance)) else @as(i128, @intCast(distance))) else 0;
    const float_target: f64 = if (current.value == .float) current.value.float + (if (subtract) -@as(f64, @floatFromInt(distance)) else @as(f64, @floatFromInt(distance))) else 0;
    var low: usize = 0;
    var high = indices.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const value = try getCell(cells, try at(indices, middle), column);
        const order: std.math.Order = if (value.sql_null)
            (if (direction.nulls_first orelse direction.descending) .lt else .gt)
        else blk: {
            const result = if (current.value == .integer)
                std.math.order(@as(i128, value.value.integer), integer_target)
            else
                std.math.order(value.value.float, float_target);
            break :blk if (direction.descending) result.invert() else result;
        };
        if (order == .lt or (end and order == .eq)) low = middle + 1 else high = middle;
    }
    return low;
}

const Frame = struct { start: usize, end: usize };
fn frame(context: anytype, spec: binding.Spec, sort: binding.Sort, cells: anytype, indices: anytype, position: usize, peer_start: usize, peer_end: usize, groups: anytype, group_index: usize) !Frame {
    const definition = spec.frame orelse ast.Window.Frame{ .mode = .range, .start = .unbounded_preceding, .end = .current };
    const start = if (definition.mode == .rows)
        try rowBoundary(context, definition.start, position, indices.len, false)
    else if (definition.mode == .groups)
        try at(groups, try rowBoundary(context, definition.start, group_index, groups.len - 1, false))
    else
        try rangeBoundary(context, definition.start, cells, indices, sort, position, peer_start, peer_end, false);
    const end = if (definition.mode == .rows)
        try rowBoundary(context, definition.end, position, indices.len, true)
    else if (definition.mode == .groups)
        try at(groups, try rowBoundary(context, definition.end, group_index, groups.len - 1, true))
    else
        try rangeBoundary(context, definition.end, cells, indices, sort, position, peer_start, peer_end, true);
    return .{ .start = start, .end = @max(start, end) };
}

/// Exclusions split a contiguous frame into at most three ordered intervals.
/// Aggregates combine indexed nodes, and value functions select by interval
/// length: neither operation walks the excluded peers or rescans frame rows.
const FrameSet = struct {
    parts: [3]Frame = undefined,
    len: usize = 0,
    fn append(self: *FrameSet, bounds: Frame, start: usize, end: usize) void {
        const clipped = Frame{ .start = @max(bounds.start, start), .end = @min(bounds.end, end) };
        if (clipped.start >= clipped.end) return;
        self.parts[self.len] = clipped;
        self.len += 1;
    }
    fn init(bounds: Frame, exclusion: ast.Window.Exclusion, position: usize, peer_start: usize, peer_end: usize) FrameSet {
        var result: FrameSet = .{};
        switch (exclusion) {
            .no_others => result.append(bounds, bounds.start, bounds.end),
            .current => {
                result.append(bounds, 0, position);
                result.append(bounds, position + 1, bounds.end);
            },
            .group, .ties => {
                result.append(bounds, 0, peer_start);
                if (exclusion == .ties) result.append(bounds, position, position + 1);
                result.append(bounds, peer_end, bounds.end);
            },
        }
        return result;
    }
    fn nth(self: FrameSet, offset: usize) ?usize {
        var remaining = offset;
        for (self.parts[0..self.len]) |part| {
            const count = part.end - part.start;
            if (remaining < count) return part.start + remaining;
            remaining -= count;
        }
        return null;
    }
};

test "SQL excluded frame intervals match every peer and row boundary" {
    const count = 8;
    for (0..count) |position| for (0..position + 1) |peer_start| for (position + 1..count + 1) |peer_end| {
        for (0..count + 1) |start| for (start..count + 1) |end| inline for (std.meta.tags(ast.Window.Exclusion)) |exclusion| {
            const selected = FrameSet.init(.{ .start = start, .end = end }, exclusion, position, peer_start, peer_end);
            var ordinal: usize = 0;
            for (start..end) |row| {
                const excluded = switch (exclusion) {
                    .no_others => false,
                    .current => row == position,
                    .group => row >= peer_start and row < peer_end,
                    .ties => row != position and row >= peer_start and row < peer_end,
                };
                if (!excluded) {
                    try std.testing.expectEqual(@as(?usize, row), selected.nth(ordinal));
                    ordinal += 1;
                }
            }
            try std.testing.expectEqual(null, selected.nth(ordinal));
        };
    };
}

const Node = struct {
    count: u64 = 0,
    integer_sum: i128 = 0,
    // Wider exponent range avoids overflowing an internal tree node whose
    // contributing rows never coexist in a requested (narrower) frame.
    number: f128 = 0,
    compensation: f128 = 0,
    selected: usize = 0,
    true_count: u64 = 0,
};
const Tree = struct {
    nodes: []Node,
    base: usize,
    cells: Cells,
    file: ?*disk.RawCache = null,
    spec: binding.Spec,

    fn create(context: anytype, alloc: Allocator, cells: anytype, indices: anytype, spec: binding.Spec) !Tree {
        const base = try std.math.ceilPowerOfTwo(usize, @max(1, indices.len));
        const count = try std.math.mul(usize, base, 2);
        const nodes: []Node = if (comptime disk.isDisk(@TypeOf(cells))) &.{} else try alloc.alloc(Node, count);
        errdefer alloc.free(nodes);
        @memset(nodes, .{});
        var result = Tree{ .nodes = nodes, .base = base, .cells = Cells.from(cells), .spec = spec };
        if (comptime disk.isDisk(@TypeOf(cells))) {
            var file = try context.spill.?.create();
            errdefer file.close();
            const zeros: [4096]u8 = @splat(0);
            var remaining = try std.math.mul(usize, count, 72);
            while (remaining != 0) {
                const size = @min(remaining, zeros.len);
                try file.writeRaw(file.size, zeros[0..size]);
                remaining -= size;
            }
            result.file = try disk.RawCache.init(alloc, file);
        }
        errdefer if (result.file) |file| file.close();
        for (0..indices.len) |index| {
            const row = try at(indices, index);
            if (index % 256 == 0) try context.checkpoint();
            if (spec.filter) |slot| {
                const accepted = try getCell(cells, row, slot);
                if (accepted.sql_null) continue;
                if (accepted.value != .bool) return error.SqlTypeMismatch;
                if (!accepted.value.bool) continue;
            }
            const value = if (spec.star) Datum.json(.{ .integer = 1 }) else try getCell(cells, row, spec.arguments[0]);
            if (value.sql_null) continue;
            var node = Node{ .count = 1, .selected = row };
            switch (spec.kind) {
                .sum, .avg => switch (value.value) {
                    .integer => |number| node.integer_sum = number,
                    .float => |number| node.number = number,
                    else => return error.SqlTypeMismatch,
                },
                .bool_and, .bool_or => {
                    if (value.value != .bool) return error.SqlTypeMismatch;
                    node.true_count = @intFromBool(value.value.bool);
                },
                else => {},
            }
            try result.setNode(base + index, node);
        }
        var index = base;
        while (index > 1) {
            index -= 1;
            if (index % 256 == 0) try context.checkpoint();
            try result.setNode(index, try result.combine(try result.getNode(2 * index), try result.getNode(2 * index + 1)));
        }
        return result;
    }
    fn deinit(self: *Tree, alloc: Allocator) void {
        if (self.file) |file| file.close();
        alloc.free(self.nodes);
    }
    fn getNode(self: Tree, index: usize) !Node {
        if (self.file) |file| {
            var bytes: [72]u8 = undefined;
            try file.readRaw(index * 72, &bytes);
            return .{ .count = std.mem.readInt(u64, bytes[0..8], .little), .integer_sum = @bitCast(std.mem.readInt(u128, bytes[8..24], .little)), .number = @bitCast(std.mem.readInt(u128, bytes[24..40], .little)), .compensation = @bitCast(std.mem.readInt(u128, bytes[40..56], .little)), .selected = @intCast(std.mem.readInt(u64, bytes[56..64], .little)), .true_count = std.mem.readInt(u64, bytes[64..72], .little) };
        }
        return self.nodes[index];
    }
    fn setNode(self: *Tree, index: usize, node: Node) !void {
        if (self.file) |file| {
            var bytes: [72]u8 = undefined;
            std.mem.writeInt(u64, bytes[0..8], node.count, .little);
            std.mem.writeInt(u128, bytes[8..24], @bitCast(node.integer_sum), .little);
            std.mem.writeInt(u128, bytes[24..40], @bitCast(node.number), .little);
            std.mem.writeInt(u128, bytes[40..56], @bitCast(node.compensation), .little);
            std.mem.writeInt(u64, bytes[56..64], node.selected, .little);
            std.mem.writeInt(u64, bytes[64..72], node.true_count, .little);
            try file.writeRaw(index * 72, &bytes);
        } else self.nodes[index] = node;
    }
    fn combine(self: Tree, left: Node, right: Node) !Node {
        if (left.count == 0) return right;
        if (right.count == 0) return left;
        var result = left;
        result.count += right.count;
        result.integer_sum += right.integer_sum;
        result.true_count += right.true_count;
        switch (self.spec.kind) {
            .min, .max => {
                const order = try scalar.compareDatums(try self.cells.get(left.selected, self.spec.arguments[0]), try self.cells.get(right.selected, self.spec.arguments[0]));
                if (order == (if (self.spec.kind == .min) std.math.Order.gt else .lt)) result.selected = right.selected;
            },
            .avg => {
                // Integer inputs retain an exact wide sum; do not invoke
                // software quad-precision arithmetic for their unused mean.
                if ((try self.cells.get(result.selected, self.spec.arguments[0])).value != .integer) {
                    const left_weight = @as(f128, @floatFromInt(left.count)) / @as(f128, @floatFromInt(result.count));
                    const right_weight = @as(f128, @floatFromInt(right.count)) / @as(f128, @floatFromInt(result.count));
                    result.number = left.number * left_weight + right.number * right_weight;
                }
            },
            .sum => {
                if (self.spec.type != .integer) {
                    for ([_]f128{ right.number, right.compensation }) |value| {
                        const sum = result.number + value;
                        result.compensation += if (@abs(result.number) >= @abs(value)) (result.number - sum) + value else (value - sum) + result.number;
                        result.number = sum;
                    }
                    if (!std.math.isFinite(result.number) or !std.math.isFinite(result.compensation)) return error.SqlNumericOutOfRange;
                }
            },
            else => {},
        }
        return result;
    }
    fn query(self: Tree, bounds: Frame) !Datum {
        return self.querySet(FrameSet.init(bounds, .no_others, 0, 0, 0));
    }
    fn querySet(self: Tree, bounds: FrameSet) !Datum {
        var result: Node = .{};
        for (bounds.parts[0..bounds.len]) |part| result = try self.combine(result, try self.queryNode(part));
        return self.finish(result);
    }
    fn queryNode(self: Tree, bounds: Frame) !Node {
        var left = self.base + bounds.start;
        var right = self.base + bounds.end;
        var result: Node = .{};
        while (left < right) {
            if (left % 2 != 0) {
                result = try self.combine(result, try self.getNode(left));
                left += 1;
            }
            if (right % 2 != 0) {
                right -= 1;
                result = try self.combine(result, try self.getNode(right));
            }
            left /= 2;
            right /= 2;
        }
        return result;
    }
    fn finish(self: Tree, result: Node) !Datum {
        if (self.spec.kind == .count) return Datum.json(.{ .integer = @intCast(result.count) });
        if (result.count == 0) return .{};
        return switch (self.spec.kind) {
            .sum => Datum.json(if (self.spec.type == .integer) .{ .integer = std.math.cast(i64, result.integer_sum) orelse return error.SqlNumericOutOfRange } else .{ .float = try finite(result.number + result.compensation) }),
            .avg => Datum.json(.{ .float = if ((try self.cells.get(result.selected, self.spec.arguments[0])).value == .integer) @as(f64, @floatFromInt(result.integer_sum)) / @as(f64, @floatFromInt(result.count)) else try finite(result.number) }),
            .bool_and => Datum.json(.{ .bool = result.true_count == result.count }),
            .bool_or => Datum.json(.{ .bool = result.true_count != 0 }),
            .min, .max => try self.cells.get(result.selected, self.spec.arguments[0]),
            else => unreachable,
        };
    }
};

fn finite(value: f128) !f64 {
    const result: f64 = @floatCast(value);
    if (!std.math.isFinite(result)) return error.SqlNumericOutOfRange;
    return result;
}

test "SQL window segment tree matches every nullable filtered moving frame" {
    const Context = struct {
        pub fn checkpoint(_: @This()) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var values: [33][2]Datum = undefined;
    var cells: [33][]Datum = undefined;
    var indices: [33]usize = undefined;
    for (&values, &cells, &indices, 0..) |*row, *cell, *index, i| {
        row.* = .{ if (i % 5 == 0) Datum{} else Datum.json(.{ .integer = @as(i64, @intCast(i)) - 16 }), Datum.json(.{ .bool = i % 3 != 0 }) };
        cell.* = row;
        index.* = i;
    }
    for ([_]binding.Kind{ .sum, .count, .min, .max, .avg }) |kind| {
        const tree = try Tree.create(Context{}, arena.allocator(), &cells, &indices, .{ .kind = kind, .arguments = &.{0}, .filter = 1, .sort = 0, .frame = null, .type = if (kind == .avg) .number else .integer, .star = false });
        for (0..34) |start| for (start..34) |end| {
            var count: i64 = 0;
            var sum: i64 = 0;
            var minimum: i64 = std.math.maxInt(i64);
            var maximum: i64 = std.math.minInt(i64);
            for (values[start..end]) |row| {
                if (row[0].sql_null or !row[1].value.bool) continue;
                count += 1;
                sum += row[0].value.integer;
                minimum = @min(minimum, row[0].value.integer);
                maximum = @max(maximum, row[0].value.integer);
            }
            const actual = try tree.query(.{ .start = start, .end = end });
            if (count == 0 and kind != .count) {
                try std.testing.expect(actual.sql_null);
                continue;
            }
            switch (kind) {
                .avg => try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count)), actual.value.float, 1e-12),
                else => try std.testing.expectEqual(switch (kind) {
                    .sum => sum,
                    .count => count,
                    .min => minimum,
                    .max => maximum,
                    else => unreachable,
                }, actual.value.integer),
            }
        };
    }
}

test "SQL window internal aggregate nodes do not reject valid narrow frames" {
    const Context = struct {
        pub fn checkpoint(_: @This()) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for ([_]Datum{ Datum.json(.{ .integer = std.math.maxInt(i64) }), Datum.json(.{ .float = 1e308 }) }) |value| {
        var first = [_]Datum{value};
        var second = [_]Datum{value};
        const cells = [_][]Datum{ &first, &second };
        const tree = try Tree.create(Context{}, arena.allocator(), &cells, &.{ 0, 1 }, .{ .kind = .sum, .arguments = &.{0}, .filter = null, .sort = 0, .frame = null, .type = if (value.value == .integer) .integer else .number, .star = false });
        const one = try tree.query(.{ .start = 0, .end = 1 });
        try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(value.value, one.value));
        try std.testing.expectError(error.SqlNumericOutOfRange, tree.query(.{ .start = 0, .end = 2 }));
    }
}

test "SQL window wide moving frames retain bounded indexed aggregate state" {
    const Context = struct {
        pub fn checkpoint(_: @This()) !void {}
    };
    var budget = @import("memory_budget.zig"){ .backing = std.testing.allocator, .limit = 4 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const count = 10000;
    const cells = try arena.allocator().alloc([]Datum, count);
    const indices = try arena.allocator().alloc(usize, count);
    var value = [_]Datum{Datum.json(.{ .integer = 1 })};
    for (cells, indices, 0..) |*row, *index, i| {
        row.* = &value;
        index.* = i;
    }
    const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    const tree = try Tree.create(Context{}, budget.allocator(), cells, indices, .{ .kind = .sum, .arguments = &.{0}, .filter = null, .sort = 0, .frame = null, .type = .integer, .star = false });
    defer budget.allocator().free(tree.nodes);
    for (0..count) |index| {
        const bounds = Frame{ .start = index -| 4096, .end = @min(count, index + 4097) };
        try std.testing.expectEqual(@as(i64, @intCast(bounds.end - bounds.start)), (try tree.query(bounds)).value.integer);
    }
    try std.testing.expect(budget.peak < 4 * 1024 * 1024);
    std.debug.print("SQL window frames: rows={d} frame_width=8193 peak_bytes={d} elapsed_ns={d}\n", .{ count, budget.peak, std.Io.Clock.awake.now(std.testing.io).nanoseconds - started });
}

// Removable exact state gives running and sliding count/integer/boolean
// frames linear work and constant memory. Floating-point, min/max and frame
// exclusions retain the tree, preserving their existing numeric semantics.
const Sliding = struct {
    bounds: Frame = .{ .start = 0, .end = 0 },
    count: u64 = 0,
    sum: i128 = 0,
    trues: u64 = 0,
    fn eligible(spec: binding.Spec) bool {
        if (spec.frame) |definition| if (definition.exclusion != .no_others) return false;
        return spec.kind == .count or spec.kind == .bool_and or spec.kind == .bool_or or (spec.kind == .sum and spec.type == .integer);
    }
    fn change(self: *Sliding, context: anytype, cells: anytype, indices: anytype, spec: binding.Spec, first: usize, end: usize, add: bool) !void {
        for (first..end) |position| {
            if (position % 256 == 0) try context.checkpoint();
            const row = try at(indices, position);
            if (spec.filter) |slot| {
                const accepted = try getCell(cells, row, slot);
                if (accepted.sql_null) continue;
                if (accepted.value != .bool) return error.SqlTypeMismatch;
                if (!accepted.value.bool) continue;
            }
            const value = if (spec.star) Datum.json(.{ .integer = 1 }) else try getCell(cells, row, spec.arguments[0]);
            if (value.sql_null) continue;
            if (add) self.count += 1 else self.count -= 1;
            switch (spec.kind) {
                .sum => {
                    if (value.value != .integer) return error.SqlTypeMismatch;
                    if (add) self.sum += value.value.integer else self.sum -= value.value.integer;
                },
                .bool_and, .bool_or => {
                    if (value.value != .bool) return error.SqlTypeMismatch;
                    if (value.value.bool) {
                        if (add) self.trues += 1 else self.trues -= 1;
                    }
                },
                else => {},
            }
        }
    }
    fn query(self: *Sliding, context: anytype, cells: anytype, indices: anytype, spec: binding.Spec, bounds: Frame) !Datum {
        // Variable frame offsets can move backwards; restart without relying
        // on monotonicity when intervals stop overlapping or reverse.
        if (bounds.start < self.bounds.start or bounds.end < self.bounds.end or bounds.start > self.bounds.end) {
            self.* = .{};
            try self.change(context, cells, indices, spec, bounds.start, bounds.end, true);
        } else {
            try self.change(context, cells, indices, spec, self.bounds.start, bounds.start, false);
            try self.change(context, cells, indices, spec, self.bounds.end, bounds.end, true);
        }
        self.bounds = bounds;
        if (spec.kind == .count) return Datum.json(.{ .integer = std.math.cast(i64, self.count) orelse return error.SqlNumericOutOfRange });
        if (self.count == 0) return .{};
        return switch (spec.kind) {
            .sum => Datum.json(.{ .integer = std.math.cast(i64, self.sum) orelse return error.SqlNumericOutOfRange }),
            .bool_and => Datum.json(.{ .bool = self.trues == self.count }),
            .bool_or => Datum.json(.{ .bool = self.trues != 0 }),
            else => unreachable,
        };
    }
};

pub fn evaluate(context: anytype, cells: anytype, indices: anytype, sort: binding.Sort, spec: binding.Spec, column: usize, peers_start: anytype, peers_end: anytype, groups: anytype) !void {
    const aggregate = switch (spec.kind) {
        .count, .sum, .avg, .min, .max, .bool_and, .bool_or => true,
        else => false,
    };
    // A single contiguous tree needs no arena growth slack. It is released
    // between specifications/partitions instead of accumulating with input.
    const sliding = aggregate and Sliding.eligible(spec);
    var running: Sliding = .{};
    var tree: ?Tree = if (aggregate and !sliding) try Tree.create(context, context.alloc, cells, indices, spec) else null;
    defer if (tree) |*value| value.deinit(context.alloc);
    var dense: i64 = 0;
    for (0..indices.len) |position| {
        const row = try at(indices, position);
        var result_arena = std.heap.ArenaAllocator.init(context.alloc);
        defer result_arena.deinit();
        try context.checkpoint();
        if ((try at(peers_start, position)) == position) dense += 1;
        const bounds = try frame(context, spec, sort, cells, indices, position, (try at(peers_start, position)), (try at(peers_end, position)), groups, @intCast(dense - 1));
        const selected = FrameSet.init(bounds, if (spec.frame) |definition| definition.exclusion else .no_others, position, (try at(peers_start, position)), (try at(peers_end, position)));
        const result: Datum = switch (spec.kind) {
            .row_number => Datum.json(.{ .integer = @intCast(position + 1) }),
            .rank => Datum.json(.{ .integer = @intCast((try at(peers_start, position)) + 1) }),
            .dense_rank => Datum.json(.{ .integer = dense }),
            .percent_rank => Datum.json(.{ .float = if (indices.len <= 1) 0 else @as(f64, @floatFromInt((try at(peers_start, position)))) / @as(f64, @floatFromInt(indices.len - 1)) }),
            .cume_dist => Datum.json(.{ .float = @as(f64, @floatFromInt((try at(peers_end, position)))) / @as(f64, @floatFromInt(indices.len)) }),
            .ntile => blk: {
                const buckets = (try integer(try getCell(cells, try at(indices, 0), spec.arguments[0]))) orelse break :blk Datum{};
                if (buckets <= 0) return error.InvalidSqlParameters;
                const count: usize = @intCast(buckets);
                const base = indices.len / count;
                const remainder = indices.len % count;
                const large = remainder * (base + 1);
                const bucket = if (position < large) position / (base + 1) else remainder + (position - large) / @max(1, base);
                break :blk Datum.json(.{ .integer = @intCast(bucket + 1) });
            },
            .lag, .lead => blk: {
                const distance = if (spec.arguments.len > 1) (try integer(try getCell(cells, row, spec.arguments[1]))) orelse break :blk Datum{} else 1;
                const target: i128 = @as(i128, @intCast(position)) + (if (spec.kind == .lag) -@as(i128, distance) else @as(i128, distance));
                if (target < 0 or target >= indices.len) break :blk if (spec.arguments.len > 2) try getCell(cells, row, spec.arguments[2]) else Datum{};
                break :blk try getCell(cells, try at(indices, @intCast(target)), spec.arguments[0]);
            },
            .first_value, .last_value, .nth_value => blk: {
                const index: usize = if (spec.kind == .last_value) last: {
                    if (selected.len == 0) break :blk Datum{};
                    break :last selected.parts[selected.len - 1].end - 1;
                } else if (spec.kind == .first_value) selected.nth(0) orelse break :blk Datum{} else nth: {
                    const n = (try integer(try getCell(cells, row, spec.arguments[1]))) orelse break :blk Datum{};
                    if (n <= 0) return error.InvalidSqlParameters;
                    break :nth selected.nth(@intCast(n - 1)) orelse break :blk Datum{};
                };
                break :blk try getCell(cells, try at(indices, index), spec.arguments[0]);
            },
            else => if (sliding) try running.query(context, cells, indices, spec, bounds) else try tree.?.querySet(selected),
        };
        const a = if (comptime disk.isDisk(@TypeOf(cells))) result_arena.allocator() else context.arena;
        const typed = if (!result.sql_null and spec.type == .array)
            try scalar.castArrayDatum(a, result, spec.element_type orelse return error.InvalidSqlProgram, .{ .output_bytes = context.limits.retained_bytes })
        else
            result;
        try setCell(cells, row, column, try describe.coerceDatum(a, typed, spec.type, spec.element_type));
    }
}

pub fn execute(context: anytype, statement: ast.Select) anyerror!@import("runtime.zig").Output {
    const bound = context.binding.window orelse return error.InvalidSqlBackendResponse;
    for (bound.specs) |spec| if (spec.frame) |definition| for ([_]ast.Window.Bound{ definition.start, definition.end }) |boundary| switch (boundary) {
        .preceding, .following => |value| _ = try offsetValue(context, value),
        else => {},
    };
    const limit = statement.capRows(try context.count(statement.limit, context.limits.result_rows));
    const offset = try context.offsetCount(statement.offset);
    if (limit > context.limits.result_rows or offset > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
    if (limit == 0) return .{ .columns = context.binding.columns, .command_tag = "SELECT" };
    if (context.spill != null) if (try @import("window_spill.zig").execute(context, statement)) |output| return output;
    var state = std.heap.ArenaAllocator.init(context.alloc);
    defer state.deinit();
    const alloc = state.allocator();
    var input_context = context;
    input_context.sink = null;
    input_context.binding = bound.input.*;
    input_context.arena = alloc;
    input_context.typed_output = true;
    input_context.limits.result_rows = context.limits.scan_rows;
    const cells = blk: {
        const input = try input_context.typedQuery(bound.statement);
        defer input.close();
        const gathered = try alloc.alloc([]Datum, input.count());
        for (gathered) |*values| {
            const row = (try input.next(alloc)) orelse return error.InvalidSqlBackendResponse;
            values.* = try alloc.alloc(Datum, row.len + bound.specs.len);
            @memset(values.*, .{});
            @memcpy(values.*[0..row.len], row);
        }
        break :blk gathered;
    };
    return evaluateCells(context, statement, cells);
}
pub fn evaluateCells(context: anytype, statement: ast.Select, cells: [][]Datum) !@import("runtime.zig").Output {
    const bound = context.binding.window.?;
    const roots = try @import("ordering_reuse.zig").plan(context.arena, bound);
    var final_indices: ?[]usize = null;
    defer if (final_indices) |indices| context.alloc.free(indices);
    var final_order: ?binding.Sort = null;
    for (bound.sorts, 0..) |strong, root| {
        if (roots[root] != root) continue;
        try context.checkpoint();
        var sorted = std.heap.ArenaAllocator.init(context.alloc);
        defer sorted.deinit();
        const indices = try sorted.allocator().alloc(usize, cells.len);
        for (indices, 0..) |*index, i| index.* = i;
        var sorter = Sorter(@TypeOf(context)){ .context = context, .cells = cells, .spec = strong };
        std.mem.sort(usize, indices, &sorter, @TypeOf(sorter).less);
        if (sorter.failure) |err| return err;
        const peer_starts = try sorted.allocator().alloc(usize, cells.len);
        const peer_ends = try sorted.allocator().alloc(usize, cells.len);
        for (bound.sorts, 0..) |sort, sort_index| {
            if (roots[sort_index] != root) continue;
            if (final_indices == null and !@import("decision_eval.zig").hasExternalPrograms(bound.outputs) and
                @import("ordering_reuse.zig").finalOrder(sort, bound.orders, statement.order_by))
            {
                // Retain one permutation, not another copy of every payload.
                // Delay applying it until all window/navigation specifications
                // have finished in the original row-identity domain.
                final_indices = try context.alloc.dupe(usize, indices);
                final_order = sort;
            }
            const needs_groups = for (bound.specs) |spec| {
                if (spec.sort == sort_index and spec.frame != null and spec.frame.?.mode == .groups) break true;
            } else false;
            const group_starts = try sorted.allocator().alloc(usize, if (needs_groups) cells.len + 1 else 0);
            var start: usize = 0;
            while (start < indices.len) {
                var end = start + 1;
                while (end < indices.len and try equal(cells, indices[start], indices[end], sort.partition)) : (end += 1) {
                    if (end % 256 == 0) try context.checkpoint();
                }
                const partition = indices[start..end];
                var peer: usize = 0;
                var group_count: usize = 0;
                while (peer < partition.len) {
                    if (needs_groups) group_starts[group_count] = peer;
                    group_count += 1;
                    var peer_end = peer + 1;
                    while (peer_end < partition.len and try equal(cells, partition[peer], partition[peer_end], sort.order)) : (peer_end += 1) {
                        if (peer_end % 256 == 0) try context.checkpoint();
                    }
                    @memset(peer_starts[peer..peer_end], peer);
                    @memset(peer_ends[peer..peer_end], peer_end);
                    peer = peer_end;
                }
                if (needs_groups) group_starts[group_count] = partition.len;
                for (bound.specs, 0..) |spec, index| if (spec.sort == sort_index) try evaluate(context, cells, partition, sort, spec, bound.input.columns.len + index, peer_starts[0..partition.len], peer_ends[0..partition.len], group_starts[0..if (needs_groups) group_count + 1 else 0]);
                start = end;
            }
        }
    }
    if (final_indices) |indices| {
        for (0..indices.len) |start| {
            if (indices[start] == start) continue;
            const saved = cells[start];
            var current = start;
            while (indices[current] != start) {
                try context.checkpoint();
                const next = indices[current];
                cells[current] = cells[next];
                indices[current] = current;
                current = next;
            }
            cells[current] = saved;
            indices[current] = current;
        }
        return finishOrderedCells(context, statement, cells, final_order);
    }
    return finishCells(context, statement, cells);
}
fn rowCells(cells: anytype, alloc: Allocator, index: usize) ![]const Datum {
    if (comptime disk.isDisk(@TypeOf(cells))) {
        const row = try cells.row(index);
        const values = try alloc.alloc(Datum, row.values.len);
        for (row.values, values) |value, *out| out.* = try operators.cloneDatum(alloc, value);
        return values;
    }
    return cells[index];
}
fn rowOrdinal(cells: anytype, index: usize) !u64 {
    return if (comptime disk.isDisk(@TypeOf(cells))) (try cells.row(index)).ordinal else index;
}
pub fn finishCells(context: anytype, statement: ast.Select, cells: anytype) !@import("runtime.zig").Output {
    return finishOrderedCells(context, statement, cells, null);
}
pub fn finishOrderedCells(context: anytype, statement: ast.Select, cells: anytype, physical_order: ?binding.Sort) !@import("runtime.zig").Output {
    const bound = context.binding.window.?;
    const limit = statement.capRows(try context.count(statement.limit, context.limits.result_rows));
    const offset = try context.offsetCount(statement.offset);
    if (physical_order) |sort| if (@import("ordering_reuse.zig").finalOrder(sort, bound.orders, statement.order_by) and
        !@import("decision_eval.zig").hasExternalPrograms(bound.outputs))
    {
        const count = @min(limit, cells.len -| offset);
        const rows = try context.arena.alloc([]const Json, if (context.sink == null) count else 0);
        const flags = try context.arena.alloc([]const bool, rows.len);
        var scratch = std.heap.ArenaAllocator.init(context.alloc);
        defer scratch.deinit();
        // Window inputs/results have already been evaluated in their own
        // domain. This projection sits below OFFSET, but an explicit LIMIT
        // need not demand the unused ordered tail. Do not silently apply the
        // response quota as a SQL limit to an unbounded query.
        const prefix = if (statement.limit != null or statement.scalar_cardinality_limit) @min(cells.len, offset +| limit) else cells.len;
        for (0..prefix) |index| {
            try context.checkpoint();
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const input = try rowCells(cells, a, index);
            const values = try a.alloc(Datum, bound.outputs.len);
            // Keep expression errors on OFFSET-skipped rows observable.
            for (bound.outputs, values) |program, *out| out.* = try context.evaluate(a, program, input);
            if (index < offset or index - offset >= count) continue;
            if (context.sink) |sink| {
                try sink.append(sink.ptr, values);
            } else {
                const output = try context.arena.alloc(Json, values.len);
                const nulls = try context.arena.alloc(bool, values.len);
                for (values, output, nulls, bound.outputs) |value, *cell, *is_null, program| {
                    cell.* = try context.outputDatum(value, program.output_type.kind, program.output_type.element_type);
                    is_null.* = value.sql_null;
                }
                rows[index - offset] = output;
                flags[index - offset] = nulls;
            }
        }
        if (statement.limit == null and cells.len -| offset > limit) return error.SqlResultTooLarge;
        return .{ .columns = context.binding.columns, .rows = rows, .sql_nulls = flags, .command_tag = "SELECT" };
    };
    var output_state = std.heap.ArenaAllocator.init(context.alloc);
    defer output_state.deinit();
    const alloc = output_state.allocator();
    const orders = try alloc.alloc(operators.Order, statement.order_by.len);
    for (statement.order_by, orders) |order, *out| out.* = .{ .descending = order.descending, .nulls_first = order.nulls_first };
    // The window input is already materialized: size the heap for rows that
    // exist, not the API's maximum result budget (especially in nested plans).
    const retained_count = @min(cells.len, offset +| limit +| @intFromBool(statement.limit == null));
    var top = try operators.TopK.initWithSpill(context.alloc, retained_count, orders, context.limits.retained_bytes, context.spill);
    defer top.deinit();
    var eval = std.heap.ArenaAllocator.init(context.alloc);
    defer eval.deinit();
    const decision = @import("decision_eval.zig");
    if (decision.hasExternalPrograms(bound.outputs) or decision.hasExternalPrograms(bound.orders)) {
        const projection = try decision.SortedProjection.init(context.arena, bound.outputs, bound.orders, bound.order_outputs);
        var begin: usize = 0;
        while (begin < cells.len) {
            try context.checkpoint();
            if (!eval.reset(.retain_capacity)) return error.OutOfMemory;
            const a = eval.allocator();
            var page: std.ArrayList([]const Datum) = .empty;
            var bytes: usize = 0;
            while (begin + page.items.len < cells.len and page.items.len < context.limits.page_rows) {
                const row = try rowCells(cells, a, begin + page.items.len);
                try page.append(a, row);
                for (row) |cell| bytes +|= try operators.datumBytes(cell);
                if (bytes >= context.limits.page_bytes) break;
            }
            const ordinals = try a.alloc(u64, page.items.len);
            for (ordinals, begin..) |*out_ordinal, index| out_ordinal.* = try rowOrdinal(cells, index);
            try projection.add(context, a, &top, page.items, ordinals);
            begin += page.items.len;
        }
        return projection.finish(context, &top, offset, limit, statement.limit == null);
    } else for (0..cells.len) |index| {
        try context.checkpoint();
        _ = eval.reset(.retain_capacity);
        const row = try rowCells(cells, eval.allocator(), index);
        const values = try eval.allocator().alloc(Datum, bound.outputs.len);
        for (bound.outputs, values) |program, *out| out.* = try context.evaluate(eval.allocator(), program, row);
        const keys = try eval.allocator().alloc(Datum, bound.orders.len);
        for (bound.orders, keys) |program, *out| out.* = try context.evaluate(eval.allocator(), program, row);
        try top.add(.{ .values = values, .keys = keys, .ordinal = try rowOrdinal(cells, index) });
    }
    if (context.sink != null) return context.emitTop(&top, offset, limit, statement.limit == null);
    const ordered = try top.finishPage(alloc, offset, limit + @intFromBool(statement.limit == null));
    const remaining = ordered.len;
    if (statement.limit == null and remaining > limit) return error.SqlResultTooLarge;
    const selected = ordered[0..@min(remaining, limit)];
    const rows = try context.arena.alloc([]const Json, selected.len);
    const flags = try context.arena.alloc([]const bool, selected.len);
    for (selected, rows, flags) |row, *out, *nulls| {
        const values = try context.arena.alloc(Json, row.values.len);
        const bits = try context.arena.alloc(bool, row.values.len);
        for (row.values, values, bits, bound.outputs) |value, *cell, *is_null, program| {
            cell.* = try context.outputDatum(value, program.output_type.kind, program.output_type.element_type);
            is_null.* = value.sql_null;
        }
        out.* = values;
        nulls.* = bits;
    }
    return .{ .columns = context.binding.columns, .rows = rows, .sql_nulls = flags, .command_tag = "SELECT" };
}

test "SQL ordered window projection demands the prefix not the unused memory or disk tail" {
    const Fixture = struct {
        checkpoints: usize = 0,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checkpoints += 1;
        }
        fn resolve(_: *anyopaque, _: Allocator, _: ast.Name, _: @import("catalog.zig").Action) !@import("catalog.zig").Table {
            return .{ .id = 1, .physical_name = "things", .schema_version = 1, .columns = &.{.{ .name = "x", .path = "x", .type = .integer }} };
        }
        fn verify(context: @import("runtime.zig").Context, statement: ast.Select, cells: anytype, want: ?[]const u8, expected_error: ?anyerror) !void {
            const order = context.binding.window.?.sorts[0];
            if (expected_error) |err| {
                try std.testing.expectError(err, finishOrderedCells(context, statement, cells, order));
            } else {
                const output = try finishOrderedCells(context, statement, cells, order);
                try std.testing.expectEqual(@as(usize, 1), output.rows.len);
                try std.testing.expectEqualStrings(want.?, output.rows[0][0].string);
            }
        }
    };
    const cases = [_]struct { sql: []const u8, want: ?[]const u8 = null, err: ?anyerror = null, prefix: usize }{
        .{ .sql = "SELECT 1/(2-ROW_NUMBER() OVER (ORDER BY x)) FROM things ORDER BY x LIMIT 1", .want = "1", .prefix = 1 },
        .{ .sql = "SELECT 1/(2-ROW_NUMBER() OVER (ORDER BY x)) FROM things ORDER BY x LIMIT 1 OFFSET 1", .err = error.SqlDivisionByZero, .prefix = 2 },
        .{ .sql = "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM things ORDER BY x LIMIT 1 OFFSET 2", .want = "3", .prefix = 3 },
        .{ .sql = "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM things ORDER BY x", .err = error.SqlResultTooLarge, .prefix = 512 },
    };
    for (cases) |case| {
        var fixture: Fixture = .{};
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        const backend: @import("catalog.zig").Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Fixture.resolve, .scan = undefined, .mutate = undefined, .checkpoint = Fixture.checkpoint } };
        const bound = try describe.bind(a, backend, &compiled, &.{});
        const context: @import("runtime.zig").Context = .{ .alloc = std.testing.allocator, .arena = a, .backend = backend, .binding = bound, .parameters = &.{}, .limits = .{ .result_rows = 1 } };
        var data: [512][2]Datum = undefined;
        var cells: [512][]Datum = undefined;
        for (&data, &cells, 0..) |*row, *cell, index| {
            row.* = @splat(Datum.json(.{ .integer = @intCast(index + 1) }));
            cell.* = row;
        }
        fixture.checkpoints = 0;
        try Fixture.verify(context, compiled.statement.select, &cells, case.want, case.err);
        try std.testing.expectEqual(case.prefix, fixture.checkpoints);

        var manager: @import("spill.zig").Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.checkpoint, .max_bytes = 1024 * 1024, .max_record_bytes = 4096, .async_writes = false };
        defer manager.deinit();
        var rows = try disk.Rows.init(std.testing.allocator, &manager, 2);
        defer rows.deinit();
        for (cells, 0..) |row, index| try rows.append(.{ .values = row, .keys = &.{}, .ordinal = index });
        fixture.checkpoints = 0;
        manager.read_calls = 0;
        try Fixture.verify(context, compiled.statement.select, &rows, case.want, case.err);
        if (case.prefix < cells.len) {
            try std.testing.expect(manager.read_calls < 64);
            try std.testing.expect(fixture.checkpoints < 64);
        }
    }
}

test "SQL sliding kernels match independent tree across nullable filtered reversing frames" {
    const Context = struct {
        pub fn checkpoint(_: @This()) !void {}
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var values: [33][2]Datum = undefined;
    var cells: [33][]Datum = undefined;
    var indices: [33]usize = undefined;
    for ([_]binding.Kind{ .count, .sum, .bool_and, .bool_or }) |kind| {
        for (&values, &cells, &indices, 0..) |*row, *cell, *index, i| {
            row.* = .{ if (i % 5 == 0) Datum{} else if (kind == .bool_and or kind == .bool_or) Datum.json(.{ .bool = i % 4 != 0 }) else Datum.json(.{ .integer = @as(i64, @intCast(i)) - 16 }), Datum.json(.{ .bool = i % 3 != 0 }) };
            cell.* = row;
            index.* = i;
        }
        const spec: binding.Spec = .{ .kind = kind, .arguments = &.{0}, .filter = 1, .sort = 0, .frame = null, .type = if (kind == .bool_and or kind == .bool_or) .boolean else .integer, .star = false };
        var tree = try Tree.create(Context{}, arena.allocator(), &cells, &indices, spec);
        defer tree.deinit(arena.allocator());
        var sliding: Sliding = .{};
        for (0..34) |first| for (first..34) |end| {
            const bounds: Frame = .{ .start = first, .end = end };
            try std.testing.expectEqualDeep(try tree.query(bounds), try sliding.query(Context{}, &cells, &indices, spec, bounds));
        };
        // Restart and narrow frames must not report overflow from unused rows.
        try std.testing.expectEqualDeep(try tree.query(.{ .start = 0, .end = 3 }), try sliding.query(Context{}, &cells, &indices, spec, .{ .start = 0, .end = 3 }));
    }
}
