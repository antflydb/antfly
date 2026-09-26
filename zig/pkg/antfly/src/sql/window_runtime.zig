// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

fn compare(a: Datum, b: Datum, direction: operators.Order) !std.math.Order {
    if (a.sql_null or b.sql_null) {
        if (a.sql_null == b.sql_null) return .eq;
        return if (a.sql_null == (direction.nulls_first orelse direction.descending)) .lt else .gt;
    }
    const result = try scalar.compare(a.value, b.value);
    return if (direction.descending) result.invert() else result;
}
fn equal(cells: []const []Datum, a: usize, b: usize, columns: []const usize) !bool {
    for (columns) |column| if (try compare(cells[a][column], cells[b][column], .{}) != .eq) return false;
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

fn rangeBoundary(context: anytype, bound: ast.Window.Bound, cells: []const []Datum, indices: []const usize, spec: binding.Sort, position: usize, peer_start: usize, peer_end: usize, end: bool) !usize {
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
    const current = cells[indices[position]][column];
    if (current.sql_null) return if (end) peer_end else peer_start;
    if (current.value != .integer and current.value != .float) return error.SqlTypeMismatch;
    const subtract = (bound == .preceding) != direction.descending;
    const integer_target: i128 = if (current.value == .integer) @as(i128, current.value.integer) + (if (subtract) -@as(i128, @intCast(distance)) else @as(i128, @intCast(distance))) else 0;
    const float_target: f64 = if (current.value == .float) current.value.float + (if (subtract) -@as(f64, @floatFromInt(distance)) else @as(f64, @floatFromInt(distance))) else 0;
    var low: usize = 0;
    var high = indices.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const value = cells[indices[middle]][column];
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
fn frame(context: anytype, spec: binding.Spec, sort: binding.Sort, cells: []const []Datum, indices: []const usize, position: usize, peer_start: usize, peer_end: usize, groups: []const usize, group_index: usize) !Frame {
    const definition = spec.frame orelse ast.Window.Frame{ .mode = .range, .start = .unbounded_preceding, .end = .current };
    const start = if (definition.mode == .rows)
        try rowBoundary(context, definition.start, position, indices.len, false)
    else if (definition.mode == .groups)
        groups[try rowBoundary(context, definition.start, group_index, groups.len - 1, false)]
    else
        try rangeBoundary(context, definition.start, cells, indices, sort, position, peer_start, peer_end, false);
    const end = if (definition.mode == .rows)
        try rowBoundary(context, definition.end, position, indices.len, true)
    else if (definition.mode == .groups)
        groups[try rowBoundary(context, definition.end, group_index, groups.len - 1, true)]
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
    cells: []const []Datum,
    spec: binding.Spec,

    fn create(context: anytype, alloc: Allocator, cells: []const []Datum, indices: []const usize, spec: binding.Spec) !Tree {
        const base = try std.math.ceilPowerOfTwo(usize, @max(1, indices.len));
        const nodes = try alloc.alloc(Node, try std.math.mul(usize, base, 2));
        errdefer alloc.free(nodes);
        @memset(nodes, .{});
        var result = Tree{ .nodes = nodes, .base = base, .cells = cells, .spec = spec };
        for (indices, 0..) |row, index| {
            if (index % 256 == 0) try context.checkpoint();
            if (spec.filter) |slot| {
                const accepted = cells[row][slot];
                if (accepted.sql_null) continue;
                if (accepted.value != .bool) return error.SqlTypeMismatch;
                if (!accepted.value.bool) continue;
            }
            const value = if (spec.star) Datum.json(.{ .integer = 1 }) else cells[row][spec.arguments[0]];
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
            nodes[base + index] = node;
        }
        var index = base;
        while (index > 1) {
            index -= 1;
            if (index % 256 == 0) try context.checkpoint();
            nodes[index] = try result.combine(nodes[2 * index], nodes[2 * index + 1]);
        }
        return result;
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
                const order = try scalar.compare(self.cells[left.selected][self.spec.arguments[0]].value, self.cells[right.selected][self.spec.arguments[0]].value);
                if (order == (if (self.spec.kind == .min) std.math.Order.gt else .lt)) result.selected = right.selected;
            },
            .avg => {
                // Integer inputs retain an exact wide sum; do not invoke
                // software quad-precision arithmetic for their unused mean.
                if (self.cells[result.selected][self.spec.arguments[0]].value != .integer) {
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
                result = try self.combine(result, self.nodes[left]);
                left += 1;
            }
            if (right % 2 != 0) {
                right -= 1;
                result = try self.combine(result, self.nodes[right]);
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
            .avg => Datum.json(.{ .float = if (self.cells[result.selected][self.spec.arguments[0]].value == .integer) @as(f64, @floatFromInt(result.integer_sum)) / @as(f64, @floatFromInt(result.count)) else try finite(result.number) }),
            .bool_and => Datum.json(.{ .bool = result.true_count == result.count }),
            .bool_or => Datum.json(.{ .bool = result.true_count != 0 }),
            .min, .max => self.cells[result.selected][self.spec.arguments[0]],
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

fn evaluate(context: anytype, cells: [][]Datum, indices: []const usize, sort: binding.Sort, spec: binding.Spec, column: usize, peers_start: []const usize, peers_end: []const usize, groups: []const usize) !void {
    const aggregate = switch (spec.kind) {
        .count, .sum, .avg, .min, .max, .bool_and, .bool_or => true,
        else => false,
    };
    // A single contiguous tree needs no arena growth slack. It is released
    // between specifications/partitions instead of accumulating with input.
    const tree: ?Tree = if (aggregate) try Tree.create(context, context.alloc, cells, indices, spec) else null;
    defer if (tree) |value| context.alloc.free(value.nodes);
    var dense: i64 = 0;
    for (indices, 0..) |row, position| {
        try context.checkpoint();
        if (peers_start[position] == position) dense += 1;
        const bounds = try frame(context, spec, sort, cells, indices, position, peers_start[position], peers_end[position], groups, @intCast(dense - 1));
        const selected = FrameSet.init(bounds, if (spec.frame) |definition| definition.exclusion else .no_others, position, peers_start[position], peers_end[position]);
        const result: Datum = switch (spec.kind) {
            .row_number => Datum.json(.{ .integer = @intCast(position + 1) }),
            .rank => Datum.json(.{ .integer = @intCast(peers_start[position] + 1) }),
            .dense_rank => Datum.json(.{ .integer = dense }),
            .percent_rank => Datum.json(.{ .float = if (indices.len <= 1) 0 else @as(f64, @floatFromInt(peers_start[position])) / @as(f64, @floatFromInt(indices.len - 1)) }),
            .cume_dist => Datum.json(.{ .float = @as(f64, @floatFromInt(peers_end[position])) / @as(f64, @floatFromInt(indices.len)) }),
            .ntile => blk: {
                const buckets = (try integer(cells[indices[0]][spec.arguments[0]])) orelse break :blk Datum{};
                if (buckets <= 0) return error.InvalidSqlParameters;
                const count: usize = @intCast(buckets);
                const base = indices.len / count;
                const remainder = indices.len % count;
                const large = remainder * (base + 1);
                const bucket = if (position < large) position / (base + 1) else remainder + (position - large) / @max(1, base);
                break :blk Datum.json(.{ .integer = @intCast(bucket + 1) });
            },
            .lag, .lead => blk: {
                const distance = if (spec.arguments.len > 1) (try integer(cells[row][spec.arguments[1]])) orelse break :blk Datum{} else 1;
                const target: i128 = @as(i128, @intCast(position)) + (if (spec.kind == .lag) -@as(i128, distance) else @as(i128, distance));
                if (target < 0 or target >= indices.len) break :blk if (spec.arguments.len > 2) cells[row][spec.arguments[2]] else Datum{};
                break :blk cells[indices[@intCast(target)]][spec.arguments[0]];
            },
            .first_value, .last_value, .nth_value => blk: {
                const index: usize = if (spec.kind == .last_value) last: {
                    if (selected.len == 0) break :blk Datum{};
                    break :last selected.parts[selected.len - 1].end - 1;
                } else if (spec.kind == .first_value) selected.nth(0) orelse break :blk Datum{} else nth: {
                    const n = (try integer(cells[row][spec.arguments[1]])) orelse break :blk Datum{};
                    if (n <= 0) return error.InvalidSqlParameters;
                    break :nth selected.nth(@intCast(n - 1)) orelse break :blk Datum{};
                };
                break :blk cells[indices[index]][spec.arguments[0]];
            },
            else => try tree.?.querySet(selected),
        };
        cells[row][column] = if (result.sql_null) result else .{
            .value = try describe.coerce(result.value, spec.type),
            .sql_null = false,
        };
    }
}

pub fn execute(context: anytype, statement: ast.Select) anyerror!@import("runtime.zig").Output {
    const bound = context.binding.window orelse return error.InvalidSqlBackendResponse;
    for (bound.specs) |spec| if (spec.frame) |definition| for ([_]ast.Window.Bound{ definition.start, definition.end }) |boundary| switch (boundary) {
        .preceding, .following => |value| _ = try offsetValue(context, value),
        else => {},
    };
    const limit = try context.count(statement.limit, context.limits.result_rows);
    const offset = try context.count(statement.offset, 0);
    if (limit > context.limits.result_rows or offset > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
    if (limit == 0) return .{ .columns = context.binding.columns, .command_tag = "SELECT" };
    var state = std.heap.ArenaAllocator.init(context.alloc);
    defer state.deinit();
    const alloc = state.allocator();
    var input_context = context;
    input_context.binding = bound.input.*;
    input_context.arena = alloc;
    input_context.typed_output = true;
    input_context.limits.result_rows = context.limits.scan_rows;
    const input = try input_context.select(bound.statement);
    const cells = try alloc.alloc([]Datum, input.rows.len);
    for (input.rows, cells, 0..) |row, *values, index| {
        values.* = try alloc.alloc(Datum, row.len + bound.specs.len);
        @memset(values.*, .{});
        for (row, bound.input.columns, values.*[0..row.len], 0..) |value, column, *out, i| out.* = .{ .value = try describe.coerce(value, column.type), .sql_null = if (input.sql_nulls) |flags| flags[index][i] else value == .null };
    }
    for (bound.sorts, 0..) |sort, sort_index| {
        try context.checkpoint();
        var sorted = std.heap.ArenaAllocator.init(context.alloc);
        defer sorted.deinit();
        const indices = try sorted.allocator().alloc(usize, cells.len);
        for (indices, 0..) |*index, i| index.* = i;
        var sorter = Sorter(@TypeOf(context)){ .context = context, .cells = cells, .spec = sort };
        std.mem.sort(usize, indices, &sorter, @TypeOf(sorter).less);
        if (sorter.failure) |err| return err;
        const peer_starts = try sorted.allocator().alloc(usize, cells.len);
        const peer_ends = try sorted.allocator().alloc(usize, cells.len);
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
    const orders = try alloc.alloc(operators.Order, statement.order_by.len);
    for (statement.order_by, orders) |order, *out| out.* = .{ .descending = order.descending, .nulls_first = order.nulls_first };
    var top = try operators.TopK.init(context.alloc, offset + limit + @intFromBool(statement.limit == null), orders, context.limits.retained_bytes);
    defer top.deinit();
    var eval = std.heap.ArenaAllocator.init(context.alloc);
    defer eval.deinit();
    for (cells, 0..) |row, index| {
        try context.checkpoint();
        _ = eval.reset(.retain_capacity);
        const values = try eval.allocator().alloc(Datum, bound.outputs.len);
        for (bound.outputs, values) |program, *out| out.* = try program.evaluate(eval.allocator(), row, context.parameters, .{});
        const keys = try eval.allocator().alloc(Datum, bound.orders.len);
        for (bound.orders, keys) |program, *out| out.* = try program.evaluate(eval.allocator(), row, context.parameters, .{});
        try top.add(.{ .values = values, .keys = keys, .ordinal = index });
    }
    const ordered = try top.finish(alloc);
    const remaining = ordered.len -| offset;
    if (statement.limit == null and remaining > limit) return error.SqlResultTooLarge;
    const selected = ordered[@min(offset, ordered.len)..][0..@min(remaining, limit)];
    const rows = try context.arena.alloc([]const Json, selected.len);
    const flags = try context.arena.alloc([]const bool, selected.len);
    for (selected, rows, flags) |row, *out, *nulls| {
        const values = try context.arena.alloc(Json, row.values.len);
        const bits = try context.arena.alloc(bool, row.values.len);
        for (row.values, values, bits) |value, *cell, *is_null| {
            cell.* = try context.outputValue(value.value);
            is_null.* = value.sql_null;
        }
        out.* = values;
        nulls.* = bits;
    }
    return .{ .columns = context.binding.columns, .rows = rows, .sql_nulls = flags, .command_tag = "SELECT" };
}
