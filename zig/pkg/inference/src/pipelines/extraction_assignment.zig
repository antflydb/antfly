// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded deterministic minimum-cost bipartite assignment, shared by record
//! decoding and permutation-invariant training. Uses the dependency-free
//! Hungarian contract in the pinned Fastino training/matching.py. The default
//! profile reproduces the pinned SciPy solver and its row-major perturbation;
//! the portable profile preserves Fastino's dependency-free fallback.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Pair = struct { row: usize, column: usize };
pub const Limits = struct {
    max_dimension: usize = 1024,
    max_work: usize = 64 * 1024 * 1024,
    control: ?Control = null,
    profile: enum { portable, scipy_perturbed } = .scipy_perturbed,
};

/// Row-major finite costs; returns min(rows,columns) unique pairs sorted by
/// original row. Caller owns the result. Exact ties visit columns in order.
pub fn solve(allocator: std.mem.Allocator, costs: []const f64, rows: usize, columns: usize, limits: Limits) ![]Pair {
    if (limits.control) |control| try control.check();
    if (rows > limits.max_dimension or columns > limits.max_dimension) return error.ExtractionAssignmentLimitExceeded;
    if (costs.len != try std.math.mul(usize, rows, columns)) return error.InvalidAssignmentShape;
    for (costs) |cost| if (!std.math.isFinite(cost)) return error.InvalidAssignmentCost;
    const n = @min(rows, columns);
    const m = @max(rows, columns);
    if (try std.math.mul(usize, try std.math.mul(usize, n, n), m) > limits.max_work) return error.ExtractionAssignmentLimitExceeded;
    if (n == 0) return allocator.alloc(Pair, 0);
    const transposed = columns < rows;
    var scale: f64 = 1;
    for (costs) |cost| scale = @max(scale, @abs(cost));
    const epsilon = if (limits.profile == .scipy_perturbed) std.math.floatEps(f64) * scale else 0;
    if (limits.profile == .scipy_perturbed) return solveScipy(allocator, costs, rows, columns, epsilon, limits);
    const u = try allocator.alloc(f64, n + 1);
    defer allocator.free(u);
    const v = try allocator.alloc(f64, m + 1);
    defer allocator.free(v);
    const assigned = try allocator.alloc(usize, m + 1);
    defer allocator.free(assigned);
    const way = try allocator.alloc(usize, m + 1);
    defer allocator.free(way);
    const minimum = try allocator.alloc(f64, m + 1);
    defer allocator.free(minimum);
    const used = try allocator.alloc(bool, m + 1);
    defer allocator.free(used);
    @memset(u, 0);
    @memset(v, 0);
    @memset(assigned, 0);
    @memset(way, 0);
    for (1..n + 1) |i| {
        assigned[0] = i;
        var column: usize = 0;
        @memset(minimum, std.math.inf(f64));
        @memset(used, false);
        while (true) {
            if (limits.control) |control| try control.check();
            used[column] = true;
            const row = assigned[column];
            var delta = std.math.inf(f64);
            var next_column: usize = 0;
            for (1..m + 1) |j| {
                if (used[j]) continue;
                const offset = if (transposed) (j - 1) * columns + row - 1 else (row - 1) * columns + j - 1;
                const cost = costs[offset] + epsilon * @as(f64, @floatFromInt(offset));
                const reduced = cost - u[row] - v[j];
                if (!std.math.isFinite(reduced)) return error.InvalidAssignmentCost;
                if (reduced < minimum[j]) {
                    minimum[j] = reduced;
                    way[j] = column;
                }
                if (minimum[j] < delta) {
                    delta = minimum[j];
                    next_column = j;
                }
            }
            if (!std.math.isFinite(delta) or next_column == 0) return error.InvalidAssignmentCost;
            for (0..m + 1) |j| {
                if (used[j]) {
                    u[assigned[j]] += delta;
                    v[j] -= delta;
                    if (!std.math.isFinite(u[assigned[j]]) or !std.math.isFinite(v[j])) return error.InvalidAssignmentCost;
                } else minimum[j] -= delta;
            }
            column = next_column;
            if (assigned[column] == 0) break;
        }
        while (true) {
            const previous = way[column];
            assigned[column] = assigned[previous];
            column = previous;
            if (column == 0) break;
        }
    }
    const pairs = try allocator.alloc(Pair, n);
    var count: usize = 0;
    for (1..m + 1) |j| {
        if (assigned[j] == 0) continue;
        pairs[count] = if (transposed) .{ .row = j - 1, .column = assigned[j] - 1 } else .{ .row = assigned[j] - 1, .column = j - 1 };
        count += 1;
    }
    std.debug.assert(count == n);
    std.mem.sort(Pair, pairs, {}, struct {
        fn less(_: void, a: Pair, b: Pair) bool {
            return a.row < b.row;
        }
    }.less);
    return pairs;
}

// Adapted from SciPy 1.16.3 rectangular_lsap.cpp by PM Larsen (BSD-3-Clause).
// The full license is retained in licenses/scipy-rectangular-lsap.txt.
// Preserve reverse column visitation and free-sink ties: replacing this with
// a generic Hungarian solver changes exact record assignments on tied scores.
fn solveScipy(allocator: std.mem.Allocator, costs: []const f64, rows: usize, columns: usize, epsilon: f64, limits: Limits) ![]Pair {
    const n = @min(rows, columns);
    const m = @max(rows, columns);
    const transposed = columns < rows;
    const none: usize = std.math.maxInt(usize);
    const u = try allocator.alloc(f64, n);
    defer allocator.free(u);
    const v = try allocator.alloc(f64, m);
    defer allocator.free(v);
    const distance = try allocator.alloc(f64, m);
    defer allocator.free(distance);
    const path = try allocator.alloc(usize, m);
    defer allocator.free(path);
    const column_for_row = try allocator.alloc(usize, n);
    defer allocator.free(column_for_row);
    const row_for_column = try allocator.alloc(usize, m);
    defer allocator.free(row_for_column);
    const visited_rows = try allocator.alloc(bool, n);
    defer allocator.free(visited_rows);
    const visited_columns = try allocator.alloc(bool, m);
    defer allocator.free(visited_columns);
    const remaining = try allocator.alloc(usize, m);
    defer allocator.free(remaining);
    @memset(u, 0);
    @memset(v, 0);
    @memset(column_for_row, none);
    @memset(row_for_column, none);
    for (0..n) |current_row| {
        @memset(visited_rows, false);
        @memset(visited_columns, false);
        @memset(distance, std.math.inf(f64));
        for (remaining, 0..) |*column, i| column.* = m - i - 1;
        var left = m;
        var row = current_row;
        var minimum: f64 = 0;
        var sink = none;
        while (sink == none) {
            if (limits.control) |control| try control.check();
            visited_rows[row] = true;
            var lowest = std.math.inf(f64);
            var lowest_index = none;
            for (remaining[0..left], 0..) |column, i| {
                const offset = if (transposed) column * columns + row else row * columns + column;
                const cost = costs[offset] + epsilon * @as(f64, @floatFromInt(offset));
                const reduced = minimum + cost - u[row] - v[column];
                if (!std.math.isFinite(reduced)) return error.InvalidAssignmentCost;
                if (reduced < distance[column]) {
                    path[column] = row;
                    distance[column] = reduced;
                }
                if (distance[column] < lowest or (distance[column] == lowest and row_for_column[column] == none)) {
                    lowest = distance[column];
                    lowest_index = i;
                }
            }
            if (lowest_index == none or !std.math.isFinite(lowest)) return error.InvalidAssignmentCost;
            minimum = lowest;
            const column = remaining[lowest_index];
            if (row_for_column[column] == none) sink = column else row = row_for_column[column];
            visited_columns[column] = true;
            left -= 1;
            remaining[lowest_index] = remaining[left];
        }
        u[current_row] += minimum;
        for (0..n) |i| if (visited_rows[i] and i != current_row) {
            u[i] += minimum - distance[column_for_row[i]];
        };
        for (0..m) |j| if (visited_columns[j]) {
            v[j] -= minimum - distance[j];
        };
        var column = sink;
        while (true) {
            const i = path[column];
            row_for_column[column] = i;
            const previous = column_for_row[i];
            column_for_row[i] = column;
            column = previous;
            if (i == current_row) break;
        }
    }
    const result = try allocator.alloc(Pair, n);
    for (column_for_row, result, 0..) |column, *pair, row| pair.* = if (transposed) .{ .row = column, .column = row } else .{ .row = row, .column = column };
    std.mem.sort(Pair, result, {}, struct {
        fn less(_: void, a: Pair, b: Pair) bool {
            return a.row < b.row;
        }
    }.less);
    return result;
}

test "extraction assignment matches exhaustive minimum for rectangular matrices" {
    const a = std.testing.allocator;
    for (0..128) |seed| {
        var costs: [12]f64 = undefined;
        for (&costs, 0..) |*cost, i| cost.* = @as(f64, @floatFromInt((seed * 37 + i * i * 13 + i * seed * 7) % 19)) - 9;
        const result = try solve(a, &costs, 3, 4, .{});
        defer a.free(result);
        var actual: f64 = 0;
        for (result) |pair| actual += costs[pair.row * 4 + pair.column];
        var best = std.math.inf(f64);
        for (0..4) |i| for (0..4) |j| {
            if (i == j) continue;
            for (0..4) |k| if (k != i and k != j) {
                best = @min(best, costs[i] + costs[4 + j] + costs[8 + k]);
            };
        };
        try std.testing.expectEqual(best, actual);
    }
}

test "extraction assignment stable ties transpose limits and ownership" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const result = try solve(a, &.{ 0, 0, 0, 0, 0, 0 }, 3, 2, .{});
            defer a.free(result);
            try std.testing.expectEqualSlices(Pair, &.{ .{ .row = 0, .column = 0 }, .{ .row = 1, .column = 1 } }, result);
            const empty = try solve(a, &.{}, 0, 3, .{});
            defer a.free(empty);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    try std.testing.expectError(error.InvalidAssignmentCost, solve(std.testing.allocator, &.{std.math.nan(f64)}, 1, 1, .{}));
    try std.testing.expectError(error.ExtractionAssignmentLimitExceeded, solve(std.testing.allocator, &.{1}, 1, 1, .{ .max_work = 0 }));
}
