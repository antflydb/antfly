// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

/// Coordinate maps for explicit broadcasts and three-way selection. Inputs
/// remain dense and unexpanded; adjacent compatible axes share one division.
pub const SelectionPlan = struct {
    shape: [8]i64 = @splat(1),
    rank: usize,
    count: usize = 1,
    counts: [3]usize = @splat(1),
    axes: usize = 0,
    divisors: [8]usize = @splat(1),
    strides: [3][8]usize = @splat(@splat(0)),

    pub fn broadcast(input: []const i64, target: []const i64, mapping: []const u8) !SelectionPlan {
        if (input.len > 8 or target.len > 8 or mapping.len != input.len) return error.InvalidTensorShape;
        var p = SelectionPlan{ .rank = target.len };
        @memcpy(p.shape[0..target.len], target);
        var used: [8]bool = @splat(false);
        var stride: usize = 1;
        var i = input.len;
        while (i > 0) {
            i -= 1;
            const axis = mapping[i];
            if (axis >= target.len or used[axis] or input[i] < 0) return error.InvalidTensorShape;
            used[axis] = true;
            if (p.shape[axis] < 0) p.shape[axis] = input[i];
            if (input[i] != 1 and input[i] != p.shape[axis]) return error.ShapeMismatch;
            p.strides[0][axis] = if (input[i] == 1) 0 else stride;
            stride = try std.math.mul(usize, stride, @intCast(input[i]));
        }
        p.counts[0] = stride;
        try p.finish();
        return p;
    }

    pub fn where(condition: []const i64, yes: []const i64, no: []const i64) !SelectionPlan {
        const branches = try Plan.init(yes, no);
        const output = try Plan.init(condition, branches.shape[0..branches.rank]);
        var p = SelectionPlan{ .rank = output.rank, .shape = output.shape };
        for ([_][]const i64{ condition, yes, no }, 0..) |input, operand| {
            var stride: usize = 1;
            var i = input.len;
            while (i > 0) {
                i -= 1;
                const axis = p.rank - input.len + i;
                p.strides[operand][axis] = if (input[i] == 1) 0 else stride;
                stride = try std.math.mul(usize, stride, @intCast(input[i]));
            }
            p.counts[operand] = stride;
        }
        try p.finish();
        return p;
    }

    fn finish(p: *SelectionPlan) !void {
        var extents: [8]usize = @splat(1);
        for (p.shape[0..p.rank], 0..) |dim, axis| {
            if (dim < 0) return error.InvalidTensorShape;
            const extent: usize = @intCast(dim);
            p.count = try std.math.mul(usize, p.count, extent);
            if (dim == 1) continue;
            var merge = p.axes > 0;
            if (merge) for (0..3) |operand| {
                if (p.strides[operand][p.axes - 1] != try std.math.mul(usize, p.strides[operand][axis], extent)) merge = false;
            };
            if (merge) {
                extents[p.axes - 1] = try std.math.mul(usize, extents[p.axes - 1], extent);
                for (0..3) |operand| p.strides[operand][p.axes - 1] = p.strides[operand][axis];
            } else {
                extents[p.axes] = extent;
                for (0..3) |operand| p.strides[operand][p.axes] = p.strides[operand][axis];
                p.axes += 1;
            }
        }
        var divisor: usize = 1;
        var axis = p.axes;
        while (axis > 0) {
            axis -= 1;
            p.divisors[axis] = divisor;
            divisor = try std.math.mul(usize, divisor, extents[axis]);
        }
    }

    pub fn identity(p: *const SelectionPlan) bool {
        return p.count == p.counts[0] and (p.count <= 1 or (p.axes == 1 and p.strides[0][0] == 1));
    }

    pub fn offsets(p: *const SelectionPlan, index: usize) [3]usize {
        if (p.axes == 1) return .{ index * p.strides[0][0], index * p.strides[1][0], index * p.strides[2][0] };
        var result: [3]usize = @splat(0);
        var remaining = index;
        for (0..p.axes) |axis| {
            const coord = remaining / p.divisors[axis];
            remaining %= p.divisors[axis];
            for (0..3) |operand| result[operand] += coord * p.strides[operand][axis];
        }
        return result;
    }
};

test "selection plan coalesces maps and preserves explicit permutations" {
    const contiguous = try SelectionPlan.broadcast(&.{ 2, 3, 4 }, &.{ 1, 2, 3, 4 }, &.{ 1, 2, 3 });
    try std.testing.expect(contiguous.identity());
    try std.testing.expectEqual(@as(usize, 1), contiguous.axes);
    const transpose = try SelectionPlan.broadcast(&.{ 2, 3 }, &.{ 3, 2 }, &.{ 1, 0 });
    try std.testing.expect(!transpose.identity());
    for ([_]usize{ 0, 3, 1, 4, 2, 5 }, 0..) |expected, i| try std.testing.expectEqual(expected, transpose.offsets(i)[0]);
    const eight = try SelectionPlan.where(&.{ 2, 1, 2, 1, 2, 1, 2, 1 }, &.{ 1, 2, 1, 2, 1, 2, 1, 2 }, &.{});
    try std.testing.expectEqual(@as(usize, 256), eight.count);
    try std.testing.expectEqual(@as(usize, 8), eight.axes);
    const scalar = try SelectionPlan.broadcast(&.{}, &.{ 2, 3 }, &.{});
    try std.testing.expectEqual(@as(usize, 0), scalar.offsets(5)[0]);
    const empty = try SelectionPlan.where(&.{ 0, 3 }, &.{ 1, 3 }, &.{});
    try std.testing.expectEqual(@as(usize, 0), empty.count);
    try std.testing.expectError(error.ShapeMismatch, SelectionPlan.where(&.{2}, &.{3}, &.{}));
    try std.testing.expectError(error.InvalidTensorShape, SelectionPlan.broadcast(&.{2}, &.{ 2, 3 }, &.{}));
    try std.testing.expectError(error.InvalidTensorShape, SelectionPlan.broadcast(&.{2}, &.{ 2, 3 }, &.{2}));
    try std.testing.expectError(error.InvalidTensorShape, SelectionPlan.broadcast(&.{}, &.{-1}, &.{}));
    try std.testing.expectError(error.Overflow, SelectionPlan.broadcast(&.{}, &.{ std.math.maxInt(i64), 3 }, &.{}));
}

/// Dense trailing-axis broadcasting, shared by exact CPU and Metal operators.
/// Zero strides repeat operands without allocating expanded input tensors.
pub const Plan = struct {
    shape: [8]i64 = @splat(1),
    rank: usize,
    count: usize,
    lhs_count: usize,
    rhs_count: usize,
    axes: usize = 0,
    extents: [8]usize = @splat(1),
    divisors: [8]usize = @splat(1),
    lhs_strides: [8]usize = @splat(0),
    rhs_strides: [8]usize = @splat(0),

    pub fn init(lhs: []const i64, rhs: []const i64) !Plan {
        if (lhs.len > 8 or rhs.len > 8) return error.InvalidTensorShape;
        var plan = Plan{ .rank = @max(lhs.len, rhs.len), .count = 1, .lhs_count = 1, .rhs_count = 1 };
        var ls: [8]usize = @splat(0);
        var rs: [8]usize = @splat(0);
        var axis = plan.rank;
        while (axis > 0) {
            axis -= 1;
            const l = if (axis + lhs.len >= plan.rank) lhs[axis + lhs.len - plan.rank] else 1;
            const r = if (axis + rhs.len >= plan.rank) rhs[axis + rhs.len - plan.rank] else 1;
            if (l < 0 or r < 0) return error.InvalidTensorShape;
            if (l != r and l != 1 and r != 1) return error.ShapeMismatch;
            // max(0, 1) is incorrect: broadcasting an empty axis stays empty.
            const dim = if (l == 1) r else l;
            plan.shape[axis] = dim;
            ls[axis] = if (l == 1) 0 else plan.lhs_count;
            rs[axis] = if (r == 1) 0 else plan.rhs_count;
            plan.lhs_count = try std.math.mul(usize, plan.lhs_count, @intCast(l));
            plan.rhs_count = try std.math.mul(usize, plan.rhs_count, @intCast(r));
            plan.count = try std.math.mul(usize, plan.count, @intCast(dim));
        }
        if (plan.count == 0) return plan;
        // Collapse contiguous axes (including repeated axes) to reduce integer
        // division in the general kernel. Keep the original output shape.
        for (plan.shape[0..plan.rank], 0..) |dim, i| {
            if (dim == 1) continue;
            const extent: usize = @intCast(dim);
            if (plan.axes > 0) {
                const previous = plan.axes - 1;
                if (plan.lhs_strides[previous] == try std.math.mul(usize, ls[i], extent) and
                    plan.rhs_strides[previous] == try std.math.mul(usize, rs[i], extent))
                {
                    plan.extents[previous] = try std.math.mul(usize, plan.extents[previous], extent);
                    plan.lhs_strides[previous] = ls[i];
                    plan.rhs_strides[previous] = rs[i];
                    continue;
                }
            }
            plan.extents[plan.axes] = extent;
            plan.lhs_strides[plan.axes] = ls[i];
            plan.rhs_strides[plan.axes] = rs[i];
            plan.axes += 1;
        }
        var divisor: usize = 1;
        axis = plan.axes;
        while (axis > 0) {
            axis -= 1;
            plan.divisors[axis] = divisor;
            divisor *= plan.extents[axis];
        }
        return plan;
    }

    pub fn flat(self: Plan) bool {
        return (self.lhs_count == self.count or self.lhs_count == 1) and
            (self.rhs_count == self.count or self.rhs_count == 1);
    }

    pub fn offsets(self: *const Plan, index: usize) struct { lhs: usize, rhs: usize } {
        if (self.flat()) return .{ .lhs = if (self.lhs_count == 1) 0 else index, .rhs = if (self.rhs_count == 1) 0 else index };
        var remaining = index;
        var lhs: usize = 0;
        var rhs: usize = 0;
        for (0..self.axes) |axis| {
            const coord = remaining / self.divisors[axis];
            remaining %= self.divisors[axis];
            lhs += coord * self.lhs_strides[axis];
            rhs += coord * self.rhs_strides[axis];
        }
        return .{ .lhs = lhs, .rhs = rhs };
    }
};

test "broadcast plan coalesces contiguous axes and validates dimensions" {
    const plan = try Plan.init(&.{ 2, 3, 4, 5 }, &.{ 4, 5 });
    try std.testing.expectEqual(@as(usize, 2), plan.axes);
    try std.testing.expectEqualSlices(usize, &.{ 6, 20 }, plan.extents[0..plan.axes]);
    for (0..plan.count) |i| {
        const offsets = plan.offsets(i);
        try std.testing.expectEqual(i, offsets.lhs);
        try std.testing.expectEqual(i % 20, offsets.rhs);
    }
    const scalar = try Plan.init(&.{}, &.{});
    try std.testing.expectEqual(@as(usize, 1), scalar.count);
    try std.testing.expect(scalar.flat());
    const empty = try Plan.init(&.{ 0, 3 }, &.{ 1, 3 });
    try std.testing.expectEqual(@as(usize, 0), empty.count);
    try std.testing.expectError(error.ShapeMismatch, Plan.init(&.{ 0, 3 }, &.{ 2, 3 }));
    try std.testing.expectError(error.InvalidTensorShape, Plan.init(&.{-1}, &.{1}));
    try std.testing.expectError(error.InvalidTensorShape, Plan.init(&.{ 1, 1, 1, 1, 1, 1, 1, 1, 1 }, &.{}));
    try std.testing.expectError(error.Overflow, Plan.init(&.{ std.math.maxInt(i64), 3 }, &.{}));
}
