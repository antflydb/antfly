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
