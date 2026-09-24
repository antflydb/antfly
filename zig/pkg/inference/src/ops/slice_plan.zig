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

/// Dense affine slice map. Coalesced axes preserve signed steps without
/// materializing indices. Concrete zero dimensions remain empty.
pub const Plan = struct {
    shape: [8]i64 = @splat(1),
    rank: usize,
    count: usize = 1,
    input_count: usize = 1,
    base: i64 = 0,
    axes: usize = 0,
    divisors: [8]usize = @splat(1),
    strides: [8]i64 = @splat(0),

    pub fn init(input: []const i64, starts: []const i64, limits: []const i64, steps: []const i64, declared: []const i64) !Plan {
        const rank = input.len;
        if (rank > 8 or starts.len != rank or limits.len != rank or steps.len != rank or declared.len != rank) return error.InvalidTensorShape;
        var p = Plan{ .rank = rank };
        var raw: [8]i64 = undefined;
        var axis = rank;
        while (axis > 0) {
            axis -= 1;
            const dim = input[axis];
            const step = steps[axis];
            if (dim < 0 or step == 0) return error.InvalidTensorShape;
            var start: i128 = starts[axis];
            var limit: i128 = limits[axis];
            if (start < 0) start += dim;
            if (limit < 0) {
                limit = if (declared[axis] < 0 and starts[axis] == 0 and step == 1) dim else limit + dim;
            }
            if (step > 0) {
                start = std.math.clamp(start, 0, dim);
                limit = std.math.clamp(limit, 0, dim);
            } else {
                start = std.math.clamp(start, 0, @max(dim - 1, 0));
                limit = std.math.clamp(limit, -1, dim - 1);
            }
            const distance = if (step > 0) limit - start else start - limit;
            const magnitude: i128 = if (step > 0) step else -@as(i128, step);
            const extent: i64 = if (dim == 0 or distance <= 0) 0 else @intCast(@divTrunc(distance - 1, magnitude) + 1);
            p.shape[axis] = extent;
            const stride = std.math.cast(i64, p.input_count) orelse return error.InvalidTensorShape;
            p.base = try std.math.add(i64, p.base, try std.math.mul(i64, @intCast(start), stride));
            // Singleton axes never advance: don't overflow on ONNX's INT64_MIN step.
            raw[axis] = if (extent <= 1) 0 else try std.math.mul(i64, stride, step);
            p.input_count = try std.math.mul(usize, p.input_count, @intCast(dim));
            p.count = try std.math.mul(usize, p.count, @intCast(extent));
        }
        if (p.count == 0) return p;
        var extents: [8]usize = @splat(1);
        for (p.shape[0..rank], 0..) |dim, i| {
            if (dim == 1) continue;
            const extent: usize = @intCast(dim);
            if (p.axes > 0 and p.strides[p.axes - 1] == std.math.mul(i64, raw[i], dim) catch return error.InvalidTensorShape) {
                extents[p.axes - 1] = try std.math.mul(usize, extents[p.axes - 1], extent);
                p.strides[p.axes - 1] = raw[i];
            } else {
                extents[p.axes] = extent;
                p.strides[p.axes] = raw[i];
                p.axes += 1;
            }
        }
        var divisor: usize = 1;
        axis = p.axes;
        while (axis > 0) {
            axis -= 1;
            p.divisors[axis] = divisor;
            divisor = try std.math.mul(usize, divisor, extents[axis]);
        }
        return p;
    }

    pub fn contiguous(p: Plan) bool {
        return p.count <= 1 or (p.axes == 1 and p.strides[0] == 1);
    }

    pub fn offset(p: *const Plan, index: usize) usize {
        var result = p.base;
        var remaining = index;
        for (0..p.axes) |axis| {
            const coordinate = remaining / p.divisors[axis];
            remaining %= p.divisors[axis];
            result += @as(i64, @intCast(coordinate)) * p.strides[axis];
        }
        return @intCast(result);
    }
};

test "typed slice plan validates and coalesces signed affine maps" {
    const contiguous = try Plan.init(&.{ 2, 3, 4 }, &.{ 1, 0, 0 }, &.{ 2, 3, 4 }, &.{ 1, 1, 1 }, &.{ 2, 3, 4 });
    try std.testing.expect(contiguous.contiguous());
    try std.testing.expectEqual(@as(usize, 1), contiguous.axes);
    try std.testing.expectEqual(@as(i64, 12), contiguous.base);
    const reverse = try Plan.init(&.{ 2, 3, 4 }, &.{ 1, 2, 3 }, &.{ -3, -4, -5 }, &.{ -1, -1, -1 }, &.{ 2, 3, 4 });
    try std.testing.expectEqual(@as(usize, 1), reverse.axes);
    for (0..24) |i| try std.testing.expectEqual(23 - i, reverse.offset(i));
    const eight = try Plan.init(&@as([8]i64, @splat(2)), &@as([8]i64, @splat(0)), &@as([8]i64, @splat(2)), &@as([8]i64, @splat(1)), &@as([8]i64, @splat(2)));
    try std.testing.expectEqual(@as(usize, 256), eight.count);
    try std.testing.expectEqual(@as(usize, 1), eight.axes);
    const empty = try Plan.init(&.{ 0, 3 }, &.{ 0, 0 }, &.{ 0, 3 }, &.{ 1, 1 }, &.{ 0, 3 });
    try std.testing.expectEqual(@as(usize, 0), empty.count);
    const symbolic = try Plan.init(&.{3}, &.{0}, &.{-1}, &.{1}, &.{-1});
    try std.testing.expectEqual(@as(usize, 3), symbolic.count);
    const clipped = try Plan.init(&.{3}, &.{std.math.minInt(i64)}, &.{std.math.maxInt(i64)}, &.{1}, &.{3});
    try std.testing.expectEqual(@as(usize, 3), clipped.count);
    try std.testing.expectError(error.InvalidTensorShape, Plan.init(&.{3}, &.{0}, &.{3}, &.{0}, &.{3}));
    try std.testing.expectError(error.InvalidTensorShape, Plan.init(&.{3}, &.{}, &.{3}, &.{1}, &.{3}));
}
