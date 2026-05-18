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

pub const max_rank = 8;

pub const DType = enum(u8) {
    f32,
    f16,
    bf16,
    f64,
    i8,
    i16,
    i32,
    i64,
    u8,
    bool_,

    pub fn byteSize(self: DType) usize {
        return switch (self) {
            .f32, .i32 => 4,
            .f16, .bf16, .i16 => 2,
            .f64, .i64 => 8,
            .i8, .u8, .bool_ => 1,
        };
    }
};

/// Constraint on a single dimension — fixed, bounded, or enumerated.
/// Used for ahead-of-time compilation targets that
/// need static or bounded buffer allocation.
pub const ShapeConstraint = union(enum) {
    /// Exactly this size (static dimension).
    fixed: i64,
    /// Dynamic but guaranteed <= max. Enables memory planning with
    /// max-size buffers and runtime tracking of actual size.
    bounded: struct { max: i64 },
    /// One of a fixed set of sizes.
    /// ANE only runs efficiently with enumerated shapes, not ranges.
    enumerated: []const i64,

    /// Return the maximum buffer size needed to accommodate this
    /// constraint. Used for memory planning.
    pub fn maxSize(self: ShapeConstraint) i64 {
        return switch (self) {
            .fixed => |v| v,
            .bounded => |b| b.max,
            .enumerated => |values| blk: {
                var m: i64 = 0;
                for (values) |v| m = @max(m, v);
                break :blk m;
            },
        };
    }

    /// Bucket a runtime value to the nearest valid size for this
    /// constraint. For fixed, returns the fixed value. For bounded,
    /// returns next-power-of-2 clamped to max. For enumerated,
    /// returns the smallest enum value >= the runtime value.
    pub fn bucket(self: ShapeConstraint, runtime_value: i64) i64 {
        switch (self) {
            .fixed => |v| return v,
            .bounded => |b| {
                if (runtime_value <= 1) return runtime_value;
                // Next power of 2, clamped to max.
                const v: u32 = @intCast(@min(runtime_value, std.math.maxInt(u32)));
                const pot = @as(i64, 1) << @intCast(@as(u6, @intCast(32 - @clz(v - 1))));
                return @min(pot, b.max);
            },
            .enumerated => |values| {
                // Smallest enum value >= runtime_value.
                var best: i64 = std.math.maxInt(i64);
                for (values) |v| {
                    if (v >= runtime_value and v < best) best = v;
                }
                // No enum value was large enough — fall back to the
                // largest available bucket. Reset before the max-loop;
                // otherwise the previous sentinel (i64.maxInt) survives
                // as the running maximum.
                if (best == std.math.maxInt(i64)) {
                    best = std.math.minInt(i64);
                    for (values) |v| best = @max(best, v);
                }
                return best;
            },
        }
    }
};

/// Compact, stack-allocated tensor shape. Negative dims denote dynamic
/// (symbolic) dimensions that are resolved at execution time.
pub const Shape = struct {
    dtype: DType = .f32,
    dims: [max_rank]i64 = .{0} ** max_rank,
    rank_: u8 = 0,
    /// Optional upper bounds for dynamic dimensions. When bounds[i] > 0
    /// and dims[i] < 0, the dynamic dimension is guaranteed <= bounds[i].
    /// Enables memory planning even for variable-length inputs.
    bounds: [max_rank]i64 = .{0} ** max_rank,

    pub fn init(dtype: DType, dims: []const i64) Shape {
        std.debug.assert(dims.len <= max_rank);
        var s = Shape{ .dtype = dtype, .rank_ = @intCast(dims.len) };
        @memcpy(s.dims[0..dims.len], dims);
        return s;
    }

    /// Create a shape with a bounded dynamic dimension.
    /// `dim_value` should be negative (dynamic), `max_value` is the bound.
    pub fn initBounded(dtype: DType, dims: []const i64, axis: u8, max_value: i64) Shape {
        var s = init(dtype, dims);
        s.bounds[axis] = max_value;
        return s;
    }

    pub fn scalar(dtype: DType) Shape {
        return .{ .dtype = dtype, .rank_ = 0 };
    }

    pub fn rank(self: Shape) u8 {
        return self.rank_;
    }

    pub fn dim(self: Shape, axis: u8) i64 {
        std.debug.assert(axis < self.rank_);
        return self.dims[axis];
    }

    /// Return the bound for a dynamic dimension, or null if unbounded.
    pub fn bound(self: Shape, axis: u8) ?i64 {
        if (axis >= self.rank_) return null;
        if (self.bounds[axis] > 0) return self.bounds[axis];
        return null;
    }

    /// Total number of elements, or null if any dimension is dynamic.
    pub fn numElements(self: Shape) ?i64 {
        if (self.rank_ == 0) return 1;
        var n: i64 = 1;
        for (self.dims[0..self.rank_]) |d| {
            if (d < 0) return null;
            n = std.math.mul(i64, n, d) catch return null;
        }
        return n;
    }

    /// Maximum number of elements using bounds for dynamic dims.
    /// Returns null only if a dynamic dim has no bound.
    pub fn maxElements(self: Shape) ?i64 {
        if (self.rank_ == 0) return 1;
        var n: i64 = 1;
        for (0..self.rank_) |i| {
            const d = self.dims[i];
            if (d < 0) {
                if (self.bounds[i] > 0) {
                    n = std.math.mul(i64, n, self.bounds[i]) catch return null;
                } else {
                    return null;
                }
            } else {
                n = std.math.mul(i64, n, d) catch return null;
            }
        }
        return n;
    }

    pub fn eq(a: Shape, b: Shape) bool {
        if (a.dtype != b.dtype) return false;
        if (a.rank_ != b.rank_) return false;
        return std.mem.eql(i64, a.dims[0..a.rank_], b.dims[0..b.rank_]);
    }

    pub fn format(self: Shape, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("{s}[", .{@tagName(self.dtype)});
        for (0..self.rank_) |i| {
            if (i > 0) try writer.writeAll(", ");
            if (self.dims[i] < 0) {
                if (self.bounds[i] > 0) {
                    try writer.print("?<={d}", .{self.bounds[i]});
                } else {
                    try writer.writeAll("?");
                }
            } else {
                try writer.print("{d}", .{self.dims[i]});
            }
        }
        try writer.writeAll("]");
    }
};

/// Cache for scalar and small tensor constants to avoid duplicate graph nodes.
pub const ConstantCache = struct {
    const Key = struct { dtype: DType, bits: u64 };

    map: std.AutoHashMapUnmanaged(Key, u32),

    pub fn init() ConstantCache {
        return .{ .map = .empty };
    }

    pub fn deinit(self: *ConstantCache, allocator: std.mem.Allocator) void {
        self.map.deinit(allocator);
    }

    pub fn getScalar(self: *const ConstantCache, dtype: DType, value: f64) ?u32 {
        return self.map.get(.{ .dtype = dtype, .bits = @bitCast(value) });
    }

    pub fn putScalar(self: *ConstantCache, allocator: std.mem.Allocator, dtype: DType, value: f64, node_id: u32) !void {
        try self.map.put(allocator, .{ .dtype = dtype, .bits = @bitCast(value) }, node_id);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "Shape.init" {
    const s = Shape.init(.f32, &.{ 2, 3, 4 });
    try std.testing.expectEqual(@as(u8, 3), s.rank());
    try std.testing.expectEqual(@as(i64, 2), s.dim(0));
    try std.testing.expectEqual(@as(i64, 3), s.dim(1));
    try std.testing.expectEqual(@as(i64, 4), s.dim(2));
    try std.testing.expectEqual(@as(?i64, 24), s.numElements());
}

test "Shape.scalar" {
    const s = Shape.scalar(.f32);
    try std.testing.expectEqual(@as(u8, 0), s.rank());
    try std.testing.expectEqual(@as(?i64, 1), s.numElements());
}

test "Shape.eq" {
    const a = Shape.init(.f32, &.{ 2, 3 });
    const b = Shape.init(.f32, &.{ 2, 3 });
    const c = Shape.init(.f32, &.{ 2, 4 });
    const d = Shape.init(.f16, &.{ 2, 3 });
    try std.testing.expect(a.eq(b));
    try std.testing.expect(!a.eq(c));
    try std.testing.expect(!a.eq(d));
}

test "Shape.dynamic dim" {
    const s = Shape.init(.f32, &.{ 2, -1, 4 });
    try std.testing.expectEqual(@as(?i64, null), s.numElements());
    try std.testing.expectEqual(@as(?i64, null), s.maxElements());
}

test "Shape.numElements returns null on overflow" {
    const s = Shape.init(.f32, &.{ std.math.maxInt(i64), 2 });
    try std.testing.expectEqual(@as(?i64, null), s.numElements());
}

test "Shape.maxElements returns null on overflow" {
    const s = Shape.initBounded(.f32, &.{ 2, -1, 4 }, 1, std.math.maxInt(i64));
    try std.testing.expectEqual(@as(?i64, null), s.maxElements());
}

test "Shape.bounded dynamic dim" {
    const s = Shape.initBounded(.f32, &.{ 2, -1, 4 }, 1, 128);
    try std.testing.expectEqual(@as(?i64, null), s.numElements());
    try std.testing.expectEqual(@as(?i64, 2 * 128 * 4), s.maxElements());
    try std.testing.expectEqual(@as(?i64, 128), s.bound(1));
    try std.testing.expectEqual(@as(?i64, null), s.bound(0));
}

test "ShapeConstraint.fixed" {
    const c = ShapeConstraint{ .fixed = 64 };
    try std.testing.expectEqual(@as(i64, 64), c.maxSize());
    try std.testing.expectEqual(@as(i64, 64), c.bucket(32));
    try std.testing.expectEqual(@as(i64, 64), c.bucket(128));
}

test "ShapeConstraint.bounded" {
    const c = ShapeConstraint{ .bounded = .{ .max = 2048 } };
    try std.testing.expectEqual(@as(i64, 2048), c.maxSize());
    try std.testing.expectEqual(@as(i64, 1), c.bucket(1));
    try std.testing.expectEqual(@as(i64, 128), c.bucket(100));
    try std.testing.expectEqual(@as(i64, 2048), c.bucket(1025));
    try std.testing.expectEqual(@as(i64, 2048), c.bucket(4096));
}

test "ShapeConstraint.enumerated" {
    const values = [_]i64{ 32, 64, 128, 256, 512 };
    const c = ShapeConstraint{ .enumerated = &values };
    try std.testing.expectEqual(@as(i64, 512), c.maxSize());
    try std.testing.expectEqual(@as(i64, 32), c.bucket(1));
    try std.testing.expectEqual(@as(i64, 128), c.bucket(65));
    try std.testing.expectEqual(@as(i64, 512), c.bucket(512));
    try std.testing.expectEqual(@as(i64, 512), c.bucket(1000));
}

test "ConstantCache" {
    const allocator = std.testing.allocator;
    var cache = ConstantCache.init();
    defer cache.deinit(allocator);

    try std.testing.expectEqual(@as(?u32, null), cache.getScalar(.f32, 1.0));
    try cache.putScalar(allocator, .f32, 1.0, 42);
    try std.testing.expectEqual(@as(?u32, 42), cache.getScalar(.f32, 1.0));
    try std.testing.expectEqual(@as(?u32, null), cache.getScalar(.f32, 2.0));
    try std.testing.expectEqual(@as(?u32, null), cache.getScalar(.f16, 1.0));
}
