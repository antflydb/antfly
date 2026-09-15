// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit resident primitives for retained training. This interface never
//! permits host math, tensor readback, or an implicit device upload. The only
//! host inputs are the two named upload requests. It is deliberately smaller
//! than the generic graph interpreter's primitive surface.
const std = @import("std");
const CT = @import("../graph/backend_contracts.zig").CT;

pub const Limits = struct {
    max_tensor_bytes: usize = 1024 * 1024 * 1024,
    /// Segmented scatter work: dense zero writes plus every submitted value.
    max_scatter_work: usize = 64 * 1024 * 1024,
    max_index_metadata_bytes: usize = 128 * 1024 * 1024,
};

pub const NormInput = struct { tensor: CT, elem_count: usize };
pub const NormLimits = struct {
    primitive: Limits = .{},
    max_tensors: usize = 4096,
    max_total_elements: usize = 1024 * 1024 * 1024,
    max_partial_bytes: usize = 64 * 1024 * 1024,
};
pub const NormSummary = struct {
    sum_squares: f64,
    norm: f64,
    finite: bool,
    tensor_count: usize,
    partial_bytes: usize,
    download_bytes: usize,
};

pub const Request = union(enum) {
    upload_f32: struct { values: []const f32, shape: []const i32 },
    upload_i32: struct { values: []const i32, shape: []const i32 },
    /// A physically independent buffer, suitable for retained tape captures.
    snapshot: struct { input: CT, shape: []const i32 },
    /// An immutable view. Captures must use snapshot instead of this request.
    reshape: struct { input: CT, shape: []const i32 },
    gather: struct { input: CT, indices: CT, input_shape: []const i64, axis: u8 = 0 },
    scatter_add: struct { values: CT, indices: CT, input_shape: []const i64, output_shape: []const i64, axis: u8 = 0 },
};

pub fn shapeElements(comptime T: type, shape: []const T, limits: Limits) !usize {
    if (shape.len > 8 or limits.max_tensor_bytes == 0 or limits.max_scatter_work == 0)
        return error.InvalidResidentTrainingShape;
    var count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0 or dim > std.math.maxInt(i32)) return error.InvalidResidentTrainingShape;
        count = std.math.mul(usize, count, @intCast(dim)) catch return error.ResourceLimitExceeded;
    }
    if (count > std.math.maxInt(i32) or
        (std.math.mul(usize, count, 4) catch return error.ResourceLimitExceeded) > limits.max_tensor_bytes)
        return error.ResourceLimitExceeded;
    return count;
}

/// Upload-time proof for immutable integer leaves. Keeping the extrema avoids
/// synchronizing or downloading the index tensor at each gather/scatter use.
pub const IndexBounds = struct {
    minimum: i32,
    maximum: i32,

    pub fn of(values: []const i32) !IndexBounds {
        if (values.len == 0) return error.InvalidResidentTrainingShape;
        var result = IndexBounds{ .minimum = values[0], .maximum = values[0] };
        for (values[1..]) |value| {
            result.minimum = @min(result.minimum, value);
            result.maximum = @max(result.maximum, value);
        }
        return result;
    }

    pub fn validate(self: IndexBounds, rows: usize) !void {
        if (rows == 0 or rows > std.math.maxInt(i32)) return error.InvalidResidentTrainingShape;
        const count: i64 = @intCast(rows);
        if (@as(i64, self.minimum) < -count or @as(i64, self.maximum) >= count)
            return error.IndexOutOfBounds;
    }
};

pub fn scatterWork(output_elements: usize, input_elements: usize, limits: Limits) !usize {
    const work = std.math.add(usize, output_elements, input_elements) catch return error.ResourceLimitExceeded;
    if (output_elements == 0 or input_elements == 0) return error.InvalidResidentTrainingShape;
    if (work > limits.max_scatter_work) return error.ResourceLimitExceeded;
    return work;
}

test "resident training primitive admission uses exact integer bounds and scatter work" {
    const bounds = try IndexBounds.of(&.{ -2, 16_777_217, 16_777_216 });
    try std.testing.expectEqual(@as(i32, 16_777_217), bounds.maximum);
    try bounds.validate(16_777_218);
    try std.testing.expectError(error.IndexOutOfBounds, bounds.validate(16_777_217));
    try std.testing.expectError(error.IndexOutOfBounds, (try IndexBounds.of(&.{std.math.minInt(i32)})).validate(5));
    try std.testing.expectEqual(@as(usize, 1), try shapeElements(i32, &.{}, .{}));
    try std.testing.expectError(error.InvalidResidentTrainingShape, shapeElements(i64, &.{ -1, 3 }, .{}));
    try std.testing.expectError(error.InvalidResidentTrainingShape, shapeElements(i64, &.{ @as(i64, std.math.maxInt(i32)) + 1, 1 }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, shapeElements(i32, &.{ 4096, 4096 }, .{ .max_tensor_bytes = 1024 }));
    try std.testing.expectError(error.ResourceLimitExceeded, scatterWork(1024, 1024, .{ .max_scatter_work = 1000 }));
}
