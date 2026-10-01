// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared allocation and transfer accounting for scalar record loss dispatch.
const std = @import("std");
const reduction = @import("reduction_plan.zig");
pub const Plan = struct {
    row: reduction.Plan,
    scratch_bytes: usize,
    device_bytes: usize,
    upload_bytes: usize,
    readback_bytes: usize,
    largest_upload_bytes: usize,
    pub fn init(rows: usize, width: usize, device: reduction.Device) !Plan {
        const n = try std.math.mul(usize, rows, width);
        if (n == 0 or n > std.math.maxInt(i32)) return error.InvalidRecordLossMathShape;
        const row = try reduction.Plan.init(&.{ @intCast(rows), @intCast(width) }, &.{1}, false, device);
        // x, masks, logp, exp terms, logp VJP, result; target columns,
        // seeds, target sums, scalar losses; plus reference-reduction scratch.
        const elements = try std.math.add(usize, try std.math.mul(usize, n, 6), try std.math.mul(usize, rows, 4));
        return .{ .row = row, .scratch_bytes = row.scratch_bytes, .device_bytes = try std.math.add(usize, try std.math.mul(usize, elements, 4), row.scratch_bytes), .upload_bytes = try std.math.mul(usize, try std.math.add(usize, n, rows), 8), .readback_bytes = try std.math.mul(usize, try std.math.add(usize, n, rows), 4), .largest_upload_bytes = try std.math.mul(usize, n, 4) };
    }
};

test "CUDA record loss plan accounts for gradients scalars and reduction scratch" {
    const device = reduction.Device{ .multiprocessors = 58, .max_threads_per_multiprocessor = 1536 };
    const small = try Plan.init(7, 193, device);
    try std.testing.expectEqual(@as(usize, (6 * 7 * 193 + 4 * 7) * 4), small.device_bytes);
    try std.testing.expectEqual(@as(usize, (7 * 193 + 7) * 8), small.upload_bytes);
    try std.testing.expectEqual(@as(usize, (7 * 193 + 7) * 4), small.readback_bytes);
    const large = try Plan.init(1, 262147, device);
    try std.testing.expect(large.scratch_bytes > 0);
    try std.testing.expectEqual(@as(usize, (6 * 262147 + 4) * 4) + large.scratch_bytes, large.device_bytes);
    try std.testing.expectError(error.InvalidRecordLossMathShape, Plan.init(0, 3, device));
}
