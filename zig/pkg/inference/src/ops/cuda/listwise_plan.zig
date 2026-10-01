// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One geometry/accounting contract for listwise admission and CUDA dispatch.
const std = @import("std");
const reduction = @import("reduction_plan.zig");
pub const Plan = struct {
    row: reduction.Plan,
    query: ?reduction.Plan,
    scratch_bytes: usize,
    device_bytes: usize,
    upload_bytes: usize,
    readback_bytes: usize,
    largest_upload_bytes: usize,

    pub fn init(batch: usize, queries: usize, candidates: usize, reduce_queries: bool, device: reduction.Device) !Plan {
        const rows = try std.math.mul(usize, batch, queries);
        const n = try std.math.mul(usize, rows, candidates);
        if (n == 0 or n > std.math.maxInt(i32) or rows > std.math.maxInt(i32) / 2) return error.InvalidListwiseLossMathShape;
        const out = if (reduce_queries) try std.math.mul(usize, batch, candidates) else n;
        const row = try reduction.Plan.init(&.{ @intCast(rows), @intCast(candidates) }, &.{1}, false, device);
        const query: ?reduction.Plan = if (reduce_queries)
            try reduction.Plan.init(&.{ @intCast(batch), @intCast(queries), @intCast(candidates) }, &.{1}, false, device)
        else
            null;
        const scratch = @max(row.scratch_bytes, if (query) |value| value.scratch_bytes else 0);
        const elements = try std.math.add(usize, try std.math.mul(usize, try std.math.add(usize, n, rows), 5), if (reduce_queries) out else 0);
        return .{ .row = row, .query = query, .scratch_bytes = scratch, .device_bytes = try std.math.add(usize, try std.math.mul(usize, elements, 4), scratch), .upload_bytes = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, n, 2), try std.math.mul(usize, rows, 3)), 4), .readback_bytes = try std.math.mul(usize, out, 4), .largest_upload_bytes = try std.math.mul(usize, @max(n, rows * 2), 4) };
    }
};

test "CUDA listwise plan bounds every temporary and transfer" {
    const device = reduction.Device{ .multiprocessors = 58, .max_threads_per_multiprocessor = 1536 };
    const plain = try Plan.init(2, 7, 31, false, device);
    const shared = try Plan.init(2, 7, 31, true, device);
    try std.testing.expectEqual(@as(usize, (5 * 434 + 5 * 14) * 4), plain.device_bytes);
    try std.testing.expectEqual(plain.device_bytes + 62 * 4, shared.device_bytes);
    try std.testing.expectEqual(@as(usize, (2 * 434 + 3 * 14) * 4), shared.upload_bytes);
    try std.testing.expectEqual(@as(usize, 62 * 4), shared.readback_bytes);
    const large = try Plan.init(1, 1, 262147, false, device);
    try std.testing.expect(large.row.config.ctas > 1);
    try std.testing.expectEqual((5 * 262147 + 5) * 4 + large.scratch_bytes, large.device_bytes);
    try std.testing.expectError(error.InvalidListwiseLossMathShape, Plan.init(1, 0, 2, true, device));
}
