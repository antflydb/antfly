// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Exact logical temporary storage for F32 row selection statistics. Physical
//! cache retention/padding remains the existing backend buffer owner's scope.
const std = @import("std");
pub const Geometry = struct {
    partial_count: u32,
    partial_bytes: usize,
    ids_bytes: usize,
    output_bytes: usize = 16 * @sizeOf(f32),
    workspace_bytes: usize,
    pub fn init(width: usize, suppression_count: usize) !Geometry {
        if (width == 0 or width > std.math.maxInt(u32) or suppression_count > width) return error.UnsupportedRowSelectionGeometry;
        const count = std.math.divCeil(usize, width, 1024) catch return error.ResourceLimitExceeded;
        const partial = std.math.mul(usize, count, 16 * @sizeOf(f32)) catch return error.ResourceLimitExceeded;
        const ids = std.math.mul(usize, @max(suppression_count, 1), @sizeOf(i32)) catch return error.ResourceLimitExceeded;
        const total = std.math.add(usize, partial, ids) catch return error.ResourceLimitExceeded;
        return .{ .partial_count = @intCast(count), .partial_bytes = partial, .ids_bytes = ids, .workspace_bytes = std.math.add(usize, total, 16 * @sizeOf(f32)) catch return error.ResourceLimitExceeded };
    }
};

pub fn completedFiniteResult(stats: *const [16]f32) bool {
    // Every raw value participates in raw LSE. NaN/+inf/all-negative-inf
    // therefore decline AFTER checked completion, preserving the caller's
    // existing full-logits semantics for those exceptional rows.
    return std.math.isFinite(stats[12]) and std.math.isFinite(stats[13]) and stats[13] > 0 and std.math.isFinite(stats[0]);
}

test "row selection statistics exact full-vocabulary workspace and exceptional result decline" {
    const small = try Geometry.init(51865, 51865);
    try std.testing.expectEqual(@as(u32, 51), small.partial_count);
    try std.testing.expectEqual(@as(usize, 3264), small.partial_bytes);
    try std.testing.expectEqual(@as(usize, 207460), small.ids_bytes);
    try std.testing.expectEqual(@as(usize, 210788), small.workspace_bytes);
    const empty_ids = try Geometry.init(1025, 0);
    try std.testing.expectEqual(@as(usize, 4), empty_ids.ids_bytes);
    try std.testing.expectEqual(@as(usize, 196), empty_ids.workspace_bytes);
    try std.testing.expectError(error.UnsupportedRowSelectionGeometry, Geometry.init(0, 0));
    try std.testing.expectError(error.UnsupportedRowSelectionGeometry, Geometry.init(8, 9));
    var stats: [16]f32 = @splat(0);
    stats[0] = 2;
    stats[12] = 3;
    stats[13] = 1;
    try std.testing.expect(completedFiniteResult(&stats));
    stats[13] = std.math.nan(f32);
    try std.testing.expect(!completedFiniteResult(&stats));
    stats[13] = 1;
    stats[12] = std.math.inf(f32);
    try std.testing.expect(!completedFiniteResult(&stats));
}
