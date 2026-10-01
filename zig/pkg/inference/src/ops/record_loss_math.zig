// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Scalar-record log-softmax / alternative-target logsumexp derivative.
//! Shared training code owns matching, weighting, row ordering and masks.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Request = struct {
    /// Contiguous [rows,width], including ABSENT in column zero. Excluded
    /// candidate values must already equal the forward mask constant -10000.
    logits: []const f32,
    /// Bit zero: live input, bit one: alternative target.
    masks: []const i32,
    /// Index of a maximum target logit per row, or -1 for inactive/list rows.
    target_columns: []const i32,
    seeds: []const f32,
    gradient: []f32,
    /// Unweighted negative target log probability; inactive rows return zero.
    losses: []f32,
    width: usize,
    max_elements: usize,
    control: ?Control = null,

    pub fn validate(self: Request) !void {
        if (self.width == 0 or self.width > std.math.maxInt(i32)) return error.InvalidRecordLossMathShape;
        const rows = self.seeds.len;
        const n = try std.math.mul(usize, rows, self.width);
        if (n > self.max_elements or n > std.math.maxInt(i32)) return error.RecordLossMathLimitExceeded;
        if (self.logits.len != n or self.masks.len != n or self.gradient.len != n or self.target_columns.len != rows or self.losses.len != rows) return error.InvalidRecordLossMathShape;
        if (self.control) |control| try control.check();
        for (self.seeds, self.target_columns, 0..) |seed, column, row| {
            if (row % 4096 == 0) if (self.control) |control| try control.check();
            if (!std.math.isFinite(seed)) return error.NonFiniteRecordLossMath;
            if (column < -1 or (column >= 0 and @as(usize, @intCast(column)) >= self.width) or (column == -1 and seed != 0)) return error.InvalidRecordLossMathTarget;
            if (column >= 0 and self.masks[row * self.width + @as(usize, @intCast(column))] & 2 == 0) return error.InvalidRecordLossMathTarget;
        }
        for (self.logits, self.masks, 0..) |value, mask, i| {
            if (i % 4096 == 0) if (self.control) |control| try control.check();
            if (!std.math.isFinite(value)) return error.NonFiniteRecordLossMath;
            if (mask < 0 or mask > 3 or (mask & 2 != 0 and mask & 1 == 0) or (i % self.width == 0 and mask & 1 == 0) or (mask & 1 == 0 and value != -10000)) return error.InvalidRecordLossMathMask;
            const target = self.target_columns[i / self.width];
            if (target >= 0 and mask & 2 != 0 and value > self.logits[(i / self.width) * self.width + @as(usize, @intCast(target))]) return error.InvalidRecordLossMathTarget;
        }
    }
};
pub const Backend = struct {
    ptr: *anyopaque,
    apply: *const fn (*anyopaque, *const Request) anyerror!void,
};

test "record loss math rejects invalid targets masks shapes and budgets" {
    var gradient: [3]f32 = undefined;
    var losses: [1]f32 = undefined;
    var request = Request{ .logits = &.{ 0, 1, -10000 }, .masks = &.{ 1, 3, 0 }, .target_columns = &.{1}, .seeds = &.{0.3}, .gradient = &gradient, .losses = &losses, .width = 3, .max_elements = 3 };
    try request.validate();
    request.target_columns = &.{0};
    try std.testing.expectError(error.InvalidRecordLossMathTarget, request.validate());
    request.target_columns = &.{-1};
    try std.testing.expectError(error.InvalidRecordLossMathTarget, request.validate());
    request.seeds = &.{0};
    try request.validate();
    request.logits = &.{ 0, 1, 0 };
    try std.testing.expectError(error.InvalidRecordLossMathMask, request.validate());
    request.logits = &.{ 0, 1, -10000 };
    request.max_elements = 2;
    try std.testing.expectError(error.RecordLossMathLimitExceeded, request.validate());
    request.max_elements = 3;
    request.width = 0;
    try std.testing.expectError(error.InvalidRecordLossMathShape, request.validate());
}
