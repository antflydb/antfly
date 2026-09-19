// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Backend arithmetic for shared, validated noisy-OR consistency cotangents.
//! Stable integer grouping is shared with resident gather backward.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const grouping = @import("resident_training_groups.zig");

pub const Plan = struct {
    upload_bytes: usize,
    readback_bytes: usize,
    largest_upload_bytes: usize,
    device_bytes: usize,
    grouping: grouping.Admission,
    work: usize,

    pub fn init(n: usize, m: usize, group_counts: [2]usize) !Plan {
        if (n == 0 or m == 0 or @max(n, m) > std.math.maxInt(i32)) return error.InvalidConsistencyLossMathShape;
        for (group_counts) |count| if (count == 0 or count > @min(n, m)) return error.InvalidConsistencyLossMathShape;
        const admitted = try grouping.plan(n, m, .{});
        const descriptors = try add(try mul(2, n), try add(try mul(2, try add(group_counts[0], group_counts[1])), 2));
        const uploads = try add(try add(try mul(4, n), try mul(4, m)), descriptors);
        return .{
            .upload_bytes = try mul(4, uploads),
            .readback_bytes = try mul(4, try add(n, try mul(2, m))),
            .largest_upload_bytes = try mul(4, @max(@max(n, m), try add(@max(group_counts[0], group_counts[1]), 1))),
            .device_bytes = try mul(4, try add(uploads, try add(try mul(3, n), try mul(6, m)))),
            .grouping = admitted,
            .work = try add(try mul(2, admitted.sort_work), try mul(64, try add(n, m))),
        };
    }
    pub fn upper(n: usize, m: usize) !Plan {
        return init(n, m, @splat(@min(n, m)));
    }
};
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b);
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}

pub const Request = struct {
    pairs: []const f32,
    margins: [2][]const f32,
    valid: []const i32,
    /// Global canonical [B,Q,N] destinations, including masked candidates.
    indices: [2][]const i32,
    keep: [2][]const i32,
    groups: [2]grouping.Grouped,
    counts: [2]usize,
    weight: f32,
    gradients: [3][]f32,
    max_elements: usize,
    control: ?Control = null,

    pub fn plan(self: Request) !Plan {
        return Plan.init(self.pairs.len, self.margins[0].len, .{ self.groups[0].rows.len, self.groups[1].rows.len });
    }
    pub fn validate(self: Request) !void {
        const n = self.pairs.len;
        const m = self.margins[0].len;
        if (@max(n, m) > self.max_elements) return error.ConsistencyLossMathLimitExceeded;
        if (self.valid.len != n or self.gradients[0].len != n or !std.math.isFinite(self.weight) or self.weight < 0)
            return error.InvalidConsistencyLossMathShape;
        _ = try self.plan();
        if (self.control) |control| try control.check();
        for (self.pairs, self.valid, 0..) |x, mask, i| {
            if (i % 4096 == 0) if (self.control) |control| try control.check();
            if (mask < 0 or mask > 1) return error.InvalidConsistencyLossMathMask;
            if (mask != 0 and !std.math.isFinite(x)) return error.NonFiniteConsistencyLossMath;
        }
        for (0..2) |d| {
            if (self.margins[d].len != m or self.gradients[d + 1].len != m or self.keep[d].len != m or self.indices[d].len != n)
                return error.InvalidConsistencyLossMathShape;
            const g = self.groups[d];
            if (g.order.len != n or g.offsets.len != g.rows.len + 1 or g.offsets[0] != 0 or g.offsets[g.rows.len] != n or g.output_rows != m)
                return error.InvalidConsistencyLossMathGroups;
            for (self.indices[d]) |index| if (index < 0 or index >= m) return error.InvalidConsistencyLossMathGroups;
            var kept: usize = 0;
            var group: usize = 0;
            for (self.margins[d], self.keep[d], 0..) |x, mask, row| {
                if (row % 4096 == 0) if (self.control) |control| try control.check();
                if (mask < 0 or mask > 1) return error.InvalidConsistencyLossMathMask;
                if (mask == 1 and !std.math.isFinite(x)) return error.NonFiniteConsistencyLossMath;
                var reached = false;
                if (group < g.rows.len and g.rows[group] == row) {
                    const lo = g.offsets[group];
                    const hi = g.offsets[group + 1];
                    if (lo < 0 or hi <= lo or hi > n) return error.InvalidConsistencyLossMathGroups;
                    var previous: i32 = -1;
                    for (g.order[@intCast(lo)..@intCast(hi)]) |ordinal| {
                        if (ordinal <= previous or ordinal >= n or self.indices[d][@intCast(ordinal)] != row) return error.InvalidConsistencyLossMathGroups;
                        reached = reached or self.valid[@intCast(ordinal)] != 0;
                        previous = ordinal;
                    }
                    group += 1;
                }
                if (mask == 1) {
                    if (!reached) return error.InvalidConsistencyLossMathMask;
                    kept += 1;
                }
            }
            if (group != g.rows.len or self.counts[d] != kept) return error.InvalidConsistencyLossMathGroups;
        }
    }
};
pub const Backend = struct {
    ptr: *anyopaque,
    apply: *const fn (*anyopaque, *const Request) anyerror!void,
};

test "consistency loss admission bounds descriptors temporaries and transfers" {
    const plan = try Plan.upper(65, 14);
    try std.testing.expectEqual(@as(usize, 4 * (6 * 65 + 4 * 14 + 4 * 14 + 2)), plan.upload_bytes);
    try std.testing.expectEqual(@as(usize, 4 * (65 + 2 * 14)), plan.readback_bytes);
    try std.testing.expectEqual(plan.upload_bytes + 4 * (3 * 65 + 6 * 14), plan.device_bytes);
    try std.testing.expectError(error.InvalidConsistencyLossMathShape, Plan.upper(0, 1));
    try std.testing.expectError(error.InvalidConsistencyLossMathShape, Plan.upper(std.math.maxInt(usize), 1));
}

test "consistency loss request rejects invalid routing masks and cancellation" {
    const a = std.testing.allocator;
    var grouped = try grouping.build(a, &.{ 0, 1 }, 2, .{}, null);
    defer grouped.deinit();
    var grad: [2]f32 = undefined;
    var request = Request{ .pairs = &.{ 0, 0 }, .margins = .{ &.{ 0, 0 }, &.{ 0, 0 } }, .valid = &.{ 1, 1 }, .indices = .{ &.{ 0, 1 }, &.{ 0, 1 } }, .keep = .{ &.{ 1, 1 }, &.{ 1, 1 } }, .groups = .{ grouped, grouped }, .counts = .{ 2, 2 }, .weight = 0.1, .gradients = .{ &grad, &grad, &grad }, .max_elements = 2 };
    try request.validate();
    request.groups[0].order = &.{ 0, 0 };
    try std.testing.expectError(error.InvalidConsistencyLossMathGroups, request.validate());
    request.groups[0] = grouped;
    request.indices[1] = &.{ 0, 2 };
    try std.testing.expectError(error.InvalidConsistencyLossMathGroups, request.validate());
    request.indices[1] = &.{ 0, 1 };
    request.counts[0] = 1;
    try std.testing.expectError(error.InvalidConsistencyLossMathGroups, request.validate());
    request.counts[0] = 2;
    request.valid = &.{ 1, 2 };
    try std.testing.expectError(error.InvalidConsistencyLossMathMask, request.validate());
    request.valid = &.{ 1, 1 };
    request.pairs = &.{ 0, std.math.nan(f32) };
    try std.testing.expectError(error.NonFiniteConsistencyLossMath, request.validate());
    request.pairs = &.{ 0, 0 };
    request.max_elements = 1;
    try std.testing.expectError(error.ConsistencyLossMathLimitExceeded, request.validate());
    request.max_elements = 2;
    const Cancel = struct {
        fn check(_: ?*anyopaque) bool {
            return true;
        }
    };
    request.control = .{ .cancellation = .{ .is_cancelled_fn = Cancel.check } };
    try std.testing.expectError(error.Cancelled, request.validate());
}
