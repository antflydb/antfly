// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Deterministic sparse scatter descriptors built only from uploaded integer
//! metadata. Activation/gradient tensors never enter this host-side planner.
const std = @import("std");
const control_mod = @import("../execution_control.zig");
const IndexBounds = @import("resident_training_ops.zig").IndexBounds;
const Control = control_mod.InferenceExecutionControl;

pub const Limits = struct {
    max_indices: usize = 4 * 1024 * 1024,
    max_metadata_bytes: usize = 128 * 1024 * 1024,
    /// Conservative bound for heap-sort key comparisons, two per tree level.
    max_sort_work: usize = 256 * 1024 * 1024,
};
pub const Admission = struct { persistent_bytes: usize, peak_bytes: usize, sort_work: usize };

pub fn plan(count: usize, output_rows: usize, limits: Limits) !Admission {
    if (count == 0 or output_rows == 0 or output_rows > std.math.maxInt(i32)) return error.InvalidResidentTrainingShape;
    if (count > limits.max_indices or count > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    const persistent = std.math.add(usize, std.math.mul(usize, count, 12) catch return error.ResourceLimitExceeded, 4) catch return error.ResourceLimitExceeded;
    const peak = std.math.add(usize, persistent, std.math.mul(usize, count, @sizeOf(Entry)) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
    const height: usize = if (count <= 1) 1 else std.math.log2_int_ceil(usize, count);
    const work = std.math.mul(usize, count, 2 * height + 2) catch return error.ResourceLimitExceeded;
    if (peak > limits.max_metadata_bytes or work > limits.max_sort_work) return error.ResourceLimitExceeded;
    return .{ .persistent_bytes = persistent, .peak_bytes = peak, .sort_work = work };
}

const Entry = struct {
    row: i32,
    ordinal: i32,
    fn lessThan(_: void, lhs: Entry, rhs: Entry) bool {
        return lhs.row < rhs.row or (lhs.row == rhs.row and lhs.ordinal < rhs.ordinal);
    }
};

pub const Grouped = struct {
    allocator: std.mem.Allocator,
    storage: []i32,
    /// Unique, strictly increasing normalized destination rows.
    rows: []const i32,
    /// Offsets into order; the final entry equals the original index count.
    offsets: []const i32,
    /// Original value ordinals, increasing within every destination group.
    order: []const i32,
    output_rows: usize,
    maximum_group_size: usize,
    admission: Admission,

    pub fn deinit(self: *Grouped) void {
        self.allocator.free(self.storage);
        self.* = undefined;
    }
};

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

pub fn build(a: std.mem.Allocator, indices: []const i32, output_rows: usize, limits: Limits, control: ?Control) !Grouped {
    try check(control);
    const admission = try plan(indices.len, output_rows, limits);
    try (try IndexBounds.of(indices)).validate(output_rows);
    const entries = try a.alloc(Entry, indices.len);
    defer a.free(entries);
    const count: i64 = @intCast(output_rows);
    for (indices, entries, 0..) |raw, *entry, ordinal| {
        if (ordinal % 4096 == 0) try check(control);
        entry.* = .{ .row = @intCast(if (raw < 0) @as(i64, raw) + count else raw), .ordinal = @intCast(ordinal) };
    }
    // Heap sort has a deterministic O(N logN) bound and allocates no scratch.
    // The ordinal tie-breaker makes the order independent of sort stability.
    std.sort.heap(Entry, entries, {}, Entry.lessThan);
    try check(control);
    const storage = try a.alloc(i32, admission.persistent_bytes / 4);
    errdefer a.free(storage);
    const order = storage[0..indices.len];
    const rows = storage[indices.len..][0..indices.len];
    const offsets = storage[2 * indices.len ..];
    var groups: usize = 0;
    var maximum_group: usize = 0;
    for (entries, 0..) |entry, ordinal| {
        if (ordinal % 4096 == 0) try check(control);
        if (groups == 0 or rows[groups - 1] != entry.row) {
            if (groups > 0) maximum_group = @max(maximum_group, ordinal - @as(usize, @intCast(offsets[groups - 1])));
            rows[groups] = entry.row;
            offsets[groups] = @intCast(ordinal);
            groups += 1;
        }
        order[ordinal] = entry.ordinal;
    }
    offsets[groups] = @intCast(indices.len);
    maximum_group = @max(maximum_group, indices.len - @as(usize, @intCast(offsets[groups - 1])));
    try check(control);
    return .{
        .allocator = a,
        .storage = storage,
        .rows = rows[0..groups],
        .offsets = offsets[0 .. groups + 1],
        .order = order,
        .output_rows = output_rows,
        .maximum_group_size = maximum_group,
        .admission = admission,
    };
}

test "resident training sparse groups preserve repeated negative and high integer routing order" {
    var groups = try build(std.testing.allocator, &.{ 16_777_217, 0, -1, 16_777_216, 0, -2 }, 16_777_218, .{}, null);
    defer groups.deinit();
    try std.testing.expectEqualSlices(i32, &.{ 0, 16_777_216, 16_777_217 }, groups.rows);
    try std.testing.expectEqualSlices(i32, &.{ 0, 2, 4, 6 }, groups.offsets);
    try std.testing.expectEqualSlices(i32, &.{ 1, 4, 3, 5, 0, 2 }, groups.order);
    try std.testing.expectEqual(@as(usize, 2), groups.maximum_group_size);
    // Metadata scales with six submitted values, not16million output rows.
    try std.testing.expectEqual(@as(usize, 124), groups.admission.peak_bytes);
}

test "resident training sparse groups equal direct ordered scatter on adversarial repeated routes" {
    const a = std.testing.allocator;
    var indices: [1025]i32 = undefined;
    var values: [1025]f32 = undefined;
    for (&indices, &values, 0..) |*index, *value, ordinal| {
        index.* = if (ordinal % 3 == 0) -1 else @intCast((ordinal * 19) % 13);
        value.* = if (ordinal % 4 == 0) 1e7 else if (ordinal % 4 == 1) -1e7 else @as(f32, @floatFromInt(ordinal % 11)) * 0.125;
    }
    var groups = try build(a, &indices, 13, .{}, null);
    defer groups.deinit();
    var direct: [13]f32 = @splat(0);
    for (indices, values) |raw, value| direct[@intCast(if (raw < 0) raw + 13 else raw)] += value;
    var grouped: [13]f32 = @splat(0);
    for (groups.rows, 0..) |row, g| {
        const begin: usize = @intCast(groups.offsets[g]);
        const end: usize = @intCast(groups.offsets[g + 1]);
        for (groups.order[begin..end]) |ordinal| grouped[@intCast(row)] += values[@intCast(ordinal)];
    }
    try std.testing.expectEqualSlices(f32, &direct, &grouped);
}

fn allocationCheck(a: std.mem.Allocator) !void {
    var groups = try build(a, &.{ 3, -1, 0, 3 }, 5, .{}, null);
    defer groups.deinit();
}

test "resident training sparse groups reject malformed resources and clean allocation failures" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.IndexOutOfBounds, build(a, &.{-6}, 5, .{}, null));
    try std.testing.expectError(error.IndexOutOfBounds, build(a, &.{5}, 5, .{}, null));
    try std.testing.expectError(error.InvalidResidentTrainingShape, plan(0, 5, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(1024, 5, .{ .max_metadata_bytes = 1024 }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(1024, 5, .{ .max_sort_work = 1024 }));
    try std.testing.checkAllAllocationFailures(a, allocationCheck, .{});
}
