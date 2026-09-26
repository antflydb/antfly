// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Feature distillation for Antenna students: regress the student's routed
//! states (after its GLiNER neck) onto a teacher encoder's final states at the
//! same routes. The processors split words and emit schema markers
//! independently of the tokenizer, so text words, queries, classification
//! labels and group parents align one to one between a ModernBERT student and
//! a DeBERTa teacher.
//!
//! The loss is MSE in the teacher's per-dimension z-space: every dimension is
//! divided by the teacher's standard deviation over the batch's valid rows, so
//! the large direction the teacher's states share costs nothing extra and the
//! text-specific residual counts in full. Word rows and marker rows are
//! averaged separately and summed, so the few markers are not drowned by the
//! words. This is the recipe that took a raw ModernBERT trunk past the
//! collapse that task labels alone cause
//! (work-log/completed/inference/antenna/2026-09-25-pilot.md).
const std = @import("std");
const Allocator = std.mem.Allocator;

/// Added to the teacher's per-dimension standard deviation.
pub const std_floor: f64 = 1e-4;

pub const Route = enum { text, queries, classifications, parents };

pub const Group = struct {
    route: Route,
    /// [rows, hidden] student states, row-major.
    student: []const f32,
    /// [rows, hidden] teacher states at the same routes.
    teacher: []const f32,
    /// One flag per row; invalid rows carry no loss and a zero gradient.
    valid: []const bool,
};

pub const Result = struct {
    allocator: Allocator,
    /// weight * (word mean + marker mean); either mean is zero when absent.
    value: f64,
    words: f64,
    markers: f64,
    /// One gradient per group, in the caller's order, same shape as `student`.
    gradients: [][]f32,

    pub fn deinit(self: *Result) void {
        for (self.gradients) |gradient| self.allocator.free(gradient);
        self.allocator.free(self.gradients);
        self.* = undefined;
    }
};

pub fn zspaceMse(a: Allocator, hidden: usize, groups: []const Group, weight: f32) !Result {
    if (hidden == 0 or !std.math.isFinite(weight) or weight < 0) return error.InvalidBoundaryDistillation;
    var rows: [2]usize = .{ 0, 0 };
    for (groups) |group| {
        if (group.student.len != group.teacher.len or group.student.len != group.valid.len * hidden) return error.BoundaryDistillationShapeMismatch;
        for (group.valid) |valid| rows[@intFromBool(group.route != .text)] += @intFromBool(valid);
    }
    const total_rows = rows[0] + rows[1];
    // The teacher's per-dimension spread over the batch; a single row has none.
    const mean = try a.alloc(f64, hidden);
    defer a.free(mean);
    const inverse_variance = try a.alloc(f64, hidden);
    defer a.free(inverse_variance);
    @memset(mean, 0);
    @memset(inverse_variance, 0);
    for (groups) |group| for (group.valid, 0..) |valid, row| {
        if (!valid) continue;
        for (group.teacher[row * hidden ..][0..hidden], mean) |value, *sum| sum.* += value;
    };
    if (total_rows != 0) for (mean) |*value| {
        value.* /= @floatFromInt(total_rows);
    };
    const squares = inverse_variance;
    for (groups) |group| for (group.valid, 0..) |valid, row| {
        if (!valid) continue;
        for (group.teacher[row * hidden ..][0..hidden], mean, squares) |value, m, *sum| sum.* += (value - m) * (value - m);
    };
    for (squares) |*value| {
        // Unbiased, as torch.std; one row falls back to the floor alone.
        const variance = if (total_rows > 1) value.* / @as(f64, @floatFromInt(total_rows - 1)) else 0;
        const deviation = @sqrt(variance) + std_floor;
        value.* = 1 / (deviation * deviation);
    }
    const gradients = try a.alloc([]f32, groups.len);
    var made: usize = 0;
    errdefer {
        for (gradients[0..made]) |gradient| a.free(gradient);
        a.free(gradients);
    }
    var sums: [2]f64 = .{ 0, 0 };
    for (groups, gradients) |group, *gradient| {
        gradient.* = try a.alloc(f32, group.student.len);
        made += 1;
        @memset(gradient.*, 0);
        const kind = @intFromBool(group.route != .text);
        if (rows[kind] == 0) continue;
        // d/ds of weight * mean_rows mean_dims (s - t)^2 / sd^2.
        const scale = @as(f64, weight) * 2 / (@as(f64, @floatFromInt(rows[kind])) * @as(f64, @floatFromInt(hidden)));
        for (group.valid, 0..) |valid, row| {
            if (!valid) continue;
            const s = group.student[row * hidden ..][0..hidden];
            const t = group.teacher[row * hidden ..][0..hidden];
            const g = gradient.*[row * hidden ..][0..hidden];
            var row_sum: f64 = 0;
            for (s, t, g, inverse_variance) |student, teacher, *out, inverse| {
                const difference = @as(f64, student) - teacher;
                if (!std.math.isFinite(difference)) return error.NonFiniteBoundaryTraining;
                row_sum += difference * difference * inverse;
                out.* = @floatCast(scale * difference * inverse);
            }
            sums[kind] += row_sum / @as(f64, @floatFromInt(hidden));
        }
    }
    const words = if (rows[0] == 0) 0 else sums[0] / @as(f64, @floatFromInt(rows[0]));
    const markers = if (rows[1] == 0) 0 else sums[1] / @as(f64, @floatFromInt(rows[1]));
    return .{ .allocator = a, .value = @as(f64, weight) * (words + markers), .words = words, .markers = markers, .gradients = gradients };
}

test "feature distillation is z-space MSE with separate word and marker means" {
    const a = std.testing.allocator;
    const hidden = 2;
    // Teacher rows (words, then markers): dimension 0 varies, dimension 1 is a
    // shared offset of 10 with a small spread.
    const teacher_words = [_]f32{ 1, 10, 3, 10.5, 0, 0 };
    const teacher_markers = [_]f32{ 2, 9.5 };
    const student_words = [_]f32{ 1.5, 10, 2, 10, 7, 7 };
    const student_markers = [_]f32{ 2, 10 };
    const groups = [_]Group{
        .{ .route = .text, .student = &student_words, .teacher = &teacher_words, .valid = &.{ true, true, false } },
        .{ .route = .classifications, .student = &student_markers, .teacher = &teacher_markers, .valid = &.{true} },
    };
    var result = try zspaceMse(a, hidden, &groups, 0.5);
    defer result.deinit();
    // Valid teacher rows: (1,10), (3,10.5), (2,9.5): means 2 and 10, unbiased
    // variances 1 and 0.25.
    const sd0 = 1.0 + std_floor;
    const sd1 = 0.5 + std_floor;
    const word0 = (0.25 / (sd0 * sd0) + 0) / 2;
    const word1 = (1 / (sd0 * sd0) + 0.25 / (sd1 * sd1)) / 2;
    const marker = (0 + 0.25 / (sd1 * sd1)) / 2;
    try std.testing.expectApproxEqRel((word0 + word1) / 2, result.words, 1e-12);
    try std.testing.expectApproxEqRel(marker, result.markers, 1e-12);
    try std.testing.expectApproxEqRel(0.5 * (result.words + result.markers), result.value, 1e-12);
    // The masked row carries nothing.
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, result.gradients[0][4..6]);

    // Every gradient entry matches a central difference of the loss.
    const step = 1e-3;
    for (0..2) |group_index| for (0..groups[group_index].student.len) |entry| {
        var plus_words = student_words;
        var plus_markers = student_markers;
        var minus_words = student_words;
        var minus_markers = student_markers;
        if (group_index == 0) {
            plus_words[entry] += step;
            minus_words[entry] -= step;
        } else {
            plus_markers[entry] += step;
            minus_markers[entry] -= step;
        }
        var plus = try zspaceMse(a, hidden, &.{
            .{ .route = .text, .student = &plus_words, .teacher = &teacher_words, .valid = groups[0].valid },
            .{ .route = .classifications, .student = &plus_markers, .teacher = &teacher_markers, .valid = groups[1].valid },
        }, 0.5);
        defer plus.deinit();
        var minus = try zspaceMse(a, hidden, &.{
            .{ .route = .text, .student = &minus_words, .teacher = &teacher_words, .valid = groups[0].valid },
            .{ .route = .classifications, .student = &minus_markers, .teacher = &teacher_markers, .valid = groups[1].valid },
        }, 0.5);
        defer minus.deinit();
        const numeric = (plus.value - minus.value) / (2 * step);
        try std.testing.expectApproxEqAbs(numeric, result.gradients[group_index][entry], 1e-3);
    };
}

test "feature distillation rejects mismatched shapes and bad weights" {
    const a = std.testing.allocator;
    const one = [_]f32{ 1, 2 };
    try std.testing.expectError(error.BoundaryDistillationShapeMismatch, zspaceMse(a, 2, &.{.{ .route = .text, .student = &one, .teacher = one[0..1], .valid = &.{true} }}, 1));
    try std.testing.expectError(error.BoundaryDistillationShapeMismatch, zspaceMse(a, 2, &.{.{ .route = .text, .student = &one, .teacher = &one, .valid = &.{ true, true } }}, 1));
    try std.testing.expectError(error.InvalidBoundaryDistillation, zspaceMse(a, 2, &.{}, -1));
    // No valid rows: zero loss and zero gradients.
    var empty = try zspaceMse(a, 2, &.{.{ .route = .queries, .student = &one, .teacher = &one, .valid = &.{false} }}, 1);
    defer empty.deinit();
    try std.testing.expectEqual(@as(f64, 0), empty.value);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, empty.gradients[0]);
}
