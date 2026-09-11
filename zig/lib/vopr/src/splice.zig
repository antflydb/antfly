// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Logical trace splicing. Observation digests identify candidate join states;
//! replaying the combined stream is still the authority that proves a join is
//! compatible with the concrete scenario.

const std = @import("std");
const choice = @import("choice.zig");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const Point = struct {
    left_choice_count: usize,
    right_choice_start: usize,
    observation_digest: u64,
};

pub fn findPoints(allocator: std.mem.Allocator, left: *const trace.Trace, right: *const trace.Trace) ![]Point {
    var points: std.ArrayListUnmanaged(Point) = .empty;
    errdefer points.deinit(allocator);
    for (left.observations.items) |left_observation| {
        if (left_observation.index > left.choices.items.len) continue;
        for (right.observations.items) |right_observation| {
            if (right_observation.index > right.choices.items.len) continue;
            if (left_observation.digest != right_observation.digest) continue;
            try points.append(allocator, .{
                .left_choice_count = @intCast(left_observation.index),
                .right_choice_start = @intCast(right_observation.index),
                .observation_digest = left_observation.digest,
            });
        }
    }
    return points.toOwnedSlice(allocator);
}

/// Choice source for a proposed splice. The prefix retains strict occurrence
/// checking. The suffix intentionally rebases occurrences, but requires the
/// stable choice site and complete enabled set to match at every step.
pub const Source = struct {
    left: []const trace.ChoiceRecord,
    left_count: usize,
    right: []const trace.ChoiceRecord,
    right_start: usize,
    cursor: usize = 0,

    pub fn init(left: []const trace.ChoiceRecord, right: []const trace.ChoiceRecord, point: Point) !Source {
        if (point.left_choice_count > left.len or point.right_choice_start > right.len) return error.InvalidSplicePoint;
        return .{
            .left = left,
            .left_count = point.left_choice_count,
            .right = right,
            .right_start = point.right_choice_start,
        };
    }

    pub fn source(self: *Source) choice.Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    pub fn combinedLength(self: *const Source) usize {
        return self.left_count + self.right.len - self.right_start;
    }

    fn choose(ptr: *anyopaque, request: choice.Request) !ids.StableId {
        const self: *Source = @ptrCast(@alignCast(ptr));
        const record = if (self.cursor < self.left_count)
            self.left[self.cursor]
        else blk: {
            const suffix_offset = self.cursor - self.left_count;
            const index = self.right_start + suffix_offset;
            if (index >= self.right.len) return error.SpliceChoiceExhausted;
            break :blk self.right[index];
        };
        if (record.site_id != request.site_id or !std.mem.eql(u8, record.site_name, request.site_name)) return error.SpliceChoiceSiteDiverged;
        if (self.cursor < self.left_count and record.occurrence != request.occurrence) return error.SpliceChoiceSiteDiverged;
        if (record.enabled_ids.len != request.enabled.len) return error.SpliceEnabledSetDiverged;
        for (request.enabled, record.enabled_ids) |enabled, expected| {
            if (enabled.id != expected) return error.SpliceEnabledSetDiverged;
        }
        self.cursor += 1;
        return record.selected_id;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Source = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.combinedLength()) return error.SpliceHasTrailingChoices;
    }
};

test "compatible points use logical observation digests" {
    var left = try trace.Trace.init(std.testing.allocator, .{ .scenario = "splice", .scenario_version = 1 }, .{ .transition_budget = 2 });
    defer left.deinit();
    var right = try trace.Trace.init(std.testing.allocator, .{ .scenario = "splice", .scenario_version = 1 }, .{ .transition_budget = 2 });
    defer right.deinit();
    try left.addObservation(.{ .index = 1, .digest = 99, .features = &.{} });
    try right.addObservation(.{ .index = 0, .digest = 99, .features = &.{} });
    // The choice lengths bound otherwise-valid checkpoint indices.
    try left.addChoice(.{ .site_id = 1, .site_name = "s", .occurrence = 0, .enabled_ids = &.{2}, .selected_id = 2 });
    const points = try findPoints(std.testing.allocator, &left, &right);
    defer std.testing.allocator.free(points);
    try std.testing.expectEqual(@as(usize, 1), points.len);
    try std.testing.expectEqual(@as(usize, 1), points[0].left_choice_count);
}

test "spliced source rebases a compatible suffix occurrence" {
    const transition = @import("transition.zig");
    const enabled = [_]transition.Transition{
        .{ .id = 10, .name = "ten", .kind = .workload },
        .{ .id = 20, .name = "twenty", .kind = .workload },
    };
    const left = [_]trace.ChoiceRecord{
        .{ .site_id = 1, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 10, 20 }, .selected_id = 10 },
    };
    const right = [_]trace.ChoiceRecord{
        .{ .site_id = 1, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 10, 20 }, .selected_id = 10 },
        .{ .site_id = 1, .site_name = "site", .occurrence = 1, .enabled_ids = &.{ 10, 20 }, .selected_id = 20 },
        .{ .site_id = 1, .site_name = "site", .occurrence = 2, .enabled_ids = &.{ 10, 20 }, .selected_id = 10 },
    };
    var spliced = try Source.init(&left, &right, .{
        .left_choice_count = 1,
        .right_choice_start = 2,
        .observation_digest = 7,
    });
    const source = spliced.source();
    try std.testing.expectEqual(@as(u64, 10), try source.choose(.{ .site_id = 1, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
    // right[2] originally occurred at 2 but is validly rebased to 1.
    try std.testing.expectEqual(@as(u64, 10), try source.choose(.{ .site_id = 1, .site_name = "site", .occurrence = 1, .enabled = &enabled }));
    try source.finish();
}
