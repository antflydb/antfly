// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Default harness-health properties included in every results report.

const std = @import("std");
const ids = @import("id.zig");
const property = @import("property.zig");
const trace = @import("trace.zig");

pub const Snapshot = struct {
    progress_expected: bool = false,
    progress_units: u64 = 0,
    active_tasks: ?u64 = null,
    expected_active_tasks: u64 = 0,
    open_descriptors: ?u64 = null,
    expected_open_descriptors: u64 = 0,
    allocator_exhausted: ?bool = null,
    storage_exhausted: ?bool = null,
    cleanup_complete: ?bool = null,
};

pub const Check = struct {
    property_id: ids.StableId,
    name: []const u8,
    kind: property.Kind = .always_or_unreachable,
    encountered: bool,
    condition: bool,
    details: []const u8,
};

pub const check_count = 8;

pub fn evaluate(history: *const trace.Trace, snapshot: ?Snapshot) [check_count]Check {
    const state = snapshot orelse Snapshot{};
    var replay_ok = true;
    var harness_ok = true;
    for (history.failures.items) |failure| switch (failure.class) {
        .replay_divergence => replay_ok = false,
        .harness => harness_ok = false,
        else => {},
    };
    return .{
        check("vopr.health.no_progress", state.progress_expected, !state.progress_expected or state.progress_units > 0, "expected progress was not observed"),
        check("vopr.health.task_leaks", state.active_tasks != null, state.active_tasks == null or state.active_tasks.? <= state.expected_active_tasks, "simulated tasks remained after cleanup"),
        check("vopr.health.descriptor_leaks", state.open_descriptors != null, state.open_descriptors == null or state.open_descriptors.? <= state.expected_open_descriptors, "descriptors remained after cleanup"),
        check("vopr.health.allocator_exhaustion", state.allocator_exhausted != null, state.allocator_exhausted == null or !state.allocator_exhausted.?, "allocator budget was exhausted"),
        check("vopr.health.storage_exhaustion", state.storage_exhausted != null, state.storage_exhausted == null or !state.storage_exhausted.?, "storage budget was exhausted"),
        check("vopr.health.cleanup", state.cleanup_complete != null, state.cleanup_complete == null or state.cleanup_complete.?, "scenario cleanup did not complete"),
        check("vopr.health.replay_divergence", true, replay_ok, "history contains replay divergence"),
        check("vopr.health.harness_error", true, harness_ok, "history contains a harness error"),
    };
}

pub fn failureCount(checks: []const Check) usize {
    var count: usize = 0;
    for (checks) |item| count += @intFromBool(item.encountered and !item.condition);
    return count;
}

fn check(name: []const u8, encountered: bool, condition: bool, details: []const u8) Check {
    return .{
        .property_id = ids.stable("property", name),
        .name = name,
        .encountered = encountered,
        .condition = condition,
        .details = if (condition) "" else details,
    };
}

test "default harness health reports leaks exhaustion and replay state" {
    const observation = @import("observation.zig");
    var history = try trace.Trace.init(std.testing.allocator, .{ .scenario = "health", .scenario_version = 1 }, .{ .transition_budget = 1 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    history.summary = .{ .transitions = 0, .final_observation_digest = digest, .property_failures = 0 };
    const checks = evaluate(&history, .{
        .progress_expected = true,
        .active_tasks = 2,
        .open_descriptors = 0,
        .allocator_exhausted = false,
        .storage_exhausted = true,
        .cleanup_complete = false,
    });
    try std.testing.expectEqual(@as(usize, 4), failureCount(&checks));
    try std.testing.expect(!checks[0].condition);
    try std.testing.expect(!checks[1].condition);
    try std.testing.expect(!checks[4].condition);
    try std.testing.expect(!checks[5].condition);
}
