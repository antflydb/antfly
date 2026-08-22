// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Semantic coverage and rarity accounting. Coverage is derived only from
//! stable trace fields, never compiler PCs, addresses, or hash iteration.

const std = @import("std");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const Novelty = struct {
    discovered: usize = 0,
    observed: usize = 0,
    rarity_score: u64 = 0,
    target_score: u64 = 0,
};

pub const Target = struct {
    feature_id: ids.StableId,
    value: ?i64 = null,
    weight: u64 = 1,
};

pub const Tracker = struct {
    allocator: std.mem.Allocator,
    hits: std.AutoHashMapUnmanaged(ids.StableId, u64) = .empty,

    pub fn init(allocator: std.mem.Allocator) Tracker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Tracker) void {
        self.hits.deinit(self.allocator);
        self.* = undefined;
    }

    /// Scores and merges one history. Duplicate features within the history
    /// count once so long histories cannot manufacture energy by repetition.
    pub fn observe(self: *Tracker, artifact: *const trace.Trace) !Novelty {
        var local: std.AutoHashMapUnmanaged(ids.StableId, void) = .empty;
        defer local.deinit(self.allocator);
        for (artifact.transitions.items) |record| try local.put(self.allocator, ids.derive("coverage.transition", record.id, @intFromEnum(record.kind)), {});
        for (artifact.events.items) |record| try local.put(self.allocator, ids.derive("coverage.event", record.id, record.payload_digest), {});
        for (artifact.observations.items) |record| {
            for (record.features) |feature| {
                try local.put(self.allocator, ids.derive("coverage.feature", feature.id, @bitCast(feature.value)), {});
            }
        }
        for (artifact.properties.items) |record| {
            try local.put(self.allocator, ids.derive("coverage.property", record.property_id, @intFromBool(record.condition)), {});
        }
        for (artifact.failures.items) |record| try local.put(self.allocator, ids.derive("coverage.failure", record.fingerprint, @intFromEnum(record.class)), {});

        var result = Novelty{ .observed = local.count() };
        var iterator = local.keyIterator();
        while (iterator.next()) |feature| {
            const entry = try self.hits.getOrPut(self.allocator, feature.*);
            if (!entry.found_existing) {
                entry.value_ptr.* = 0;
                result.discovered += 1;
            }
            entry.value_ptr.* +|= 1;
            // Fixed-point inverse frequency. New and rare features carry more
            // energy without introducing floating-point instability.
            result.rarity_score +|= 1_000_000 / entry.value_ptr.*;
        }
        return result;
    }

    pub fn hitCount(self: *const Tracker, feature: ids.StableId) u64 {
        return self.hits.get(feature) orelse 0;
    }
};

/// Score explicit target-state features without mutating global coverage.
/// Repeated observations count once per target, so extending a history cannot
/// manufacture target energy without reaching another requested state.
pub fn scoreTargets(allocator: std.mem.Allocator, artifact: *const trace.Trace, targets: []const Target) !u64 {
    var matched = try allocator.alloc(bool, targets.len);
    defer allocator.free(matched);
    @memset(matched, false);
    for (artifact.observations.items) |record| {
        for (record.features) |feature| {
            for (targets, 0..) |target, index| {
                if (matched[index] or feature.id != target.feature_id) continue;
                if (target.value) |expected| if (feature.value != expected) continue;
                matched[index] = true;
            }
        }
    }
    var score: u64 = 0;
    for (targets, matched) |target, hit| if (hit) {
        score +|= target.weight;
    };
    return score;
}

test "semantic coverage rewards new features and then decays rarity" {
    var artifact = try trace.Trace.init(std.testing.allocator, .{ .scenario = "coverage", .scenario_version = 1 }, .{ .transition_budget = 1 });
    defer artifact.deinit();
    try artifact.addChoice(.{ .site_id = 1, .site_name = "choice", .occurrence = 0, .enabled_ids = &.{2}, .selected_id = 2 });
    try artifact.addTransition(.{ .index = 1, .id = 2, .name = "step", .kind = .workload });
    try artifact.addObservation(.{ .index = 0, .digest = 0xcbf29ce484222325, .features = &.{} });
    const feature = @import("observation.zig").Feature{ .id = 3, .name = "state", .value = 4 };
    try artifact.addObservation(.{ .index = 1, .digest = @import("observation.zig").digestFeatures(&.{feature}), .features = &.{feature} });
    artifact.summary = .{ .transitions = 1, .final_observation_digest = artifact.observations.items[1].digest, .property_failures = 0 };
    try artifact.validate();

    var tracker = Tracker.init(std.testing.allocator);
    defer tracker.deinit();
    const first = try tracker.observe(&artifact);
    const second = try tracker.observe(&artifact);
    try std.testing.expect(first.discovered > 0);
    try std.testing.expectEqual(@as(usize, 0), second.discovered);
    try std.testing.expect(first.rarity_score > second.rarity_score);
}

test "target scoring is stable and does not reward repeated observations" {
    var artifact = try trace.Trace.init(std.testing.allocator, .{ .scenario = "target", .scenario_version = 1 }, .{ .transition_budget = 1 });
    defer artifact.deinit();
    const feature = @import("observation.zig").Feature{ .id = 31, .name = "phase", .value = 4 };
    const digest = @import("observation.zig").digestFeatures(&.{feature});
    try artifact.addObservation(.{ .index = 0, .digest = digest, .features = &.{feature} });
    try artifact.addObservation(.{ .index = 1, .digest = digest, .features = &.{feature} });
    artifact.summary = .{ .transitions = 0, .final_observation_digest = digest, .property_failures = 0 };
    try std.testing.expectEqual(@as(u64, 7), try scoreTargets(std.testing.allocator, &artifact, &.{.{ .feature_id = 31, .value = 4, .weight = 7 }}));
    try std.testing.expectEqual(@as(u64, 0), try scoreTargets(std.testing.allocator, &artifact, &.{.{ .feature_id = 31, .value = 5, .weight = 7 }}));
}
