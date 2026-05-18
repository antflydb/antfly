// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ir = @import("ir.zig");
const token = @import("token.zig");

pub const Lifecycle = enum {
    observing,
    recommended,
    backfilling,
    ready,
    stale,
    rebuild_required,
    dematerialize_recommended,
};

pub const Policy = struct {
    observe: bool = true,
    lazy_materialization: bool = false,
    dematerialization: bool = false,
    min_observations: u64 = 3,
    max_auto_materializations_per_index: u64 = 32,
    max_backfill_rows_per_tick: u64 = 10_000,
    min_estimated_scan_rows_saved: u64 = 1_000,
    dematerialize_after_observation_misses: u64 = 3,
    observation_decay_after_misses: u64 = 0,
    observation_decay_retain_percent: u8 = 50,
    path_profile_history_retention: u64 = 64,
};

pub const Observation = struct {
    shape: []u8,
    count: u64 = 0,
    last_reason: []u8,
    recommendation: ?[]u8 = null,
    lifecycle: Lifecycle = .observing,

    pub fn deinit(self: *Observation, alloc: Allocator) void {
        alloc.free(self.shape);
        alloc.free(self.last_reason);
        if (self.recommendation) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn shapeKeyAlloc(alloc: Allocator, query: ir.Query) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, "shape:v1");
    try parts.append(alloc, @tagName(query.kind));
    try parts.append(alloc, query.aggregation_name);
    try parts.append(alloc, query.bucket_field orelse "");
    try parts.append(alloc, query.time_field orelse "");
    try parts.append(alloc, query.time_bucket orelse "");
    if (query.metric) |metric| {
        try parts.append(alloc, metric.name);
        try parts.append(alloc, @tagName(metric.op));
        try parts.append(alloc, metric.field);
    }
    for (query.constraints) |constraint| {
        try parts.append(alloc, "where");
        try parts.append(alloc, constraint.field);
    }
    for (query.child_metrics) |metric| {
        try parts.append(alloc, "child");
        try parts.append(alloc, metric.name);
        try parts.append(alloc, @tagName(metric.op));
        try parts.append(alloc, metric.field);
    }
    if (query.join) |join_ref| {
        try parts.append(alloc, "join");
        try parts.append(alloc, join_ref.name);
        try parts.append(alloc, @tagName(join_ref.kind));
        try parts.append(alloc, join_ref.group_side orelse "");
        try parts.append(alloc, join_ref.measure_side orelse "");
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn recommendationAlloc(alloc: Allocator, query: ir.Query) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, "recommendation:v1");
    try parts.append(alloc, query.aggregation_name);
    if (query.metric) |metric| {
        try parts.append(alloc, @tagName(metric.op));
        try parts.append(alloc, metric.field);
    } else {
        try parts.append(alloc, "count");
        try parts.append(alloc, "");
    }
    if (query.bucket_field) |field| {
        try parts.append(alloc, "group");
        try parts.append(alloc, field);
    }
    for (query.constraints) |constraint| {
        try parts.append(alloc, "group");
        try parts.append(alloc, constraint.field);
    }
    if (query.time_field) |field| {
        try parts.append(alloc, "time");
        try parts.append(alloc, field);
        try parts.append(alloc, query.time_bucket orelse "");
    }
    if (query.join) |join_ref| {
        if (join_ref.group_side == null or join_ref.measure_side == null) return error.InvalidAlgebraicAdaptiveRecommendation;
        try parts.append(alloc, "join");
        try parts.append(alloc, join_ref.name);
        try parts.append(alloc, @tagName(join_ref.kind));
        try parts.append(alloc, join_ref.group_side.?);
        try parts.append(alloc, join_ref.measure_side.?);
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn pathPromotionRecommendationAlloc(
    alloc: Allocator,
    path: []const u8,
    stable_kind: ?[]const u8,
    reason: []const u8,
) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{
        "path-recommendation:v1",
        path,
        stable_kind orelse "",
        reason,
    });
}

pub fn observationKeyAlloc(alloc: Allocator, index_name: []const u8, shape: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, "v4", "observe", shape });
}

pub fn materializationStateKeyAlloc(alloc: Allocator, index_name: []const u8, recommendation: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, "v4", "materialization-state", recommendation });
}

pub fn materializationIdAlloc(alloc: Allocator, recommendation: []const u8) ![]u8 {
    const hash = token.hash128(recommendation);
    return try std.fmt.allocPrint(alloc, "am_{x:0>16}{x:0>16}", .{ hash.hi, hash.lo });
}

pub fn candidateKeyAlloc(alloc: Allocator, index_name: []const u8, recommendation: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, "v4", "adaptive-candidate", recommendation });
}

pub fn progressKeyAlloc(alloc: Allocator, index_name: []const u8, recommendation: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "\x00\x00__algebraic__", index_name, "v4", "adaptive-progress", recommendation });
}

pub fn validLifecycleTransition(from: Lifecycle, to: Lifecycle) bool {
    if (from == to) return true;
    return switch (from) {
        .observing => to == .recommended or to == .stale or to == .rebuild_required,
        .recommended => to == .backfilling or to == .stale or to == .rebuild_required or to == .dematerialize_recommended,
        .backfilling => to == .ready or to == .stale or to == .rebuild_required,
        .ready => to == .stale or to == .rebuild_required or to == .dematerialize_recommended,
        .stale => to == .backfilling or to == .rebuild_required or to == .dematerialize_recommended,
        .rebuild_required => to == .backfilling or to == .dematerialize_recommended,
        .dematerialize_recommended => to == .stale or to == .rebuild_required,
    };
}

pub fn nextLifecycle(policy: Policy, observation_count: u64, current: Lifecycle) Lifecycle {
    if (current == .ready or current == .stale or current == .rebuild_required) return current;
    if (!policy.observe) return current;
    if (observation_count < policy.min_observations) return .observing;
    if (!policy.lazy_materialization) return .recommended;
    return if (current == .backfilling) .backfilling else .recommended;
}

test "adaptive shapes are canonical and recommendation-ready" {
    const alloc = std.testing.allocator;
    const constraints = [_]ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    const query = ir.Query{
        .kind = .terms,
        .aggregation_name = "amount_by_customer",
        .bucket_field = "customer",
        .constraints = constraints[0..],
        .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
    };
    const shape = try shapeKeyAlloc(alloc, query);
    defer alloc.free(shape);
    const recommendation = try recommendationAlloc(alloc, query);
    defer alloc.free(recommendation);
    try std.testing.expect(std.mem.indexOf(u8, shape, "shape:v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, recommendation, "recommendation:v1") != null);
}

test "adaptive schemaless path promotion recommendations are canonical" {
    const alloc = std.testing.allocator;
    const recommendation = try pathPromotionRecommendationAlloc(alloc, "/meta/score", "string", "stable_numeric_string_path");
    defer alloc.free(recommendation);
    const again = try pathPromotionRecommendationAlloc(alloc, "/meta/score", "string", "stable_numeric_string_path");
    defer alloc.free(again);
    const different = try pathPromotionRecommendationAlloc(alloc, "/meta/score", "string", "stable_datetime_string_path");
    defer alloc.free(different);

    try std.testing.expectEqualStrings(recommendation, again);
    try std.testing.expect(!std.mem.eql(u8, recommendation, different));
    try std.testing.expect(std.mem.indexOf(u8, recommendation, "path-recommendation:v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, recommendation, "/meta/score") != null);
}

test "adaptive join recommendations require explicit query sides" {
    const alloc = std.testing.allocator;
    const missing_sides = ir.Query{
        .kind = .terms,
        .aggregation_name = "amount_by_segment",
        .bucket_field = "segment",
        .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers" },
    };
    const shape = try shapeKeyAlloc(alloc, missing_sides);
    defer alloc.free(shape);
    try std.testing.expect(std.mem.indexOf(u8, shape, "shape:v1") != null);
    try std.testing.expectError(error.InvalidAlgebraicAdaptiveRecommendation, recommendationAlloc(alloc, missing_sides));

    const sided = ir.Query{
        .kind = .terms,
        .aggregation_name = "amount_by_segment",
        .bucket_field = "segment",
        .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    const recommendation = try recommendationAlloc(alloc, sided);
    defer alloc.free(recommendation);
    const parts = try token.decodeTupleAlloc(alloc, recommendation);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expect(std.mem.indexOf(u8, recommendation, "recommendation:v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, recommendation, "right") != null);
    try std.testing.expect(std.mem.indexOf(u8, recommendation, "left") != null);
}

test "adaptive lifecycle gates lazy materialization" {
    const passive = Policy{ .lazy_materialization = false, .min_observations = 2 };
    try std.testing.expectEqual(Lifecycle.observing, nextLifecycle(passive, 1, .observing));
    try std.testing.expectEqual(Lifecycle.recommended, nextLifecycle(passive, 2, .observing));

    const lazy = Policy{ .lazy_materialization = true, .min_observations = 2 };
    try std.testing.expectEqual(Lifecycle.recommended, nextLifecycle(lazy, 2, .observing));
    try std.testing.expectEqual(Lifecycle.ready, nextLifecycle(lazy, 10, .ready));
}

test "adaptive materialization ids and lifecycle transitions are deterministic" {
    const alloc = std.testing.allocator;
    const recommendation = try token.canonicalTupleAlloc(alloc, &.{ "recommendation:v1", "sales", "sum", "amount" });
    defer alloc.free(recommendation);
    const a = try materializationIdAlloc(alloc, recommendation);
    defer alloc.free(a);
    const b = try materializationIdAlloc(alloc, recommendation);
    defer alloc.free(b);
    try std.testing.expectEqualStrings(a, b);
    try std.testing.expect(std.mem.startsWith(u8, a, "am_"));
    try std.testing.expect(validLifecycleTransition(.recommended, .backfilling));
    try std.testing.expect(!validLifecycleTransition(.backfilling, .recommended));
}
