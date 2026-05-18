// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const builtin = @import("builtin");
const types = @import("types.zig");
const kmeans_metal = @import("kmeans_metal.zig");
const vec = @import("antfly_vector").vector;

var fallback_counter_ns: u64 = 0;

pub const Point = struct {
    stable_id: u64,
    vector: []const f32,
    weight: usize = 1,
};

pub const Entry = struct {
    point_index: usize,
    cluster: usize,
    distance: f32,
};

pub const RunConfig = struct {
    dims: usize,
    metric: vec.DistanceMetric,
    max_iter: u32,
    backend: types.HBCConfig.KmeansBackend = .auto,
    update_strategy: types.HBCConfig.KmeansUpdateStrategy = .auto,
    dense_vectors: ?[]const f32 = null,
};

pub const RunStats = struct {
    assignment_calls: u64 = 0,
    assignment_cpu_calls: u64 = 0,
    assignment_metal_calls: u64 = 0,
    assignment_points_total: u64 = 0,
    assignment_ns: u64 = 0,
    assignment_cpu_ns: u64 = 0,
    assignment_metal_ns: u64 = 0,
    update_calls: u64 = 0,
    update_cpu_calls: u64 = 0,
    update_metal_calls: u64 = 0,
    update_ns: u64 = 0,
    update_cpu_ns: u64 = 0,
    update_metal_ns: u64 = 0,
};

const AssignmentResult = struct {
    used_metal: bool,
    ns: u64,
};

const UpdateKind = enum {
    scatter,
    segmented,
    metal,
};

pub fn run(
    config: RunConfig,
    points: []const Point,
    first_index: usize,
    centroids: []f32,
    next_centroids: []f32,
    assignments: []usize,
    distances: []f32,
    counts: []usize,
    entries: []Entry,
) !RunStats {
    if (points.len == 0) return error.TooFewVectors;
    if (config.dims == 0) return error.InvalidDimensions;
    const cluster_count = centroids.len / config.dims;
    if (cluster_count == 0) return error.TooFewVectors;
    if (centroids.len != cluster_count * config.dims or next_centroids.len != centroids.len) return error.InvalidDimensions;
    if (assignments.len != points.len or distances.len != points.len or entries.len != points.len) return error.InvalidDimensions;
    if (counts.len != cluster_count) return error.InvalidDimensions;
    if (config.dense_vectors) |dense_vectors| {
        if (dense_vectors.len != points.len * config.dims) return error.InvalidDimensions;
    }

    try initCentroids(config, points, first_index, centroids, distances);

    var stats = RunStats{};
    var metal_context = try initMetalContext(config, points.len, cluster_count);
    defer if (metal_context) |*context| context.deinit();

    const update_kind = try chooseUpdateKind(config.update_strategy, points, cluster_count, metal_context != null);
    for (0..config.max_iter) |_| {
        const assignment_result = try assignWithBackend(config, points, centroids, assignments, distances, if (metal_context) |*context| context else null);
        try recordAssignmentStats(&stats, points.len, assignment_result);
        const actual_update_kind = if (update_kind == .metal and !assignment_result.used_metal)
            cpuUpdateKind(points.len, cluster_count)
        else
            update_kind;
        const update_start_ns = monotonicNs();
        switch (actual_update_kind) {
            .metal => if (metal_context) |*context| {
                try recomputeCentroidsMetal(config, context, centroids, next_centroids, counts);
            } else unreachable,
            .segmented => {
                populateEntries(entries, assignments, distances);
                sortEntries(points, entries);
                recomputeCentroidsSegmented(config, points, entries, centroids, next_centroids, counts);
            },
            .scatter => recomputeCentroidsScatter(config, points, assignments, centroids, next_centroids, counts),
        }
        recordUpdateStats(&stats, actual_update_kind == .metal, elapsedNsSince(update_start_ns));
        const shift = centroidShiftSquared(centroids, next_centroids);
        @memcpy(centroids, next_centroids);
        if (shift <= 0.000001) break;
    }

    try recordAssignmentStats(&stats, points.len, try assignWithBackend(config, points, centroids, assignments, distances, if (metal_context) |*context| context else null));
    populateEntries(entries, assignments, distances);
    sortEntries(points, entries);
    return stats;
}

pub fn shouldUseSegmentedUpdate(
    strategy: types.HBCConfig.KmeansUpdateStrategy,
    point_count: usize,
    cluster_count: usize,
) bool {
    return switch (strategy) {
        .scatter => false,
        .segmented => true,
        .metal => false,
        .auto => point_count >= 4096 and cluster_count >= 64,
    };
}

fn chooseUpdateKind(
    strategy: types.HBCConfig.KmeansUpdateStrategy,
    points: []const Point,
    cluster_count: usize,
    has_metal_context: bool,
) !UpdateKind {
    const metal_supported = has_metal_context and allUnitWeights(points);
    return switch (strategy) {
        .scatter => .scatter,
        .segmented => .segmented,
        .metal => if (metal_supported) .metal else if (shouldUseSegmentedUpdate(.auto, points.len, cluster_count)) .segmented else .scatter,
        .auto => if (shouldUseSegmentedUpdate(.auto, points.len, cluster_count)) .segmented else .scatter,
    };
}

fn cpuUpdateKind(point_count: usize, cluster_count: usize) UpdateKind {
    return if (shouldUseSegmentedUpdate(.auto, point_count, cluster_count)) .segmented else .scatter;
}

fn allUnitWeights(points: []const Point) bool {
    for (points) |point| {
        if (point.weight != 1) return false;
    }
    return true;
}

pub fn centroidShiftSquared(lhs: []const f32, rhs: []const f32) f32 {
    var shift: f32 = 0;
    for (lhs, rhs) |a, b| {
        const d = a - b;
        shift += d * d;
    }
    return shift;
}

fn initCentroids(
    config: RunConfig,
    points: []const Point,
    first_index: usize,
    centroids: []f32,
    min_distances: []f32,
) !void {
    const cluster_count = centroids.len / config.dims;
    if (cluster_count == 0 or points.len == 0) return error.TooFewVectors;

    const first = first_index % points.len;
    @memcpy(centroids[0..config.dims], points[first].vector);
    normalizeCentroidForMetric(config, centroids[0..config.dims]);

    for (points, 0..) |point, i| {
        min_distances[i] = vec.distance(point.vector, centroids[0..config.dims], config.metric);
    }

    var cluster: usize = 1;
    while (cluster < cluster_count) : (cluster += 1) {
        var farthest_idx: usize = (cluster * points.len) / cluster_count;
        var farthest_dist: f32 = -std.math.inf(f32);
        for (min_distances, 0..) |distance, i| {
            if (distance > farthest_dist) {
                farthest_dist = distance;
                farthest_idx = i;
            }
        }
        if (!(farthest_dist > 0)) {
            farthest_idx = (cluster * points.len) / cluster_count;
        }

        const centroid = centroids[cluster * config.dims ..][0..config.dims];
        @memcpy(centroid, points[farthest_idx].vector);
        normalizeCentroidForMetric(config, centroid);

        for (points, 0..) |point, i| {
            const distance = vec.distance(point.vector, centroid, config.metric);
            if (distance < min_distances[i]) min_distances[i] = distance;
        }
    }
}

fn assign(
    config: RunConfig,
    points: []const Point,
    centroids: []const f32,
    assignments: []usize,
    distances: []f32,
) void {
    const cluster_count = centroids.len / config.dims;
    for (points, 0..) |point, i| {
        var best_cluster: usize = 0;
        var best_distance = vec.distance(point.vector, centroids[0..config.dims], config.metric);
        var cluster: usize = 1;
        while (cluster < cluster_count) : (cluster += 1) {
            const centroid = centroids[cluster * config.dims ..][0..config.dims];
            const distance = vec.distance(point.vector, centroid, config.metric);
            if (distance < best_distance) {
                best_distance = distance;
                best_cluster = cluster;
            }
        }
        assignments[i] = best_cluster;
        distances[i] = best_distance;
    }
}

fn assignWithBackend(
    config: RunConfig,
    points: []const Point,
    centroids: []const f32,
    assignments: []usize,
    distances: []f32,
    metal_context: ?*kmeans_metal.Context,
) !AssignmentResult {
    const start_ns = monotonicNs();
    if (try assignMetal(config, points, centroids, assignments, distances, metal_context)) {
        return .{ .used_metal = true, .ns = elapsedNsSince(start_ns) };
    }
    assign(config, points, centroids, assignments, distances);
    return .{ .used_metal = false, .ns = elapsedNsSince(start_ns) };
}

fn initMetalContext(
    config: RunConfig,
    point_count: usize,
    cluster_count: usize,
) !?kmeans_metal.Context {
    if (!shouldTryMetal(config, point_count, cluster_count)) {
        if (config.backend != .cpu and config.update_strategy == .metal) return error.MetalUnavailable;
        return null;
    }
    const dense_vectors = config.dense_vectors orelse {
        if (config.backend == .metal or config.update_strategy == .metal) return error.MetalRequiresDenseVectors;
        return null;
    };

    return kmeans_metal.Context.init(dense_vectors, point_count, cluster_count, config.dims) catch |err| {
        if (config.backend == .metal or config.update_strategy == .metal) return err;
        return null;
    };
}

fn shouldTryMetal(
    config: RunConfig,
    point_count: usize,
    cluster_count: usize,
) bool {
    return switch (config.backend) {
        .cpu => false,
        .metal => true,
        .auto => if (config.update_strategy == .metal)
            true
        else
            kmeans_metal.available() and point_count >= 4096 and cluster_count >= 64,
    };
}

fn assignMetal(
    config: RunConfig,
    points: []const Point,
    centroids: []const f32,
    assignments: []usize,
    distances: []f32,
    metal_context: ?*kmeans_metal.Context,
) !bool {
    _ = points;
    const context = metal_context orelse return false;

    context.assign(
        centroids,
        config.dims,
        @intFromEnum(config.metric),
        assignments,
        distances,
    ) catch |err| {
        if (config.backend == .metal or config.update_strategy == .metal) return err;
        return false;
    };
    return true;
}

fn recordAssignmentStats(stats: *RunStats, point_count: usize, result: AssignmentResult) !void {
    stats.assignment_calls += 1;
    stats.assignment_points_total += std.math.cast(u64, point_count) orelse return error.Overflow;
    stats.assignment_ns += result.ns;
    if (result.used_metal) {
        stats.assignment_metal_calls += 1;
        stats.assignment_metal_ns += result.ns;
    } else {
        stats.assignment_cpu_calls += 1;
        stats.assignment_cpu_ns += result.ns;
    }
}

fn recordUpdateStats(stats: *RunStats, used_metal: bool, ns: u64) void {
    stats.update_calls += 1;
    stats.update_ns += ns;
    if (used_metal) {
        stats.update_metal_calls += 1;
        stats.update_metal_ns += ns;
    } else {
        stats.update_cpu_calls += 1;
        stats.update_cpu_ns += ns;
    }
}

fn elapsedNsSince(start_ns: u64) u64 {
    return monotonicNs() -| start_ns;
}

fn monotonicNs() u64 {
    if (builtin.os.tag == .freestanding) {
        fallback_counter_ns +%= 1;
        return fallback_counter_ns;
    }
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn recomputeCentroidsScatter(
    config: RunConfig,
    points: []const Point,
    assignments: []const usize,
    old_centroids: []const f32,
    centroids: []f32,
    counts: []usize,
) void {
    @memset(centroids, 0);
    @memset(counts, 0);

    for (points, 0..) |point, i| {
        const cluster = assignments[i];
        addWeightedVector(centroids[cluster * config.dims ..][0..config.dims], point.vector, point.weight);
        counts[cluster] += point.weight;
    }

    finishCentroids(config, old_centroids, centroids, counts);
}

fn recomputeCentroidsSegmented(
    config: RunConfig,
    points: []const Point,
    entries: []const Entry,
    old_centroids: []const f32,
    centroids: []f32,
    counts: []usize,
) void {
    @memset(centroids, 0);
    @memset(counts, 0);

    var start: usize = 0;
    while (start < entries.len) {
        const cluster = entries[start].cluster;
        const centroid = centroids[cluster * config.dims ..][0..config.dims];
        var end = start;
        while (end < entries.len and entries[end].cluster == cluster) : (end += 1) {
            const point = points[entries[end].point_index];
            addWeightedVector(centroid, point.vector, point.weight);
            counts[cluster] += point.weight;
        }
        start = end;
    }

    finishCentroids(config, old_centroids, centroids, counts);
}

fn recomputeCentroidsMetal(
    config: RunConfig,
    context: *kmeans_metal.Context,
    old_centroids: []const f32,
    centroids: []f32,
    counts: []usize,
) !void {
    try context.updateCentroids(old_centroids, centroids, counts, config.dims, @intFromEnum(config.metric));
    normalizeCentroidsForMetric(config, centroids);
}

fn finishCentroids(
    config: RunConfig,
    old_centroids: []const f32,
    centroids: []f32,
    counts: []const usize,
) void {
    for (counts, 0..) |count, cluster| {
        const centroid = centroids[cluster * config.dims ..][0..config.dims];
        if (count == 0) {
            @memcpy(centroid, old_centroids[cluster * config.dims ..][0..config.dims]);
            continue;
        }
        vec.scale(1.0 / @as(f32, @floatFromInt(count)), centroid);
        normalizeCentroidForMetric(config, centroid);
    }
}

fn populateEntries(
    entries: []Entry,
    assignments: []const usize,
    distances: []const f32,
) void {
    for (entries, 0..) |*entry, i| {
        entry.* = .{
            .point_index = i,
            .cluster = assignments[i],
            .distance = distances[i],
        };
    }
}

fn sortEntries(points: []const Point, entries: []Entry) void {
    std.mem.sort(Entry, entries, points, struct {
        fn lessThan(ctx: []const Point, a: Entry, b: Entry) bool {
            if (a.cluster != b.cluster) return a.cluster < b.cluster;
            if (a.distance != b.distance) return a.distance < b.distance;
            return ctx[a.point_index].stable_id < ctx[b.point_index].stable_id;
        }
    }.lessThan);
}

fn addWeightedVector(sum: []f32, values: []const f32, weight: usize) void {
    const w: f32 = @floatFromInt(weight);
    for (sum, values) |*dst, value| dst.* += value * w;
}

fn normalizeCentroidForMetric(config: RunConfig, centroid: []f32) void {
    if (config.metric == .cosine and centroid.len > 0) {
        _ = vec.normalize(centroid);
    }
}

fn normalizeCentroidsForMetric(config: RunConfig, centroids: []f32) void {
    if (config.metric != .cosine) return;
    const cluster_count = centroids.len / config.dims;
    var cluster: usize = 0;
    while (cluster < cluster_count) : (cluster += 1) {
        normalizeCentroidForMetric(config, centroids[cluster * config.dims ..][0..config.dims]);
    }
}

test "kmeans cpu scatter run records CPU assignment and update stats" {
    const dims = 2;
    var dense_vectors = [_]f32{
        0,  0,
        0,  1,
        10, 10,
        10, 11,
    };
    var points = [_]Point{
        .{ .stable_id = 10, .vector = dense_vectors[0..2] },
        .{ .stable_id = 11, .vector = dense_vectors[2..4] },
        .{ .stable_id = 20, .vector = dense_vectors[4..6] },
        .{ .stable_id = 21, .vector = dense_vectors[6..8] },
    };
    var centroids: [4]f32 = undefined;
    var next_centroids: [4]f32 = undefined;
    var assignments: [4]usize = undefined;
    var distances: [4]f32 = undefined;
    var counts: [2]usize = undefined;
    var entries: [4]Entry = undefined;

    const stats = try run(.{
        .dims = dims,
        .metric = .l2_squared,
        .max_iter = 4,
        .backend = .cpu,
        .update_strategy = .scatter,
        .dense_vectors = &dense_vectors,
    }, &points, 0, &centroids, &next_centroids, &assignments, &distances, &counts, &entries);

    try std.testing.expect(stats.assignment_calls >= 2);
    try std.testing.expectEqual(stats.assignment_calls, stats.assignment_cpu_calls);
    try std.testing.expectEqual(@as(u64, 0), stats.assignment_metal_calls);
    try std.testing.expect(stats.update_calls >= 1);
    try std.testing.expectEqual(stats.update_calls, stats.update_cpu_calls);
    try std.testing.expectEqual(@as(u64, 0), stats.update_metal_calls);
    try std.testing.expectEqual(@as(u64, stats.assignment_calls * points.len), stats.assignment_points_total);
    try std.testing.expectEqual(@as(usize, 0), entries[0].cluster);
    try std.testing.expectEqual(@as(usize, 0), entries[1].cluster);
    try std.testing.expectEqual(@as(usize, 1), entries[2].cluster);
    try std.testing.expectEqual(@as(usize, 1), entries[3].cluster);
}

test "kmeans cpu backend lets metal update strategy fall back to CPU update" {
    const dims = 2;
    var dense_vectors = [_]f32{
        0,  0,
        0,  1,
        10, 10,
        10, 11,
    };
    var points = [_]Point{
        .{ .stable_id = 10, .vector = dense_vectors[0..2] },
        .{ .stable_id = 11, .vector = dense_vectors[2..4] },
        .{ .stable_id = 20, .vector = dense_vectors[4..6] },
        .{ .stable_id = 21, .vector = dense_vectors[6..8] },
    };
    var centroids: [4]f32 = undefined;
    var next_centroids: [4]f32 = undefined;
    var assignments: [4]usize = undefined;
    var distances: [4]f32 = undefined;
    var counts: [2]usize = undefined;
    var entries: [4]Entry = undefined;

    const stats = try run(.{
        .dims = dims,
        .metric = .l2_squared,
        .max_iter = 4,
        .backend = .cpu,
        .update_strategy = .metal,
        .dense_vectors = &dense_vectors,
    }, &points, 0, &centroids, &next_centroids, &assignments, &distances, &counts, &entries);

    try std.testing.expectEqual(stats.assignment_calls, stats.assignment_cpu_calls);
    try std.testing.expectEqual(@as(u64, 0), stats.assignment_metal_calls);
    try std.testing.expectEqual(stats.update_calls, stats.update_cpu_calls);
    try std.testing.expectEqual(@as(u64, 0), stats.update_metal_calls);
}

test "kmeans metal update strategy requires Metal context when backend is auto" {
    const dims = 2;
    var dense_vectors = [_]f32{
        0,  0,
        0,  1,
        10, 10,
        10, 11,
    };
    var points = [_]Point{
        .{ .stable_id = 10, .vector = dense_vectors[0..2] },
        .{ .stable_id = 11, .vector = dense_vectors[2..4] },
        .{ .stable_id = 20, .vector = dense_vectors[4..6] },
        .{ .stable_id = 21, .vector = dense_vectors[6..8] },
    };
    var centroids: [4]f32 = undefined;
    var next_centroids: [4]f32 = undefined;
    var assignments: [4]usize = undefined;
    var distances: [4]f32 = undefined;
    var counts: [2]usize = undefined;
    var entries: [4]Entry = undefined;

    try std.testing.expectError(error.MetalUnavailable, run(.{
        .dims = dims,
        .metric = .l2_squared,
        .max_iter = 4,
        .backend = .auto,
        .update_strategy = .metal,
        .dense_vectors = &dense_vectors,
    }, &points, 0, &centroids, &next_centroids, &assignments, &distances, &counts, &entries));
}

test "kmeans forced metal backend requires dense vectors" {
    const dims = 2;
    var vectors = [_]f32{
        0,  0,
        0,  1,
        10, 10,
        10, 11,
    };
    var points = [_]Point{
        .{ .stable_id = 10, .vector = vectors[0..2] },
        .{ .stable_id = 11, .vector = vectors[2..4] },
        .{ .stable_id = 20, .vector = vectors[4..6] },
        .{ .stable_id = 21, .vector = vectors[6..8] },
    };
    var centroids: [4]f32 = undefined;
    var next_centroids: [4]f32 = undefined;
    var assignments: [4]usize = undefined;
    var distances: [4]f32 = undefined;
    var counts: [2]usize = undefined;
    var entries: [4]Entry = undefined;

    try std.testing.expectError(error.MetalRequiresDenseVectors, run(.{
        .dims = dims,
        .metric = .l2_squared,
        .max_iter = 4,
        .backend = .metal,
        .update_strategy = .scatter,
    }, &points, 0, &centroids, &next_centroids, &assignments, &distances, &counts, &entries));
}
