// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
    emeans: bool = false,
    allocator: ?std.mem.Allocator = null,
    emeans_epochs: u32 = 5,
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

    if (config.emeans) return runEmeans(config, points, first_index, centroids, next_centroids, assignments, distances, counts, entries);

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

/// Route unit-weight points to fixed centers and produce the sorted entries
/// expected by the bulk leaf packer.
pub fn assignFixedCenters(
    config: RunConfig,
    points: []const Point,
    centers: []const f32,
    assignments: []usize,
    distances: []f32,
    entries: []Entry,
) !RunStats {
    if (config.dims == 0 or centers.len == 0 or centers.len % config.dims != 0) return error.InvalidDimensions;
    if (assignments.len != points.len or distances.len != points.len or entries.len != points.len) return error.InvalidDimensions;
    const started = monotonicNs();
    assign(config, points, centers, assignments, distances);
    var stats: RunStats = .{};
    try recordAssignmentStats(&stats, points.len, .{ .used_metal = false, .ns = elapsedNsSince(started) });
    populateEntries(entries, assignments, distances);
    sortEntries(points, entries);
    return stats;
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
        @backingInt(config.metric),
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

pub fn monotonicNs() u64 {
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
    try context.updateCentroids(old_centroids, centroids, counts, config.dims, @backingInt(config.metric));
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

// Bessa et al. (2026), https://arxiv.org/abs/2611.09501.
// CPU e-means: fitted laws, shuffled batches, cosine decay, revival, closing
// means. One extra exact assignment supplies HBC's final packing labels.
fn runEmeans(config: RunConfig, points: []const Point, seed: usize, centroids: []f32, sums: []f32, assignments: []usize, distances: []f32, counts: []usize, entries: []Entry) !RunStats {
    if (config.metric != .l2_squared) return error.EmeansRequiresL2;
    if (config.backend == .metal) return error.EmeansRequiresCpu;
    if (!allUnitWeights(points)) return error.EmeansRequiresUnitWeights;
    if (config.emeans_epochs == 0) return error.InvalidEmeansEpochs;
    for (points) |point| {
        if (point.vector.len != config.dims) return error.InvalidDimensions;
        for (point.vector) |v| if (!std.math.isFinite(v)) return error.InvalidEmeansVector;
    }
    const alloc = config.allocator orelse return error.EmeansRequiresAllocator;
    const k = counts.len;
    if (k > points.len) return error.TooFewVectors;
    const order = try alloc.alloc(usize, points.len);
    defer alloc.free(order);
    const mass = try alloc.alloc(f32, k);
    defer alloc.free(mass);
    const closing_sums = try alloc.alloc(f32, centroids.len);
    defer alloc.free(closing_sums);
    const closing_counts = try alloc.alloc(usize, k);
    defer alloc.free(closing_counts);
    var prng = std.Random.DefaultPrng.init(@intCast(seed));
    const random = prng.random();
    for (order, 0..) |*index, i| index.* = i;
    random.shuffle(usize, order);
    for (0..k) |c| @memcpy(centroids[c * config.dims ..][0..config.dims], points[order[c]].vector);
    @memset(mass, 0x1p-16);
    const recipe = emeansRecipe(points.len, k, config.dims, config.emeans_epochs);
    const steps = std.math.divCeil(usize, points.len, recipe.batch) catch unreachable;
    const total_steps = @as(f64, @floatFromInt(steps)) * @as(f64, @floatFromInt(config.emeans_epochs));
    var step: usize = 0;
    var stats = RunStats{};
    for (0..config.emeans_epochs) |epoch| {
        random.shuffle(usize, order);
        const closing = epoch + 1 == config.emeans_epochs;
        if (closing) {
            @memset(closing_sums, 0);
            @memset(closing_counts, 0);
        }
        var begin: usize = 0;
        while (begin < points.len) {
            const end = @min(begin + recipe.batch, points.len);
            const batch = order[begin..end];
            const started = monotonicNs();
            for (batch) |i| assign(config, points[i..][0..1], centroids, assignments[i..][0..1], distances[i..][0..1]);
            try recordAssignmentStats(&stats, batch.len, .{ .used_metal = false, .ns = elapsedNsSince(started) });
            const update_started = monotonicNs();
            @memset(sums, 0);
            @memset(counts, 0);
            for (batch) |i| {
                const c = assignments[i];
                counts[c] += 1;
                addWeightedVector(sums[c * config.dims ..][0..config.dims], points[i].vector, 1);
                if (closing) {
                    closing_counts[c] += 1;
                    addWeightedVector(closing_sums[c * config.dims ..][0..config.dims], points[i].vector, 1);
                }
            }
            const progress = @as(f64, @floatFromInt(step)) / total_steps;
            const alpha: f32 = @floatCast(recipe.alpha * (0.1 + 0.45 * (1 + @cos(std.math.pi * progress))));
            for (0..k) |c| {
                const n: f32 = @floatFromInt(counts[c]);
                mass[c] = (1 - alpha) * mass[c] + alpha * n;
                if (n == 0) continue;
                const weight = alpha * n / mass[c];
                for (centroids[c * config.dims ..][0..config.dims], sums[c * config.dims ..][0..config.dims]) |*value, sum| value.* += weight * (sum / n - value.*);
            }
            step += 1;
            if (config.dims > 2 and step % 5 == 0) reviveEmeans(config.dims, centroids, mass, counts, random);
            recordUpdateStats(&stats, false, elapsedNsSince(update_started));
            begin = end;
        }
    }
    const closing_started = monotonicNs();
    for (closing_counts, 0..) |n, c| {
        if (n == 0) continue;
        for (centroids[c * config.dims ..][0..config.dims], closing_sums[c * config.dims ..][0..config.dims]) |*value, sum| value.* = sum / @as(f32, @floatFromInt(n));
    }
    recordUpdateStats(&stats, false, elapsedNsSince(closing_started));
    const started = monotonicNs();
    assign(config, points, centroids, assignments, distances);
    try recordAssignmentStats(&stats, points.len, .{ .used_metal = false, .ns = elapsedNsSince(started) });
    @memset(counts, 0);
    for (assignments) |c| counts[c] += 1;
    populateEntries(entries, assignments, distances);
    sortEntries(points, entries);
    return stats;
}

fn emeansRecipe(n: usize, k: usize, dims: usize, epochs: u32) struct { batch: usize, alpha: f64 } {
    const nf: f64 = @floatFromInt(n);
    const kf: f64 = @floatFromInt(k);
    const t: f64 = @floatFromInt(epochs);
    const coefficient: f64 = if (dims <= 16) 0.00439175 else 0.035134;
    const raw = coefficient * std.math.pow(f64, kf, 0.134749) * std.math.pow(f64, nf, 0.876473) * std.math.pow(f64, t, 0.813344);
    const cap = if (dims <= 16) n / 2 else roundEmeansBatch(nf / 2);
    const batch = @max(1, @min(n, @min(cap, roundEmeansBatch(raw))));
    const alpha = 0.8244 * std.math.pow(f64, kf, -0.4617) * std.math.pow(f64, @floatFromInt(batch), 0.9003) * std.math.pow(f64, nf, -0.4498) * std.math.pow(f64, t, -0.1057);
    return .{ .batch = batch, .alpha = std.math.clamp(alpha, 0.0001, 1) };
}
fn roundEmeansBatch(value: f64) usize {
    if (value <= 1) return 1;
    return @intFromFloat(std.math.pow(f64, 2, @floor(@log2(value) + 0.5)));
}
fn reviveEmeans(dims: usize, centroids: []f32, mass: []f32, used: []usize, random: std.Random) void {
    var total: f32 = 0;
    for (mass) |m| total += m;
    const threshold = 0.1 * total / @as(f32, @floatFromInt(mass.len));
    @memset(used, 0);
    for (mass, 0..) |m, target| {
        if (m >= threshold or used[target] != 0) continue;
        var donor_mass: f64 = 0;
        for (mass, used) |weight, selected| {
            if (selected == 0 and weight >= threshold) donor_mass += weight;
        }
        if (donor_mass == 0) break;
        var ticket = random.float(f64) * donor_mass;
        var donor: usize = 0;
        for (mass, used, 0..) |weight, selected, i| {
            if (selected != 0 or weight < threshold) continue;
            donor = i;
            ticket -= weight;
            if (ticket <= 0) break;
        }
        used[donor] = 1;
        used[target] = 1;
        for (0..dims) |d| {
            const value = centroids[donor * dims + d];
            const offset: f32 = if (d % 2 == 0) 0.0001 else -0.0001;
            centroids[target * dims + d] = value * (1 + offset);
            centroids[donor * dims + d] = value * (1 - offset);
        }
        if (std.mem.eql(f32, centroids[donor * dims ..][0..dims], centroids[target * dims ..][0..dims])) {
            centroids[target * dims] = std.math.nextAfter(f32, centroids[target * dims], std.math.inf(f32));
        }
        mass[donor] *= 0.5;
        mass[target] = mass[donor];
    }
}

test "kmeans e-means deterministic separated clusters and exact final labels" {
    const raw = [_]f32{ 0, 0, 0, 1, 10, 10, 10, 11 };
    var points: [4]Point = undefined;
    for (&points, 0..) |*point, i| point.* = .{ .stable_id = i, .vector = raw[i * 2 ..][0..2] };
    var centroids: [4]f32 = undefined;
    var next: [4]f32 = undefined;
    var labels: [4]usize = undefined;
    var distances: [4]f32 = undefined;
    var counts: [2]usize = undefined;
    var entries: [4]Entry = undefined;
    var config = RunConfig{ .dims = 2, .metric = .l2_squared, .max_iter = 16, .emeans = true, .allocator = std.testing.allocator };
    const stats = try run(config, &points, 42, &centroids, &next, &labels, &distances, &counts, &entries);
    try std.testing.expectEqual(@as(u64, 24), stats.assignment_points_total); // 5 epochs + final assignment
    try std.testing.expectEqual(@as(u64, 0), stats.assignment_metal_calls);
    try std.testing.expectEqual(labels[0], labels[1]);
    try std.testing.expectEqual(labels[2], labels[3]);
    try std.testing.expect(labels[0] != labels[2]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), centroids[labels[0] * 2 + 1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 10.5), centroids[labels[2] * 2 + 1], 0.0001);
    for (points, 0..) |point, i| {
        const actual = vec.distance(point.vector, centroids[labels[i] * 2 ..][0..2], .l2_squared);
        try std.testing.expectEqual(actual, distances[i]);
        for (0..2) |c| try std.testing.expect(actual <= vec.distance(point.vector, centroids[c * 2 ..][0..2], .l2_squared));
    }
    const expected = centroids;
    _ = try run(config, &points, 42, &centroids, &next, &labels, &distances, &counts, &entries);
    try std.testing.expectEqualSlices(f32, &expected, &centroids);
    config.metric = .cosine;
    try std.testing.expectError(error.EmeansRequiresL2, run(config, &points, 42, &centroids, &next, &labels, &distances, &counts, &entries));
    config.metric = .l2_squared;
    config.backend = .metal;
    try std.testing.expectError(error.EmeansRequiresCpu, run(config, &points, 42, &centroids, &next, &labels, &distances, &counts, &entries));
    config.backend = .cpu;
    points[0].weight = 2;
    try std.testing.expectError(error.EmeansRequiresUnitWeights, run(config, &points, 42, &centroids, &next, &labels, &distances, &counts, &entries));
}

test "kmeans e-means revival splits mass without reusing donors" {
    var centroids = [_]f32{ 1, 2, 3, 0, 0, 0, 0, 0, 0 };
    var masses = [_]f32{ 100, 0, 0 };
    var used: [3]usize = undefined;
    var prng = std.Random.DefaultPrng.init(42);
    reviveEmeans(3, &centroids, &masses, &used, prng.random());
    try std.testing.expectEqual(@as(f32, 50), masses[0]);
    try std.testing.expectEqual(@as(f32, 50), masses[1]);
    try std.testing.expectEqual(@as(f32, 0), masses[2]);
    try std.testing.expect(centroids[0] < centroids[3]);
}

test "kmeans e-means identical vectors and singleton remain finite" {
    const raw = [_]f32{ 2, 2, 2 };
    const points = [_]Point{ .{ .stable_id = 1, .vector = &raw }, .{ .stable_id = 2, .vector = &raw }, .{ .stable_id = 3, .vector = &raw } };
    var centers: [6]f32 = undefined;
    var sums: [6]f32 = undefined;
    var labels: [3]usize = undefined;
    var distances: [3]f32 = undefined;
    var counts: [2]usize = undefined;
    var entries: [3]Entry = undefined;
    var config = RunConfig{ .dims = 3, .metric = .l2_squared, .max_iter = 16, .emeans = true, .allocator = std.testing.allocator };
    _ = try run(config, &points, 0, &centers, &sums, &labels, &distances, &counts, &entries);
    for (centers) |v| try std.testing.expect(std.math.isFinite(v));
    for (distances) |v| try std.testing.expectEqual(@as(f32, 0), v);
    _ = try run(config, points[0..1], 0, centers[0..3], sums[0..3], labels[0..1], distances[0..1], counts[0..1], entries[0..1]);
    try std.testing.expectEqualSlices(f32, &raw, centers[0..3]);
    config.emeans_epochs = 0;
    try std.testing.expectError(error.InvalidEmeansEpochs, run(config, &points, 0, &centers, &sums, &labels, &distances, &counts, &entries));
}

test "kmeans e-means fitted recipe follows reference laws" {
    const recipe = emeansRecipe(10000, 100, 128, 5);
    try std.testing.expectEqual(@as(usize, 1024), recipe.batch);
    try std.testing.expect(recipe.alpha >= 0.0001 and recipe.alpha <= 1);
    try std.testing.expectEqual(@as(usize, 128), emeansRecipe(10000, 100, 2, 5).batch);
}
