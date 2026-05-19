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
const vector_quantizer = @import("antfly_vector").quantizer;
const vector_types = @import("antfly_vector").vector;
const vector_segment_mod = @import("../vector_segment/mod.zig");

pub const BuildPolicy = struct {
    target_cluster_count: ?usize = null,
    base_probe_count: ?u32 = null,
    shortlist_multiplier: ?u32 = null,
};

pub fn buildClusteredSegmentAlloc(
    alloc: Allocator,
    metric: vector_types.DistanceMetric,
    dims: u32,
    entries: []vector_segment_mod.Entry,
) !vector_segment_mod.Segment {
    return try buildClusteredSegmentWithPolicyAlloc(alloc, metric, dims, entries, .{});
}

pub fn buildClusteredSegmentWithPolicyAlloc(
    alloc: Allocator,
    metric: vector_types.DistanceMetric,
    dims: u32,
    entries: []vector_segment_mod.Entry,
    policy: BuildPolicy,
) !vector_segment_mod.Segment {
    const dim_count: usize = @intCast(dims);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    if (metric == .cosine) {
        for (entries) |entry| {
            _ = vector_types.normalize(entry.vector);
        }
    }

    const cluster_count = desiredClusterCount(dims, entries.len, policy);
    const centroids = try alloc.alloc([]f32, cluster_count);
    defer {
        for (centroids) |centroid| alloc.free(centroid);
        alloc.free(centroids);
    }

    try initializeCentroids(alloc, metric, centroids, entries);

    const cluster_ids = try alloc.alloc(u32, entries.len);
    defer alloc.free(cluster_ids);

    const scratch_counts = try alloc.alloc(u32, cluster_count);
    defer alloc.free(scratch_counts);
    const scratch_sums = try alloc.alloc(f32, cluster_count * dim_count);
    defer alloc.free(scratch_sums);

    for (0..3) |_| {
        assignClusters(metric, entries, centroids, cluster_ids);
        try recomputeCentroids(alloc, metric, centroids, entries, cluster_ids, scratch_counts, scratch_sums);
    }
    assignClusters(metric, entries, centroids, cluster_ids);
    try rebalanceClusters(alloc, metric, entries, centroids, cluster_ids);
    try recomputeCentroids(alloc, metric, centroids, entries, cluster_ids, scratch_counts, scratch_sums);

    const counts = try alloc.alloc(u32, cluster_count);
    defer alloc.free(counts);
    @memset(counts, 0);
    for (cluster_ids) |cluster_id| counts[cluster_id] += 1;

    const clusters = try alloc.alloc(vector_segment_mod.Cluster, cluster_count);
    errdefer alloc.free(clusters);
    var cluster_init: usize = 0;
    errdefer {
        for (clusters[0..cluster_init]) |*cluster| cluster.deinit(alloc);
    }

    var offset: u32 = 0;
    for (0..cluster_count) |cluster_idx| {
        const centroid = try alloc.dupe(f32, centroids[cluster_idx]);
        clusters[cluster_idx] = .{
            .centroid = centroid,
            .start_index = offset,
            .entry_count = counts[cluster_idx],
            .routing_distance_min = 0,
            .routing_distance_max = 0,
            .routing_distance_avg = 0,
            .quantized_set = try alloc.alloc(u8, 0),
            .exact_entries = try alloc.alloc(u8, 0),
        };
        offset += counts[cluster_idx];
        cluster_init += 1;
    }

    const sorted_entries = try alloc.alloc(vector_segment_mod.Entry, entries.len);
    errdefer alloc.free(sorted_entries);
    var sorted_init: usize = 0;
    errdefer {
        for (sorted_entries[0..sorted_init]) |*entry| entry.deinit(alloc);
    }

    const write_offsets = try alloc.alloc(u32, cluster_count);
    defer alloc.free(write_offsets);
    for (clusters, 0..) |cluster, idx| write_offsets[idx] = cluster.start_index;

    for (entries, 0..) |entry, idx| {
        const cluster_idx: usize = cluster_ids[idx];
        const dst = write_offsets[cluster_idx];
        write_offsets[cluster_idx] += 1;
        sorted_entries[dst] = .{
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .vector = try alloc.dupe(f32, entry.vector),
        };
        sorted_init += 1;
    }

    var quantizer = try vector_quantizer.RaBitQuantizer.init(alloc, dim_count, 42, metric);
    defer quantizer.deinit();
    for (clusters) |*cluster| {
        const start: usize = cluster.start_index;
        const end: usize = start + cluster.entry_count;
        alloc.free(cluster.quantized_set);
        cluster.quantized_set = try alloc.alloc(u8, 0);
        alloc.free(cluster.exact_entries);
        cluster.exact_entries = try alloc.alloc(u8, 0);
        if (start >= end) continue;

        std.mem.sort(
            vector_segment_mod.Entry,
            sorted_entries[start..end],
            ClusterSortContext{
                .centroid = cluster.centroid,
                .metric = metric,
            },
            lessEntryForCluster,
        );

        var distance_min = std.math.inf(f32);
        var distance_max: f32 = 0;
        var distance_sum: f32 = 0;
        for (sorted_entries[start..end]) |entry| {
            const distance = routingDistance(entry.vector, cluster.centroid, metric);
            distance_min = @min(distance_min, distance);
            distance_max = @max(distance_max, distance);
            distance_sum += distance;
        }
        cluster.routing_distance_min = if (distance_min == std.math.inf(f32)) 0 else distance_min;
        cluster.routing_distance_max = distance_max;
        cluster.routing_distance_avg = distance_sum / @as(f32, @floatFromInt(@max(@as(u32, 1), cluster.entry_count)));

        const encoded_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, sorted_entries[start..end]);
        alloc.free(cluster.exact_entries);
        cluster.exact_entries = encoded_exact;

        const flat_vectors = try alloc.alloc(f32, @as(usize, cluster.entry_count) * dim_count);
        defer alloc.free(flat_vectors);
        for (sorted_entries[start..end], 0..) |entry, idx| {
            @memcpy(flat_vectors[idx * dim_count ..][0..dim_count], entry.vector);
        }
        var quantized = try quantizer.quantize(cluster.centroid, flat_vectors, @intCast(cluster.entry_count));
        defer quantized.deinit(alloc);
        const encoded_quantized = try quantized.encode(alloc);
        alloc.free(cluster.quantized_set);
        cluster.quantized_set = encoded_quantized;
    }

    return .{
        .dims = dims,
        .metric = metric,
        .base_probe_count = policy.base_probe_count orelse suggestBaseProbeCount(cluster_count),
        .shortlist_multiplier = policy.shortlist_multiplier orelse suggestShortlistMultiplier(cluster_count, entries.len),
        .clusters = clusters,
        .entries = sorted_entries,
    };
}

fn suggestBaseProbeCount(cluster_count: usize) u32 {
    if (cluster_count <= 1) return 1;
    const logish: usize = @intFromFloat(@ceil(std.math.log2(@as(f64, @floatFromInt(cluster_count)))));
    return @intCast(@min(cluster_count, @max(@as(usize, 2), logish + 1)));
}

fn suggestShortlistMultiplier(cluster_count: usize, entry_count: usize) u32 {
    if (entry_count <= cluster_count * 2) return 2;
    if (cluster_count >= 16) return 4;
    if (cluster_count >= 8) return 3;
    return 2;
}

fn desiredClusterCount(dims: u32, entry_count: usize, policy: BuildPolicy) usize {
    if (entry_count == 0) return 0;
    if (policy.target_cluster_count) |target| {
        return @min(entry_count, @max(@as(usize, 1), target));
    }
    const sqrt_estimate: usize = @intFromFloat(@ceil(@sqrt(@as(f64, @floatFromInt(entry_count)))));
    const target_cluster_size: usize = if (dims <= 64) 64 else if (dims <= 256) 48 else 32;
    const size_estimate = std.math.divCeil(usize, entry_count, target_cluster_size) catch 1;
    const blended = @max(size_estimate, sqrt_estimate);
    return @min(entry_count, @max(@as(usize, 1), @min(@as(usize, 64), blended)));
}

fn rebalanceClusters(
    alloc: Allocator,
    metric: vector_types.DistanceMetric,
    entries: []const vector_segment_mod.Entry,
    centroids: []const []f32,
    cluster_ids: []u32,
) !void {
    if (centroids.len <= 1 or entries.len <= centroids.len) return;

    const target_size = std.math.divCeil(usize, entries.len, centroids.len) catch entries.len;
    const max_size = @max(target_size + @max(@as(usize, 2), target_size / 2), target_size + 1);
    const min_size = if (target_size <= 2) 1 else target_size - @max(@as(usize, 1), target_size / 3);

    const counts = try alloc.alloc(usize, centroids.len);
    defer alloc.free(counts);
    @memset(counts, 0);
    for (cluster_ids) |cluster_id| counts[cluster_id] += 1;

    for (entries, 0..) |entry, entry_idx| {
        const current_cluster: usize = cluster_ids[entry_idx];
        if (counts[current_cluster] <= max_size) continue;

        var best_cluster: ?usize = null;
        var best_score = routingScore(entry.vector, centroids[current_cluster], metric);

        for (centroids, 0..) |centroid, candidate_cluster| {
            if (candidate_cluster == current_cluster) continue;
            if (counts[candidate_cluster] >= target_size and counts[candidate_cluster] >= min_size) continue;
            const score = routingScore(entry.vector, centroid, metric);
            if (best_cluster == null or score > best_score) {
                best_cluster = candidate_cluster;
                best_score = score;
            }
        }

        if (best_cluster) |candidate_cluster| {
            counts[current_cluster] -= 1;
            counts[candidate_cluster] += 1;
            cluster_ids[entry_idx] = @intCast(candidate_cluster);
        }
    }
}

fn initializeCentroids(
    alloc: Allocator,
    metric: vector_types.DistanceMetric,
    centroids: [][]f32,
    entries: []const vector_segment_mod.Entry,
) !void {
    if (centroids.len == 0) return;
    centroids[0] = try centroidCopyAlloc(alloc, metric, entries[0].vector);
    for (1..centroids.len) |idx| {
        const selected = selectFarthestEntryIndex(metric, entries, centroids[0..idx]);
        centroids[idx] = try centroidCopyAlloc(alloc, metric, entries[selected].vector);
    }
}

fn selectFarthestEntryIndex(
    metric: vector_types.DistanceMetric,
    entries: []const vector_segment_mod.Entry,
    centroids: []const []f32,
) usize {
    var best_index: usize = 0;
    var best_distance: f32 = -std.math.inf(f32);
    for (entries, 0..) |entry, idx| {
        var closest_distance: f32 = std.math.inf(f32);
        for (centroids) |centroid| {
            const distance = routingDistance(entry.vector, centroid, metric);
            if (distance < closest_distance) closest_distance = distance;
        }
        if (closest_distance > best_distance) {
            best_distance = closest_distance;
            best_index = idx;
        }
    }
    return best_index;
}

fn assignClusters(
    metric: vector_types.DistanceMetric,
    entries: []const vector_segment_mod.Entry,
    centroids: []const []f32,
    cluster_ids: []u32,
) void {
    for (entries, 0..) |entry, idx| {
        var best_cluster: usize = 0;
        var best_score: f32 = -std.math.inf(f32);
        for (centroids, 0..) |centroid, cluster_idx| {
            const score = routingScore(entry.vector, centroid, metric);
            if (score > best_score) {
                best_score = score;
                best_cluster = cluster_idx;
            }
        }
        cluster_ids[idx] = @intCast(best_cluster);
    }
}

fn recomputeCentroids(
    alloc: Allocator,
    metric: vector_types.DistanceMetric,
    centroids: [][]f32,
    entries: []const vector_segment_mod.Entry,
    cluster_ids: []const u32,
    counts: []u32,
    sums: []f32,
) !void {
    const dim_count = centroids[0].len;
    @memset(counts, 0);
    @memset(sums, 0);

    for (entries, 0..) |entry, idx| {
        const cluster_idx: usize = cluster_ids[idx];
        counts[cluster_idx] += 1;
        const sum_slice = sums[cluster_idx * dim_count ..][0..dim_count];
        for (entry.vector, 0..) |value, dim_idx| sum_slice[dim_idx] += value;
    }

    for (centroids, 0..) |centroid, cluster_idx| {
        if (counts[cluster_idx] == 0) {
            const replacement = selectFarthestEntryIndex(metric, entries, centroids);
            @memcpy(centroid, entries[replacement].vector);
            finalizeCentroid(metric, centroid);
            continue;
        }
        const sum_slice = sums[cluster_idx * dim_count ..][0..dim_count];
        for (0..dim_count) |dim_idx| {
            centroid[dim_idx] = sum_slice[dim_idx] / @as(f32, @floatFromInt(counts[cluster_idx]));
        }
        finalizeCentroid(metric, centroid);
    }

    _ = alloc;
}

fn centroidCopyAlloc(alloc: Allocator, metric: vector_types.DistanceMetric, vector: []const f32) ![]f32 {
    const copy = try alloc.dupe(f32, vector);
    finalizeCentroid(metric, copy);
    return copy;
}

fn finalizeCentroid(metric: vector_types.DistanceMetric, vector: []f32) void {
    if (metric == .cosine) _ = vector_types.normalize(vector);
}

fn routingScore(lhs: []const f32, rhs: []const f32, metric: vector_types.DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => -vector_types.distance(lhs, rhs, .l2_squared),
        .inner_product => vector_types.dot(lhs, rhs),
        .cosine => vector_types.cosineSimilarity(lhs, rhs),
    };
}

fn routingDistance(lhs: []const f32, rhs: []const f32, metric: vector_types.DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => vector_types.distance(lhs, rhs, .l2_squared),
        .inner_product => -vector_types.dot(lhs, rhs),
        .cosine => vector_types.distance(lhs, rhs, .cosine),
    };
}

const ClusterSortContext = struct {
    centroid: []const f32,
    metric: vector_types.DistanceMetric,
};

fn lessEntryForCluster(ctx: ClusterSortContext, lhs: vector_segment_mod.Entry, rhs: vector_segment_mod.Entry) bool {
    const lhs_distance = routingDistance(lhs.vector, ctx.centroid, ctx.metric);
    const rhs_distance = routingDistance(rhs.vector, ctx.centroid, ctx.metric);
    if (lhs_distance == rhs_distance) {
        return std.mem.order(u8, lhs.doc_id, rhs.doc_id) == .lt;
    }
    return lhs_distance < rhs_distance;
}

test "vector builder scales cluster count beyond four for larger corpora" {
    const alloc = std.testing.allocator;
    const dims: u32 = 2;
    const entry_count: usize = 25;
    const entries = try alloc.alloc(vector_segment_mod.Entry, entry_count);
    errdefer alloc.free(entries);
    for (entries, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "doc-{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt((idx % 5) + 1)),
                @as(f32, @floatFromInt((idx / 5) + 1)),
            }),
        };
    }

    var segment = try buildClusteredSegmentAlloc(alloc, .cosine, dims, entries);
    defer vector_segment_mod.freeSegment(alloc, &segment);

    try std.testing.expect(segment.clusters.len > 4);
    try std.testing.expectEqual(entry_count, segment.entries.len);
    try std.testing.expectEqual(@as(u32, @intCast(entry_count)), segment.clusters[segment.clusters.len - 1].start_index + segment.clusters[segment.clusters.len - 1].entry_count);
    for (segment.clusters) |cluster| {
        try std.testing.expect(cluster.routing_distance_max >= cluster.routing_distance_min);
        try std.testing.expect(cluster.routing_distance_avg >= cluster.routing_distance_min);
    }
}

test "vector builder preserves inner product centroids without cosine normalization" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(vector_segment_mod.Entry, 2);
    errdefer alloc.free(entries);
    entries[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .vector = try alloc.dupe(f32, &.{ 4.0, 0.0 }),
    };
    entries[1] = .{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .vector = try alloc.dupe(f32, &.{ 0.0, 2.0 }),
    };

    var segment = try buildClusteredSegmentAlloc(alloc, .inner_product, 2, entries);
    defer vector_segment_mod.freeSegment(alloc, &segment);

    try std.testing.expect(segment.clusters.len >= 1);
    const centroid = segment.clusters[0].centroid;
    try std.testing.expect(vector_types.norm(centroid) != 1.0);
}

test "vector builder uses more clusters for higher dimensional data" {
    const alloc = std.testing.allocator;
    const entry_count: usize = 128;

    const low_dim_entries = try alloc.alloc(vector_segment_mod.Entry, entry_count);
    errdefer alloc.free(low_dim_entries);
    for (low_dim_entries, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "low-{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt(idx % 7)),
                @as(f32, @floatFromInt(idx % 11)),
            }),
        };
    }

    const high_dim_entries = try alloc.alloc(vector_segment_mod.Entry, entry_count);
    errdefer alloc.free(high_dim_entries);
    for (high_dim_entries, 0..) |*entry, idx| {
        const vector = try alloc.alloc(f32, 384);
        for (vector, 0..) |*value, dim_idx| {
            value.* = @floatFromInt((idx + dim_idx) % 13);
        }
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "high-{d}", .{idx}),
            .vector = vector,
        };
    }

    var low_segment = try buildClusteredSegmentAlloc(alloc, .cosine, 2, low_dim_entries);
    defer vector_segment_mod.freeSegment(alloc, &low_segment);
    var high_segment = try buildClusteredSegmentAlloc(alloc, .cosine, 384, high_dim_entries);
    defer vector_segment_mod.freeSegment(alloc, &high_segment);

    try std.testing.expect(high_segment.clusters.len >= low_segment.clusters.len);
}

test "vector builder rebalances oversized clusters" {
    const alloc = std.testing.allocator;
    const entry_count: usize = 96;
    const entries = try alloc.alloc(vector_segment_mod.Entry, entry_count);
    errdefer alloc.free(entries);

    for (entries, 0..) |*entry, idx| {
        const x: f32 = if (idx < 80) @floatFromInt(idx % 5) else @floatFromInt(100 + (idx % 8));
        const y: f32 = if (idx < 80) @floatFromInt((idx % 3) + 1) else @floatFromInt(100 + (idx % 6));
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "doc-{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{ x, y }),
        };
    }

    var segment = try buildClusteredSegmentAlloc(alloc, .cosine, 2, entries);
    defer vector_segment_mod.freeSegment(alloc, &segment);

    var min_count: u32 = std.math.maxInt(u32);
    var max_count: u32 = 0;
    for (segment.clusters) |cluster| {
        min_count = @min(min_count, cluster.entry_count);
        max_count = @max(max_count, cluster.entry_count);
    }
    try std.testing.expect(max_count - min_count <= 48);
}

test "vector builder honors explicit target cluster count policy" {
    const alloc = std.testing.allocator;
    const entry_count: usize = 12;
    const entries = try alloc.alloc(vector_segment_mod.Entry, entry_count);
    errdefer alloc.free(entries);
    for (entries, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "doc-{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt(idx)),
                @as(f32, @floatFromInt(idx % 3)),
            }),
        };
    }

    var segment = try buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries, .{
        .target_cluster_count = 5,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment);

    try std.testing.expectEqual(@as(usize, 5), segment.clusters.len);
}

test "vector builder honors query tuning policy" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(vector_segment_mod.Entry, 4);
    errdefer alloc.free(entries);
    for (entries, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "doc-{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{ @floatFromInt(idx), @floatFromInt(idx + 1) }),
        };
    }

    var segment = try buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries, .{
        .base_probe_count = 5,
        .shortlist_multiplier = 7,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment);

    try std.testing.expectEqual(@as(u32, 5), segment.base_probe_count);
    try std.testing.expectEqual(@as(u32, 7), segment.shortlist_multiplier);
}
