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
const vector_types = @import("types.zig");
const shared_vector = @import("antfly_vector").vector;
const bounded_decode = @import("../bounded_decode.zig");

pub const DecodeLimits = bounded_decode.Limits;

pub const Header = struct {
    metric: shared_vector.DistanceMetric,
    dims: u32,
    cluster_count: u32,
    entry_count: u32,
    base_probe_count: u32,
    shortlist_multiplier: u32,
};

pub const header_len: usize = 24;

pub fn clusterRecordLen(dims: u32) usize {
    return 36 + (@as(usize, dims) * @sizeOf(f32));
}

pub fn decodeHeader(payload: []const u8) !Header {
    if (payload.len < header_len) return error.InvalidVectorSegmentPayload;
    const metric_raw = std.mem.readInt(u32, payload[0..4], .little);
    return .{
        .metric = switch (metric_raw) {
            @backingInt(shared_vector.DistanceMetric.l2_squared) => .l2_squared,
            @backingInt(shared_vector.DistanceMetric.inner_product) => .inner_product,
            @backingInt(shared_vector.DistanceMetric.cosine) => .cosine,
            else => return error.InvalidVectorSegmentPayload,
        },
        .dims = std.mem.readInt(u32, payload[4..8], .little),
        .cluster_count = std.mem.readInt(u32, payload[8..12], .little),
        .entry_count = std.mem.readInt(u32, payload[12..16], .little),
        .base_probe_count = std.mem.readInt(u32, payload[16..20], .little),
        .shortlist_multiplier = std.mem.readInt(u32, payload[20..24], .little),
    };
}

pub fn encodeAlloc(alloc: Allocator, segment: vector_types.Segment) ![]u8 {
    const total_len = try encodedSize(segment);
    const metadata_len = std.math.mul(usize, clusterRecordLen(segment.dims), segment.clusters.len) catch unreachable;

    const buf = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(@backingInt(segment.metric)), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], segment.dims, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.clusters.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.entries.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], segment.base_probe_count, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], segment.shortlist_multiplier, .little);
    pos += 4;

    var payload_pos: u32 = @intCast(header_len + metadata_len);
    for (segment.clusters) |cluster| {
        const quantized_offset = payload_pos;
        const quantized_len: u32 = @intCast(cluster.quantized_set.len);
        payload_pos += quantized_len;
        const exact_entries_offset = payload_pos;
        const exact_entries_len: u32 = @intCast(cluster.exact_entries.len);
        payload_pos += exact_entries_len;

        std.mem.writeInt(u32, buf[pos..][0..4], cluster.start_index, .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], cluster.entry_count, .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(cluster.routing_distance_min), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(cluster.routing_distance_max), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(cluster.routing_distance_avg), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], quantized_offset, .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], quantized_len, .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], exact_entries_offset, .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], exact_entries_len, .little);
        pos += 4;
        for (cluster.centroid) |value| {
            const bits: u32 = @bitCast(value);
            std.mem.writeInt(u32, buf[pos..][0..4], bits, .little);
            pos += 4;
        }
    }

    for (segment.clusters) |cluster| {
        @memcpy(buf[pos..][0..cluster.quantized_set.len], cluster.quantized_set);
        pos += cluster.quantized_set.len;
        @memcpy(buf[pos..][0..cluster.exact_entries.len], cluster.exact_entries);
        pos += cluster.exact_entries.len;
    }
    return buf;
}

pub fn encodedSize(segment: vector_types.Segment) !usize {
    _ = std.math.cast(u32, segment.clusters.len) orelse return error.VectorSegmentTooLarge;
    _ = std.math.cast(u32, segment.entries.len) orelse return error.VectorSegmentTooLarge;
    const dim_count: usize = @intCast(segment.dims);
    const metadata_len = std.math.mul(usize, clusterRecordLen(segment.dims), segment.clusters.len) catch
        return error.VectorSegmentTooLarge;
    var total_len = std.math.add(usize, header_len, metadata_len) catch return error.VectorSegmentTooLarge;
    var covered_entries: usize = 0;
    for (segment.clusters) |cluster| {
        if (cluster.centroid.len != dim_count or cluster.start_index != covered_entries) {
            return error.InvalidVectorSegment;
        }
        covered_entries = std.math.add(usize, covered_entries, cluster.entry_count) catch
            return error.VectorSegmentTooLarge;
        if (covered_entries > segment.entries.len) return error.InvalidVectorSegment;
        _ = std.math.cast(u32, cluster.quantized_set.len) orelse return error.VectorSegmentTooLarge;
        _ = std.math.cast(u32, cluster.exact_entries.len) orelse return error.VectorSegmentTooLarge;
        total_len = std.math.add(usize, total_len, cluster.quantized_set.len) catch return error.VectorSegmentTooLarge;
        total_len = std.math.add(usize, total_len, cluster.exact_entries.len) catch return error.VectorSegmentTooLarge;
    }
    if (covered_entries != segment.entries.len) return error.InvalidVectorSegment;
    _ = std.math.cast(u32, total_len) orelse return error.VectorSegmentTooLarge;
    return total_len;
}

test "lake vector segment codec rejects forged entry counts before allocation" {
    var payload = @as([header_len]u8, @splat(0));
    std.mem.writeInt(u32, payload[0..4], @backingInt(shared_vector.DistanceMetric.l2_squared), .little);
    std.mem.writeInt(u32, payload[4..8], 2, .little);
    std.mem.writeInt(u32, payload[12..16], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.DecodedArtifactTooLarge, decodeAlloc(std.testing.allocator, &payload));
}

pub fn decodeAlloc(alloc: Allocator, payload: []const u8) !vector_types.Segment {
    return try decodeAllocWithLimits(alloc, payload, .{});
}

pub fn decodeAllocWithLimits(alloc: Allocator, payload: []const u8, limits: DecodeLimits) !vector_types.Segment {
    var budget = try bounded_decode.Budget.init(payload.len, limits);
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, limits.max_allocation_bytes);
    return decodeBoundedAlloc(limiter.allocator(), payload, &budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.DecodedArtifactTooLarge;
        return err;
    };
}

fn decodeBoundedAlloc(alloc: Allocator, payload: []const u8, budget: *bounded_decode.Budget) !vector_types.Segment {
    const header = try decodeHeader(payload);
    const table_len = std.math.mul(usize, clusterRecordLen(header.dims), @intCast(header.cluster_count)) catch
        return error.InvalidVectorSegmentPayload;
    const table_end = std.math.add(usize, header_len, table_len) catch return error.InvalidVectorSegmentPayload;
    if (payload.len < table_end) return error.InvalidVectorSegmentPayload;
    _ = try budget.admitCount(vector_types.Cluster, header.cluster_count, payload.len - header_len, clusterRecordLen(header.dims));
    const min_entry_len = std.math.add(
        usize,
        4,
        std.math.mul(usize, @intCast(header.dims), 2) catch return error.InvalidVectorSegmentPayload,
    ) catch return error.InvalidVectorSegmentPayload;
    _ = try budget.admitCount(vector_types.Entry, header.entry_count, payload.len - table_end, min_entry_len);
    const table = payload[header_len..table_end];
    const clusters = try decodeClusterTableAlloc(alloc, header.dims, header.cluster_count, table);
    errdefer {
        for (clusters) |*cluster| cluster.deinit(alloc);
        alloc.free(clusters);
    }

    var expected_payload_offset: usize = table_end;
    var covered_entries: usize = 0;
    for (clusters) |cluster| {
        if (cluster.start_index != covered_entries or cluster.quantized_offset != expected_payload_offset) {
            return error.InvalidVectorSegmentPayload;
        }
        covered_entries = std.math.add(usize, covered_entries, cluster.entry_count) catch
            return error.InvalidVectorSegmentPayload;
        if (covered_entries > header.entry_count) return error.InvalidVectorSegmentPayload;
        const quantized_start = cluster.quantized_offset;
        const quantized_end = std.math.add(usize, quantized_start, cluster.quantized_len) catch return error.InvalidVectorSegmentPayload;
        if (quantized_end > payload.len or cluster.exact_entries_offset != quantized_end) return error.InvalidVectorSegmentPayload;
        const exact_start = cluster.exact_entries_offset;
        const exact_end = std.math.add(usize, exact_start, cluster.exact_entries_len) catch return error.InvalidVectorSegmentPayload;
        if (exact_end > payload.len) return error.InvalidVectorSegmentPayload;
        const minimum_exact_len = std.math.mul(usize, cluster.entry_count, min_entry_len) catch
            return error.InvalidVectorSegmentPayload;
        if (cluster.exact_entries_len < minimum_exact_len) return error.InvalidVectorSegmentPayload;
        expected_payload_offset = exact_end;
    }
    if (expected_payload_offset != payload.len or covered_entries != header.entry_count) {
        return error.InvalidVectorSegmentPayload;
    }

    for (clusters) |*cluster| {
        const quantized_start: usize = cluster.quantized_offset;
        const quantized_end = quantized_start + cluster.quantized_len;
        alloc.free(cluster.quantized_set);
        cluster.quantized_set = try alloc.dupe(u8, payload[quantized_start..quantized_end]);
        const exact_start: usize = cluster.exact_entries_offset;
        const exact_end = exact_start + cluster.exact_entries_len;
        alloc.free(cluster.exact_entries);
        cluster.exact_entries = try alloc.dupe(u8, payload[exact_start..exact_end]);
    }

    const entries = try alloc.alloc(vector_types.Entry, @intCast(header.entry_count));
    errdefer alloc.free(entries);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (clusters) |cluster| {
        const block = try decodeExactEntriesAlloc(
            alloc,
            header.dims,
            @intCast(cluster.entry_count),
            cluster.exact_entries,
        );
        defer alloc.free(block);
        const start: usize = cluster.start_index;
        const end = start + cluster.entry_count;
        std.debug.assert(end <= entries.len and block.len == end - start);
        for (block, start..) |entry, idx| {
            entries[idx] = entry;
            initialized += 1;
        }
    }
    if (initialized != entries.len) return error.InvalidVectorSegmentPayload;

    return .{
        .dims = header.dims,
        .metric = header.metric,
        .base_probe_count = header.base_probe_count,
        .shortlist_multiplier = header.shortlist_multiplier,
        .clusters = clusters,
        .entries = entries,
    };
}

pub fn decodeClusterTableAlloc(
    alloc: Allocator,
    dims: u32,
    cluster_count: u32,
    payload: []const u8,
) ![]vector_types.Cluster {
    const dim_count: usize = @intCast(dims);
    const record_len = clusterRecordLen(dims);
    const required_len = std.math.mul(usize, record_len, @intCast(cluster_count)) catch return error.InvalidVectorSegmentPayload;
    if (payload.len < required_len) return error.InvalidVectorSegmentPayload;

    const clusters = try alloc.alloc(vector_types.Cluster, @intCast(cluster_count));
    errdefer alloc.free(clusters);
    var initialized: usize = 0;
    errdefer {
        for (clusters[0..initialized]) |*cluster| cluster.deinit(alloc);
    }

    var pos: usize = 0;
    for (0..@as(usize, @intCast(cluster_count))) |idx| {
        const start_index = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        const entry_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        const routing_distance_min: f32 = @bitCast(std.mem.readInt(u32, payload[pos..][0..4], .little));
        pos += 4;
        const routing_distance_max: f32 = @bitCast(std.mem.readInt(u32, payload[pos..][0..4], .little));
        pos += 4;
        const routing_distance_avg: f32 = @bitCast(std.mem.readInt(u32, payload[pos..][0..4], .little));
        pos += 4;
        const quantized_offset = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        const quantized_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        const exact_entries_offset = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        const exact_entries_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;

        const centroid = try alloc.alloc(f32, dim_count);
        errdefer alloc.free(centroid);
        for (0..dim_count) |dim_idx| {
            const bits = std.mem.readInt(u32, payload[pos..][0..4], .little);
            pos += 4;
            centroid[dim_idx] = @bitCast(bits);
        }

        clusters[idx] = .{
            .centroid = centroid,
            .start_index = start_index,
            .entry_count = entry_count,
            .routing_distance_min = routing_distance_min,
            .routing_distance_max = routing_distance_max,
            .routing_distance_avg = routing_distance_avg,
            .quantized_offset = quantized_offset,
            .quantized_len = quantized_len,
            .exact_entries_offset = exact_entries_offset,
            .exact_entries_len = exact_entries_len,
            .quantized_set = try alloc.alloc(u8, 0),
            .exact_entries = try alloc.alloc(u8, 0),
        };
        initialized += 1;
    }
    return clusters;
}

test "vector segment codec round-trips entries" {
    const alloc = std.testing.allocator;
    var entry_a = vector_types.Entry{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .vector = try alloc.dupe(f32, &.{ 1.0, 2.0, 3.0 }),
    };
    defer entry_a.deinit(alloc);
    var entry_b = vector_types.Entry{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .vector = try alloc.dupe(f32, &.{ 0.5, 0.25, 0.125 }),
    };
    defer entry_b.deinit(alloc);
    const exact_block = try encodeExactEntriesAlloc(alloc, &.{ entry_a, entry_b });
    defer alloc.free(exact_block);

    var segment = vector_types.Segment{
        .dims = 3,
        .metric = .cosine,
        .base_probe_count = 3,
        .shortlist_multiplier = 4,
        .clusters = try alloc.alloc(vector_types.Cluster, 1),
        .entries = try alloc.alloc(vector_types.Entry, 2),
    };
    defer vector_types.freeSegment(alloc, &segment);
    segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 0.75, 1.125, 1.5625 }),
        .start_index = 0,
        .entry_count = 2,
        .routing_distance_min = 0.1,
        .routing_distance_max = 0.8,
        .routing_distance_avg = 0.45,
        .quantized_set = try alloc.dupe(u8, "cluster-0"),
        .exact_entries = try alloc.dupe(u8, exact_block),
    };
    segment.entries[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .vector = try alloc.dupe(f32, &.{ 1.0, 2.0, 3.0 }),
    };
    segment.entries[1] = .{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .vector = try alloc.dupe(f32, &.{ 0.5, 0.25, 0.125 }),
    };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer vector_types.freeSegment(alloc, &decoded);
    try std.testing.expectEqual(@as(u32, 3), decoded.dims);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, decoded.metric);
    try std.testing.expectEqual(@as(u32, 3), decoded.base_probe_count);
    try std.testing.expectEqual(@as(u32, 4), decoded.shortlist_multiplier);
    try std.testing.expectEqual(@as(usize, 1), decoded.clusters.len);
    try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
    try std.testing.expectEqual(@as(u32, 2), decoded.clusters[0].entry_count);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), decoded.clusters[0].routing_distance_min, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), decoded.clusters[0].routing_distance_max, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.45), decoded.clusters[0].routing_distance_avg, 0.0001);
    try std.testing.expectEqualStrings("cluster-0", decoded.clusters[0].quantized_set);
    try std.testing.expectEqualStrings("doc-b", decoded.entries[1].doc_id);
    try std.testing.expectEqual(@as(f32, 0.125), decoded.entries[1].vector[2]);
}

pub fn encodeExactEntriesAlloc(alloc: Allocator, entries: []const vector_types.Entry) ![]u8 {
    var total_len: usize = 0;
    for (entries) |entry| {
        total_len += 4 + entry.doc_id.len;
        total_len += entry.vector.len * @sizeOf(u16);
    }
    const buf = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    for (entries) |entry| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(entry.doc_id.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..entry.doc_id.len], entry.doc_id);
        pos += entry.doc_id.len;
        for (entry.vector) |value| {
            const bits: u16 = @bitCast(@as(f16, @floatCast(value)));
            std.mem.writeInt(u16, buf[pos..][0..2], bits, .little);
            pos += 2;
        }
    }
    return buf;
}

pub fn decodeExactEntriesAlloc(
    alloc: Allocator,
    dims: u32,
    expected_count: usize,
    payload: []const u8,
) ![]vector_types.Entry {
    const entries = try alloc.alloc(vector_types.Entry, expected_count);
    errdefer alloc.free(entries);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }

    var pos: usize = 0;
    const dim_count: usize = @intCast(dims);
    for (0..expected_count) |idx| {
        if (pos + 4 > payload.len) return error.InvalidVectorSegmentPayload;
        const doc_id_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        const vector_bytes = std.math.mul(usize, dim_count, 2) catch return error.InvalidVectorSegmentPayload;
        const entry_bytes = std.math.add(usize, doc_id_len, vector_bytes) catch return error.InvalidVectorSegmentPayload;
        if (pos > payload.len or entry_bytes > payload.len - pos) return error.InvalidVectorSegmentPayload;
        const doc_id = try alloc.dupe(u8, payload[pos .. pos + doc_id_len]);
        pos += doc_id_len;
        errdefer alloc.free(doc_id);

        const vector = try alloc.alloc(f32, dim_count);
        errdefer alloc.free(vector);
        for (0..dim_count) |dim_idx| {
            const bits = std.mem.readInt(u16, payload[pos..][0..2], .little);
            pos += 2;
            vector[dim_idx] = @floatCast(@as(f16, @bitCast(bits)));
        }
        entries[idx] = .{
            .doc_id = doc_id,
            .vector = vector,
        };
        initialized += 1;
    }
    if (pos != payload.len) return error.InvalidVectorSegmentPayload;
    return entries;
}

test "exact entry block codec round-trips entries" {
    const alloc = std.testing.allocator;
    const entries = try alloc.dupe(vector_types.Entry, &.{
        .{ .doc_id = try alloc.dupe(u8, "doc-a"), .vector = try alloc.dupe(f32, &.{ 1.0, 2.0 }) },
        .{ .doc_id = try alloc.dupe(u8, "doc-b"), .vector = try alloc.dupe(f32, &.{ 3.0, 4.0 }) },
    });
    defer {
        for (entries) |entry| {
            var owned = entry;
            owned.deinit(alloc);
        }
        alloc.free(entries);
    }

    const encoded = try encodeExactEntriesAlloc(alloc, entries);
    defer alloc.free(encoded);
    const decoded = try decodeExactEntriesAlloc(alloc, 2, 2, encoded);
    defer {
        for (decoded) |*entry| entry.deinit(alloc);
        alloc.free(decoded);
    }
    try std.testing.expectEqualStrings("doc-b", decoded[1].doc_id);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), decoded[1].vector[1], 0.001);
}

test "exact entry block uses compressed payload smaller than f32 vectors" {
    const alloc = std.testing.allocator;
    const entries = try alloc.dupe(vector_types.Entry, &.{
        .{ .doc_id = try alloc.dupe(u8, "doc-a"), .vector = try alloc.dupe(f32, &.{ 1.25, 2.5, 3.75 }) },
    });
    defer {
        for (entries) |entry| {
            var owned = entry;
            owned.deinit(alloc);
        }
        alloc.free(entries);
    }

    const encoded = try encodeExactEntriesAlloc(alloc, entries);
    defer alloc.free(encoded);
    try std.testing.expectEqual(@as(usize, 4 + "doc-a".len + (3 * 2)), encoded.len);
}
