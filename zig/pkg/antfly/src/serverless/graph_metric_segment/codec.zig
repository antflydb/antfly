// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const bounded_decode = @import("../bounded_decode.zig");
const types = @import("types.zig");

pub const wire_magic = "AFGM";
pub const wire_version: u16 = 3;
const fixed_header_len_v1 = 4 + 2 + 1 + 1 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4;
const fixed_header_len_v2 = fixed_header_len_v1 + 2;
const fixed_header_len = fixed_header_len_v2 + 8;

pub const Header = struct {
    kind: @import("../../graph/graph.zig").GraphMetricKind,
    materialization_state: types.MaterializationState,
    rejection_reason: types.RejectionReason,
    config_fingerprint: u64,
    materializer_fingerprint: u64,
    graph_index_name: []const u8,
    metric_name: []const u8,
    source_graph_artifact_id: []const u8,
    source_graph_checksum: []const u8,
};

/// Decodes only provenance and lifecycle metadata from a bounded prefix. Score
/// vectors and edge filters are deliberately not materialized.
pub fn decodeHeader(data: []const u8) !Header {
    if (data.len < fixed_header_len_v1) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(data, &pos, 4), wire_magic)) return error.InvalidGraphMetricSegment;
    const version = try readInt(u16, data, &pos);
    if (version == 0 or version > wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    if (pos >= data.len) return error.InvalidGraphMetricSegment;
    const kind = std.enums.fromInt(@import("../../graph/graph.zig").GraphMetricKind, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const materialization_state: types.MaterializationState = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.MaterializationState, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .ready;
    const rejection_reason: types.RejectionReason = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.RejectionReason, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .none;
    const converged_byte = (try take(data, &pos, 1))[0];
    if (converged_byte > 1) return error.InvalidGraphMetricSegment;
    const iterations = try readInt(u32, data, &pos);
    const fingerprint = try readInt(u64, data, &pos);
    const materializer_fingerprint = if (version >= 3) try readInt(u64, data, &pos) else 0;
    const delta: f64 = @bitCast(try readInt(u64, data, &pos));
    if (!std.math.isFinite(delta)) return error.InvalidGraphMetricSegment;
    switch (materialization_state) {
        .ready => if (rejection_reason != .none) return error.InvalidGraphMetricSegment,
        .rejected => if (rejection_reason == .none or converged_byte != 0 or iterations != 0 or delta != 0) return error.InvalidGraphMetricSegment,
    }
    const graph_len = try readInt(u32, data, &pos);
    const metric_len = try readInt(u32, data, &pos);
    const artifact_len = try readInt(u32, data, &pos);
    const checksum_len = try readInt(u32, data, &pos);
    _ = try readInt(u32, data, &pos); // edge type count
    const graph_index_name = try take(data, &pos, graph_len);
    const metric_name = try take(data, &pos, metric_len);
    const source_graph_artifact_id = try take(data, &pos, artifact_len);
    const source_graph_checksum = try take(data, &pos, checksum_len);
    return .{
        .kind = kind,
        .materialization_state = materialization_state,
        .rejection_reason = rejection_reason,
        .config_fingerprint = fingerprint,
        .materializer_fingerprint = materializer_fingerprint,
        .graph_index_name = graph_index_name,
        .metric_name = metric_name,
        .source_graph_artifact_id = source_graph_artifact_id,
        .source_graph_checksum = source_graph_checksum,
    };
}

pub fn encodedSize(segment: types.Segment) !usize {
    try validateSegment(segment);
    var size: usize = fixed_header_len;
    size = std.math.add(usize, size, segment.graph_index_name.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.metric_name.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.source_graph_artifact_id.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.source_graph_checksum.len) catch return error.GraphMetricSegmentTooLarge;
    for (segment.edge_filter.types) |edge_type| {
        size = std.math.add(usize, size, 4 + edge_type.len) catch return error.GraphMetricSegmentTooLarge;
    }
    for (segment.scores) |score| {
        size = std.math.add(usize, size, 4 + 8) catch return error.GraphMetricSegmentTooLarge;
        size = std.math.add(usize, size, score.node_id.len) catch return error.GraphMetricSegmentTooLarge;
    }
    return size;
}

pub fn encodeAlloc(alloc: Allocator, segment: types.Segment) ![]u8 {
    const size = try encodedSize(segment);
    const data = try alloc.alloc(u8, size);
    errdefer alloc.free(data);
    var pos: usize = 0;
    putBytes(data, &pos, wire_magic);
    putInt(u16, data, &pos, wire_version);
    data[pos] = @intFromEnum(segment.kind);
    pos += 1;
    data[pos] = @intFromEnum(segment.materialization_state);
    pos += 1;
    data[pos] = @intFromEnum(segment.rejection_reason);
    pos += 1;
    data[pos] = @intFromBool(segment.converged);
    pos += 1;
    putInt(u32, data, &pos, segment.iterations_completed);
    putInt(u64, data, &pos, segment.config_fingerprint);
    putInt(u64, data, &pos, segment.materializer_fingerprint);
    putInt(u64, data, &pos, @bitCast(segment.delta));
    putInt(u32, data, &pos, @intCast(segment.graph_index_name.len));
    putInt(u32, data, &pos, @intCast(segment.metric_name.len));
    putInt(u32, data, &pos, @intCast(segment.source_graph_artifact_id.len));
    putInt(u32, data, &pos, @intCast(segment.source_graph_checksum.len));
    putInt(u32, data, &pos, @intCast(segment.edge_filter.types.len));
    putBytes(data, &pos, segment.graph_index_name);
    putBytes(data, &pos, segment.metric_name);
    putBytes(data, &pos, segment.source_graph_artifact_id);
    putBytes(data, &pos, segment.source_graph_checksum);
    for (segment.edge_filter.types) |edge_type| {
        putInt(u32, data, &pos, @intCast(edge_type.len));
        putBytes(data, &pos, edge_type);
    }
    putInt(u32, data, &pos, @intCast(segment.scores.len));
    for (segment.scores) |score| {
        putInt(u32, data, &pos, @intCast(score.node_id.len));
        putInt(u64, data, &pos, @bitCast(score.value));
        putBytes(data, &pos, score.node_id);
    }
    std.debug.assert(pos == data.len);
    return data;
}

pub fn decodeAlloc(alloc: Allocator, data: []const u8) !types.Segment {
    return decodeAllocWithLimits(alloc, data, .{});
}

pub fn decodeAllocWithLimits(alloc: Allocator, data: []const u8, limits: bounded_decode.Limits) !types.Segment {
    var budget = try bounded_decode.Budget.init(data.len, limits);
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, limits.max_allocation_bytes);
    return decodeBoundedAlloc(limiter.allocator(), data, &budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.DecodedArtifactTooLarge;
        return err;
    };
}

fn decodeBoundedAlloc(alloc: Allocator, data: []const u8, budget: *bounded_decode.Budget) !types.Segment {
    if (data.len < fixed_header_len_v1 + 4) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, take(data, &pos, 4) catch return error.InvalidGraphMetricSegment, wire_magic)) return error.InvalidGraphMetricSegment;
    const version = readInt(u16, data, &pos) catch return error.InvalidGraphMetricSegment;
    if (version == 0 or version > wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    const kind = std.enums.fromInt(@import("../../graph/graph.zig").GraphMetricKind, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const materialization_state: types.MaterializationState = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.MaterializationState, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .ready;
    const rejection_reason: types.RejectionReason = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.RejectionReason, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .none;
    const converged_byte = data[pos];
    pos += 1;
    if (converged_byte > 1) return error.InvalidGraphMetricSegment;
    const iterations = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const fingerprint = readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment;
    const materializer_fingerprint = if (version >= 3)
        readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment
    else
        0;
    const delta: f64 = @bitCast(readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment);
    if (!std.math.isFinite(delta)) return error.InvalidGraphMetricSegment;
    const graph_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const metric_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const artifact_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const checksum_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const edge_type_count = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const names_len = std.math.add(usize, graph_len, metric_len) catch return error.InvalidGraphMetricSegment;
    const identity_len = std.math.add(usize, artifact_len, checksum_len) catch return error.InvalidGraphMetricSegment;
    try budget.admitBytes(std.math.add(usize, names_len, identity_len) catch return error.InvalidGraphMetricSegment);
    const graph_name = try dupeTake(alloc, data, &pos, graph_len);
    errdefer alloc.free(graph_name);
    const metric_name = try dupeTake(alloc, data, &pos, metric_len);
    errdefer alloc.free(metric_name);
    const artifact_id = try dupeTake(alloc, data, &pos, artifact_len);
    errdefer alloc.free(artifact_id);
    const checksum = try dupeTake(alloc, data, &pos, checksum_len);
    errdefer alloc.free(checksum);
    _ = try budget.admitCount([]const u8, edge_type_count, data.len - pos, 4);
    const edge_types = try alloc.alloc([]const u8, edge_type_count);
    errdefer alloc.free(edge_types);
    var initialized_edge_types: usize = 0;
    errdefer for (edge_types[0..initialized_edge_types]) |edge_type| alloc.free(edge_type);
    for (edge_types) |*edge_type| {
        const edge_type_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
        try budget.admitBytes(edge_type_len);
        edge_type.* = try dupeTake(alloc, data, &pos, edge_type_len);
        initialized_edge_types += 1;
    }
    const score_count = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    _ = try budget.admitCount(types.Score, score_count, data.len - pos, 12);
    const scores = try alloc.alloc(types.Score, score_count);
    errdefer alloc.free(scores);
    var initialized: usize = 0;
    errdefer for (scores[0..initialized]) |*score| score.deinit(alloc);
    for (scores) |*score| {
        const node_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
        const value: f64 = @bitCast(readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment);
        if (!std.math.isFinite(value)) return error.InvalidGraphMetricSegment;
        try budget.admitBytes(node_len);
        score.* = .{ .node_id = try dupeTake(alloc, data, &pos, node_len), .value = value };
        initialized += 1;
    }
    if (pos != data.len) return error.InvalidGraphMetricSegment;
    var segment = types.Segment{ .graph_index_name = graph_name, .metric_name = metric_name, .kind = kind, .source_graph_artifact_id = artifact_id, .source_graph_checksum = checksum, .config_fingerprint = fingerprint, .materializer_fingerprint = materializer_fingerprint, .materialization_state = materialization_state, .rejection_reason = rejection_reason, .edge_filter = .{ .mode = if (edge_type_count == 0) .all else .types, .types = edge_types }, .converged = converged_byte == 1, .iterations_completed = iterations, .delta = delta, .scores = scores };
    errdefer segment.deinit(alloc);
    try validateSegment(segment);
    return segment;
}

fn validateSegment(segment: types.Segment) !void {
    if (segment.graph_index_name.len == 0 or segment.metric_name.len == 0 or segment.source_graph_artifact_id.len == 0 or segment.source_graph_checksum.len == 0) return error.InvalidGraphMetricSegment;
    _ = std.math.cast(u32, segment.graph_index_name.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.metric_name.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.source_graph_artifact_id.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.source_graph_checksum.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.scores.len) orelse return error.GraphMetricSegmentTooLarge;
    if (!std.math.isFinite(segment.delta)) return error.InvalidGraphMetricSegment;
    switch (segment.materialization_state) {
        .ready => if (segment.rejection_reason != .none) return error.InvalidGraphMetricSegment,
        .rejected => {
            if (segment.rejection_reason == .none or segment.scores.len != 0 or segment.converged or segment.iterations_completed != 0 or segment.delta != 0) return error.InvalidGraphMetricSegment;
        },
    }
    if ((segment.edge_filter.mode == .all) != (segment.edge_filter.types.len == 0)) return error.InvalidGraphMetricSegment;
    for (segment.edge_filter.types, 0..) |edge_type, i| {
        if (edge_type.len == 0) return error.InvalidGraphMetricSegment;
        _ = std.math.cast(u32, edge_type.len) orelse return error.GraphMetricSegmentTooLarge;
        if (i > 0 and std.mem.order(u8, segment.edge_filter.types[i - 1], edge_type) != .lt) return error.InvalidGraphMetricSegment;
    }
    for (segment.scores, 0..) |score, i| {
        if (score.node_id.len == 0 or !std.math.isFinite(score.value)) return error.InvalidGraphMetricSegment;
        _ = std.math.cast(u32, score.node_id.len) orelse return error.GraphMetricSegmentTooLarge;
        if (i > 0 and std.mem.order(u8, segment.scores[i - 1].node_id, score.node_id) != .lt) return error.InvalidGraphMetricSegment;
    }
}

fn putBytes(data: []u8, pos: *usize, value: []const u8) void {
    @memcpy(data[pos.*..][0..value.len], value);
    pos.* += value.len;
}
fn putInt(comptime T: type, data: []u8, pos: *usize, value: T) void {
    std.mem.writeInt(T, data[pos.*..][0..@sizeOf(T)], value, .little);
    pos.* += @sizeOf(T);
}
fn readInt(comptime T: type, data: []const u8, pos: *usize) !T {
    if (pos.* + @sizeOf(T) > data.len) return error.InvalidGraphMetricSegment;
    const value = std.mem.readInt(T, data[pos.*..][0..@sizeOf(T)], .little);
    pos.* += @sizeOf(T);
    return value;
}
fn take(data: []const u8, pos: *usize, len: usize) ![]const u8 {
    if (pos.* > data.len or len > data.len - pos.*) return error.InvalidGraphMetricSegment;
    const value = data[pos.*..][0..len];
    pos.* += len;
    return value;
}
fn dupeTake(alloc: Allocator, data: []const u8, pos: *usize, len: usize) ![]u8 {
    return try alloc.dupe(u8, try take(data, pos, len));
}

test "serverless graph metric segment round trips with binary-search lookup" {
    const alloc = std.testing.allocator;
    var scores = try alloc.alloc(types.Score, 2);
    scores[0] = .{ .node_id = try alloc.dupe(u8, "a"), .value = 0.25 };
    scores[1] = .{ .node_id = try alloc.dupe(u8, "b"), .value = 0.75 };
    var segment = types.Segment{ .graph_index_name = try alloc.dupe(u8, "graph"), .metric_name = try alloc.dupe(u8, "pagerank"), .kind = .pagerank, .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"), .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"), .config_fingerprint = 42, .edge_filter = .{}, .converged = true, .iterations_completed = 12, .delta = 0.00001, .scores = scores };
    defer segment.deinit(alloc);
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, 0.75), decoded.score("b"));
    try std.testing.expect(decoded.score("missing") == null);
}

test "serverless graph metric segment round trips terminal materialization rejection" {
    const alloc = std.testing.allocator;
    var segment = types.Segment{
        .graph_index_name = try alloc.dupe(u8, "graph"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .kind = .pagerank,
        .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"),
        .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"),
        .config_fingerprint = 42,
        .materialization_state = .rejected,
        .rejection_reason = .build_budget_exceeded,
        .edge_filter = .{},
        .converged = false,
        .iterations_completed = 0,
        .delta = 0,
        .scores = try alloc.alloc(types.Score, 0),
    };
    defer segment.deinit(alloc);
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(types.MaterializationState.rejected, decoded.materialization_state);
    try std.testing.expectEqual(types.RejectionReason.build_budget_exceeded, decoded.rejection_reason);
    try std.testing.expectEqual(@as(usize, 0), decoded.scores.len);
}

test "serverless graph metric segment decodes version one artifacts as ready" {
    const alloc = std.testing.allocator;
    var segment = types.Segment{
        .graph_index_name = try alloc.dupe(u8, "graph"),
        .metric_name = try alloc.dupe(u8, "degree"),
        .kind = .degree,
        .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"),
        .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"),
        .config_fingerprint = 9,
        .edge_filter = .{},
        .converged = true,
        .iterations_completed = 1,
        .delta = 0,
        .scores = try alloc.alloc(types.Score, 0),
    };
    defer segment.deinit(alloc);
    const version_three = try encodeAlloc(alloc, segment);
    defer alloc.free(version_three);
    const version_one = try alloc.alloc(u8, version_three.len - 10);
    defer alloc.free(version_one);
    @memcpy(version_one[0..7], version_three[0..7]);
    @memcpy(version_one[7..20], version_three[9..22]);
    @memcpy(version_one[20..], version_three[30..]);
    std.mem.writeInt(u16, version_one[4..6], 1, .little);

    const header = try decodeHeader(version_one);
    try std.testing.expectEqual(types.MaterializationState.ready, header.materialization_state);
    try std.testing.expectEqual(types.RejectionReason.none, header.rejection_reason);
    var decoded = try decodeAlloc(alloc, version_one);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(types.MaterializationState.ready, decoded.materialization_state);
    try std.testing.expectEqual(types.RejectionReason.none, decoded.rejection_reason);
    try std.testing.expectEqual(@as(u64, 0), decoded.materializer_fingerprint);

    const version_two = try alloc.alloc(u8, version_three.len - 8);
    defer alloc.free(version_two);
    @memcpy(version_two[0..22], version_three[0..22]);
    @memcpy(version_two[22..], version_three[30..]);
    std.mem.writeInt(u16, version_two[4..6], 2, .little);
    var decoded_v2 = try decodeAlloc(alloc, version_two);
    defer decoded_v2.deinit(alloc);
    try std.testing.expectEqual(types.MaterializationState.ready, decoded_v2.materialization_state);
    try std.testing.expectEqual(@as(u64, 0), decoded_v2.materializer_fingerprint);
}
