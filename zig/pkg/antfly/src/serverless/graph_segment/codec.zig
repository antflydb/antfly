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
const graph_types = @import("types.zig");
const bounded_decode = @import("../bounded_decode.zig");

pub const DecodeLimits = bounded_decode.Limits;

pub const wire_magic = "AFSG";
pub const wire_version: u16 = 1;

const header_len = 4 + 2 + 4;

fn edgeEncodedSize(edge: graph_types.Edge) !usize {
    _ = std.math.cast(u32, edge.neighbor_id.len) orelse return error.GraphSegmentTooLarge;
    _ = std.math.cast(u32, edge.edge_type.len) orelse return error.GraphSegmentTooLarge;
    var size: usize = 12;
    size = std.math.add(usize, size, edge.neighbor_id.len) catch return error.GraphSegmentTooLarge;
    return std.math.add(usize, size, edge.edge_type.len) catch error.GraphSegmentTooLarge;
}

pub fn encodeAlloc(alloc: Allocator, segment: graph_types.Segment) ![]u8 {
    const size = try encodedSize(segment);

    const buf = try alloc.alloc(u8, size);
    errdefer alloc.free(buf);

    var pos: usize = 0;
    @memcpy(buf[pos..][0..4], wire_magic);
    pos += 4;
    std.mem.writeInt(u16, buf[pos..][0..2], wire_version, .little);
    pos += 2;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.adjacencies.len), .little);
    pos += 4;

    for (segment.adjacencies) |adjacency| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(adjacency.node_id.len), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(adjacency.out_edges.len), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(adjacency.in_edges.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..adjacency.node_id.len], adjacency.node_id);
        pos += adjacency.node_id.len;
        for (adjacency.out_edges) |edge| pos += encodeEdge(buf[pos..], edge);
        for (adjacency.in_edges) |edge| pos += encodeEdge(buf[pos..], edge);
    }

    std.debug.assert(pos == buf.len);
    return buf;
}

pub fn encodedSize(segment: graph_types.Segment) !usize {
    _ = std.math.cast(u32, segment.adjacencies.len) orelse return error.GraphSegmentTooLarge;
    var size: usize = header_len;
    for (segment.adjacencies) |adjacency| {
        _ = std.math.cast(u32, adjacency.node_id.len) orelse return error.GraphSegmentTooLarge;
        _ = std.math.cast(u32, adjacency.out_edges.len) orelse return error.GraphSegmentTooLarge;
        _ = std.math.cast(u32, adjacency.in_edges.len) orelse return error.GraphSegmentTooLarge;
        size = std.math.add(usize, size, 12) catch return error.GraphSegmentTooLarge;
        size = std.math.add(usize, size, adjacency.node_id.len) catch return error.GraphSegmentTooLarge;
        for (adjacency.out_edges) |edge| size = std.math.add(usize, size, try edgeEncodedSize(edge)) catch return error.GraphSegmentTooLarge;
        for (adjacency.in_edges) |edge| size = std.math.add(usize, size, try edgeEncodedSize(edge)) catch return error.GraphSegmentTooLarge;
    }
    return size;
}

test "lake graph segment codec rejects forged adjacency counts before allocation" {
    var payload = [_]u8{0} ** header_len;
    @memcpy(payload[0..4], wire_magic);
    std.mem.writeInt(u16, payload[4..6], wire_version, .little);
    std.mem.writeInt(u32, payload[6..10], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(std.testing.allocator, &payload));
}

pub fn decodeAlloc(alloc: Allocator, data: []const u8) !graph_types.Segment {
    return try decodeAllocWithLimits(alloc, data, .{});
}

pub fn decodeAllocWithLimits(alloc: Allocator, data: []const u8, limits: DecodeLimits) !graph_types.Segment {
    var budget = try bounded_decode.Budget.init(data.len, limits);
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, limits.max_allocation_bytes);
    return decodeBoundedAlloc(limiter.allocator(), data, &budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.DecodedArtifactTooLarge;
        return err;
    };
}

fn decodeBoundedAlloc(alloc: Allocator, data: []const u8, budget: *bounded_decode.Budget) !graph_types.Segment {
    if (data.len < header_len) return error.InvalidGraphSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, data[pos..][0..4], wire_magic)) return error.InvalidGraphSegment;
    pos += 4;
    const version = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    if (version != wire_version) return error.UnsupportedGraphSegmentVersion;
    const adjacency_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (@as(usize, adjacency_count) > (data.len - pos) / 12) return error.InvalidGraphSegment;
    _ = try budget.admitCount(graph_types.Adjacency, adjacency_count, data.len - pos, 12);

    const adjacencies = try alloc.alloc(graph_types.Adjacency, adjacency_count);
    errdefer alloc.free(adjacencies);
    var initialized: usize = 0;
    errdefer {
        for (adjacencies[0..initialized]) |*adjacency| adjacency.deinit(alloc);
    }

    for (0..adjacency_count) |idx| {
        if (pos + 12 > data.len) return error.InvalidGraphSegment;
        const node_id_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        const out_count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        const in_count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        if (node_id_len > data.len - pos) return error.InvalidGraphSegment;
        try budget.admitBytes(node_id_len);
        const node_id = try alloc.dupe(u8, data[pos .. pos + node_id_len]);
        pos += node_id_len;
        errdefer alloc.free(node_id);

        const out_edges = try decodeEdgesAlloc(alloc, data, &pos, out_count, budget);
        errdefer {
            for (out_edges) |*edge| edge.deinit(alloc);
            alloc.free(out_edges);
        }
        const in_edges = try decodeEdgesAlloc(alloc, data, &pos, in_count, budget);
        errdefer {
            for (in_edges) |*edge| edge.deinit(alloc);
            alloc.free(in_edges);
        }

        adjacencies[idx] = .{
            .node_id = node_id,
            .out_edges = out_edges,
            .in_edges = in_edges,
        };
        initialized += 1;
    }

    if (pos != data.len) return error.InvalidGraphSegment;
    return .{ .adjacencies = adjacencies };
}

fn encodeEdge(buf: []u8, edge: graph_types.Edge) usize {
    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(edge.neighbor_id.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(edge.edge_type.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(edge.weight), .little);
    pos += 4;
    @memcpy(buf[pos..][0..edge.neighbor_id.len], edge.neighbor_id);
    pos += edge.neighbor_id.len;
    @memcpy(buf[pos..][0..edge.edge_type.len], edge.edge_type);
    pos += edge.edge_type.len;
    return pos;
}

fn decodeEdgesAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
    edge_count: u32,
    budget: *bounded_decode.Budget,
) ![]graph_types.Edge {
    if (pos.* > data.len or @as(usize, edge_count) > (data.len - pos.*) / 12) return error.InvalidGraphSegment;
    _ = try budget.admitCount(graph_types.Edge, edge_count, data.len - pos.*, 12);
    const edges = try alloc.alloc(graph_types.Edge, edge_count);
    errdefer alloc.free(edges);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |*edge| edge.deinit(alloc);
    }

    for (0..edge_count) |idx| {
        if (pos.* + 12 > data.len) return error.InvalidGraphSegment;
        const neighbor_id_len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        const edge_type_len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        const weight_bits = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        const names_len = std.math.add(usize, neighbor_id_len, edge_type_len) catch return error.InvalidGraphSegment;
        if (pos.* > data.len or names_len > data.len - pos.*) return error.InvalidGraphSegment;
        try budget.admitBytes(names_len);
        const neighbor_id = try alloc.dupe(u8, data[pos.* .. pos.* + neighbor_id_len]);
        pos.* += neighbor_id_len;
        errdefer alloc.free(neighbor_id);
        const edge_type = try alloc.dupe(u8, data[pos.* .. pos.* + edge_type_len]);
        pos.* += edge_type_len;
        edges[idx] = .{
            .neighbor_id = neighbor_id,
            .edge_type = edge_type,
            .weight = @bitCast(weight_bits),
        };
        initialized += 1;
    }
    return edges;
}

test "graph segment codec round-trips" {
    const alloc = std.testing.allocator;
    var segment = graph_types.Segment{
        .adjacencies = try alloc.alloc(graph_types.Adjacency, 2),
    };
    defer graph_types.freeSegment(alloc, &segment);

    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_types.Edge, 1),
        .in_edges = try alloc.alloc(graph_types.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[1] = .{
        .node_id = try alloc.dupe(u8, "doc-b"),
        .out_edges = try alloc.alloc(graph_types.Edge, 0),
        .in_edges = try alloc.alloc(graph_types.Edge, 1),
    };
    segment.adjacencies[1].in_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-a"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer graph_types.freeSegment(alloc, &decoded);

    try std.testing.expectEqual(@as(usize, 2), decoded.adjacencies.len);
    try std.testing.expectEqualStrings("doc-a", decoded.adjacencies[0].node_id);
    try std.testing.expectEqualStrings("doc-b", decoded.adjacencies[0].out_edges[0].neighbor_id);
    try std.testing.expectEqualStrings("cites", decoded.adjacencies[1].in_edges[0].edge_type);
}
