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

pub const wire_magic = "AFSG";
pub const wire_version: u16 = 1;

const header_len = 4 + 2 + 4;

fn edgeEncodedSize(edge: graph_types.Edge) usize {
    return 4 + 4 + 4 + edge.neighbor_id.len + edge.edge_type.len;
}

pub fn encodeAlloc(alloc: Allocator, segment: graph_types.Segment) ![]u8 {
    var size: usize = header_len;
    for (segment.adjacencies) |adjacency| {
        size += 4 + 4 + 4 + adjacency.node_id.len;
        for (adjacency.out_edges) |edge| size += edgeEncodedSize(edge);
        for (adjacency.in_edges) |edge| size += edgeEncodedSize(edge);
    }

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

pub fn decodeAlloc(alloc: Allocator, data: []const u8) !graph_types.Segment {
    if (data.len < header_len) return error.InvalidGraphSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, data[pos..][0..4], wire_magic)) return error.InvalidGraphSegment;
    pos += 4;
    const version = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    if (version != wire_version) return error.UnsupportedGraphSegmentVersion;
    const adjacency_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;

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
        if (pos + node_id_len > data.len) return error.InvalidGraphSegment;
        const node_id = try alloc.dupe(u8, data[pos .. pos + node_id_len]);
        pos += node_id_len;
        errdefer alloc.free(node_id);

        const out_edges = try decodeEdgesAlloc(alloc, data, &pos, out_count);
        errdefer {
            for (out_edges) |*edge| edge.deinit(alloc);
            alloc.free(out_edges);
        }
        const in_edges = try decodeEdgesAlloc(alloc, data, &pos, in_count);
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

fn decodeEdgesAlloc(alloc: Allocator, data: []const u8, pos: *usize, edge_count: u32) ![]graph_types.Edge {
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
        if (pos.* + neighbor_id_len + edge_type_len > data.len) return error.InvalidGraphSegment;
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
