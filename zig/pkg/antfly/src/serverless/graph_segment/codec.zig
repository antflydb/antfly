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
const graph_edge_type = @import("../../graph/edge_type.zig");
const bounded_decode = @import("../bounded_decode.zig");

pub const DecodeLimits = bounded_decode.Limits;

pub const wire_magic = "AFSG";
pub const wire_version: u16 = 2;
const legacy_wire_version: u16 = 1;

const legacy_header_len = 4 + 2 + 4;
const header_len = 4 + 2 + 4 + 4;
const no_neighbor_table = std.math.maxInt(u32);

fn edgeEncodedSize(edge: graph_types.Edge, version: u16) !usize {
    _ = std.math.cast(u32, edge.neighbor_id.len) orelse return error.GraphSegmentTooLarge;
    graph_edge_type.validateStored(edge.edge_type) catch return error.InvalidGraphSegment;
    _ = std.math.cast(u32, edge.edge_type.len) orelse return error.GraphSegmentTooLarge;
    if (version == legacy_wire_version and edge.neighbor_table_id != null) return error.InvalidGraphSegment;
    var size: usize = if (version == wire_version) 16 else 12;
    size = std.math.add(usize, size, edge.neighbor_id.len) catch return error.GraphSegmentTooLarge;
    return std.math.add(usize, size, edge.edge_type.len) catch error.GraphSegmentTooLarge;
}

pub fn encodeAlloc(alloc: Allocator, segment: graph_types.Segment) ![]u8 {
    const version = encodingVersion(segment);
    const size = try encodedSize(segment);

    const buf = try alloc.alloc(u8, size);
    errdefer alloc.free(buf);

    var pos: usize = 0;
    @memcpy(buf[pos..][0..4], wire_magic);
    pos += 4;
    std.mem.writeInt(u16, buf[pos..][0..2], version, .little);
    pos += 2;
    if (version == wire_version) {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.neighbor_tables.len), .little);
        pos += 4;
    }
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.adjacencies.len), .little);
    pos += 4;

    if (version == wire_version) {
        for (segment.neighbor_tables) |table| {
            std.mem.writeInt(u32, buf[pos..][0..4], @intCast(table.len), .little);
            pos += 4;
            @memcpy(buf[pos..][0..table.len], table);
            pos += table.len;
        }
    }

    for (segment.adjacencies) |adjacency| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(adjacency.node_id.len), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(adjacency.out_edges.len), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(adjacency.in_edges.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..adjacency.node_id.len], adjacency.node_id);
        pos += adjacency.node_id.len;
        for (adjacency.out_edges) |edge| pos += encodeEdge(buf[pos..], edge, version);
        for (adjacency.in_edges) |edge| pos += encodeEdge(buf[pos..], edge, version);
    }

    std.debug.assert(pos == buf.len);
    return buf;
}

pub fn encodedSize(segment: graph_types.Segment) !usize {
    const version = encodingVersion(segment);
    _ = std.math.cast(u32, segment.neighbor_tables.len) orelse return error.GraphSegmentTooLarge;
    _ = std.math.cast(u32, segment.adjacencies.len) orelse return error.GraphSegmentTooLarge;
    var size: usize = if (version == wire_version) header_len else legacy_header_len;
    if (version == wire_version) {
        for (segment.neighbor_tables) |table| {
            _ = std.math.cast(u32, table.len) orelse return error.GraphSegmentTooLarge;
            size = std.math.add(usize, size, 4) catch return error.GraphSegmentTooLarge;
            size = std.math.add(usize, size, table.len) catch return error.GraphSegmentTooLarge;
        }
    }
    for (segment.adjacencies) |adjacency| {
        _ = std.math.cast(u32, adjacency.node_id.len) orelse return error.GraphSegmentTooLarge;
        _ = std.math.cast(u32, adjacency.out_edges.len) orelse return error.GraphSegmentTooLarge;
        _ = std.math.cast(u32, adjacency.in_edges.len) orelse return error.GraphSegmentTooLarge;
        size = std.math.add(usize, size, 12) catch return error.GraphSegmentTooLarge;
        size = std.math.add(usize, size, adjacency.node_id.len) catch return error.GraphSegmentTooLarge;
        for (adjacency.out_edges) |edge| {
            if (edge.neighbor_table_id) |id| if (id >= segment.neighbor_tables.len) return error.InvalidGraphSegment;
            size = std.math.add(usize, size, try edgeEncodedSize(edge, version)) catch return error.GraphSegmentTooLarge;
        }
        for (adjacency.in_edges) |edge| {
            if (edge.neighbor_table_id) |id| if (id >= segment.neighbor_tables.len) return error.InvalidGraphSegment;
            size = std.math.add(usize, size, try edgeEncodedSize(edge, version)) catch return error.GraphSegmentTooLarge;
        }
    }
    return size;
}

fn encodingVersion(segment: graph_types.Segment) u16 {
    return if (segment.neighbor_tables.len == 0) legacy_wire_version else wire_version;
}

test "lake graph segment codec rejects forged adjacency counts before allocation" {
    var payload = [_]u8{0} ** legacy_header_len;
    @memcpy(payload[0..4], wire_magic);
    std.mem.writeInt(u16, payload[4..6], legacy_wire_version, .little);
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
    if (data.len < legacy_header_len) return error.InvalidGraphSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, data[pos..][0..4], wire_magic)) return error.InvalidGraphSegment;
    pos += 4;
    const version = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    if (version != legacy_wire_version and version != wire_version) return error.UnsupportedGraphSegmentVersion;
    const neighbor_table_count: u32 = if (version == wire_version) blk: {
        if (pos + 4 > data.len) return error.InvalidGraphSegment;
        const count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk count;
    } else 0;
    if (pos + 4 > data.len) return error.InvalidGraphSegment;
    const adjacency_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;

    const neighbor_tables = try decodeNeighborTablesAlloc(alloc, data, &pos, neighbor_table_count, budget);
    errdefer {
        for (neighbor_tables) |table| alloc.free(table);
        if (neighbor_tables.len > 0) alloc.free(neighbor_tables);
    }
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

        const out_edges = try decodeEdgesAlloc(alloc, data, &pos, out_count, version, neighbor_tables.len, budget);
        errdefer {
            for (out_edges) |*edge| edge.deinit(alloc);
            alloc.free(out_edges);
        }
        const in_edges = try decodeEdgesAlloc(alloc, data, &pos, in_count, version, neighbor_tables.len, budget);
        errdefer {
            for (in_edges) |*edge| edge.deinit(alloc);
            alloc.free(in_edges);
        }
        // Exact public probes use binary lookup. Reject non-canonical artifacts
        // at the trust boundary instead of risking an exact-looking false miss.
        if (!graph_types.edgesHaveCanonicalLookupOrder(out_edges) or
            !graph_types.edgesHaveCanonicalLookupOrder(in_edges))
            return error.InvalidGraphSegment;

        adjacencies[idx] = .{
            .node_id = node_id,
            .out_edges = out_edges,
            .in_edges = in_edges,
        };
        initialized += 1;
    }

    if (pos != data.len) return error.InvalidGraphSegment;
    return .{ .neighbor_tables = neighbor_tables, .adjacencies = adjacencies };
}

fn decodeNeighborTablesAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
    table_count: u32,
    budget: *bounded_decode.Budget,
) ![][]u8 {
    if (pos.* > data.len or @as(usize, table_count) > (data.len - pos.*) / 4) return error.InvalidGraphSegment;
    _ = try budget.admitCount([]u8, table_count, data.len - pos.*, 4);
    const tables = try alloc.alloc([]u8, table_count);
    errdefer if (tables.len > 0) alloc.free(tables);
    var initialized: usize = 0;
    errdefer for (tables[0..initialized]) |table| alloc.free(table);
    for (0..table_count) |idx| {
        if (pos.* + 4 > data.len) return error.InvalidGraphSegment;
        const table_len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        if (table_len == 0 or table_len > data.len - pos.*) return error.InvalidGraphSegment;
        try budget.admitBytes(table_len);
        tables[idx] = try alloc.dupe(u8, data[pos.* .. pos.* + table_len]);
        pos.* += table_len;
        initialized += 1;
    }
    return tables;
}

fn encodeEdge(buf: []u8, edge: graph_types.Edge, version: u16) usize {
    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(edge.neighbor_id.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(edge.edge_type.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(edge.weight), .little);
    pos += 4;
    if (version == wire_version) {
        std.mem.writeInt(u32, buf[pos..][0..4], edge.neighbor_table_id orelse no_neighbor_table, .little);
        pos += 4;
    }
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
    version: u16,
    neighbor_table_count: usize,
    budget: *bounded_decode.Budget,
) ![]graph_types.Edge {
    const fixed_edge_len: usize = if (version == wire_version) 16 else 12;
    if (pos.* > data.len or @as(usize, edge_count) > (data.len - pos.*) / fixed_edge_len) return error.InvalidGraphSegment;
    _ = try budget.admitCount(graph_types.Edge, edge_count, data.len - pos.*, fixed_edge_len);
    const edges = try alloc.alloc(graph_types.Edge, edge_count);
    errdefer alloc.free(edges);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |*edge| edge.deinit(alloc);
    }

    for (0..edge_count) |idx| {
        if (pos.* + fixed_edge_len > data.len) return error.InvalidGraphSegment;
        const neighbor_id_len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        const edge_type_len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        if (edge_type_len == 0 or edge_type_len > graph_edge_type.max_bytes)
            return error.InvalidGraphSegment;
        const weight_bits = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        const neighbor_table_id: ?u32 = if (version == wire_version) blk: {
            const raw_id = std.mem.readInt(u32, data[pos.*..][0..4], .little);
            pos.* += 4;
            if (raw_id == no_neighbor_table) break :blk null;
            if (raw_id >= neighbor_table_count) return error.InvalidGraphSegment;
            break :blk raw_id;
        } else null;
        const names_len = std.math.add(usize, neighbor_id_len, edge_type_len) catch return error.InvalidGraphSegment;
        if (pos.* > data.len or names_len > data.len - pos.*) return error.InvalidGraphSegment;
        try budget.admitBytes(names_len);
        const neighbor_id = try alloc.dupe(u8, data[pos.* .. pos.* + neighbor_id_len]);
        pos.* += neighbor_id_len;
        errdefer alloc.free(neighbor_id);
        const edge_type_bytes = data[pos.* .. pos.* + edge_type_len];
        if (!graph_edge_type.isValid(edge_type_bytes)) return error.InvalidGraphSegment;
        const edge_type = try alloc.dupe(u8, edge_type_bytes);
        pos.* += edge_type_len;
        edges[idx] = .{
            .neighbor_id = neighbor_id,
            .edge_type = edge_type,
            .weight = @bitCast(weight_bits),
            .neighbor_table_id = neighbor_table_id,
        };
        initialized += 1;
    }
    return edges;
}

test "serverless graph segment codec round-trips" {
    const alloc = std.testing.allocator;
    var segment = graph_types.Segment{
        .neighbor_tables = try alloc.alloc([]u8, 1),
        .adjacencies = try alloc.alloc(graph_types.Adjacency, 2),
    };
    defer graph_types.freeSegment(alloc, &segment);
    segment.neighbor_tables[0] = try alloc.dupe(u8, "entities");

    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_types.Edge, 1),
        .in_edges = try alloc.alloc(graph_types.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
        .neighbor_table_id = 0,
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
    try std.testing.expectEqual(wire_version, std.mem.readInt(u16, encoded[4..6], .little));
    var decoded = try decodeAlloc(alloc, encoded);
    defer graph_types.freeSegment(alloc, &decoded);

    try std.testing.expectEqual(@as(usize, 2), decoded.adjacencies.len);
    try std.testing.expectEqualStrings("doc-a", decoded.adjacencies[0].node_id);
    try std.testing.expectEqualStrings("doc-b", decoded.adjacencies[0].out_edges[0].neighbor_id);
    try std.testing.expectEqualStrings("cites", decoded.adjacencies[1].in_edges[0].edge_type);
    try std.testing.expectEqualStrings("entities", decoded.neighborTable(decoded.adjacencies[0].out_edges[0]).?);
}

test "serverless graph segment codec rejects invalid edge types" {
    const alloc = std.testing.allocator;
    var segment = graph_types.Segment{
        .adjacencies = try alloc.alloc(graph_types.Adjacency, 1),
    };
    defer graph_types.freeSegment(alloc, &segment);
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_types.Edge, 1),
        .in_edges = try alloc.alloc(graph_types.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, ""),
        .weight = 1,
    };

    try std.testing.expectError(error.InvalidGraphSegment, encodeAlloc(alloc, segment));
    alloc.free(segment.adjacencies[0].out_edges[0].edge_type);
    segment.adjacencies[0].out_edges[0].edge_type = try alloc.dupe(u8, "x" ** (graph_edge_type.max_bytes + 1));
    try std.testing.expectError(error.InvalidGraphSegment, encodeAlloc(alloc, segment));
    alloc.free(segment.adjacencies[0].out_edges[0].edge_type);
    segment.adjacencies[0].out_edges[0].edge_type = try alloc.dupe(u8, "\xff");
    try std.testing.expectError(error.InvalidGraphSegment, encodeAlloc(alloc, segment));

    alloc.free(segment.adjacencies[0].out_edges[0].edge_type);
    segment.adjacencies[0].out_edges[0].edge_type = try alloc.dupe(u8, "x");
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    const edge_type_len_offset = legacy_header_len + 12 + segment.adjacencies[0].node_id.len + 4;
    std.mem.writeInt(u32, encoded[edge_type_len_offset..][0..4], 0, .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(alloc, encoded));
    std.mem.writeInt(u32, encoded[edge_type_len_offset..][0..4], graph_edge_type.max_bytes + 1, .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(alloc, encoded));
    std.mem.writeInt(u32, encoded[edge_type_len_offset..][0..4], 1, .little);
    encoded[encoded.len - 1] = 0xff;
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(alloc, encoded));
}

test "serverless graph segment codec rejects non-canonical edge ordering" {
    const alloc = std.testing.allocator;
    var segment = graph_types.Segment{
        .adjacencies = try alloc.alloc(graph_types.Adjacency, 1),
    };
    defer graph_types.freeSegment(alloc, &segment);
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_types.Edge, 2),
        .in_edges = try alloc.alloc(graph_types.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "z"),
        .edge_type = try alloc.dupe(u8, "mentions"),
        .weight = 1,
    };
    segment.adjacencies[0].out_edges[1] = .{
        .neighbor_id = try alloc.dupe(u8, "a"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1,
    };
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(alloc, encoded));
}

test "serverless graph segment codec keeps local artifacts on v1" {
    const alloc = std.testing.allocator;
    var segment = graph_types.Segment{
        .adjacencies = try alloc.alloc(graph_types.Adjacency, 1),
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
        .weight = 1,
    };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    try std.testing.expectEqual(legacy_wire_version, std.mem.readInt(u16, encoded[4..6], .little));

    var decoded = try decodeAlloc(alloc, encoded);
    defer graph_types.freeSegment(alloc, &decoded);
    try std.testing.expectEqualStrings("doc-b", decoded.adjacencies[0].out_edges[0].neighbor_id);
}

test "serverless graph segment codec decodes legacy v1 artifacts" {
    const alloc = std.testing.allocator;
    var payload: [37]u8 = undefined;
    var pos: usize = 0;
    @memcpy(payload[pos..][0..4], wire_magic);
    pos += 4;
    std.mem.writeInt(u16, payload[pos..][0..2], legacy_wire_version, .little);
    pos += 2;
    std.mem.writeInt(u32, payload[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u32, payload[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u32, payload[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u32, payload[pos..][0..4], 0, .little);
    pos += 4;
    payload[pos] = 'a';
    pos += 1;
    std.mem.writeInt(u32, payload[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u32, payload[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u32, payload[pos..][0..4], @bitCast(@as(f32, 2.0)), .little);
    pos += 4;
    payload[pos] = 'b';
    pos += 1;
    payload[pos] = 'e';
    pos += 1;
    try std.testing.expectEqual(payload.len, pos);

    var decoded = try decodeAlloc(alloc, &payload);
    defer graph_types.freeSegment(alloc, &decoded);
    try std.testing.expectEqual(@as(usize, 0), decoded.neighbor_tables.len);
    try std.testing.expectEqualStrings("b", decoded.adjacencies[0].out_edges[0].neighbor_id);
    try std.testing.expect(decoded.adjacencies[0].out_edges[0].neighbor_table_id == null);
}
