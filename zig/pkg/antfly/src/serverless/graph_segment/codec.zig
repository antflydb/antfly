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
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const graph_types = @import("types.zig");
const graph_edge_type = @import("../../graph/edge_type.zig");
pub const compact = @import("packed.zig");
pub const DecodeLimits = @import("../bounded_decode.zig").Limits;
pub const wire_magic = compact.wire_magic;
pub const wire_version = compact.wire_version;
const header_len = compact.header_len;
pub const encodeAlloc = compact.encodeAlloc;
pub const encodedSize = compact.encodedSize;
pub const decodedRetainedBytes = compact.decodedRetainedBytes;
pub const decodeAllocWithLimitsAndCancellation = compact.decodeAllocWithLimitsAndCancellation;
pub fn decodeAlloc(alloc: Allocator, data: []const u8) !graph_types.Segment {
    return decodeAllocWithLimitsAndCancellation(alloc, data, .{}, .none);
}
pub fn decodeAllocWithLimits(alloc: Allocator, data: []const u8, limits: DecodeLimits) !graph_types.Segment {
    return decodeAllocWithLimitsAndCancellation(alloc, data, limits, .none);
}
pub fn decodeAllocWithCancellation(alloc: Allocator, data: []const u8, cancellation: CancellationToken) !graph_types.Segment {
    return decodeAllocWithLimitsAndCancellation(alloc, data, .{}, cancellation);
}

test "lake graph segment codec rejects forged adjacency counts before allocation" {
    var payload = [_]u8{0} ** header_len;
    @memcpy(payload[0..4], wire_magic);
    std.mem.writeInt(u16, payload[4..6], wire_version, .little);
    std.mem.writeInt(u32, payload[6..10], 0, .little);
    std.mem.writeInt(u32, payload[18..22], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(std.testing.allocator, &payload));
}

test "serverless packed graph decode observes cancellation inside dictionary scans" {
    const alloc = std.testing.allocator;
    var segment = graph_types.Segment{ .adjacencies = try alloc.alloc(graph_types.Adjacency, 257) };
    for (segment.adjacencies, 0..) |*adjacency, i| adjacency.* = .{
        .node_id = try std.fmt.allocPrint(alloc, "node-{d:0>4}", .{i}),
        .out_edges = try alloc.alloc(graph_types.Edge, 0),
        .in_edges = try alloc.alloc(graph_types.Edge, 0),
    };
    defer segment.deinit(alloc);
    const payload = try encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    const State = struct {
        calls: usize = 0,
        fn cancelled(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.calls += 1;
            return self.calls >= 3;
        }
    };
    var state = State{};
    const cancellation = CancellationToken{ .ptr = &state, .is_cancelled_fn = State.cancelled };
    try std.testing.expectError(error.Canceled, decodeAllocWithCancellation(alloc, payload, cancellation));
    try std.testing.expectEqual(@as(usize, 3), state.calls);
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
    const expected_retained = @sizeOf([]u8) + "entities".len +
        2 * @sizeOf(graph_types.Adjacency) + "doc-a".len + "doc-b".len +
        2 * @sizeOf(graph_types.Edge) + 2 * ("doc-a".len + "cites".len);
    try std.testing.expectEqual(expected_retained, try decodedRetainedBytes(alloc, encoded));
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
    const edge_type_len_offset = header_len + 4 + "doc-a".len + 4 + "doc-b".len;
    std.mem.writeInt(u32, encoded[edge_type_len_offset..][0..4], 0, .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(alloc, encoded));
    std.mem.writeInt(u32, encoded[edge_type_len_offset..][0..4], graph_edge_type.max_bytes + 1, .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeAlloc(alloc, encoded));
    std.mem.writeInt(u32, encoded[edge_type_len_offset..][0..4], 1, .little);
    encoded[edge_type_len_offset + 4] = 0xff;
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

test "serverless graph segment codec encodes local artifacts as packed v3" {
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
    try std.testing.expectEqual(wire_version, std.mem.readInt(u16, encoded[4..6], .little));

    var decoded = try decodeAlloc(alloc, encoded);
    defer graph_types.freeSegment(alloc, &decoded);
    try std.testing.expectEqualStrings("doc-b", decoded.adjacencies[0].out_edges[0].neighbor_id);
}

test "serverless graph segment codec rejects superseded v1 artifacts" {
    const alloc = std.testing.allocator;
    var payload: [37]u8 = undefined;
    var pos: usize = 0;
    @memcpy(payload[pos..][0..4], wire_magic);
    pos += 4;
    std.mem.writeInt(u16, payload[pos..][0..2], 1, .little);
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

    try std.testing.expectError(error.UnsupportedGraphSegmentVersion, decodeAlloc(alloc, &payload));
}
