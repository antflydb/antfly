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

pub const Edge = struct {
    neighbor_id: []u8,
    edge_type: []u8,
    weight: f32,
    /// Index into Segment.neighbor_tables. Null means the segment's source
    /// table. Keeping this dictionary encoded avoids repeating a table name on
    /// every cross-table edge.
    neighbor_table_id: ?u32 = null,

    pub fn deinit(self: *Edge, alloc: Allocator) void {
        alloc.free(self.neighbor_id);
        alloc.free(self.edge_type);
        self.* = undefined;
    }
};

/// Lookup-key ordering used by published graph segments. Weight and table
/// metadata do not change key equality: a probe asks whether a `(type,
/// neighbor)` relationship exists. The publisher uses weight only as a stable
/// tie-break so duplicate physical edges remain adjacent and deterministic.
pub fn edgeLookupOrder(
    lhs_type: []const u8,
    lhs_neighbor: []const u8,
    rhs_type: []const u8,
    rhs_neighbor: []const u8,
) std.math.Order {
    const type_order = std.mem.order(u8, lhs_type, rhs_type);
    if (type_order != .eq) return type_order;
    return std.mem.order(u8, lhs_neighbor, rhs_neighbor);
}

pub fn edgesHaveCanonicalLookupOrder(edges: []const Edge) bool {
    if (edges.len == 0) return true;
    if (!std.math.isFinite(edges[0].weight)) return false;
    for (edges[1..], edges[0 .. edges.len - 1]) |current, previous| {
        if (!std.math.isFinite(current.weight)) return false;
        const lookup_order = edgeLookupOrder(
            previous.edge_type,
            previous.neighbor_id,
            current.edge_type,
            current.neighbor_id,
        );
        if (lookup_order == .gt or (lookup_order == .eq and previous.weight > current.weight))
            return false;
    }
    return true;
}

pub const EdgeLookup = struct {
    edge: ?Edge,
    inspected: usize,
};

/// Exact lookup over the canonical `(edge_type, neighbor_id)` ordering. The
/// inspected count lets public execution charge the work actually performed
/// instead of repeatedly charging the entire adjacency for batched probes.
pub fn findEdgeByTypeAndNeighbor(
    edges: []const Edge,
    edge_type: []const u8,
    neighbor_id: []const u8,
) EdgeLookup {
    var lower: usize = 0;
    var upper: usize = edges.len;
    var inspected: usize = 0;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        inspected += 1;
        const edge = edges[middle];
        switch (edgeLookupOrder(edge.edge_type, edge.neighbor_id, edge_type, neighbor_id)) {
            .lt => lower = middle + 1,
            .eq, .gt => upper = middle,
        }
    }
    // Equal lookup keys are ordered by weight by the sidecar builder. Use the
    // lower bound rather than an arbitrary binary-search hit so duplicate
    // physical edges retain the same deterministic minimum-weight semantics
    // as the stateful graph runtime.
    if (lower < edges.len) {
        inspected += 1;
        const edge = edges[lower];
        if (edgeLookupOrder(edge.edge_type, edge.neighbor_id, edge_type, neighbor_id) == .eq)
            return .{ .edge = edge, .inspected = inspected };
    }
    return .{ .edge = null, .inspected = inspected };
}

pub const Adjacency = struct {
    node_id: []u8,
    out_edges: []Edge,
    in_edges: []Edge,

    pub fn deinit(self: *Adjacency, alloc: Allocator) void {
        alloc.free(self.node_id);
        for (self.out_edges) |*edge| edge.deinit(alloc);
        alloc.free(self.out_edges);
        for (self.in_edges) |*edge| edge.deinit(alloc);
        alloc.free(self.in_edges);
        self.* = undefined;
    }
};

pub const Segment = struct {
    neighbor_tables: [][]u8 = &.{},
    adjacencies: []Adjacency,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
        for (self.neighbor_tables) |table| alloc.free(table);
        if (self.neighbor_tables.len > 0) alloc.free(self.neighbor_tables);
        for (self.adjacencies) |*adjacency| adjacency.deinit(alloc);
        alloc.free(self.adjacencies);
        self.* = undefined;
    }

    pub fn neighborTable(self: Segment, edge: Edge) ?[]const u8 {
        const table_id = edge.neighbor_table_id orelse return null;
        if (table_id >= self.neighbor_tables.len) return null;
        return self.neighbor_tables[table_id];
    }
};

/// Immutable lookup built over a decoded Segment. Keys borrow `node_id` storage
/// from the segment, so the index must be deinitialized before the segment.
pub const AdjacencyIndex = struct {
    const Map = std.StringHashMapUnmanaged(usize);

    by_node_id: Map = .empty,

    pub fn init(alloc: Allocator, segment: Segment) !AdjacencyIndex {
        return try initWithCancellation(alloc, segment, .none);
    }

    pub fn initWithCancellation(
        alloc: Allocator,
        segment: Segment,
        cancellation: CancellationToken,
    ) !AdjacencyIndex {
        try cancellation.check();
        var self = AdjacencyIndex{};
        errdefer self.deinit(alloc);
        const capacity = std.math.cast(Map.Size, segment.adjacencies.len) orelse
            return error.InvalidGraphSegment;
        try self.by_node_id.ensureTotalCapacity(alloc, capacity);
        for (segment.adjacencies, 0..) |adjacency, idx| {
            if (idx % 64 == 0) try cancellation.check();
            const gop = self.by_node_id.getOrPutAssumeCapacity(adjacency.node_id);
            if (gop.found_existing) return error.InvalidGraphSegment;
            gop.value_ptr.* = idx;
        }
        return self;
    }

    pub fn deinit(self: *AdjacencyIndex, alloc: Allocator) void {
        self.by_node_id.deinit(alloc);
        self.* = .{};
    }

    pub fn find(self: AdjacencyIndex, segment: Segment, node_id: []const u8) ?Adjacency {
        const idx = self.by_node_id.get(node_id) orelse return null;
        return segment.adjacencies[idx];
    }
};

pub fn freeSegment(alloc: Allocator, segment: *Segment) void {
    segment.deinit(alloc);
}

test "graph segment types free owned storage" {
    const alloc = std.testing.allocator;
    var segment = Segment{
        .adjacencies = try alloc.alloc(Adjacency, 1),
    };
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(Edge, 1),
        .in_edges = try alloc.alloc(Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    freeSegment(alloc, &segment);
}

test "lake graph adjacency index provides constant-time lookup and rejects duplicate nodes" {
    const alloc = std.testing.allocator;
    var segment = Segment{ .adjacencies = try alloc.alloc(Adjacency, 2) };
    defer freeSegment(alloc, &segment);
    for (segment.adjacencies, 0..) |*adjacency, idx| {
        adjacency.* = .{
            .node_id = try alloc.dupe(u8, if (idx == 0) "doc-b" else "doc-a"),
            .out_edges = try alloc.alloc(Edge, 0),
            .in_edges = try alloc.alloc(Edge, 0),
        };
    }

    {
        var index = try AdjacencyIndex.init(alloc, segment);
        defer index.deinit(alloc);
        try std.testing.expectEqualStrings("doc-a", index.find(segment, "doc-a").?.node_id);
        try std.testing.expect(index.find(segment, "missing") == null);
    }

    alloc.free(segment.adjacencies[1].node_id);
    segment.adjacencies[1].node_id = try alloc.dupe(u8, "doc-b");
    try std.testing.expectError(error.InvalidGraphSegment, AdjacencyIndex.init(alloc, segment));
}

test "canonical graph edges support bounded exact lookup" {
    const edges = [_]Edge{
        .{ .neighbor_id = @constCast("a"), .edge_type = @constCast("cites"), .weight = 1 },
        .{ .neighbor_id = @constCast("b"), .edge_type = @constCast("cites"), .weight = 0.25 },
        .{ .neighbor_id = @constCast("b"), .edge_type = @constCast("cites"), .weight = 1 },
        .{ .neighbor_id = @constCast("a"), .edge_type = @constCast("mentions"), .weight = 1 },
    };
    try std.testing.expect(edgesHaveCanonicalLookupOrder(&edges));
    const found = findEdgeByTypeAndNeighbor(&edges, "cites", "b");
    try std.testing.expect(found.edge != null);
    try std.testing.expectEqualStrings("b", found.edge.?.neighbor_id);
    try std.testing.expectEqual(@as(f64, 0.25), found.edge.?.weight);
    try std.testing.expect(found.inspected <= 4);
    const missing = findEdgeByTypeAndNeighbor(&edges, "links", "missing");
    try std.testing.expect(missing.edge == null);
    try std.testing.expect(missing.inspected <= 4);

    const unsorted = [_]Edge{ edges[2], edges[0] };
    try std.testing.expect(!edgesHaveCanonicalLookupOrder(&unsorted));
    const duplicate_weights_unsorted = [_]Edge{ edges[2], edges[1] };
    try std.testing.expect(!edgesHaveCanonicalLookupOrder(&duplicate_weights_unsorted));
    const non_finite = [_]Edge{.{
        .neighbor_id = @constCast("a"),
        .edge_type = @constCast("cites"),
        .weight = std.math.nan(f32),
    }};
    try std.testing.expect(!edgesHaveCanonicalLookupOrder(&non_finite));
    try std.testing.expect(edgesHaveCanonicalLookupOrder(&.{}));
}
