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
const graph_segment_mod = @import("../graph_segment/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const request_mod = @import("request.zig");
const runtime_mod = @import("runtime.zig");

pub const Neighbor = struct {
    doc_id: []u8,
    edge_type: []u8,
    weight: f32,
    direction: request_mod.GraphQueryDirection,

    pub fn deinit(self: *Neighbor, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.edge_type);
        self.* = undefined;
    }
};

pub const TraversalNode = struct {
    doc_id: []u8,
    depth: u32,
    parent_doc_id: ?[]u8 = null,
    via_edge_type: ?[]u8 = null,
    path: ?[][]u8 = null,
    edge_path: ?[]PathHop = null,

    pub fn deinit(self: *TraversalNode, alloc: Allocator) void {
        alloc.free(self.doc_id);
        if (self.parent_doc_id) |parent_doc_id| alloc.free(parent_doc_id);
        if (self.via_edge_type) |via_edge_type| alloc.free(via_edge_type);
        if (self.path) |path| {
            for (path) |segment| alloc.free(segment);
            alloc.free(path);
        }
        if (self.edge_path) |edge_path| freePathHops(alloc, edge_path);
        self.* = undefined;
    }
};

pub const PathHop = struct {
    from_doc_id: []u8,
    to_doc_id: []u8,
    edge_type: []u8,
    weight: f32,
    direction: request_mod.GraphQueryDirection,

    pub fn deinit(self: *PathHop, alloc: Allocator) void {
        alloc.free(self.from_doc_id);
        alloc.free(self.to_doc_id);
        alloc.free(self.edge_type);
        self.* = undefined;
    }
};

pub const ShortestPath = struct {
    depth: u32,
    path: [][]u8,
    edge_path: []PathHop,

    pub fn deinit(self: *ShortestPath, alloc: Allocator) void {
        for (self.path) |segment| alloc.free(segment);
        alloc.free(self.path);
        freePathHops(alloc, self.edge_path);
        self.* = undefined;
    }
};

const QueueItem = struct {
    doc_id: []const u8,
    depth: u32,
};

const ParentInfo = struct {
    parent_doc_id: []u8,
    via_edge_type: []u8,
    direction: request_mod.GraphQueryDirection,
    weight: f32,
};

pub fn freeNeighbors(alloc: Allocator, neighbors: []Neighbor) void {
    for (neighbors) |*neighbor| neighbor.deinit(alloc);
    alloc.free(neighbors);
}

pub fn freeTraversalNodes(alloc: Allocator, nodes: []TraversalNode) void {
    for (nodes) |*node| node.deinit(alloc);
    alloc.free(nodes);
}

pub fn freePathHops(alloc: Allocator, hops: []PathHop) void {
    for (hops) |*hop| hop.deinit(alloc);
    alloc.free(hops);
}

pub fn freeShortestPath(alloc: Allocator, path: *ShortestPath) void {
    path.deinit(alloc);
}

pub fn neighborsAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphNeighborsRequest,
) ![]Neighbor {
    const graph_index = findGraphArtifactIndex(session, req.index_name) orelse return error.GraphSegmentNotFound;
    try session.warmArtifact(graph_index);
    const payload = try session.fetchArtifactAlloc(graph_index);
    defer alloc.free(payload);
    var segment = try graph_segment_mod.decodeAlloc(alloc, payload);
    defer graph_segment_mod.freeSegment(alloc, &segment);

    const adjacency = findAdjacency(segment, req.doc_id) orelse return try alloc.alloc(Neighbor, 0);
    var neighbors = std.ArrayListUnmanaged(Neighbor).empty;
    errdefer freeNeighbors(alloc, neighbors.items);

    if (req.direction == .out or req.direction == .both) {
        try appendEdgesAlloc(alloc, &neighbors, adjacency.out_edges, .out, req);
    }
    if (req.direction == .in or req.direction == .both) {
        try appendEdgesAlloc(alloc, &neighbors, adjacency.in_edges, .in, req);
    }

    std.mem.sort(Neighbor, neighbors.items, {}, lessNeighbor);
    if (neighbors.items.len > req.limit) {
        var out = try alloc.alloc(Neighbor, req.limit);
        errdefer alloc.free(out);
        for (neighbors.items[0..req.limit], 0..) |neighbor, idx| out[idx] = neighbor;
        for (neighbors.items[req.limit..]) |*neighbor| neighbor.deinit(alloc);
        alloc.free(neighbors.items);
        return out;
    }
    return try neighbors.toOwnedSlice(alloc);
}

pub fn traverseAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphTraverseRequest,
) ![]TraversalNode {
    const graph_index = findGraphArtifactIndex(session, req.index_name) orelse return error.GraphSegmentNotFound;
    try session.warmArtifact(graph_index);
    const payload = try session.fetchArtifactAlloc(graph_index);
    defer alloc.free(payload);
    var segment = try graph_segment_mod.decodeAlloc(alloc, payload);
    defer graph_segment_mod.freeSegment(alloc, &segment);

    if (findAdjacency(segment, req.start_doc_id) == null) return try alloc.alloc(TraversalNode, 0);

    var queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, .{ .doc_id = req.start_doc_id, .depth = 0 });

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.put(alloc, req.start_doc_id, {});

    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer {
        var it = parents.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.parent_doc_id);
            alloc.free(entry.value_ptr.via_edge_type);
        }
        parents.deinit(alloc);
    }

    var out = std.ArrayListUnmanaged(TraversalNode).empty;
    errdefer freeTraversalNodes(alloc, out.items);

    var cursor: usize = 0;
    while (cursor < queue.items.len and out.items.len < req.limit) : (cursor += 1) {
        const item = queue.items[cursor];
        if (item.depth > req.max_depth) continue;
        if (item.depth > 0 or req.include_start) {
            const parent = parents.get(item.doc_id);
            const path = try buildPathAlloc(alloc, &parents, item.doc_id, req.include_start);
            errdefer {
                for (path) |path_segment| alloc.free(path_segment);
                alloc.free(path);
            }
            const edge_path = try buildEdgePathAlloc(alloc, &parents, item.doc_id);
            errdefer freePathHops(alloc, edge_path);
            try out.append(alloc, .{
                .doc_id = try alloc.dupe(u8, item.doc_id),
                .depth = item.depth,
                .parent_doc_id = if (parent) |value| try alloc.dupe(u8, value.parent_doc_id) else null,
                .via_edge_type = if (parent) |value| try alloc.dupe(u8, value.via_edge_type) else null,
                .path = path,
                .edge_path = edge_path,
            });
            if (out.items.len >= req.limit) break;
        }
        if (item.depth == req.max_depth) continue;
        const adjacency = findAdjacency(segment, item.doc_id) orelse continue;
        if (req.direction == .out or req.direction == .both) {
            try enqueueEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.out_edges, item, .out, req);
        }
        if (req.direction == .in or req.direction == .both) {
            try enqueueEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.in_edges, item, .in, req);
        }
    }
    return try out.toOwnedSlice(alloc);
}

pub fn shortestPathAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphShortestPathRequest,
) !?ShortestPath {
    const graph_index = findGraphArtifactIndex(session, req.index_name) orelse return error.GraphSegmentNotFound;
    try session.warmArtifact(graph_index);
    const payload = try session.fetchArtifactAlloc(graph_index);
    defer alloc.free(payload);
    var segment = try graph_segment_mod.decodeAlloc(alloc, payload);
    defer graph_segment_mod.freeSegment(alloc, &segment);

    if (findAdjacency(segment, req.start_doc_id) == null) return null;
    if (findAdjacency(segment, req.end_doc_id) == null) return null;

    if (std.mem.eql(u8, req.start_doc_id, req.end_doc_id)) {
        const path = try alloc.alloc([]u8, 1);
        errdefer alloc.free(path);
        path[0] = try alloc.dupe(u8, req.start_doc_id);
        return .{
            .depth = 0,
            .path = path,
            .edge_path = try alloc.alloc(PathHop, 0),
        };
    }

    var queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, .{ .doc_id = req.start_doc_id, .depth = 0 });

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.put(alloc, req.start_doc_id, {});

    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer {
        var it = parents.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.parent_doc_id);
            alloc.free(entry.value_ptr.via_edge_type);
        }
        parents.deinit(alloc);
    }

    var found_depth: ?u32 = null;
    var cursor: usize = 0;
    search: while (cursor < queue.items.len) : (cursor += 1) {
        const item = queue.items[cursor];
        if (item.depth >= req.max_depth) continue;
        const adjacency = findAdjacency(segment, item.doc_id) orelse continue;
        if (req.direction == .out or req.direction == .both) {
            if (try enqueueShortestPathEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.out_edges, item, .out, req)) |depth| {
                found_depth = depth;
                break :search;
            }
        }
        if (req.direction == .in or req.direction == .both) {
            if (try enqueueShortestPathEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.in_edges, item, .in, req)) |depth| {
                found_depth = depth;
                break :search;
            }
        }
    }

    if (found_depth == null) return null;
    const path = try buildPathAlloc(alloc, &parents, req.end_doc_id, true);
    errdefer {
        for (path) |segment_id| alloc.free(segment_id);
        alloc.free(path);
    }
    const edge_path = try buildEdgePathAlloc(alloc, &parents, req.end_doc_id);
    errdefer freePathHops(alloc, edge_path);
    return .{
        .depth = found_depth.?,
        .path = path,
        .edge_path = edge_path,
    };
}

pub fn findGraphArtifactIndex(session: *const runtime_mod.QuerySession, index_name: []const u8) ?usize {
    if (index_name.len > 0) {
        if (session.findNamedArtifactIndex(.graph_segment, index_name)) |artifact_index| return artifact_index;
    }

    var graph_count: usize = 0;
    var last_graph_index: ?usize = null;
    for (0..session.artifactCount()) |artifact_index| {
        const artifact = session.artifactRef(artifact_index) orelse continue;
        if (artifact.kind != .graph_segment) continue;
        graph_count += 1;
        last_graph_index = artifact_index;
    }
    if (graph_count == 1) return last_graph_index;
    return null;
}

fn appendEdgesAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(Neighbor),
    edges: []const graph_segment_mod.Edge,
    direction: request_mod.GraphQueryDirection,
    req: request_mod.GraphNeighborsRequest,
) !void {
    for (edges) |edge| {
        if (!matchesEdgeTypes(req.edge_types, edge.edge_type)) continue;
        try out.append(alloc, .{
            .doc_id = try alloc.dupe(u8, edge.neighbor_id),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
            .direction = direction,
        });
    }
}

fn enqueueEdgesAlloc(
    alloc: Allocator,
    queue: *std.ArrayListUnmanaged(QueueItem),
    seen: *std.StringHashMapUnmanaged(void),
    parents: *std.StringHashMapUnmanaged(ParentInfo),
    edges: []const graph_segment_mod.Edge,
    current: QueueItem,
    direction: request_mod.GraphQueryDirection,
    req: request_mod.GraphTraverseRequest,
) !void {
    for (edges) |edge| {
        if (!matchesEdgeTypes(req.edge_types, edge.edge_type)) continue;
        const gop = try seen.getOrPut(alloc, edge.neighbor_id);
        if (gop.found_existing) continue;
        try queue.append(alloc, .{
            .doc_id = edge.neighbor_id,
            .depth = current.depth + 1,
        });
        try parents.put(alloc, try alloc.dupe(u8, edge.neighbor_id), .{
            .parent_doc_id = try alloc.dupe(u8, current.doc_id),
            .via_edge_type = try alloc.dupe(u8, edge.edge_type),
            .direction = direction,
            .weight = edge.weight,
        });
    }
}

fn enqueueShortestPathEdgesAlloc(
    alloc: Allocator,
    queue: *std.ArrayListUnmanaged(QueueItem),
    seen: *std.StringHashMapUnmanaged(void),
    parents: *std.StringHashMapUnmanaged(ParentInfo),
    edges: []const graph_segment_mod.Edge,
    current: QueueItem,
    direction: request_mod.GraphQueryDirection,
    req: request_mod.GraphShortestPathRequest,
) !?u32 {
    for (edges) |edge| {
        if (!matchesEdgeTypes(req.edge_types, edge.edge_type)) continue;
        const gop = try seen.getOrPut(alloc, edge.neighbor_id);
        if (gop.found_existing) continue;
        try parents.put(alloc, try alloc.dupe(u8, edge.neighbor_id), .{
            .parent_doc_id = try alloc.dupe(u8, current.doc_id),
            .via_edge_type = try alloc.dupe(u8, edge.edge_type),
            .direction = direction,
            .weight = edge.weight,
        });
        if (std.mem.eql(u8, edge.neighbor_id, req.end_doc_id)) return current.depth + 1;
        try queue.append(alloc, .{
            .doc_id = edge.neighbor_id,
            .depth = current.depth + 1,
        });
    }
    return null;
}

fn matchesEdgeTypes(edge_types: ?[]const []const u8, candidate: []const u8) bool {
    const values = edge_types orelse return true;
    for (values) |edge_type| {
        if (std.mem.eql(u8, edge_type, candidate)) return true;
    }
    return false;
}

fn buildPathAlloc(
    alloc: Allocator,
    parents: *const std.StringHashMapUnmanaged(ParentInfo),
    doc_id: []const u8,
    include_start: bool,
) ![][]u8 {
    var reversed = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (reversed.items) |segment| alloc.free(segment);
        reversed.deinit(alloc);
    }

    var current = doc_id;
    while (true) {
        try reversed.append(alloc, try alloc.dupe(u8, current));
        const parent = parents.get(current) orelse break;
        current = parent.parent_doc_id;
    }

    if (!include_start and reversed.items.len > 0) {
        alloc.free(reversed.items[reversed.items.len - 1]);
        _ = reversed.pop();
    }

    const out = try alloc.alloc([]u8, reversed.items.len);
    errdefer alloc.free(out);
    for (reversed.items, 0..) |_, idx| {
        out[idx] = reversed.items[reversed.items.len - 1 - idx];
    }
    reversed.deinit(alloc);
    return out;
}

fn buildEdgePathAlloc(
    alloc: Allocator,
    parents: *const std.StringHashMapUnmanaged(ParentInfo),
    doc_id: []const u8,
) ![]PathHop {
    var reversed = std.ArrayListUnmanaged(PathHop).empty;
    errdefer freePathHops(alloc, reversed.items);

    var current = doc_id;
    while (parents.get(current)) |parent| {
        try reversed.append(alloc, .{
            .from_doc_id = try alloc.dupe(u8, parent.parent_doc_id),
            .to_doc_id = try alloc.dupe(u8, current),
            .edge_type = try alloc.dupe(u8, parent.via_edge_type),
            .weight = parent.weight,
            .direction = parent.direction,
        });
        current = parent.parent_doc_id;
    }

    const out = try alloc.alloc(PathHop, reversed.items.len);
    errdefer alloc.free(out);
    for (reversed.items, 0..) |_, idx| {
        out[idx] = reversed.items[reversed.items.len - 1 - idx];
    }
    reversed.deinit(alloc);
    return out;
}

fn findAdjacency(segment: graph_segment_mod.Segment, doc_id: []const u8) ?graph_segment_mod.Adjacency {
    for (segment.adjacencies) |adjacency| {
        if (std.mem.eql(u8, adjacency.node_id, doc_id)) return adjacency;
    }
    return null;
}

fn lessNeighbor(_: void, lhs: Neighbor, rhs: Neighbor) bool {
    if (@intFromEnum(lhs.direction) != @intFromEnum(rhs.direction)) {
        return @intFromEnum(lhs.direction) < @intFromEnum(rhs.direction);
    }
    const edge_type_order = std.mem.order(u8, lhs.edge_type, rhs.edge_type);
    if (edge_type_order != .eq) return edge_type_order == .lt;
    return std.mem.order(u8, lhs.doc_id, rhs.doc_id) == .lt;
}

fn lessTraversalNode(_: void, lhs: TraversalNode, rhs: TraversalNode) bool {
    if (lhs.depth != rhs.depth) return lhs.depth < rhs.depth;
    return std.mem.order(u8, lhs.doc_id, rhs.doc_id) == .lt;
}

test "graph reader filters by direction and edge type" {
    const alloc = std.testing.allocator;
    var segment = graph_segment_mod.Segment{
        .adjacencies = try alloc.alloc(graph_segment_mod.Adjacency, 1),
    };
    defer graph_segment_mod.freeSegment(alloc, &segment);
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_segment_mod.Edge, 2),
        .in_edges = try alloc.alloc(graph_segment_mod.Edge, 1),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[0].out_edges[1] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-c"),
        .edge_type = try alloc.dupe(u8, "rel"),
        .weight = 0.5,
    };
    segment.adjacencies[0].in_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-z"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 2.0,
    };

    const req = request_mod.GraphNeighborsRequest{
        .doc_id = @constCast("doc-a"),
        .direction = .both,
        .edge_types = &.{@constCast("cites")},
        .limit = 10,
    };
    const adjacency = findAdjacency(segment, req.doc_id).?;
    var neighbors = std.ArrayListUnmanaged(Neighbor).empty;
    defer {
        for (neighbors.items) |*neighbor| neighbor.deinit(alloc);
        neighbors.deinit(alloc);
    }
    try appendEdgesAlloc(alloc, &neighbors, adjacency.out_edges, .out, req);
    try appendEdgesAlloc(alloc, &neighbors, adjacency.in_edges, .in, req);
    try std.testing.expectEqual(@as(usize, 2), neighbors.items.len);
}

test "graph reader traverses breadth-first with parent metadata" {
    const alloc = std.testing.allocator;
    var segment = graph_segment_mod.Segment{
        .adjacencies = try alloc.alloc(graph_segment_mod.Adjacency, 3),
    };
    defer graph_segment_mod.freeSegment(alloc, &segment);

    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(graph_segment_mod.Edge, 2),
        .in_edges = try alloc.alloc(graph_segment_mod.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[0].out_edges[1] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-c"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[1] = .{
        .node_id = try alloc.dupe(u8, "doc-b"),
        .out_edges = try alloc.alloc(graph_segment_mod.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment_mod.Edge, 0),
    };
    segment.adjacencies[1].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-d"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[2] = .{
        .node_id = try alloc.dupe(u8, "doc-c"),
        .out_edges = try alloc.alloc(graph_segment_mod.Edge, 0),
        .in_edges = try alloc.alloc(graph_segment_mod.Edge, 0),
    };

    var queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer queue.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer {
        var it = parents.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.parent_doc_id);
            alloc.free(entry.value_ptr.via_edge_type);
        }
        parents.deinit(alloc);
    }

    try queue.append(alloc, .{ .doc_id = "doc-a", .depth = 0 });
    try seen.put(alloc, "doc-a", {});
    const req = request_mod.GraphTraverseRequest{
        .start_doc_id = @constCast("doc-a"),
        .direction = .out,
        .max_depth = 2,
        .limit = 10,
    };
    const adjacency = findAdjacency(segment, req.start_doc_id).?;
    try enqueueEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.out_edges, .{ .doc_id = "doc-a", .depth = 0 }, .out, req);
    try std.testing.expectEqual(@as(usize, 3), queue.items.len);

    var out = std.ArrayListUnmanaged(TraversalNode).empty;
    defer {
        for (out.items) |*node| node.deinit(alloc);
        out.deinit(alloc);
    }
    try out.append(alloc, .{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .depth = 1,
        .parent_doc_id = try alloc.dupe(u8, "doc-a"),
        .via_edge_type = try alloc.dupe(u8, "cites"),
        .path = try alloc.dupe([]u8, &.{ try alloc.dupe(u8, "doc-a"), try alloc.dupe(u8, "doc-b") }),
        .edge_path = try alloc.dupe(PathHop, &.{.{
            .from_doc_id = try alloc.dupe(u8, "doc-a"),
            .to_doc_id = try alloc.dupe(u8, "doc-b"),
            .edge_type = try alloc.dupe(u8, "cites"),
            .weight = 1.0,
            .direction = .out,
        }}),
    });
    try out.append(alloc, .{
        .doc_id = try alloc.dupe(u8, "doc-c"),
        .depth = 1,
        .parent_doc_id = try alloc.dupe(u8, "doc-a"),
        .via_edge_type = try alloc.dupe(u8, "cites"),
        .path = try alloc.dupe([]u8, &.{ try alloc.dupe(u8, "doc-a"), try alloc.dupe(u8, "doc-c") }),
        .edge_path = try alloc.dupe(PathHop, &.{.{
            .from_doc_id = try alloc.dupe(u8, "doc-a"),
            .to_doc_id = try alloc.dupe(u8, "doc-c"),
            .edge_type = try alloc.dupe(u8, "cites"),
            .weight = 1.0,
            .direction = .out,
        }}),
    });
    std.mem.sort(TraversalNode, out.items, {}, lessTraversalNode);
    try std.testing.expectEqualStrings("doc-b", out.items[0].doc_id);
    try std.testing.expectEqual(@as(usize, 2), out.items[0].path.?.len);
    try std.testing.expectEqualStrings("doc-a", out.items[0].path.?[0]);
    try std.testing.expectEqual(@as(usize, 1), out.items[0].edge_path.?.len);
    try std.testing.expectEqualStrings("doc-a", out.items[0].edge_path.?[0].from_doc_id);
    try std.testing.expectEqualStrings("doc-b", out.items[0].edge_path.?[0].to_doc_id);
}

test "graph reader builds shortest path with typed edge hops" {
    const alloc = std.testing.allocator;
    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer {
        var it = parents.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.parent_doc_id);
            alloc.free(entry.value_ptr.via_edge_type);
        }
        parents.deinit(alloc);
    }

    try parents.put(alloc, try alloc.dupe(u8, "b"), .{
        .parent_doc_id = try alloc.dupe(u8, "a"),
        .via_edge_type = try alloc.dupe(u8, "cites"),
        .direction = .out,
        .weight = 1.0,
    });
    try parents.put(alloc, try alloc.dupe(u8, "d"), .{
        .parent_doc_id = try alloc.dupe(u8, "b"),
        .via_edge_type = try alloc.dupe(u8, "rel"),
        .direction = .out,
        .weight = 2.0,
    });

    const path = try buildPathAlloc(alloc, &parents, "d", true);
    defer {
        for (path) |segment_id| alloc.free(segment_id);
        alloc.free(path);
    }
    const edge_path = try buildEdgePathAlloc(alloc, &parents, "d");
    defer freePathHops(alloc, edge_path);

    try std.testing.expectEqual(@as(usize, 3), path.len);
    try std.testing.expectEqualStrings("a", path[0]);
    try std.testing.expectEqualStrings("d", path[2]);
    try std.testing.expectEqual(@as(usize, 2), edge_path.len);
    try std.testing.expectEqualStrings("a", edge_path[0].from_doc_id);
    try std.testing.expectEqualStrings("b", edge_path[0].to_doc_id);
    try std.testing.expectEqualStrings("rel", edge_path[1].edge_type);
}
