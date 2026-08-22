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
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

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

pub const GraphNeighborLimits = struct {
    max_limit: usize = 100_000,
    max_edge_types: usize = 64,
    max_edge_type_bytes: usize = 64 * 1024,
    max_edges_scanned: usize = 1_000_000,
    max_result_bytes: usize = 64 * 1024 * 1024,

    pub fn validate(self: GraphNeighborLimits) !void {
        if (self.max_limit == 0 or self.max_edge_types == 0 or self.max_edge_type_bytes == 0 or self.max_edges_scanned == 0 or self.max_result_bytes == 0) {
            return error.InvalidGraphNeighborLimits;
        }
    }
};

pub const GraphTraversalLimits = struct {
    max_limit: usize = 100_000,
    max_depth: u32 = 64,
    max_nodes_visited: usize = 100_000,
    max_edge_types: usize = 64,
    max_edge_type_bytes: usize = 64 * 1024,
    max_edges_scanned: usize = 1_000_000,
    max_result_bytes: usize = 64 * 1024 * 1024,

    pub fn validate(self: GraphTraversalLimits) !void {
        if (self.max_limit == 0 or self.max_nodes_visited == 0 or self.max_edge_types == 0 or self.max_edge_type_bytes == 0 or self.max_edges_scanned == 0 or self.max_result_bytes == 0) {
            return error.InvalidGraphTraversalLimits;
        }
    }
};

const GraphTraversalBudget = struct {
    limits: GraphTraversalLimits,
    cancellation: CancellationToken,
    nodes_visited: usize = 1,
    edges_scanned: usize = 0,
    result_bytes: usize = 0,

    fn init(limits: GraphTraversalLimits, cancellation: CancellationToken) !GraphTraversalBudget {
        try limits.validate();
        return .{ .limits = limits, .cancellation = cancellation };
    }

    fn admitEdge(self: *GraphTraversalBudget) !void {
        self.edges_scanned = std.math.add(usize, self.edges_scanned, 1) catch
            return error.GraphTraversalQueryBudgetExceeded;
        if (self.edges_scanned > self.limits.max_edges_scanned) {
            return error.GraphTraversalQueryBudgetExceeded;
        }
        if (self.edges_scanned % 64 == 0) try self.cancellation.check();
    }

    fn admitNode(self: *GraphTraversalBudget) !void {
        self.nodes_visited = std.math.add(usize, self.nodes_visited, 1) catch
            return error.GraphTraversalQueryBudgetExceeded;
        if (self.nodes_visited > self.limits.max_nodes_visited) {
            return error.GraphTraversalQueryBudgetExceeded;
        }
    }

    fn admitResultBytes(self: *GraphTraversalBudget, byte_len: usize) !void {
        self.result_bytes = std.math.add(usize, self.result_bytes, byte_len) catch
            return error.GraphTraversalQueryBudgetExceeded;
        if (self.result_bytes > self.limits.max_result_bytes) {
            return error.GraphTraversalQueryBudgetExceeded;
        }
    }
};

const BorrowedNeighbor = struct {
    doc_id: []const u8,
    edge_type: []const u8,
    weight: f32,
    direction: request_mod.GraphQueryDirection,
};

const NeighborQueue = std.PriorityQueue(BorrowedNeighbor, void, compareWorstNeighborFirst);

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
    // These strings borrow the decoded graph segment for the duration of one
    // query. Keeping traversal state borrowed avoids duplicating every visited
    // node and edge label before the final, bounded result is materialized.
    parent_doc_id: []const u8,
    via_edge_type: []const u8,
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
    return try neighborsWithLimitsAlloc(alloc, session, req, .{});
}

pub fn neighborsWithLimitsAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphNeighborsRequest,
    limits: GraphNeighborLimits,
) ![]Neighbor {
    try limits.validate();
    try session.checkCancellation();
    if (req.limit > limits.max_limit or !edgeTypeFilterWithinLimits(req.edge_types, limits.max_edge_types, limits.max_edge_type_bytes)) {
        return error.GraphNeighborQueryBudgetExceeded;
    }
    if (req.limit == 0) return try alloc.alloc(Neighbor, 0);
    const graph_index = findGraphArtifactIndex(session, req.index_name) orelse return error.GraphSegmentNotFound;
    const payload = try session.fetchArtifactAlloc(graph_index);
    defer alloc.free(payload);
    var segment = try graph_segment_mod.decodeAlloc(alloc, payload);
    defer graph_segment_mod.freeSegment(alloc, &segment);

    var adjacency_index = try graph_segment_mod.AdjacencyIndex.init(alloc, segment);
    defer adjacency_index.deinit(alloc);
    const adjacency = adjacency_index.find(segment, req.doc_id) orelse return try alloc.alloc(Neighbor, 0);
    return try selectNeighborsAlloc(alloc, session, adjacency, req, limits);
}

pub fn traverseAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphTraverseRequest,
) ![]TraversalNode {
    return try traverseWithLimitsAlloc(alloc, session, req, .{});
}

pub fn traverseWithLimitsAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphTraverseRequest,
    limits: GraphTraversalLimits,
) ![]TraversalNode {
    var budget = try GraphTraversalBudget.init(limits, session.cancellation);
    try session.checkCancellation();
    if (req.limit > limits.max_limit or req.max_depth > limits.max_depth or
        !edgeTypeFilterWithinLimits(req.edge_types, limits.max_edge_types, limits.max_edge_type_bytes))
    {
        return error.GraphTraversalQueryBudgetExceeded;
    }
    if (req.limit == 0) return try alloc.alloc(TraversalNode, 0);
    const graph_index = findGraphArtifactIndex(session, req.index_name) orelse return error.GraphSegmentNotFound;
    const payload = try session.fetchArtifactAlloc(graph_index);
    defer alloc.free(payload);
    var segment = try graph_segment_mod.decodeAlloc(alloc, payload);
    defer graph_segment_mod.freeSegment(alloc, &segment);

    var adjacency_index = try graph_segment_mod.AdjacencyIndex.init(alloc, segment);
    defer adjacency_index.deinit(alloc);
    if (adjacency_index.find(segment, req.start_doc_id) == null) return try alloc.alloc(TraversalNode, 0);

    var queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, .{ .doc_id = req.start_doc_id, .depth = 0 });

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.put(alloc, req.start_doc_id, {});

    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer parents.deinit(alloc);

    var out = std.ArrayListUnmanaged(TraversalNode).empty;
    errdefer {
        for (out.items) |*node| node.deinit(alloc);
        out.deinit(alloc);
    }

    var cursor: usize = 0;
    while (cursor < queue.items.len and out.items.len < req.limit) : (cursor += 1) {
        if (cursor % 64 == 0) try session.checkCancellation();
        const item = queue.items[cursor];
        if (item.depth > req.max_depth) continue;
        if (item.depth > 0 or req.include_start) {
            try budget.admitResultBytes(try traversalNodeAllocatedBytes(&parents, item.doc_id, req.include_start));
            try out.ensureUnusedCapacity(alloc, 1);
            out.appendAssumeCapacity(try buildTraversalNodeAlloc(alloc, &parents, item, req.include_start));
            if (out.items.len >= req.limit) break;
        }
        if (item.depth == req.max_depth) continue;
        const adjacency = adjacency_index.find(segment, item.doc_id) orelse continue;
        if (req.direction == .out or req.direction == .both) {
            try enqueueEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.out_edges, item, .out, req, &budget);
        }
        if (req.direction == .in or req.direction == .both) {
            try enqueueEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.in_edges, item, .in, req, &budget);
        }
    }
    return try out.toOwnedSlice(alloc);
}

pub fn shortestPathAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphShortestPathRequest,
) !?ShortestPath {
    return try shortestPathWithLimitsAlloc(alloc, session, req, .{});
}

pub fn shortestPathWithLimitsAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: request_mod.GraphShortestPathRequest,
    limits: GraphTraversalLimits,
) !?ShortestPath {
    var budget = try GraphTraversalBudget.init(limits, session.cancellation);
    try session.checkCancellation();
    if (req.max_depth > limits.max_depth or
        !edgeTypeFilterWithinLimits(req.edge_types, limits.max_edge_types, limits.max_edge_type_bytes))
    {
        return error.GraphTraversalQueryBudgetExceeded;
    }
    const graph_index = findGraphArtifactIndex(session, req.index_name) orelse return error.GraphSegmentNotFound;
    const payload = try session.fetchArtifactAlloc(graph_index);
    defer alloc.free(payload);
    var segment = try graph_segment_mod.decodeAlloc(alloc, payload);
    defer graph_segment_mod.freeSegment(alloc, &segment);

    var adjacency_index = try graph_segment_mod.AdjacencyIndex.init(alloc, segment);
    defer adjacency_index.deinit(alloc);
    if (adjacency_index.find(segment, req.start_doc_id) == null) return null;
    if (adjacency_index.find(segment, req.end_doc_id) == null) return null;

    if (std.mem.eql(u8, req.start_doc_id, req.end_doc_id)) {
        const result_bytes = std.math.add(usize, @sizeOf(ShortestPath) + @sizeOf([]u8), req.start_doc_id.len) catch
            return error.GraphTraversalQueryBudgetExceeded;
        try budget.admitResultBytes(result_bytes);
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
    defer parents.deinit(alloc);

    var found_depth: ?u32 = null;
    var cursor: usize = 0;
    search: while (cursor < queue.items.len) : (cursor += 1) {
        if (cursor % 64 == 0) try session.checkCancellation();
        const item = queue.items[cursor];
        if (item.depth >= req.max_depth) continue;
        const adjacency = adjacency_index.find(segment, item.doc_id) orelse continue;
        if (req.direction == .out or req.direction == .both) {
            if (try enqueueShortestPathEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.out_edges, item, .out, req, &budget)) |depth| {
                found_depth = depth;
                break :search;
            }
        }
        if (req.direction == .in or req.direction == .both) {
            if (try enqueueShortestPathEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.in_edges, item, .in, req, &budget)) |depth| {
                found_depth = depth;
                break :search;
            }
        }
    }

    if (found_depth == null) return null;
    const path_bytes = try pathResultAllocatedBytes(&parents, req.end_doc_id, true);
    try budget.admitResultBytes(std.math.add(usize, @sizeOf(ShortestPath), path_bytes) catch
        return error.GraphTraversalQueryBudgetExceeded);
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

fn selectNeighborsAlloc(
    alloc: Allocator,
    session: *const runtime_mod.QuerySession,
    adjacency: graph_segment_mod.Adjacency,
    req: request_mod.GraphNeighborsRequest,
    limits: GraphNeighborLimits,
) ![]Neighbor {
    const edges_to_scan = try admittedNeighborScanCount(adjacency, req.direction, limits.max_edges_scanned);

    var selected = NeighborQueue.empty;
    defer selected.deinit(alloc);
    try selected.ensureTotalCapacityPrecise(alloc, @min(req.limit, edges_to_scan));

    var scanned: usize = 0;
    if (req.direction == .out or req.direction == .both) {
        try selectEdges(alloc, session.cancellation, &selected, adjacency.out_edges, .out, req, &scanned);
    }
    if (req.direction == .in or req.direction == .both) {
        try selectEdges(alloc, session.cancellation, &selected, adjacency.in_edges, .in, req, &scanned);
    }
    try session.checkCancellation();

    std.mem.sort(BorrowedNeighbor, selected.items, {}, lessBorrowedNeighbor);
    var result_bytes = std.math.mul(usize, selected.items.len, @sizeOf(Neighbor)) catch
        return error.GraphNeighborQueryBudgetExceeded;
    for (selected.items) |neighbor| {
        result_bytes = std.math.add(usize, result_bytes, neighbor.doc_id.len) catch
            return error.GraphNeighborQueryBudgetExceeded;
        result_bytes = std.math.add(usize, result_bytes, neighbor.edge_type.len) catch
            return error.GraphNeighborQueryBudgetExceeded;
    }
    if (result_bytes > limits.max_result_bytes) return error.GraphNeighborQueryBudgetExceeded;

    const out = try alloc.alloc(Neighbor, selected.items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer for (out[0..initialized]) |*neighbor| neighbor.deinit(alloc);
    for (selected.items, 0..) |neighbor, idx| {
        const doc_id = try alloc.dupe(u8, neighbor.doc_id);
        errdefer alloc.free(doc_id);
        const edge_type = try alloc.dupe(u8, neighbor.edge_type);
        errdefer alloc.free(edge_type);
        out[idx] = .{
            .doc_id = doc_id,
            .edge_type = edge_type,
            .weight = neighbor.weight,
            .direction = neighbor.direction,
        };
        initialized += 1;
    }
    return out;
}

fn admittedNeighborScanCount(
    adjacency: graph_segment_mod.Adjacency,
    direction: request_mod.GraphQueryDirection,
    max_edges_scanned: usize,
) !usize {
    var edges_to_scan: usize = 0;
    if (direction == .out or direction == .both) {
        edges_to_scan = adjacency.out_edges.len;
    }
    if (direction == .in or direction == .both) {
        edges_to_scan = std.math.add(usize, edges_to_scan, adjacency.in_edges.len) catch
            return error.GraphNeighborQueryBudgetExceeded;
    }
    if (edges_to_scan > max_edges_scanned) return error.GraphNeighborQueryBudgetExceeded;
    return edges_to_scan;
}

fn selectEdges(
    alloc: Allocator,
    cancellation: CancellationToken,
    selected: *NeighborQueue,
    edges: []const graph_segment_mod.Edge,
    direction: request_mod.GraphQueryDirection,
    req: request_mod.GraphNeighborsRequest,
    scanned: *usize,
) !void {
    for (edges) |edge| {
        scanned.* += 1;
        if (scanned.* % 64 == 0) try cancellation.check();
        if (!matchesEdgeTypes(req.edge_types, edge.edge_type)) continue;
        const candidate = BorrowedNeighbor{
            .doc_id = edge.neighbor_id,
            .edge_type = edge.edge_type,
            .weight = edge.weight,
            .direction = direction,
        };
        if (selected.count() < req.limit) {
            // Capacity is admitted and reserved before the scan.
            try selected.push(alloc, candidate);
        } else if (neighborOrder(candidate, selected.peek().?) == .lt) {
            _ = selected.pop();
            try selected.push(alloc, candidate);
        }
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
    budget: *GraphTraversalBudget,
) !void {
    for (edges) |edge| {
        try budget.admitEdge();
        if (!matchesEdgeTypes(req.edge_types, edge.edge_type)) continue;
        if (seen.contains(edge.neighbor_id)) continue;
        try budget.admitNode();
        try seen.put(alloc, edge.neighbor_id, {});
        try queue.append(alloc, .{
            .doc_id = edge.neighbor_id,
            .depth = current.depth + 1,
        });
        try parents.put(alloc, edge.neighbor_id, .{
            .parent_doc_id = current.doc_id,
            .via_edge_type = edge.edge_type,
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
    budget: *GraphTraversalBudget,
) !?u32 {
    for (edges) |edge| {
        try budget.admitEdge();
        if (!matchesEdgeTypes(req.edge_types, edge.edge_type)) continue;
        if (seen.contains(edge.neighbor_id)) continue;
        try budget.admitNode();
        try seen.put(alloc, edge.neighbor_id, {});
        try parents.put(alloc, edge.neighbor_id, .{
            .parent_doc_id = current.doc_id,
            .via_edge_type = edge.edge_type,
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

fn edgeTypeFilterWithinLimits(
    edge_types: ?[]const []const u8,
    max_edge_types: usize,
    max_edge_type_bytes: usize,
) bool {
    const values = edge_types orelse return true;
    if (values.len > max_edge_types) return false;
    var total_bytes: usize = 0;
    for (values) |edge_type| {
        total_bytes = std.math.add(usize, total_bytes, edge_type.len) catch return false;
        if (total_bytes > max_edge_type_bytes) return false;
    }
    return true;
}

fn traversalNodeAllocatedBytes(
    parents: *const std.StringHashMapUnmanaged(ParentInfo),
    doc_id: []const u8,
    include_start: bool,
) !usize {
    var total = try graphResultAdd(@sizeOf(TraversalNode), doc_id.len);
    if (parents.get(doc_id)) |parent| {
        total = try graphResultAdd(total, parent.parent_doc_id.len);
        total = try graphResultAdd(total, parent.via_edge_type.len);
    }
    return try graphResultAdd(total, try pathResultAllocatedBytes(parents, doc_id, include_start));
}

fn pathResultAllocatedBytes(
    parents: *const std.StringHashMapUnmanaged(ParentInfo),
    doc_id: []const u8,
    include_start: bool,
) !usize {
    var node_count: usize = 1;
    var node_string_bytes = doc_id.len;
    var hop_count: usize = 0;
    var hop_string_bytes: usize = 0;
    var current = doc_id;
    while (parents.get(current)) |parent| {
        hop_count = try graphResultAdd(hop_count, 1);
        hop_string_bytes = try graphResultAdd(hop_string_bytes, parent.parent_doc_id.len);
        hop_string_bytes = try graphResultAdd(hop_string_bytes, current.len);
        hop_string_bytes = try graphResultAdd(hop_string_bytes, parent.via_edge_type.len);
        current = parent.parent_doc_id;
        node_count = try graphResultAdd(node_count, 1);
        node_string_bytes = try graphResultAdd(node_string_bytes, current.len);
    }
    if (!include_start) {
        node_count -= 1;
        node_string_bytes -= current.len;
    }

    var total = try graphResultAdd(
        try graphResultMul(node_count, @sizeOf([]u8)),
        node_string_bytes,
    );
    total = try graphResultAdd(total, try graphResultMul(hop_count, @sizeOf(PathHop)));
    return try graphResultAdd(total, hop_string_bytes);
}

fn graphResultAdd(lhs: usize, rhs: usize) !usize {
    return std.math.add(usize, lhs, rhs) catch error.GraphTraversalQueryBudgetExceeded;
}

fn graphResultMul(lhs: usize, rhs: usize) !usize {
    return std.math.mul(usize, lhs, rhs) catch error.GraphTraversalQueryBudgetExceeded;
}

fn buildTraversalNodeAlloc(
    alloc: Allocator,
    parents: *const std.StringHashMapUnmanaged(ParentInfo),
    item: QueueItem,
    include_start: bool,
) !TraversalNode {
    const doc_id = try alloc.dupe(u8, item.doc_id);
    errdefer alloc.free(doc_id);
    const parent = parents.get(item.doc_id);
    const parent_doc_id = if (parent) |value| try alloc.dupe(u8, value.parent_doc_id) else null;
    errdefer if (parent_doc_id) |value| alloc.free(value);
    const via_edge_type = if (parent) |value| try alloc.dupe(u8, value.via_edge_type) else null;
    errdefer if (via_edge_type) |value| alloc.free(value);
    const path = try buildPathAlloc(alloc, parents, item.doc_id, include_start);
    errdefer {
        for (path) |path_segment| alloc.free(path_segment);
        alloc.free(path);
    }
    const edge_path = try buildEdgePathAlloc(alloc, parents, item.doc_id);
    errdefer freePathHops(alloc, edge_path);
    return .{
        .doc_id = doc_id,
        .depth = item.depth,
        .parent_doc_id = parent_doc_id,
        .via_edge_type = via_edge_type,
        .path = path,
        .edge_path = edge_path,
    };
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
        try reversed.ensureUnusedCapacity(alloc, 1);
        reversed.appendAssumeCapacity(try alloc.dupe(u8, current));
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
    errdefer {
        for (reversed.items) |hop| {
            alloc.free(hop.from_doc_id);
            alloc.free(hop.to_doc_id);
            alloc.free(hop.edge_type);
        }
        reversed.deinit(alloc);
    }

    var current = doc_id;
    while (parents.get(current)) |parent| {
        try reversed.ensureUnusedCapacity(alloc, 1);
        const from_doc_id = try alloc.dupe(u8, parent.parent_doc_id);
        errdefer alloc.free(from_doc_id);
        const to_doc_id = try alloc.dupe(u8, current);
        errdefer alloc.free(to_doc_id);
        const edge_type = try alloc.dupe(u8, parent.via_edge_type);
        errdefer alloc.free(edge_type);
        reversed.appendAssumeCapacity(.{
            .from_doc_id = from_doc_id,
            .to_doc_id = to_doc_id,
            .edge_type = edge_type,
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

fn neighborOrder(lhs: BorrowedNeighbor, rhs: BorrowedNeighbor) std.math.Order {
    if (@intFromEnum(lhs.direction) != @intFromEnum(rhs.direction)) {
        return std.math.order(@intFromEnum(lhs.direction), @intFromEnum(rhs.direction));
    }
    const edge_type_order = std.mem.order(u8, lhs.edge_type, rhs.edge_type);
    if (edge_type_order != .eq) return edge_type_order;
    return std.mem.order(u8, lhs.doc_id, rhs.doc_id);
}

fn compareWorstNeighborFirst(_: void, lhs: BorrowedNeighbor, rhs: BorrowedNeighbor) std.math.Order {
    return neighborOrder(rhs, lhs);
}

fn lessBorrowedNeighbor(_: void, lhs: BorrowedNeighbor, rhs: BorrowedNeighbor) bool {
    return neighborOrder(lhs, rhs) == .lt;
}

fn lessTraversalNode(_: void, lhs: TraversalNode, rhs: TraversalNode) bool {
    if (lhs.depth != rhs.depth) return lhs.depth < rhs.depth;
    return std.mem.order(u8, lhs.doc_id, rhs.doc_id) == .lt;
}

test "serverless graph reader filters by direction and edge type" {
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
    var selected = NeighborQueue.empty;
    defer selected.deinit(alloc);
    try selected.ensureTotalCapacityPrecise(alloc, req.limit);
    var scanned: usize = 0;
    try selectEdges(alloc, .none, &selected, adjacency.out_edges, .out, req, &scanned);
    try selectEdges(alloc, .none, &selected, adjacency.in_edges, .in, req, &scanned);
    try std.testing.expectEqual(@as(usize, 2), selected.count());
    try std.testing.expectEqual(@as(usize, 3), scanned);
}

test "serverless graph neighbor selection retains only deterministic top k" {
    const alloc = std.testing.allocator;
    const edges = [_]graph_segment_mod.Edge{
        .{ .neighbor_id = @constCast("doc-z"), .edge_type = @constCast("rel"), .weight = 1.0 },
        .{ .neighbor_id = @constCast("doc-d"), .edge_type = @constCast("cites"), .weight = 1.0 },
        .{ .neighbor_id = @constCast("doc-b"), .edge_type = @constCast("cites"), .weight = 1.0 },
        .{ .neighbor_id = @constCast("doc-a"), .edge_type = @constCast("cites"), .weight = 1.0 },
        .{ .neighbor_id = @constCast("doc-c"), .edge_type = @constCast("cites"), .weight = 1.0 },
    };
    const req = request_mod.GraphNeighborsRequest{
        .doc_id = @constCast("root"),
        .limit = 3,
    };
    var selected = NeighborQueue.empty;
    defer selected.deinit(alloc);
    try selected.ensureTotalCapacityPrecise(alloc, req.limit);
    var scanned: usize = 0;
    try selectEdges(alloc, .none, &selected, &edges, .out, req, &scanned);
    try std.testing.expectEqual(req.limit, selected.count());
    try std.testing.expectEqual(edges.len, scanned);
    std.mem.sort(BorrowedNeighbor, selected.items, {}, lessBorrowedNeighbor);
    try std.testing.expectEqualStrings("doc-a", selected.items[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", selected.items[1].doc_id);
    try std.testing.expectEqualStrings("doc-c", selected.items[2].doc_id);
}

test "serverless graph neighbor selection admits scans and observes cancellation" {
    const alloc = std.testing.allocator;
    const edge = graph_segment_mod.Edge{
        .neighbor_id = @constCast("doc-a"),
        .edge_type = @constCast("cites"),
        .weight = 1.0,
    };
    const edges = [_]graph_segment_mod.Edge{edge} ** 64;
    const adjacency = graph_segment_mod.Adjacency{
        .node_id = @constCast("root"),
        .out_edges = @constCast(edges[0..]),
        .in_edges = @constCast(edges[0..1]),
    };
    try std.testing.expectEqual(@as(usize, 65), try admittedNeighborScanCount(adjacency, .both, 65));
    try std.testing.expectError(
        error.GraphNeighborQueryBudgetExceeded,
        admittedNeighborScanCount(adjacency, .both, 64),
    );

    const req = request_mod.GraphNeighborsRequest{ .doc_id = @constCast("root"), .limit = 1 };
    var selected = NeighborQueue.empty;
    defer selected.deinit(alloc);
    try selected.ensureTotalCapacityPrecise(alloc, 1);
    var cancelled = std.atomic.Value(bool).init(true);
    var scanned: usize = 0;
    try std.testing.expectError(
        error.Canceled,
        selectEdges(alloc, CancellationToken.fromAtomic(&cancelled), &selected, &edges, .out, req, &scanned),
    );
    try std.testing.expectEqual(@as(usize, 64), scanned);

    const edge_types = [_][]const u8{ "cites", "related" };
    try std.testing.expect(edgeTypeFilterWithinLimits(&edge_types, 2, 12));
    try std.testing.expect(!edgeTypeFilterWithinLimits(&edge_types, 1, 12));
    try std.testing.expect(!edgeTypeFilterWithinLimits(&edge_types, 2, 11));
}

test "serverless graph traversal bounds edge scans, visited nodes, results, and cancellation" {
    const alloc = std.testing.allocator;
    const edges = [_]graph_segment_mod.Edge{
        .{ .neighbor_id = @constCast("doc-b"), .edge_type = @constCast("cites"), .weight = 1.0 },
        .{ .neighbor_id = @constCast("doc-c"), .edge_type = @constCast("cites"), .weight = 1.0 },
    };
    const req = request_mod.GraphTraverseRequest{
        .start_doc_id = @constCast("doc-a"),
        .limit = 1,
    };

    var queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, .{ .doc_id = "doc-a", .depth = 0 });
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.put(alloc, "doc-a", {});
    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer parents.deinit(alloc);

    var edge_budget = try GraphTraversalBudget.init(.{ .max_edges_scanned = 1 }, .none);
    try std.testing.expectError(
        error.GraphTraversalQueryBudgetExceeded,
        enqueueEdgesAlloc(alloc, &queue, &seen, &parents, &edges, .{ .doc_id = "doc-a", .depth = 0 }, .out, req, &edge_budget),
    );
    try std.testing.expectEqual(@as(usize, 2), edge_budget.edges_scanned);

    var node_queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer node_queue.deinit(alloc);
    try node_queue.append(alloc, .{ .doc_id = "doc-a", .depth = 0 });
    var node_seen = std.StringHashMapUnmanaged(void).empty;
    defer node_seen.deinit(alloc);
    try node_seen.put(alloc, "doc-a", {});
    var node_parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer node_parents.deinit(alloc);
    var node_budget = try GraphTraversalBudget.init(.{ .max_nodes_visited = 1 }, .none);
    try std.testing.expectError(
        error.GraphTraversalQueryBudgetExceeded,
        enqueueEdgesAlloc(alloc, &node_queue, &node_seen, &node_parents, edges[0..1], .{ .doc_id = "doc-a", .depth = 0 }, .out, req, &node_budget),
    );
    try std.testing.expectEqual(@as(usize, 1), node_seen.count());

    const repeated = [_]graph_segment_mod.Edge{edges[0]} ** 64;
    var cancelled = std.atomic.Value(bool).init(true);
    var cancel_budget = try GraphTraversalBudget.init(.{}, CancellationToken.fromAtomic(&cancelled));
    try std.testing.expectError(
        error.Canceled,
        enqueueEdgesAlloc(alloc, &node_queue, &node_seen, &node_parents, &repeated, .{ .doc_id = "doc-a", .depth = 0 }, .out, req, &cancel_budget),
    );
    try std.testing.expectEqual(@as(usize, 64), cancel_budget.edges_scanned);

    try node_parents.put(alloc, "doc-b", .{
        .parent_doc_id = "doc-a",
        .via_edge_type = "cites",
        .direction = .out,
        .weight = 1.0,
    });
    const result_bytes = try traversalNodeAllocatedBytes(&node_parents, "doc-b", true);
    var result_budget = try GraphTraversalBudget.init(.{ .max_result_bytes = result_bytes }, .none);
    try result_budget.admitResultBytes(result_bytes);
    try std.testing.expectError(
        error.GraphTraversalQueryBudgetExceeded,
        result_budget.admitResultBytes(1),
    );
}

test "serverless graph traversal result materialization is allocation-failure safe" {
    const alloc = std.testing.allocator;
    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer parents.deinit(alloc);
    try parents.put(alloc, "doc-b", .{
        .parent_doc_id = "doc-a",
        .via_edge_type = "cites",
        .direction = .out,
        .weight = 1.0,
    });

    const AllocationRunner = struct {
        fn run(a: Allocator, parent_map: *const std.StringHashMapUnmanaged(ParentInfo)) !void {
            var node = try buildTraversalNodeAlloc(a, parent_map, .{ .doc_id = "doc-b", .depth = 1 }, true);
            defer node.deinit(a);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{&parents});
}

test "serverless graph reader traverses breadth-first with parent metadata" {
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
    defer parents.deinit(alloc);

    try queue.append(alloc, .{ .doc_id = "doc-a", .depth = 0 });
    try seen.put(alloc, "doc-a", {});
    const req = request_mod.GraphTraverseRequest{
        .start_doc_id = @constCast("doc-a"),
        .direction = .out,
        .max_depth = 2,
        .limit = 10,
    };
    const adjacency = findAdjacency(segment, req.start_doc_id).?;
    var budget = try GraphTraversalBudget.init(.{}, .none);
    try enqueueEdgesAlloc(alloc, &queue, &seen, &parents, adjacency.out_edges, .{ .doc_id = "doc-a", .depth = 0 }, .out, req, &budget);
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

test "serverless graph reader builds shortest path with typed edge hops" {
    const alloc = std.testing.allocator;
    var parents = std.StringHashMapUnmanaged(ParentInfo).empty;
    defer parents.deinit(alloc);

    try parents.put(alloc, "b", .{
        .parent_doc_id = "a",
        .via_edge_type = "cites",
        .direction = .out,
        .weight = 1.0,
    });
    try parents.put(alloc, "d", .{
        .parent_doc_id = "b",
        .via_edge_type = "rel",
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
