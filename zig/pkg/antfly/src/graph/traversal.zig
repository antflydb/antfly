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

//! BFS-based graph traversal engine.
//!
//! Matches Go antfly's TraverseEdges algorithm (db.go:5240-5384):
//!   - BFS with configurable depth, direction, edge type, and weight filters
//!   - Deduplication via visited set
//!   - Optional path tracking

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_time = @import("antfly_platform").time;
const graph_mod = @import("graph.zig");
const Edge = graph_mod.Edge;
const EdgeDirection = graph_mod.EdgeDirection;
const GraphIndex = graph_mod.GraphIndex;
const NodeAdmission = @import("node_admission.zig").NodeAdmission;
const NodeRef = @import("node_admission.zig").NodeRef;
const node_identity = @import("node_identity.zig");

// ============================================================================
// Traversal types
// ============================================================================

pub const TraversalRules = struct {
    edge_types: []const []const u8 = &.{}, // empty = all types
    direction: EdgeDirection = .out,
    max_depth: u32 = 3,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0, // 0 = no upper limit
    max_results: u32 = 100,
    deduplicate: bool = true,
    include_paths: bool = false,
    node_admission: ?NodeAdmission = null,
};

pub const TraversalResult = struct {
    key: []const u8,
    depth: u32,
    total_weight: f64,
    path: ?[]const []const u8, // if include_paths
    /// Table of the reached node, when the edge that reached it declared a
    /// cross-table endpoint (`target_table` in its metadata). Owned.
    target_table: ?[]const u8 = null,
};

/// Extract `target_table` from an edge's metadata JSON
/// (`{"target_table":"entities",...}`) without a full parse. Returns a slice
/// into `metadata`; caller copies it if it must outlive the edge.
pub fn metadataTargetTable(metadata: []const u8) ?[]const u8 {
    const marker = "\"target_table\":\"";
    const start = std.mem.indexOf(u8, metadata, marker) orelse return null;
    const value_start = start + marker.len;
    const end = std.mem.indexOfScalarPos(u8, metadata, value_start, '"') orelse return null;
    if (end == value_start) return null;
    return metadata[value_start..end];
}

// ============================================================================
// BFS traversal
// ============================================================================

const QueueEntry = struct {
    key: []const u8,
    depth: u32,
    total_weight: f64,
    path: ?std.ArrayListUnmanaged([]const u8),
    target_table: ?[]const u8 = null,

    fn deinit(self: *QueueEntry, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.target_table) |table| alloc.free(table);
        if (self.path) |path| freeQueuePath(alloc, path);
        self.* = undefined;
    }
};

/// Perform BFS graph traversal from start_key using the given rules.
/// Caller owns all returned memory (use freeResults to clean up).
pub fn traverse(alloc: Allocator, graph_index: *GraphIndex, start_key: []const u8, rules: TraversalRules) ![]TraversalResult {
    const Reader = struct {
        graph_index: *GraphIndex,

        pub fn getEdges(self: @This(), a: Allocator, key: []const u8, direction: EdgeDirection) ![]Edge {
            return try self.graph_index.getEdges(a, key, "", direction);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []Edge) void {
            GraphIndex.freeEdges(a, edges);
        }
    };
    return try traverseWithEdgeReader(alloc, Reader{ .graph_index = graph_index }, start_key, rules);
}

/// Reader-generic traversal over an immutable graph snapshot. The reader owns
/// the representation-specific edge lookup and cleanup while traversal keeps
/// one implementation of filtering, deduplication, and admission semantics.
pub fn traverseWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    rules: TraversalRules,
) ![]TraversalResult {
    var results = std.ArrayListUnmanaged(TraversalResult).empty;
    errdefer {
        freeResults(alloc, results.items);
        results.deinit(alloc);
    }

    if (rules.node_admission) |admission| {
        if (!try startNodeAdmittedWithEdgeReader(alloc, edge_reader, start_key, rules.direction, admission)) {
            return try results.toOwnedSlice(alloc);
        }
    }

    var visited = node_identity.Map(void){};
    defer visited.deinit(alloc);

    // Queue
    var queue = std.ArrayListUnmanaged(QueueEntry).empty;
    var queue_head: usize = 0;
    defer {
        for (queue.items[queue_head..]) |*entry| entry.deinit(alloc);
        queue.deinit(alloc);
    }

    // Seed with start node
    var start_entry = QueueEntry{
        .key = try alloc.dupe(u8, start_key),
        .depth = 0,
        .total_weight = 1.0,
        .path = null,
    };
    var start_entry_owned = true;
    errdefer if (start_entry_owned) start_entry.deinit(alloc);
    var start_path: ?std.ArrayListUnmanaged([]const u8) = null;
    if (rules.include_paths) {
        start_path = std.ArrayListUnmanaged([]const u8).empty;
        const path_key = try alloc.dupe(u8, start_key);
        start_path.?.append(alloc, path_key) catch |err| {
            alloc.free(path_key);
            return err;
        };
    }
    start_entry.path = start_path;
    try queue.append(alloc, start_entry);
    start_entry_owned = false;

    if (rules.deduplicate) {
        _ = try visited.putIfAbsent(alloc, .{ .table = null, .key = start_key }, {});
    }

    while (queue_head < queue.items.len) {
        // Dequeue from front (index-tracked)
        var current = queue.items[queue_head];
        queue_head += 1;
        defer current.deinit(alloc);

        // Add to results (skip depth 0 = start node)
        if (current.depth > 0) {
            const result = try traversalResultFromQueueEntry(alloc, current);
            var result_owned = true;
            errdefer if (result_owned) freeResult(alloc, result);
            try results.append(alloc, result);
            result_owned = false;

            if (rules.max_results > 0 and results.items.len >= rules.max_results) {
                break;
            }
        }

        // Check max depth
        if (rules.max_depth > 0 and current.depth >= rules.max_depth) continue;

        // Cross-table nodes are expanded by the distributed owner router.
        // Looking them up in this source-table index aliases distinct node
        // namespaces when their keys happen to be equal.
        if (current.target_table != null) continue;

        // Get edges
        const edges = try edge_reader.getEdges(alloc, current.key, rules.direction);
        defer edge_reader.freeEdges(alloc, edges);

        const admitted_edges = if (rules.node_admission) |admission| blk: {
            const edge_mask = try alloc.alloc(bool, edges.len);
            @memset(edge_mask, false);
            errdefer alloc.free(edge_mask);
            var candidate_indexes = std.ArrayListUnmanaged(usize).empty;
            defer candidate_indexes.deinit(alloc);
            var candidate_nodes = std.ArrayListUnmanaged(NodeRef).empty;
            defer candidate_nodes.deinit(alloc);
            try candidate_indexes.ensureTotalCapacity(alloc, edges.len);
            try candidate_nodes.ensureTotalCapacity(alloc, edges.len);
            for (edges, 0..) |edge, edge_index| {
                if (!shouldTraverseEdge(&rules, &edge)) continue;
                const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;
                const target_table = if (std.mem.eql(u8, next_key, edge.target))
                    metadataTargetTable(edge.metadata)
                else
                    null;
                if (rules.deduplicate and visited.contains(.{
                    .table = target_table,
                    .key = next_key,
                })) continue;
                candidate_indexes.appendAssumeCapacity(edge_index);
                candidate_nodes.appendAssumeCapacity(.{
                    .key = next_key,
                    .table = target_table,
                    .external = std.mem.eql(u8, next_key, edge.target) and
                        (admission.external_targets or
                            target_table != null),
                });
            }
            const candidate_mask = try admission.filterAlloc(alloc, candidate_nodes.items);
            defer alloc.free(candidate_mask);
            for (candidate_indexes.items, candidate_mask) |edge_index, allowed| {
                edge_mask[edge_index] = allowed;
            }
            break :blk edge_mask;
        } else null;
        defer if (admitted_edges) |mask| alloc.free(mask);

        for (edges, 0..) |edge, edge_index| {
            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;

            if (admitted_edges) |mask| {
                if (!mask[edge_index]) continue;
            } else {
                if (!shouldTraverseEdge(&rules, &edge)) continue;
            }
            const target_table = if (std.mem.eql(u8, next_key, edge.target))
                metadataTargetTable(edge.metadata)
            else
                null;
            if (rules.deduplicate and !try visited.putIfAbsent(
                alloc,
                .{ .table = target_table, .key = next_key },
                {},
            )) continue;

            // Build path for next node
            var next_path: ?std.ArrayListUnmanaged([]const u8) = null;
            if (rules.include_paths) {
                next_path = std.ArrayListUnmanaged([]const u8).empty;
                errdefer if (next_path) |path| freeQueuePath(alloc, path);
                if (current.path) |cp| {
                    for (cp.items) |path_key| {
                        const owned_key = try alloc.dupe(u8, path_key);
                        next_path.?.append(alloc, owned_key) catch |err| {
                            alloc.free(owned_key);
                            return err;
                        };
                    }
                }
                const owned_key = try alloc.dupe(u8, next_key);
                next_path.?.append(alloc, owned_key) catch |err| {
                    alloc.free(owned_key);
                    return err;
                };
            }

            // The reached node's table comes from the edge that points at it
            // (only meaningful when traversing toward the edge's target).
            var next_target_table: ?[]const u8 = if (target_table) |table|
                try alloc.dupe(u8, table)
            else
                null;
            errdefer if (next_target_table) |tt| alloc.free(tt);

            var next_entry = QueueEntry{
                .key = try alloc.dupe(u8, next_key),
                .depth = current.depth + 1,
                .total_weight = current.total_weight * edge.weight,
                .path = next_path,
                .target_table = next_target_table,
            };
            next_path = null;
            next_target_table = null;
            var next_entry_owned = true;
            errdefer if (next_entry_owned) next_entry.deinit(alloc);
            try queue.append(alloc, next_entry);
            next_entry_owned = false;
        }
    }

    const owned = try alloc.dupe(TraversalResult, results.items);
    results.deinit(alloc);
    return owned;
}

fn freeQueuePath(alloc: Allocator, path: std.ArrayListUnmanaged([]const u8)) void {
    for (path.items) |key| alloc.free(key);
    var owned = path;
    owned.deinit(alloc);
}

fn traversalResultFromQueueEntry(
    alloc: Allocator,
    entry: QueueEntry,
) !TraversalResult {
    const key = try alloc.dupe(u8, entry.key);
    errdefer alloc.free(key);
    const path = if (entry.path) |source| blk: {
        const owned = try alloc.alloc([]const u8, source.items.len);
        var initialized: usize = 0;
        errdefer {
            for (owned[0..initialized]) |item| alloc.free(item);
            alloc.free(owned);
        }
        for (source.items, 0..) |item, i| {
            owned[i] = try alloc.dupe(u8, item);
            initialized += 1;
        }
        break :blk owned;
    } else null;
    errdefer if (path) |items| {
        for (items) |item| alloc.free(item);
        alloc.free(items);
    };
    const target_table = if (entry.target_table) |table|
        try alloc.dupe(u8, table)
    else
        null;
    errdefer if (target_table) |table| alloc.free(table);
    return .{
        .key = key,
        .depth = entry.depth,
        .total_weight = entry.total_weight,
        .path = path,
        .target_table = target_table,
    };
}

/// Admit a traversal start according to the role it plays in this direction.
/// Resolver-produced cross-table targets are identified from incoming edge
/// metadata only after normal local admission rejects the key.
pub fn startNodeAdmitted(
    alloc: Allocator,
    graph_index: *GraphIndex,
    start_key: []const u8,
    direction: EdgeDirection,
    admission: NodeAdmission,
) !bool {
    const Reader = struct {
        graph_index: *GraphIndex,

        pub fn getEdges(self: @This(), a: Allocator, key: []const u8, edge_direction: EdgeDirection) ![]Edge {
            return try self.graph_index.getEdges(a, key, "", edge_direction);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []Edge) void {
            GraphIndex.freeEdges(a, edges);
        }
    };
    return try startNodeAdmittedWithEdgeReader(
        alloc,
        Reader{ .graph_index = graph_index },
        start_key,
        direction,
        admission,
    );
}

pub fn startNodeAdmittedWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    direction: EdgeDirection,
    admission: NodeAdmission,
) !bool {
    const statically_external = admission.external_targets and direction == .in;
    const keys = [_][]const u8{start_key};
    const admitted = try admission.filterKeysAlloc(alloc, &keys, statically_external);
    defer alloc.free(admitted);
    if (admitted[0] or statically_external or direction == .out) return admitted[0];

    const incoming = try edge_reader.getEdges(alloc, start_key, .in);
    defer edge_reader.freeEdges(alloc, incoming);
    for (incoming) |edge| {
        if (std.mem.eql(u8, edge.target, start_key) and
            (admission.external_targets or metadataTargetTable(edge.metadata) != null))
        {
            return true;
        }
    }
    return false;
}

fn shouldTraverseEdge(rules: *const TraversalRules, edge: *const Edge) bool {
    // Weight filter
    if (rules.min_weight > 0 and edge.weight < rules.min_weight) return false;
    if (rules.max_weight > 0 and edge.weight > rules.max_weight) return false;

    // Edge type filter
    if (rules.edge_types.len > 0) {
        for (rules.edge_types) |et| {
            if (std.mem.eql(u8, edge.edge_type, et)) return true;
        }
        return false;
    }
    return true;
}

/// Free traversal results.
fn freeResult(alloc: Allocator, result: TraversalResult) void {
    alloc.free(result.key);
    if (result.path) |path| {
        for (path) |key| alloc.free(key);
        alloc.free(path);
    }
    if (result.target_table) |table| alloc.free(table);
}

pub fn freeResults(alloc: Allocator, results: []const TraversalResult) void {
    for (results) |result| freeResult(alloc, result);
}

/// Free results returned from traverse().
pub fn freeOwnedResults(alloc: Allocator, results: []TraversalResult) void {
    freeResults(alloc, results);
    alloc.free(results);
}

// ============================================================================
// Tests
// ============================================================================

const docstore = @import("../storage/docstore.zig");

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-trav-{s}-{d}\x00", .{ label, ns }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "traversal basic BFS depth 1" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts1");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr1");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "knows", 0.9, 0, 0, "");
    try g.addEdge("A", "C", "knows", 0.8, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{ .max_depth = 1 });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    for (results) |r| {
        try std.testing.expectEqual(@as(u32, 1), r.depth);
    }
}

test "traversal max_depth limiting" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts2");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr2");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    // A -> B -> C -> D (chain)
    try g.addEdge("A", "B", "next", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "next", 1.0, 0, 0, "");
    try g.addEdge("C", "D", "next", 1.0, 0, 0, "");

    // Depth 2: should reach B and C but not D
    const results = try traverse(alloc, &g, "A", .{ .max_depth = 2 });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("B", results[0].key);
    try std.testing.expectEqual(@as(u32, 1), results[0].depth);
    try std.testing.expectEqualStrings("C", results[1].key);
    try std.testing.expectEqual(@as(u32, 2), results[1].depth);
}

test "traversal deduplication" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts3");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr3");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    // Diamond: A -> B, A -> C, B -> D, C -> D
    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try g.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try g.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try g.addEdge("C", "D", "e", 1.0, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{ .max_depth = 3, .deduplicate = true });
    defer freeOwnedResults(alloc, results);

    // D should appear only once (dedup)
    try std.testing.expectEqual(@as(usize, 3), results.len); // B, C, D
}

test "traversal deduplicates by table-scoped node identity" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "table-identity-store");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "table-identity-graph");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "shared", "local", 1.0, 0, 0, "");
    try g.addEdge(
        "A",
        "shared",
        "external",
        1.0,
        0,
        0,
        "{\"target_table\":\"entities\"}",
    );

    const results = try traverse(alloc, &g, "A", .{
        .max_depth = 1,
        .deduplicate = true,
    });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    var local_count: usize = 0;
    var external_count: usize = 0;
    for (results) |result| {
        try std.testing.expectEqualStrings("shared", result.key);
        if (result.target_table) |table| {
            try std.testing.expectEqualStrings("entities", table);
            external_count += 1;
        } else {
            local_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), local_count);
    try std.testing.expectEqual(@as(usize, 1), external_count);
}

test "local traversal does not expand a cross-table node in the source index" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "external-terminal-store");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "external-terminal-graph");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer graph.close();

    try graph.addEdge(
        "A",
        "shared",
        "external",
        1.0,
        0,
        0,
        "{\"target_table\":\"entities\"}",
    );
    try graph.addEdge("shared", "source-only", "local", 1.0, 0, 0, "");

    const results = try traverse(alloc, &graph, "A", .{
        .max_depth = 2,
        .deduplicate = true,
    });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("shared", results[0].key);
    try std.testing.expectEqualStrings("entities", results[0].target_table.?);
}

test "traversal with path tracking" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts4");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr4");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "e", 1.0, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{
        .max_depth = 3,
        .include_paths = true,
    });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);

    // B's path: [A, B]
    const b_path = results[0].path.?;
    try std.testing.expectEqual(@as(usize, 2), b_path.len);
    try std.testing.expectEqualStrings("A", b_path[0]);
    try std.testing.expectEqualStrings("B", b_path[1]);

    // C's path: [A, B, C]
    const c_path = results[1].path.?;
    try std.testing.expectEqual(@as(usize, 3), c_path.len);
    try std.testing.expectEqualStrings("C", c_path[2]);
}

test "traversal weight filter" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts5");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr5");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 0.9, 0, 0, "");
    try g.addEdge("A", "C", "e", 0.3, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{
        .max_depth = 1,
        .min_weight = 0.5,
    });
    defer freeOwnedResults(alloc, results);

    // Only B (weight 0.9) should pass the min_weight filter
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("B", results[0].key);
}
