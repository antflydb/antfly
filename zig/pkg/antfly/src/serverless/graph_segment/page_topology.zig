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

//! Selected type ranges become computation-local dense ordinals. Neither the
//! persisted graph nor unselected endpoint strings are materialized. Per-type
//! connectivity digests use the numerical layer's existing canonical contract.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tree = @import("page_tree.zig");
const graph = @import("page_graph.zig");
const keys = @import("page_keys.zig");
const data = @import("topology_data.zig");
const Sha256 = std.crypto.hash.sha2.Sha256;

const Kind = struct { name: []u8, count: u64 };

fn addKind(alloc: Allocator, store: tree.Store, root: graph.Root, kinds: *std.ArrayListUnmanaged(Kind), name: []const u8, edge_count: *usize, max_edges: usize) !void {
    // Both discovery paths supply sorted unique kinds. Searching all previous
    // kinds here would make wide-type preparation quadratic.
    const count = try graph.topologyEdgeCount(alloc, store, root, name);
    if (count == 0) return;
    const next = std.math.add(usize, edge_count.*, std.math.cast(usize, count) orelse return error.GraphMetricBuildBudgetExceeded) catch return error.GraphMetricBuildBudgetExceeded;
    if (next > max_edges or next > std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
    const owned = try alloc.dupe(u8, name);
    errdefer alloc.free(owned);
    try kinds.append(alloc, .{ .name = owned, .count = count });
    edge_count.* = next;
}

fn intern(alloc: Allocator, nodes: *std.StringArrayHashMapUnmanaged([32]u8), name: []const u8, limit: usize) !u32 {
    if (nodes.getIndex(name)) |index| return @intCast(index);
    if (nodes.count() >= limit or nodes.count() >= std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
    const owned = try alloc.dupe(u8, name);
    errdefer alloc.free(owned);
    var digest: [32]u8 = undefined;
    Sha256.hash(name, &digest, .{});
    const index: u32 = @intCast(nodes.count());
    try nodes.put(alloc, owned, digest);
    return index;
}

/// null selects every local edge type; a non-null empty list selects none.
/// The caller supplies its operation-local admitted allocator and store budget.
pub fn readAlloc(alloc: Allocator, store: tree.Store, root: graph.Root, requested: ?[]const []const u8, max_nodes: usize, max_edges: usize) !data.Topology {
    try store.check(store.ptr);
    var kinds: std.ArrayListUnmanaged(Kind) = .empty;
    var edge_count: usize = 0;
    defer {
        for (kinds.items) |kind| alloc.free(kind.name);
        kinds.deinit(alloc);
    }
    if (requested) |names| {
        const ordered = try alloc.dupe([]const u8, names);
        defer alloc.free(ordered);
        std.mem.sort([]const u8, ordered, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        for (ordered, 0..) |name, i| {
            if (i != 0 and std.mem.eql(u8, ordered[i - 1], name)) continue;
            try addKind(alloc, store, root, &kinds, name, &edge_count, max_edges);
        }
    } else {
        var prefix = try keys.Key.topology(alloc, null);
        defer prefix.deinit(alloc);
        const end = try prefix.successorAlloc(alloc);
        defer alloc.free(end);
        var lower = try alloc.dupe(u8, prefix.bytes.items);
        defer alloc.free(lower);
        while (true) {
            var cursor = try tree.Cursor.init(alloc, store, root.page, lower, end);
            defer cursor.deinit();
            const record = try cursor.next() orelse break;
            const scratch = try alloc.alloc(u8, record.key.len);
            defer alloc.free(scratch);
            const decoded = try keys.decode(record.key, scratch);
            if (!decoded.topology) return error.InvalidGraphPageKey;
            const kind = decoded.edge.?.kind;
            try addKind(alloc, store, root, &kinds, kind, &edge_count, max_edges);
            var next_prefix = try keys.Key.topology(alloc, kind);
            defer next_prefix.deinit(alloc);
            const next = try next_prefix.successorAlloc(alloc);
            alloc.free(lower);
            lower = next;
        }
    }
    const edges = try alloc.alloc(data.Edge, edge_count);
    errdefer alloc.free(edges);
    const offsets = try alloc.alloc(u32, kinds.items.len + 1);
    errdefer alloc.free(offsets);
    const checksums = try alloc.alloc([32]u8, kinds.items.len);
    errdefer alloc.free(checksums);
    var nodes: std.StringArrayHashMapUnmanaged([32]u8) = .empty;
    defer {
        for (nodes.keys()) |node| alloc.free(node);
        nodes.deinit(alloc);
    }
    var edge_index: usize = 0;
    for (kinds.items, 0..) |kind, type_index| {
        offsets[type_index] = @intCast(edge_index);
        var hash = Sha256.init(.{});
        hash.update("antfly:unweighted-type:v1");
        var value: [8]u8 = undefined;
        std.mem.writeInt(u64, &value, kind.name.len, .little);
        hash.update(&value);
        hash.update(kind.name);
        std.mem.writeInt(u64, &value, kind.count, .little);
        hash.update(&value);
        var cursor = try graph.Cursor.topology(alloc, store, root, kind.name);
        defer cursor.deinit();
        var actual: u64 = 0;
        while (try cursor.next()) |edge| {
            if (edge_index >= edges.len or actual >= kind.count or !std.mem.eql(u8, edge.kind, kind.name)) return error.InvalidGraphRoot;
            const source = try intern(alloc, &nodes, edge.source, max_nodes);
            const target = try intern(alloc, &nodes, edge.target, max_nodes);
            edges[edge_index] = .{ .source = source, .target = target };
            hash.update(&nodes.values()[source]);
            hash.update(&nodes.values()[target]);
            edge_index += 1;
            actual += 1;
        }
        if (actual != kind.count) return error.InvalidGraphRoot;
        hash.final(&checksums[type_index]);
    }
    offsets[kinds.items.len] = @intCast(edge_index);
    const order = try alloc.alloc(u32, nodes.count());
    defer alloc.free(order);
    const mapping = try alloc.alloc(u32, nodes.count());
    defer alloc.free(mapping);
    for (order, 0..) |*index, i| index.* = @intCast(i);
    std.mem.sort(u32, order, nodes.keys(), struct {
        fn less(names: []const []const u8, a: u32, b: u32) bool {
            return std.mem.order(u8, names[a], names[b]) == .lt;
        }
    }.less);
    var byte_count: usize = 0;
    for (nodes.keys()) |node| byte_count = try std.math.add(usize, byte_count, node.len);
    for (kinds.items) |kind| byte_count = try std.math.add(usize, byte_count, kind.name.len);
    const strings = try alloc.alloc(u8, byte_count);
    errdefer alloc.free(strings);
    const node_ids = try alloc.alloc([]const u8, nodes.count());
    errdefer alloc.free(node_ids);
    const edge_types = try alloc.alloc([]const u8, kinds.items.len);
    errdefer alloc.free(edge_types);
    var position: usize = 0;
    for (order, 0..) |old, index| {
        if (index % 1024 == 0) try store.check(store.ptr);
        const name = nodes.keys()[old];
        @memcpy(strings[position..][0..name.len], name);
        node_ids[index] = strings[position..][0..name.len];
        position += name.len;
        mapping[old] = @intCast(index);
    }
    for (kinds.items, edge_types) |kind, *name| {
        @memcpy(strings[position..][0..kind.name.len], kind.name);
        name.* = strings[position..][0..kind.name.len];
        position += kind.name.len;
    }
    for (edges, 0..) |*edge, i| {
        if (i % 1024 == 0) try store.check(store.ptr);
        edge.source = mapping[edge.source];
        edge.target = mapping[edge.target];
    }
    return .{
        .node_ids = node_ids,
        .edge_types = edge_types,
        .string_bytes = strings,
        .edge_type_offsets = offsets,
        .edges = edges,
        .source_node_count = std.math.cast(usize, root.nodes) orelse return error.GraphMetricBuildBudgetExceeded,
        .source_edge_count = std.math.cast(usize, root.edges) orelse return error.GraphMetricBuildBudgetExceeded,
        .type_checksums = checksums,
        .retained_bytes = strings.len + (node_ids.len + edge_types.len) * @sizeOf([]const u8) + edges.len * @sizeOf(data.Edge) + offsets.len * 4 + checksums.len * 32,
    };
}

test "serverless paged topology selects local connectivity with computation-local ordinals" {
    const alloc = std.testing.allocator;
    var backing: tree.testing.MemoryStore = .{ .alloc = alloc };
    defer backing.deinit();
    const edges = [_]keys.Edge{
        .{ .source = "z", .target = "b", .kind = "link", .weight = 2 },
        .{ .source = "z", .target = "b", .kind = "link", .weight = 1 },
        .{ .source = "z", .target = "foreign", .kind = "link", .table = "remote" },
        .{ .source = "z", .target = "unrelated", .kind = "other" },
    };
    var plan = try graph.plan(alloc, backing.store(), .{}, &.{.{ .id = "z", .edges = &edges }});
    defer plan.deinit();
    const root = try plan.publish(backing.store(), .{});
    var selected = try readAlloc(alloc, backing.store(), root, &.{"link"}, 2, 2);
    defer selected.deinit(alloc);
    try std.testing.expectEqual(2, selected.node_ids.len);
    try std.testing.expectEqualStrings("b", selected.node_ids[0]);
    try std.testing.expectEqualStrings("z", selected.node_ids[1]);
    try std.testing.expectEqualSlices(data.Edge, &.{ .{ .source = 1, .target = 0 }, .{ .source = 1, .target = 0 } }, selected.edges);
    try std.testing.expectEqual(4, selected.source_edge_count);
    try std.testing.expectEqual(3, selected.source_node_count);
    var duplicate_filter = try readAlloc(alloc, backing.store(), root, &.{ "missing", "link", "link" }, 2, 2);
    defer duplicate_filter.deinit(alloc);
    try std.testing.expectEqualSlices(data.Edge, selected.edges, duplicate_filter.edges);
    try std.testing.expectEqualSlices([32]u8, selected.type_checksums, duplicate_filter.type_checksums);
    const reads = backing.reads;
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, readAlloc(alloc, backing.store(), root, null, 0, 0));
    try std.testing.expectEqual(2, backing.reads - reads); // first kind only, no endpoint scan
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, readAlloc(alloc, backing.store(), root, null, 2, 2));
    var empty = try readAlloc(alloc, backing.store(), root, &.{}, 0, 0);
    defer empty.deinit(alloc);
    try std.testing.expectEqual(0, empty.edges.len);
}

fn allocationExercise(alloc: Allocator) !void {
    var backing: tree.testing.MemoryStore = .{ .alloc = std.testing.allocator };
    defer backing.deinit();
    const edges = [_]keys.Edge{.{ .source = "a", .target = "b", .kind = "link" }};
    var plan = try graph.plan(std.testing.allocator, backing.store(), .{}, &.{.{ .id = "a", .edges = &edges }});
    defer plan.deinit();
    const root = try plan.publish(backing.store(), .{});
    var topology = try readAlloc(alloc, backing.store(), root, null, 2, 1);
    defer topology.deinit(alloc);
    var selected = try readAlloc(alloc, backing.store(), root, &.{ "missing", "link", "link" }, 2, 1);
    defer selected.deinit(alloc);
}

test "serverless paged topology releases every failed preparation allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
}
