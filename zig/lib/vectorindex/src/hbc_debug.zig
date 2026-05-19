// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const Allocator = std.mem.Allocator;
const search_mod = @import("search.zig");
const search_runtime = @import("search_runtime.zig");
const search_types = @import("search_types.zig");
const hbc = @import("hbc.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const types = @import("types.zig");
const quantizer_mod = @import("antfly_vector").quantizer;
const vec = @import("antfly_vector").vector;

pub fn debugLeafForVector(self: anytype, vector_id: u64, is_not_found: fn (anyerror) bool) !?u64 {
    var txn = try self.beginReadTxn();
    defer txn.abort();
    return self.getVecLeaf(&txn, vector_id) catch |err| {
        if (is_not_found(err)) return null;
        return err;
    };
}

pub fn debugLeafMembers(self: anytype, alloc: Allocator, leaf_id: u64) ![]u64 {
    var txn = try self.beginReadTxn();
    defer txn.abort();
    var node = try self.loadNode(&txn, leaf_id);
    defer node.deinit(self.alloc);
    if (!node.is_leaf) return error.NotLeaf;
    return alloc.dupe(u64, node.members);
}

pub fn debugScanLeafForVector(self: anytype, vector_id: u64) !?u64 {
    var txn = try self.beginReadTxn();
    defer txn.abort();
    return debugScanLeafForVectorTxn(self, &txn, self.metadata.root_node, vector_id);
}

pub fn debugDumpNodes(self: anytype, alloc: Allocator) ![]search_types.HBCDebugNode {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    var queue = std.ArrayListUnmanaged(u64).empty;
    defer queue.deinit(alloc);
    var out = std.ArrayListUnmanaged(search_types.HBCDebugNode).empty;
    errdefer {
        for (out.items) |*node| node.deinit(alloc);
        out.deinit(alloc);
    }
    var seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen.deinit(alloc);

    try queue.append(alloc, self.metadata.root_node);
    while (queue.items.len > 0) {
        const node_id = queue.pop() orelse unreachable;
        if (node_id == 0) continue;
        if (seen.contains(node_id)) continue;
        try seen.put(alloc, node_id, {});

        var node = try self.loadNode(&txn, node_id);
        defer node.deinit(self.alloc);

        try out.append(alloc, .{
            .id = node.id,
            .is_leaf = node.is_leaf,
            .parent = node.parent,
            .level = node.level,
            .children = try alloc.dupe(u64, node.children),
            .members = try alloc.dupe(u64, node.members),
        });

        for (node.children) |child_id| {
            try queue.append(alloc, child_id);
        }
    }

    std.mem.sort(search_types.HBCDebugNode, out.items, {}, struct {
        fn lessThan(_: void, a: search_types.HBCDebugNode, b: search_types.HBCDebugNode) bool {
            return a.id < b.id;
        }
    }.lessThan);

    return out.toOwnedSlice(alloc);
}

pub fn debugScoreLeaf(self: anytype, alloc: Allocator, leaf_id: u64, query: []const f32) ![]search_types.DebugLeafScore {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    var leaf = try self.loadNode(&txn, leaf_id);
    defer leaf.deinit(self.alloc);
    return debugScoreLoadedLeaf(self, alloc, &txn, &leaf, query, false);
}

pub fn debugScoreLeafFreshQuantized(self: anytype, alloc: Allocator, leaf_id: u64, query: []const f32) ![]search_types.DebugLeafScore {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    var leaf = try self.loadNode(&txn, leaf_id);
    defer leaf.deinit(self.alloc);
    return debugScoreLoadedLeaf(self, alloc, &txn, &leaf, query, true);
}

pub fn debugLeafCentroidL2Error(self: anytype, alloc: Allocator, leaf_id: u64) !f32 {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    var leaf = try self.loadNode(&txn, leaf_id);
    defer leaf.deinit(self.alloc);
    if (!leaf.is_leaf) return error.NotLeaf;
    if (leaf.members.len == 0) return 0;

    const centroid = try alloc.alloc(f32, self.config.dims);
    defer alloc.free(centroid);
    @memset(centroid, 0);

    const raw_scratch = try alloc.alloc(f32, self.config.dims);
    defer alloc.free(raw_scratch);
    const transformed = try alloc.alloc(f32, self.config.dims);
    defer alloc.free(transformed);

    for (leaf.members) |member_id| {
        const v = try self.getVectorScratch(&txn, member_id, raw_scratch);
        _ = self.transformVector(v, transformed);
        vec.add(centroid, transformed);
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(leaf.members.len)), centroid);
    return vec.l2SquaredDistance(centroid, leaf.centroid);
}

pub fn debugLeafCentroid(self: anytype, alloc: Allocator, leaf_id: u64) ![]f32 {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    var leaf = try self.loadNode(&txn, leaf_id);
    defer leaf.deinit(self.alloc);
    if (!leaf.is_leaf) return error.NotLeaf;
    return try alloc.dupe(f32, leaf.centroid);
}

pub fn debugRootChildDistances(self: anytype, alloc: Allocator, query: []const f32) ![]search_types.DebugNodeDistance {
    return debugChildDistances(self, alloc, self.metadata.root_node, query);
}

pub fn debugFindLeafForQuery(self: anytype, query: []const f32, allow_quantized: bool) !u64 {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    const transformed_query = try self.alloc.alloc(f32, self.config.dims);
    defer self.alloc.free(transformed_query);
    _ = self.transformVector(query, transformed_query);
    return self.findLeafWithOptions(&txn, self.metadata.root_node, transformed_query, allow_quantized);
}

pub fn debugChildDistances(self: anytype, alloc: Allocator, node_id: u64, query: []const f32) ![]search_types.DebugNodeDistance {
    var txn = try self.beginReadTxn();
    defer txn.abort();

    const node = try self.getNodePtr(&txn, node_id);
    if (node.is_leaf) return error.NotInternal;

    // Copy children to local storage before further getNodePtr calls
    // which may trigger clearNodeCache and free the backing buffer.
    const child_ids = try alloc.dupe(u64, node.children);
    defer alloc.free(child_ids);

    const transformed_query = try alloc.alloc(f32, self.config.dims);
    defer alloc.free(transformed_query);
    _ = self.transformVector(query, transformed_query);

    const query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(transformed_query, transformed_query),
        .cosine => vec.norm(transformed_query),
        .inner_product => 0,
    };

    var out = try alloc.alloc(search_types.DebugNodeDistance, child_ids.len);
    for (child_ids, 0..) |child_id, i| {
        const child = try self.getNodePtr(&txn, child_id);
        out[i] = .{
            .node_id = child_id,
            .distance = vec.distanceToQuery(transformed_query, query_measure, child.centroid, self.config.metric),
        };
    }
    return out;
}

fn debugScoreLoadedLeaf(self: anytype, alloc: Allocator, txn: anytype, leaf: *types.Node, query: []const f32, force_fresh_quantized: bool) ![]search_types.DebugLeafScore {
    if (!leaf.is_leaf) return error.NotLeaf;

    const count = leaf.members.len;
    var scores = try alloc.alloc(search_types.DebugLeafScore, count);
    errdefer alloc.free(scores);

    const transformed_query = try alloc.alloc(f32, self.config.dims);
    defer alloc.free(transformed_query);
    const raw_scratch = try alloc.alloc(f32, self.config.dims);
    defer alloc.free(raw_scratch);
    _ = self.transformVector(query, transformed_query);

    const transformed_query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(transformed_query),
        .inner_product => 0,
    };
    const exact_query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(query),
        .inner_product => 0,
    };

    const leaf_uses_nonquantized_payload = leaf.parent == 0;
    if (leaf.members.len > 0 and (force_fresh_quantized or try self.getQuantized(txn, leaf.id, leaf_uses_nonquantized_payload, leaf.members.len) != null)) {
        const approx_distances = try alloc.alloc(f32, count);
        defer alloc.free(approx_distances);
        const error_bounds = try alloc.alloc(f32, count);
        defer alloc.free(error_bounds);
        var estimate = try quantizer_mod.RaBitQuantizer.EstimateScratch.init(alloc, self.config.dims);
        defer estimate.deinit(alloc);

        var owned_fresh: ?hbc_runtime.QuantizedSet = null;
        defer if (owned_fresh) |*set| set.deinit(self.alloc);

        const quantized: *const hbc_runtime.QuantizedSet = if (force_fresh_quantized) blk: {
            var vectors = vec.Set{
                .dims = self.config.dims,
                .count = count,
                .data = try alloc.alloc(f32, self.config.dims * count),
            };
            defer alloc.free(vectors.data);
            for (leaf.members, 0..) |member_id, i| {
                const member_vec = try self.getVectorScratch(txn, member_id, raw_scratch);
                const transformed = vectors.at(i);
                _ = self.transformVector(member_vec, transformed);
            }
            if (leaf_uses_nonquantized_payload) {
                owned_fresh = .{ .nonquant = .{
                    .vectors = .{
                        .dims = @intCast(self.config.dims),
                        .count = @intCast(count),
                        .data = try self.alloc.dupe(f32, vectors.data),
                    },
                } };
            } else {
                owned_fresh = .{ .rabit = try self.quantizer.quantize(leaf.centroid, vectors.data, count) };
            }
            break :blk &owned_fresh.?;
        } else (try self.getQuantized(txn, leaf.id, leaf_uses_nonquantized_payload, leaf.members.len)).?;

        try self.estimateQuantizedDistances(quantized, transformed_query, transformed_query_measure, approx_distances, error_bounds, &estimate);

        for (leaf.members, 0..) |member_id, i| {
            const member_vec = try self.getVectorScratch(txn, member_id, raw_scratch);
            scores[i] = .{
                .vector_id = member_id,
                .approx_distance = approx_distances[i],
                .error_bound = error_bounds[i],
                .exact_distance = search_runtime.exactDistanceToStoredVector(self.config, query, exact_query_measure, member_vec),
            };
        }
    } else {
        for (leaf.members, 0..) |member_id, i| {
            const member_vec = try self.getVectorScratch(txn, member_id, raw_scratch);
            const dist = search_runtime.exactDistanceToStoredVector(self.config, query, exact_query_measure, member_vec);
            scores[i] = .{
                .vector_id = member_id,
                .approx_distance = dist,
                .error_bound = 0,
                .exact_distance = dist,
            };
        }
    }

    search_mod.sortDebugLeafScores(scores);
    return scores;
}

fn debugScanLeafForVectorTxn(self: anytype, txn: anytype, node_id: u64, vector_id: u64) !?u64 {
    var node = try self.loadNode(txn, node_id);
    defer node.deinit(self.alloc);
    if (node.is_leaf) {
        for (node.members) |member_id| {
            if (member_id == vector_id) return node.id;
        }
        return null;
    }
    for (node.children) |child_id| {
        if (try debugScanLeafForVectorTxn(self, txn, child_id, vector_id)) |found| return found;
    }
    return null;
}
