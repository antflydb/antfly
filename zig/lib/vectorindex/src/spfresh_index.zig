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
const types = @import("types.zig");
const hbc = @import("hbc.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const posting = @import("posting.zig");
const search_types = @import("search_types.zig");
const proto = @import("antfly_vector").proto;
const vec = @import("antfly_vector").vector;

pub const FlatCentroidBlock = struct {
    posting_ids: []u64,
    quantized: proto.RaBitQuantizedVectorSet,

    fn deinit(self: *FlatCentroidBlock, alloc: std.mem.Allocator) void {
        alloc.free(self.posting_ids);
        self.quantized.deinit(alloc);
        self.* = undefined;
    }
};

pub const FlatCentroidDirectory = struct {
    blocks: []FlatCentroidBlock = &.{},
    ref_count: std.atomic.Value(u32) = .init(1),
    root_node_snapshot: u64 = 0,
    node_count_snapshot: u64 = 0,
    publish_generation_snapshot: u64 = 0,
    posting_count: usize = 0,

    pub fn retain(self: *FlatCentroidDirectory) void {
        _ = self.ref_count.fetchAdd(1, .acq_rel);
    }

    pub fn release(self: *FlatCentroidDirectory, alloc: std.mem.Allocator) void {
        if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
        self.deinit(alloc);
        alloc.destroy(self);
    }

    fn deinit(self: *FlatCentroidDirectory, alloc: std.mem.Allocator) void {
        for (self.blocks) |*block| block.deinit(alloc);
        alloc.free(self.blocks);
        self.* = .{};
    }
};

pub const FlatCentroidProbe = struct {
    posting_id: u64,
    distance: f32,
    error_bound: f32,
};

fn lockAtomicMutex(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn isNotFoundGeneric(err: anyerror) bool {
    return err == error.NotFound;
}

fn nowNsI128Fixed() i128 {
    return 0;
}

fn elapsedSinceNsFixed(start: i128) u64 {
    _ = start;
    return 0;
}

fn nowNsU64Fixed() u64 {
    return 0;
}

fn elapsedSinceU64Fixed(start: u64) u64 {
    _ = start;
    return 0;
}

fn savePackedNodeValue(self: anytype, txn: anytype, node: *const types.Node) !void {
    const header = hbc.NodeHeader{
        .is_leaf = node.is_leaf,
        .level = node.level,
        .parent = node.parent,
    };
    const centroid_bytes = std.mem.sliceAsBytes(node.centroid);
    const ids_bytes = if (node.is_leaf) std.mem.sliceAsBytes(node.members) else std.mem.sliceAsBytes(node.children);
    const packed_len = hbc.packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    const packed_value = try self.alloc.alloc(u8, packed_len);
    defer self.alloc.free(packed_value);
    const encoded = try hbc.encodePackedNodeValue(packed_value, header, centroid_bytes, ids_bytes);
    var key_buf: [12]u8 = undefined;
    try self.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node.id, .packed_node), encoded);
}

fn insertFlatProbe(probes: []FlatCentroidProbe, count: *usize, candidate: FlatCentroidProbe) void {
    if (probes.len == 0) return;
    if (count.* < probes.len) {
        probes[count.*] = candidate;
        count.* += 1;
    } else {
        var worst_index: usize = 0;
        var worst_score = probes[0].distance - probes[0].error_bound;
        for (probes[1..], 1..) |probe, i| {
            const score = probe.distance - probe.error_bound;
            if (score > worst_score) {
                worst_score = score;
                worst_index = i;
            }
        }
        if (candidate.distance - candidate.error_bound >= worst_score) return;
        probes[worst_index] = candidate;
    }
}

fn flatProbeLess(_: void, lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    return lhs.distance - lhs.error_bound < rhs.distance - rhs.error_bound;
}

fn publishedRootNodeSnapshot(self: anytype) u64 {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "publishedRootNode")) return self.publishedRootNode();
    return self.metadata.root_node;
}

fn publishedNodeCountSnapshot(self: anytype) u64 {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "publishedNodeCount")) return self.publishedNodeCount();
    return self.metadata.node_count;
}

fn publishedGenerationSnapshot(self: anytype) u64 {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "publishedGeneration")) return self.publishedGeneration();
    return 0;
}

const PublishedSnapshot = struct {
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
};

fn loadStablePublishedSnapshot(self: anytype) PublishedSnapshot {
    const Index = comptime @TypeOf(self.*);
    if (comptime !@hasDecl(Index, "publishedGeneration")) {
        return .{
            .root_node = publishedRootNodeSnapshot(self),
            .node_count = publishedNodeCountSnapshot(self),
            .publish_generation = 0,
        };
    }

    while (true) {
        const generation = publishedGenerationSnapshot(self);
        if ((generation & 1) != 0) {
            std.atomic.spinLoopHint();
            continue;
        }
        const root_node = publishedRootNodeSnapshot(self);
        const node_count = publishedNodeCountSnapshot(self);
        const generation_after = publishedGenerationSnapshot(self);
        if (generation == generation_after and (generation_after & 1) == 0) {
            return .{
                .root_node = root_node,
                .node_count = node_count,
                .publish_generation = generation,
            };
        }
        std.atomic.spinLoopHint();
    }
}

fn loadPublishedNode(self: anytype, txn: anytype, node_id: u64) !types.Node {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "loadSearchNodeFromStorage")) {
        return try self.loadSearchNodeFromStorage(txn, node_id);
    }
    return try self.loadNode(txn, node_id);
}

fn directoryMatches(directory: *const FlatCentroidDirectory, root_node: u64, node_count: u64, publish_generation: u64) bool {
    return directory.root_node_snapshot == root_node and
        directory.node_count_snapshot == node_count and
        directory.publish_generation_snapshot == publish_generation;
}

fn appendFlatCentroidBlock(
    self: anytype,
    blocks: *std.ArrayListUnmanaged(FlatCentroidBlock),
    posting_ids: []const u64,
    centroids: []const f32,
    dims: usize,
) !void {
    if (posting_ids.len == 0) return;
    const zero = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(zero);
    @memset(zero, 0);

    const ids = try self.alloc.dupe(u64, posting_ids);
    errdefer self.alloc.free(ids);
    var quantized = try self.quantizer.quantize(zero, centroids, posting_ids.len);
    errdefer quantized.deinit(self.alloc);
    try blocks.append(self.alloc, .{
        .posting_ids = ids,
        .quantized = quantized,
    });
}

fn buildFlatCentroidDirectory(self: anytype, txn: anytype, root_node: u64, node_count: u64, publish_generation: u64) !FlatCentroidDirectory {
    const dims: usize = @intCast(self.config.dims);
    const block_size = @max(self.config.flat_centroid_block_size, @as(usize, 1));
    var blocks = std.ArrayListUnmanaged(FlatCentroidBlock).empty;
    errdefer {
        for (blocks.items) |*block| block.deinit(self.alloc);
        blocks.deinit(self.alloc);
    }

    var posting_ids = try self.alloc.alloc(u64, block_size);
    defer self.alloc.free(posting_ids);
    var centroids = try self.alloc.alloc(f32, block_size * dims);
    defer self.alloc.free(centroids);
    var pending = std.ArrayListUnmanaged(u64).empty;
    defer pending.deinit(self.alloc);
    try pending.append(self.alloc, root_node);

    var block_count: usize = 0;
    var posting_count: usize = 0;

    var cursor: usize = 0;
    while (cursor < pending.items.len) : (cursor += 1) {
        const node_id = pending.items[cursor];
        var node = loadPublishedNode(self, txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        defer node.deinit(self.alloc);
        if (!node.is_leaf) {
            for (node.children) |child_id| try pending.append(self.alloc, child_id);
            continue;
        }
        if (node.members.len == 0 or node.centroid.len != dims) continue;

        posting_ids[block_count] = node.id;
        @memcpy(centroids[block_count * dims ..][0..dims], node.centroid);
        block_count += 1;
        posting_count += 1;

        if (block_count == block_size) {
            try appendFlatCentroidBlock(self, &blocks, posting_ids[0..block_count], centroids[0 .. block_count * dims], dims);
            block_count = 0;
        }
    }
    if (block_count > 0) {
        try appendFlatCentroidBlock(self, &blocks, posting_ids[0..block_count], centroids[0 .. block_count * dims], dims);
    }

    return .{
        .blocks = try blocks.toOwnedSlice(self.alloc),
        .root_node_snapshot = root_node,
        .node_count_snapshot = node_count,
        .publish_generation_snapshot = publish_generation,
        .posting_count = posting_count,
    };
}

pub fn clearFlatCentroidDirectory(self: anytype) void {
    var stale: ?*FlatCentroidDirectory = null;
    lockAtomicMutex(&self.flat_centroid_mu);
    stale = self.flat_centroid_directory;
    self.flat_centroid_directory = null;
    self.flat_centroid_mu.unlock();
    if (stale) |directory| directory.release(self.alloc);
}

fn acquireFlatCentroidDirectory(self: anytype, txn: anytype) !*FlatCentroidDirectory {
    while (true) {
        const snapshot = loadStablePublishedSnapshot(self);

        var stale: ?*FlatCentroidDirectory = null;
        lockAtomicMutex(&self.flat_centroid_mu);
        if (self.flat_centroid_directory) |directory| {
            if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                directory.retain();
                self.flat_centroid_mu.unlock();
                return directory;
            }
            stale = directory;
            self.flat_centroid_directory = null;
        }
        self.flat_centroid_mu.unlock();
        if (stale) |directory| directory.release(self.alloc);

        const built = try self.alloc.create(FlatCentroidDirectory);
        errdefer self.alloc.destroy(built);
        const Index = comptime @TypeOf(self.*);
        if (comptime @hasDecl(Index, "beginRuntimeSearchTxn")) {
            var build_txn = try self.beginRuntimeSearchTxn();
            defer build_txn.abort();
            built.* = try buildFlatCentroidDirectory(self, &build_txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation);
        } else {
            built.* = try buildFlatCentroidDirectory(self, txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation);
        }
        errdefer built.deinit(self.alloc);

        const current = loadStablePublishedSnapshot(self);
        if (current.root_node != snapshot.root_node or
            current.node_count != snapshot.node_count or
            current.publish_generation != snapshot.publish_generation)
        {
            built.deinit(self.alloc);
            self.alloc.destroy(built);
            continue;
        }

        lockAtomicMutex(&self.flat_centroid_mu);
        if (self.flat_centroid_directory) |directory| {
            if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                directory.retain();
                self.flat_centroid_mu.unlock();
                built.deinit(self.alloc);
                self.alloc.destroy(built);
                return directory;
            }
            stale = directory;
            self.flat_centroid_directory = null;
        } else {
            stale = null;
        }
        built.retain();
        self.flat_centroid_directory = built;
        self.flat_centroid_mu.unlock();
        if (stale) |directory| directory.release(self.alloc);
        return built;
    }
}

pub fn selectFlatRabitqPostings(
    self: anytype,
    txn: anytype,
    query: []const f32,
    limit: usize,
    probes: []FlatCentroidProbe,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !usize {
    if (limit == 0 or probes.len == 0) return 0;
    const probe_limit = @min(limit, probes.len);
    const start = now_fn_u64();
    const directory = try acquireFlatCentroidDirectory(self, txn);
    defer directory.release(self.alloc);
    defer profile.child_expand_ns += elapsed_fn_u64(start);
    var probe_count: usize = 0;

    for (directory.blocks) |*block| {
        const count = block.posting_ids.len;
        try scratch.ensureVectorFetchCapacity(self.alloc, count);
        const distances = scratch.distances[0..count];
        const error_bounds = scratch.error_bounds[0..count];
        try self.quantizer.estimateDistancesWithScratch(&block.quantized, query, distances, error_bounds, &scratch.estimate);
        for (block.posting_ids, 0..) |posting_id, i| {
            insertFlatProbe(probes[0..probe_limit], &probe_count, .{
                .posting_id = posting_id,
                .distance = distances[i],
                .error_bound = error_bounds[i],
            });
        }
    }

    std.mem.sort(FlatCentroidProbe, probes[0..probe_count], {}, flatProbeLess);
    profile.approx_nodes_expanded += @intCast(directory.blocks.len);
    return probe_count;
}

fn recomputeAncestorCentroids(
    self: anytype,
    txn: anytype,
    start_parent_id: u64,
    options: hbc_runtime.BatchInsertOptions,
) !void {
    var parent_id = start_parent_id;
    while (parent_id != 0) {
        var parent = try self.loadNode(txn, parent_id);
        defer parent.deinit(self.alloc);
        try self.recomputeInternalCentroid(txn, &parent);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);
        parent_id = parent.parent;
    }
}

pub fn repairDirtyPostingsTxn(self: anytype, txn: anytype) !posting.PostingMaintenanceResult {
    return try repairDirtyPostingsTxnWithOptions(self, txn, .{});
}

pub fn postingBacklogStatsTxn(self: anytype, txn: anytype) !posting.PostingBacklogStats {
    var result: posting.PostingBacklogStats = .{};

    var node_id: u64 = 1;
    while (node_id <= self.metadata.node_count) : (node_id += 1) {
        var node = self.loadNode(txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) {
                result.skipped_missing += 1;
                continue;
            }
            return err;
        };
        defer node.deinit(self.alloc);

        result.scanned_nodes += 1;
        if (!node.is_leaf) continue;
        result.scanned_postings += 1;

        const state = node.posting_state;
        result.max_mutation_version = @max(result.max_mutation_version, state.mutation_version);
        if (!state.dirty) continue;

        result.dirty_postings += 1;
        if (state.centroid_dirty) {
            result.centroid_dirty_postings += 1;
            result.max_centroid_version_lag = @max(
                result.max_centroid_version_lag,
                state.mutation_version -| state.centroid_version,
            );
        }
        if (state.payload_dirty) {
            result.payload_dirty_postings += 1;
            result.max_payload_version_lag = @max(
                result.max_payload_version_lag,
                state.mutation_version -| state.payload_version,
            );
        }
    }

    return result;
}

pub fn runAutoPostingMaintenanceTxn(self: anytype, txn: anytype) !void {
    const max_postings = self.config.auto_posting_maintenance_max_postings;
    if (max_postings == 0) return;
    _ = try repairDirtyPostingsTxnWithOptions(self, txn, .{ .max_postings = max_postings });
}

fn mergeUnderfullPostingWithNearestSibling(
    self: anytype,
    txn: anytype,
    leaf: *const types.Node,
) !bool {
    if (!leaf.is_leaf or leaf.parent == 0 or leaf.members.len == 0) return false;
    if (leaf.members.len >= self.minLeafOccupancy()) return false;

    var parent = try self.loadNode(txn, leaf.parent);
    defer parent.deinit(self.alloc);
    try parent.ensureUnbacked(self.alloc);

    var best_sibling_id: u64 = 0;
    var best_dist: f32 = std.math.inf(f32);
    for (parent.children) |cid| {
        if (cid == leaf.id) continue;
        var sibling = try self.loadNode(txn, cid);
        defer sibling.deinit(self.alloc);
        if (!sibling.is_leaf) continue;
        if (sibling.members.len + leaf.members.len > self.config.leaf_size) continue;
        const dist = vec.distance(leaf.centroid, sibling.centroid, self.config.metric);
        if (dist < best_dist) {
            best_dist = dist;
            best_sibling_id = cid;
        }
    }
    if (best_sibling_id == 0) return false;

    var sibling = try self.loadNode(txn, best_sibling_id);
    defer sibling.deinit(self.alloc);
    try sibling.ensureUnbacked(self.alloc);

    const merged_len = sibling.members.len + leaf.members.len;
    var merged = try self.alloc.alloc(u64, merged_len);
    errdefer self.alloc.free(merged);
    @memcpy(merged[0..sibling.members.len], sibling.members);
    @memcpy(merged[sibling.members.len..], leaf.members);
    self.alloc.free(sibling.members);
    sibling.members = merged;
    posting.PostingStore.noteMembersChanged(&sibling);
    try posting.PostingStore.recomputeCentroid(self, txn, &sibling);
    if (self.config.use_quantization) {
        const refresh_options: hbc_runtime.BatchInsertOptions = .{};
        try self.refreshQuantizedWithOptions(txn, &sibling, refresh_options);
    }
    posting.PostingStore.notePayloadRefreshed(&sibling);
    const save_options: hbc_runtime.BatchInsertOptions = .{};
    try self.saveNodeWithOptionsMode(txn, &sibling, save_options, false);

    for (leaf.members) |mid| try self.putVecLeaf(txn, mid, best_sibling_id);

    var new_children = try self.alloc.alloc(u64, parent.children.len - 1);
    errdefer self.alloc.free(new_children);
    var wi_child: usize = 0;
    for (parent.children) |cid| {
        if (cid == leaf.id) continue;
        new_children[wi_child] = cid;
        wi_child += 1;
    }
    self.alloc.free(parent.children);
    parent.children = new_children;
    try self.recomputeInternalCentroid(txn, &parent);
    try self.saveNodeWithOptionsMode(txn, &parent, save_options, false);
    try self.deleteNode(txn, leaf.id);
    try self.collapseSingleChildParents(txn, leaf.parent);
    return true;
}

const BoundaryMove = struct {
    vector_id: u64,
    from_index: usize,
    to_index: usize,
};

fn targetedBoundaryReassignParent(
    self: anytype,
    txn: anytype,
    parent_id: u64,
    max_moves: usize,
    min_improvement: f32,
) !usize {
    if (parent_id == 0 or max_moves == 0) return 0;

    var parent = self.loadNode(txn, parent_id) catch |err| {
        if (isNotFoundGeneric(err)) return 0;
        return err;
    };
    defer parent.deinit(self.alloc);
    if (parent.is_leaf or parent.children.len < 2) return 0;

    var leaves = try self.alloc.alloc(types.Node, parent.children.len);
    var initialized: usize = 0;
    defer {
        for (leaves[0..initialized]) |*leaf| leaf.deinit(self.alloc);
        self.alloc.free(leaves);
    }

    for (parent.children) |child_id| {
        var child = self.loadNode(txn, child_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        if (!child.is_leaf) {
            child.deinit(self.alloc);
            return 0;
        }
        try child.ensureUnbacked(self.alloc);
        leaves[initialized] = child;
        initialized += 1;
    }
    if (initialized < 2) return 0;

    const moves_cap = @min(max_moves, @as(usize, @intCast(std.math.maxInt(u32))));
    var moves = try self.alloc.alloc(BoundaryMove, moves_cap);
    defer self.alloc.free(moves);
    const planned_out = try self.alloc.alloc(usize, initialized);
    defer self.alloc.free(planned_out);
    @memset(planned_out, 0);

    const dims: usize = @intCast(self.config.dims);
    const raw_scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(raw_scratch);
    const transformed = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(transformed);

    var move_count: usize = 0;
    const min_source_members = self.minLeafOccupancy();
    for (leaves[0..initialized], 0..) |*source, source_index| {
        if (move_count >= moves.len) break;
        if (source.members.len <= min_source_members) continue;
        for (source.members) |member_id| {
            if (move_count >= moves.len) break;
            if (source.members.len - planned_out[source_index] <= min_source_members) break;

            const raw = try self.getVectorScratch(txn, member_id, raw_scratch);
            _ = self.transformVector(raw, transformed);
            const current_dist = vec.distance(source.centroid, transformed, self.config.metric);

            var best_index = source_index;
            var best_dist = current_dist;
            for (leaves[0..initialized], 0..) |*candidate, candidate_index| {
                if (candidate_index == source_index) continue;
                if (candidate.members.len >= self.config.leaf_size) continue;
                const dist = vec.distance(candidate.centroid, transformed, self.config.metric);
                if (dist + min_improvement < best_dist) {
                    best_dist = dist;
                    best_index = candidate_index;
                }
            }
            if (best_index == source_index) continue;

            moves[move_count] = .{
                .vector_id = member_id,
                .from_index = source_index,
                .to_index = best_index,
            };
            move_count += 1;
            planned_out[source_index] += 1;
        }
    }
    if (move_count == 0) return 0;

    const changed = try self.alloc.alloc(bool, initialized);
    defer self.alloc.free(changed);
    @memset(changed, false);

    var applied: usize = 0;
    for (moves[0..move_count]) |move| {
        if (move.from_index == move.to_index) continue;
        posting.PostingStore.removeMember(self.alloc, &leaves[move.from_index], move.vector_id) catch continue;
        _ = try posting.PostingStore.appendMember(self.alloc, &leaves[move.to_index], move.vector_id);
        try self.putVecLeaf(txn, move.vector_id, leaves[move.to_index].id);
        changed[move.from_index] = true;
        changed[move.to_index] = true;
        applied += 1;
    }
    if (applied == 0) return 0;

    for (leaves[0..initialized], 0..) |*leaf, i| {
        if (!changed[i]) continue;
        if (leaf.members.len == 0) {
            @memset(leaf.centroid, 0);
        } else {
            try posting.PostingStore.recomputeCentroid(self, txn, leaf);
        }
        if (self.config.use_quantization) {
            const refresh_options: hbc_runtime.BatchInsertOptions = .{};
            try self.refreshQuantizedWithOptions(txn, leaf, refresh_options);
        }
        posting.PostingStore.notePayloadRefreshed(leaf);
        const save_options: hbc_runtime.BatchInsertOptions = .{};
        try self.saveNodeWithOptionsMode(txn, leaf, save_options, false);
    }

    try parent.ensureUnbacked(self.alloc);
    try self.recomputeInternalCentroid(txn, &parent);
    const save_options: hbc_runtime.BatchInsertOptions = .{};
    try self.saveNodeWithOptionsMode(txn, &parent, save_options, false);
    return applied;
}

pub fn repairDirtyPostingsTxnWithOptions(
    self: anytype,
    txn: anytype,
    options: posting.PostingMaintenanceOptions,
) !posting.PostingMaintenanceResult {
    var result: posting.PostingMaintenanceResult = .{};
    if (options.max_postings == 0) {
        result.limit_reached = true;
        return result;
    }

    var layout_changes: usize = 0;
    var boundary_moves: usize = 0;

    var node_id: u64 = 1;
    while (node_id <= self.metadata.node_count) : (node_id += 1) {
        var node = self.loadNode(txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) {
                result.skipped_missing += 1;
                continue;
            }
            return err;
        };
        defer node.deinit(self.alloc);

        result.scanned_nodes += 1;
        if (!node.is_leaf) continue;
        result.scanned_postings += 1;

        if (options.rebalance_layout and layout_changes < options.max_layout_changes) {
            if (node.members.len > self.config.leaf_size) {
                const old_parent_id = node.parent;
                const split_options: hbc_runtime.BatchInsertOptions = .{};
                try self.splitLeafWithOptions(txn, &node, split_options);
                result.split_postings += 1;
                layout_changes += 1;
                if (boundary_moves < options.max_boundary_reassignments) {
                    const parent_id = if (old_parent_id == 0) self.metadata.root_node else old_parent_id;
                    const moved = try targetedBoundaryReassignParent(
                        self,
                        txn,
                        parent_id,
                        options.max_boundary_reassignments - boundary_moves,
                        options.boundary_reassignment_min_improvement,
                    );
                    boundary_moves += moved;
                    result.boundary_reassigned_vectors += @intCast(moved);
                }
                continue;
            }

            if (try mergeUnderfullPostingWithNearestSibling(self, txn, &node)) {
                result.merged_postings += 1;
                layout_changes += 1;
                if (boundary_moves < options.max_boundary_reassignments) {
                    const moved = try targetedBoundaryReassignParent(
                        self,
                        txn,
                        node.parent,
                        options.max_boundary_reassignments - boundary_moves,
                        options.boundary_reassignment_min_improvement,
                    );
                    boundary_moves += moved;
                    result.boundary_reassigned_vectors += @intCast(moved);
                }
                continue;
            }
        } else if (options.rebalance_layout and layout_changes >= options.max_layout_changes) {
            result.limit_reached = true;
        }

        if (!node.posting_state.dirty) continue;

        result.dirty_postings += 1;
        if (result.repaired_postings >= options.max_postings) {
            result.limit_reached = true;
            break;
        }

        try node.ensureUnbacked(self.alloc);

        var refreshed_centroid = false;
        var refreshed_payload = false;
        if (node.posting_state.centroid_dirty) {
            try posting.PostingStore.recomputeCentroid(self, txn, &node);
            refreshed_centroid = true;
            result.centroid_refreshed += 1;
        }

        if (node.posting_state.payload_dirty and options.refresh_payloads) {
            if (self.config.use_quantization) {
                const quant_start = nowNsU64Fixed();
                try self.refreshQuantizedWithOptions(txn, &node, .{});
                self.write_profile.refresh_quantized_ns += elapsedSinceU64Fixed(quant_start);
            }
            posting.PostingStore.notePayloadRefreshed(&node);
            refreshed_payload = true;
            result.payload_refreshed += 1;
        }

        if (!node.posting_state.centroid_dirty and !node.posting_state.payload_dirty) {
            node.posting_state.dirty = false;
        }

        if (refreshed_centroid or refreshed_payload) {
            try savePackedNodeValue(self, txn, &node);
        }
        try posting.PostingStore.saveState(self, txn, node.id, node.posting_state);
        try self.cacheNode(&node);

        if (refreshed_centroid and options.refresh_ancestors and node.parent != 0) {
            try recomputeAncestorCentroids(self, txn, node.parent, .{});
            result.ancestor_refresh_roots += 1;
        }

        if (refreshed_centroid or refreshed_payload) {
            result.repaired_postings += 1;
        }
    }

    self.write_profile.posting_maintenance_scanned_nodes += result.scanned_nodes;
    self.write_profile.posting_maintenance_scanned_postings += result.scanned_postings;
    self.write_profile.posting_maintenance_dirty_postings += result.dirty_postings;
    self.write_profile.posting_maintenance_repaired_postings += result.repaired_postings;
    self.write_profile.posting_maintenance_centroid_refreshed += result.centroid_refreshed;
    self.write_profile.posting_maintenance_payload_refreshed += result.payload_refreshed;
    self.write_profile.posting_maintenance_ancestor_refresh_roots += result.ancestor_refresh_roots;
    self.write_profile.posting_maintenance_split_postings += result.split_postings;
    self.write_profile.posting_maintenance_merged_postings += result.merged_postings;
    self.write_profile.posting_maintenance_boundary_reassigned_vectors += result.boundary_reassigned_vectors;

    return result;
}

test "posting backlog stats starts clean" {
    const TestIndex = struct {
        alloc: std.mem.Allocator,
        metadata: hbc.IndexMetadata = .{ .node_count = 0 },
        config: types.HBCConfig = .{},
    };
    var index = TestIndex{ .alloc = std.testing.allocator };
    const txn = {};
    const stats = try postingBacklogStatsTxn(&index, txn);
    try std.testing.expectEqual(@as(u64, 0), stats.scanned_nodes);
    try std.testing.expect(!stats.needsRepair());
}

test "flat centroid directory match includes publish generation" {
    const directory = FlatCentroidDirectory{
        .root_node_snapshot = 11,
        .node_count_snapshot = 42,
        .publish_generation_snapshot = 7,
    };

    try std.testing.expect(directoryMatches(&directory, 11, 42, 7));
    try std.testing.expect(!directoryMatches(&directory, 11, 42, 8));
}
