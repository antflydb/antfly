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

const hilbert_coord_stack_capacity = 2048;
const flat_centroid_block_metadata_stack_capacity = 256;
const flat_centroid_query_probe_stack_capacity = 512;
const flat_centroid_zero_stack_capacity = 4096;
const flat_centroid_coarse_scratch_stack_capacity = 8192;
const boundary_reassignment_vector_scratch_stack_capacity = 8192;

pub const FlatCentroidBlock = struct {
    posting_ids: []u64,
    parents: []u64,
    levels: []u16,
    states: []types.PostingState,
    posting_offset: usize = 0,
    centroid: []f32,
    radius: f32 = 0,
    radii: []f32,
    centroids: []f32,
    centroid_measures: []f32,
    quantized: proto.RaBitQuantizedVectorSet,

    fn deinit(self: *FlatCentroidBlock, alloc: std.mem.Allocator) void {
        alloc.free(self.posting_ids);
        alloc.free(self.parents);
        alloc.free(self.levels);
        alloc.free(self.states);
        alloc.free(self.centroid);
        alloc.free(self.radii);
        alloc.free(self.centroids);
        alloc.free(self.centroid_measures);
        self.quantized.deinit(alloc);
        self.* = undefined;
    }
};

pub const FlatCentroidDirectory = struct {
    blocks: []FlatCentroidBlock = &.{},
    coarse_quantized: ?proto.RaBitQuantizedVectorSet = null,
    posting_quantized: ?proto.RaBitQuantizedVectorSet = null,
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
        if (self.coarse_quantized) |*coarse| coarse.deinit(alloc);
        if (self.posting_quantized) |*posting_q| posting_q.deinit(alloc);
        self.* = .{};
    }
};

const FlatCentroidBlocksVectorSource = struct {
    blocks: []const FlatCentroidBlock,
    dims: usize,
    total_count: usize,

    pub fn count(self: FlatCentroidBlocksVectorSource) usize {
        return self.total_count;
    }

    fn vectorInBlock(self: FlatCentroidBlocksVectorSource, block: *const FlatCentroidBlock, index: usize) []const f32 {
        const entry_index = index - block.posting_offset;
        return block.centroids[entry_index * self.dims ..][0..self.dims];
    }

    pub fn vectorAt(self: FlatCentroidBlocksVectorSource, index: usize) []const f32 {
        if (self.blocks.len != 0) {
            const first_block_size = self.blocks[0].posting_ids.len;
            if (first_block_size != 0) {
                const block_index = index / first_block_size;
                if (block_index < self.blocks.len) {
                    const block = &self.blocks[block_index];
                    const block_start = block.posting_offset;
                    const block_end = block_start + block.posting_ids.len;
                    if (index >= block_start and index < block_end) return self.vectorInBlock(block, index);
                }
            }
        }

        var lo: usize = 0;
        var hi: usize = self.blocks.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const block = &self.blocks[mid];
            const block_start = block.posting_offset;
            const block_end = block_start + block.posting_ids.len;
            if (index < block_start) {
                hi = mid;
            } else if (index >= block_end) {
                lo = mid + 1;
            } else {
                return self.vectorInBlock(block, index);
            }
        }
        unreachable;
    }
};

pub const FlatCentroidProbe = struct {
    posting_id: u64,
    parent: u64 = 0,
    level: u16 = 0,
    state: types.PostingState = .{},
    block_index: usize = 0,
    entry_index: usize = 0,
    distance: f32,
    error_bound: f32,
};

const FlatCentroidEntry = struct {
    posting_id: u64,
    parent: u64,
    level: u16,
    state: types.PostingState,
    bounds_radius: f32 = 0,
    centroid: []f32,
    sort_key: []u8 = &.{},
    sort_key_storage: []u8 = &.{},
};

const FlatCentroidEntrySortContext = struct {
    dim: usize,
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

fn centroidDirectoryFlagsFromPostingState(state: types.PostingState) u8 {
    var flags: u8 = 0;
    if (state.dirty) flags |= posting.CentroidDirectoryFormat.dirty_flag;
    if (state.centroid_dirty) flags |= posting.CentroidDirectoryFormat.centroid_dirty_flag;
    if (state.payload_dirty) flags |= posting.CentroidDirectoryFormat.payload_dirty_flag;
    return flags;
}

fn postingStateFromCentroidDirectoryRecord(record: *const posting.OwnedCentroidDirectoryRecord) types.PostingState {
    return .{
        .mutation_version = record.mutation_version,
        .centroid_version = record.generation,
        .payload_version = record.payload_version,
        .dirty = (record.flags & posting.CentroidDirectoryFormat.dirty_flag) != 0,
        .centroid_dirty = (record.flags & posting.CentroidDirectoryFormat.centroid_dirty_flag) != 0,
        .payload_dirty = (record.flags & posting.CentroidDirectoryFormat.payload_dirty_flag) != 0,
    };
}

fn postingStateForCentroidDirectoryRecord(self: anytype, txn: anytype, record: *const posting.OwnedCentroidDirectoryRecord) !types.PostingState {
    return posting.PostingStore.loadState(self, txn, record.posting_id, isNotFoundGeneric) catch |err| {
        if (isNotFoundGeneric(err)) return postingStateFromCentroidDirectoryRecord(record);
        return err;
    };
}

const PostingStateLookup = struct {
    states: std.AutoHashMapUnmanaged(u64, types.PostingState) = .empty,
    loaded_by_scan: bool = false,

    fn init(self: anytype, txn: anytype) !PostingStateLookup {
        var lookup = PostingStateLookup{};
        if (comptime posting.PostingStore.canLoadStatesByScan(@TypeOf(self), @TypeOf(txn))) {
            const records = posting.PostingStore.loadStatesByScan(self, txn) catch |err| switch (err) {
                error.Unsupported => return lookup,
                else => return err,
            };
            defer self.alloc.free(records);
            lookup.loaded_by_scan = true;
            try lookup.states.ensureTotalCapacity(self.alloc, @intCast(records.len));
            for (records) |record| lookup.states.putAssumeCapacity(record.posting_id, record.state);
        }
        return lookup;
    }

    fn deinit(self: *PostingStateLookup, alloc: std.mem.Allocator) void {
        self.states.deinit(alloc);
    }

    fn stateForRecord(
        self: *const PostingStateLookup,
        index: anytype,
        txn: anytype,
        record: *const posting.OwnedCentroidDirectoryRecord,
    ) !types.PostingState {
        if (self.loaded_by_scan) return self.states.get(record.posting_id) orelse .{};
        return try postingStateForCentroidDirectoryRecord(index, txn, record);
    }
};

fn savePackedNodeValue(self: anytype, txn: anytype, node: *const types.Node) !void {
    const header = hbc.NodeHeader{
        .is_leaf = node.is_leaf,
        .level = node.level,
        .parent = node.parent,
    };
    const centroid_bytes = std.mem.sliceAsBytes(node.centroid);
    const ids_bytes = if (node.is_leaf)
        if (self.config.posting_storage_mode == .base_delta) &.{} else std.mem.sliceAsBytes(node.members)
    else
        std.mem.sliceAsBytes(node.children);
    const packed_len = hbc.packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    const packed_value = try self.alloc.alloc(u8, packed_len);
    defer self.alloc.free(packed_value);
    const encoded = try hbc.encodePackedNodeValue(packed_value, header, centroid_bytes, ids_bytes);
    var key_buf: [12]u8 = undefined;
    try self.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node.id, .packed_node), encoded);
}

fn flatProbeLowerBound(probe: FlatCentroidProbe) f32 {
    return probe.distance - probe.error_bound;
}

const FlatProbeCollector = struct {
    probes: []FlatCentroidProbe,
    count: usize = 0,
    heap_ready: bool = false,

    fn init(probes: []FlatCentroidProbe) FlatProbeCollector {
        return .{ .probes = probes };
    }

    fn insert(self: *FlatProbeCollector, candidate: FlatCentroidProbe) void {
        if (self.probes.len == 0) return;
        if (self.count < self.probes.len) {
            self.probes[self.count] = candidate;
            self.count += 1;
            if (self.count == self.probes.len) self.buildHeap();
            return;
        }

        if (!self.heap_ready) self.buildHeap();
        if (flatProbeLowerBound(candidate) >= flatProbeLowerBound(self.probes[0])) return;
        self.probes[0] = candidate;
        self.siftDown(0);
    }

    fn items(self: *const FlatProbeCollector) []FlatCentroidProbe {
        return self.probes[0..self.count];
    }

    fn wouldRejectLowerBound(self: *FlatProbeCollector, lower_bound: f32) bool {
        if (self.probes.len == 0) return true;
        if (self.count < self.probes.len) return false;
        if (!self.heap_ready) self.buildHeap();
        return lower_bound >= flatProbeLowerBound(self.probes[0]);
    }

    fn buildHeap(self: *FlatProbeCollector) void {
        std.debug.assert(self.count == self.probes.len);
        var i = self.count / 2;
        while (i > 0) {
            i -= 1;
            self.siftDown(i);
        }
        self.heap_ready = true;
    }

    fn siftDown(self: *FlatProbeCollector, start_index: usize) void {
        var index = start_index;
        while (true) {
            const left = index * 2 + 1;
            if (left >= self.count) break;
            const right = left + 1;
            var largest = left;
            if (right < self.count and flatProbeLowerBound(self.probes[right]) > flatProbeLowerBound(self.probes[left])) {
                largest = right;
            }
            if (flatProbeLowerBound(self.probes[index]) >= flatProbeLowerBound(self.probes[largest])) break;
            std.mem.swap(FlatCentroidProbe, &self.probes[index], &self.probes[largest]);
            index = largest;
        }
    }
};

fn flatProbeUpperBound(probe: FlatCentroidProbe) f32 {
    return probe.distance + probe.error_bound;
}

fn flatProbeLess(_: void, lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    return flatProbeLowerBound(lhs) < flatProbeLowerBound(rhs);
}

fn centroidBlockProbeLess(_: void, lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    const lhs_lower = flatProbeLowerBound(lhs);
    const rhs_lower = flatProbeLowerBound(rhs);
    if (lhs_lower == rhs_lower) return lhs.distance < rhs.distance;
    return lhs_lower < rhs_lower;
}

fn centroidBlockProbeWorse(lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    return centroidBlockProbeLess({}, rhs, lhs);
}

const CentroidBlockProbeCollector = struct {
    probes: []FlatCentroidProbe,
    count: usize = 0,
    heap_ready: bool = false,

    fn init(probes: []FlatCentroidProbe) CentroidBlockProbeCollector {
        return .{ .probes = probes };
    }

    fn insert(self: *CentroidBlockProbeCollector, candidate: FlatCentroidProbe) void {
        if (self.probes.len == 0) return;
        if (self.count < self.probes.len) {
            self.probes[self.count] = candidate;
            self.count += 1;
            if (self.count == self.probes.len) self.buildHeap();
            return;
        }

        if (!self.heap_ready) self.buildHeap();
        if (!centroidBlockProbeWorse(self.probes[0], candidate)) return;
        self.probes[0] = candidate;
        self.siftDown(0);
    }

    fn items(self: *const CentroidBlockProbeCollector) []FlatCentroidProbe {
        return self.probes[0..self.count];
    }

    fn buildHeap(self: *CentroidBlockProbeCollector) void {
        std.debug.assert(self.count == self.probes.len);
        var i = self.count / 2;
        while (i > 0) {
            i -= 1;
            self.siftDown(i);
        }
        self.heap_ready = true;
    }

    fn siftDown(self: *CentroidBlockProbeCollector, start_index: usize) void {
        var index = start_index;
        while (true) {
            const left = index * 2 + 1;
            if (left >= self.count) break;
            const right = left + 1;
            var worst = left;
            if (right < self.count and centroidBlockProbeWorse(self.probes[right], self.probes[left])) {
                worst = right;
            }
            if (!centroidBlockProbeWorse(self.probes[worst], self.probes[index])) break;
            std.mem.swap(FlatCentroidProbe, &self.probes[index], &self.probes[worst]);
            index = worst;
        }
    }
};

fn centroidBlockProbeErrorBound(metric: vec.DistanceMetric, distance: f32, radius: f32) f32 {
    if (radius <= 0) return 0;
    return switch (metric) {
        .l2_squared => radius + 2.0 * @sqrt(@max(distance, 0) * radius),
        .cosine => radius,
        .inner_product => 0,
    };
}

fn leafBoundsRadiusFromStorage(self: anytype, txn: anytype, node: *const types.Node) !f32 {
    if (!node.is_leaf) return error.ExpectedLeaf;
    if (node.members.len == 0 or node.centroid.len == 0) return 0;
    const dims: usize = @intCast(self.metadata.dims);
    const vectors = try self.alloc.alloc(f32, node.members.len * dims);
    defer self.alloc.free(vectors);
    try posting.PostingStore.loadTransformedVectorsForQuantizedRefresh(self, txn, node, vectors, .{});
    var radius: f32 = 0;
    for (0..node.members.len) |i| {
        const vector = vectors[i * dims ..][0..dims];
        radius = @max(radius, vec.distance(node.centroid, vector, self.config.metric));
    }
    return radius;
}

fn integerSqrtCeil(value: usize) usize {
    if (value <= 1) return value;
    var lo: usize = 1;
    var hi: usize = value;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (mid > value / mid) {
            hi = mid;
        } else if (mid * mid >= value) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

fn defaultFlatCentroidBlockProbeCount(block_count: usize, block_size: usize, posting_probe_limit: usize) usize {
    if (block_count <= 1) return block_count;
    const sqrt_blocks = integerSqrtCeil(block_count);
    const effective_block_size = @max(block_size, @as(usize, 1));
    const blocks_for_posting_probe_limit = std.math.divCeil(usize, posting_probe_limit, effective_block_size) catch unreachable;
    return @max(sqrt_blocks, blocks_for_posting_probe_limit);
}

const small_two_level_full_scan_blocks: usize = 32;

fn effectiveFlatCentroidBlockProbeCount(
    fixed: bool,
    configured_count: usize,
    block_count: usize,
    block_size: usize,
    posting_probe_limit: usize,
) usize {
    if (block_count <= 1) return block_count;
    if (fixed) return @min(@max(configured_count, @as(usize, 1)), block_count);
    if (block_count <= small_two_level_full_scan_blocks) return block_count;
    return @min(@max(defaultFlatCentroidBlockProbeCount(block_count, block_size, posting_probe_limit), @as(usize, 1)), block_count);
}

fn shouldBuildGlobalPostingQuantized(self: anytype, block_count: usize, posting_count: usize) bool {
    if (!self.config.use_quantization or posting_count == 0) return false;
    if (block_count <= 1) return true;
    if (self.config.centroid_directory_mode != .two_level_rabitq) return true;
    if (block_count <= small_two_level_full_scan_blocks) return true;

    return self.config.flat_centroid_block_probe_count >= block_count;
}

fn adaptiveFlatCentroidBlockProbeCount(sorted_block_probes: []const FlatCentroidProbe, base_count: usize) usize {
    if (sorted_block_probes.len == 0) return 0;
    var count = @min(@max(base_count, @as(usize, 1)), sorted_block_probes.len);
    if (count == sorted_block_probes.len) return count;
    const expansion_cap = @min(sorted_block_probes.len, @max(count, count * 2));

    var selected_boundary_upper = flatProbeUpperBound(sorted_block_probes[0]);
    for (sorted_block_probes[1..count]) |block_probe| {
        selected_boundary_upper = @max(selected_boundary_upper, flatProbeUpperBound(block_probe));
    }
    while (count < expansion_cap) {
        if (flatProbeLowerBound(sorted_block_probes[count]) > selected_boundary_upper) break;
        count += 1;
    }
    return count;
}

fn flatCentroidBlockProbeCandidateCount(fixed: bool, block_count: usize, base_count: usize) usize {
    if (block_count == 0) return 0;
    const bounded_base = @min(@max(base_count, @as(usize, 1)), block_count);
    if (fixed) return bounded_base;
    return @min(block_count, @max(bounded_base, bounded_base *| 2));
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
    parents: []const u64,
    levels: []const u16,
    states: []const types.PostingState,
    radii: []const f32,
    centroids: []const f32,
    dims: usize,
    posting_offset: usize,
) !void {
    if (posting_ids.len == 0) return;
    std.debug.assert(parents.len == posting_ids.len);
    std.debug.assert(levels.len == posting_ids.len);
    std.debug.assert(states.len == posting_ids.len);
    std.debug.assert(radii.len == posting_ids.len);
    var zero_stack: [flat_centroid_zero_stack_capacity]f32 = undefined;
    const use_zero_stack = dims <= zero_stack.len;
    const zero = if (use_zero_stack) zero_stack[0..dims] else try self.alloc.alloc(f32, dims);
    defer if (!use_zero_stack) self.alloc.free(zero);
    @memset(zero, 0);

    const ids = try self.alloc.dupe(u64, posting_ids);
    errdefer self.alloc.free(ids);
    const owned_parents = try self.alloc.dupe(u64, parents);
    errdefer self.alloc.free(owned_parents);
    const owned_levels = try self.alloc.dupe(u16, levels);
    errdefer self.alloc.free(owned_levels);
    const owned_states = try self.alloc.dupe(types.PostingState, states);
    errdefer self.alloc.free(owned_states);
    const owned_radii = try self.alloc.dupe(f32, radii);
    errdefer self.alloc.free(owned_radii);
    const owned_centroids = try self.alloc.dupe(f32, centroids);
    errdefer self.alloc.free(owned_centroids);
    const centroid_measures = try self.alloc.alloc(f32, posting_ids.len);
    errdefer self.alloc.free(centroid_measures);
    for (0..posting_ids.len) |i| {
        const centroid = centroids[i * dims ..][0..dims];
        centroid_measures[i] = vec.vectorMeasureForMetric(centroid, self.config.metric);
    }
    const block_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(block_centroid);
    @memset(block_centroid, 0);
    for (0..posting_ids.len) |i| {
        const centroid = centroids[i * dims ..][0..dims];
        vec.add(block_centroid, centroid);
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(posting_ids.len)), block_centroid);
    if (self.config.metric == .cosine and block_centroid.len > 0) {
        _ = vec.normalize(block_centroid);
    }
    var block_radius: f32 = 0;
    for (0..posting_ids.len) |i| {
        const centroid = centroids[i * dims ..][0..dims];
        block_radius = @max(block_radius, vec.distance(block_centroid, centroid, self.config.metric) + radii[i]);
    }
    var quantized = try self.quantizer.quantize(zero, centroids, posting_ids.len);
    errdefer quantized.deinit(self.alloc);
    try blocks.append(self.alloc, .{
        .posting_ids = ids,
        .parents = owned_parents,
        .levels = owned_levels,
        .states = owned_states,
        .posting_offset = posting_offset,
        .centroid = block_centroid,
        .radius = block_radius,
        .radii = owned_radii,
        .centroids = owned_centroids,
        .centroid_measures = centroid_measures,
        .quantized = quantized,
    });
}

fn appendFlatCentroidEntry(
    self: anytype,
    entries: *std.ArrayListUnmanaged(FlatCentroidEntry),
    posting_id: u64,
    parent: u64,
    level: u16,
    state: types.PostingState,
    bounds_radius: f32,
    centroid: []const f32,
) !void {
    const owned = try self.alloc.dupe(f32, centroid);
    errdefer self.alloc.free(owned);
    try entries.append(self.alloc, .{
        .posting_id = posting_id,
        .parent = parent,
        .level = level,
        .state = state,
        .bounds_radius = bounds_radius,
        .centroid = owned,
    });
}

fn appendFlatCentroidEntryOwned(
    self: anytype,
    entries: *std.ArrayListUnmanaged(FlatCentroidEntry),
    posting_id: u64,
    parent: u64,
    level: u16,
    state: types.PostingState,
    bounds_radius: f32,
    centroid: []f32,
) !void {
    try entries.append(self.alloc, .{
        .posting_id = posting_id,
        .parent = parent,
        .level = level,
        .state = state,
        .bounds_radius = bounds_radius,
        .centroid = centroid,
    });
}

fn deinitFlatCentroidEntries(alloc: std.mem.Allocator, entries: *std.ArrayListUnmanaged(FlatCentroidEntry)) void {
    for (entries.items) |entry| {
        alloc.free(entry.centroid);
        if (entry.sort_key_storage.len != 0) alloc.free(entry.sort_key_storage);
    }
    entries.deinit(alloc);
    entries.* = .empty;
}

fn flatCentroidEntryLess(ctx: FlatCentroidEntrySortContext, lhs: FlatCentroidEntry, rhs: FlatCentroidEntry) bool {
    if (lhs.sort_key.len != 0 and rhs.sort_key.len != 0) {
        const order = std.mem.order(u8, lhs.sort_key, rhs.sort_key);
        if (order != .eq) return order == .lt;
        return lhs.posting_id < rhs.posting_id;
    }
    const lhs_value = if (ctx.dim < lhs.centroid.len) lhs.centroid[ctx.dim] else 0;
    const rhs_value = if (ctx.dim < rhs.centroid.len) rhs.centroid[ctx.dim] else 0;
    if (lhs_value == rhs_value) return lhs.posting_id < rhs.posting_id;
    return lhs_value < rhs_value;
}

fn chooseFlatCentroidSortDimension(entries: []const FlatCentroidEntry, dims: usize) usize {
    if (dims == 0 or entries.len == 0) return 0;
    var best_dim: usize = 0;
    var best_range: f32 = 0;
    for (0..dims) |dim| {
        var min_value: f32 = std.math.inf(f32);
        var max_value: f32 = -std.math.inf(f32);
        for (entries) |entry| {
            if (dim >= entry.centroid.len) continue;
            min_value = @min(min_value, entry.centroid[dim]);
            max_value = @max(max_value, entry.centroid[dim]);
        }
        const range = max_value - min_value;
        if (range > best_range) {
            best_range = range;
            best_dim = dim;
        }
    }
    return best_dim;
}

fn populateFlatCentroidHilbertSortKeys(self: anytype, entries: []FlatCentroidEntry, dims: usize) !bool {
    const Index = comptime @TypeOf(self.*);
    if (comptime !@hasDecl(Index, "getHilbert")) return false;
    if (dims == 0 or entries.len == 0) return false;

    const hilbert = try self.getHilbert();
    if (hilbert.dimension != dims) return false;
    const embedding_len = hilbert.byteLen();
    if (embedding_len == 0) return false;
    const sort_key_storage_len = try std.math.mul(usize, entries.len, embedding_len);
    const sort_key_storage = try self.alloc.alloc(u8, sort_key_storage_len);
    var storage_owned_by_entries = false;
    errdefer if (!storage_owned_by_entries) self.alloc.free(sort_key_storage);
    entries[0].sort_key_storage = sort_key_storage;
    storage_owned_by_entries = true;

    var coords_stack: [hilbert_coord_stack_capacity]u32 = undefined;
    const use_coords_stack = hilbert.dimension <= coords_stack.len;
    const coords = if (use_coords_stack)
        coords_stack[0..hilbert.dimension]
    else
        try self.alloc.alloc(u32, hilbert.dimension);
    defer if (!use_coords_stack) self.alloc.free(coords);
    for (entries, 0..) |*entry, i| {
        if (entry.centroid.len != dims) return error.DimensionMismatch;
        const key = sort_key_storage[i * embedding_len ..][0..embedding_len];
        try hilbert.encodeVecBytesInto(entry.centroid, coords, key);
        entry.sort_key = key;
    }
    return true;
}

fn appendFlatCentroidBlocksFromEntries(
    self: anytype,
    blocks: *std.ArrayListUnmanaged(FlatCentroidBlock),
    entries: *std.ArrayListUnmanaged(FlatCentroidEntry),
    dims: usize,
    block_size: usize,
) !usize {
    const posting_count = entries.items.len;
    if (posting_count == 0) return 0;
    if (self.config.centroid_directory_mode == .two_level_rabitq and entries.items.len > block_size and dims > 0) {
        const sorted_by_hilbert = try populateFlatCentroidHilbertSortKeys(self, entries.items, dims);
        const sort_dim = if (sorted_by_hilbert) 0 else chooseFlatCentroidSortDimension(entries.items, dims);
        std.mem.sort(FlatCentroidEntry, entries.items, FlatCentroidEntrySortContext{ .dim = sort_dim }, flatCentroidEntryLess);
    }

    var posting_ids_stack: [flat_centroid_block_metadata_stack_capacity]u64 = undefined;
    var parents_stack: [flat_centroid_block_metadata_stack_capacity]u64 = undefined;
    var levels_stack: [flat_centroid_block_metadata_stack_capacity]u16 = undefined;
    var states_stack: [flat_centroid_block_metadata_stack_capacity]types.PostingState = undefined;
    var radii_stack: [flat_centroid_block_metadata_stack_capacity]f32 = undefined;
    const use_metadata_stack = block_size <= flat_centroid_block_metadata_stack_capacity;
    var posting_ids = if (use_metadata_stack) posting_ids_stack[0..block_size] else try self.alloc.alloc(u64, block_size);
    defer if (!use_metadata_stack) self.alloc.free(posting_ids);
    var parents = if (use_metadata_stack) parents_stack[0..block_size] else try self.alloc.alloc(u64, block_size);
    defer if (!use_metadata_stack) self.alloc.free(parents);
    var levels = if (use_metadata_stack) levels_stack[0..block_size] else try self.alloc.alloc(u16, block_size);
    defer if (!use_metadata_stack) self.alloc.free(levels);
    var states = if (use_metadata_stack) states_stack[0..block_size] else try self.alloc.alloc(types.PostingState, block_size);
    defer if (!use_metadata_stack) self.alloc.free(states);
    var radii = if (use_metadata_stack) radii_stack[0..block_size] else try self.alloc.alloc(f32, block_size);
    defer if (!use_metadata_stack) self.alloc.free(radii);
    var centroids = try self.alloc.alloc(f32, block_size * dims);
    defer self.alloc.free(centroids);

    var block_count: usize = 0;
    var posting_offset: usize = 0;
    for (entries.items) |*entry| {
        posting_ids[block_count] = entry.posting_id;
        parents[block_count] = entry.parent;
        levels[block_count] = entry.level;
        states[block_count] = entry.state;
        radii[block_count] = entry.bounds_radius;
        @memcpy(centroids[block_count * dims ..][0..dims], entry.centroid[0..dims]);
        self.alloc.free(entry.centroid);
        entry.centroid = &.{};
        block_count += 1;

        if (block_count == block_size) {
            try appendFlatCentroidBlock(self, blocks, posting_ids[0..block_count], parents[0..block_count], levels[0..block_count], states[0..block_count], radii[0..block_count], centroids[0 .. block_count * dims], dims, posting_offset);
            posting_offset += block_count;
            block_count = 0;
        }
    }
    if (block_count > 0) {
        try appendFlatCentroidBlock(self, blocks, posting_ids[0..block_count], parents[0..block_count], levels[0..block_count], states[0..block_count], radii[0..block_count], centroids[0 .. block_count * dims], dims, posting_offset);
    }
    deinitFlatCentroidEntries(self.alloc, entries);
    return posting_count;
}

fn finalizeFlatCentroidDirectory(
    self: anytype,
    blocks: *std.ArrayListUnmanaged(FlatCentroidBlock),
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
    posting_count: usize,
) !FlatCentroidDirectory {
    const dims: usize = @intCast(self.config.dims);
    const owned_blocks = try blocks.toOwnedSlice(self.alloc);
    errdefer {
        for (owned_blocks) |*block| block.deinit(self.alloc);
        self.alloc.free(owned_blocks);
    }

    var coarse_quantized: ?proto.RaBitQuantizedVectorSet = null;
    var posting_quantized: ?proto.RaBitQuantizedVectorSet = null;
    if (owned_blocks.len > 0) {
        var zero_stack: [flat_centroid_zero_stack_capacity]f32 = undefined;
        const use_zero_stack = dims <= zero_stack.len;
        const zero = if (use_zero_stack) zero_stack[0..dims] else try self.alloc.alloc(f32, dims);
        defer if (!use_zero_stack) self.alloc.free(zero);
        @memset(zero, 0);

        const coarse_centroid_count = owned_blocks.len * dims;
        var coarse_centroids_stack: [flat_centroid_coarse_scratch_stack_capacity]f32 = undefined;
        const use_coarse_centroids_stack = coarse_centroid_count <= coarse_centroids_stack.len;
        const coarse_centroids = if (use_coarse_centroids_stack)
            coarse_centroids_stack[0..coarse_centroid_count]
        else
            try self.alloc.alloc(f32, coarse_centroid_count);
        defer if (!use_coarse_centroids_stack) self.alloc.free(coarse_centroids);
        for (owned_blocks, 0..) |*block, i| {
            @memcpy(coarse_centroids[i * dims ..][0..dims], block.centroid);
        }
        coarse_quantized = try self.quantizer.quantize(zero, coarse_centroids, owned_blocks.len);
        errdefer if (coarse_quantized) |*coarse| coarse.deinit(self.alloc);

        if (shouldBuildGlobalPostingQuantized(self, owned_blocks.len, posting_count)) {
            posting_quantized = try self.quantizer.quantizeFromSource(zero, FlatCentroidBlocksVectorSource{
                .blocks = owned_blocks,
                .dims = dims,
                .total_count = posting_count,
            });
        }
    }

    return .{
        .blocks = owned_blocks,
        .coarse_quantized = coarse_quantized,
        .posting_quantized = posting_quantized,
        .root_node_snapshot = root_node,
        .node_count_snapshot = node_count,
        .publish_generation_snapshot = publish_generation,
        .posting_count = posting_count,
    };
}

fn shouldBuildFlatDirectoryFromCentroidRecords(self: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(self))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(self),
    };
    if (comptime !@hasField(Index, "config")) return false;
    return switch (self.config.posting_storage_mode) {
        .shadow_base_delta, .base_delta => true,
        .packed_hbc => false,
    };
}

fn buildFlatCentroidDirectoryFromRecords(self: anytype, txn: anytype, root_node: u64, node_count: u64, publish_generation: u64) !FlatCentroidDirectory {
    const dims: usize = @intCast(self.config.dims);
    const block_size = @max(self.config.flat_centroid_block_size, @as(usize, 1));
    var blocks = std.ArrayListUnmanaged(FlatCentroidBlock).empty;
    errdefer {
        for (blocks.items) |*block| block.deinit(self.alloc);
        blocks.deinit(self.alloc);
    }
    var entries = std.ArrayListUnmanaged(FlatCentroidEntry).empty;
    defer deinitFlatCentroidEntries(self.alloc, &entries);
    const records = posting.PostingStore.loadCentroidDirectoryRecords(self, txn) catch |err| switch (err) {
        error.Unsupported => return buildFlatCentroidDirectoryFromRecordProbes(self, txn, root_node, node_count, publish_generation),
        else => return err,
    };
    defer {
        for (records) |*record| record.deinit(self.alloc);
        self.alloc.free(records);
    }
    var state_lookup = try PostingStateLookup.init(self, txn);
    defer state_lookup.deinit(self.alloc);

    for (records) |*record| {
        if (record.member_count == 0 or record.centroid.len != dims) continue;
        const state = try state_lookup.stateForRecord(self, txn, record);
        try appendFlatCentroidEntryOwned(self, &entries, record.posting_id, record.parent, record.level, state, record.bounds_radius, record.centroid);
        record.centroid = &.{};
    }
    const posting_count = try appendFlatCentroidBlocksFromEntries(self, &blocks, &entries, dims, block_size);

    return try finalizeFlatCentroidDirectory(self, &blocks, root_node, node_count, publish_generation, posting_count);
}

fn buildFlatCentroidDirectoryFromRecordProbes(self: anytype, txn: anytype, root_node: u64, node_count: u64, publish_generation: u64) !FlatCentroidDirectory {
    const dims: usize = @intCast(self.config.dims);
    const block_size = @max(self.config.flat_centroid_block_size, @as(usize, 1));
    var blocks = std.ArrayListUnmanaged(FlatCentroidBlock).empty;
    errdefer {
        for (blocks.items) |*block| block.deinit(self.alloc);
        blocks.deinit(self.alloc);
    }
    var entries = std.ArrayListUnmanaged(FlatCentroidEntry).empty;
    defer deinitFlatCentroidEntries(self.alloc, &entries);
    var state_lookup = try PostingStateLookup.init(self, txn);
    defer state_lookup.deinit(self.alloc);

    var posting_id: u64 = 1;
    while (posting_id <= node_count) : (posting_id += 1) {
        var record = posting.PostingStore.loadCentroidDirectoryRecord(self, txn, posting_id, isNotFoundGeneric) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        defer record.deinit(self.alloc);
        if (record.member_count == 0 or record.centroid.len != dims) continue;
        const state = try state_lookup.stateForRecord(self, txn, &record);
        try appendFlatCentroidEntryOwned(self, &entries, record.posting_id, record.parent, record.level, state, record.bounds_radius, record.centroid);
        record.centroid = &.{};
    }
    const posting_count = try appendFlatCentroidBlocksFromEntries(self, &blocks, &entries, dims, block_size);

    return try finalizeFlatCentroidDirectory(self, &blocks, root_node, node_count, publish_generation, posting_count);
}

fn buildFlatCentroidDirectoryFromNodes(self: anytype, txn: anytype, root_node: u64, node_count: u64, publish_generation: u64) !FlatCentroidDirectory {
    const dims: usize = @intCast(self.config.dims);
    const block_size = @max(self.config.flat_centroid_block_size, @as(usize, 1));
    var blocks = std.ArrayListUnmanaged(FlatCentroidBlock).empty;
    errdefer {
        for (blocks.items) |*block| block.deinit(self.alloc);
        blocks.deinit(self.alloc);
    }
    var entries = std.ArrayListUnmanaged(FlatCentroidEntry).empty;
    defer deinitFlatCentroidEntries(self.alloc, &entries);
    var pending = std.ArrayListUnmanaged(u64).empty;
    defer pending.deinit(self.alloc);
    try pending.append(self.alloc, root_node);

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
        try appendFlatCentroidEntry(self, &entries, node.id, node.parent, node.level, node.posting_state, 0, node.centroid);
    }
    const posting_count = try appendFlatCentroidBlocksFromEntries(self, &blocks, &entries, dims, block_size);

    return try finalizeFlatCentroidDirectory(self, &blocks, root_node, node_count, publish_generation, posting_count);
}

fn buildFlatCentroidDirectory(self: anytype, txn: anytype, root_node: u64, node_count: u64, publish_generation: u64) !FlatCentroidDirectory {
    if (shouldBuildFlatDirectoryFromCentroidRecords(self)) {
        var directory = try buildFlatCentroidDirectoryFromRecords(self, txn, root_node, node_count, publish_generation);
        if (directory.posting_count > 0) return directory;
        directory.deinit(self.alloc);
    }
    return try buildFlatCentroidDirectoryFromNodes(self, txn, root_node, node_count, publish_generation);
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
    const dims: usize = @intCast(self.config.dims);
    const query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(query),
        .inner_product => 0,
    };
    var probe_collector = FlatProbeCollector.init(probes[0..probe_limit]);

    const max_block_postings = @max(self.config.flat_centroid_block_size, @as(usize, 1));
    const distance_capacity = if (self.config.use_quantization and directory.posting_quantized != null and directory.posting_count != 0)
        @max(directory.posting_count, @max(directory.blocks.len, max_block_postings))
    else
        @max(directory.blocks.len, max_block_postings);
    try scratch.ensureDistanceCapacity(self.alloc, distance_capacity);
    var selected_block_storage = scratch.positions[0..directory.blocks.len];
    var selected_blocks: []usize = selected_block_storage[0..0];
    if (self.config.centroid_directory_mode == .two_level_rabitq and directory.blocks.len > 1) {
        const fixed_block_probe_count = self.config.flat_centroid_block_probe_count != 0;
        const block_probe_limit = effectiveFlatCentroidBlockProbeCount(
            fixed_block_probe_count,
            self.config.flat_centroid_block_probe_count,
            directory.blocks.len,
            self.config.flat_centroid_block_size,
            probe_limit,
        );
        if (block_probe_limit >= directory.blocks.len) {
            for (directory.blocks, 0..) |_, block_index| selected_block_storage[block_index] = block_index;
            selected_blocks = selected_block_storage[0..directory.blocks.len];
        } else {
            const distances = scratch.distances[0..directory.blocks.len];
            const error_bounds = scratch.error_bounds[0..directory.blocks.len];
            if (directory.coarse_quantized) |*coarse| {
                try self.quantizer.estimateDistancesWithScratch(coarse, query, distances, error_bounds, &scratch.estimate);
                profile.centroid_directory_block_centroid_estimates += @intCast(directory.blocks.len);
                for (directory.blocks, 0..) |*block, block_index| {
                    error_bounds[block_index] += centroidBlockProbeErrorBound(self.config.metric, distances[block_index], block.radius);
                }
            } else {
                for (directory.blocks, 0..) |*block, block_index| {
                    distances[block_index] = vec.distanceToQuery(query, query_measure, block.centroid, self.config.metric);
                    error_bounds[block_index] = centroidBlockProbeErrorBound(self.config.metric, distances[block_index], block.radius);
                }
                profile.centroid_directory_block_centroids_scored += @intCast(directory.blocks.len);
            }

            const block_probe_candidate_count = flatCentroidBlockProbeCandidateCount(fixed_block_probe_count, directory.blocks.len, block_probe_limit);
            var block_probe_candidates_stack: [flat_centroid_query_probe_stack_capacity]FlatCentroidProbe = undefined;
            const use_block_probe_candidates_stack = block_probe_candidate_count <= block_probe_candidates_stack.len;
            const block_probe_candidates = if (use_block_probe_candidates_stack)
                block_probe_candidates_stack[0..block_probe_candidate_count]
            else
                try self.alloc.alloc(FlatCentroidProbe, block_probe_candidate_count);
            defer if (!use_block_probe_candidates_stack) self.alloc.free(block_probe_candidates);
            var block_probe_collector = CentroidBlockProbeCollector.init(block_probe_candidates);
            for (0..directory.blocks.len) |block_index| {
                block_probe_collector.insert(.{
                    .posting_id = @intCast(block_index),
                    .distance = distances[block_index],
                    .error_bound = error_bounds[block_index],
                });
            }
            const block_probes = block_probe_collector.items();
            std.mem.sort(FlatCentroidProbe, block_probes, {}, centroidBlockProbeLess);
            const selected_block_count = if (fixed_block_probe_count)
                block_probes.len
            else
                adaptiveFlatCentroidBlockProbeCount(block_probes, block_probe_limit);
            for (block_probes[0..selected_block_count], 0..) |block_probe, i| {
                selected_block_storage[i] = @intCast(block_probe.posting_id);
            }
            selected_blocks = selected_block_storage[0..selected_block_count];
        }
        profile.approx_nodes_expanded += @intCast(directory.blocks.len);
        profile.centroid_directory_blocks_scanned += @intCast(directory.blocks.len);
        profile.centroid_directory_blocks_selected += @intCast(selected_blocks.len);
        profile.centroid_directory_block_probe_limit += @intCast(block_probe_limit);
        profile.centroid_directory_block_probe_count += @intCast(selected_blocks.len);
    } else {
        for (directory.blocks, 0..) |_, block_index| selected_block_storage[block_index] = block_index;
        selected_blocks = selected_block_storage[0..directory.blocks.len];
        profile.centroid_directory_blocks_scanned += @intCast(directory.blocks.len);
        profile.centroid_directory_blocks_selected += @intCast(selected_blocks.len);
    }

    const global_posting_quantized = global: {
        if (!self.config.use_quantization or
            selected_blocks.len != directory.blocks.len or
            directory.posting_count == 0)
        {
            break :global null;
        }
        if (directory.posting_quantized) |*posting_q| break :global posting_q;
        break :global null;
    };
    const quantized_posting_candidate_limit = if (self.config.use_quantization and selected_blocks.len != 0)
        @min(
            if (global_posting_quantized != null) directory.posting_count else max_block_postings,
            if (global_posting_quantized != null)
                @max(@as(usize, 1), @max(probe_limit *| 2, selected_blocks.len))
            else
                @max(
                    @as(usize, 1),
                    (std.math.divCeil(usize, probe_limit, selected_blocks.len) catch unreachable) * 2,
                ),
        )
    else
        0;
    var quantized_posting_candidates: []FlatCentroidProbe = &.{};
    var quantized_posting_candidates_stack: [flat_centroid_query_probe_stack_capacity]FlatCentroidProbe = undefined;
    const use_quantized_posting_candidates_stack = quantized_posting_candidate_limit <= quantized_posting_candidates_stack.len;
    if (quantized_posting_candidate_limit != 0) {
        quantized_posting_candidates = if (use_quantized_posting_candidates_stack)
            quantized_posting_candidates_stack[0..quantized_posting_candidate_limit]
        else
            try self.alloc.alloc(FlatCentroidProbe, quantized_posting_candidate_limit);
    }
    defer if (quantized_posting_candidate_limit != 0 and !use_quantized_posting_candidates_stack) self.alloc.free(quantized_posting_candidates);

    if (global_posting_quantized) |posting_q| {
        try self.quantizer.estimateDistancesWithScratch(posting_q, query, scratch.distances[0..directory.posting_count], scratch.error_bounds[0..directory.posting_count], &scratch.estimate);
        profile.centroid_directory_posting_centroid_estimates += @intCast(directory.posting_count);

        var candidate_collector = FlatProbeCollector.init(quantized_posting_candidates);
        for (selected_blocks) |block_index| {
            const block = &directory.blocks[block_index];
            const distances = scratch.distances[block.posting_offset..][0..block.posting_ids.len];
            const error_bounds = scratch.error_bounds[block.posting_offset..][0..block.posting_ids.len];
            for (block.posting_ids, 0..) |posting_id, i| {
                const posting_error_bound = centroidBlockProbeErrorBound(self.config.metric, distances[i], block.radii[i]);
                candidate_collector.insert(.{
                    .posting_id = posting_id,
                    .parent = block.parents[i],
                    .level = block.levels[i],
                    .state = block.states[i],
                    .block_index = block_index,
                    .entry_index = i,
                    .distance = distances[i],
                    .error_bound = error_bounds[i] + posting_error_bound,
                });
            }
        }
        const candidates = candidate_collector.items();
        std.mem.sort(FlatCentroidProbe, candidates, {}, flatProbeLess);
        var scored_candidates: usize = 0;
        for (candidates) |candidate| {
            if (probe_collector.wouldRejectLowerBound(flatProbeLowerBound(candidate))) break;
            const block = &directory.blocks[candidate.block_index];
            const centroid = block.centroids[candidate.entry_index * dims ..][0..dims];
            const distance = vec.distanceToQueryWithCandidateMeasure(
                query,
                query_measure,
                centroid,
                block.centroid_measures[candidate.entry_index],
                self.config.metric,
            );
            scored_candidates += 1;
            probe_collector.insert(.{
                .posting_id = candidate.posting_id,
                .parent = candidate.parent,
                .level = candidate.level,
                .state = candidate.state,
                .block_index = candidate.block_index,
                .entry_index = candidate.entry_index,
                .distance = distance,
                .error_bound = centroidBlockProbeErrorBound(self.config.metric, distance, block.radii[candidate.entry_index]),
            });
        }
        profile.centroid_directory_posting_centroids_scored += @intCast(scored_candidates);
    } else {
        for (selected_blocks) |block_index| {
            const block = &directory.blocks[block_index];
            const count = block.posting_ids.len;
            if (self.config.use_quantization) {
                const distances = scratch.distances[0..count];
                const error_bounds = scratch.error_bounds[0..count];
                try self.quantizer.estimateDistancesWithScratch(&block.quantized, query, distances, error_bounds, &scratch.estimate);
                profile.centroid_directory_posting_centroid_estimates += @intCast(count);

                var candidate_collector = FlatProbeCollector.init(quantized_posting_candidates);
                for (block.posting_ids, 0..) |posting_id, i| {
                    const posting_error_bound = centroidBlockProbeErrorBound(self.config.metric, distances[i], block.radii[i]);
                    candidate_collector.insert(.{
                        .posting_id = posting_id,
                        .parent = block.parents[i],
                        .level = block.levels[i],
                        .state = block.states[i],
                        .block_index = block_index,
                        .entry_index = i,
                        .distance = distances[i],
                        .error_bound = error_bounds[i] + posting_error_bound,
                    });
                }
                const candidates = candidate_collector.items();
                std.mem.sort(FlatCentroidProbe, candidates, {}, flatProbeLess);
                var scored_candidates: usize = 0;
                for (candidates) |candidate| {
                    if (probe_collector.wouldRejectLowerBound(flatProbeLowerBound(candidate))) break;
                    const centroid = block.centroids[candidate.entry_index * dims ..][0..dims];
                    const distance = vec.distanceToQueryWithCandidateMeasure(
                        query,
                        query_measure,
                        centroid,
                        block.centroid_measures[candidate.entry_index],
                        self.config.metric,
                    );
                    scored_candidates += 1;
                    probe_collector.insert(.{
                        .posting_id = candidate.posting_id,
                        .parent = candidate.parent,
                        .level = candidate.level,
                        .state = candidate.state,
                        .block_index = block_index,
                        .entry_index = candidate.entry_index,
                        .distance = distance,
                        .error_bound = centroidBlockProbeErrorBound(self.config.metric, distance, block.radii[candidate.entry_index]),
                    });
                }
                profile.centroid_directory_posting_centroids_scored += @intCast(scored_candidates);
            } else {
                const distances = scratch.distances[0..count];
                const error_bounds = scratch.error_bounds[0..count];
                profile.centroid_directory_posting_centroids_scored += @intCast(count);
                for (0..count) |i| {
                    const centroid = block.centroids[i * dims ..][0..dims];
                    distances[i] = vec.distanceToQueryWithCandidateMeasure(query, query_measure, centroid, block.centroid_measures[i], self.config.metric);
                    error_bounds[i] = centroidBlockProbeErrorBound(self.config.metric, distances[i], block.radii[i]);
                }
                for (block.posting_ids, 0..) |posting_id, i| {
                    probe_collector.insert(.{
                        .posting_id = posting_id,
                        .parent = block.parents[i],
                        .level = block.levels[i],
                        .state = block.states[i],
                        .block_index = block_index,
                        .entry_index = i,
                        .distance = distances[i],
                        .error_bound = error_bounds[i],
                    });
                }
            }
        }
    }

    const selected_probes = probe_collector.items();
    std.mem.sort(FlatCentroidProbe, selected_probes, {}, flatProbeLess);
    profile.approx_nodes_expanded += @intCast(selected_blocks.len);
    return selected_probes.len;
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
    var min_dirty_mutation_version: u64 = std.math.maxInt(u64);

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
        if (node.members.len >= self.config.leaf_size) {
            result.postings_at_capacity += 1;
            if (node.members.len > self.config.leaf_size) {
                result.overfull_postings += 1;
                result.max_over_capacity_members = @max(
                    result.max_over_capacity_members,
                    @as(u64, @intCast(node.members.len - self.config.leaf_size)),
                );
            }
        }

        const state = node.posting_state;
        result.max_mutation_version = @max(result.max_mutation_version, state.mutation_version);
        if (comptime posting.txnSupportsDeltaTailScan(@TypeOf(txn))) {
            if (self.config.posting_storage_mode != .packed_hbc) {
                try updatePostingTailBacklogStats(self, txn, node.id, &result);
            }
        }
        if (!state.dirty) continue;

        result.dirty_postings += 1;
        min_dirty_mutation_version = @min(min_dirty_mutation_version, state.mutation_version);
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

    if (result.dirty_postings != 0) {
        result.min_dirty_mutation_version = min_dirty_mutation_version;
        result.max_dirty_version_age = result.max_mutation_version -| min_dirty_mutation_version;
    }
    return result;
}

fn updatePostingTailBacklogStats(
    self: anytype,
    txn: anytype,
    posting_id: u64,
    result: *posting.PostingBacklogStats,
) !void {
    const base_header = posting.PostingStore.loadBaseHeader(self, txn, posting_id, isNotFoundGeneric) catch |err| {
        if (isNotFoundGeneric(err)) {
            result.skipped_missing += 1;
            return;
        }
        return err;
    };
    const delta_stats = try posting.PostingStore.deltaTailStats(self, txn, posting_id, base_header.generation);
    const delta_records_after_base: u64 = @intCast(delta_stats.records_after_generation);
    const tombstone_records_after_base: u64 = @intCast(delta_stats.tombstones_after_generation);
    if (delta_records_after_base == 0) return;

    const denominator: u64 = @max(@as(u64, @intCast(base_header.member_count)), 1);
    const ratio_bps = (delta_records_after_base * 10_000) / denominator;
    result.delta_tail_postings += 1;
    result.max_delta_tail_records = @max(result.max_delta_tail_records, delta_records_after_base);
    result.max_tombstone_tail_records = @max(result.max_tombstone_tail_records, tombstone_records_after_base);
    result.max_delta_tail_key_bytes = @max(result.max_delta_tail_key_bytes, @as(u64, @intCast(delta_stats.encoded_key_bytes)));
    result.max_delta_tail_value_bytes = @max(result.max_delta_tail_value_bytes, @as(u64, @intCast(delta_stats.encoded_value_bytes)));
    result.max_delta_tail_sequence = @max(result.max_delta_tail_sequence, delta_stats.max_sequence_after_generation);
    result.max_delta_to_base_ratio_bps = @max(result.max_delta_to_base_ratio_bps, ratio_bps);
}

pub fn runAutoPostingMaintenanceTxn(self: anytype, txn: anytype) !void {
    const max_postings = self.config.auto_posting_maintenance_max_postings;
    if (max_postings == 0) return;
    if (self.config.posting_storage_mode == .base_delta and
        comptime !posting.txnSupportsDeltaTailScan(@TypeOf(txn)))
    {
        return;
    }
    if (!try autoPostingMaintenanceShouldRun(self, txn)) return;
    const max_layout_changes = self.config.auto_posting_maintenance_max_layout_changes;
    const max_boundary_reassignments = self.config.auto_posting_maintenance_max_boundary_reassignments;
    const result = try repairDirtyPostingsTxnWithOptions(self, txn, .{
        .max_postings = max_postings,
        .fold_delta_tails = self.config.auto_posting_maintenance_fold_delta_tails,
        .min_delta_records_to_fold = self.config.auto_posting_maintenance_min_delta_records_to_fold,
        .min_tombstone_records_to_fold = self.config.auto_posting_maintenance_min_tombstone_records_to_fold,
        .min_delta_to_base_ratio_bps = self.config.auto_posting_maintenance_min_delta_to_base_ratio_bps,
        .min_delta_value_bytes_to_fold = self.config.auto_posting_maintenance_min_delta_value_bytes_to_fold,
        .max_delta_tail_postings = self.config.auto_posting_maintenance_max_delta_tail_postings,
        .rebalance_layout = max_layout_changes != 0,
        .split_full_postings = self.config.auto_posting_maintenance_split_full_postings,
        .max_layout_changes = max_layout_changes,
        .max_boundary_reassignments = max_boundary_reassignments,
        .reassign_dirty_postings = max_boundary_reassignments != 0,
        .allow_overfull_reassignment = self.config.auto_posting_maintenance_allow_overfull_reassignment,
        .max_overfull_reassignment_postings = self.config.auto_posting_maintenance_max_overfull_reassignment_postings,
        .max_over_capacity_reassignment_members = self.config.auto_posting_maintenance_max_over_capacity_reassignment_members,
        .boundary_reassignment_min_improvement = self.config.auto_posting_maintenance_boundary_reassignment_min_improvement,
    });
    const observed_layout_changes = result.split_postings + result.merged_postings;
    self.write_profile.auto_posting_maintenance_runs += 1;
    self.write_profile.auto_posting_maintenance_observed_max_repaired_postings = @max(
        self.write_profile.auto_posting_maintenance_observed_max_repaired_postings,
        result.repaired_postings,
    );
    self.write_profile.auto_posting_maintenance_observed_max_layout_changes = @max(
        self.write_profile.auto_posting_maintenance_observed_max_layout_changes,
        observed_layout_changes,
    );
    self.write_profile.auto_posting_maintenance_observed_max_split_postings = @max(
        self.write_profile.auto_posting_maintenance_observed_max_split_postings,
        result.split_postings,
    );
    self.write_profile.auto_posting_maintenance_observed_max_merged_postings = @max(
        self.write_profile.auto_posting_maintenance_observed_max_merged_postings,
        result.merged_postings,
    );
    self.write_profile.auto_posting_maintenance_observed_max_boundary_reassigned_vectors = @max(
        self.write_profile.auto_posting_maintenance_observed_max_boundary_reassigned_vectors,
        result.boundary_reassigned_vectors,
    );
    self.write_profile.auto_posting_maintenance_observed_max_delta_fold_records = @max(
        self.write_profile.auto_posting_maintenance_observed_max_delta_fold_records,
        result.delta_fold_records,
    );
}

fn autoPostingMaintenanceShouldRun(self: anytype, txn: anytype) !bool {
    const min_dirty_postings = self.config.auto_posting_maintenance_min_dirty_postings;
    const max_dirty_version_age = self.config.auto_posting_maintenance_max_dirty_version_age;
    const min_delta_records = self.config.auto_posting_maintenance_min_delta_records_to_run;
    const min_tombstone_records = self.config.auto_posting_maintenance_min_tombstone_records_to_run;
    const min_delta_to_base_ratio_bps = self.config.auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run;
    const min_centroid_version_lag = self.config.auto_posting_maintenance_min_centroid_version_lag;
    const min_payload_version_lag = self.config.auto_posting_maintenance_min_payload_version_lag;
    const min_overfull_postings = self.config.auto_posting_maintenance_min_overfull_postings_to_run;
    const min_postings_at_capacity = self.config.auto_posting_maintenance_min_postings_at_capacity_to_run;
    const max_delta_tail_postings = self.config.auto_posting_maintenance_max_delta_tail_postings;
    if (min_dirty_postings == 0 and
        max_dirty_version_age == 0 and
        min_delta_records == 0 and
        min_tombstone_records == 0 and
        min_delta_to_base_ratio_bps == 0 and
        max_delta_tail_postings == std.math.maxInt(usize) and
        min_centroid_version_lag == 0 and
        min_payload_version_lag == 0 and
        min_overfull_postings == 0 and
        min_postings_at_capacity == 0)
    {
        return true;
    }

    const stats = try postingBacklogStatsTxn(self, txn);
    if (stats.dirty_postings == 0 and stats.delta_tail_postings == 0 and stats.overfull_postings == 0 and stats.postings_at_capacity == 0) return false;
    if (min_dirty_postings != 0 and stats.dirty_postings >= min_dirty_postings) return true;
    if (max_dirty_version_age != 0 and stats.max_dirty_version_age >= max_dirty_version_age) return true;
    if (min_delta_records != 0 and stats.max_delta_tail_records >= min_delta_records) return true;
    if (min_tombstone_records != 0 and stats.max_tombstone_tail_records >= min_tombstone_records) return true;
    if (min_delta_to_base_ratio_bps != 0 and stats.max_delta_to_base_ratio_bps >= min_delta_to_base_ratio_bps) return true;
    if (max_delta_tail_postings != std.math.maxInt(usize) and stats.delta_tail_postings > max_delta_tail_postings) return true;
    if (min_centroid_version_lag != 0 and stats.max_centroid_version_lag >= min_centroid_version_lag) return true;
    if (min_payload_version_lag != 0 and stats.max_payload_version_lag >= min_payload_version_lag) return true;
    if (min_overfull_postings != 0 and stats.overfull_postings >= min_overfull_postings) return true;
    if (min_postings_at_capacity != 0 and stats.postings_at_capacity >= min_postings_at_capacity) return true;
    return false;
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
    swap_vector_id: ?u64 = null,
};

const BoundaryReassignmentResult = struct {
    moved_vectors: usize = 0,
    capacity_skips: usize = 0,
    min_source_skips: usize = 0,
    swap_moves: usize = 0,
};

fn projectedLeafMemberCount(leaf: *const types.Node, planned_in: usize, planned_out: usize, extra_in: usize) usize {
    return leaf.members.len + planned_in + extra_in -| planned_out;
}

fn plannedDirectMoveExceedsOverfullLimits(
    leaves: []const types.Node,
    planned_in: []const usize,
    planned_out: []const usize,
    target_index: usize,
    leaf_size: usize,
    max_overfull_postings: usize,
    max_over_capacity_members: usize,
) bool {
    var overfull_count: usize = 0;
    for (leaves, 0..) |*leaf, i| {
        const projected = projectedLeafMemberCount(
            leaf,
            planned_in[i],
            planned_out[i],
            if (i == target_index) 1 else 0,
        );
        if (projected <= leaf_size) continue;
        overfull_count += 1;
        if (overfull_count > max_overfull_postings) return true;
        if (projected - leaf_size > max_over_capacity_members) return true;
    }
    return false;
}

fn directMoveExceedsOverfullLimits(
    leaves: []const types.Node,
    target_index: usize,
    leaf_size: usize,
    max_overfull_postings: usize,
    max_over_capacity_members: usize,
) bool {
    var overfull_count: usize = 0;
    for (leaves, 0..) |*leaf, i| {
        const projected = leaf.members.len + if (i == target_index) @as(usize, 1) else 0;
        if (projected <= leaf_size) continue;
        overfull_count += 1;
        if (overfull_count > max_overfull_postings) return true;
        if (projected - leaf_size > max_over_capacity_members) return true;
    }
    return false;
}

fn successfulDeltaFoldCalls(result: posting.PostingMaintenanceResult) u64 {
    return result.delta_fold_attempts -| result.delta_fold_skipped;
}

fn foldPostingDeltaTailIfNeeded(
    self: anytype,
    txn: anytype,
    posting_id: u64,
    options: posting.PostingMaintenanceOptions,
    force_fold: bool,
    result: *posting.PostingMaintenanceResult,
) !void {
    if (!options.fold_delta_tails) return;
    if (self.config.posting_storage_mode != .shadow_base_delta and
        self.config.posting_storage_mode != .base_delta)
    {
        return;
    }
    if (comptime !posting.txnSupportsDeltaTailScan(@TypeOf(txn))) return;

    const folded = blk: {
        const min_delta_records = if (force_fold) 1 else options.min_delta_records_to_fold;
        const min_tombstone_records = if (force_fold) 0 else options.min_tombstone_records_to_fold;
        const min_delta_to_base_ratio_bps = if (force_fold) 0 else options.min_delta_to_base_ratio_bps;
        const min_delta_value_bytes = if (force_fold) 0 else options.min_delta_value_bytes_to_fold;
        break :blk posting.PostingStore.foldDeltaTailIntoBaseWithOptions(self, txn, posting_id, isNotFoundGeneric, .{
            .min_delta_records = min_delta_records,
            .min_tombstone_records = min_tombstone_records,
            .min_delta_to_base_ratio_bps = min_delta_to_base_ratio_bps,
            .min_delta_value_bytes = min_delta_value_bytes,
            .max_materialized_members = options.max_delta_fold_materialized_members,
            .max_materialized_bytes = options.max_delta_fold_materialized_bytes,
        }) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            break :blk posting.FoldDeltaTailResult{};
        };
    };
    if (folded.delta_records == 0) return;

    result.delta_fold_attempts += 1;
    result.delta_fold_peak_scratch_bytes = @max(result.delta_fold_peak_scratch_bytes, @as(u64, @intCast(folded.peak_scratch_bytes)));
    if (folded.skipped) {
        result.delta_fold_skipped += 1;
    } else {
        result.delta_fold_records += @intCast(folded.delta_records);
    }
}

fn boundaryMoveContainsVector(moves: []const BoundaryMove, vector_id: u64) bool {
    for (moves) |move| {
        if (move.vector_id == vector_id) return true;
        if (move.swap_vector_id != null and move.swap_vector_id.? == vector_id) return true;
    }
    return false;
}

fn targetedBoundaryReassignParent(
    self: anytype,
    txn: anytype,
    parent_id: u64,
    max_moves: usize,
    allow_overfull: bool,
    max_overfull_postings: usize,
    max_over_capacity_members: usize,
    min_improvement: f32,
) !BoundaryReassignmentResult {
    var result: BoundaryReassignmentResult = .{};
    if (parent_id == 0 or max_moves == 0) return result;

    var parent = self.loadNode(txn, parent_id) catch |err| {
        if (isNotFoundGeneric(err)) return result;
        return err;
    };
    defer parent.deinit(self.alloc);
    if (parent.is_leaf or parent.children.len < 2) return result;

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
            return result;
        }
        try child.ensureUnbacked(self.alloc);
        leaves[initialized] = child;
        initialized += 1;
    }
    if (initialized < 2) return result;

    const moves_cap = @min(max_moves, @as(usize, @intCast(std.math.maxInt(u32))));
    var moves = try self.alloc.alloc(BoundaryMove, moves_cap);
    defer self.alloc.free(moves);
    const planned_out = try self.alloc.alloc(usize, initialized);
    defer self.alloc.free(planned_out);
    @memset(planned_out, 0);
    const planned_in = try self.alloc.alloc(usize, initialized);
    defer self.alloc.free(planned_in);
    @memset(planned_in, 0);

    const dims: usize = @intCast(self.config.dims);
    const vector_scratch_len = try std.math.mul(usize, dims, 4);
    var vector_scratch_stack: [boundary_reassignment_vector_scratch_stack_capacity]f32 = undefined;
    const use_vector_scratch_stack = vector_scratch_len <= vector_scratch_stack.len;
    const vector_scratch = if (use_vector_scratch_stack)
        vector_scratch_stack[0..vector_scratch_len]
    else
        try self.alloc.alloc(f32, vector_scratch_len);
    defer if (!use_vector_scratch_stack) self.alloc.free(vector_scratch);
    const raw_scratch = vector_scratch[0..dims];
    const transformed = vector_scratch[dims..][0..dims];
    const swap_raw_scratch = vector_scratch[dims * 2 ..][0..dims];
    const swap_transformed = vector_scratch[dims * 3 ..][0..dims];

    var move_count: usize = 0;
    const min_source_members = self.minLeafOccupancy();
    for (leaves[0..initialized], 0..) |*source, source_index| {
        if (move_count >= moves.len) break;
        if (source.members.len <= min_source_members) {
            result.min_source_skips += 1;
            continue;
        }
        for (source.members) |member_id| {
            if (move_count >= moves.len) break;
            if (source.members.len - planned_out[source_index] <= min_source_members) {
                result.min_source_skips += 1;
                break;
            }
            if (boundaryMoveContainsVector(moves[0..move_count], member_id)) continue;

            const raw = try self.getVectorScratch(txn, member_id, raw_scratch);
            _ = self.transformVector(raw, transformed);
            const current_dist = vec.distance(source.centroid, transformed, self.config.metric);

            var best_index = source_index;
            var best_dist = current_dist;
            var best_swap_vector_id: ?u64 = null;
            for (leaves[0..initialized], 0..) |*candidate, candidate_index| {
                if (candidate_index == source_index) continue;
                const projected_members = projectedLeafMemberCount(
                    candidate,
                    planned_in[candidate_index],
                    planned_out[candidate_index],
                    1,
                );
                if (projected_members > self.config.leaf_size and
                    (!allow_overfull or plannedDirectMoveExceedsOverfullLimits(
                        leaves[0..initialized],
                        planned_in,
                        planned_out,
                        candidate_index,
                        self.config.leaf_size,
                        max_overfull_postings,
                        max_over_capacity_members,
                    )))
                {
                    var best_candidate_swap: ?u64 = null;
                    var best_candidate_swap_gain: f32 = 0;
                    for (candidate.members) |swap_member_id| {
                        if (boundaryMoveContainsVector(moves[0..move_count], swap_member_id)) continue;
                        const swap_raw = try self.getVectorScratch(txn, swap_member_id, swap_raw_scratch);
                        _ = self.transformVector(swap_raw, swap_transformed);
                        const swap_current_dist = vec.distance(candidate.centroid, swap_transformed, self.config.metric);
                        const swap_source_dist = vec.distance(source.centroid, swap_transformed, self.config.metric);
                        if (swap_source_dist + min_improvement >= swap_current_dist) continue;
                        const swap_gain = swap_current_dist - swap_source_dist;
                        if (best_candidate_swap == null or swap_gain > best_candidate_swap_gain) {
                            best_candidate_swap = swap_member_id;
                            best_candidate_swap_gain = swap_gain;
                        }
                    }
                    if (best_candidate_swap == null) {
                        result.capacity_skips += 1;
                        continue;
                    }
                    const dist = vec.distance(candidate.centroid, transformed, self.config.metric);
                    if (dist + min_improvement < best_dist) {
                        best_dist = dist;
                        best_index = candidate_index;
                        best_swap_vector_id = best_candidate_swap;
                    }
                    continue;
                }
                const dist = vec.distance(candidate.centroid, transformed, self.config.metric);
                if (dist + min_improvement < best_dist) {
                    best_dist = dist;
                    best_index = candidate_index;
                    best_swap_vector_id = null;
                }
            }
            if (best_index == source_index) continue;

            moves[move_count] = .{
                .vector_id = member_id,
                .from_index = source_index,
                .to_index = best_index,
                .swap_vector_id = best_swap_vector_id,
            };
            move_count += 1;
            if (best_swap_vector_id == null) {
                planned_out[source_index] += 1;
                planned_in[best_index] += 1;
            }
        }
    }
    if (move_count == 0) return result;

    const changed = try self.alloc.alloc(bool, initialized);
    defer self.alloc.free(changed);
    @memset(changed, false);

    var applied: usize = 0;
    for (moves[0..move_count]) |move| {
        if (move.from_index == move.to_index) continue;
        if (move.swap_vector_id) |swap_vector_id| {
            posting.PostingStore.removeMember(self.alloc, &leaves[move.from_index], move.vector_id) catch continue;
            posting.PostingStore.removeMember(self.alloc, &leaves[move.to_index], swap_vector_id) catch {
                _ = try posting.PostingStore.appendMember(self.alloc, &leaves[move.from_index], move.vector_id);
                continue;
            };
            _ = try posting.PostingStore.appendMember(self.alloc, &leaves[move.to_index], move.vector_id);
            _ = try posting.PostingStore.appendMember(self.alloc, &leaves[move.from_index], swap_vector_id);
            try self.putVecLeaf(txn, swap_vector_id, leaves[move.from_index].id);
            result.swap_moves += 1;
        } else {
            if (leaves[move.to_index].members.len >= self.config.leaf_size and
                (!allow_overfull or directMoveExceedsOverfullLimits(
                    leaves[0..initialized],
                    move.to_index,
                    self.config.leaf_size,
                    max_overfull_postings,
                    max_over_capacity_members,
                )))
            {
                result.capacity_skips += 1;
                continue;
            }
            if (leaves[move.from_index].members.len <= min_source_members) {
                result.min_source_skips += 1;
                continue;
            }
            posting.PostingStore.removeMember(self.alloc, &leaves[move.from_index], move.vector_id) catch continue;
            _ = try posting.PostingStore.appendMember(self.alloc, &leaves[move.to_index], move.vector_id);
        }
        try self.putVecLeaf(txn, move.vector_id, leaves[move.to_index].id);
        changed[move.from_index] = true;
        changed[move.to_index] = true;
        applied += 1;
    }
    if (applied == 0) return result;

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
    result.moved_vectors = applied;
    return result;
}

pub fn repairDirtyPostingsTxnWithOptions(
    self: anytype,
    txn: anytype,
    options: posting.PostingMaintenanceOptions,
) !posting.PostingMaintenanceResult {
    var result: posting.PostingMaintenanceResult = .{};
    if (options.max_postings == 0) {
        try noteRemainingPostingMaintenanceDebt(self, txn, options, &result);
        result.limit_reached = true;
        return result;
    }

    var layout_changes: usize = 0;
    var boundary_moves: usize = 0;
    var forced_delta_folds_remaining: usize = 0;
    if (options.fold_delta_tails and
        options.max_delta_tail_postings != std.math.maxInt(usize) and
        self.config.posting_storage_mode == .base_delta and
        comptime posting.txnSupportsDeltaTailScan(@TypeOf(txn)))
    {
        const stats = try postingBacklogStatsTxn(self, txn);
        if (stats.delta_tail_postings > options.max_delta_tail_postings) {
            forced_delta_folds_remaining = @intCast(stats.delta_tail_postings - options.max_delta_tail_postings);
        }
    }

    var stop_scanning = false;
    while (true) {
        const pass_layout_changes_before = layout_changes;
        const split_full_this_pass = split_full_blk: {
            if (!options.rebalance_layout or !options.split_full_postings or layout_changes >= options.max_layout_changes) {
                break :split_full_blk false;
            }
            const stats = try postingBacklogStatsTxn(self, txn);
            break :split_full_blk stats.overfull_postings == 0;
        };

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
                if (node.members.len > self.config.leaf_size or
                    (split_full_this_pass and node.members.len == self.config.leaf_size))
                {
                    const old_parent_id = node.parent;
                    const split_options: hbc_runtime.BatchInsertOptions = .{
                        .capacity_balance_leaf_split = true,
                    };
                    try self.splitLeafWithOptions(txn, &node, split_options);
                    result.split_postings += 1;
                    layout_changes += 1;
                    if (boundary_moves < options.max_boundary_reassignments) {
                        const parent_id = if (old_parent_id == 0) self.metadata.root_node else old_parent_id;
                        const reassigned = try targetedBoundaryReassignParent(
                            self,
                            txn,
                            parent_id,
                            options.max_boundary_reassignments - boundary_moves,
                            false,
                            options.max_overfull_reassignment_postings,
                            options.max_over_capacity_reassignment_members,
                            options.boundary_reassignment_min_improvement,
                        );
                        boundary_moves += reassigned.moved_vectors;
                        result.boundary_reassigned_vectors += @intCast(reassigned.moved_vectors);
                        result.boundary_reassignment_capacity_skips += @intCast(reassigned.capacity_skips);
                        result.boundary_reassignment_min_source_skips += @intCast(reassigned.min_source_skips);
                        result.boundary_reassignment_swap_moves += @intCast(reassigned.swap_moves);
                    }
                    continue;
                }

                if (try mergeUnderfullPostingWithNearestSibling(self, txn, &node)) {
                    result.merged_postings += 1;
                    layout_changes += 1;
                    if (boundary_moves < options.max_boundary_reassignments) {
                        const reassigned = try targetedBoundaryReassignParent(
                            self,
                            txn,
                            node.parent,
                            options.max_boundary_reassignments - boundary_moves,
                            false,
                            options.max_overfull_reassignment_postings,
                            options.max_over_capacity_reassignment_members,
                            options.boundary_reassignment_min_improvement,
                        );
                        boundary_moves += reassigned.moved_vectors;
                        result.boundary_reassigned_vectors += @intCast(reassigned.moved_vectors);
                        result.boundary_reassignment_capacity_skips += @intCast(reassigned.capacity_skips);
                        result.boundary_reassignment_min_source_skips += @intCast(reassigned.min_source_skips);
                        result.boundary_reassignment_swap_moves += @intCast(reassigned.swap_moves);
                    }
                    continue;
                }
            } else if (options.rebalance_layout and layout_changes >= options.max_layout_changes) {
                result.limit_reached = true;
            }

            if (!node.posting_state.dirty) {
                if (options.fold_delta_tails and successfulDeltaFoldCalls(result) >= options.max_postings) {
                    result.limit_reached = true;
                    if (options.rebalance_layout and layout_changes < options.max_layout_changes) {
                        continue;
                    }
                    stop_scanning = true;
                    break;
                }
                const force_fold = forced_delta_folds_remaining != 0;
                const folds_before = successfulDeltaFoldCalls(result);
                try foldPostingDeltaTailIfNeeded(self, txn, node.id, options, force_fold, &result);
                if (force_fold and successfulDeltaFoldCalls(result) > folds_before) {
                    forced_delta_folds_remaining -|= 1;
                }
                continue;
            }

            result.dirty_postings += 1;
            if (result.repaired_postings >= options.max_postings) {
                result.limit_reached = true;
                if (options.rebalance_layout and layout_changes < options.max_layout_changes) {
                    continue;
                }
                stop_scanning = true;
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
            if ((self.config.posting_storage_mode == .shadow_base_delta or
                self.config.posting_storage_mode == .base_delta) and
                refreshed_centroid)
            {
                try posting.PostingStore.saveCentroidDirectoryRecord(self, txn, .{
                    .posting_id = node.id,
                    .generation = node.posting_state.centroid_version,
                    .mutation_version = node.posting_state.mutation_version,
                    .payload_version = node.posting_state.payload_version,
                    .flags = centroidDirectoryFlagsFromPostingState(node.posting_state),
                    .parent = node.parent,
                    .level = node.level,
                    .member_count = node.members.len,
                    .bounds_radius = try leafBoundsRadiusFromStorage(self, txn, &node),
                    .centroid = node.centroid,
                });
            }
            try posting.PostingStore.saveState(self, txn, node.id, node.posting_state);
            try self.cacheNode(&node);

            const force_fold = forced_delta_folds_remaining != 0;
            const folds_before = successfulDeltaFoldCalls(result);
            try foldPostingDeltaTailIfNeeded(self, txn, node.id, options, force_fold, &result);
            if (force_fold and successfulDeltaFoldCalls(result) > folds_before) {
                forced_delta_folds_remaining -|= 1;
            }

            if (refreshed_centroid and options.refresh_ancestors and node.parent != 0) {
                try recomputeAncestorCentroids(self, txn, node.parent, .{});
                result.ancestor_refresh_roots += 1;
            }

            if (options.reassign_dirty_postings and
                refreshed_centroid and
                node.parent != 0 and
                boundary_moves < options.max_boundary_reassignments)
            {
                const reassigned = try targetedBoundaryReassignParent(
                    self,
                    txn,
                    node.parent,
                    options.max_boundary_reassignments - boundary_moves,
                    options.allow_overfull_reassignment,
                    options.max_overfull_reassignment_postings,
                    options.max_over_capacity_reassignment_members,
                    options.boundary_reassignment_min_improvement,
                );
                boundary_moves += reassigned.moved_vectors;
                result.boundary_reassigned_vectors += @intCast(reassigned.moved_vectors);
                result.boundary_reassignment_capacity_skips += @intCast(reassigned.capacity_skips);
                result.boundary_reassignment_min_source_skips += @intCast(reassigned.min_source_skips);
                result.boundary_reassignment_swap_moves += @intCast(reassigned.swap_moves);
            }

            if (refreshed_centroid or refreshed_payload) {
                result.repaired_postings += 1;
            }
        }

        if (stop_scanning or !options.rebalance_layout or layout_changes >= options.max_layout_changes or layout_changes == pass_layout_changes_before) break;
    }

    try noteRemainingPostingMaintenanceDebt(self, txn, options, &result);

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
    self.write_profile.posting_maintenance_boundary_reassignment_capacity_skips += result.boundary_reassignment_capacity_skips;
    self.write_profile.posting_maintenance_boundary_reassignment_min_source_skips += result.boundary_reassignment_min_source_skips;
    self.write_profile.posting_maintenance_boundary_reassignment_swap_moves += result.boundary_reassignment_swap_moves;
    self.write_profile.posting_maintenance_delta_fold_attempts += result.delta_fold_attempts;
    self.write_profile.posting_maintenance_delta_fold_skipped += result.delta_fold_skipped;
    self.write_profile.posting_maintenance_delta_fold_records += result.delta_fold_records;
    self.write_profile.posting_delta_fold_peak_scratch_bytes = @max(
        self.write_profile.posting_delta_fold_peak_scratch_bytes,
        result.delta_fold_peak_scratch_bytes,
    );

    return result;
}

fn noteRemainingPostingMaintenanceDebt(
    self: anytype,
    txn: anytype,
    options: posting.PostingMaintenanceOptions,
    result: *posting.PostingMaintenanceResult,
) !void {
    const stats = try postingBacklogStatsTxn(self, txn);
    result.remaining_dirty_postings = stats.dirty_postings;
    result.remaining_delta_tail_postings = stats.delta_tail_postings;
    result.remaining_overfull_postings = stats.overfull_postings;
    result.remaining_postings_at_capacity = stats.postings_at_capacity;
    result.remaining_max_over_capacity_members = stats.max_over_capacity_members;

    const layout_changes = result.split_postings + result.merged_postings;
    result.limit_reached =
        (stats.dirty_postings != 0 and result.repaired_postings >= options.max_postings) or
        (options.rebalance_layout and stats.overfull_postings != 0) or
        (options.rebalance_layout and
            options.split_full_postings and
            stats.postings_at_capacity != 0 and
            layout_changes >= options.max_layout_changes) or
        (options.fold_delta_tails and stats.delta_tail_postings > options.max_delta_tail_postings);
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

test "centroid directory record state preserves payload freshness" {
    var record = posting.OwnedCentroidDirectoryRecord{
        .posting_id = 7,
        .generation = 11,
        .mutation_version = 13,
        .payload_version = 13,
        .flags = posting.CentroidDirectoryFormat.dirty_flag | posting.CentroidDirectoryFormat.centroid_dirty_flag,
        .parent = 3,
        .level = 1,
        .member_count = 16,
        .bounds_radius = 0,
        .centroid = &.{},
    };

    const state = postingStateFromCentroidDirectoryRecord(&record);
    try std.testing.expectEqual(@as(u64, 13), state.mutation_version);
    try std.testing.expectEqual(@as(u64, 11), state.centroid_version);
    try std.testing.expectEqual(@as(u64, 13), state.payload_version);
    try std.testing.expect(state.dirty);
    try std.testing.expect(state.centroid_dirty);
    try std.testing.expect(!state.payload_dirty);
}

test "adaptive flat centroid block probing expands ambiguous boundary" {
    const probes = [_]FlatCentroidProbe{
        .{ .posting_id = 1, .distance = 1.0, .error_bound = 0.10 },
        .{ .posting_id = 2, .distance = 1.1, .error_bound = 0.10 },
        .{ .posting_id = 3, .distance = 1.3, .error_bound = 0.15 },
        .{ .posting_id = 4, .distance = 3.0, .error_bound = 0.05 },
    };

    try std.testing.expectEqual(@as(usize, 3), adaptiveFlatCentroidBlockProbeCount(&probes, 2));
}

test "flat probe collector keeps bounded lowest lower bounds" {
    var storage: [3]FlatCentroidProbe = undefined;
    var collector = FlatProbeCollector.init(&storage);

    collector.insert(.{ .posting_id = 1, .distance = 10, .error_bound = 0 });
    collector.insert(.{ .posting_id = 2, .distance = 5, .error_bound = 0 });
    collector.insert(.{ .posting_id = 3, .distance = 7, .error_bound = 0 });
    collector.insert(.{ .posting_id = 4, .distance = 3, .error_bound = 0 });
    collector.insert(.{ .posting_id = 5, .distance = 8, .error_bound = 0 });
    collector.insert(.{ .posting_id = 6, .distance = 1, .error_bound = 0 });
    collector.insert(.{ .posting_id = 7, .distance = 5, .error_bound = 0 });

    try std.testing.expect(collector.wouldRejectLowerBound(5));
    try std.testing.expect(!collector.wouldRejectLowerBound(4));

    const items = collector.items();
    std.mem.sort(FlatCentroidProbe, items, {}, flatProbeLess);
    try std.testing.expectEqual(@as(usize, 3), items.len);
    try std.testing.expectEqual(@as(u64, 6), items[0].posting_id);
    try std.testing.expectEqual(@as(u64, 4), items[1].posting_id);
    try std.testing.expectEqual(@as(u64, 2), items[2].posting_id);
}

test "flat probe collector candidates sort by lower bound before exact scoring" {
    var storage: [3]FlatCentroidProbe = undefined;
    var collector = FlatProbeCollector.init(&storage);

    collector.insert(.{ .posting_id = 1, .distance = 9, .error_bound = 0 });
    collector.insert(.{ .posting_id = 2, .distance = 5, .error_bound = 0 });
    collector.insert(.{ .posting_id = 3, .distance = 7, .error_bound = 0 });
    collector.insert(.{ .posting_id = 4, .distance = 3, .error_bound = 0 });

    const items = collector.items();
    std.mem.sort(FlatCentroidProbe, items, {}, flatProbeLess);
    try std.testing.expectEqual(@as(u64, 4), items[0].posting_id);
    try std.testing.expectEqual(@as(u64, 2), items[1].posting_id);
    try std.testing.expectEqual(@as(u64, 3), items[2].posting_id);
}

test "flat probe collector rejection is monotonic for sorted lower bounds" {
    var storage: [2]FlatCentroidProbe = undefined;
    var collector = FlatProbeCollector.init(&storage);

    collector.insert(.{ .posting_id = 1, .distance = 1, .error_bound = 0 });
    collector.insert(.{ .posting_id = 2, .distance = 2, .error_bound = 0 });

    try std.testing.expect(!collector.wouldRejectLowerBound(1));
    try std.testing.expect(collector.wouldRejectLowerBound(2));
    try std.testing.expect(collector.wouldRejectLowerBound(3));
}

test "centroid block probe collector preserves distance tie ordering" {
    var storage: [2]FlatCentroidProbe = undefined;
    var collector = CentroidBlockProbeCollector.init(&storage);

    collector.insert(.{ .posting_id = 1, .distance = 5, .error_bound = 1 }); // lower = 4
    collector.insert(.{ .posting_id = 2, .distance = 4, .error_bound = 0 }); // lower = 4, better tie
    collector.insert(.{ .posting_id = 3, .distance = 6, .error_bound = 2 }); // lower = 4, worse tie
    collector.insert(.{ .posting_id = 4, .distance = 3, .error_bound = 0 }); // lower = 3

    const items = collector.items();
    std.mem.sort(FlatCentroidProbe, items, {}, centroidBlockProbeLess);
    try std.testing.expectEqual(@as(usize, 2), items.len);
    try std.testing.expectEqual(@as(u64, 4), items[0].posting_id);
    try std.testing.expectEqual(@as(u64, 2), items[1].posting_id);
}

test "adaptive flat centroid block probing stops at clear margin" {
    const probes = [_]FlatCentroidProbe{
        .{ .posting_id = 1, .distance = 1.0, .error_bound = 0.05 },
        .{ .posting_id = 2, .distance = 1.2, .error_bound = 0.05 },
        .{ .posting_id = 3, .distance = 2.0, .error_bound = 0.05 },
    };

    try std.testing.expectEqual(@as(usize, 2), adaptiveFlatCentroidBlockProbeCount(&probes, 2));
}

test "adaptive flat centroid block probing caps ambiguous expansion" {
    const probes = [_]FlatCentroidProbe{
        .{ .posting_id = 1, .distance = 1.0, .error_bound = 10.0 },
        .{ .posting_id = 2, .distance = 1.1, .error_bound = 10.0 },
        .{ .posting_id = 3, .distance = 1.2, .error_bound = 10.0 },
        .{ .posting_id = 4, .distance = 1.3, .error_bound = 10.0 },
        .{ .posting_id = 5, .distance = 1.4, .error_bound = 10.0 },
        .{ .posting_id = 6, .distance = 1.5, .error_bound = 10.0 },
        .{ .posting_id = 7, .distance = 1.6, .error_bound = 10.0 },
        .{ .posting_id = 8, .distance = 1.7, .error_bound = 10.0 },
    };

    try std.testing.expectEqual(@as(usize, 4), adaptiveFlatCentroidBlockProbeCount(&probes, 2));
}

test "flat centroid block probe candidate buffer follows adaptive expansion cap" {
    try std.testing.expectEqual(@as(usize, 0), flatCentroidBlockProbeCandidateCount(false, 0, 7));
    try std.testing.expectEqual(@as(usize, 7), flatCentroidBlockProbeCandidateCount(true, 47, 7));
    try std.testing.expectEqual(@as(usize, 14), flatCentroidBlockProbeCandidateCount(false, 47, 7));
    try std.testing.expectEqual(@as(usize, 47), flatCentroidBlockProbeCandidateCount(false, 47, 32));
    try std.testing.expectEqual(@as(usize, 47), flatCentroidBlockProbeCandidateCount(true, 47, 94));
}

test "flat centroid query stack covers default 2x posting candidate window" {
    try std.testing.expect(flat_centroid_query_probe_stack_capacity >= 512);
}

test "centroid block probe radius expands l2 squared ambiguity" {
    try std.testing.expectEqual(@as(f32, 0), centroidBlockProbeErrorBound(.l2_squared, 16, 0));
    try std.testing.expectEqual(@as(f32, 20), centroidBlockProbeErrorBound(.l2_squared, 16, 4));
    try std.testing.expectEqual(@as(f32, 0.25), centroidBlockProbeErrorBound(.cosine, 16, 0.25));
    try std.testing.expectEqual(@as(f32, 0), centroidBlockProbeErrorBound(.inner_product, 16, 4));
}

test "effective centroid block probing full-scans small directories and prunes 1m vdbb shape" {
    try std.testing.expectEqual(@as(usize, 16), effectiveFlatCentroidBlockProbeCount(false, 0, 16, 8, 32));
    try std.testing.expectEqual(@as(usize, 12), effectiveFlatCentroidBlockProbeCount(true, 12, 16, 8, 32));

    const posting_count = 5953; // roughly 1M vectors at the VDBB leaf_size=168 shape.
    const block_count = std.math.divCeil(usize, posting_count, 128) catch unreachable;
    try std.testing.expectEqual(@as(usize, 47), block_count);
    try std.testing.expectEqual(@as(usize, 7), defaultFlatCentroidBlockProbeCount(block_count, 128, 32));
    try std.testing.expectEqual(@as(usize, 7), effectiveFlatCentroidBlockProbeCount(false, 0, block_count, 128, 32));
}

test "global posting quantized payload is only built for deterministic full block scans" {
    const TestIndex = struct {
        config: types.HBCConfig = .{
            .use_quantization = true,
            .centroid_directory_mode = .two_level_rabitq,
            .flat_centroid_block_size = 128,
            .flat_centroid_probe_count = 256,
            .flat_centroid_block_probe_count = 0,
            .search_width = 256,
        },
    };

    var index = TestIndex{};
    try std.testing.expect(!shouldBuildGlobalPostingQuantized(&index, 47, 5953));
    index.config.flat_centroid_probe_count = 5953;
    try std.testing.expect(!shouldBuildGlobalPostingQuantized(&index, 47, 5953));

    index.config.flat_centroid_block_probe_count = 94;
    try std.testing.expect(shouldBuildGlobalPostingQuantized(&index, 47, 5953));

    index.config.flat_centroid_block_probe_count = 0;
    index.config.flat_centroid_probe_count = 256;
    try std.testing.expect(shouldBuildGlobalPostingQuantized(&index, small_two_level_full_scan_blocks, 1024));

    index.config.centroid_directory_mode = .flat_rabitq;
    try std.testing.expect(shouldBuildGlobalPostingQuantized(&index, 47, 5953));

    index.config.use_quantization = false;
    try std.testing.expect(!shouldBuildGlobalPostingQuantized(&index, 47, 5953));
}

test "flat centroid block vector source maps regular and irregular offsets" {
    const ids_a = [_]u64{ 1, 2 };
    const ids_b = [_]u64{3};
    const centroids_a = [_]f32{
        1, 2,
        3, 4,
    };
    const centroids_b = [_]f32{
        5, 6,
    };
    const regular_blocks = [_]FlatCentroidBlock{
        .{
            .posting_ids = ids_a[0..],
            .parents = &.{},
            .levels = &.{},
            .states = &.{},
            .posting_offset = 0,
            .centroid = &.{},
            .radii = &.{},
            .centroids = centroids_a[0..],
            .centroid_measures = &.{},
            .quantized = .{},
        },
        .{
            .posting_ids = ids_b[0..],
            .parents = &.{},
            .levels = &.{},
            .states = &.{},
            .posting_offset = 2,
            .centroid = &.{},
            .radii = &.{},
            .centroids = centroids_b[0..],
            .centroid_measures = &.{},
            .quantized = .{},
        },
    };
    const regular = FlatCentroidBlocksVectorSource{
        .blocks = regular_blocks[0..],
        .dims = 2,
        .total_count = 3,
    };
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2 }, regular.vectorAt(0));
    try std.testing.expectEqualSlices(f32, &[_]f32{ 3, 4 }, regular.vectorAt(1));
    try std.testing.expectEqualSlices(f32, &[_]f32{ 5, 6 }, regular.vectorAt(2));

    const ids_c = [_]u64{4};
    const ids_d = [_]u64{ 5, 6 };
    const centroids_c = [_]f32{
        7, 8,
    };
    const centroids_d = [_]f32{
        9,  10,
        11, 12,
    };
    const irregular_blocks = [_]FlatCentroidBlock{
        .{
            .posting_ids = ids_c[0..],
            .parents = &.{},
            .levels = &.{},
            .states = &.{},
            .posting_offset = 0,
            .centroid = &.{},
            .radii = &.{},
            .centroids = centroids_c[0..],
            .centroid_measures = &.{},
            .quantized = .{},
        },
        .{
            .posting_ids = ids_d[0..],
            .parents = &.{},
            .levels = &.{},
            .states = &.{},
            .posting_offset = 1,
            .centroid = &.{},
            .radii = &.{},
            .centroids = centroids_d[0..],
            .centroid_measures = &.{},
            .quantized = .{},
        },
    };
    const irregular = FlatCentroidBlocksVectorSource{
        .blocks = irregular_blocks[0..],
        .dims = 2,
        .total_count = 3,
    };
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7, 8 }, irregular.vectorAt(0));
    try std.testing.expectEqualSlices(f32, &[_]f32{ 9, 10 }, irregular.vectorAt(1));
    try std.testing.expectEqualSlices(f32, &[_]f32{ 11, 12 }, irregular.vectorAt(2));
}

test "flat centroid block ordering uses widest centroid dimension" {
    var a = [_]f32{ 0, 0 };
    var b = [_]f32{ 1, 10 };
    var c = [_]f32{ 2, -10 };
    var entries = [_]FlatCentroidEntry{
        .{ .posting_id = 1, .centroid = a[0..] },
        .{ .posting_id = 2, .centroid = b[0..] },
        .{ .posting_id = 3, .centroid = c[0..] },
    };

    const sort_dim = chooseFlatCentroidSortDimension(entries[0..], 2);
    try std.testing.expectEqual(@as(usize, 1), sort_dim);
    std.mem.sort(FlatCentroidEntry, entries[0..], FlatCentroidEntrySortContext{ .dim = sort_dim }, flatCentroidEntryLess);
    try std.testing.expectEqual(@as(u64, 3), entries[0].posting_id);
    try std.testing.expectEqual(@as(u64, 1), entries[1].posting_id);
    try std.testing.expectEqual(@as(u64, 2), entries[2].posting_id);
}

test "flat centroid entry sort keys can share one backing allocation" {
    const alloc = std.testing.allocator;
    var entries = std.ArrayListUnmanaged(FlatCentroidEntry).empty;
    defer deinitFlatCentroidEntries(alloc, &entries);

    const key_storage = try alloc.alloc(u8, 6);
    @memcpy(key_storage, &[_]u8{ 2, 0, 1, 0, 3, 0 });
    try entries.append(alloc, .{
        .posting_id = 1,
        .parent = 0,
        .level = 0,
        .state = .{},
        .centroid = try alloc.dupe(f32, &[_]f32{0}),
        .sort_key = key_storage[0..2],
        .sort_key_storage = key_storage,
    });
    try entries.append(alloc, .{
        .posting_id = 2,
        .parent = 0,
        .level = 0,
        .state = .{},
        .centroid = try alloc.dupe(f32, &[_]f32{0}),
        .sort_key = key_storage[2..4],
    });
    try entries.append(alloc, .{
        .posting_id = 3,
        .parent = 0,
        .level = 0,
        .state = .{},
        .centroid = try alloc.dupe(f32, &[_]f32{0}),
        .sort_key = key_storage[4..6],
    });

    std.mem.sort(FlatCentroidEntry, entries.items, FlatCentroidEntrySortContext{ .dim = 0 }, flatCentroidEntryLess);
    try std.testing.expectEqual(@as(u64, 2), entries.items[0].posting_id);
    try std.testing.expectEqual(@as(u64, 1), entries.items[1].posting_id);
    try std.testing.expectEqual(@as(u64, 3), entries.items[2].posting_id);
}

test "planned boundary reassignment enforces overfull debt limits" {
    var centroid: [0]f32 = .{};
    var children: [0]u64 = .{};
    var a_members = [_]u64{ 1, 2, 3, 4, 5 };
    var b_members = [_]u64{ 6, 7, 8, 9 };
    var c_members = [_]u64{ 10, 11 };
    var leaves = [_]types.Node{
        .{ .id = 1, .is_leaf = true, .level = 0, .parent = 9, .centroid = centroid[0..], .children = children[0..], .members = a_members[0..] },
        .{ .id = 2, .is_leaf = true, .level = 0, .parent = 9, .centroid = centroid[0..], .children = children[0..], .members = b_members[0..] },
        .{ .id = 3, .is_leaf = true, .level = 0, .parent = 9, .centroid = centroid[0..], .children = children[0..], .members = c_members[0..] },
    };
    var planned_in = [_]usize{ 0, 0, 0 };
    var planned_out = [_]usize{ 0, 0, 0 };

    try std.testing.expect(plannedDirectMoveExceedsOverfullLimits(
        leaves[0..],
        planned_in[0..],
        planned_out[0..],
        1,
        4,
        1,
        1,
    ));

    try std.testing.expect(!plannedDirectMoveExceedsOverfullLimits(
        leaves[0..],
        planned_in[0..],
        planned_out[0..],
        1,
        4,
        2,
        1,
    ));

    try std.testing.expect(plannedDirectMoveExceedsOverfullLimits(
        leaves[0..],
        planned_in[0..],
        planned_out[0..],
        1,
        4,
        2,
        0,
    ));

    planned_out[0] = 1;
    try std.testing.expect(!plannedDirectMoveExceedsOverfullLimits(
        leaves[0..],
        planned_in[0..],
        planned_out[0..],
        1,
        4,
        1,
        1,
    ));

    try std.testing.expect(directMoveExceedsOverfullLimits(leaves[0..], 1, 4, 1, 1));
    try std.testing.expect(!directMoveExceedsOverfullLimits(leaves[0..], 1, 4, 2, 1));
}

test "boundary reassignment without overfull uses capacity-neutral swap" {
    const alloc = std.testing.allocator;

    const TestIndex = struct {
        alloc: std.mem.Allocator,
        config: types.HBCConfig = .{
            .dims = 2,
            .leaf_size = 2,
            .branching_factor = 2,
            .search_width = 2,
            .use_quantization = false,
        },
        metadata: hbc.IndexMetadata = .{
            .dims = 2,
            .root_node = 9,
            .node_count = 9,
            .active_count = 4,
        },
        write_profile: hbc_runtime.WriteProfile = .{},
        parent: types.Node,
        left: types.Node,
        right: types.Node,
        assignments: [16]u64 = .{0} ** 16,

        fn deinit(self: *@This()) void {
            self.parent.deinit(self.alloc);
            self.left.deinit(self.alloc);
            self.right.deinit(self.alloc);
        }

        fn loadNode(self: *@This(), _: anytype, node_id: u64) !types.Node {
            return switch (node_id) {
                1 => try self.left.clone(self.alloc),
                2 => try self.right.clone(self.alloc),
                9 => try self.parent.clone(self.alloc),
                else => error.NotFound,
            };
        }

        fn getVectorScratch(_: *@This(), _: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
            const vector: []const f32 = switch (vector_id) {
                1 => &[_]f32{ 0.0, 0.0 },
                2 => &[_]f32{ 10.0, 0.0 },
                3 => &[_]f32{ 10.2, 0.0 },
                4 => &[_]f32{ 0.2, 0.0 },
                else => return error.NotFound,
            };
            if (scratch.len < vector.len) return error.BufferTooSmall;
            @memcpy(scratch[0..vector.len], vector);
            return scratch[0..vector.len];
        }

        fn transformVector(_: *@This(), vector: []const f32, scratch: []f32) []const f32 {
            @memcpy(scratch[0..vector.len], vector);
            return scratch[0..vector.len];
        }

        fn minLeafOccupancy(_: *@This()) usize {
            return 1;
        }

        fn putVecLeaf(self: *@This(), _: anytype, vector_id: u64, leaf_id: u64) !void {
            self.assignments[@intCast(vector_id)] = leaf_id;
        }

        fn recomputeInternalCentroid(_: *@This(), _: anytype, _: *types.Node) !void {}

        fn saveNodeWithOptionsMode(self: *@This(), _: anytype, node: *const types.Node, _: hbc_runtime.BatchInsertOptions, _: bool) !void {
            const clone = try node.clone(self.alloc);
            switch (node.id) {
                1 => {
                    self.left.deinit(self.alloc);
                    self.left = clone;
                },
                2 => {
                    self.right.deinit(self.alloc);
                    self.right = clone;
                },
                9 => {
                    self.parent.deinit(self.alloc);
                    self.parent = clone;
                },
                else => return error.NotFound,
            }
        }

        fn cacheNode(_: *@This(), _: *const types.Node) !void {}
    };

    const parent_centroid = try alloc.dupe(f32, &[_]f32{ 5.0, 0.0 });
    errdefer alloc.free(parent_centroid);
    const parent_children = try alloc.dupe(u64, &[_]u64{ 1, 2 });
    errdefer alloc.free(parent_children);
    const left_centroid = try alloc.dupe(f32, &[_]f32{ 0.0, 0.0 });
    errdefer alloc.free(left_centroid);
    const left_members = try alloc.dupe(u64, &[_]u64{ 1, 2 });
    errdefer alloc.free(left_members);
    const right_centroid = try alloc.dupe(f32, &[_]f32{ 10.0, 0.0 });
    errdefer alloc.free(right_centroid);
    const right_members = try alloc.dupe(u64, &[_]u64{ 3, 4 });
    errdefer alloc.free(right_members);

    var index = TestIndex{
        .alloc = alloc,
        .parent = .{
            .id = 9,
            .is_leaf = false,
            .level = 1,
            .parent = 0,
            .centroid = parent_centroid,
            .children = parent_children,
            .members = &.{},
        },
        .left = .{
            .id = 1,
            .is_leaf = true,
            .level = 0,
            .parent = 9,
            .centroid = left_centroid,
            .children = &.{},
            .members = left_members,
        },
        .right = .{
            .id = 2,
            .is_leaf = true,
            .level = 0,
            .parent = 9,
            .centroid = right_centroid,
            .children = &.{},
            .members = right_members,
        },
    };
    defer index.deinit();
    index.assignments[1] = 1;
    index.assignments[2] = 1;
    index.assignments[3] = 2;
    index.assignments[4] = 2;

    const result = try targetedBoundaryReassignParent(
        &index,
        {},
        9,
        1,
        false,
        0,
        0,
        0.0,
    );

    try std.testing.expectEqual(@as(usize, 1), result.moved_vectors);
    try std.testing.expectEqual(@as(usize, 1), result.swap_moves);
    try std.testing.expectEqual(@as(usize, 0), result.capacity_skips);
    try std.testing.expectEqual(@as(usize, 2), index.left.members.len);
    try std.testing.expectEqual(@as(usize, 2), index.right.members.len);
    try std.testing.expect(std.mem.indexOfScalar(u64, index.left.members, 4) != null);
    try std.testing.expect(std.mem.indexOfScalar(u64, index.right.members, 2) != null);
    try std.testing.expectEqual(@as(u64, 2), index.assignments[2]);
    try std.testing.expectEqual(@as(u64, 1), index.assignments[4]);
}

test "boundary reassignment without overfull skips when full target has no useful swap" {
    const alloc = std.testing.allocator;

    const TestIndex = struct {
        alloc: std.mem.Allocator,
        config: types.HBCConfig = .{
            .dims = 2,
            .leaf_size = 2,
            .branching_factor = 2,
            .search_width = 2,
            .use_quantization = false,
        },
        metadata: hbc.IndexMetadata = .{
            .dims = 2,
            .root_node = 9,
            .node_count = 9,
            .active_count = 4,
        },
        write_profile: hbc_runtime.WriteProfile = .{},
        parent: types.Node,
        left: types.Node,
        right: types.Node,
        assignments: [16]u64 = .{0} ** 16,

        fn deinit(self: *@This()) void {
            self.parent.deinit(self.alloc);
            self.left.deinit(self.alloc);
            self.right.deinit(self.alloc);
        }

        fn loadNode(self: *@This(), _: anytype, node_id: u64) !types.Node {
            return switch (node_id) {
                1 => try self.left.clone(self.alloc),
                2 => try self.right.clone(self.alloc),
                9 => try self.parent.clone(self.alloc),
                else => error.NotFound,
            };
        }

        fn getVectorScratch(_: *@This(), _: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
            const vector: []const f32 = switch (vector_id) {
                1 => &[_]f32{ 0.0, 0.0 },
                2 => &[_]f32{ 10.0, 0.0 },
                3 => &[_]f32{ 10.2, 0.0 },
                4 => &[_]f32{ 10.4, 0.0 },
                else => return error.NotFound,
            };
            if (scratch.len < vector.len) return error.BufferTooSmall;
            @memcpy(scratch[0..vector.len], vector);
            return scratch[0..vector.len];
        }

        fn transformVector(_: *@This(), vector: []const f32, scratch: []f32) []const f32 {
            @memcpy(scratch[0..vector.len], vector);
            return scratch[0..vector.len];
        }

        fn minLeafOccupancy(_: *@This()) usize {
            return 1;
        }

        fn putVecLeaf(self: *@This(), _: anytype, vector_id: u64, leaf_id: u64) !void {
            self.assignments[@intCast(vector_id)] = leaf_id;
        }

        fn recomputeInternalCentroid(_: *@This(), _: anytype, _: *types.Node) !void {}

        fn saveNodeWithOptionsMode(self: *@This(), _: anytype, node: *const types.Node, _: hbc_runtime.BatchInsertOptions, _: bool) !void {
            const clone = try node.clone(self.alloc);
            switch (node.id) {
                1 => {
                    self.left.deinit(self.alloc);
                    self.left = clone;
                },
                2 => {
                    self.right.deinit(self.alloc);
                    self.right = clone;
                },
                9 => {
                    self.parent.deinit(self.alloc);
                    self.parent = clone;
                },
                else => return error.NotFound,
            }
        }

        fn cacheNode(_: *@This(), _: *const types.Node) !void {}
    };

    const parent_centroid = try alloc.dupe(f32, &[_]f32{ 5.0, 0.0 });
    errdefer alloc.free(parent_centroid);
    const parent_children = try alloc.dupe(u64, &[_]u64{ 1, 2 });
    errdefer alloc.free(parent_children);
    const left_centroid = try alloc.dupe(f32, &[_]f32{ 0.0, 0.0 });
    errdefer alloc.free(left_centroid);
    const left_members = try alloc.dupe(u64, &[_]u64{ 1, 2 });
    errdefer alloc.free(left_members);
    const right_centroid = try alloc.dupe(f32, &[_]f32{ 10.0, 0.0 });
    errdefer alloc.free(right_centroid);
    const right_members = try alloc.dupe(u64, &[_]u64{ 3, 4 });
    errdefer alloc.free(right_members);

    var index = TestIndex{
        .alloc = alloc,
        .parent = .{
            .id = 9,
            .is_leaf = false,
            .level = 1,
            .parent = 0,
            .centroid = parent_centroid,
            .children = parent_children,
            .members = &.{},
        },
        .left = .{
            .id = 1,
            .is_leaf = true,
            .level = 0,
            .parent = 9,
            .centroid = left_centroid,
            .children = &.{},
            .members = left_members,
        },
        .right = .{
            .id = 2,
            .is_leaf = true,
            .level = 0,
            .parent = 9,
            .centroid = right_centroid,
            .children = &.{},
            .members = right_members,
        },
    };
    defer index.deinit();
    index.assignments[1] = 1;
    index.assignments[2] = 1;
    index.assignments[3] = 2;
    index.assignments[4] = 2;

    const result = try targetedBoundaryReassignParent(
        &index,
        {},
        9,
        1,
        false,
        0,
        0,
        0.0,
    );

    try std.testing.expectEqual(@as(usize, 0), result.moved_vectors);
    try std.testing.expectEqual(@as(usize, 0), result.swap_moves);
    try std.testing.expect(result.capacity_skips > 0);
    try std.testing.expectEqual(@as(usize, 2), index.left.members.len);
    try std.testing.expectEqual(@as(usize, 2), index.right.members.len);
    try std.testing.expect(std.mem.indexOfScalar(u64, index.left.members, 2) != null);
    try std.testing.expect(std.mem.indexOfScalar(u64, index.right.members, 3) != null);
    try std.testing.expect(std.mem.indexOfScalar(u64, index.right.members, 4) != null);
    try std.testing.expectEqual(@as(u64, 1), index.assignments[2]);
    try std.testing.expectEqual(@as(u64, 2), index.assignments[3]);
    try std.testing.expectEqual(@as(u64, 2), index.assignments[4]);
}
