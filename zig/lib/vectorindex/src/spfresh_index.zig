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
const builtin = @import("builtin");
const types = @import("types.zig");
const hbc = @import("hbc.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const posting = @import("posting.zig");
const search_types = @import("search_types.zig");
const proto = @import("antfly_vector").proto;
const vec = @import("antfly_vector").vector;

pub const FlatCentroidBlock = struct {
    posting_ids: []u64,
    covering_radii: []f32,
    quantized: proto.RaBitQuantizedVectorSet,

    fn deinit(self: *FlatCentroidBlock, alloc: std.mem.Allocator) void {
        alloc.free(self.posting_ids);
        alloc.free(self.covering_radii);
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
    missing_node_count: usize = 0,
    invalid_posting_count: usize = 0,
    accounting_context: ?*anyopaque = null,
    release_accounting: ?*const fn (*anyopaque, u64) void = null,
    accounted_bytes: u64 = 0,

    pub fn retain(self: *FlatCentroidDirectory) void {
        _ = self.ref_count.fetchAdd(1, .acq_rel);
    }

    pub fn release(self: *FlatCentroidDirectory, alloc: std.mem.Allocator) void {
        if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
        self.deinit(alloc);
        alloc.destroy(self);
    }

    fn deinit(self: *FlatCentroidDirectory, alloc: std.mem.Allocator) void {
        const accounting_context = self.accounting_context;
        const release_accounting = self.release_accounting;
        const accounted_bytes = self.accounted_bytes;
        for (self.blocks) |*block| block.deinit(alloc);
        alloc.free(self.blocks);
        self.* = .{};
        if (release_accounting) |release_fn| release_fn(accounting_context.?, accounted_bytes);
    }

    fn complete(self: *const FlatCentroidDirectory) bool {
        return self.missing_node_count == 0 and self.invalid_posting_count == 0;
    }

    pub fn bytes(self: *const FlatCentroidDirectory) u64 {
        var total: u64 = @sizeOf(FlatCentroidDirectory) +|
            @as(u64, @intCast(self.blocks.len * @sizeOf(FlatCentroidBlock)));
        for (self.blocks) |*block| {
            total +|= @as(u64, @intCast(block.posting_ids.len * @sizeOf(u64)));
            total +|= @as(u64, @intCast(block.covering_radii.len * @sizeOf(f32)));
            total +|= @as(u64, @intCast(block.quantized.centroid.len * @sizeOf(f32)));
            total +|= @as(u64, @intCast(block.quantized.codes.data.len * @sizeOf(u64)));
            total +|= @as(u64, @intCast(block.quantized.code_counts.len * @sizeOf(u32)));
            total +|= @as(u64, @intCast(block.quantized.centroid_distances.len * @sizeOf(f32)));
            total +|= @as(u64, @intCast(block.quantized.quantized_dot_products.len * @sizeOf(f32)));
            total +|= @as(u64, @intCast(block.quantized.centroid_dot_products.len * @sizeOf(f32)));
        }
        return total;
    }
};

fn checkedAddMul(total: *u64, count: u64, element_size: u64) !void {
    const bytes = std.math.mul(u64, count, element_size) catch return error.OutOfMemory;
    total.* = std.math.add(u64, total.*, bytes) catch return error.OutOfMemory;
}

pub const FlatCentroidBuildReservation = struct {
    transient_bytes: u64 = 0,
    retained_bytes: u64 = 0,

    fn empty(self: @This()) bool {
        return self.transient_bytes == 0 and self.retained_bytes == 0;
    }
};

pub const FlatCentroidBuildClaim = union(enum) {
    owner,
    retry,
    /// Owns one retained reference; the caller must eventually release it.
    ready: *FlatCentroidDirectory,
};

pub const FlatCentroidBuildOutcome = union(enum) {
    retry,
    failed: anyerror,
    /// The flight retains its own reference until every waiter has consumed
    /// the outcome.
    ready: *FlatCentroidDirectory,
};

/// Conservative peak allocation for a cold flat-directory build. It covers
/// the retained RaBitQ output for the worst case (every published node is a
/// leaf), traversal arrays, raw centroid blocks, and RaBitQ's normalization
/// workspace. ArrayList growth is budgeted at twice its logical capacity.
pub fn projectedFlatCentroidDirectoryBuildBytes(
    node_count: u64,
    dims: usize,
    block_size_raw: usize,
    max_node_entries: usize,
) !FlatCentroidBuildReservation {
    const block_size = @max(block_size_raw, @as(usize, 1));
    const block_count = std.math.divCeil(u64, node_count, @as(u64, @intCast(block_size))) catch return error.OutOfMemory;
    const code_width = std.math.divCeil(u64, @as(u64, @intCast(dims)), 64) catch return error.OutOfMemory;
    const visited_words = std.math.divCeil(u64, node_count, @bitSizeOf(usize)) catch return error.OutOfMemory;

    var retained: u64 = @sizeOf(FlatCentroidDirectory);
    // Retained directory and its worst-case block-array capacity.
    try checkedAddMul(&retained, @max(block_count *| 2, 16), @sizeOf(FlatCentroidBlock));
    try checkedAddMul(&retained, node_count, @sizeOf(u64) + @sizeOf(f32));
    try checkedAddMul(&retained, node_count, code_width *| @sizeOf(u64));
    try checkedAddMul(&retained, node_count, 4 * @sizeOf(f32) + @sizeOf(u32));
    try checkedAddMul(&retained, block_count, @as(u64, @intCast(dims)) *| @sizeOf(f32));
    // Traversal and block construction workspace. Quantization temporarily
    // holds a normalized copy alongside the raw centroid block.
    var transient: u64 = 0;
    try checkedAddMul(&transient, @max(node_count *| 2, 16), @sizeOf(u64));
    try checkedAddMul(&transient, visited_words, @sizeOf(usize));
    try checkedAddMul(&transient, @intCast(block_size), @sizeOf(u64) + @sizeOf(f32));
    try checkedAddMul(&transient, @intCast(block_size), @as(u64, @intCast(dims)) *| @sizeOf(f32));
    try checkedAddMul(&transient, @min(node_count, @as(u64, @intCast(block_size))), @as(u64, @intCast(dims)) *| @sizeOf(f32));
    try checkedAddMul(&transient, 1, @as(u64, @intCast(dims)) *| @sizeOf(f32));
    // One decoded node is live at a time while traversing the directory.
    try checkedAddMul(&transient, 1, @sizeOf(types.Node));
    try checkedAddMul(&transient, 1, @as(u64, @intCast(dims)) *| @sizeOf(f32));
    try checkedAddMul(&transient, @intCast(max_node_entries), @sizeOf(u64));
    return .{ .transient_bytes = transient, .retained_bytes = retained };
}

pub const FlatCentroidProbe = search_types.FlatCentroidProbe;
const cancellable_flat_sort_chunk_size: usize = 4_096;

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
    const encoded = try hbc.encodePackedNodeValue(packed_value, header, node.covering_radius, centroid_bytes, ids_bytes);
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
        var worst_score = flatAnnScore(probes[0]);
        for (probes[1..], 1..) |probe, i| {
            const score = flatAnnScore(probe);
            if (score > worst_score) {
                worst_score = score;
                worst_index = i;
            }
        }
        if (flatAnnScore(candidate) >= worst_score) return;
        probes[worst_index] = candidate;
    }
}

fn flatAnnScore(probe: FlatCentroidProbe) f32 {
    return probe.distance - probe.error_bound;
}

fn flatProbeLess(_: void, lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    const lhs_score = flatAnnScore(lhs);
    const rhs_score = flatAnnScore(rhs);
    if (std.math.isNan(lhs_score)) {
        if (std.math.isNan(rhs_score)) return lhs.posting_id < rhs.posting_id;
        return false;
    }
    if (std.math.isNan(rhs_score)) return true;
    if (lhs_score != rhs_score) return lhs_score < rhs_score;
    return lhs.posting_id < rhs.posting_id;
}

fn sortFlatProbesCancellable(
    probes: []FlatCentroidProbe,
    merge_buffer: []FlatCentroidProbe,
    cancellation: ?search_types.CancellationToken,
) !void {
    try checkCancellation(cancellation);
    if (probes.len < 2) return;

    // Preserve the standard adaptive block sort for the overwhelmingly common
    // small frontier and for direct library callers without cancellation. Large
    // cancellable frontiers are sorted in bounded chunks and merged linearly;
    // this avoids heapsort's poor locality and O(n log n) work on already sorted
    // inputs while keeping cancellation latency independent of total index size.
    if (cancellation == null or probes.len <= cancellable_flat_sort_chunk_size) {
        std.mem.sort(FlatCentroidProbe, probes, {}, flatProbeLess);
        try checkCancellation(cancellation);
        return;
    }

    var chunk_start: usize = 0;
    while (chunk_start < probes.len) {
        try checkCancellation(cancellation);
        const chunk_end = chunk_start + @min(cancellable_flat_sort_chunk_size, probes.len - chunk_start);
        std.mem.sort(FlatCentroidProbe, probes[chunk_start..chunk_end], {}, flatProbeLess);
        chunk_start = chunk_end;
    }

    if (merge_buffer.len < probes.len) return error.BufferTooSmall;
    var source = probes;
    var destination = merge_buffer;
    var run_width: usize = cancellable_flat_sort_chunk_size;
    while (run_width < probes.len) {
        try checkCancellation(cancellation);
        var start: usize = 0;
        while (start < probes.len) {
            const middle = start + @min(run_width, probes.len - start);
            const end = middle + @min(run_width, probes.len - middle);
            try mergeFlatProbeRunsCancellable(source, destination, start, middle, end, cancellation);
            start = end;
        }
        const previous_source = source;
        source = destination;
        destination = previous_source;
        if (run_width > probes.len / 2) break;
        run_width *= 2;
    }

    if (source.ptr != probes.ptr) {
        var offset: usize = 0;
        while (offset < probes.len) {
            try checkCancellation(cancellation);
            const end = offset + @min(cancellable_flat_sort_chunk_size, probes.len - offset);
            @memcpy(probes[offset..end], source[offset..end]);
            offset = end;
        }
    }
    try checkCancellation(cancellation);
}

fn mergeFlatProbeRunsCancellable(
    source: []const FlatCentroidProbe,
    destination: []FlatCentroidProbe,
    start: usize,
    middle: usize,
    end: usize,
    cancellation: ?search_types.CancellationToken,
) !void {
    var left = start;
    var right = middle;
    var out = start;
    while (left < middle and right < end) : (out += 1) {
        if ((out & 0xff) == 0) try checkCancellation(cancellation);
        if (flatProbeLess({}, source[right], source[left])) {
            destination[out] = source[right];
            right += 1;
        } else {
            destination[out] = source[left];
            left += 1;
        }
    }
    while (left < middle) : ({
        left += 1;
        out += 1;
    }) {
        if ((out & 0xff) == 0) try checkCancellation(cancellation);
        destination[out] = source[left];
    }
    while (right < end) : ({
        right += 1;
        out += 1;
    }) {
        if ((out & 0xff) == 0) try checkCancellation(cancellation);
        destination[out] = source[right];
    }
}

fn checkCancellation(cancellation: ?search_types.CancellationToken) !void {
    if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
}

pub fn flatProbeLessForTesting(lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    if (!builtin.is_test) @compileError("test-only flat probe ordering seam");
    return flatProbeLess({}, lhs, rhs);
}

test "flat frontier cancellable sort preserves deterministic ordering" {
    var probes = [_]FlatCentroidProbe{
        .{ .posting_id = 9, .distance = 4, .error_bound = 1 },
        .{ .posting_id = 7, .distance = 2, .error_bound = 0 },
        .{ .posting_id = 3, .distance = 3, .error_bound = 1 },
        .{ .posting_id = 1, .distance = 2, .error_bound = 0 },
        .{ .posting_id = 5, .distance = 1, .error_bound = 0 },
    };
    try sortFlatProbesCancellable(&probes, &.{}, null);
    for (probes[0 .. probes.len - 1], probes[1..]) |lhs, rhs| {
        try std.testing.expect(!flatProbeLess({}, rhs, lhs));
    }
    try std.testing.expectEqualSlices(u64, &.{ 5, 1, 3, 7, 9 }, &.{
        probes[0].posting_id,
        probes[1].posting_id,
        probes[2].posting_id,
        probes[3].posting_id,
        probes[4].posting_id,
    });
}

test "flat frontier scoring sort honors cancellation" {
    var cancelled = std.atomic.Value(bool).init(true);
    var probes = [_]FlatCentroidProbe{
        .{ .posting_id = 2, .distance = 2, .error_bound = 0 },
        .{ .posting_id = 1, .distance = 1, .error_bound = 0 },
    };
    try std.testing.expectError(
        error.Cancelled,
        sortFlatProbesCancellable(&probes, &.{}, search_types.CancellationToken.fromAtomic(&cancelled)),
    );
}

test "large cancellable flat frontier uses deterministic chunked merge" {
    const probe_count = 4_097;
    const probes = try std.testing.allocator.alloc(FlatCentroidProbe, probe_count);
    defer std.testing.allocator.free(probes);
    const merge_buffer = try std.testing.allocator.alloc(FlatCentroidProbe, probe_count);
    defer std.testing.allocator.free(merge_buffer);
    for (probes, 0..) |*probe, i| {
        const reverse_id: u64 = @intCast(probe_count - i);
        probe.* = .{
            .posting_id = reverse_id,
            .distance = @floatFromInt(reverse_id % 31),
            .error_bound = 0,
        };
    }
    var cancelled = std.atomic.Value(bool).init(false);
    try sortFlatProbesCancellable(
        probes,
        merge_buffer,
        search_types.CancellationToken.fromAtomic(&cancelled),
    );
    for (probes[0 .. probes.len - 1], probes[1..]) |lhs, rhs| {
        try std.testing.expect(!flatProbeLess({}, rhs, lhs));
    }
}

fn flatL2MemberLowerBound(distance: f32, error_bound: f32, covering_radius: f32) ?f32 {
    if (!std.math.isFinite(covering_radius) or covering_radius < 0) return null;
    const centroid_squared_lower = @max(@as(f32, 0), distance - error_bound);
    if (!std.math.isFinite(centroid_squared_lower)) return null;
    const member_distance_lower = @max(@as(f32, 0), @sqrt(centroid_squared_lower) - covering_radius);
    return member_distance_lower * member_distance_lower;
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

pub const PublishedSnapshot = struct {
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
};

fn waitForStablePublicationIfSupported(
    self: anytype,
    generation: u64,
    cancellation: ?search_types.CancellationToken,
) !void {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "waitForPublishedSearchState")) {
        return try self.waitForPublishedSearchState(generation, cancellation);
    }
    if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
    if (builtin.os.tag == .freestanding) {
        std.atomic.spinLoopHint();
    } else {
        std.Thread.yield() catch {};
    }
}

fn loadStablePublishedSnapshot(
    self: anytype,
    cancellation: ?search_types.CancellationToken,
) !PublishedSnapshot {
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
            try waitForStablePublicationIfSupported(self, generation, cancellation);
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
        try waitForStablePublicationIfSupported(self, generation_after, cancellation);
    }
}

fn loadPublishedNode(self: anytype, txn: anytype, node_id: u64) !types.Node {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "loadFlatCentroidDirectoryNodeFromStorage")) {
        return try self.loadFlatCentroidDirectoryNodeFromStorage(txn, node_id);
    }
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

fn snapshotStillPublished(self: anytype, snapshot: PublishedSnapshot) bool {
    const generation = publishedGenerationSnapshot(self);
    return (generation & 1) == 0 and
        generation == snapshot.publish_generation and
        publishedRootNodeSnapshot(self) == snapshot.root_node and
        publishedNodeCountSnapshot(self) == snapshot.node_count;
}

fn finishFlatCentroidBuildIfSupported(
    self: anytype,
    generation: u64,
    outcome: FlatCentroidBuildOutcome,
) void {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "finishFlatCentroidDirectoryBuild")) {
        self.finishFlatCentroidDirectoryBuild(generation, outcome);
    }
}

fn flatCentroidBuildFailureOutcome(err: anyerror) FlatCentroidBuildOutcome {
    return switch (err) {
        // A request-local cancellation or publication race says nothing about
        // whether another waiter can build the same generation successfully.
        error.Cancelled, error.StalePublishedSnapshot => .retry,
        else => .{ .failed = err },
    };
}

fn appendFlatCentroidBlock(
    self: anytype,
    blocks: *std.ArrayListUnmanaged(FlatCentroidBlock),
    posting_ids: []const u64,
    covering_radii: []const f32,
    centroids: []const f32,
    dims: usize,
) !void {
    if (posting_ids.len == 0) return;
    const zero = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(zero);
    @memset(zero, 0);

    const ids = try self.alloc.dupe(u64, posting_ids);
    errdefer self.alloc.free(ids);
    const radii = try self.alloc.dupe(f32, covering_radii);
    errdefer self.alloc.free(radii);
    var quantized = try self.quantizer.quantize(zero, centroids, posting_ids.len);
    errdefer quantized.deinit(self.alloc);
    try blocks.append(self.alloc, .{
        .posting_ids = ids,
        .covering_radii = radii,
        .quantized = quantized,
    });
}

fn buildFlatCentroidDirectory(
    self: anytype,
    txn: anytype,
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
    cancellation: ?search_types.CancellationToken,
) !FlatCentroidDirectory {
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
    var covering_radii = try self.alloc.alloc(f32, block_size);
    defer self.alloc.free(covering_radii);
    var pending = std.ArrayListUnmanaged(u64).empty;
    defer pending.deinit(self.alloc);
    // Published node ids are bounded by node_count. A dense bitset is exact,
    // has predictable resource use, and avoids an O(nodes) hash-table peak on
    // cold large-index directory construction.
    const visited_word_bits = @bitSizeOf(usize);
    const visited_words_u64 = std.math.divCeil(u64, node_count, visited_word_bits) catch return error.OutOfMemory;
    const visited_word_count = std.math.cast(usize, visited_words_u64) orelse return error.OutOfMemory;
    const visited_words = try self.alloc.alloc(usize, visited_word_count);
    defer self.alloc.free(visited_words);
    @memset(visited_words, 0);

    var block_count: usize = 0;
    var posting_count: usize = 0;
    var missing_node_count: usize = 0;
    var invalid_posting_count: usize = 0;

    if (root_node == 0 or root_node > node_count) {
        invalid_posting_count += 1;
    } else {
        const zero_based = root_node - 1;
        const visited_word_index: usize = @intCast(zero_based / visited_word_bits);
        const visited_bit_index: std.math.Log2Int(usize) = @intCast(zero_based % visited_word_bits);
        visited_words[visited_word_index] |= @as(usize, 1) << visited_bit_index;
        try pending.append(self.alloc, root_node);
    }

    var cursor: usize = 0;
    while (cursor < pending.items.len) : (cursor += 1) {
        const node_id = pending.items[cursor];
        if (cursor % 64 == 0) {
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        }
        var node = loadPublishedNode(self, txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) {
                missing_node_count += 1;
                continue;
            }
            return err;
        };
        defer node.deinit(self.alloc);
        if (!node.is_leaf) {
            for (node.children) |child_id| {
                if (child_id == 0 or child_id > node_count) {
                    invalid_posting_count += 1;
                    continue;
                }
                const zero_based = child_id - 1;
                const visited_word_index: usize = @intCast(zero_based / visited_word_bits);
                const visited_bit_index: std.math.Log2Int(usize) = @intCast(zero_based % visited_word_bits);
                const visited_mask = @as(usize, 1) << visited_bit_index;
                if (visited_words[visited_word_index] & visited_mask != 0) {
                    // A valid HBC topology is a strict tree. Mark children when
                    // they are scheduled so corrupt duplicate edges and cycles
                    // cannot grow the traversal queue beyond node_count.
                    invalid_posting_count += 1;
                    continue;
                }
                visited_words[visited_word_index] |= visited_mask;
                try pending.append(self.alloc, child_id);
            }
            continue;
        }
        if (node.members.len == 0) continue;
        if (node.centroid.len != dims) {
            invalid_posting_count += 1;
            continue;
        }

        posting_ids[block_count] = node.id;
        covering_radii[block_count] = node.covering_radius;
        @memcpy(centroids[block_count * dims ..][0..dims], node.centroid);
        block_count += 1;
        posting_count += 1;

        if (block_count == block_size) {
            try appendFlatCentroidBlock(self, &blocks, posting_ids[0..block_count], covering_radii[0..block_count], centroids[0 .. block_count * dims], dims);
            block_count = 0;
        }
    }
    if (block_count > 0) {
        try appendFlatCentroidBlock(self, &blocks, posting_ids[0..block_count], covering_radii[0..block_count], centroids[0 .. block_count * dims], dims);
    }

    return .{
        .blocks = try blocks.toOwnedSlice(self.alloc),
        .root_node_snapshot = root_node,
        .node_count_snapshot = node_count,
        .publish_generation_snapshot = publish_generation,
        .posting_count = posting_count,
        .missing_node_count = missing_node_count,
        .invalid_posting_count = invalid_posting_count,
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

fn acquireFlatCentroidDirectory(
    self: anytype,
    txn: anytype,
    expected_snapshot: ?PublishedSnapshot,
    cancellation: ?search_types.CancellationToken,
) !*FlatCentroidDirectory {
    const Index = comptime @TypeOf(self.*);
    while (true) {
        // Complete callers retain the generation-bound transaction supplied by
        // their snapshot. Best-effort callers open a fresh runtime transaction
        // for the elected build owner below.
        const snapshot = expected_snapshot orelse try loadStablePublishedSnapshot(self, cancellation);

        var stale: ?*FlatCentroidDirectory = null;
        lockAtomicMutex(&self.flat_centroid_mu);
        if (self.flat_centroid_directory) |directory| {
            if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                directory.retain();
                self.flat_centroid_mu.unlock();
                return directory;
            }
        }
        self.flat_centroid_mu.unlock();

        if (comptime @hasDecl(Index, "beginFlatCentroidDirectoryBuild")) {
            switch (try self.beginFlatCentroidDirectoryBuild(snapshot.publish_generation, cancellation)) {
                .owner => {},
                .retry => continue,
                .ready => |directory| {
                    // The adapter keys flights by generation, but retain the
                    // full snapshot check at this generic boundary. This
                    // keeps a buggy or wrapped generation from handing a
                    // caller a directory for different topology.
                    if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                        return directory;
                    }
                    directory.release(self.alloc);
                    continue;
                },
            }
        }
        var build_flight_open = comptime @hasDecl(Index, "finishFlatCentroidDirectoryBuild");
        var build_outcome: FlatCentroidBuildOutcome = .retry;
        defer if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, build_outcome);
        errdefer |err| build_outcome = flatCentroidBuildFailureOutcome(err);

        var build_reservation: FlatCentroidBuildReservation = .{};
        if (comptime @hasDecl(Index, "reserveFlatCentroidDirectoryBuildBytes")) {
            build_reservation = try self.reserveFlatCentroidDirectoryBuildBytes(
                try projectedFlatCentroidDirectoryBuildBytes(
                    snapshot.node_count,
                    @intCast(self.config.dims),
                    self.config.flat_centroid_block_size,
                    @max(self.config.leaf_size, self.config.branching_factor),
                ),
            );
        }
        var build_reservation_open = !build_reservation.empty();
        errdefer if (comptime @hasDecl(Index, "releaseFlatCentroidDirectoryBuildBytes")) {
            if (build_reservation_open) self.releaseFlatCentroidDirectoryBuildBytes(build_reservation);
        };

        const built = try self.alloc.create(FlatCentroidDirectory);
        errdefer self.alloc.destroy(built);
        if (expected_snapshot != null) {
            built.* = try buildFlatCentroidDirectory(self, txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation, cancellation);
        } else if (comptime @hasDecl(Index, "beginRuntimeSearchTxn")) {
            var build_txn = try self.beginRuntimeSearchTxn();
            defer build_txn.abort();
            built.* = try buildFlatCentroidDirectory(self, &build_txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation, cancellation);
        } else {
            built.* = try buildFlatCentroidDirectory(self, txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation, cancellation);
        }
        errdefer built.deinit(self.alloc);
        if (comptime @hasDecl(Index, "accountFlatCentroidDirectory")) {
            try self.accountFlatCentroidDirectory(built, build_reservation);
            build_reservation_open = false;
        } else if (comptime @hasDecl(Index, "releaseFlatCentroidDirectoryBuildBytes")) {
            if (build_reservation_open) {
                self.releaseFlatCentroidDirectoryBuildBytes(build_reservation);
                build_reservation_open = false;
            }
        }

        if (expected_snapshot == null) {
            const current = try loadStablePublishedSnapshot(self, cancellation);
            if (current.root_node != snapshot.root_node or
                current.node_count != snapshot.node_count or
                current.publish_generation != snapshot.publish_generation)
            {
                built.deinit(self.alloc);
                self.alloc.destroy(built);
                if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .retry);
                build_flight_open = false;
                continue;
            }
        }

        lockAtomicMutex(&self.flat_centroid_mu);
        if (expected_snapshot != null and !snapshotStillPublished(self, snapshot)) {
            self.flat_centroid_mu.unlock();
            if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .{ .ready = built });
            build_flight_open = false;
            return built;
        }
        if (self.flat_centroid_directory) |directory| {
            if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                directory.retain();
                self.flat_centroid_mu.unlock();
                built.deinit(self.alloc);
                self.alloc.destroy(built);
                if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .{ .ready = directory });
                build_flight_open = false;
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
        if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .{ .ready = built });
        build_flight_open = false;
        return built;
    }
}

/// Scores and returns the complete ordered flat frontier from one retained
/// directory publication. Allocation capacity is derived from that same
/// directory, so a concurrent publication cannot make the caller silently
/// truncate newer postings with an older capacity snapshot.
pub fn selectFlatRabitqPostingsAlloc(
    self: anytype,
    txn: anytype,
    query: []const f32,
    scratch_handle: anytype,
    profile: *search_types.SearchProfile,
    coverage_policy: search_types.CoveragePolicy,
    expected_snapshot: ?PublishedSnapshot,
    cancellation: ?search_types.CancellationToken,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) ![]FlatCentroidProbe {
    const scratch = &scratch_handle.scratch;
    const start = now_fn_u64();
    const directory = try acquireFlatCentroidDirectory(self, txn, expected_snapshot, cancellation);
    defer directory.release(self.alloc);
    defer profile.child_expand_ns += elapsed_fn_u64(start);
    if (coverage_policy == .complete_snapshot and !directory.complete()) {
        return error.IncompletePublishedSnapshot;
    }

    var posting_count: usize = 0;
    var max_block_count: usize = 0;
    for (directory.blocks) |*block| {
        posting_count = std.math.add(usize, posting_count, block.posting_ids.len) catch return error.OutOfMemory;
        max_block_count = @max(max_block_count, block.posting_ids.len);
    }
    std.debug.assert(posting_count == directory.posting_count);
    const needs_merge = cancellation != null and posting_count > cancellable_flat_sort_chunk_size;
    const previous_accounted_bytes = scratch_handle.accounted_bytes;
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "reserveSearchScratchBytes")) {
        const target_bytes = try scratch.projectedBytesWithFlatProbeCapacity(
            posting_count,
            needs_merge,
            max_block_count,
        );
        try self.reserveSearchScratchBytes(scratch_handle, target_bytes);
    }
    errdefer if (comptime @hasDecl(Index, "rollbackSearchScratchBytes")) {
        self.rollbackSearchScratchBytes(scratch_handle, previous_accounted_bytes);
    };
    try scratch.ensureFlatProbeCapacity(self.alloc, posting_count, needs_merge);
    const probes = scratch.flat_probes[0..posting_count];
    var probe_count: usize = 0;

    for (directory.blocks) |*block| {
        try checkCancellation(cancellation);
        const count = block.posting_ids.len;
        try scratch.ensureVectorFetchCapacity(self.alloc, count);
        const distances = scratch.distances[0..count];
        const error_bounds = scratch.error_bounds[0..count];
        try self.quantizer.estimateDistancesWithScratch(&block.quantized, query, distances, error_bounds, &scratch.estimate);
        for (block.posting_ids, 0..) |posting_id, i| {
            if ((i & 0xff) == 0) try checkCancellation(cancellation);
            const member_lower_bound = if (self.config.metric == .l2_squared)
                flatL2MemberLowerBound(distances[i], error_bounds[i], block.covering_radii[i])
            else
                null;
            insertFlatProbe(probes, &probe_count, .{
                .posting_id = posting_id,
                .distance = distances[i],
                .error_bound = error_bounds[i],
                .member_lower_bound = member_lower_bound orelse -std.math.inf(f32),
                .bound_resolved = member_lower_bound != null,
            });
        }
    }

    std.debug.assert(probe_count == posting_count);
    try sortFlatProbesCancellable(probes[0..probe_count], scratch.flat_probe_merge, cancellation);
    profile.approx_nodes_expanded += @intCast(directory.blocks.len);
    return probes;
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

fn replaceOwnedU64Slice(alloc: std.mem.Allocator, target: *[]u64, replacement: *?[]u64) void {
    const owned = replacement.* orelse unreachable;
    if (target.*.len > 0) alloc.free(target.*);
    target.* = owned;
    replacement.* = null;
}

fn removeUniqueChildLinkAlloc(
    alloc: std.mem.Allocator,
    children: []const u64,
    child_id: u64,
) !?[]u64 {
    var match_count: usize = 0;
    for (children) |cid| {
        if (cid == child_id) match_count += 1;
    }
    // A merge is safe only when the recorded parent owns exactly one link to
    // the leaf. Zero links are a stale parent pointer; duplicate links are
    // tree corruption. In either case, leave both postings untouched for the
    // tree-link repair sweep instead of sizing a len-1 array incorrectly.
    if (match_count != 1) return null;

    const replacement = try alloc.alloc(u64, children.len - 1);
    errdefer alloc.free(replacement);
    var wi: usize = 0;
    for (children) |cid| {
        if (cid == child_id) continue;
        replacement[wi] = cid;
        wi += 1;
    }
    std.debug.assert(wi == replacement.len);
    return replacement;
}

fn noteTreeLinkInconsistencyIfSupported(self: anytype) void {
    const Index = comptime switch (@typeInfo(@TypeOf(self))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(self),
    };
    if (comptime @hasDecl(Index, "noteTreeLinkInconsistency")) {
        self.noteTreeLinkInconsistency();
    }
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

    var new_children: ?[]u64 = try removeUniqueChildLinkAlloc(self.alloc, parent.children, leaf.id);
    defer if (new_children) |owned| self.alloc.free(owned);
    if (new_children == null) {
        std.log.warn(
            "hbc: underfull leaf {d} is not uniquely linked by recorded parent {d}; skipping merge",
            .{ leaf.id, leaf.parent },
        );
        noteTreeLinkInconsistencyIfSupported(self);
        return false;
    }

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
    var merged: ?[]u64 = try self.alloc.alloc(u64, merged_len);
    errdefer if (merged) |owned| self.alloc.free(owned);
    @memcpy(merged.?[0..sibling.members.len], sibling.members);
    @memcpy(merged.?[sibling.members.len..], leaf.members);
    replaceOwnedU64Slice(self.alloc, &sibling.members, &merged);
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

    replaceOwnedU64Slice(self.alloc, &parent.children, &new_children);
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

test "owned slice replacement survives later error cleanup" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var node = types.Node{
                .id = 1,
                .is_leaf = true,
                .level = 0,
                .parent = 0,
                .centroid = &.{},
                .children = &.{},
                .members = try alloc.dupe(u64, &.{ 1, 2 }),
            };
            defer node.deinit(alloc);

            var replacement: ?[]u64 = try alloc.dupe(u64, &.{ 3, 4, 5 });
            errdefer if (replacement) |owned| alloc.free(owned);
            replaceOwnedU64Slice(alloc, &node.members, &replacement);
            return error.InjectedFailure;
        }
    };

    try std.testing.expectError(error.InjectedFailure, Runner.run(std.testing.allocator));
}

test "underfull merge child unlink requires exactly one parent link" {
    const alloc = std.testing.allocator;

    try std.testing.expect((try removeUniqueChildLinkAlloc(alloc, &.{ 2, 3, 4 }, 9)) == null);
    try std.testing.expect((try removeUniqueChildLinkAlloc(alloc, &.{ 2, 3, 2 }, 2)) == null);

    const replacement = (try removeUniqueChildLinkAlloc(alloc, &.{ 2, 3, 4 }, 3)) orelse
        return error.TestUnexpectedResult;
    defer alloc.free(replacement);
    try std.testing.expectEqualSlices(u64, &.{ 2, 4 }, replacement);
}

test "posting backlog stats starts clean" {
    const TestIndex = struct {
        alloc: std.mem.Allocator,
        metadata: hbc.IndexMetadata = .{
            .dims = 0,
            .branching_factor = 0,
            .leaf_size = 0,
            .node_count = 0,
        },
        config: types.HBCConfig = .{ .dims = 0 },

        fn loadNode(_: *@This(), _: anytype, _: u64) !types.Node {
            return error.NotFound;
        }
    };
    var index = TestIndex{ .alloc = std.testing.allocator };
    const txn = {};
    const stats = try postingBacklogStatsTxn(&index, txn);
    try std.testing.expectEqual(@as(u64, 0), stats.scanned_nodes);
    try std.testing.expect(!stats.hasMaintenanceDebt());
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

test "flat centroid build failures retry only request-local outcomes" {
    for ([_]anyerror{ error.Cancelled, error.StalePublishedSnapshot }) |err| {
        switch (flatCentroidBuildFailureOutcome(err)) {
            .retry => {},
            else => return error.TestUnexpectedResult,
        }
    }
    for ([_]anyerror{ error.Canceled, error.ResourceBudgetExceeded }) |expected| {
        switch (flatCentroidBuildFailureOutcome(expected)) {
            .failed => |err| try std.testing.expectEqual(expected, err),
            else => return error.TestUnexpectedResult,
        }
    }
}
