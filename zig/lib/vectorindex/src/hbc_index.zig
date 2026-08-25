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
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const hbc = @import("hbc.zig");
const bulk_build = @import("bulk_build.zig");
const kmeans = @import("kmeans.zig");
const search_types = @import("search_types.zig");
const search_mod = @import("search.zig");
const search_runtime = @import("search_runtime.zig");
const search_results = @import("search_results.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const posting = @import("posting.zig");
const spfresh_index = @import("spfresh_index.zig");
const proto = @import("antfly_vector").proto;
const quantizer_mod = @import("antfly_vector").quantizer;
const rabitq = @import("antfly_vector").rabitq;
const vec = @import("antfly_vector").vector;

fn debugHitFromApprox(item: search_results.ApproxSearchResult) search_types.DebugHit {
    return .{
        .id = item.vector_id,
        .distance = item.distance,
        .error_bound = item.error_bound,
        .lower_bound = item.distance - item.error_bound,
        .upper_bound = item.distance + item.error_bound,
    };
}

fn debugPairFromApprox(left: search_results.ApproxSearchResult, right: search_results.ApproxSearchResult) search_types.DebugPair {
    const left_hit = debugHitFromApprox(left);
    const right_hit = debugHitFromApprox(right);
    const interval_gap = right_hit.lower_bound - left_hit.upper_bound;
    return .{
        .left = left_hit,
        .right = right_hit,
        .distance_gap = right.distance - left.distance,
        .interval_gap = interval_gap,
        .overlaps = interval_gap <= 0,
    };
}

fn approxLowerBound(item: search_results.ApproxSearchResult) f32 {
    return item.distance - item.error_bound;
}

fn approxUpperBound(item: search_results.ApproxSearchResult) f32 {
    return item.distance + item.error_bound;
}

fn approxIntervalsOverlap(a: search_results.ApproxSearchResult, b: search_results.ApproxSearchResult) bool {
    return approxLowerBound(a) <= approxUpperBound(b) and approxLowerBound(b) <= approxUpperBound(a);
}

pub const BuiltBulkNode = struct {
    node_id: u64,
    centroid: []f32,
    range: ?types.NodeSplitRange,
    level: u16,
    member_count: usize,

    pub fn deinit(self: *BuiltBulkNode, alloc: Allocator) void {
        alloc.free(self.centroid);
        if (self.range) |*range| range.deinit(alloc);
        self.* = undefined;
    }
};

pub const LeafKeyEntry = struct {
    index: usize,
    member_id: u64,
    key: []const u8,
};

const FixedKeyLookup = search_runtime.RerankLookup;

pub const SplitResult = struct {
    c1: []f32,
    g1: []u64,
    c2: []f32,
    g2: []u64,
};

const BulkRecursiveScratch = struct {
    vec_data: []f32,
    positions: []u64,
    partitioned_indexes: []usize,
};

fn txnSupportsGetManySorted(comptime Txn: type) bool {
    return switch (@typeInfo(Txn)) {
        .pointer => |ptr| @hasDecl(ptr.child, "getManySorted"),
        else => @hasDecl(Txn, "getManySorted"),
    };
}

fn indexHasExternalVectorLoader(self: anytype) bool {
    const Self = @TypeOf(self);
    const Index = switch (@typeInfo(Self)) {
        .pointer => |ptr| ptr.child,
        else => Self,
    };
    if (comptime @hasDecl(Index, "hasExternalVectorLoader")) return self.hasExternalVectorLoader();
    return false;
}

fn recordKmeansRunStats(self: anytype, stats: kmeans.RunStats) void {
    const Self = @TypeOf(self);
    const Index = switch (@typeInfo(Self)) {
        .pointer => |ptr| ptr.child,
        else => Self,
    };
    if (comptime !@hasField(Index, "write_profile")) return;

    self.write_profile.kmeans_assignment_calls += stats.assignment_calls;
    self.write_profile.kmeans_assignment_cpu_calls += stats.assignment_cpu_calls;
    self.write_profile.kmeans_assignment_metal_calls += stats.assignment_metal_calls;
    self.write_profile.kmeans_assignment_points_total += stats.assignment_points_total;
    self.write_profile.kmeans_assignment_ns += stats.assignment_ns;
    self.write_profile.kmeans_assignment_cpu_ns += stats.assignment_cpu_ns;
    self.write_profile.kmeans_assignment_metal_ns += stats.assignment_metal_ns;
    self.write_profile.kmeans_update_calls += stats.update_calls;
    self.write_profile.kmeans_update_cpu_calls += stats.update_cpu_calls;
    self.write_profile.kmeans_update_metal_calls += stats.update_metal_calls;
    self.write_profile.kmeans_update_ns += stats.update_ns;
    self.write_profile.kmeans_update_cpu_ns += stats.update_cpu_ns;
    self.write_profile.kmeans_update_metal_ns += stats.update_metal_ns;
}

fn childType(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .pointer => |ptr| ptr.child,
        else => T,
    };
}

pub fn CachedNodeReadHandle(comptime T: type) type {
    const Index = childType(T);
    const Borrowed = if (comptime @hasDecl(Index, "BorrowedNode")) Index.BorrowedNode else void;
    return union(enum) {
        borrowed: Borrowed,
        owned: types.Node,

        pub fn ptr(self: *const @This()) *const types.Node {
            return switch (self.*) {
                .borrowed => |*lease| if (Borrowed == void) unreachable else lease.ptr(),
                .owned => |*node| node,
            };
        }

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            switch (self.*) {
                .borrowed => |*lease| {
                    if (Borrowed != void) lease.deinit();
                },
                .owned => |*node| node.deinit(alloc),
            }
            self.* = undefined;
        }
    };
}

pub fn CachedQuantizedReadHandle(comptime T: type) type {
    const Index = childType(T);
    const Borrowed = if (comptime @hasDecl(Index, "BorrowedQuantized")) Index.BorrowedQuantized else void;
    return union(enum) {
        borrowed: Borrowed,
        owned: hbc_runtime.QuantizedSet,

        pub fn ptr(self: *const @This()) *const hbc_runtime.QuantizedSet {
            return switch (self.*) {
                .borrowed => |*lease| if (Borrowed == void) unreachable else lease.ptr(),
                .owned => |*qs| qs,
            };
        }

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            switch (self.*) {
                .borrowed => |*lease| {
                    if (Borrowed != void) lease.deinit();
                },
                .owned => |*qs| qs.deinit(alloc),
            }
            self.* = undefined;
        }
    };
}

fn CachedVectorReadHandle(comptime T: type) type {
    const Index = childType(T);
    const Borrowed = if (comptime @hasDecl(Index, "BorrowedVector")) Index.BorrowedVector else void;
    return union(enum) {
        borrowed: Borrowed,

        fn view(self: *const @This()) []const f32 {
            return switch (self.*) {
                .borrowed => |*lease| if (Borrowed == void) unreachable else lease.view(),
            };
        }

        fn deinit(self: *@This()) void {
            switch (self.*) {
                .borrowed => |*lease| {
                    if (Borrowed != void) lease.deinit();
                },
            }
            self.* = undefined;
        }
    };
}

fn CachedMetadataReadHandle(comptime T: type) type {
    const Index = childType(T);
    const Borrowed = if (comptime @hasDecl(Index, "BorrowedMetadata")) Index.BorrowedMetadata else void;
    return union(enum) {
        borrowed: Borrowed,

        fn view(self: *const @This()) []const u8 {
            return switch (self.*) {
                .borrowed => |*lease| if (Borrowed == void) unreachable else lease.view(),
            };
        }

        fn deinit(self: *@This()) void {
            switch (self.*) {
                .borrowed => |*lease| {
                    if (Borrowed != void) lease.deinit();
                },
            }
            self.* = undefined;
        }
    };
}

fn borrowCachedNodeHandle(self: anytype, node_id: u64) !?CachedNodeReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedNode")) {
        if (self.borrowCachedNode(node_id)) |borrowed| return .{ .borrowed = borrowed };
        return null;
    }
    if (try self.getCachedNodeClone(node_id)) |cached| return .{ .owned = cached };
    return null;
}

fn borrowSearchCachedNodeHandle(self: anytype, node_id: u64) !?CachedNodeReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedNodeForSearch")) {
        if (self.borrowCachedNodeForSearch(node_id)) |borrowed| return .{ .borrowed = borrowed };
        return null;
    }
    return try borrowCachedNodeHandle(self, node_id);
}

fn loadSearchNodeFromStorage(self: anytype, txn: anytype, node_id: u64) !types.Node {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "loadSearchNodeFromStorage")) {
        return try self.loadSearchNodeFromStorage(txn, node_id);
    }
    return try self.loadNodeFromStorage(txn, node_id);
}

const SearchCacheFill = struct {
    guarded: bool,
    epoch: ?u64,
};

fn searchCacheFillForTxn(self: anytype, txn: anytype) SearchCacheFill {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "beginSearchCacheFill")) {
        const Txn = comptime childType(@TypeOf(txn));
        if (comptime @hasField(Txn, "cache_fill_epoch")) {
            return .{ .guarded = true, .epoch = txn.cache_fill_epoch };
        }
        // A miss-time epoch is insufficient: the transaction may already be
        // an old MVCC snapshot after a writer completed. Fail closed unless the
        // adapter bound the epoch to transaction creation.
        return .{ .guarded = true, .epoch = null };
    }
    return .{ .guarded = false, .epoch = null };
}

fn cacheSearchNodeAfterLoad(self: anytype, node: *const types.Node, fill: SearchCacheFill) !void {
    const Index = comptime childType(@TypeOf(self));
    if (fill.guarded) {
        const epoch = fill.epoch orelse return;
        if (comptime @hasDecl(Index, "cacheSearchNodeIfFillCurrent")) {
            try self.cacheSearchNodeIfFillCurrent(node, epoch);
        }
        return;
    }
    if (comptime @hasDecl(Index, "cacheSearchNode")) {
        try self.cacheSearchNode(node);
    } else {
        try self.cacheNode(node);
    }
}

fn cacheQuantizedAfterLoad(self: anytype, node_id: u64, qs: *const hbc_runtime.QuantizedSet, fill: SearchCacheFill) !void {
    const Index = comptime childType(@TypeOf(self));
    if (fill.guarded) {
        const epoch = fill.epoch orelse return;
        if (comptime @hasDecl(Index, "cacheQuantizedIfFillCurrent")) {
            try self.cacheQuantizedIfFillCurrent(node_id, qs, epoch);
        }
        return;
    }
    try self.cacheQuantized(node_id, qs);
}

fn cacheMetadataAfterLoad(self: anytype, vector_id: u64, metadata: []const u8, fill: SearchCacheFill) ![]const u8 {
    const Index = comptime childType(@TypeOf(self));
    if (fill.guarded) {
        const epoch = fill.epoch orelse return metadata;
        if (comptime @hasDecl(Index, "cacheMetadataIfFillCurrent")) {
            return try self.cacheMetadataIfFillCurrent(vector_id, metadata, epoch);
        }
        return metadata;
    }
    return try self.cacheMetadata(vector_id, metadata);
}

fn borrowCachedVectorHandle(self: anytype, vector_id: u64) ?CachedVectorReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedVector")) {
        if (self.borrowCachedVector(vector_id)) |borrowed| return .{ .borrowed = borrowed };
    }
    return null;
}

const VectorCacheFill = struct {
    guarded: bool,
    epoch: ?u64,
};

fn beginVectorCacheFillIfSupported(self: anytype, vector_id: u64) VectorCacheFill {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "beginVectorCacheFill")) {
        return .{ .guarded = true, .epoch = self.beginVectorCacheFill(vector_id) };
    }
    return .{ .guarded = false, .epoch = null };
}

fn cacheVectorAfterLoad(self: anytype, vector_id: u64, vector: []const f32, fill: VectorCacheFill) ![]const f32 {
    const Index = comptime childType(@TypeOf(self));
    if (fill.guarded) {
        const epoch = fill.epoch orelse return vector;
        if (comptime @hasDecl(Index, "cacheVectorIfFillCurrent")) {
            return try self.cacheVectorIfFillCurrent(vector_id, vector, epoch);
        }
        return vector;
    }
    return try self.cacheVector(vector_id, vector);
}

fn abortVectorCacheMutationsIfSupported(self: anytype) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "abortVectorCacheMutations")) self.abortVectorCacheMutations();
}

fn seedRetainedVectorsAfterCommit(self: anytype, items: []const hbc_runtime.BatchInsertItem, options: anytype) void {
    const Options = @TypeOf(options);
    if (comptime !@hasField(Options, "skip_vector_store")) return;
    if (!options.skip_vector_store) return;
    // Skip-store embeddings are authoritative in the caller's LSM. Cache
    // them only after the index transaction publishes, never from its
    // uncommitted mutation window.
    for (items) |item| _ = self.cacheVector(item.vector_id, item.vector) catch {};
}

fn shouldSeedRetainedVectorCacheOnSkipStore(self: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(self))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(self),
    };
    if (comptime @hasField(Index, "retained_vector_cache_enabled")) {
        if (!self.retained_vector_cache_enabled) return false;
    }
    if (comptime @hasField(Index, "bypass_external_vector_cache")) {
        return !self.bypass_external_vector_cache;
    }
    return true;
}

fn borrowCachedMetadataHandle(self: anytype, vector_id: u64) ?CachedMetadataReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedMetadata")) {
        if (self.borrowCachedMetadata(vector_id)) |borrowed| return .{ .borrowed = borrowed };
    }
    return null;
}

pub fn loadNodeReadHandleProfiled(
    self: anytype,
    txn: anytype,
    node_id: u64,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !CachedNodeReadHandle(@TypeOf(self)) {
    return loadNodeReadHandleProfiledWithCachePolicy(self, txn, node_id, profile, true, now_fn, elapsed_fn);
}

fn loadNodeReadHandleProfiledWithCachePolicy(
    self: anytype,
    txn: anytype,
    node_id: u64,
    profile: *search_types.SearchProfile,
    comptime use_cache: bool,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !CachedNodeReadHandle(@TypeOf(self)) {
    const lookup_start = now_fn();
    if (use_cache) {
        if (try borrowSearchCachedNodeHandle(self, node_id)) |cached| {
            profile.node_cache_lookup_ns += elapsed_fn(lookup_start);
            return cached;
        }
    }
    profile.node_cache_lookup_ns += elapsed_fn(lookup_start);

    const start = now_fn();
    const fill = if (use_cache) searchCacheFillForTxn(self, txn) else SearchCacheFill{ .guarded = false, .epoch = null };
    var loaded = try loadSearchNodeFromStorage(self, txn, node_id);
    errdefer loaded.deinit(self.alloc);
    if (use_cache) try cacheSearchNodeAfterLoad(self, &loaded, fill);
    profile.node_cache_miss_ns += elapsed_fn(start);
    profile.node_cache_misses += 1;
    return .{ .owned = loaded };
}

pub fn loadNodeReadHandle(
    self: anytype,
    txn: anytype,
    node_id: u64,
) !CachedNodeReadHandle(@TypeOf(self)) {
    return loadNodeReadHandleWithCachePolicy(self, txn, node_id, true);
}

fn loadNodeReadHandleWithCachePolicy(
    self: anytype,
    txn: anytype,
    node_id: u64,
    comptime use_cache: bool,
) !CachedNodeReadHandle(@TypeOf(self)) {
    if (use_cache) {
        if (try borrowSearchCachedNodeHandle(self, node_id)) |cached| return cached;
    }

    const fill = if (use_cache) searchCacheFillForTxn(self, txn) else SearchCacheFill{ .guarded = false, .epoch = null };
    var loaded = try loadSearchNodeFromStorage(self, txn, node_id);
    errdefer loaded.deinit(self.alloc);
    if (use_cache) try cacheSearchNodeAfterLoad(self, &loaded, fill);
    return .{ .owned = loaded };
}

fn loadMutationNodeReadHandle(
    self: anytype,
    txn: anytype,
    node_id: u64,
) !CachedNodeReadHandle(@TypeOf(self)) {
    // Mutation routing must see nodes staged by earlier writes in the active
    // publication session. The search helper intentionally bypasses those
    // caches while a session is open so public queries remain on committed
    // state; using it here turns every routed child into an LSM point read.
    if (try borrowCachedNodeHandle(self, node_id)) |cached| return cached;

    var loaded = try self.loadNodeFromStorage(txn, node_id);
    errdefer loaded.deinit(self.alloc);
    try self.cacheNode(&loaded);
    return .{ .owned = loaded };
}

pub fn loadQuantizedReadHandleProfiled(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
    is_not_found: fn (anyerror) bool,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    return loadQuantizedReadHandleProfiledWithCachePolicy(
        self,
        txn,
        node_id,
        is_root,
        expected_count,
        profile,
        true,
        now_fn,
        elapsed_fn,
        is_not_found,
    );
}

fn loadQuantizedReadHandleProfiledWithCachePolicy(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    profile: *search_types.SearchProfile,
    comptime use_cache: bool,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
    is_not_found: fn (anyerror) bool,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    const lookup_start = now_fn();
    if (use_cache and comptime @hasDecl(Index, "borrowCachedQuantized")) {
        if (self.borrowCachedQuantized(node_id)) |borrowed| {
            profile.quantized_cache_lookup_ns += elapsed_fn(lookup_start);
            var handle = borrowed;
            const cached = handle.ptr();
            validateQuantizedSet(self, cached, expected_count) catch |err| {
                handle.deinit();
                switch (err) {
                    error.Corrupted => {
                        self.invalidateQuantizedCache(node_id);
                        return null;
                    },
                }
            };
            return .{ .borrowed = borrowed };
        }
        profile.quantized_cache_lookup_ns += elapsed_fn(lookup_start);
    } else {
        if (use_cache) {
            if (try self.getCachedQuantizedClone(node_id)) |cached| {
                profile.quantized_cache_lookup_ns += elapsed_fn(lookup_start);
                validateQuantizedSet(self, &cached, expected_count) catch |err| switch (err) {
                    error.Corrupted => self.invalidateQuantizedCache(node_id),
                };
                if (try self.getCachedQuantizedClone(node_id)) |valid| return .{ .owned = valid };
            }
        }
        profile.quantized_cache_lookup_ns += elapsed_fn(lookup_start);
    }

    const start = now_fn();
    const fill = if (use_cache) searchCacheFillForTxn(self, txn) else SearchCacheFill{ .guarded = false, .epoch = null };
    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) return null;
        return err;
    };
    profile.quantized_cache_miss_ns += elapsed_fn(start);
    profile.quantized_cache_misses += 1;
    if (use_cache and self.cache_enabled) {
        cacheQuantizedAfterLoad(self, node_id, &decoded, fill) catch {};
    }
    return .{ .owned = decoded };
}

pub fn loadQuantizedReadHandle(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    is_not_found: fn (anyerror) bool,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedQuantized")) {
        if (self.borrowCachedQuantized(node_id)) |borrowed| {
            var handle = borrowed;
            const cached = handle.ptr();
            validateQuantizedSet(self, cached, expected_count) catch |err| {
                handle.deinit();
                switch (err) {
                    error.Corrupted => {
                        self.invalidateQuantizedCache(node_id);
                        return null;
                    },
                }
            };
            return .{ .borrowed = borrowed };
        }
    } else if (try self.getCachedQuantizedClone(node_id)) |cached| {
        validateQuantizedSet(self, &cached, expected_count) catch |err| switch (err) {
            error.Corrupted => self.invalidateQuantizedCache(node_id),
        };
        if (try self.getCachedQuantizedClone(node_id)) |valid| return .{ .owned = valid };
    }

    const fill = searchCacheFillForTxn(self, txn);
    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) return null;
        return err;
    };
    if (self.cache_enabled) {
        cacheQuantizedAfterLoad(self, node_id, &decoded, fill) catch {};
    }
    return .{ .owned = decoded };
}

/// Return an independently owned quantized value for mutation paths. Cached
/// entries are immutable while published: writers clone, modify, persist, and
/// replace them instead of retaining a lease and racing readers in place.
fn loadQuantizedOwned(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    is_not_found: fn (anyerror) bool,
) !?hbc_runtime.QuantizedSet {
    if (try self.getCachedQuantizedClone(node_id)) |cached_value| {
        var cached = cached_value;
        validateQuantizedSet(self, &cached, expected_count) catch {
            cached.deinit(self.alloc);
            self.invalidateQuantizedCache(node_id);
            return loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
                if (is_not_found(err) or err == error.Corrupted) return null;
                return err;
            };
        };
        return cached;
    }

    return loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) return null;
        return err;
    };
}

fn recordDeferredQuantizedNode(self: anytype, node_id: u64) !void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "recordDeferredQuantizedNode")) {
        try self.recordDeferredQuantizedNode(node_id);
    }
}

fn clearDeferredQuantizedNode(self: anytype, node_id: u64) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "clearDeferredQuantizedNode")) {
        self.clearDeferredQuantizedNode(node_id);
    }
}

fn rebuildDeferredQuantizedNodes(self: anytype, txn: anytype, options: hbc_runtime.BatchInsertOptions) !bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "rebuildDeferredQuantizedNodesWithOptions")) {
        try self.rebuildDeferredQuantizedNodesWithOptions(txn, options);
        return true;
    }
    if (comptime @hasDecl(Index, "rebuildDeferredQuantizedNodes")) {
        try self.rebuildDeferredQuantizedNodes(txn);
        return true;
    }
    return false;
}

fn deferLeafSplitToBatchFinish(options: hbc_runtime.BatchInsertOptions) bool {
    return options.defer_leaf_splits_to_batch_finish;
}

fn suppressQuantizedPayloadPersist(options: anytype) bool {
    const Options = @TypeOf(options);
    if (@hasField(Options, "suppress_quantized_payload_persist")) {
        return @field(options, "suppress_quantized_payload_persist");
    }
    return false;
}

fn shouldDeferOversizedLeafSplit(self: anytype, leaf: *const types.Node, options: hbc_runtime.BatchInsertOptions) bool {
    if (!leaf.is_leaf or leaf.members.len <= self.config.leaf_size) return false;
    return deferLeafSplitToBatchFinish(options) or shouldDeferLeafSplitToBulkFinish(self, options);
}

fn normalizeDeferredOversizedLeavesForBatchFinish(self: anytype, txn: anytype, options: hbc_runtime.BatchInsertOptions) !bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "normalizeDeferredOversizedLeavesForBatchFinishTxn")) {
        try self.normalizeDeferredOversizedLeavesForBatchFinishTxn(txn, options);
        return true;
    }
    return false;
}

fn publishDeferredNodeKeysForBatchFinish(self: anytype, txn: anytype, options: hbc_runtime.BatchInsertOptions) !void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "publishDeferredNodeKeysForBatchFinishTxn")) {
        try self.publishDeferredNodeKeysForBatchFinishTxn(txn, options);
    }
}

fn batchVectorLookup(options: anytype) ?hbc_runtime.BatchVectorLookup {
    const Options = @TypeOf(options);
    if (comptime @hasField(Options, "batch_vectors")) return options.batch_vectors;
    return null;
}

fn getBatchVectorViewOrScratch(self: anytype, txn: anytype, vector_id: u64, scratch: []f32, options: anytype) ![]const f32 {
    if (batchVectorLookup(options)) |lookup| {
        if (lookup.get(vector_id)) |vector| {
            if (vector.len > scratch.len) return error.BufferTooSmall;
            return vector;
        }
    }
    return try self.getVectorViewOrScratch(txn, vector_id, scratch);
}

fn getBatchVectorScratch(self: anytype, txn: anytype, vector_id: u64, scratch: []f32, options: anytype) ![]const f32 {
    if (batchVectorLookup(options)) |lookup| {
        if (lookup.get(vector_id)) |vector| {
            if (vector.len > scratch.len) return error.BufferTooSmall;
            return vector;
        }
    }
    return try self.getVectorScratch(txn, vector_id, scratch);
}

fn shouldDeferQuantizedRebuildToBulkFinish(self: anytype, options: hbc_runtime.BatchInsertOptions) bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "shouldDeferQuantizedRebuildToBulkFinish")) {
        return self.shouldDeferQuantizedRebuildToBulkFinish(options);
    }
    return false;
}

fn shouldDeferLeafSplitToBulkFinish(self: anytype, options: anytype) bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "shouldDeferLeafSplitToBulkFinish")) {
        return self.shouldDeferLeafSplitToBulkFinish(options);
    }
    return false;
}

fn optionDeferLeafSplitToBatchFinish(options: anytype) bool {
    const Options = @TypeOf(options);
    if (comptime @hasField(Options, "defer_leaf_splits_to_batch_finish")) {
        return @field(options, "defer_leaf_splits_to_batch_finish");
    }
    return false;
}

fn shouldDeferOversizedLeafQuantizedPayload(self: anytype, node: *const types.Node, options: anytype) bool {
    if (!node.is_leaf) return false;
    if (node.members.len <= self.config.leaf_size) return false;
    if (optionDeferLeafSplitToBatchFinish(options)) return true;
    const Options = @TypeOf(options);
    if (comptime @hasField(Options, "defer_leaf_splits_to_bulk_finish")) {
        if (@field(options, "defer_leaf_splits_to_bulk_finish")) {
            return shouldDeferLeafSplitToBulkFinish(self, options);
        }
    }
    return false;
}

fn recordDeferredOversizedLeaf(self: anytype, leaf_id: u64) !void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "recordDeferredOversizedLeaf")) {
        try self.recordDeferredOversizedLeaf(leaf_id);
    }
}

fn noteMutatedCachedQuantized(self: anytype, node_id: u64) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "noteMutatedCachedQuantized")) {
        self.noteMutatedCachedQuantized(node_id);
    }
}

fn invalidateCachedQuantizedIfAvailable(self: anytype, node_id: u64) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "invalidateQuantizedCache")) {
        self.invalidateQuantizedCache(node_id);
    }
}

fn addApplyWorkspaceBytes(self: anytype, bytes: u64) void {
    const Index = comptime childType(@TypeOf(self));
    if (bytes == 0) return;
    if (comptime @hasDecl(Index, "addApplyWorkspaceBytes")) {
        self.addApplyWorkspaceBytes(bytes);
    }
}

fn releaseApplyWorkspaceBytes(self: anytype, bytes: u64) void {
    const Index = comptime childType(@TypeOf(self));
    if (bytes == 0) return;
    if (comptime @hasDecl(Index, "releaseApplyWorkspaceBytes")) {
        self.releaseApplyWorkspaceBytes(bytes);
    }
}

fn lessFixedKeyLookup(_: void, lhs: FixedKeyLookup, rhs: FixedKeyLookup) bool {
    return std.mem.order(u8, lhs.key[0..], rhs.key[0..]) == .lt;
}

pub fn loadNode(self: anytype, txn: anytype, node_id: u64) !types.Node {
    if (try self.getCachedNodeClone(node_id)) |cached| return cached;

    var loaded = try self.loadNodeFromStorage(txn, node_id);
    errdefer loaded.deinit(self.alloc);
    try self.cacheNode(&loaded);
    return loaded;
}

pub fn loadNodeProfiled(
    self: anytype,
    txn: anytype,
    node_id: u64,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !types.Node {
    if (try self.getCachedNodeClone(node_id)) |cached| return cached;

    const start = now_fn();
    var loaded = try self.loadNodeFromStorage(txn, node_id);
    errdefer loaded.deinit(self.alloc);
    try self.cacheNode(&loaded);
    profile.node_cache_miss_ns += elapsed_fn(start);
    profile.node_cache_misses += 1;
    return loaded;
}

pub fn deleteNode(self: anytype, txn: anytype, node_id: u64) !void {
    var key_buf: [12]u8 = undefined;
    self.deleteNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .packed_node)) catch {};
    self.deleteNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .range)) catch {};
    self.deleteNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .posting)) catch {};
    var qkey_buf: [10]u8 = undefined;
    self.deleteNamespaced(txn, .quant, hbc.encodeQuantKey(&qkey_buf, node_id)) catch {};
    self.invalidateNodeCache(node_id);
    self.invalidateQuantizedCache(node_id);
}

pub fn updateParent(self: anytype, txn: anytype, node_id: u64, new_parent: u64, now_fn: fn () u64, elapsed_fn: fn (u64) u64) !void {
    const start = now_fn();
    defer {
        self.write_profile.update_parent_ns += elapsed_fn(start);
        self.write_profile.update_parent_calls += 1;
    }
    var key_buf: [12]u8 = undefined;
    const packed_data = try self.getNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .packed_node));
    const decoded = try hbc.decodePackedNodeValue(packed_data);
    const packed_value = try self.alloc.alloc(u8, hbc.packedNodeValueSize(decoded.centroid_bytes.len, decoded.ids_bytes.len));
    defer self.alloc.free(packed_value);
    var header = decoded.header;
    const old_parent = header.parent;
    header.parent = new_parent;
    _ = try hbc.encodePackedNodeValue(packed_value, header, decoded.covering_radius, decoded.centroid_bytes, decoded.ids_bytes);
    try self.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .packed_node), packed_value);
    self.invalidateNodeCache(node_id);
    if (self.config.use_quantization and (old_parent == 0) != (new_parent == 0)) {
        self.invalidateQuantizedCache(node_id);
        var node = try loadNode(self, txn, node_id);
        defer node.deinit(self.alloc);
        try refreshQuantized(self, txn, &node, now_fn, elapsed_fn);
    }
}

pub fn loadNodeParent(self: anytype, txn: anytype, node_id: u64) !u64 {
    var key_buf: [12]u8 = undefined;
    const packed_data = try self.getNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .packed_node));
    return (try hbc.decodePackedNodeValue(packed_data)).header.parent;
}

pub fn putVector(self: anytype, txn: anytype, vector_id: u64, vector_data: []const f32) !void {
    var key_buf: [10]u8 = undefined;
    try self.putNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id), std.mem.sliceAsBytes(vector_data));
    self.invalidateVectorCache(vector_id);
}

pub fn getVector(self: anytype, txn: anytype, vector_id: u64) ![]f32 {
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id));
    const n_floats = data.len / 4;
    const result = try self.alloc.alloc(f32, n_floats);
    @memcpy(std.mem.sliceAsBytes(result), data);
    return result;
}

pub fn getVectorInto(self: anytype, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
    if (borrowCachedVectorHandle(self, vector_id)) |cached_handle| {
        var handle = cached_handle;
        defer handle.deinit();
        const cached = handle.view();
        if (cached.len > scratch.len) return error.BufferTooSmall;
        @memcpy(scratch[0..cached.len], cached);
        return scratch[0..cached.len];
    }
    const fill = beginVectorCacheFillIfSupported(self, vector_id);
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id));
    const view = try vectorViewFromRaw(data, scratch);
    return try cacheVectorAfterLoad(self, vector_id, view, fill);
}

pub fn getVectorIntoUncached(self: anytype, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id));
    return try vectorViewFromRaw(data, scratch);
}

pub fn getVectorViewOrScratch(self: anytype, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
    return getVectorInto(self, txn, vector_id, scratch);
}

pub fn getVectorViewOrScratchWithCursor(self: anytype, cursor: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
    if (borrowCachedVectorHandle(self, vector_id)) |cached_handle| {
        var handle = cached_handle;
        defer handle.deinit();
        const cached = handle.view();
        if (cached.len > scratch.len) return error.BufferTooSmall;
        @memcpy(scratch[0..cached.len], cached);
        return scratch[0..cached.len];
    }
    const fill = beginVectorCacheFillIfSupported(self, vector_id);
    var key_buf: [10]u8 = undefined;
    const key = hbc.encodeVecKey(&key_buf, vector_id);
    const entry = (try cursor.seekAtOrAfter(key)) orelse return error.NotFound;
    if (!std.mem.eql(u8, entry.key, key)) return error.NotFound;
    const view = try vectorViewFromRaw(entry.value, scratch);
    return try cacheVectorAfterLoad(self, vector_id, view, fill);
}

pub fn getVectorScratch(self: anytype, txn: anytype, vector_id: u64, scratch: []f32) ![]const f32 {
    if (borrowCachedVectorHandle(self, vector_id)) |cached_handle| {
        var handle = cached_handle;
        defer handle.deinit();
        const cached = handle.view();
        if (cached.len > scratch.len) return error.BufferTooSmall;
        @memcpy(scratch[0..cached.len], cached);
        return scratch[0..cached.len];
    }
    const fill = beginVectorCacheFillIfSupported(self, vector_id);
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id));
    const view = try vectorViewFromRaw(data, scratch);
    return try cacheVectorAfterLoad(self, vector_id, view, fill);
}

pub fn vectorViewFromRaw(data: []const u8, scratch: []f32) ![]const f32 {
    const n_floats = data.len / 4;
    if ((@intFromPtr(data.ptr) & (@alignOf(f32) - 1)) == 0) {
        const aligned_ptr: [*]align(@alignOf(f32)) const f32 = @ptrCast(@alignCast(data.ptr));
        return aligned_ptr[0..n_floats];
    }
    if (n_floats > scratch.len) return error.BufferTooSmall;
    @memcpy(std.mem.sliceAsBytes(scratch[0..n_floats]), data);
    return scratch[0..n_floats];
}

pub fn putVecLeaf(self: anytype, txn: anytype, vector_id: u64, leaf_id: u64) !void {
    try posting.AssignmentMap.put(self, txn, vector_id, leaf_id);
}

pub fn getVecLeaf(self: anytype, txn: anytype, vector_id: u64) !u64 {
    return try posting.AssignmentMap.get(self, txn, vector_id);
}

pub fn loadMetadataRaw(self: anytype, txn: anytype, vector_id: u64, is_not_found: fn (anyerror) bool) !?[]const u8 {
    return loadMetadataRawWithCachePolicy(self, txn, vector_id, true, is_not_found);
}

pub fn loadMetadataRawUncached(self: anytype, txn: anytype, vector_id: u64, is_not_found: fn (anyerror) bool) !?[]const u8 {
    return loadMetadataRawWithCachePolicy(self, txn, vector_id, false, is_not_found);
}

fn loadMetadataRawWithCachePolicy(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    comptime use_cache: bool,
    is_not_found: fn (anyerror) bool,
) !?[]const u8 {
    const Index = comptime childType(@TypeOf(self));
    // Retained-cache implementations cannot return a raw cached slice: a
    // concurrent reclaimer may evict it immediately after the lookup lock is
    // released. Scalar callers receive the transaction-owned storage view.
    if (use_cache and comptime !@hasDecl(Index, "borrowCachedMetadata")) {
        if (self.getCachedMetadata(vector_id)) |cached| return cached;
    }
    var key_buf: [10]u8 = undefined;
    const fill = if (use_cache) searchCacheFillForTxn(self, txn) else SearchCacheFill{ .guarded = false, .epoch = null };
    const data = self.getNamespaced(txn, .vecs, hbc.encodeVecMetaKey(&key_buf, vector_id)) catch |err| {
        if (is_not_found(err)) return null;
        return err;
    };
    return if (use_cache) try cacheMetadataAfterLoad(self, vector_id, data, fill) else data;
}

pub fn putMetadata(self: anytype, txn: anytype, vector_id: u64, metadata: []const u8) !void {
    var key_buf: [10]u8 = undefined;
    try self.putNamespaced(txn, .vecs, hbc.encodeVecMetaKey(&key_buf, vector_id), metadata);
    _ = try self.cacheMetadata(vector_id, metadata);
}

pub fn loadNodeSplitRange(self: anytype, txn: anytype, node_id: u64, is_not_found: fn (anyerror) bool) !?types.NodeSplitRange {
    var key_buf: [12]u8 = undefined;
    const data = self.getNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .range)) catch |err| {
        if (is_not_found(err)) return null;
        return err;
    };
    return try bulk_build.decodeNodeRange(self.alloc, data);
}

fn usesNonQuantizedPayload(node: *const types.Node) bool {
    return node.parent == 0;
}

fn hasFreshStoredPayload(node: *const types.Node) bool {
    return !node.posting_state.payload_dirty;
}

fn shouldDeferPostingCentroidRefresh(self: anytype, node: *const types.Node) bool {
    return node.is_leaf and
        self.config.lazy_posting_maintenance and
        node.centroid.len > 0 and
        node.posting_state.centroid_dirty;
}

fn shouldDeferPostingPayloadRefresh(self: anytype, node: *const types.Node) bool {
    return node.is_leaf and
        self.config.lazy_posting_maintenance and
        self.config.use_quantization and
        node.posting_state.payload_dirty;
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

pub fn saveNode(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveNodeWithOptions(self, txn, node, .{}, now_fn, elapsed_fn);
}

pub fn saveNodeWithOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveNodeWithOptionsMode(self, txn, node, options, true, now_fn, elapsed_fn);
}

pub fn saveNodeWithOptionsMode(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    options: anytype,
    write_header: bool,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    const start = now_fn();
    defer {
        self.write_profile.save_node_ns += elapsed_fn(start);
        self.write_profile.save_node_calls += 1;
    }
    try saveNodeBodyInternal(self, txn, node, null, options, write_header, now_fn, elapsed_fn);
    const range_start = now_fn();
    try saveNodeSplitRange(self, txn, node, isNotFoundGeneric);
    self.write_profile.save_split_range_ns += elapsed_fn(range_start);
}

pub fn saveNodeBody(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveNodeBodyInternal(self, txn, node, null, .{}, true, now_fn, elapsed_fn);
}

fn refreshQuantizedWithKnownVectors(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    vectors: []const f32,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (!self.config.use_quantization) return;
    if (node.centroid.len == 0) return;

    var key_buf: [10]u8 = undefined;
    const count = if (node.is_leaf) node.members.len else node.children.len;
    if (count == 0) {
        self.deleteNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node.id)) catch {};
        self.invalidateQuantizedCache(node.id);
        return;
    }

    const dims: usize = @intCast(self.metadata.dims);
    if (vectors.len < count * dims) return error.InvalidArgument;

    const compute_start = now_fn();
    var qs: hbc_runtime.QuantizedSet = if (usesNonQuantizedPayload(node))
        .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(dims),
                .count = @intCast(count),
                .data = try self.alloc.dupe(f32, vectors[0 .. count * dims]),
            },
        } }
    else
        .{ .rabit = try self.quantizer.quantize(node.centroid, vectors[0 .. count * dims], count) };
    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
    defer qs.deinit(self.alloc);

    const store_start = now_fn();
    try saveQuantized(self, txn, node.id, &qs, now_fn, elapsed_fn);
    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
}

fn saveLeafNodeBodyWithKnownVectors(
    self: anytype,
    txn: anytype,
    node: *types.Node,
    vectors: []const f32,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    if (!node.is_leaf) return error.InvalidArgument;

    const start = now_fn();
    defer {
        self.write_profile.save_node_ns += elapsed_fn(start);
        self.write_profile.save_node_calls += 1;
    }

    node.covering_radius = if (self.config.metric == .l2_squared)
        l2CoveringRadiusForMatrix(node.centroid, vectors, node.members.len)
    else
        std.math.nan(f32);
    try savePackedNodeValue(self, txn, node);
    try refreshQuantizedWithKnownVectors(self, txn, node, vectors, nowNsU64Fixed, elapsedSinceU64Fixed);
    clearDeferredQuantizedNode(self, node.id);
    try self.cacheNode(node);
}

fn saveLeafNodeWithKnownVectors(
    self: anytype,
    txn: anytype,
    node: *types.Node,
    vectors: []const f32,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveLeafNodeBodyWithKnownVectors(self, txn, node, vectors, now_fn, elapsed_fn);
    const range_start = now_fn();
    try saveNodeSplitRange(self, txn, node, isNotFoundGeneric);
    self.write_profile.save_split_range_ns += elapsed_fn(range_start);
}

fn copyNodeMemberVectorsFromSource(
    self: anytype,
    node: *const types.Node,
    source_ids: []const u64,
    source_vectors: []const f32,
) ![]f32 {
    if (!node.is_leaf) return error.InvalidArgument;
    const dims: usize = @intCast(self.metadata.dims);
    if (source_vectors.len < source_ids.len * dims) return error.InvalidArgument;

    const out = try self.alloc.alloc(f32, node.members.len * dims);
    errdefer self.alloc.free(out);

    var positions = std.AutoHashMapUnmanaged(u64, usize).empty;
    defer positions.deinit(self.alloc);
    try positions.ensureTotalCapacity(self.alloc, @intCast(source_ids.len));
    for (source_ids, 0..) |source_id, i| {
        positions.putAssumeCapacity(source_id, i);
    }

    for (node.members, 0..) |member_id, i| {
        const source_index = positions.get(member_id) orelse return error.Corrupted;
        const src = source_vectors[source_index * dims ..][0..dims];
        const dst = out[i * dims ..][0..dims];
        @memcpy(dst, src);
    }

    return out;
}

pub fn saveNodeBodyWithAddedVector(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vector: []const f32,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveNodeBodyWithAddedVectorOptions(self, txn, node, transformed_vector, .{}, now_fn, elapsed_fn);
}

pub fn saveNodeBodyWithAddedVectorOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vector: []const f32,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveNodeBodyInternal(
        self,
        txn,
        node,
        transformed_vector,
        options,
        true,
        now_fn,
        elapsed_fn,
    );
}

pub fn saveExistingNodeBodyWithAddedVectorOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vector: []const f32,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveNodeBodyInternal(
        self,
        txn,
        node,
        transformed_vector,
        options,
        false,
        now_fn,
        elapsed_fn,
    );
}

pub fn saveExistingNodeBodyWithAddedVectorsOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vectors: []const f32,
    added_count: usize,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    var posting_state_to_save = node.posting_state;
    var posting_payload_refreshed = node.is_leaf and !self.config.use_quantization;
    const defer_posting_payload_refresh = shouldDeferPostingPayloadRefresh(self, node);

    try savePackedNodeValue(self, txn, node);

    if (defer_posting_payload_refresh) {
        self.write_profile.posting_lazy_payload_deferrals += 1;
    } else if (deferQuantizedRebuild(options) and suppressQuantizedPayloadPersist(options)) {
        self.invalidateQuantizedCache(node.id);
        try recordDeferredQuantizedNode(self, node.id);
    } else if (deferQuantizedRebuild(options)) {
        const quant_start = now_fn();
        if (shouldDeferOversizedLeafQuantizedPayload(self, node, options)) {
            invalidateCachedQuantizedIfAvailable(self, node.id);
            try recordDeferredQuantizedNode(self, node.id);
        } else if (try updateQuantizedWithAddedVectors(self, txn, node, transformed_vectors, added_count, now_fn, elapsed_fn, quant_start)) {
            posting_payload_refreshed = node.is_leaf;
        } else {
            _ = try primeDeferredLeafNonQuantCacheWithAddedVectors(self, txn, node, transformed_vectors, added_count, now_fn, elapsed_fn, quant_start);
            try recordDeferredQuantizedNode(self, node.id);
        }
    } else {
        const quant_start = now_fn();
        if (try updateQuantizedWithAddedVectors(self, txn, node, transformed_vectors, added_count, now_fn, elapsed_fn, quant_start)) {
            posting_payload_refreshed = node.is_leaf;
        } else {
            try refreshQuantizedWithOptions(self, txn, node, options, nowNsU64Fixed, elapsedSinceU64Fixed);
            self.write_profile.refresh_quantized_ns += elapsed_fn(quant_start);
            posting_payload_refreshed = node.is_leaf;
        }
    }
    if (node.is_leaf) {
        if (posting_payload_refreshed) posting_state_to_save.notePayloadRefreshed();
        try posting.PostingStore.saveState(self, txn, node.id, posting_state_to_save);
        var node_for_cache = node.*;
        node_for_cache.posting_state = posting_state_to_save;
        try self.cacheNode(&node_for_cache);
    } else {
        try self.cacheNode(node);
    }
}

pub fn saveNodeBodyInternal(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    added_vector: ?[]const f32,
    options: anytype,
    write_header: bool,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    const defer_quantized_rebuild = deferQuantizedRebuild(options);
    _ = write_header;
    var posting_state_to_save = node.posting_state;
    var posting_payload_refreshed = node.is_leaf and !self.config.use_quantization;
    const defer_posting_payload_refresh = shouldDeferPostingPayloadRefresh(self, node);

    try savePackedNodeValue(self, txn, node);
    if (defer_posting_payload_refresh) {
        self.write_profile.posting_lazy_payload_deferrals += 1;
    } else if (defer_quantized_rebuild and suppressQuantizedPayloadPersist(options)) {
        self.invalidateQuantizedCache(node.id);
        try recordDeferredQuantizedNode(self, node.id);
    } else if (defer_quantized_rebuild) {
        const quant_start = now_fn();
        if (shouldDeferOversizedLeafQuantizedPayload(self, node, options)) {
            invalidateCachedQuantizedIfAvailable(self, node.id);
            try recordDeferredQuantizedNode(self, node.id);
        } else if (added_vector) |v| {
            if (try updateQuantizedWithAddedVector(self, txn, node, v, now_fn, elapsed_fn, quant_start)) {
                posting_payload_refreshed = node.is_leaf;
            } else {
                _ = try primeDeferredLeafNonQuantCacheWithAddedVector(self, txn, node, v, now_fn, elapsed_fn, quant_start);
                try recordDeferredQuantizedNode(self, node.id);
            }
        } else {
            try recordDeferredQuantizedNode(self, node.id);
        }
    } else {
        const quant_start = now_fn();
        if (added_vector) |v| {
            if (try updateQuantizedWithAddedVector(self, txn, node, v, now_fn, elapsed_fn, quant_start)) {
                posting_payload_refreshed = node.is_leaf;
            } else {
                try refreshQuantizedWithOptions(self, txn, node, options, nowNsU64Fixed, elapsedSinceU64Fixed);
                self.write_profile.refresh_quantized_ns += elapsed_fn(quant_start);
                posting_payload_refreshed = node.is_leaf;
            }
        } else {
            try refreshQuantizedWithOptions(self, txn, node, options, nowNsU64Fixed, elapsedSinceU64Fixed);
            self.write_profile.refresh_quantized_ns += elapsed_fn(quant_start);
            posting_payload_refreshed = node.is_leaf;
        }
    }
    if (node.is_leaf) {
        if (posting_payload_refreshed) posting_state_to_save.notePayloadRefreshed();
        try posting.PostingStore.saveState(self, txn, node.id, posting_state_to_save);
        var node_for_cache = node.*;
        node_for_cache.posting_state = posting_state_to_save;
        try self.cacheNode(&node_for_cache);
    } else {
        try self.cacheNode(node);
    }
}

pub fn updateQuantizedWithAddedVector(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vector: []const f32,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
    compute_start: i128,
) !bool {
    if (!self.config.use_quantization) return false;
    if (!node.is_leaf) return false;
    if (node.centroid.len == 0) return false;
    if (node.members.len == 0) return false;

    const previous_count = node.members.len - 1;
    var cached = (loadQuantizedOwned(self, txn, node.id, usesNonQuantizedPayload(node), previous_count, isNotFoundGeneric) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    }) orelse return false;
    defer cached.deinit(self.alloc);

    switch (cached) {
        .nonquant => |*set| {
            const old_len = set.vectors.data.len;
            set.vectors.dims = @intCast(self.config.dims);
            set.vectors.count += 1;
            if (set.vectors.data.len == 0) {
                set.vectors.data = try self.alloc.alloc(f32, old_len + transformed_vector.len);
            } else {
                set.vectors.data = try self.alloc.realloc(set.vectors.data, old_len + transformed_vector.len);
            }
            @memcpy(set.vectors.data[old_len..], transformed_vector);
        },
        .rabit => |*set| {
            try self.quantizer.quantizeWithSet(set, transformed_vector, 1);
        },
    }
    noteMutatedCachedQuantized(self, node.id);
    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);

    const store_start = now_fn();
    try self.putQuantizedCached(txn, node.id, &cached);
    try self.cacheQuantized(node.id, &cached);
    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
    self.write_profile.refresh_quantized_ns += elapsed_fn(compute_start);
    return true;
}

fn primeDeferredLeafNonQuantCacheWithAddedVector(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vector: []const f32,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
    compute_start: i128,
) !bool {
    _ = now_fn;
    if (!self.config.use_quantization) return false;
    if (!node.is_leaf) return false;
    if (!usesNonQuantizedPayload(node)) return false;
    if (node.centroid.len == 0) return false;
    if (node.members.len == 0) return false;

    const previous_count = node.members.len - 1;
    if (previous_count == 0) {
        const fresh: hbc_runtime.QuantizedSet = .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(self.config.dims),
                .count = 1,
                .data = try self.alloc.dupe(f32, transformed_vector),
            },
        } };
        try self.cacheQuantizedOwned(node.id, fresh);
        self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        return true;
    }

    var cached = (try loadQuantizedOwned(self, txn, node.id, true, previous_count, isNotFoundGeneric)) orelse return false;
    defer cached.deinit(self.alloc);
    switch (cached) {
        .nonquant => |*set| {
            const old_len = set.vectors.data.len;
            set.vectors.dims = @intCast(self.config.dims);
            set.vectors.count += 1;
            if (old_len == 0) {
                set.vectors.data = try self.alloc.dupe(f32, transformed_vector);
            } else {
                set.vectors.data = try self.alloc.realloc(set.vectors.data, old_len + transformed_vector.len);
                @memcpy(set.vectors.data[old_len..][0..transformed_vector.len], transformed_vector);
            }
            noteMutatedCachedQuantized(self, node.id);
            try self.cacheQuantized(node.id, &cached);
            self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
            return true;
        },
        .rabit => return false,
    }
}

pub fn updateQuantizedWithAddedVectors(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vectors: []const f32,
    added_count: usize,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
    compute_start: i128,
) !bool {
    if (!self.config.use_quantization) return false;
    if (!node.is_leaf) return false;
    if (node.centroid.len == 0) return false;
    if (node.members.len == 0) return false;
    if (added_count == 0) return true;

    const previous_count = node.members.len - added_count;
    var cached = (loadQuantizedOwned(self, txn, node.id, usesNonQuantizedPayload(node), previous_count, isNotFoundGeneric) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    }) orelse return false;
    defer cached.deinit(self.alloc);

    switch (cached) {
        .nonquant => |*set| {
            const old_len = set.vectors.data.len;
            set.vectors.dims = @intCast(self.config.dims);
            set.vectors.count += @intCast(added_count);
            if (set.vectors.data.len == 0) {
                set.vectors.data = try self.alloc.alloc(f32, old_len + transformed_vectors.len);
            } else {
                set.vectors.data = try self.alloc.realloc(set.vectors.data, old_len + transformed_vectors.len);
            }
            @memcpy(set.vectors.data[old_len..], transformed_vectors);
        },
        .rabit => |*set| {
            try self.quantizer.quantizeWithSet(set, transformed_vectors, added_count);
        },
    }
    noteMutatedCachedQuantized(self, node.id);
    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);

    const store_start = now_fn();
    try self.putQuantizedCached(txn, node.id, &cached);
    try self.cacheQuantized(node.id, &cached);
    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
    self.write_profile.refresh_quantized_ns += elapsed_fn(compute_start);
    return true;
}

fn primeDeferredLeafNonQuantCacheWithAddedVectors(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    transformed_vectors: []const f32,
    added_count: usize,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
    compute_start: i128,
) !bool {
    _ = now_fn;
    if (!self.config.use_quantization) return false;
    if (!node.is_leaf) return false;
    if (!usesNonQuantizedPayload(node)) return false;
    if (node.centroid.len == 0) return false;
    if (node.members.len == 0) return false;
    if (added_count == 0) return true;

    const previous_count = node.members.len - added_count;
    if (previous_count == 0) {
        const fresh: hbc_runtime.QuantizedSet = .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(self.config.dims),
                .count = @intCast(added_count),
                .data = try self.alloc.dupe(f32, transformed_vectors),
            },
        } };
        try self.cacheQuantizedOwned(node.id, fresh);
        self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        return true;
    }

    var cached = (try loadQuantizedOwned(self, txn, node.id, true, previous_count, isNotFoundGeneric)) orelse return false;
    defer cached.deinit(self.alloc);
    switch (cached) {
        .nonquant => |*set| {
            const old_len = set.vectors.data.len;
            set.vectors.dims = @intCast(self.config.dims);
            set.vectors.count += @intCast(added_count);
            if (old_len == 0) {
                set.vectors.data = try self.alloc.dupe(f32, transformed_vectors);
            } else {
                set.vectors.data = try self.alloc.realloc(set.vectors.data, old_len + transformed_vectors.len);
                @memcpy(set.vectors.data[old_len..][0..transformed_vectors.len], transformed_vectors);
            }
            noteMutatedCachedQuantized(self, node.id);
            try self.cacheQuantized(node.id, &cached);
            self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
            return true;
        },
        .rabit => return false,
    }
}

pub fn computeNodeSplitRange(self: anytype, txn: anytype, node: *const types.Node, is_not_found: fn (anyerror) bool) !?types.NodeSplitRange {
    if (node.is_leaf) {
        var min_key: ?[]u8 = null;
        errdefer if (min_key) |key| self.alloc.free(key);
        var max_key: ?[]u8 = null;
        errdefer if (max_key) |key| self.alloc.free(key);

        const metadata_values = try self.alloc.alloc(?[]const u8, node.members.len);
        defer self.alloc.free(metadata_values);
        try getMetadataManySortedInTxn(self, txn, node.members, metadata_values);
        for (metadata_values) |maybe_metadata| {
            const metadata = maybe_metadata orelse continue;
            if (min_key == null) {
                min_key = try self.alloc.dupe(u8, metadata);
                max_key = try self.alloc.dupe(u8, metadata);
                continue;
            }
            if (std.mem.order(u8, metadata, min_key.?) == .lt) {
                self.alloc.free(min_key.?);
                min_key = try self.alloc.dupe(u8, metadata);
            }
            if (std.mem.order(u8, metadata, max_key.?) == .gt) {
                self.alloc.free(max_key.?);
                max_key = try self.alloc.dupe(u8, metadata);
            }
        }

        if (min_key == null or max_key == null) {
            if (min_key) |key| self.alloc.free(key);
            if (max_key) |key| self.alloc.free(key);
            return null;
        }
        return .{
            .min_key = min_key.?,
            .max_key = max_key.?,
        };
    }

    var min_key: ?[]u8 = null;
    errdefer if (min_key) |key| self.alloc.free(key);
    var max_key: ?[]u8 = null;
    errdefer if (max_key) |key| self.alloc.free(key);

    const RangeLookup = struct {
        child_id: u64,
        key: [12]u8,
        key_len: u8,

        fn lessThan(_: void, a: @This(), b: @This()) bool {
            return std.mem.order(u8, a.key[0..a.key_len], b.key[0..b.key_len]) == .lt;
        }
    };
    const lookups = try self.alloc.alloc(RangeLookup, node.children.len);
    defer self.alloc.free(lookups);
    for (node.children, lookups) |child_id, *lookup| {
        lookup.child_id = child_id;
        const key = hbc.encodeNodeKey(&lookup.key, child_id, .range);
        lookup.key_len = @intCast(key.len);
    }
    std.mem.sort(RangeLookup, lookups, {}, RangeLookup.lessThan);

    const keys = try self.alloc.alloc([]const u8, lookups.len);
    defer self.alloc.free(keys);
    for (lookups, keys) |*lookup, *key| key.* = lookup.key[0..lookup.key_len];
    const values = try self.alloc.alloc(?[]const u8, lookups.len);
    defer self.alloc.free(values);
    if (comptime txnSupportsGetManySorted(@TypeOf(txn))) {
        try txn.getManySorted(.nodes, keys, values);
    } else {
        @memset(values, null);
        for (keys, values) |key, *value| {
            value.* = txn.get(.nodes, key) catch |err| {
                if (is_not_found(err)) continue;
                return err;
            };
        }
    }

    for (lookups, values) |lookup, maybe_value| {
        const child_range = blk: {
            if (maybe_value) |value| break :blk try bulk_build.decodeNodeRange(self.alloc, value);
            var child = try loadNode(self, txn, lookup.child_id);
            defer child.deinit(self.alloc);
            const computed = (try computeNodeSplitRange(self, txn, &child, is_not_found)) orelse continue;
            break :blk computed;
        };
        defer {
            var owned = child_range;
            owned.deinit(self.alloc);
        }
        try includeSplitRangeBounds(self.alloc, &min_key, &max_key, &child_range);
    }

    if (min_key == null or max_key == null) {
        if (min_key) |key| self.alloc.free(key);
        if (max_key) |key| self.alloc.free(key);
        return null;
    }
    return .{
        .min_key = min_key.?,
        .max_key = max_key.?,
    };
}

fn includeSplitRangeBounds(
    alloc: std.mem.Allocator,
    min_key: *?[]u8,
    max_key: *?[]u8,
    range: *const types.NodeSplitRange,
) !void {
    if (min_key.* == null) {
        min_key.* = try alloc.dupe(u8, range.min_key);
        errdefer {
            alloc.free(min_key.*.?);
            min_key.* = null;
        }
        max_key.* = try alloc.dupe(u8, range.max_key);
        return;
    }
    if (std.mem.order(u8, range.min_key, min_key.*.?) == .lt) {
        const replacement = try alloc.dupe(u8, range.min_key);
        alloc.free(min_key.*.?);
        min_key.* = replacement;
    }
    if (std.mem.order(u8, range.max_key, max_key.*.?) == .gt) {
        const replacement = try alloc.dupe(u8, range.max_key);
        alloc.free(max_key.*.?);
        max_key.* = replacement;
    }
}

test "internal node split range loads child ranges in one sorted batch" {
    const TestIndex = struct {
        alloc: Allocator,

        fn getCachedMetadata(_: @This(), _: u64) ?[]const u8 {
            return null;
        }

        fn cacheMetadata(_: @This(), _: u64, metadata: []const u8) ![]const u8 {
            return metadata;
        }

        fn getCachedNodeClone(_: @This(), _: u64) !?types.Node {
            return null;
        }

        fn loadNodeFromStorage(_: @This(), _: anytype, _: u64) !types.Node {
            return error.UnexpectedNodeLoad;
        }

        fn cacheNode(_: @This(), _: *const types.Node) !void {}
    };
    const TestTxn = struct {
        first: []const u8,
        second: []const u8,
        calls: usize = 0,

        fn getManySorted(self: *@This(), namespace: anytype, keys: []const []const u8, values: []?[]const u8) !void {
            try std.testing.expectEqual(.nodes, namespace);
            try std.testing.expectEqual(@as(usize, 2), keys.len);
            try std.testing.expect(std.mem.order(u8, keys[0], keys[1]) == .lt);
            self.calls += 1;
            values[0] = self.first;
            values[1] = self.second;
        }
    };

    const first = try bulk_build.encodeNodeRange(std.testing.allocator, &.{
        .min_key = @constCast("a"),
        .max_key = @constCast("c"),
    });
    defer std.testing.allocator.free(first);
    const second = try bulk_build.encodeNodeRange(std.testing.allocator, &.{
        .min_key = @constCast("x"),
        .max_key = @constCast("z"),
    });
    defer std.testing.allocator.free(second);

    var children = [_]u64{ 9, 3 };
    const node = types.Node{
        .id = 1,
        .is_leaf = false,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &children,
        .members = &.{},
    };
    const index = TestIndex{ .alloc = std.testing.allocator };
    var txn = TestTxn{ .first = first, .second = second };
    var range = (try computeNodeSplitRange(index, &txn, &node, isNotFoundGeneric)).?;
    defer range.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), txn.calls);
    try std.testing.expectEqualStrings("a", range.min_key);
    try std.testing.expectEqualStrings("z", range.max_key);
}

test "leaf node split range loads member metadata in one sorted batch" {
    const TestIndex = struct {
        alloc: Allocator,

        fn getCachedMetadata(_: @This(), _: u64) ?[]const u8 {
            return null;
        }

        fn cacheMetadata(_: @This(), _: u64, metadata: []const u8) ![]const u8 {
            return metadata;
        }

        fn getCachedNodeClone(_: @This(), _: u64) !?types.Node {
            return null;
        }

        fn loadNodeFromStorage(_: @This(), _: anytype, _: u64) !types.Node {
            return error.UnexpectedNodeLoad;
        }

        fn cacheNode(_: @This(), _: *const types.Node) !void {}
    };
    const TestTxn = struct {
        calls: usize = 0,

        fn getManySorted(self: *@This(), namespace: anytype, keys: []const []const u8, values: []?[]const u8) !void {
            try std.testing.expectEqual(.vecs, namespace);
            try std.testing.expectEqual(@as(usize, 2), keys.len);
            try std.testing.expect(std.mem.order(u8, keys[0], keys[1]) == .lt);
            self.calls += 1;
            values[0] = "a";
            values[1] = "z";
        }
    };

    var members = [_]u64{ 9, 3 };
    const node = types.Node{
        .id = 1,
        .is_leaf = true,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &.{},
        .members = &members,
    };
    const index = TestIndex{ .alloc = std.testing.allocator };
    var txn = TestTxn{};
    var range = (try computeNodeSplitRange(index, &txn, &node, isNotFoundGeneric)).?;
    defer range.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), txn.calls);
    try std.testing.expectEqualStrings("a", range.min_key);
    try std.testing.expectEqualStrings("z", range.max_key);
}

pub fn saveNodeSplitRange(self: anytype, txn: anytype, node: *const types.Node, is_not_found: fn (anyerror) bool) !void {
    const maybe_range = try computeNodeSplitRange(self, txn, node, is_not_found);
    try putNodeSplitRange(self, txn, node.id, if (maybe_range) |*range| range else null, is_not_found);
    if (maybe_range) |range| {
        var owned = range;
        owned.deinit(self.alloc);
    }
}

pub fn putNodeSplitRange(
    self: anytype,
    txn: anytype,
    node_id: u64,
    range: ?*const types.NodeSplitRange,
    is_not_found: fn (anyerror) bool,
) !void {
    var key_buf: [12]u8 = undefined;
    const key = hbc.encodeNodeKey(&key_buf, node_id, .range);
    if (range) |owned| {
        const encoded = try bulk_build.encodeNodeRange(self.alloc, owned);
        defer self.alloc.free(encoded);
        try self.putNamespaced(txn, .nodes, key, encoded);
        self.write_profile.range_put_calls += 1;
        self.write_profile.range_key_bytes += @intCast(key.len);
        self.write_profile.range_value_bytes += @intCast(encoded.len);
    } else {
        self.deleteNamespaced(txn, .nodes, key) catch |err| {
            if (is_not_found(err)) return;
            return err;
        };
        self.write_profile.range_delete_calls += 1;
        self.write_profile.range_key_bytes += @intCast(key.len);
    }
}

pub fn getMetadata(self: anytype, vector_id: u64) !?[]u8 {
    var txn = try self.beginRuntimeSearchTxn();
    defer txn.abort();

    const data = (try loadMetadataRaw(self, &txn, vector_id, isNotFoundGeneric)) orelse return null;
    return try self.alloc.dupe(u8, data);
}

pub fn getMetadataInTxn(self: anytype, txn: anytype, vector_id: u64, is_not_found: fn (anyerror) bool) !?[]const u8 {
    return loadMetadataRaw(self, txn, vector_id, is_not_found);
}

pub fn getMetadataManySortedInTxn(self: anytype, txn: anytype, vector_ids: []const u64, out_metadata: []?[]const u8) !void {
    const lookups = try self.alloc.alloc(FixedKeyLookup, vector_ids.len);
    defer self.alloc.free(lookups);
    const key_views = try self.alloc.alloc([]const u8, vector_ids.len);
    defer self.alloc.free(key_views);
    const values = try self.alloc.alloc(?[]const u8, vector_ids.len);
    defer self.alloc.free(values);
    try getMetadataManySortedInTxnWithScratch(self, txn, vector_ids, out_metadata, lookups, key_views, values);
}

/// Return transaction-owned metadata views without consulting or populating
/// the retained metadata cache. Single-pass callers such as exact scoring keep
/// the transaction alive for the complete scan and would otherwise pay a
/// clone/lock/eviction cycle for entries they never reuse.
pub fn getMetadataManySortedInTxnUncached(self: anytype, txn: anytype, vector_ids: []const u64, out_metadata: []?[]const u8) !void {
    const lookups = try self.alloc.alloc(FixedKeyLookup, vector_ids.len);
    defer self.alloc.free(lookups);
    const key_views = try self.alloc.alloc([]const u8, vector_ids.len);
    defer self.alloc.free(key_views);
    const values = try self.alloc.alloc(?[]const u8, vector_ids.len);
    defer self.alloc.free(values);
    try getMetadataManySortedInTxnWithScratchProfiled(
        self,
        txn,
        vector_ids,
        out_metadata,
        lookups,
        key_views,
        values,
        false,
        null,
        null,
        null,
    );
}

pub fn getMetadataManySortedInTxnWithScratch(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    out_metadata: []?[]const u8,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
) !void {
    return try getMetadataManySortedInTxnWithScratchProfiled(
        self,
        txn,
        vector_ids,
        out_metadata,
        lookup_storage,
        key_views_storage,
        values_storage,
        true,
        null,
        null,
        null,
    );
}

pub fn getMetadataManySortedInTxnWithScratchUncached(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    out_metadata: []?[]const u8,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
) !void {
    return try getMetadataManySortedInTxnWithScratchProfiled(
        self,
        txn,
        vector_ids,
        out_metadata,
        lookup_storage,
        key_views_storage,
        values_storage,
        false,
        null,
        null,
        null,
    );
}

fn getMetadataManySortedInTxnWithScratchProfiled(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    out_metadata: []?[]const u8,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
    comptime use_cache: bool,
    profile: ?*search_types.SearchProfile,
    now_fn_u64: ?*const fn () u64,
    elapsed_fn_u64: ?*const fn (u64) u64,
) !void {
    if (vector_ids.len != out_metadata.len) return error.InvalidArgument;
    if (lookup_storage.len < vector_ids.len) return error.InvalidArgument;
    if (key_views_storage.len < vector_ids.len) return error.InvalidArgument;
    if (values_storage.len < vector_ids.len) return error.InvalidArgument;
    for (out_metadata) |*slot| slot.* = null;
    if (vector_ids.len == 0) return;

    var lookup_count: usize = 0;
    const Index = comptime childType(@TypeOf(self));
    for (vector_ids, 0..) |vector_id, index| {
        // See loadMetadataRaw: this API returns views that remain live after
        // the function returns, so retained-cache adapters must use the
        // transaction-owned ordered read. Result population uses borrowed
        // handles where the lifetime is naturally bounded.
        if (use_cache and comptime !@hasDecl(Index, "borrowCachedMetadata")) {
            if (self.getCachedMetadata(vector_id)) |cached| {
                if (profile) |p| p.metadata_cache_hits += 1;
                out_metadata[index] = cached;
                continue;
            }
        }
        if (profile) |p| p.metadata_cache_misses += 1;
        var key: [10]u8 = undefined;
        _ = hbc.encodeVecMetaKey(&key, vector_id);
        lookup_storage[lookup_count] = .{
            .item_index = index,
            .vector_id = vector_id,
            .key = key,
        };
        lookup_count += 1;
    }
    if (lookup_count == 0) return;

    const lookups = lookup_storage[0..lookup_count];
    const key_views = key_views_storage[0..lookup_count];
    const values = values_storage[0..lookup_count];
    @memset(values, null);
    std.mem.sort(FixedKeyLookup, lookups, {}, lessFixedKeyLookup);
    for (lookups, 0..) |*lookup, i| key_views[i] = lookup.key[0..];

    const fill = if (use_cache) searchCacheFillForTxn(self, txn) else SearchCacheFill{ .guarded = false, .epoch = null };
    const miss_start = if (now_fn_u64) |now| now() else 0;
    if (comptime txnSupportsGetManySorted(@TypeOf(txn))) {
        try txn.getManySorted(.vecs, key_views, values);
    } else {
        for (key_views, 0..) |key, i| {
            values[i] = txn.get(.vecs, key) catch |err| switch (err) {
                error.NotFound => null,
                else => if (isNotFoundGeneric(err)) null else return err,
            };
        }
    }
    if (profile) |p| p.metadata_cache_miss_ns += elapsed_fn_u64.?(miss_start);
    for (values, 0..) |maybe_value, i| {
        const value = maybe_value orelse continue;
        out_metadata[lookups[i].item_index] = if (use_cache)
            try cacheMetadataAfterLoad(self, lookups[i].vector_id, value, fill)
        else
            value;
    }
}

test "getMetadataManySortedInTxnWithScratch validates scratch capacity" {
    const TestIndex = struct {
        alloc: Allocator,

        fn getCachedMetadata(_: @This(), _: u64) ?[]const u8 {
            return null;
        }

        fn cacheMetadata(_: @This(), _: u64, metadata: []const u8) ![]const u8 {
            return metadata;
        }
    };

    const TestTxn = struct {
        fn getManySorted(_: @This(), _: anytype, _: []const []const u8, _: []?[]const u8) !void {}
    };

    var out_metadata: [2]?[]const u8 = .{ null, null };
    var lookups: [1]FixedKeyLookup = undefined;
    var key_views: [2][]const u8 = undefined;
    var values: [2]?[]const u8 = undefined;
    const index = TestIndex{ .alloc = std.testing.allocator };
    const txn = TestTxn{};

    try std.testing.expectError(
        error.InvalidArgument,
        getMetadataManySortedInTxnWithScratch(
            index,
            txn,
            &.{ 1, 2 },
            out_metadata[0..],
            lookups[0..],
            key_views[0..],
            values[0..],
        ),
    );
}

pub fn getNodeSplitRange(self: anytype, node_id: u64, is_not_found: fn (anyerror) bool) !?types.NodeSplitRange {
    var txn = try self.beginRuntimeReadTxn();
    defer txn.abort();
    return try loadNodeSplitRange(self, &txn, node_id, is_not_found);
}

pub fn classifyNodeForSplitInTxn(
    self: anytype,
    txn: anytype,
    node_id: u64,
    split_key: []const u8,
    is_not_found: fn (anyerror) bool,
) !types.NodeSplitClass {
    const maybe_range = try loadNodeSplitRange(self, txn, node_id, is_not_found);
    if (maybe_range) |range| {
        var owned = range;
        defer owned.deinit(self.alloc);
        return owned.classify(split_key);
    }
    var node = try loadNode(self, txn, node_id);
    defer node.deinit(self.alloc);
    const computed = try computeNodeSplitRange(self, txn, &node, is_not_found);
    if (computed) |range| {
        var owned = range;
        defer owned.deinit(self.alloc);
        return owned.classify(split_key);
    }
    return .unknown;
}

pub fn classifyNodeForSplit(
    self: anytype,
    node_id: u64,
    split_key: []const u8,
    is_not_found: fn (anyerror) bool,
) !types.NodeSplitClass {
    var txn = try self.beginRuntimeReadTxn();
    defer txn.abort();
    return try classifyNodeForSplitInTxn(self, &txn, node_id, split_key, is_not_found);
}

pub fn search(self: anytype, query: []const f32, k: usize, now_fn_u64: fn () u64, elapsed_fn_u64: fn (u64) u64) !search_results.SearchResults {
    const profiled = try searchProfiledRequest(self, .{ .query = query, .k = k }, now_fn_u64, elapsed_fn_u64);
    return profiled.results;
}

pub fn searchWithRequest(
    self: anytype,
    req: search_types.SearchRequest,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !search_results.SearchResults {
    const profiled = try searchProfiledRequest(self, req, now_fn_u64, elapsed_fn_u64);
    return profiled.results;
}

pub fn searchProfiled(
    self: anytype,
    query: []const f32,
    k: usize,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !search_types.ProfiledSearchResults {
    return searchProfiledRequest(self, .{ .query = query, .k = k }, now_fn_u64, elapsed_fn_u64);
}

pub fn searchProfiledRequest(
    self: anytype,
    req: search_types.SearchRequest,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !search_types.ProfiledSearchResults {
    if (!search_types.requiresExhaustiveCoverage(req)) {
        return try searchProfiledRequestAttempt(self, req, null, false, now_fn_u64, elapsed_fn_u64);
    }

    const Index = comptime childType(@TypeOf(self));
    var pessimistic = false;
    while (true) {
        const capture_durable_snapshot = pessimistic and comptime @hasDecl(Index, "beginCompleteSnapshotRead");
        const token = CompleteSnapshotAttempt{
            .snapshot = try loadStableSearchPublishedSnapshot(self, req.cancellation),
            .mutation_epoch = publishedMutationEpoch(self),
        };

        const profiled = (if (capture_durable_snapshot)
            searchProfiledRequestAttempt(self, req, token.snapshot, true, now_fn_u64, elapsed_fn_u64)
        else
            searchProfiledRequestAttempt(self, req, token.snapshot, false, now_fn_u64, elapsed_fn_u64)) catch |err| {
            const attempt_current = completeSnapshotAttemptStillCurrent(self, token);
            if (err == error.StalePublishedSnapshot or
                (err == error.IncompletePublishedSnapshot and !attempt_current))
            {
                // The attempt overlapped a publisher. Retry under a short
                // reader fence to capture a durable MVCC transaction.
                // Traversal then runs cache-free after releasing the fence, so
                // writers are not serialized behind the O(N) exhaustive pass.
                pessimistic = true;
                continue;
            }
            if (err == error.IncompletePublishedSnapshot) {
                noteIncompletePublishedSnapshotIfSupported(self, token.snapshot.publish_generation);
            }
            return err;
        };

        if (capture_durable_snapshot) return profiled;
        if (completeSnapshotAttemptStillCurrent(self, token)) return profiled;

        var stale = profiled;
        stale.results.deinit();
        pessimistic = true;
    }
}

const CompleteSnapshotAttempt = struct {
    snapshot: SearchPublishedSnapshot,
    mutation_epoch: u64,
};

fn publishedMutationEpoch(self: anytype) u64 {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "publishedMutationEpoch")) return self.publishedMutationEpoch();
    return 0;
}

fn completeSnapshotAttemptStillCurrent(self: anytype, attempt: CompleteSnapshotAttempt) bool {
    return publishedSnapshotStillCurrent(self, attempt.snapshot) and
        publishedMutationEpoch(self) == attempt.mutation_epoch and
        (attempt.mutation_epoch & 1) == 0;
}

fn searchProfiledRequestAttempt(
    self: anytype,
    req: search_types.SearchRequest,
    expected_snapshot: ?SearchPublishedSnapshot,
    comptime capture_durable_snapshot: bool,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !search_types.ProfiledSearchResults {
    var profile = search_types.SearchProfile{};
    const Index = comptime childType(@TypeOf(self));
    defer if (!capture_durable_snapshot and comptime @hasDecl(Index, "observeSearchCacheBenefit")) {
        self.observeSearchCacheBenefit(&profile);
    };
    const total_start = now_fn_u64();
    try search_types.checkCancelled(req);
    const coverage_policy = search_types.coveragePolicy(req);
    const exhaustive_coverage = coverage_policy == .complete_snapshot;
    var published_snapshot = expected_snapshot orelse try loadStableSearchPublishedSnapshot(self, req.cancellation);
    if (published_snapshot.active_count == 0) {
        if (capture_durable_snapshot and comptime @hasDecl(Index, "beginCompleteSnapshotRead")) {
            try self.beginCompleteSnapshotRead(req.cancellation);
            defer self.endCompleteSnapshotRead();
            if (!publishedSnapshotStillCurrent(self, published_snapshot)) return error.StalePublishedSnapshot;
            const empty = search_results.SearchResults.init(self.alloc, req.k);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{ .results = empty, .profile = profile };
        }
        if (!exhaustive_coverage or publishedSnapshotStillCurrent(self, published_snapshot)) {
            const empty = search_results.SearchResults.init(self.alloc, req.k);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{
                .results = empty,
                .profile = profile,
            };
        }
        published_snapshot = try loadStableSearchPublishedSnapshot(self, req.cancellation);
    }
    var coverage_tracker = CompleteCoverageTracker.init(try beginCompleteCoverageValidationIfSupported(
        self,
        exhaustive_coverage,
        published_snapshot.publish_generation,
        req.cancellation,
    ), published_snapshot.active_count);
    defer if (coverage_tracker.claim_held) {
        finishCompleteCoverageValidationIfSupported(self, published_snapshot.publish_generation, false);
    };
    // A waiter may have spent time behind the generation validator. Do not
    // retain an MVCC snapshot or request workspace while waiting, and honor a
    // disconnect before acquiring either resource.
    try search_types.checkCancelled(req);
    const setup_start = total_start;
    const txn_start = now_fn_u64();
    var snapshot_fence_held = false;
    if (capture_durable_snapshot and comptime @hasDecl(Index, "beginCompleteSnapshotRead")) {
        try self.beginCompleteSnapshotRead(req.cancellation);
        snapshot_fence_held = true;
        if (!publishedSnapshotStillCurrent(self, published_snapshot)) {
            self.endCompleteSnapshotRead();
            snapshot_fence_held = false;
            return error.StalePublishedSnapshot;
        }
    }
    errdefer if (snapshot_fence_held) self.endCompleteSnapshotRead();
    var txn = if (comptime @hasDecl(Index, "beginRuntimeSearchTxnForCoverage"))
        try self.beginRuntimeSearchTxnForCoverage(exhaustive_coverage)
    else
        try self.beginRuntimeSearchTxn();
    defer txn.abort();
    if (exhaustive_coverage and !publishedSnapshotStillCurrent(self, published_snapshot)) {
        return error.StalePublishedSnapshot;
    }
    if (snapshot_fence_held) {
        self.endCompleteSnapshotRead();
        snapshot_fence_held = false;
    }
    if (exhaustive_coverage) notifyCompleteSnapshotCapturedForTestIfSupported(self);
    hbc_runtime.beginSearchEpoch(self);
    defer hbc_runtime.endSearchEpoch(self);
    const use_search_cache = !capture_durable_snapshot;
    profile.runtime_txn_ns += elapsed_fn_u64(txn_start);
    if (use_search_cache and comptime @hasDecl(Index, "pinUpperTreeCache")) {
        try self.pinUpperTreeCache(&txn);
    }
    var filter_state = try search_types.RequestFilterState.init(self.alloc, req);
    defer filter_state.deinit(self.alloc);
    const scratch_start = now_fn_u64();
    var scratch_handle = try self.acquireSearchScratch();
    profile.scratch_acquire_ns += elapsed_fn_u64(scratch_start);
    defer {
        if (comptime @hasDecl(Index, "refreshSearchScratchAccounting")) {
            self.refreshSearchScratchAccounting(&scratch_handle);
        }
        self.releaseSearchScratch(&scratch_handle);
    }
    const scratch = &scratch_handle.scratch;
    const transformed_query = self.transformVector(req.query, scratch.transformed_query);
    const transformed_query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(req.query, req.query),
        .cosine => vec.norm(transformed_query),
        .inner_product => 0,
    };
    const exact_query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(req.query, req.query),
        .cosine => vec.norm(req.query),
        .inner_product => 0,
    };
    const search_width = req.search_width orelse self.config.search_width;
    const epsilon = req.epsilon orelse self.config.epsilon;
    const rerank_factor: usize = req.rerank_factor orelse search_mod.rerankFactor(epsilon);
    const should_rerank = self.config.use_quantization and self.config.rerank_policy != .never;
    const candidate_limit: usize = if (should_rerank) req.k * rerank_factor else req.k;
    const candidate_capacity: usize = search_mod.candidateCapacity(search_width, self.metadata.branching_factor);
    const root_node_id = published_snapshot.root_node;

    try coverage_tracker.prepare(self, scratch);
    if (exhaustive_coverage) {
        try scratch.resetCoverageVisited(self.alloc, published_snapshot.node_count);
    }
    if (exhaustive_coverage and comptime @hasDecl(Index, "refreshSearchScratchAccounting")) {
        // Coverage storage is retained by SearchScratch and governed just like
        // the rest of the request workspace. Account immediately after growth
        // so a large first-generation validation is never invisible.
        self.refreshSearchScratchAccounting(&scratch_handle);
    }

    var candidates = std.PriorityQueue(types.PriorityItem, void, search_types.candidateLessThan).initContext({});
    defer candidates.deinit(self.alloc);
    try candidates.ensureTotalCapacity(self.alloc, candidate_capacity);

    var approx_results = try search_results.ApproxSearchResults.initCapacity(self.alloc, req.k, candidate_limit, candidate_limit);
    errdefer approx_results.deinit();
    profile.setup_ns += elapsed_fn_u64(setup_start);

    if (self.config.centroid_directory_mode == .flat_rabitq and self.config.use_quantization) {
        const configured_probe_count = if (self.config.flat_centroid_probe_count != 0)
            self.config.flat_centroid_probe_count
        else
            @as(usize, @intCast(search_width));
        const requested_probe_limit = @max(configured_probe_count, @as(usize, 1));
        // The flat directory is compact (one id/radius plus a quantized
        // centroid per posting). Keep the full ordered frontier so selective
        // filters can advance without rebuilding it and stopping can be based
        // on a real lower bound instead of the old fixed probe cutoff.
        const probes = try spfresh_index.selectFlatRabitqPostingsAlloc(
            self,
            &txn,
            transformed_query,
            &scratch_handle,
            &profile,
            coverage_policy,
            if (exhaustive_coverage) .{
                .root_node = published_snapshot.root_node,
                .node_count = published_snapshot.node_count,
                .publish_generation = published_snapshot.publish_generation,
            } else null,
            req.cancellation,
            now_fn_u64,
            elapsed_fn_u64,
        );
        const probe_count = probes.len;
        const initial_probe_limit = if (exhaustive_coverage)
            probe_count
        else
            @min(requested_probe_limit, probe_count);

        var flat_leaves_scored: usize = 0;
        var previous_wave_end: usize = 0;
        var next_wave_end: usize = @min(initial_probe_limit, probe_count);
        profile.traversal_initial_wave_leaves = @intCast(@min(next_wave_end, std.math.maxInt(u32)));
        for (probes[0..probe_count], 0..) |probe, i| {
            if (i == next_wave_end and next_wave_end < probe_count) {
                profile.traversal_waves += 1;
                profile.traversal_max_wave_leaves = @max(
                    profile.traversal_max_wave_leaves,
                    @as(u64, @intCast(next_wave_end - previous_wave_end)),
                );
                // Covering-radius bounds remain shadow telemetry until their
                // end-to-end certification includes payload freshness and
                // quantized candidate retention. Preserve the ANN budget as a
                // floor, then expand only when filters or sparse postings have
                // not filled the requested rerank pool.
                if (!exhaustive_coverage and approx_results.items.items.len >= candidate_limit) {
                    profile.traversal_frontier_remaining = @intCast(probe_count - i);
                    break;
                }

                const explored = @max(flat_leaves_scored, 1);
                const eligible = @max(profile.traversal_eligible_vectors, 1);
                const still_needed = @max(candidate_limit -| approx_results.items.items.len, 1);
                const projected_more = std.math.divCeil(
                    usize,
                    still_needed *| explored,
                    @as(usize, @intCast(eligible)),
                ) catch probe_count;
                const current_wave = @max(next_wave_end - previous_wave_end, 1);
                const next_size = @max(current_wave, @min(projected_more, current_wave *| 2));
                previous_wave_end = next_wave_end;
                next_wave_end = @min(probe_count, next_wave_end +| next_size);
            }
            if (i % 64 == 0) try search_types.checkCancelled(req);
            profile.nodes_visited += 1;
            var leaf_handle = loadNodeReadHandleProfiledWithCachePolicy(self, &txn, probe.posting_id, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64) catch |err| {
                try handleTraversalNodeLoadError(err, coverage_policy);
                continue;
            };
            var leaf_handle_active = true;
            defer if (leaf_handle_active) leaf_handle.deinit(self.alloc);
            const leaf = leaf_handle.ptr();
            if (!leaf.is_leaf) {
                leaf_handle.deinit(self.alloc);
                leaf_handle_active = false;
                if (exhaustive_coverage) return error.IncompletePublishedSnapshot;
                continue;
            }
            const leaf_posting = try posting.PostingStore.view(leaf);
            const member_ids = try posting.PostingStore.copyMemberIds(self.alloc, scratch, leaf_posting);
            try coverage_tracker.observe(self, &txn, scratch, leaf_posting.id, member_ids);
            const leaf_id = leaf_posting.id;
            const leaf_uses_nonquantized_payload = leaf_posting.usesNonQuantizedPayload();
            const leaf_has_fresh_stored_payload = leaf_posting.hasFreshStoredPayload();
            leaf_handle.deinit(self.alloc);
            leaf_handle_active = false;
            try @This().scoreLeafMemberIds(self, &txn, leaf_id, leaf_uses_nonquantized_payload, leaf_has_fresh_stored_payload, member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64);
            profile.leaves_explored += 1;
            flat_leaves_scored += 1;
        }

        if (flat_leaves_scored > previous_wave_end) {
            profile.traversal_waves += 1;
            profile.traversal_max_wave_leaves = @max(
                profile.traversal_max_wave_leaves,
                @as(u64, @intCast(flat_leaves_scored - previous_wave_end)),
            );
        }

        if (flat_leaves_scored > 0) {
            try validateCompleteCoverage(self, &txn, scratch, &coverage_tracker, published_snapshot.publish_generation);
            if (should_rerank) {
                const reranked = try rerankResultsWithCachePolicy(self, &txn, &approx_results, req.query, exact_query_measure, req, &filter_state, scratch, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64);
                approx_results.deinit();
                profile.total_ns = elapsed_fn_u64(total_start);
                return .{ .results = reranked, .profile = profile };
            }

            var results = try approx_results.toFinalResults();
            approx_results.deinit();
            results.sort();
            if (req.load_metadata) try populateMetadataWithCachePolicy(self, &txn, &results, use_search_cache);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{ .results = results, .profile = profile };
        }
    }

    const root_start = now_fn_u64();
    var root_handle = loadNodeReadHandleProfiledWithCachePolicy(self, &txn, root_node_id, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64) catch |err| switch (err) {
        error.NotFound => {
            if (exhaustive_coverage) return error.IncompletePublishedSnapshot;
            approx_results.deinit();
            const empty = search_results.SearchResults.init(self.alloc, req.k);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{
                .results = empty,
                .profile = profile,
            };
        },
        else => return err,
    };
    var root_handle_active = true;
    defer if (root_handle_active) root_handle.deinit(self.alloc);
    profile.root_load_ns += elapsed_fn_u64(root_start);
    if (exhaustive_coverage and !scratch.markCoverageNodeVisited(root_node_id, published_snapshot.node_count)) {
        return error.IncompletePublishedSnapshot;
    }

    {
        const root = root_handle.ptr();
        if (root.is_leaf) {
            const root_posting = try posting.PostingStore.view(root);
            const member_ids = try posting.PostingStore.copyMemberIds(self.alloc, scratch, root_posting);
            try coverage_tracker.observe(self, &txn, scratch, root_posting.id, member_ids);
            const leaf_id = root_posting.id;
            const leaf_uses_nonquantized_payload = root_posting.usesNonQuantizedPayload();
            const leaf_has_fresh_stored_payload = root_posting.hasFreshStoredPayload();
            root_handle.deinit(self.alloc);
            root_handle_active = false;
            @This().scoreLeafMemberIds(self, &txn, leaf_id, leaf_uses_nonquantized_payload, leaf_has_fresh_stored_payload, member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64) catch |err| switch (err) {
                error.NotFound => {
                    if (coverage_policy == .complete_snapshot) return error.IncompletePublishedSnapshot;
                    approx_results.deinit();
                    const empty = search_results.SearchResults.init(self.alloc, req.k);
                    profile.total_ns = elapsed_fn_u64(total_start);
                    return .{
                        .results = empty,
                        .profile = profile,
                    };
                },
                else => return err,
            };
            try validateCompleteCoverage(self, &txn, scratch, &coverage_tracker, published_snapshot.publish_generation);
            if (should_rerank) {
                const reranked = try rerankResultsWithCachePolicy(self, &txn, &approx_results, req.query, exact_query_measure, req, &filter_state, scratch, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64);
                approx_results.deinit();
                profile.total_ns = elapsed_fn_u64(total_start);
                return .{ .results = reranked, .profile = profile };
            }
            var results = try approx_results.toFinalResults();
            approx_results.deinit();
            results.sort();
            if (req.load_metadata) try populateMetadataWithCachePolicy(self, &txn, &results, use_search_cache);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{ .results = results, .profile = profile };
        }

        try scratch.ensureMemberIdCapacity(self.alloc, root.children.len);
        const root_child_ids = scratch.member_ids[0..root.children.len];
        @memcpy(root_child_ids, root.children);
        const root_id = root.id;
        const root_uses_nonquantized_payload = usesNonQuantizedPayload(root);
        root_handle.deinit(self.alloc);
        root_handle_active = false;
        try addChildCandidatesFromIds(self, &txn, root_id, root_uses_nonquantized_payload, root_child_ids, transformed_query, transformed_query_measure, &candidates, scratch, &profile, coverage_policy, use_search_cache, now_fn_u64, elapsed_fn_u64);
    }

    var beam_state = search_mod.BeamSearchState{};
    const initial_wave_leaves: u32 = @min(search_width, @max(@as(u32, 4), @min(@as(u32, @intCast(req.k)), @as(u32, 16))));
    var previous_wave_leaf: u32 = 0;
    var next_wave_leaf: u32 = initial_wave_leaves;
    profile.traversal_initial_wave_leaves = initial_wave_leaves;
    while (true) {
        try search_types.checkCancelled(req);
        // Best-effort search avoids the exhaustive visited bitmap on its hot
        // path, but still needs a corruption ceiling: a valid strict tree can
        // never pop more nodes than the published node high-water mark.
        if (!exhaustive_coverage and profile.nodes_visited >= published_snapshot.node_count) break;
        if (!exhaustive_coverage and beam_state.leaves_explored >= next_wave_leaf) {
            profile.traversal_waves += 1;
            profile.traversal_max_wave_leaves = @max(profile.traversal_max_wave_leaves, next_wave_leaf - previous_wave_leaf);
            if (next_wave_leaf < search_width) {
                const explored = @max(beam_state.leaves_explored, 1);
                const eligible = @max(profile.traversal_eligible_vectors, 1);
                const still_needed = @max(candidate_limit -| approx_results.items.items.len, 1);
                const projected_more_u64 = std.math.divCeil(
                    u64,
                    @as(u64, @intCast(still_needed)) *| explored,
                    eligible,
                ) catch @as(u64, search_width);
                const current_wave = @max(next_wave_leaf - previous_wave_leaf, 1);
                const projected_more: u32 = @intCast(@min(projected_more_u64, std.math.maxInt(u32)));
                const next_size = @max(current_wave, @min(projected_more, current_wave *| 2));
                previous_wave_leaf = next_wave_leaf;
                next_wave_leaf = @min(search_width, next_wave_leaf +| next_size);
            }
        }
        var candidate = candidates.pop() orelse break;
        if (!exhaustive_coverage and search_mod.shouldStopBeamSearch(&beam_state, search_width)) break;
        if (exhaustive_coverage and !scratch.markCoverageNodeVisited(candidate.id, published_snapshot.node_count)) {
            return error.IncompletePublishedSnapshot;
        }
        profile.nodes_visited += 1;

        var node_handle = loadNodeReadHandleProfiledWithCachePolicy(self, &txn, candidate.id, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64) catch |err| {
            try handleTraversalNodeLoadError(err, coverage_policy);
            continue;
        };
        var node_handle_active = true;
        defer if (node_handle_active) node_handle.deinit(self.alloc);
        const node = node_handle.ptr();
        if (!candidate.bound_resolved) {
            profile.traversal_bound_resolutions += 1;
            // Mutable ancestors are intentionally not used as proof objects:
            // foreground appends update a posting without rewriting its whole
            // ancestor chain. Resolve/expand those internal nodes first, then
            // order the resulting leaf frontier by durable posting radii.
            if (node.is_leaf and self.config.metric == .l2_squared) {
                if (l2SubtreeLowerBound(candidate, node.covering_radius)) |lower_bound| {
                    candidate.lower_bound = lower_bound;
                    candidate.bound_resolved = true;
                    candidate.is_leaf = node.is_leaf;
                } else {
                    profile.traversal_bound_fallbacks += 1;
                    candidate.bound_resolved = true;
                }
            } else {
                profile.traversal_bound_fallbacks += 1;
                candidate.bound_resolved = true;
            }
        }
        const allow_dynamic_pruning = !exhaustive_coverage and self.config.metric != .inner_product;
        if (allow_dynamic_pruning and !node.is_leaf and search_mod.shouldBreakOnInternalCandidate(candidate, &approx_results)) {
            node_handle.deinit(self.alloc);
            node_handle_active = false;
            break;
        }
        if (allow_dynamic_pruning and !node.is_leaf and search_mod.shouldSkipInternalCandidate(candidate, &approx_results, &beam_state, epsilon)) {
            node_handle.deinit(self.alloc);
            node_handle_active = false;
            continue;
        }

        if (node.is_leaf) {
            if (allow_dynamic_pruning and search_mod.shouldSkipLeafCandidate(candidate, &approx_results, &beam_state, epsilon)) {
                node_handle.deinit(self.alloc);
                node_handle_active = false;
                continue;
            }
            const leaf_posting = try posting.PostingStore.view(node);
            const member_ids = try posting.PostingStore.copyMemberIds(self.alloc, scratch, leaf_posting);
            try coverage_tracker.observe(self, &txn, scratch, leaf_posting.id, member_ids);
            const leaf_id = leaf_posting.id;
            const leaf_uses_nonquantized_payload = leaf_posting.usesNonQuantizedPayload();
            const leaf_has_fresh_stored_payload = leaf_posting.hasFreshStoredPayload();
            node_handle.deinit(self.alloc);
            node_handle_active = false;
            try @This().scoreLeafMemberIds(self, &txn, leaf_id, leaf_uses_nonquantized_payload, leaf_has_fresh_stored_payload, member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64);
            search_mod.noteLeafExplored(&beam_state);
            profile.leaves_explored += 1;
        } else {
            try scratch.ensureMemberIdCapacity(self.alloc, node.children.len);
            const child_ids = scratch.member_ids[0..node.children.len];
            @memcpy(child_ids, node.children);
            const node_id = node.id;
            const node_uses_nonquantized_payload = usesNonQuantizedPayload(node);
            node_handle.deinit(self.alloc);
            node_handle_active = false;
            try addChildCandidatesFromIds(self, &txn, node_id, node_uses_nonquantized_payload, child_ids, transformed_query, transformed_query_measure, &candidates, scratch, &profile, coverage_policy, use_search_cache, now_fn_u64, elapsed_fn_u64);
        }
    }

    try validateCompleteCoverage(self, &txn, scratch, &coverage_tracker, published_snapshot.publish_generation);

    if (should_rerank) {
        const reranked = try rerankResultsWithCachePolicy(self, &txn, &approx_results, req.query, exact_query_measure, req, &filter_state, scratch, &profile, use_search_cache, now_fn_u64, elapsed_fn_u64);
        approx_results.deinit();
        profile.total_ns = elapsed_fn_u64(total_start);
        return .{ .results = reranked, .profile = profile };
    }

    var results = try approx_results.toFinalResults();
    approx_results.deinit();
    results.sort();
    if (req.load_metadata) try populateMetadataWithCachePolicy(self, &txn, &results, use_search_cache);
    profile.total_ns = elapsed_fn_u64(total_start);
    return .{ .results = results, .profile = profile };
}

fn publishSearchStateIfSupported(self: anytype) !void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "shouldPublishSearchStateAfterWrite")) {
        if (!self.shouldPublishSearchStateAfterWrite()) return;
    }
    if (comptime @hasDecl(Index, "refreshPublishedSearchStateIo")) {
        try self.refreshPublishedSearchStateIo();
    } else if (comptime @hasDecl(Index, "refreshPublishedSearchState")) {
        self.refreshPublishedSearchState();
    }
}

fn beginPublishSearchStateIfSupported(self: anytype) !bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "shouldPublishSearchStateAfterWrite")) {
        if (!self.shouldPublishSearchStateAfterWrite()) return false;
    }
    if (comptime @hasDecl(Index, "beginPublishedSearchStateRefreshIo")) {
        try self.beginPublishedSearchStateRefreshIo();
        return true;
    }
    if (comptime @hasDecl(Index, "beginPublishedSearchStateRefresh")) {
        self.beginPublishedSearchStateRefresh();
        return true;
    }
    return false;
}

fn finishPublishSearchStateIfSupported(self: anytype, publishing: bool) !void {
    if (!publishing) {
        try publishSearchStateIfSupported(self);
        return;
    }
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "finishPublishedSearchStateRefresh")) {
        self.finishPublishedSearchStateRefresh();
    }
}

fn markPublishSearchStateCommittingIfSupported(self: anytype, publishing: bool) !void {
    if (!publishing) return;
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "markPublishedSearchStateCommitting")) {
        try self.markPublishedSearchStateCommitting();
    }
}

fn abortPublishSearchStateIfSupported(self: anytype, publishing: bool) void {
    if (!publishing) return;
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "abortPublishedSearchStateRefresh")) {
        self.abortPublishedSearchStateRefresh();
    }
}

const SearchPublishedSnapshot = struct {
    root_node: u64,
    active_count: u64,
    node_count: u64,
    publish_generation: u64,
};

fn waitForStableSearchPublicationIfSupported(
    self: anytype,
    generation: u64,
    cancellation: ?search_types.CancellationToken,
) !void {
    const Index = comptime childType(@TypeOf(self));
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

fn loadStableSearchPublishedSnapshot(
    self: anytype,
    cancellation: ?search_types.CancellationToken,
) !SearchPublishedSnapshot {
    const Index = comptime childType(@TypeOf(self));
    if (comptime !@hasDecl(Index, "publishedGeneration")) {
        return .{
            .root_node = searchRootNode(self),
            .active_count = self.metadata.active_count,
            .node_count = self.metadata.node_count,
            .publish_generation = 0,
        };
    }

    while (true) {
        const generation = self.publishedGeneration();
        if ((generation & 1) != 0) {
            try waitForStableSearchPublicationIfSupported(self, generation, cancellation);
            continue;
        }
        const snapshot: SearchPublishedSnapshot = .{
            .root_node = self.publishedRootNode(),
            .active_count = self.publishedActiveCount(),
            .node_count = self.publishedNodeCount(),
            .publish_generation = generation,
        };
        const generation_after = self.publishedGeneration();
        if (generation == generation_after and (generation_after & 1) == 0) return snapshot;
        try waitForStableSearchPublicationIfSupported(self, generation_after, cancellation);
    }
}

fn publishedSnapshotStillCurrent(self: anytype, snapshot: SearchPublishedSnapshot) bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime !@hasDecl(Index, "publishedGeneration")) return true;
    return self.publishedGeneration() == snapshot.publish_generation;
}

fn notifyCompleteSnapshotCapturedForTestIfSupported(self: anytype) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "notifyCompleteSnapshotCapturedForTest")) {
        self.notifyCompleteSnapshotCapturedForTest();
    }
}

fn noteIncompletePublishedSnapshotIfSupported(self: anytype, generation: u64) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "noteIncompletePublishedSnapshotForGeneration")) {
        self.noteIncompletePublishedSnapshotForGeneration(generation);
    } else if (comptime @hasDecl(Index, "noteIncompletePublishedSnapshot")) {
        self.noteIncompletePublishedSnapshot();
    }
}

const CompleteCoverageValidationClaim = struct {
    enabled: bool,
    held: bool,
};

fn beginCompleteCoverageValidationIfSupported(
    self: anytype,
    exhaustive: bool,
    generation: u64,
    cancellation: ?search_types.CancellationToken,
) !CompleteCoverageValidationClaim {
    if (!exhaustive) return .{ .enabled = false, .held = false };
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "beginCompleteCoverageValidation")) {
        const claimed = try self.beginCompleteCoverageValidation(generation, cancellation);
        return .{ .enabled = claimed, .held = claimed };
    }
    if (comptime @hasDecl(Index, "completeCoverageAlreadyValidated")) {
        return .{
            .enabled = !self.completeCoverageAlreadyValidated(generation),
            .held = false,
        };
    }
    return .{ .enabled = true, .held = false };
}

fn finishCompleteCoverageValidationIfSupported(self: anytype, generation: u64, validated: bool) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "finishCompleteCoverageValidation")) {
        self.finishCompleteCoverageValidation(generation, validated);
    } else if (validated and comptime @hasDecl(Index, "noteCompleteCoverageValidated")) {
        self.noteCompleteCoverageValidated(generation);
    }
}

const CompleteCoverageTracker = struct {
    const assignment_batch_size: usize = 8_192;

    enabled: bool,
    claim_held: bool,
    expected_count: u64,
    observed_count: u64 = 0,
    assignment_count: usize = 0,

    fn init(claim: CompleteCoverageValidationClaim, expected_count: u64) CompleteCoverageTracker {
        return .{
            .enabled = claim.enabled,
            .claim_held = claim.held,
            .expected_count = expected_count,
        };
    }

    fn prepare(self: *CompleteCoverageTracker, index: anytype, scratch: anytype) !void {
        if (!self.enabled) return;
        try scratch.ensureCoverageMemberCapacity(index.alloc, assignment_batch_size);
        try scratch.ensureLookupCapacity(index.alloc, assignment_batch_size);
    }

    fn observe(
        self: *CompleteCoverageTracker,
        index: anytype,
        txn: anytype,
        scratch: anytype,
        leaf_id: u64,
        member_ids: []const u64,
    ) !void {
        if (!self.enabled) return;
        self.observed_count = std.math.add(u64, self.observed_count, @intCast(member_ids.len)) catch
            return error.IncompletePublishedSnapshot;
        if (self.observed_count > self.expected_count) return error.IncompletePublishedSnapshot;
        if (member_ids.len == 0) return;

        // Detect same-posting duplicates exactly without coupling the global
        // validation batch size to a leaf's fanout.
        try scratch.ensureVectorIdCapacity(index.alloc, member_ids.len);
        const sorted_ids = scratch.vector_ids[0..member_ids.len];
        @memcpy(sorted_ids, member_ids);
        std.mem.sort(u64, sorted_ids, {}, std.sort.asc(u64));
        for (sorted_ids[1..], sorted_ids[0 .. sorted_ids.len - 1]) |current, previous| {
            if (current == previous) return error.IncompletePublishedSnapshot;
        }

        for (member_ids) |member_id| {
            if (self.assignment_count == assignment_batch_size) {
                try self.flushAssignments(txn, scratch);
            }
            scratch.coverage_members[self.assignment_count] = .{
                .vector_id = member_id,
                .leaf_id = leaf_id,
            };
            self.assignment_count += 1;
        }
    }

    fn flushAssignments(self: *CompleteCoverageTracker, txn: anytype, scratch: anytype) !void {
        if (self.assignment_count == 0) return;
        const assignments = scratch.coverage_members[0..self.assignment_count];
        std.mem.sort(search_runtime.CoverageMember, assignments, {}, struct {
            fn lessThan(_: void, lhs: search_runtime.CoverageMember, rhs: search_runtime.CoverageMember) bool {
                return lhs.vector_id < rhs.vector_id;
            }
        }.lessThan);
        for (assignments[1..], assignments[0 .. assignments.len - 1]) |current, previous| {
            if (current.vector_id == previous.vector_id) return error.IncompletePublishedSnapshot;
        }

        const lookups = scratch.lookups[0..assignments.len];
        const key_views = scratch.key_views[0..assignments.len];
        const values = scratch.values[0..assignments.len];
        for (assignments, 0..) |assignment, i| {
            lookups[i].vector_id = assignment.vector_id;
            _ = hbc.encodeVecLeafKey(&lookups[i].key, assignment.vector_id);
            key_views[i] = lookups[i].key[0..];
        }
        try txn.getManySorted(.vecs, key_views, values);
        for (assignments, values) |assignment, maybe_value| {
            const value = maybe_value orelse return error.IncompletePublishedSnapshot;
            if (value.len < @sizeOf(u64) or std.mem.readInt(u64, value[0..8], .little) != assignment.leaf_id) {
                return error.IncompletePublishedSnapshot;
            }
        }
        self.assignment_count = 0;
    }

    fn validate(self: *CompleteCoverageTracker, txn: anytype, scratch: anytype) !void {
        if (!self.enabled) return;
        if (self.observed_count != self.expected_count) return error.IncompletePublishedSnapshot;
        return self.flushAssignments(txn, scratch);
    }
};

fn validateCompleteCoverage(
    self: anytype,
    txn: anytype,
    scratch: anytype,
    tracker: *CompleteCoverageTracker,
    generation: u64,
) !void {
    try tracker.validate(txn, scratch);
    if (!tracker.enabled) return;
    finishCompleteCoverageValidationIfSupported(self, generation, true);
    tracker.claim_held = false;
    tracker.enabled = false;
}

fn searchRootNode(self: anytype) u64 {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "publishedRootNode")) {
        return self.publishedRootNode();
    }
    return self.metadata.root_node;
}

pub fn addChildCandidates(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    query: []const f32,
    query_measure: f32,
    candidates: *std.PriorityQueue(types.PriorityItem, void, search_types.candidateLessThan),
    scratch: anytype,
    profile: *search_types.SearchProfile,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    try scratch.ensureMemberIdCapacity(self.alloc, node.children.len);
    const child_ids = scratch.member_ids[0..node.children.len];
    @memcpy(child_ids, node.children);
    return try addChildCandidatesFromIds(self, txn, node.id, usesNonQuantizedPayload(node), child_ids, query, query_measure, candidates, scratch, profile, .best_effort, true, now_fn_u64, elapsed_fn_u64);
}

fn addChildCandidatesFromIds(
    self: anytype,
    txn: anytype,
    node_id: u64,
    uses_nonquantized_payload: bool,
    child_ids: []const u64,
    query: []const f32,
    query_measure: f32,
    candidates: *std.PriorityQueue(types.PriorityItem, void, search_types.candidateLessThan),
    scratch: anytype,
    profile: *search_types.SearchProfile,
    coverage_policy: search_types.CoveragePolicy,
    comptime use_search_cache: bool,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const start = now_fn_u64();
    defer profile.child_expand_ns += elapsed_fn_u64(start);
    const child_count = child_ids.len;
    if (self.config.use_quantization) {
        if (try loadQuantizedReadHandleProfiledWithCachePolicy(self, txn, node_id, uses_nonquantized_payload, child_count, profile, use_search_cache, now_fn_u64, elapsed_fn_u64, isNotFoundGeneric)) |quantized_handle| {
            defer {
                var handle = quantized_handle;
                handle.deinit(self.alloc);
            }
            const quantized = quantized_handle.ptr();
            profile.approx_nodes_expanded += 1;

            const count = child_count;
            const distances = scratch.distances[0..count];
            const error_bounds = scratch.error_bounds[0..count];

            try self.estimateQuantizedDistances(quantized, query, query_measure, distances, error_bounds, &scratch.estimate);
            for (child_ids, 0..) |child_id, i| {
                _ = error_bounds[i];
                try candidates.push(self.alloc, .{
                    .id = child_id,
                    .distance = distances[i],
                    .error_bound = error_bounds[i],
                    .bound_resolved = false,
                });
            }
            return;
        }
    }

    for (child_ids) |child_id| {
        if (use_search_cache) {
            if (try borrowSearchCachedNodeHandle(self, child_id)) |cached_handle| {
                defer {
                    var handle = cached_handle;
                    handle.deinit(self.alloc);
                }
                const dist = vec.distanceToQuery(query, query_measure, cached_handle.ptr().centroid, self.config.metric);
                try candidates.push(self.alloc, .{ .id = child_id, .distance = dist, .error_bound = 0, .bound_resolved = false });
                continue;
            }
        }

        var child_handle = loadNodeReadHandleWithCachePolicy(self, txn, child_id, use_search_cache) catch |err| {
            try handleTraversalNodeLoadError(err, coverage_policy);
            continue;
        };
        defer child_handle.deinit(self.alloc);
        const dist = vec.distanceToQuery(query, query_measure, child_handle.ptr().centroid, self.config.metric);
        try candidates.push(self.alloc, .{ .id = child_id, .distance = dist, .error_bound = 0, .bound_resolved = false });
    }
}

pub fn scoreLeafMembers(
    self: anytype,
    txn: anytype,
    leaf: *const types.Node,
    approx_query: []const f32,
    approx_query_measure: f32,
    exact_query: []const f32,
    exact_query_measure: f32,
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
    results: *search_results.ApproxSearchResults,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const leaf_posting = try posting.PostingStore.view(leaf);
    const member_ids = try posting.PostingStore.copyMemberIds(self.alloc, scratch, leaf_posting);
    return try @This().scoreLeafMemberIds(self, txn, leaf_posting.id, leaf_posting.usesNonQuantizedPayload(), leaf_posting.hasFreshStoredPayload(), member_ids, approx_query, approx_query_measure, exact_query, exact_query_measure, req, filter_state, results, scratch, profile, true, now_fn_u64, elapsed_fn_u64);
}

fn scoreLeafMemberIds(
    self: anytype,
    txn: anytype,
    leaf_id: u64,
    leaf_uses_nonquantized_payload: bool,
    leaf_has_fresh_stored_payload: bool,
    member_ids: []const u64,
    approx_query: []const f32,
    approx_query_measure: f32,
    exact_query: []const f32,
    exact_query_measure: f32,
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
    results: *search_results.ApproxSearchResults,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    comptime use_search_cache: bool,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const start = now_fn_u64();
    defer profile.leaf_score_ns += elapsed_fn_u64(start);
    const coverage_policy = search_types.coveragePolicy(req);
    try scratch.ensureVectorFetchCapacity(self.alloc, member_ids.len);

    // Resolve selective ID and metadata-prefix predicates once per leaf. The
    // sorted metadata batch reuses LSM blocks and replaces the former scalar
    // point lookup (plus cache lock) for every quantized candidate.
    const filters_active = !filter_state.isTrivial() or req.filter_prefix.len > 0;
    var filtered_count: usize = 0;
    for (member_ids, 0..) |member_id, original_index| {
        if (filters_active) profile.filter_candidates += 1;
        if (filter_state.rejects(member_id)) {
            profile.filter_rejected += 1;
            continue;
        }
        scratch.member_ids[filtered_count] = member_id;
        scratch.positions[filtered_count] = original_index;
        filtered_count += 1;
    }
    if (req.filter_prefix.len > 0 and filtered_count > 0) {
        const filter_start = now_fn_u64();
        profile.filter_metadata_batches += 1;
        const candidates = scratch.member_ids[0..filtered_count];
        try getMetadataManySortedInTxnWithScratchProfiled(
            self,
            txn,
            candidates,
            scratch.metadata[0..filtered_count],
            scratch.lookups[0..filtered_count],
            scratch.key_views[0..filtered_count],
            scratch.values[0..filtered_count],
            use_search_cache,
            profile,
            now_fn_u64,
            elapsed_fn_u64,
        );
        profile.filter_metadata_batch_ns += elapsed_fn_u64(filter_start);
        var prefix_count: usize = 0;
        for (candidates, scratch.metadata[0..filtered_count], scratch.positions[0..filtered_count]) |member_id, maybe_metadata, original_index| {
            const metadata = maybe_metadata orelse {
                // Metadata is optional. Absence is a valid non-match for a
                // prefix predicate, not evidence that vector coverage is
                // incomplete.
                profile.filter_rejected += 1;
                continue;
            };
            if (!std.mem.startsWith(u8, metadata, req.filter_prefix)) {
                profile.filter_rejected += 1;
                continue;
            }
            scratch.member_ids[prefix_count] = member_id;
            scratch.positions[prefix_count] = original_index;
            prefix_count += 1;
        }
        filtered_count = prefix_count;
    }
    if (filtered_count == 0) return;
    profile.traversal_eligible_vectors += @intCast(filtered_count);
    const scoring_member_ids = scratch.member_ids[0..filtered_count];
    const original_positions = scratch.positions[0..filtered_count];
    const has_extra_filters = req.distance_over != null or req.distance_under != null;
    var scoring_req = req;
    scoring_req.filter_prefix = "";
    scoring_req.filter_ids = &.{};
    scoring_req.exclude_ids = &.{};
    const empty_filter_state = search_types.RequestFilterState{};
    if (self.config.use_quantization and leaf_has_fresh_stored_payload) {
        if (try loadQuantizedReadHandleProfiledWithCachePolicy(self, txn, leaf_id, leaf_uses_nonquantized_payload, member_ids.len, profile, use_search_cache, now_fn_u64, elapsed_fn_u64, isNotFoundGeneric)) |quantized_handle| {
            defer {
                var handle = quantized_handle;
                handle.deinit(self.alloc);
            }
            const quantized = quantized_handle.ptr();
            profile.approx_leaves_scored += 1;
            const count = member_ids.len;
            const distances = scratch.distances[0..count];
            const error_bounds = scratch.error_bounds[0..count];
            try self.estimateQuantizedDistances(quantized, approx_query, approx_query_measure, distances, error_bounds, &scratch.estimate);
            if (!has_extra_filters) {
                for (scoring_member_ids, original_positions) |member_id, i| {
                    if (i % 256 == 0) try search_types.checkCancelled(req);
                    results.addApproxResult(member_id, distances[i], error_bounds[i]);
                }
            } else {
                for (scoring_member_ids, original_positions) |member_id, i| {
                    if (i % 256 == 0) try search_types.checkCancelled(req);
                    if (!try memberMatchesRequestWithCachePolicy(self, txn, member_id, distances[i], error_bounds[i], scoring_req, &empty_filter_state, true, use_search_cache)) continue;
                    results.addApproxResult(member_id, distances[i], error_bounds[i]);
                }
            }
            profile.approx_vectors_scored += count;
            return;
        }
    }

    if (self.config.use_quantization) {
        if (!leaf_has_fresh_stored_payload) {
            profile.leaf_payload_stale += 1;
        } else {
            profile.leaf_payload_missing += 1;
        }
    }

    const fetch_member_ids = scratch.vector_ids[0..scoring_member_ids.len];
    var fetch_count: usize = 0;
    for (scoring_member_ids, 0..) |member_id, i| {
        if (i % 256 == 0) try search_types.checkCancelled(req);
        if (use_search_cache) {
            if (borrowCachedVectorHandle(self, member_id)) |cached_handle| {
                profile.vector_cache_hits += 1;
                var handle = cached_handle;
                defer handle.deinit();
                const dist = vec.distanceToQuery(exact_query, exact_query_measure, handle.view(), self.config.metric);
                if (has_extra_filters and !try memberMatchesRequestWithCachePolicy(self, txn, member_id, dist, 0, scoring_req, &empty_filter_state, false, use_search_cache)) {
                    continue;
                }
                results.addResult(member_id, dist, 0);
                profile.exact_vectors_scored += 1;
                continue;
            }
        }
        if (!indexHasExternalVectorLoader(self)) profile.vector_cache_misses += 1;
        fetch_member_ids[fetch_count] = member_id;
        fetch_count += 1;
    }

    if (fetch_count == 0) return;

    const exact_distances = scratch.distances[0..fetch_count];
    var external_scored = false;
    const Index = comptime childType(@TypeOf(self));
    if (indexHasExternalVectorLoader(self) and comptime @hasDecl(Index, "scoreExternalVectorsSortedWithScratch")) {
        external_scored = if (!use_search_cache and comptime @hasDecl(Index, "scoreExternalVectorsSortedWithScratchUncached"))
            try self.scoreExternalVectorsSortedWithScratchUncached(
                txn,
                fetch_member_ids[0..fetch_count],
                exact_query,
                exact_query_measure,
                exact_distances,
                scratch.metadata,
                scratch.lookups,
                scratch.key_views,
                scratch.values,
                scratch.vector_batch,
            )
        else
            try self.scoreExternalVectorsSortedWithScratch(
                txn,
                fetch_member_ids[0..fetch_count],
                exact_query,
                exact_query_measure,
                exact_distances,
                scratch.metadata,
                scratch.lookups,
                scratch.key_views,
                scratch.values,
                scratch.vector_batch,
            );
    }

    if (external_scored) {
        for (fetch_member_ids[0..fetch_count], 0..) |member_id, i| {
            const dist = exact_distances[i];
            if (!std.math.isFinite(dist)) {
                if (coverage_policy == .complete_snapshot) return error.IncompletePublishedSnapshot;
                continue;
            }
            if (has_extra_filters and !try memberMatchesRequestWithCachePolicy(self, txn, member_id, dist, 0, scoring_req, &empty_filter_state, false, use_search_cache)) {
                continue;
            }
            results.addResult(member_id, dist, 0);
            profile.exact_vectors_scored += 1;
        }
        return;
    }

    const vector_views = scratch.vector_views[0..fetch_count];
    try loadVectorIdsSortedWithScratchWithCachePolicy(
        self,
        txn,
        fetch_member_ids[0..fetch_count],
        vector_views,
        scratch.lookups,
        scratch.key_views,
        scratch.values,
        scratch.vector,
        scratch.vector_batch,
        use_search_cache,
    );
    const scored_positions = scratch.positions[0..fetch_count];
    var scored_count: usize = 0;
    for (vector_views, 0..) |member_vec, i| {
        if (member_vec.len == 0) continue;
        vector_views[scored_count] = member_vec;
        scored_positions[scored_count] = i;
        scored_count += 1;
    }
    if (coverage_policy == .complete_snapshot and scored_count != fetch_count) {
        return error.IncompletePublishedSnapshot;
    }
    if (scored_count == 0) return;
    try search_types.checkCancelled(req);
    try search_runtime.exactDistancesToStoredVectorsCancellable(
        self.config,
        exact_query,
        exact_query_measure,
        vector_views[0..scored_count],
        exact_distances[0..scored_count],
        req.cancellation,
    );
    try search_types.checkCancelled(req);
    for (scored_positions[0..scored_count], 0..) |member_index, dist_index| {
        if (dist_index % 256 == 0) try search_types.checkCancelled(req);
        const member_id = fetch_member_ids[member_index];
        const dist = exact_distances[dist_index];
        if (has_extra_filters and !try memberMatchesRequestWithCachePolicy(self, txn, member_id, dist, 0, scoring_req, &empty_filter_state, false, use_search_cache)) {
            continue;
        }
        results.addResult(member_id, dist, 0);
        profile.exact_vectors_scored += 1;
    }
}

pub fn rerankResults(
    self: anytype,
    txn: anytype,
    approx_results: *search_results.ApproxSearchResults,
    query: []const f32,
    query_measure: f32,
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !search_results.SearchResults {
    return rerankResultsWithCachePolicy(self, txn, approx_results, query, query_measure, req, filter_state, scratch, profile, true, now_fn_u64, elapsed_fn_u64);
}

fn rerankResultsWithCachePolicy(
    self: anytype,
    txn: anytype,
    approx_results: *search_results.ApproxSearchResults,
    query: []const f32,
    query_measure: f32,
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    comptime use_search_cache: bool,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !search_results.SearchResults {
    const start = now_fn_u64();
    defer profile.rerank_ns += elapsed_fn_u64(start);
    const coverage_policy = search_types.coveragePolicy(req);
    const ranked_items = approx_results.items.items;
    const has_extra_filters = search_runtime.requestHasExtraFilters(req, filter_state);
    try scratch.ensureRerankCapacity(self.alloc, ranked_items.len);

    const prepare_start = now_fn_u64();
    search_mod.sortApproxResultsByDistance(ranked_items);

    const rerank_selection = selectRerankCandidatesInto(scratch.flags[0..ranked_items.len], ranked_items, rerankBoundaryK(req), req, self.config.rerank_policy);

    profile.approx_candidate_count = ranked_items.len;
    profile.top_k_count = rerank_selection.top_k_count;
    profile.rerank_candidate_count = rerank_selection.rerank_candidate_count;
    profile.ambiguous_top_k_pairs = rerank_selection.ambiguous_top_k_pairs;
    profile.ambiguous_boundary_pairs = rerank_selection.ambiguous_boundary_pairs;
    profile.ambiguous_distance_over_hits = rerank_selection.ambiguous_distance_over_hits;
    profile.ambiguous_distance_under_hits = rerank_selection.ambiguous_distance_under_hits;
    profile.full_rerank_due_to_threshold = rerank_selection.full_rerank_due_to_threshold;
    profile.min_distance_gap_top_k = rerank_selection.min_distance_gap_top_k;
    profile.min_interval_gap_top_k = rerank_selection.min_interval_gap_top_k;
    profile.closest_pair_top_k = rerank_selection.closest_pair_top_k;
    profile.boundary_pair = rerank_selection.boundary_pair;
    profile.boundary_tail_error_avg = rerank_selection.boundary_tail_error_avg;
    profile.boundary_tail_error_max = rerank_selection.boundary_tail_error_max;
    profile.boundary_tail_distance_gap_avg = rerank_selection.boundary_tail_distance_gap_avg;
    profile.boundary_tail_distance_gap_min = rerank_selection.boundary_tail_distance_gap_min;
    profile.boundary_tail_distance_gap_max = rerank_selection.boundary_tail_distance_gap_max;
    profile.boundary_tail_interval_gap_avg = rerank_selection.boundary_tail_interval_gap_avg;
    profile.boundary_tail_interval_gap_min = rerank_selection.boundary_tail_interval_gap_min;
    profile.boundary_tail_interval_gap_max = rerank_selection.boundary_tail_interval_gap_max;
    profile.approx_top_count = rerank_selection.approx_top_count;
    profile.approx_top = rerank_selection.approx_top;
    profile.rerank_prepare_ns += elapsed_fn_u64(prepare_start);

    const rerank_count = rerank_selection.rerank_candidate_count;
    if (rerank_count > 0) {
        const select_start = now_fn_u64();
        const rerank_positions = selectedRerankCandidatePositionsInto(ranked_items, rerank_selection.flags, scratch.positions[0..rerank_count]);
        profile.rerank_select_positions_ns += elapsed_fn_u64(select_start);
        const vector_views = scratch.vector_views[0..rerank_count];
        const exact_distances = scratch.distances[0..rerank_count];

        var external_scored = false;
        const Index = comptime childType(@TypeOf(self));
        if (indexHasExternalVectorLoader(self) and comptime @hasDecl(Index, "scoreExternalRerankVectorsSortedWithScratch")) {
            const max_external_rerank_batch: usize = 128;
            var offset: usize = 0;
            while (offset < rerank_positions.len) {
                const batch_end = @min(offset + max_external_rerank_batch, rerank_positions.len);
                const batch_positions = rerank_positions[offset..batch_end];
                const batch_distances = exact_distances[0..batch_positions.len];
                const score_start = now_fn_u64();
                const handled = if (!use_search_cache and comptime @hasDecl(Index, "scoreExternalRerankVectorsSortedWithScratchUncached"))
                    try self.scoreExternalRerankVectorsSortedWithScratchUncached(
                        txn,
                        ranked_items,
                        batch_positions,
                        query,
                        query_measure,
                        batch_distances,
                        scratch.vector_ids,
                        scratch.metadata,
                        scratch.lookups,
                        scratch.key_views,
                        scratch.values,
                        scratch.vector_batch,
                        scratch.error_bounds,
                        profile,
                    )
                else
                    try self.scoreExternalRerankVectorsSortedWithScratch(
                        txn,
                        ranked_items,
                        batch_positions,
                        query,
                        query_measure,
                        batch_distances,
                        scratch.vector_ids,
                        scratch.metadata,
                        scratch.lookups,
                        scratch.key_views,
                        scratch.values,
                        scratch.vector_batch,
                        scratch.error_bounds,
                        profile,
                    );
                const score_elapsed = elapsed_fn_u64(score_start);
                profile.rerank_prefetch_ns += score_elapsed;
                profile.rerank_vector_load_ns += score_elapsed;
                if (!handled) {
                    if (offset != 0) return error.ExternalRerankCapabilityChanged;
                    break;
                }
                external_scored = true;
                profile.rerank_batches += 1;
                profile.rerank_max_batch_size = @max(profile.rerank_max_batch_size, batch_positions.len);

                const apply_start = now_fn_u64();
                for (batch_positions, 0..) |index, slot| {
                    if (slot % 64 == 0) try search_types.checkCancelled(req);
                    const item = &ranked_items[index];
                    const dist = batch_distances[slot];
                    rerank_selection.flags[index] = false;
                    if (!std.math.isFinite(dist)) {
                        if (coverage_policy == .complete_snapshot) return error.IncompletePublishedSnapshot;
                        item.distance = std.math.inf(f32);
                        item.error_bound = 0;
                        continue;
                    }
                    if (has_extra_filters and !try memberMatchesRequestWithCachePolicy(self, txn, item.vector_id, dist, 0, req, filter_state, false, use_search_cache)) {
                        item.distance = std.math.inf(f32);
                        item.error_bound = 0;
                        continue;
                    }
                    item.distance = dist;
                    item.error_bound = 0;
                    profile.exact_vectors_scored += 1;
                    profile.reranked_vectors += 1;
                }
                profile.rerank_apply_ns += elapsed_fn_u64(apply_start);
                offset = batch_end;

                if (offset < rerank_positions.len and remainingRerankCandidatesCannotEnter(
                    ranked_items,
                    rerank_selection.flags,
                    rerank_selection.top_k_count,
                    scratch.distances,
                )) {
                    profile.rerank_candidates_skipped_by_bound += rerank_positions.len - offset;
                    break;
                }
            }
        }

        if (!external_scored) {
            const preload_start = now_fn_u64();
            try loadRerankVectorsSortedWithScratch(
                self,
                txn,
                ranked_items,
                rerank_positions,
                vector_views,
                scratch.vector_ids,
                scratch.lookups,
                scratch.key_views,
                scratch.values,
                scratch.vector,
                scratch.vector_batch,
                use_search_cache,
            );
            const preload_elapsed = elapsed_fn_u64(preload_start);
            profile.rerank_prefetch_ns += preload_elapsed;
            profile.rerank_vector_load_ns += preload_elapsed;

            var loaded_count: usize = 0;
            for (rerank_positions, 0..) |index, slot| {
                const item = &ranked_items[index];
                const member_vec = vector_views[slot];
                if (member_vec.len == 0) {
                    if (coverage_policy == .complete_snapshot) return error.IncompletePublishedSnapshot;
                    item.distance = std.math.inf(f32);
                    item.error_bound = 0;
                    continue;
                }
                vector_views[loaded_count] = member_vec;
                loaded_count += 1;
            }
            const dist_start = now_fn_u64();
            try search_types.checkCancelled(req);
            try search_runtime.exactDistancesToStoredVectorsCancellable(self.config, query, query_measure, vector_views[0..loaded_count], exact_distances[0..loaded_count], req.cancellation);
            try search_types.checkCancelled(req);
            profile.rerank_distance_ns += elapsed_fn_u64(dist_start);
        }

        if (!external_scored) {
            profile.rerank_batches += 1;
            profile.rerank_max_batch_size = @max(profile.rerank_max_batch_size, rerank_positions.len);
            const apply_start = now_fn_u64();
            var exact_idx: usize = 0;
            for (rerank_positions, 0..) |index, slot| {
                if (slot % 256 == 0) try search_types.checkCancelled(req);
                const item = &ranked_items[index];
                if (!std.math.isFinite(item.distance)) continue;
                const dist = exact_distances[exact_idx];
                exact_idx += 1;
                if (!std.math.isFinite(dist)) {
                    item.distance = std.math.inf(f32);
                    item.error_bound = 0;
                    continue;
                }
                if (has_extra_filters and !try memberMatchesRequestWithCachePolicy(self, txn, item.vector_id, dist, 0, req, filter_state, false, use_search_cache)) {
                    item.distance = std.math.inf(f32);
                    item.error_bound = 0;
                    continue;
                }
                item.distance = dist;
                item.error_bound = 0;
                profile.exact_vectors_scored += 1;
                profile.reranked_vectors += 1;
            }
            profile.rerank_apply_ns += elapsed_fn_u64(apply_start);
        }

        const resort_start = now_fn_u64();
        search_mod.sortApproxResultsByDistance(ranked_items);
        profile.rerank_resort_ns += elapsed_fn_u64(resort_start);
    }

    const finalize_start = now_fn_u64();
    var exact_results = try search_results.SearchResults.fromSortedApproxSlice(self.alloc, req.k, ranked_items);
    profile.rerank_finalize_ns += elapsed_fn_u64(finalize_start);

    if (req.load_metadata) {
        const metadata_start = now_fn_u64();
        try populateMetadataWithScratch(self, txn, &exact_results, scratch, use_search_cache);
        profile.rerank_metadata_ns += elapsed_fn_u64(metadata_start);
    }
    return exact_results;
}

fn selectRerankCandidates(
    alloc: std.mem.Allocator,
    ranked_items: []const search_results.ApproxSearchResult,
    k: usize,
    req: search_types.SearchRequest,
    policy: types.HBCConfig.RerankPolicy,
) !RerankSelection {
    const flags = try alloc.alloc(bool, ranked_items.len);
    return selectRerankCandidatesInto(flags, ranked_items, k, req, policy);
}

fn selectRerankCandidatesInto(
    flags_storage: []bool,
    ranked_items: []const search_results.ApproxSearchResult,
    k: usize,
    req: search_types.SearchRequest,
    policy: types.HBCConfig.RerankPolicy,
) RerankSelection {
    const flags = flags_storage[0..ranked_items.len];
    @memset(flags, false);
    var selection = RerankSelection{
        .flags = flags,
        .approx_candidate_count = ranked_items.len,
        .top_k_count = @min(k, ranked_items.len),
    };
    switch (policy) {
        .never => {},
        .always => {
            @memset(flags, true);
            selection.rerank_candidate_count = ranked_items.len;
        },
        .boundary => markBoundaryRerankCandidates(&selection, ranked_items, req),
    }
    return selection;
}

fn rerankBoundaryK(req: search_types.SearchRequest) usize {
    const explicit = req.rerank_k orelse return req.k;
    if (explicit == 0) return 0;
    return @min(explicit, req.k);
}

const RerankSelection = struct {
    flags: []bool,
    approx_candidate_count: usize,
    top_k_count: usize,
    rerank_candidate_count: usize = 0,
    ambiguous_top_k_pairs: usize = 0,
    ambiguous_boundary_pairs: usize = 0,
    ambiguous_distance_over_hits: usize = 0,
    ambiguous_distance_under_hits: usize = 0,
    full_rerank_due_to_threshold: bool = false,
    min_distance_gap_top_k: f32 = std.math.floatMax(f32),
    min_interval_gap_top_k: f32 = std.math.floatMax(f32),
    closest_pair_top_k: ?search_types.DebugPair = null,
    boundary_pair: ?search_types.DebugPair = null,
    boundary_tail_error_sum: f64 = 0,
    boundary_tail_error_avg: f32 = 0,
    boundary_tail_error_max: f32 = 0,
    boundary_tail_distance_gap_sum: f64 = 0,
    boundary_tail_distance_gap_avg: f32 = 0,
    boundary_tail_distance_gap_min: f32 = std.math.floatMax(f32),
    boundary_tail_distance_gap_max: f32 = -std.math.floatMax(f32),
    boundary_tail_interval_gap_sum: f64 = 0,
    boundary_tail_interval_gap_avg: f32 = 0,
    boundary_tail_interval_gap_min: f32 = std.math.floatMax(f32),
    boundary_tail_interval_gap_max: f32 = -std.math.floatMax(f32),
    approx_top_count: usize = 0,
    approx_top: [5]search_types.DebugHit = .{ .{}, .{}, .{}, .{}, .{} },
};

fn markBoundaryRerankCandidates(
    selection: *RerankSelection,
    ranked_items: []const search_results.ApproxSearchResult,
    req: search_types.SearchRequest,
) void {
    const flags = selection.flags;
    const limit = selection.top_k_count;
    if (limit == 0) return;

    selection.approx_top_count = @min(limit, selection.approx_top.len);
    for (0..selection.approx_top_count) |i| {
        selection.approx_top[i] = debugHitFromApprox(ranked_items[i]);
    }

    for (ranked_items[0..limit]) |item| {
        if (req.distance_over) |threshold| {
            if (approxResultMaybeOver(item, threshold) and !approxResultDefinitelyOver(item, threshold)) {
                selection.ambiguous_distance_over_hits += 1;
                @memset(flags, true);
                selection.rerank_candidate_count = ranked_items.len;
                selection.full_rerank_due_to_threshold = true;
                return;
            }
        }
        if (req.distance_under) |threshold| {
            if (approxResultMaybeUnder(item, threshold) and !approxResultDefinitelyUnder(item, threshold)) {
                selection.ambiguous_distance_under_hits += 1;
                @memset(flags, true);
                selection.rerank_candidate_count = ranked_items.len;
                selection.full_rerank_due_to_threshold = true;
                return;
            }
        }
    }

    for (0..limit) |i| {
        for (i + 1..limit) |j| {
            const pair = debugPairFromApprox(ranked_items[i], ranked_items[j]);
            if (pair.distance_gap < selection.min_distance_gap_top_k) {
                selection.min_distance_gap_top_k = pair.distance_gap;
            }
            if (pair.interval_gap < selection.min_interval_gap_top_k) {
                selection.min_interval_gap_top_k = pair.interval_gap;
                selection.closest_pair_top_k = pair;
            }
            if (pair.overlaps) {
                selection.ambiguous_top_k_pairs += 1;
            }
        }
    }
    if (selection.min_distance_gap_top_k == std.math.floatMax(f32)) selection.min_distance_gap_top_k = 0;
    if (selection.min_interval_gap_top_k == std.math.floatMax(f32)) selection.min_interval_gap_top_k = 0;

    const boundary_index = limit - 1;
    if (selection.boundary_pair == null and limit < ranked_items.len) {
        selection.boundary_pair = debugPairFromApprox(ranked_items[boundary_index], ranked_items[limit]);
    }
    if (limit < ranked_items.len) {
        const boundary = ranked_items[boundary_index];
        var boundary_has_overlap = false;
        for (0..ranked_items.len) |j| {
            if (j == boundary_index) continue;
            if (!approxIntervalsOverlap(boundary, ranked_items[j])) continue;
            flags[j] = true;
            boundary_has_overlap = true;
            selection.ambiguous_boundary_pairs += 1;
            const pair = debugPairFromApprox(boundary, ranked_items[j]);
            selection.boundary_tail_error_sum += ranked_items[j].error_bound;
            selection.boundary_tail_error_max = @max(selection.boundary_tail_error_max, ranked_items[j].error_bound);
            selection.boundary_tail_distance_gap_sum += pair.distance_gap;
            selection.boundary_tail_distance_gap_min = @min(selection.boundary_tail_distance_gap_min, pair.distance_gap);
            selection.boundary_tail_distance_gap_max = @max(selection.boundary_tail_distance_gap_max, pair.distance_gap);
            selection.boundary_tail_interval_gap_sum += pair.interval_gap;
            selection.boundary_tail_interval_gap_min = @min(selection.boundary_tail_interval_gap_min, pair.interval_gap);
            selection.boundary_tail_interval_gap_max = @max(selection.boundary_tail_interval_gap_max, pair.interval_gap);
            if (selection.boundary_pair == null or pair.interval_gap < selection.boundary_pair.?.interval_gap) {
                selection.boundary_pair = pair;
            }
        }
        if (boundary_has_overlap) flags[boundary_index] = true;
    }
    if (selection.ambiguous_boundary_pairs > 0) {
        const count = @as(f64, @floatFromInt(selection.ambiguous_boundary_pairs));
        selection.boundary_tail_error_avg = @floatCast(selection.boundary_tail_error_sum / count);
        selection.boundary_tail_distance_gap_avg = @floatCast(selection.boundary_tail_distance_gap_sum / count);
        selection.boundary_tail_interval_gap_avg = @floatCast(selection.boundary_tail_interval_gap_sum / count);
    } else {
        selection.boundary_tail_distance_gap_min = 0;
        selection.boundary_tail_distance_gap_max = 0;
        selection.boundary_tail_interval_gap_min = 0;
        selection.boundary_tail_interval_gap_max = 0;
    }
    selection.rerank_candidate_count = countSelectedRerankCandidates(flags);
}

fn approxResultMaybeOver(item: search_results.ApproxSearchResult, distance: f32) bool {
    return item.distance + item.error_bound >= distance;
}

fn approxResultDefinitelyOver(item: search_results.ApproxSearchResult, distance: f32) bool {
    return item.distance - item.error_bound > distance;
}

fn approxResultMaybeUnder(item: search_results.ApproxSearchResult, distance: f32) bool {
    return item.distance - item.error_bound <= distance;
}

fn approxResultDefinitelyUnder(item: search_results.ApproxSearchResult, distance: f32) bool {
    return item.distance + item.error_bound < distance;
}

fn countSelectedRerankCandidates(flags: []const bool) usize {
    var count: usize = 0;
    for (flags) |selected| {
        if (selected) count += 1;
    }
    return count;
}

/// Returns true only when `top_k` already-scored/permanently-retained
/// candidates have upper bounds strictly below every still-selected
/// candidate's lower bound. This lets external reranking stop between sorted
/// artifact batches without weakening membership recall.
fn remainingRerankCandidatesCannotEnter(
    ranked_items: []const search_results.ApproxSearchResult,
    pending_flags: []const bool,
    top_k: usize,
    upper_storage: []f32,
) bool {
    if (top_k == 0 or upper_storage.len < ranked_items.len) return false;
    var retained_count: usize = 0;
    var pending_lower = std.math.inf(f32);
    for (ranked_items, pending_flags) |item, pending| {
        if (pending) {
            pending_lower = @min(pending_lower, item.distance - item.error_bound);
            continue;
        }
        const upper = item.distance + item.error_bound;
        if (!std.math.isFinite(upper)) continue;
        upper_storage[retained_count] = upper;
        retained_count += 1;
    }
    if (retained_count < top_k or !std.math.isFinite(pending_lower)) return false;
    std.mem.sort(f32, upper_storage[0..retained_count], {}, struct {
        fn lessThan(_: void, lhs: f32, rhs: f32) bool {
            return lhs < rhs;
        }
    }.lessThan);
    return upper_storage[top_k - 1] < pending_lower;
}

test "progressive rerank stops only beyond retained kth upper bound" {
    const items = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 1.0, .error_bound = 0 },
        .{ .vector_id = 2, .distance = 2.0, .error_bound = 0.1 },
        .{ .vector_id = 3, .distance = 4.0, .error_bound = 0.2 },
        .{ .vector_id = 4, .distance = 6.0, .error_bound = 0.5 },
    };
    var storage: [items.len]f32 = undefined;
    try std.testing.expect(remainingRerankCandidatesCannotEnter(&items, &.{ false, false, true, true }, 2, &storage));
    try std.testing.expect(!remainingRerankCandidatesCannotEnter(&items, &.{ false, true, true, true }, 2, &storage));

    var overlapping = items;
    overlapping[2].distance = 2.0;
    overlapping[2].error_bound = 0.5;
    try std.testing.expect(!remainingRerankCandidatesCannotEnter(&overlapping, &.{ false, false, true, true }, 2, &storage));
}

fn selectedRerankCandidatePositions(
    alloc: std.mem.Allocator,
    ranked_items: []const search_results.ApproxSearchResult,
    flags: []const bool,
    count: usize,
) ![]usize {
    const positions = try alloc.alloc(usize, count);
    const used = selectedRerankCandidatePositionsInto(ranked_items, flags, positions);
    std.debug.assert(used.len == count);
    return positions;
}

fn selectedRerankCandidatePositionsInto(
    ranked_items: []const search_results.ApproxSearchResult,
    flags: []const bool,
    positions: []usize,
) []usize {
    var out: usize = 0;
    for (flags, 0..) |selected, index| {
        if (!selected) continue;
        positions[out] = index;
        out += 1;
    }
    const used = positions[0..out];
    std.mem.sort(usize, used, ranked_items, struct {
        fn lessThan(items: []const search_results.ApproxSearchResult, a: usize, b: usize) bool {
            return items[a].vector_id < items[b].vector_id;
        }
    }.lessThan);
    return used;
}

fn loadRerankVectorsSorted(
    self: anytype,
    txn: anytype,
    ranked_items: []const search_results.ApproxSearchResult,
    rerank_positions: []const usize,
    vector_views: [][]const f32,
    scratch: []f32,
) !void {
    const lookups = try self.alloc.alloc(FixedKeyLookup, rerank_positions.len);
    defer self.alloc.free(lookups);
    const key_views = try self.alloc.alloc([]const u8, rerank_positions.len);
    defer self.alloc.free(key_views);
    const values = try self.alloc.alloc(?[]const u8, rerank_positions.len);
    defer self.alloc.free(values);
    const vector_ids = try self.alloc.alloc(u64, rerank_positions.len);
    defer self.alloc.free(vector_ids);
    const batch_scratch = try self.alloc.alloc(f32, scratch.len * rerank_positions.len);
    defer self.alloc.free(batch_scratch);
    try loadRerankVectorsSortedWithScratch(self, txn, ranked_items, rerank_positions, vector_views, vector_ids, lookups, key_views, values, scratch, batch_scratch, true);
}

fn loadRerankVectorsSortedWithScratch(
    self: anytype,
    txn: anytype,
    ranked_items: []const search_results.ApproxSearchResult,
    rerank_positions: []const usize,
    vector_views: [][]const f32,
    vector_id_storage: []u64,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
    scratch: []f32,
    batch_scratch: []f32,
    comptime use_cache: bool,
) !void {
    if (vector_id_storage.len < rerank_positions.len) return error.InvalidArgument;
    const vector_ids = vector_id_storage[0..rerank_positions.len];
    for (rerank_positions, 0..) |index, slot| vector_ids[slot] = ranked_items[index].vector_id;
    try loadVectorIdsSortedWithScratchWithCachePolicy(self, txn, vector_ids, vector_views, lookup_storage, key_views_storage, values_storage, scratch, batch_scratch, use_cache);
}

fn loadVectorIdsSortedWithScratch(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    vector_views: [][]const f32,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
    scratch: []f32,
    batch_scratch: []f32,
) !void {
    return loadVectorIdsSortedWithScratchWithCachePolicy(
        self,
        txn,
        vector_ids,
        vector_views,
        lookup_storage,
        key_views_storage,
        values_storage,
        scratch,
        batch_scratch,
        true,
    );
}

fn loadVectorIdsSortedWithScratchWithCachePolicy(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    vector_views: [][]const f32,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
    scratch: []f32,
    batch_scratch: []f32,
    comptime use_cache: bool,
) !void {
    const Index = comptime childType(@TypeOf(self));
    std.debug.assert(vector_views.len >= vector_ids.len);
    std.debug.assert(lookup_storage.len >= vector_ids.len);
    std.debug.assert(key_views_storage.len >= vector_ids.len);
    std.debug.assert(values_storage.len >= vector_ids.len);
    for (vector_views[0..vector_ids.len]) |*view| view.* = &.{};
    if (indexHasExternalVectorLoader(self)) {
        if (comptime @hasDecl(Index, "getExternalVectorViewsSortedWithScratch")) {
            const loaded = if (!use_cache and comptime @hasDecl(Index, "getExternalVectorViewsSortedWithScratchUncached"))
                try self.getExternalVectorViewsSortedWithScratchUncached(
                    txn,
                    vector_ids,
                    vector_views,
                    lookup_storage,
                    key_views_storage,
                    values_storage,
                    scratch,
                    batch_scratch,
                )
            else
                try self.getExternalVectorViewsSortedWithScratch(
                    txn,
                    vector_ids,
                    vector_views,
                    lookup_storage,
                    key_views_storage,
                    values_storage,
                    scratch,
                    batch_scratch,
                );
            if (loaded) return;
        }
        const dims = scratch.len;
        std.debug.assert(batch_scratch.len >= dims * vector_ids.len);
        for (vector_ids, 0..) |vector_id, slot| {
            const slot_scratch = batch_scratch[slot * dims ..][0..dims];
            vector_views[slot] = if (!use_cache and comptime @hasDecl(Index, "getVectorIntoUncached"))
                self.getVectorIntoUncached(txn, vector_id, slot_scratch) catch &.{}
            else
                self.getVectorInto(txn, vector_id, slot_scratch) catch &.{};
        }
        return;
    }

    if (comptime !txnSupportsGetManySorted(@TypeOf(txn))) {
        const dims = scratch.len;
        std.debug.assert(batch_scratch.len >= dims * vector_ids.len);
        for (vector_ids, 0..) |vector_id, slot| {
            const slot_scratch = batch_scratch[slot * dims ..][0..dims];
            vector_views[slot] = if (!use_cache and comptime @hasDecl(Index, "getVectorIntoUncached"))
                self.getVectorIntoUncached(txn, vector_id, slot_scratch) catch &.{}
            else
                self.getVectorInto(txn, vector_id, slot_scratch) catch &.{};
        }
        return;
    }

    var lookup_count: usize = 0;
    for (vector_ids, 0..) |vector_id, slot| {
        if (use_cache) {
            if (borrowCachedVectorHandle(self, vector_id)) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                const cached = handle.view();
                const slot_scratch = batch_scratch[slot * scratch.len ..][0..scratch.len];
                if (cached.len > slot_scratch.len) return error.BufferTooSmall;
                @memcpy(slot_scratch[0..cached.len], cached);
                if (builtin.is_test and comptime @hasDecl(Index, "notifyVectorViewLoadForTest")) {
                    self.notifyVectorViewLoadForTest(vector_id);
                }
                vector_views[slot] = slot_scratch[0..cached.len];
                continue;
            }
        }
        var key: [10]u8 = undefined;
        _ = hbc.encodeVecKey(&key, vector_id);
        lookup_storage[lookup_count] = .{
            .item_index = slot,
            .vector_id = vector_id,
            .key = key,
            .vector_cache_fill_epoch = if (use_cache) beginVectorCacheFillIfSupported(self, vector_id).epoch else 0,
        };
        lookup_count += 1;
    }
    if (lookup_count == 0) return;

    const lookups = lookup_storage[0..lookup_count];
    const key_views = key_views_storage[0..lookup_count];
    const values = values_storage[0..lookup_count];
    std.mem.sort(FixedKeyLookup, lookups, {}, lessFixedKeyLookup);
    for (lookups, 0..) |*lookup, i| key_views[i] = lookup.key[0..];

    const vector_namespace = if (comptime @hasDecl(Index, "vectorArtifactReadNamespace"))
        self.vectorArtifactReadNamespace()
    else
        .vecs;
    try txn.getManySorted(vector_namespace, key_views, values);
    for (values, 0..) |maybe_value, i| {
        const value = maybe_value orelse continue;
        if (use_cache) {
            if (borrowCachedVectorHandle(self, lookups[i].vector_id)) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                const cached = handle.view();
                const slot_scratch = batch_scratch[lookups[i].item_index * scratch.len ..][0..scratch.len];
                if (cached.len > slot_scratch.len) return error.BufferTooSmall;
                @memcpy(slot_scratch[0..cached.len], cached);
                if (builtin.is_test and comptime @hasDecl(Index, "notifyVectorViewLoadForTest")) {
                    self.notifyVectorViewLoadForTest(lookups[i].vector_id);
                }
                vector_views[lookups[i].item_index] = slot_scratch[0..cached.len];
                continue;
            }
        }
        // Cache admission is an optimization, not an ownership contract.
        // Decode into this result's stable batch slot so a rejected cache
        // fill cannot leave every returned view aliasing the shared scalar
        // scratch buffer (and therefore the final vector decoded).
        const slot_scratch = batch_scratch[lookups[i].item_index * scratch.len ..][0..scratch.len];
        const view = try vectorViewFromRaw(value, slot_scratch);
        if (builtin.is_test and comptime @hasDecl(Index, "notifyVectorViewLoadForTest")) {
            self.notifyVectorViewLoadForTest(lookups[i].vector_id);
        }
        if (use_cache) {
            const guarded = comptime @hasDecl(Index, "beginVectorCacheFill");
            vector_views[lookups[i].item_index] = try cacheVectorAfterLoad(self, lookups[i].vector_id, view, .{
                .guarded = guarded,
                .epoch = lookups[i].vector_cache_fill_epoch,
            });
        } else {
            vector_views[lookups[i].item_index] = view;
        }
    }
}

/// Narrow test seam for the cache-admission ownership regression. Keeping the
/// generic loader private in production avoids widening the vector-index API.
pub fn loadVectorIdsSortedWithScratchForTest(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    vector_views: [][]const f32,
    lookup_storage: []search_runtime.RerankLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
    scratch: []f32,
    batch_scratch: []f32,
) !void {
    if (!builtin.is_test) @compileError("test-only vector batch loader seam");
    return try loadVectorIdsSortedWithScratch(
        self,
        txn,
        vector_ids,
        vector_views,
        lookup_storage,
        key_views_storage,
        values_storage,
        scratch,
        batch_scratch,
    );
}

fn loadTransformedVectorIdsIntoMatrix(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    matrix: []f32,
    options: anytype,
) !void {
    const dims = self.config.dims;
    const matrix_floats = std.math.mul(usize, vector_ids.len, dims) catch return error.BufferTooSmall;
    if (matrix.len < matrix_floats) return error.BufferTooSmall;

    const missing_ids = try self.alloc.alloc(u64, vector_ids.len);
    defer self.alloc.free(missing_ids);
    const missing_positions = try self.alloc.alloc(usize, vector_ids.len);
    defer self.alloc.free(missing_positions);

    var missing_count: usize = 0;
    const lookup = batchVectorLookup(options);
    for (vector_ids, 0..) |vector_id, i| {
        if (lookup) |batch_vectors| {
            if (batch_vectors.get(vector_id)) |vector| {
                const transformed = matrix[i * dims ..][0..dims];
                _ = self.transformVector(vector, transformed);
                continue;
            }
        }
        missing_ids[missing_count] = vector_id;
        missing_positions[missing_count] = i;
        missing_count += 1;
    }
    if (missing_count == 0) return;

    const lookups = try self.alloc.alloc(FixedKeyLookup, missing_count);
    defer self.alloc.free(lookups);
    const key_views = try self.alloc.alloc([]const u8, missing_count);
    defer self.alloc.free(key_views);
    const values = try self.alloc.alloc(?[]const u8, missing_count);
    defer self.alloc.free(values);
    const vector_scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(vector_scratch);

    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "loadExternalVectorsTransformedIntoMatrix")) {
        if (try self.loadExternalVectorsTransformedIntoMatrix(
            txn,
            missing_ids[0..missing_count],
            missing_positions[0..missing_count],
            matrix,
            lookups,
            key_views,
            values,
            vector_scratch,
        )) return;
    }

    const vector_views = try self.alloc.alloc([]const f32, missing_count);
    defer self.alloc.free(vector_views);
    const batch_scratch_floats = std.math.mul(usize, missing_count, dims) catch return error.BufferTooSmall;
    const batch_scratch = try self.alloc.alloc(f32, batch_scratch_floats);
    const batch_scratch_bytes = std.math.mul(usize, batch_scratch_floats, @sizeOf(f32)) catch return error.BufferTooSmall;
    addApplyWorkspaceBytes(self, @intCast(batch_scratch_bytes));
    defer {
        releaseApplyWorkspaceBytes(self, @intCast(batch_scratch_bytes));
        self.alloc.free(batch_scratch);
    }

    try loadVectorIdsSortedWithScratch(
        self,
        txn,
        missing_ids[0..missing_count],
        vector_views,
        lookups,
        key_views,
        values,
        vector_scratch,
        batch_scratch,
    );
    for (vector_views, 0..) |vector, i| {
        if (vector.len == 0) return error.NotFound;
        const transformed = matrix[missing_positions[i] * dims ..][0..dims];
        _ = self.transformVector(vector, transformed);
    }
}

pub fn loadPostingVectorsTransformed(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    matrix: []f32,
) !void {
    try loadTransformedVectorIdsIntoMatrix(self, txn, vector_ids, matrix, .{});
}

pub fn loadPostingVectorsTransformedWithOptions(
    self: anytype,
    txn: anytype,
    vector_ids: []const u64,
    matrix: []f32,
    options: anytype,
) !void {
    try loadTransformedVectorIdsIntoMatrix(self, txn, vector_ids, matrix, options);
}

fn recomputeAncestorCentroidsWithOptions(
    self: anytype,
    txn: anytype,
    start_parent_id: u64,
    options: hbc_runtime.BatchInsertOptions,
) !void {
    var parent_id = start_parent_id;
    while (parent_id != 0) {
        var parent = try loadNode(self, txn, parent_id);
        defer parent.deinit(self.alloc);
        try recomputeInternalCentroid(self, txn, &parent);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);
        parent_id = parent.parent;
    }
}

pub fn repairDirtyPostingsTxn(self: anytype, txn: anytype) !posting.PostingMaintenanceResult {
    return try spfresh_index.repairDirtyPostingsTxn(self, txn);
}

pub fn postingBacklogStatsTxn(self: anytype, txn: anytype) !posting.PostingBacklogStats {
    return try spfresh_index.postingBacklogStatsTxn(self, txn);
}

pub fn runAutoPostingMaintenanceTxn(self: anytype, txn: anytype) !void {
    return try spfresh_index.runAutoPostingMaintenanceTxn(self, txn);
}

pub fn repairDirtyPostingsTxnWithOptions(
    self: anytype,
    txn: anytype,
    options: posting.PostingMaintenanceOptions,
) !posting.PostingMaintenanceResult {
    return try spfresh_index.repairDirtyPostingsTxnWithOptions(self, txn, options);
}

test "loadVectorIdsSortedWithScratch external fallback keeps per-id vector views disjoint" {
    const TestTxn = struct {};
    const TestIndex = struct {
        fn hasExternalVectorLoader(_: @This()) bool {
            return true;
        }

        fn getVectorInto(_: @This(), _: TestTxn, vector_id: u64, scratch: []f32) ![]const f32 {
            scratch[0] = @floatFromInt(vector_id);
            scratch[1] = @floatFromInt(vector_id * 10);
            return scratch[0..2];
        }
    };

    const index = TestIndex{};
    const txn = TestTxn{};
    var vector_views: [2][]const f32 = undefined;
    var lookups: [2]FixedKeyLookup = undefined;
    var key_views: [2][]const u8 = undefined;
    var values: [2]?[]const u8 = .{ null, null };
    var scratch: [2]f32 = undefined;
    var batch_scratch: [4]f32 = undefined;

    try loadVectorIdsSortedWithScratch(
        index,
        txn,
        &.{ 1, 2 },
        vector_views[0..],
        lookups[0..],
        key_views[0..],
        values[0..],
        scratch[0..],
        batch_scratch[0..],
    );

    try std.testing.expectEqual(@as(usize, 2), vector_views[0].len);
    try std.testing.expectEqual(@as(usize, 2), vector_views[1].len);
    try std.testing.expectEqual(@as(f32, 1), vector_views[0][0]);
    try std.testing.expectEqual(@as(f32, 10), vector_views[0][1]);
    try std.testing.expectEqual(@as(f32, 2), vector_views[1][0]);
    try std.testing.expectEqual(@as(f32, 20), vector_views[1][1]);
    try std.testing.expect(@intFromPtr(vector_views[0].ptr) != @intFromPtr(vector_views[1].ptr));
}

test "loadTransformedVectorIdsIntoMatrix uses external transformed matrix loader" {
    const TestTxn = struct {};
    const TestConfig = struct {
        dims: usize = 2,
    };
    const TestIndex = struct {
        alloc: std.mem.Allocator,
        config: TestConfig,
        direct_calls: *usize,
        fallback_calls: *usize,

        fn transformVector(_: @This(), original: []const f32, transformed: []f32) []const f32 {
            @memcpy(transformed, original);
            return transformed;
        }

        fn loadExternalVectorsTransformedIntoMatrix(
            self: @This(),
            _: TestTxn,
            vector_ids: []const u64,
            matrix_positions: []const usize,
            matrix: []f32,
            _: []FixedKeyLookup,
            _: [][]const u8,
            _: []?[]const u8,
            _: []f32,
        ) !bool {
            self.direct_calls.* += 1;
            for (vector_ids, 0..) |vector_id, i| {
                const out = matrix[matrix_positions[i] * self.config.dims ..][0..self.config.dims];
                out[0] = @floatFromInt(vector_id);
                out[1] = @floatFromInt(vector_id * 10);
            }
            return true;
        }

        fn getVectorViewOrScratch(self: @This(), _: TestTxn, _: u64, _: []f32) ![]const f32 {
            self.fallback_calls.* += 1;
            return error.UnexpectedFallback;
        }

        fn getVectorInto(self: @This(), txn: TestTxn, vector_id: u64, scratch: []f32) ![]const f32 {
            return self.getVectorViewOrScratch(txn, vector_id, scratch);
        }
    };

    var direct_calls: usize = 0;
    var fallback_calls: usize = 0;
    const index = TestIndex{
        .alloc = std.testing.allocator,
        .config = .{},
        .direct_calls = &direct_calls,
        .fallback_calls = &fallback_calls,
    };
    var matrix: [4]f32 = undefined;
    try loadTransformedVectorIdsIntoMatrix(index, TestTxn{}, &.{ 3, 4 }, matrix[0..], .{});

    try std.testing.expectEqual(@as(usize, 1), direct_calls);
    try std.testing.expectEqual(@as(usize, 0), fallback_calls);
    try std.testing.expectEqual(@as(f32, 3), matrix[0]);
    try std.testing.expectEqual(@as(f32, 30), matrix[1]);
    try std.testing.expectEqual(@as(f32, 4), matrix[2]);
    try std.testing.expectEqual(@as(f32, 40), matrix[3]);
}

test "loadVectorIdsSortedWithScratch uses external batch scratch loader" {
    const TestTxn = struct {};
    const TestIndex = struct {
        calls: *usize,

        fn hasExternalVectorLoader(_: @This()) bool {
            return true;
        }

        fn getExternalVectorViewsSortedWithScratch(
            self: @This(),
            _: TestTxn,
            vector_ids: []const u64,
            vector_views: [][]const f32,
            _: []FixedKeyLookup,
            _: [][]const u8,
            _: []?[]const u8,
            _: []f32,
            batch_scratch: []f32,
        ) !bool {
            self.calls.* += 1;
            for (vector_ids, 0..) |vector_id, slot| {
                const view = batch_scratch[slot * 2 ..][0..2];
                view[0] = @floatFromInt(vector_id);
                view[1] = @floatFromInt(vector_id * 10);
                vector_views[slot] = view;
            }
            return true;
        }

        fn getVectorInto(_: @This(), _: TestTxn, _: u64, _: []f32) ![]const f32 {
            return error.UnexpectedFallback;
        }
    };

    var calls: usize = 0;
    const index = TestIndex{ .calls = &calls };
    const txn = TestTxn{};
    var vector_views: [2][]const f32 = undefined;
    var lookups: [2]FixedKeyLookup = undefined;
    var key_views: [2][]const u8 = undefined;
    var values: [2]?[]const u8 = .{ null, null };
    var scratch: [2]f32 = undefined;
    var batch_scratch: [4]f32 = undefined;

    try loadVectorIdsSortedWithScratch(
        index,
        txn,
        &.{ 1, 2 },
        vector_views[0..],
        lookups[0..],
        key_views[0..],
        values[0..],
        scratch[0..],
        batch_scratch[0..],
    );

    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqual(@as(f32, 1), vector_views[0][0]);
    try std.testing.expectEqual(@as(f32, 10), vector_views[0][1]);
    try std.testing.expectEqual(@as(f32, 2), vector_views[1][0]);
    try std.testing.expectEqual(@as(f32, 20), vector_views[1][1]);
    try std.testing.expect(@intFromPtr(vector_views[0].ptr) != @intFromPtr(vector_views[1].ptr));
}

test "boundary rerank selects only tail candidates overlapping kth candidate" {
    var ranked_items = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 0.10, .error_bound = 0.01 },
        .{ .vector_id = 2, .distance = 0.20, .error_bound = 0.01 },
        .{ .vector_id = 3, .distance = 0.205, .error_bound = 0.02 },
        .{ .vector_id = 4, .distance = 0.50, .error_bound = 0.01 },
    };
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 2,
    };

    const boundary = try selectRerankCandidates(std.testing.allocator, ranked_items[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(boundary.flags);
    try std.testing.expect(!boundary.flags[0]);
    try std.testing.expect(boundary.flags[1]);
    try std.testing.expect(boundary.flags[2]);
    try std.testing.expect(!boundary.flags[3]);

    const always = try selectRerankCandidates(std.testing.allocator, ranked_items[0..], req.k, req, .always);
    defer std.testing.allocator.free(always.flags);
    for (always.flags) |selected| try std.testing.expect(selected);

    const never = try selectRerankCandidates(std.testing.allocator, ranked_items[0..], req.k, req, .never);
    defer std.testing.allocator.free(never.flags);
    for (never.flags) |selected| try std.testing.expect(!selected);
}

test "boundary rerank uses explicit rerank boundary below candidate window" {
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 1024,
        .rerank_k = 2,
    };
    try std.testing.expectEqual(@as(usize, 2), rerankBoundaryK(req));

    var ranked_items = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 0.10, .error_bound = 0.01 },
        .{ .vector_id = 2, .distance = 0.20, .error_bound = 0.01 },
        .{ .vector_id = 3, .distance = 0.205, .error_bound = 0.02 },
        .{ .vector_id = 4, .distance = 0.50, .error_bound = 0.01 },
    };
    var flags = [_]bool{false} ** ranked_items.len;
    const selected = selectRerankCandidatesInto(flags[0..], ranked_items[0..], rerankBoundaryK(req), req, .boundary);
    try std.testing.expectEqual(@as(usize, 2), selected.top_k_count);
    try std.testing.expect(!selected.flags[0]);
    try std.testing.expect(selected.flags[1]);
    try std.testing.expect(selected.flags[2]);
    try std.testing.expect(!selected.flags[3]);
}

test "boundary rerank skips stable ordering and top-k-only ambiguity" {
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 2,
    };

    var stable = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 1.0, .error_bound = 0.01 },
        .{ .vector_id = 2, .distance = 2.0, .error_bound = 0.01 },
        .{ .vector_id = 3, .distance = 3.0, .error_bound = 0.01 },
    };
    const stable_flags = try selectRerankCandidates(std.testing.allocator, stable[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(stable_flags.flags);
    for (stable_flags.flags) |selected| try std.testing.expect(!selected);

    var top_k_only = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 1.0, .error_bound = 0.3 },
        .{ .vector_id = 2, .distance = 1.2, .error_bound = 0.3 },
        .{ .vector_id = 3, .distance = 3.0, .error_bound = 0.01 },
    };
    const top_k_flags = try selectRerankCandidates(std.testing.allocator, top_k_only[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(top_k_flags.flags);
    try std.testing.expect(top_k_flags.flags[0]);
    try std.testing.expect(top_k_flags.flags[1]);
    try std.testing.expect(!top_k_flags.flags[2]);
}

test "boundary rerank ignores non-boundary top-k to tail overlap" {
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 2,
    };

    var non_boundary_overlap = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 0.10, .error_bound = 0.05 },
        .{ .vector_id = 2, .distance = 0.40, .error_bound = 0.01 },
        .{ .vector_id = 3, .distance = 0.55, .error_bound = 0.02 },
    };
    const selected = try selectRerankCandidates(std.testing.allocator, non_boundary_overlap[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(selected.flags);
    for (selected.flags) |flag| try std.testing.expect(!flag);
    try std.testing.expectEqual(@as(usize, 0), selected.rerank_candidate_count);
    try std.testing.expectEqual(@as(usize, 0), selected.ambiguous_boundary_pairs);
}

test "boundary rerank includes tail interval overlap from candidate uncertainty" {
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 2,
    };

    var tail_bound_overlap = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 0.10, .error_bound = 0.01 },
        .{ .vector_id = 2, .distance = 0.40, .error_bound = 0.01 },
        .{ .vector_id = 3, .distance = 0.55, .error_bound = 0.20 },
    };
    const selected = try selectRerankCandidates(std.testing.allocator, tail_bound_overlap[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(selected.flags);
    try std.testing.expect(!selected.flags[0]);
    try std.testing.expect(selected.flags[1]);
    try std.testing.expect(selected.flags[2]);
    try std.testing.expectEqual(@as(usize, 2), selected.rerank_candidate_count);
    try std.testing.expectEqual(@as(usize, 1), selected.ambiguous_boundary_pairs);
}

test "boundary rerank selects boundary overlap band only" {
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 2,
    };

    var boundary_band = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 0.10, .error_bound = 0.01 },
        .{ .vector_id = 2, .distance = 0.40, .error_bound = 0.10 },
        .{ .vector_id = 3, .distance = 0.45, .error_bound = 0.02 },
        .{ .vector_id = 4, .distance = 0.80, .error_bound = 0.01 },
    };
    const selected = try selectRerankCandidates(std.testing.allocator, boundary_band[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(selected.flags);
    try std.testing.expect(!selected.flags[0]);
    try std.testing.expect(selected.flags[1]);
    try std.testing.expect(selected.flags[2]);
    try std.testing.expect(!selected.flags[3]);
    try std.testing.expectEqual(@as(usize, 2), selected.rerank_candidate_count);
    try std.testing.expectEqual(@as(usize, 1), selected.ambiguous_boundary_pairs);
}

test "boundary rerank threshold ambiguity selects retained approximate set" {
    var ranked_items = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 1, .distance = 1.0, .error_bound = 0.2 },
        .{ .vector_id = 2, .distance = 2.0, .error_bound = 0.2 },
    };
    const distance_over: f32 = 1.1;
    const req: search_types.SearchRequest = .{
        .query = &.{},
        .k = 1,
        .distance_over = distance_over,
    };

    const boundary = try selectRerankCandidates(std.testing.allocator, ranked_items[0..], req.k, req, .boundary);
    defer std.testing.allocator.free(boundary.flags);
    for (boundary.flags) |selected| try std.testing.expect(selected);
}

test "estimate quantized distances rejects stale quantized count" {
    const alloc = std.testing.allocator;
    var quantizer = try quantizer_mod.RaBitQuantizer.init(alloc, 2, 42, .l2_squared);
    defer quantizer.deinit();

    const TestIndex = struct {
        config: types.HBCConfig,
        quantizer: quantizer_mod.RaBitQuantizer,
    };
    const self = TestIndex{
        .config = .{
            .dims = 2,
            .metric = .l2_squared,
        },
        .quantizer = quantizer,
    };

    const data = try alloc.dupe(f32, &.{ 1, 0, 0, 1, 1, 1 });
    defer alloc.free(data);
    const qs = hbc_runtime.QuantizedSet{
        .nonquant = .{
            .vectors = .{
                .dims = 2,
                .count = 3,
                .data = data,
            },
        },
    };

    var distances: [2]f32 = undefined;
    var error_bounds: [2]f32 = undefined;
    var scratch = try quantizer_mod.RaBitQuantizer.EstimateScratch.init(alloc, 2);
    defer scratch.deinit(alloc);

    try std.testing.expectError(
        error.Corrupted,
        estimateQuantizedDistances(&self, &qs, &.{ 1, 0 }, 0, distances[0..], error_bounds[0..], &scratch),
    );
}

test "only root nodes use nonquantized payloads" {
    var root_leaf = types.Node{
        .id = 1,
        .is_leaf = true,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &.{},
        .members = &.{},
    };
    try std.testing.expect(usesNonQuantizedPayload(&root_leaf));

    var root_internal = types.Node{
        .id = 2,
        .is_leaf = false,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &.{},
        .members = &.{},
    };
    try std.testing.expect(usesNonQuantizedPayload(&root_internal));

    var child_leaf = types.Node{
        .id = 3,
        .is_leaf = true,
        .level = 1,
        .parent = 2,
        .centroid = &.{},
        .children = &.{},
        .members = &.{},
    };
    try std.testing.expect(!usesNonQuantizedPayload(&child_leaf));

    var child_internal = types.Node{
        .id = 4,
        .is_leaf = false,
        .level = 1,
        .parent = 2,
        .centroid = &.{},
        .children = &.{},
        .members = &.{},
    };
    try std.testing.expect(!usesNonQuantizedPayload(&child_internal));
}

test "dirty leaf payloads are not fresh stored payloads" {
    var leaf = types.Node{
        .id = 3,
        .is_leaf = true,
        .level = 1,
        .parent = 2,
        .centroid = &.{},
        .children = &.{},
        .members = &.{},
        .posting_state = .{ .payload_dirty = true, .dirty = true },
    };
    try std.testing.expect(!usesNonQuantizedPayload(&leaf));
    try std.testing.expect(!hasFreshStoredPayload(&leaf));

    leaf.posting_state.notePayloadRefreshed();
    try std.testing.expect(hasFreshStoredPayload(&leaf));
}

test "kmeans bulk builder packs bounded leaves" {
    const go_rand = @import("antfly_vector").go_rand;
    const MockIndex = struct {
        alloc: Allocator,
        config: types.HBCConfig,
        rng: go_rand.GoPcg,
        next_id: u64 = 1,
        leaf_count: usize = 0,
        max_leaf_members: usize = 0,
        internal_count: usize = 0,
        max_internal_children: usize = 0,

        fn nextNodeId(self: *@This()) u64 {
            const id = self.next_id;
            self.next_id += 1;
            return id;
        }

        fn putVecLeaf(_: *@This(), _: anytype, _: u64, _: u64) !void {}

        fn saveNodeBody(self: *@This(), _: anytype, node: *types.Node) !void {
            if (node.is_leaf) {
                self.leaf_count += 1;
                self.max_leaf_members = @max(self.max_leaf_members, node.members.len);
            } else {
                self.internal_count += 1;
                self.max_internal_children = @max(self.max_internal_children, node.children.len);
            }
        }

        fn putNodeSplitRange(_: *@This(), _: anytype, _: u64, _: anytype) !void {}

        fn updateParent(_: *@This(), _: anytype, _: u64, _: u64) !void {}

        fn getCachedNodeClone(_: *@This(), _: u64) !?types.Node {
            return null;
        }

        fn loadNodeFromStorage(_: *@This(), _: anytype, _: u64) !types.Node {
            return error.UnexpectedNodeLoad;
        }

        fn cacheNode(_: *@This(), _: *const types.Node) !void {}
    };

    const raw = [_]f32{
        0.0,  0.0,
        0.1,  0.0,
        10.0, 10.0,
        10.1, 10.0,
        20.0, 0.0,
        20.1, 0.0,
    };
    const metadata = [_][]const u8{ "a", "b", "c", "d", "e", "f" };
    var inputs: [6]bulk_build.PreparedBulkBuildInput = undefined;
    for (&inputs, 0..) |*input, i| {
        const vector = raw[i * 2 ..][0..2];
        input.* = .{
            .vector_id = @intCast(i + 1),
            .vector = vector,
            .transformed = vector,
            .metadata = metadata[i],
        };
    }

    var mock = MockIndex{
        .alloc = std.testing.allocator,
        .config = .{
            .dims = 2,
            .leaf_size = 2,
            .branching_factor = 2,
            .metric = .cosine,
            .kmeans_max_iter = 4,
            .kmeans_update_strategy = .segmented,
            .use_quantization = false,
        },
        .rng = go_rand.GoPcg.init(42, 1024),
    };

    var built = try buildBulkKmeansFromInputs(&mock, {}, &inputs);
    defer built.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 3), mock.leaf_count);
    try std.testing.expect(mock.max_leaf_members <= 2);
    try std.testing.expect(mock.internal_count > 1);
    try std.testing.expect(mock.max_internal_children <= 2);
    try std.testing.expectEqual(@as(usize, inputs.len), built.member_count);
}

pub fn populateMetadata(self: anytype, txn: anytype, results: *search_results.SearchResults) !void {
    return populateMetadataWithCachePolicy(self, txn, results, true);
}

fn populateMetadataWithCachePolicy(self: anytype, txn: anytype, results: *search_results.SearchResults, comptime use_cache: bool) !void {
    try self.bindTxnLike(txn);
    if (comptime txnSupportsGetManySorted(@TypeOf(txn))) {
        try populateMetadataBatched(self, txn, results, use_cache);
        return;
    }
    for (results.items.items) |*item| {
        if (item.metadata != null) continue;
        const data = (try loadMetadataRawWithCachePolicy(self, txn, item.vector_id, use_cache, isNotFoundGeneric)) orelse continue;
        item.metadata = try self.alloc.dupe(u8, data);
    }
}

fn populateMetadataWithScratch(self: anytype, txn: anytype, results: *search_results.SearchResults, scratch: anytype, comptime use_cache: bool) !void {
    try self.bindTxnLike(txn);
    if (comptime txnSupportsGetManySorted(@TypeOf(txn))) {
        try populateMetadataBatchedWithScratch(self, txn, results, scratch.lookups, scratch.key_views, scratch.values, use_cache);
        return;
    }
    try populateMetadataWithCachePolicy(self, txn, results, use_cache);
}

fn populateMetadataBatched(self: anytype, txn: anytype, results: *search_results.SearchResults, comptime use_cache: bool) !void {
    const lookups = try self.alloc.alloc(FixedKeyLookup, results.items.items.len);
    defer self.alloc.free(lookups);
    const key_views = try self.alloc.alloc([]const u8, results.items.items.len);
    defer self.alloc.free(key_views);
    const values = try self.alloc.alloc(?[]const u8, results.items.items.len);
    defer self.alloc.free(values);
    try populateMetadataBatchedWithScratch(self, txn, results, lookups, key_views, values, use_cache);
}

fn populateMetadataBatchedWithScratch(
    self: anytype,
    txn: anytype,
    results: *search_results.SearchResults,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
    comptime use_cache: bool,
) !void {
    var lookup_count: usize = 0;
    for (results.items.items, 0..) |item, index| {
        if (item.metadata != null) continue;
        if (use_cache) {
            if (borrowCachedMetadataHandle(self, item.vector_id)) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                results.items.items[index].metadata = try self.alloc.dupe(u8, handle.view());
                continue;
            }
        }
        const Index = comptime childType(@TypeOf(self));
        if (use_cache and comptime !@hasDecl(Index, "borrowCachedMetadata")) {
            if (self.getCachedMetadata(item.vector_id)) |cached| {
                results.items.items[index].metadata = try self.alloc.dupe(u8, cached);
                continue;
            }
        }
        var key: [10]u8 = undefined;
        _ = hbc.encodeVecMetaKey(&key, item.vector_id);
        lookup_storage[lookup_count] = .{
            .item_index = index,
            .vector_id = item.vector_id,
            .key = key,
        };
        lookup_count += 1;
    }
    if (lookup_count == 0) return;

    const lookups = lookup_storage[0..lookup_count];
    const key_views = key_views_storage[0..lookup_count];
    const values = values_storage[0..lookup_count];
    std.mem.sort(FixedKeyLookup, lookups, {}, lessFixedKeyLookup);
    for (lookups, 0..) |*lookup, i| key_views[i] = lookup.key[0..];

    const fill = if (use_cache) searchCacheFillForTxn(self, txn) else SearchCacheFill{ .guarded = false, .epoch = null };
    try txn.getManySorted(.vecs, key_views, values);
    for (values, 0..) |maybe_value, i| {
        const value = maybe_value orelse continue;
        if (use_cache) _ = try cacheMetadataAfterLoad(self, lookups[i].vector_id, value, fill);
        results.items.items[lookups[i].item_index].metadata = try self.alloc.dupe(u8, value);
    }
}

pub fn memberMatchesRequest(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    distance: f32,
    error_bound: f32,
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
    approximate: bool,
) !bool {
    return memberMatchesRequestWithCachePolicy(self, txn, vector_id, distance, error_bound, req, filter_state, approximate, true);
}

fn memberMatchesRequestWithCachePolicy(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    distance: f32,
    error_bound: f32,
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
    approximate: bool,
    comptime use_cache: bool,
) !bool {
    try self.bindTxnLike(txn);
    if (filter_state.rejects(vector_id)) return false;
    if (req.distance_over) |threshold| {
        if (approximate) {
            if (distance + error_bound < threshold) return false;
        } else if (distance <= threshold) return false;
    }
    if (req.distance_under) |threshold| {
        if (approximate) {
            if (distance - error_bound > threshold) return false;
        } else if (distance >= threshold) return false;
    }
    if (req.filter_prefix.len > 0) {
        if (use_cache) {
            if (borrowCachedMetadataHandle(self, vector_id)) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                if (!std.mem.startsWith(u8, handle.view(), req.filter_prefix)) return false;
                return true;
            }
        }
        const metadata = (try loadMetadataRawWithCachePolicy(self, txn, vector_id, use_cache, isNotFoundGeneric)) orelse return false;
        if (!std.mem.startsWith(u8, metadata, req.filter_prefix)) return false;
    }
    return true;
}

pub fn minLeafOccupancy(self: anytype) usize {
    if (self.config.leaf_size <= 2) return 1;
    return self.config.leaf_size / 2;
}

fn normalizeCentroidForMetric(self: anytype, centroid: []f32) void {
    if (self.config.metric == .cosine and centroid.len > 0) {
        _ = vec.normalize(centroid);
    }
}

fn l2CoveringRadiusForMatrix(centroid: []const f32, vectors: []const f32, count: usize) f32 {
    if (count == 0) return 0;
    if (centroid.len == 0 or vectors.len < count * centroid.len) return std.math.nan(f32);
    var max_squared: f32 = 0;
    for (0..count) |row| {
        const candidate = vectors[row * centroid.len ..][0..centroid.len];
        var squared: f32 = 0;
        for (centroid, candidate) |center, value| {
            const delta = value - center;
            squared += delta * delta;
        }
        max_squared = @max(max_squared, squared);
    }
    return @sqrt(max_squared);
}

fn expandL2RadiusAfterAppend(node: *types.Node, appended: []const f32) void {
    if (node.members.len <= 1) {
        node.covering_radius = 0;
        return;
    }
    if (!std.math.isFinite(node.covering_radius) or node.covering_radius < 0 or appended.len != node.centroid.len) {
        node.covering_radius = std.math.nan(f32);
        return;
    }
    const old_count: f32 = @floatFromInt(node.members.len - 1);
    var shift_squared: f32 = 0;
    var appended_squared: f32 = 0;
    for (node.centroid, appended) |new_center, value| {
        const appended_delta = value - new_center;
        appended_squared += appended_delta * appended_delta;
        const shift = appended_delta / old_count;
        shift_squared += shift * shift;
    }
    node.covering_radius = @max(node.covering_radius + @sqrt(shift_squared), @sqrt(appended_squared));
}

fn expandL2RadiusAfterBatchAppend(node: *types.Node, appended: []const f32, added_count: usize) void {
    if (added_count == 0) return;
    if (node.members.len == added_count) {
        node.covering_radius = l2CoveringRadiusForMatrix(node.centroid, appended, added_count);
        return;
    }
    if (!std.math.isFinite(node.covering_radius) or node.covering_radius < 0 or appended.len < added_count * node.centroid.len) {
        node.covering_radius = std.math.nan(f32);
        return;
    }
    const old_count = node.members.len - added_count;
    const old_count_f: f32 = @floatFromInt(old_count);
    const total_count_f: f32 = @floatFromInt(node.members.len);
    var shift_squared: f32 = 0;
    for (node.centroid, 0..) |new_center, dim| {
        var appended_sum: f32 = 0;
        for (0..added_count) |row| appended_sum += appended[row * node.centroid.len + dim];
        const old_center = (new_center * total_count_f - appended_sum) / old_count_f;
        const shift = old_center - new_center;
        shift_squared += shift * shift;
    }
    var radius = node.covering_radius + @sqrt(shift_squared);
    for (0..added_count) |row| {
        const candidate = appended[row * node.centroid.len ..][0..node.centroid.len];
        var squared: f32 = 0;
        for (node.centroid, candidate) |center, value| {
            const delta = value - center;
            squared += delta * delta;
        }
        radius = @max(radius, @sqrt(squared));
    }
    node.covering_radius = radius;
}

fn l2SubtreeLowerBound(candidate: types.PriorityItem, covering_radius: f32) ?f32 {
    if (!std.math.isFinite(covering_radius) or covering_radius < 0) return null;
    const centroid_squared_lower = @max(@as(f32, 0), candidate.distance - candidate.error_bound);
    if (!std.math.isFinite(centroid_squared_lower)) return null;
    const vector_distance_lower = @max(@as(f32, 0), @sqrt(centroid_squared_lower) - covering_radius);
    return vector_distance_lower * vector_distance_lower;
}

fn computeInternalCoveringRadius(self: anytype, txn: anytype, node: *const types.Node) !f32 {
    if (self.config.metric != .l2_squared) return std.math.nan(f32);
    if (node.children.len == 0) return 0;
    var radius: f32 = 0;
    for (node.children) |child_id| {
        var child = try loadNode(self, txn, child_id);
        defer child.deinit(self.alloc);
        if (!std.math.isFinite(child.covering_radius) or child.covering_radius < 0) return std.math.nan(f32);
        const center_squared = vec.distanceToQuery(node.centroid, vec.dot(node.centroid, node.centroid), child.centroid, .l2_squared);
        radius = @max(radius, @sqrt(@max(center_squared, 0)) + child.covering_radius);
    }
    return radius;
}

pub fn recomputeLeafCentroid(self: anytype, txn: anytype, leaf: *types.Node) !void {
    try posting.PostingStore.recomputeCentroid(self, txn, leaf);
}

fn applyLeafCentroidDelta(self: anytype, leaf: *types.Node, delta: []const f32) !void {
    if (leaf.members.len == 0) {
        @memset(leaf.centroid, 0);
        return;
    }
    if (leaf.centroid.len != self.config.dims or delta.len != self.config.dims) {
        return error.InvalidArgument;
    }
    const n: f32 = @floatFromInt(leaf.members.len);
    for (leaf.centroid, 0..) |*c, i| c.* += delta[i] / n;
    normalizeCentroidForMetric(self, leaf.centroid);
    // An in-place replacement removes one old point and adds another. The
    // centroid delta alone cannot tighten the old sphere safely without both
    // endpoints, so maintenance recomputes it and search uses width meanwhile.
    leaf.covering_radius = std.math.nan(f32);
    posting.PostingStore.noteCentroidRefreshed(leaf);
}

const DeferredLeafCentroidDelta = struct {
    leaf_id: u64,
    delta_sum: []f32,
};

fn appendLeafCentroidDelta(
    self: anytype,
    deltas: *std.ArrayListUnmanaged(DeferredLeafCentroidDelta),
    leaf_id: u64,
    old_transformed: []const f32,
    new_transformed: []const f32,
) !void {
    for (deltas.items) |*entry| {
        if (entry.leaf_id != leaf_id) continue;
        for (entry.delta_sum, 0..) |*delta, i| delta.* += new_transformed[i] - old_transformed[i];
        return;
    }
    const delta_sum = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(delta_sum);
    for (delta_sum, 0..) |*delta, i| delta.* = new_transformed[i] - old_transformed[i];
    try deltas.append(self.alloc, .{
        .leaf_id = leaf_id,
        .delta_sum = delta_sum,
    });
}

pub fn recomputeInternalCentroid(self: anytype, txn: anytype, node: *types.Node) !void {
    if (node.children.len == 0) {
        @memset(node.centroid, 0);
        return;
    }
    if (node.centroid.len != self.config.dims) {
        if (node.centroid.len > 0) self.alloc.free(node.centroid);
        node.centroid = try self.alloc.alloc(f32, self.config.dims);
    }
    @memset(node.centroid, 0);
    for (node.children) |child_id| {
        var child = try loadNode(self, txn, child_id);
        defer child.deinit(self.alloc);
        vec.add(node.centroid, child.centroid);
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(node.children.len)), node.centroid);
    normalizeCentroidForMetric(self, node.centroid);
    node.covering_radius = try computeInternalCoveringRadius(self, txn, node);
}

fn updateInternalCentroidForLeafSplit(
    self: anytype,
    parent: *types.Node,
    previous_child_centroid: []const f32,
    left_child_centroid: []const f32,
    right_child_centroid: []const f32,
    previous_child_count: usize,
) !void {
    if (previous_child_count == 0) return error.Corrupted;
    if (parent.centroid.len != self.config.dims) {
        if (parent.centroid.len > 0) self.alloc.free(parent.centroid);
        parent.centroid = try self.alloc.alloc(f32, self.config.dims);
    }

    const old_weight: f32 = @floatFromInt(previous_child_count);
    const new_weight: f32 = @floatFromInt(previous_child_count + 1);

    for (parent.centroid, previous_child_centroid, left_child_centroid, right_child_centroid) |*dst, previous_child, left_child, right_child| {
        dst.* = ((dst.* * old_weight) - previous_child + left_child + right_child) / new_weight;
    }
    normalizeCentroidForMetric(self, parent.centroid);
}

pub fn collapseSingleChildParents(self: anytype, txn: anytype, start_node_id: u64) !void {
    var node_id = start_node_id;
    while (node_id != 0) {
        var node = try loadNode(self, txn, node_id);
        defer node.deinit(self.alloc);
        if (node.is_leaf or node.children.len != 1) return;

        const child_id = node.children[0];
        var child = try loadNode(self, txn, child_id);
        defer child.deinit(self.alloc);
        const parent_id = node.parent;
        child.parent = parent_id;
        try self.saveNode(txn, &child);

        if (parent_id == 0) {
            self.metadata.root_node = child_id;
            try deleteNode(self, txn, node_id);
            return;
        }

        var parent = try loadNode(self, txn, parent_id);
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);
        for (parent.children) |*cid| {
            if (cid.* == node_id) {
                cid.* = child_id;
                break;
            }
        }
        try recomputeInternalCentroid(self, txn, &parent);
        try self.saveNode(txn, &parent);
        try deleteNode(self, txn, node_id);
        node_id = parent_id;
    }
}

pub fn delete(self: anytype, vector_id: u64) !void {
    const publishing = try beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    var txn = try self.beginRuntimeWriteTxn();
    errdefer txn.abort();
    errdefer abortVectorCacheMutationsIfSupported(self);
    try deleteTxn(self, &txn, vector_id);
    try runAutoPostingMaintenanceTxn(self, &txn);
    try self.flushMetadata(&txn);
    try markPublishSearchStateCommittingIfSupported(self, publishing);
    try txn.commit();
    try finishPublishSearchStateIfSupported(self, publishing);
}

pub fn batchDelete(self: anytype, vector_ids: []const u64) !void {
    if (vector_ids.len == 0) return;
    if (vector_ids.len == 1) return delete(self, vector_ids[0]);

    const publishing = try beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    var batch = try self.beginRuntimeBatchTxn();
    errdefer batch.abort();
    errdefer abortVectorCacheMutationsIfSupported(self);
    try batchDeleteTxn(self, &batch, vector_ids);
    try runAutoPostingMaintenanceTxn(self, &batch);
    try self.flushMetadata(&batch);
    try markPublishSearchStateCommittingIfSupported(self, publishing);
    try batch.commit();
    try finishPublishSearchStateIfSupported(self, publishing);
}

const PreparedBatchDelete = struct {
    vector_id: u64,
    leaf_id: u64,
};

fn lessPreparedBatchDelete(_: void, lhs: PreparedBatchDelete, rhs: PreparedBatchDelete) bool {
    return if (lhs.leaf_id == rhs.leaf_id)
        lhs.vector_id < rhs.vector_id
    else
        lhs.leaf_id < rhs.leaf_id;
}

fn batchDeleteTxn(self: anytype, txn: anytype, vector_ids: []const u64) !void {
    try self.bindTxnLike(txn);
    if (vector_ids.len == 0) return;
    if (vector_ids.len == 1) return deleteTxn(self, txn, vector_ids[0]) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };

    var prepared = std.ArrayListUnmanaged(PreparedBatchDelete).empty;
    defer prepared.deinit(self.alloc);
    try prepared.ensureTotalCapacity(self.alloc, @intCast(vector_ids.len));

    for (vector_ids) |vector_id| {
        const leaf_id = self.getVecLeaf(txn, vector_id) catch |err| blk: {
            if (!isNotFoundGeneric(err)) return err;
            break :blk (try findLeafContainingMember(self, txn, self.metadata.root_node, vector_id)) orelse continue;
        };
        prepared.appendAssumeCapacity(.{ .vector_id = vector_id, .leaf_id = leaf_id });
    }
    if (prepared.items.len == 0) return;

    std.mem.sort(PreparedBatchDelete, prepared.items, {}, lessPreparedBatchDelete);

    var group_start: usize = 0;
    while (group_start < prepared.items.len) {
        var group_end = group_start + 1;
        while (group_end < prepared.items.len and prepared.items[group_end].leaf_id == prepared.items[group_start].leaf_id) : (group_end += 1) {}
        const group = prepared.items[group_start..group_end];
        const leaf_id = group[0].leaf_id;

        var leaf = loadNode(self, txn, leaf_id) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            // Stale vec→leaf mappings into a deleted node: the vectors are
            // unreachable by search, so drop their keys instead of failing
            // the whole batch, and flag a repair sweep.
            noteTreeLinkInconsistencyIfSupported(self);
            for (group) |entry| deleteVectorKeys(self, txn, entry.vector_id);
            self.metadata.active_count -|= @intCast(group.len);
            group_start = group_end;
            continue;
        };
        defer leaf.deinit(self.alloc);
        try leaf.ensureUnbacked(self.alloc);

        const remove_ids = try self.alloc.alloc(u64, group.len);
        defer self.alloc.free(remove_ids);
        for (group, 0..) |entry, i| remove_ids[i] = entry.vector_id;
        const removed_count = try posting.PostingStore.removeMembers(self.alloc, &leaf, remove_ids);
        if (removed_count == 0) {
            group_start = group_end;
            continue;
        }

        if (leaf.members.len > 0 and shouldDeferPostingCentroidRefresh(self, &leaf)) {
            self.write_profile.posting_lazy_centroid_deferrals += 1;
        } else if (leaf.members.len > 0) {
            try posting.PostingStore.recomputeCentroid(self, txn, &leaf);
        } else {
            @memset(leaf.centroid, 0);
        }

        if (leaf.members.len == 0 and leaf.parent != 0) {
            unlink: {
                var parent = loadNode(self, txn, leaf.parent) catch |err| {
                    if (!isNotFoundGeneric(err)) return err;
                    // Dangling parent pointer: delete the empty leaf and let
                    // the repair sweep clear whatever still references it.
                    noteTreeLinkInconsistencyIfSupported(self);
                    try deleteNode(self, txn, leaf_id);
                    break :unlink;
                };
                defer parent.deinit(self.alloc);
                try parent.ensureUnbacked(self.alloc);
                if (try removeChildLink(self, &parent, leaf_id)) {
                    try recomputeInternalCentroid(self, txn, &parent);
                    try self.saveNode(txn, &parent);
                    try deleteNode(self, txn, leaf_id);
                    try collapseSingleChildParents(self, txn, leaf.parent);
                } else {
                    try deleteNode(self, txn, leaf_id);
                }
            }
        } else {
            try self.saveNode(txn, &leaf);
        }

        for (group) |entry| deleteVectorKeys(self, txn, entry.vector_id);
        self.metadata.active_count -|= @intCast(removed_count);
        group_start = group_end;
    }
}

/// Removes leaf_id from parent.children, replacing the slice. Returns false
/// — leaving the parent untouched — when the recorded parent does not
/// actually reference the leaf. That happens when a stale parent link
/// survives tree maintenance; rebuilding children with len-1 in that state
/// used to overrun the new array and panic the whole process. When this
/// fires, some OTHER node may still list leaf_id as a child, so the index
/// is flagged for a repairTreeLinks sweep that clears the dangling
/// reference instead of leaving it latent.
fn removeChildLink(self: anytype, parent: anytype, leaf_id: u64) !bool {
    var match_count: usize = 0;
    for (parent.children) |cid| {
        if (cid == leaf_id) match_count += 1;
    }
    if (match_count == 0) {
        std.log.warn("hbc: leaf {d} not referenced by its recorded parent; skipping unlink", .{leaf_id});
        noteTreeLinkInconsistencyIfSupported(self);
        return false;
    }
    if (match_count > 1) {
        // Duplicate links to the same child are corruption. The leaf is
        // being unlinked for deletion, so drop every occurrence — sizing
        // the new array by the exact match count, or the skip loop below
        // would underfill it and persist an uninitialized child id — and
        // flag a repair sweep for the rest of the tree.
        std.log.warn("hbc: leaf {d} referenced {d} times by its parent; removing all links", .{ leaf_id, match_count });
        noteTreeLinkInconsistencyIfSupported(self);
    }
    var new_children = try self.alloc.alloc(u64, parent.children.len - match_count);
    errdefer self.alloc.free(new_children);
    var wi_child: usize = 0;
    for (parent.children) |cid| {
        if (cid == leaf_id) continue;
        new_children[wi_child] = cid;
        wi_child += 1;
    }
    self.alloc.free(parent.children);
    parent.children = new_children;
    return true;
}

/// Signals the concrete index (when it supports it) that a tree-link
/// inconsistency was observed, so background maintenance schedules a
/// repairTreeLinks sweep.
fn noteTreeLinkInconsistencyIfSupported(self: anytype) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "noteTreeLinkInconsistency")) {
        self.noteTreeLinkInconsistency();
    }
}

pub const TreeLinkReport = struct {
    nodes_visited: u64 = 0,
    /// Child ids referenced by an internal node whose node record is gone,
    /// plus duplicate references to an already-visited node.
    dangling_children: u64 = 0,
    /// Nodes whose recorded parent differs from the node that references them.
    parent_mismatches: u64 = 0,
    /// Internal nodes with zero children.
    empty_internal_nodes: u64 = 0,
    /// Leaf members whose vec→leaf mapping is missing or points elsewhere.
    vec_leaf_mismatches: u64 = 0,

    pub fn consistent(self: TreeLinkReport) bool {
        return self.dangling_children == 0 and
            self.parent_mismatches == 0 and
            self.empty_internal_nodes == 0 and
            self.vec_leaf_mismatches == 0;
    }
};

/// Walks the tree from the root and checks the structural invariants that
/// maintenance must preserve: every referenced child exists, every node's
/// recorded parent is the node that references it, internal nodes are
/// non-empty, and every leaf member's vec→leaf mapping points back at its
/// leaf. Read-only; usable with a read transaction. Intended for tests and
/// debugging — it visits every node and every member mapping.
pub fn verifyTreeLinks(self: anytype, txn: anytype) !TreeLinkReport {
    var report: TreeLinkReport = .{};
    const QueueItem = struct { id: u64, parent: u64 };
    var queue = std.ArrayListUnmanaged(QueueItem).empty;
    defer queue.deinit(self.alloc);
    var seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen.deinit(self.alloc);

    try queue.append(self.alloc, .{ .id = self.metadata.root_node, .parent = 0 });
    try seen.put(self.alloc, self.metadata.root_node, {});

    var qi: usize = 0;
    while (qi < queue.items.len) : (qi += 1) {
        const item = queue.items[qi];
        var node = loadNode(self, txn, item.id) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            report.dangling_children += 1;
            continue;
        };
        defer node.deinit(self.alloc);
        report.nodes_visited += 1;
        if (node.parent != item.parent) report.parent_mismatches += 1;
        if (node.is_leaf) {
            for (node.members) |member_id| {
                const mapped = self.getVecLeaf(txn, member_id) catch |err| {
                    if (!isNotFoundGeneric(err)) return err;
                    report.vec_leaf_mismatches += 1;
                    continue;
                };
                if (mapped != item.id) report.vec_leaf_mismatches += 1;
            }
            continue;
        }
        if (node.children.len == 0) report.empty_internal_nodes += 1;
        for (node.children) |child_id| {
            const gop = try seen.getOrPut(self.alloc, child_id);
            if (gop.found_existing) {
                // The same node is referenced from two places (duplicate
                // link or cycle); count it without re-walking.
                report.dangling_children += 1;
                continue;
            }
            try queue.append(self.alloc, .{ .id = child_id, .parent = item.id });
        }
    }
    return report;
}

pub const TreeLinkRepairReport = struct {
    nodes_visited: u64 = 0,
    dangling_children_removed: u64 = 0,
    duplicate_children_removed: u64 = 0,
    parent_pointers_fixed: u64 = 0,
    empty_nodes_removed: u64 = 0,
    vec_leaf_mappings_fixed: u64 = 0,
    /// False when the node budget ran out before the walk finished; call
    /// again to continue (repairs already made are persisted).
    completed: bool = true,

    pub fn repaired(self: TreeLinkRepairReport) u64 {
        return self.dangling_children_removed +
            self.duplicate_children_removed +
            self.parent_pointers_fixed +
            self.empty_nodes_removed +
            self.vec_leaf_mappings_fixed;
    }
};

/// Repairs the invariants verifyTreeLinks checks, treating the reachable
/// children lists as authoritative:
///   - a child id whose node record is gone is dropped from its parent;
///   - a child referenced both here and by its recorded parent keeps the
///     recorded link and loses the duplicate;
///   - a child whose recorded parent is stale (gone, or not referencing it)
///     is repointed at the node that actually references it;
///   - an internal node left with zero children is unlinked and deleted
///     (the root is converted back to an empty leaf instead);
///   - leaf members' vec→leaf mappings are repointed at the leaf that
///     actually holds them.
/// Bounded by `max_nodes` per call; repairs are persisted as the walk goes,
/// so a budget-exhausted sweep (`completed == false`) resumes safely on the
/// next call. Must run inside a write transaction; single-writer, like all
/// HBC maintenance.
pub fn repairTreeLinks(self: anytype, txn: anytype, max_nodes: usize) !TreeLinkRepairReport {
    try self.bindTxnLike(txn);
    var report: TreeLinkRepairReport = .{};
    var queue = std.ArrayListUnmanaged(u64).empty;
    defer queue.deinit(self.alloc);
    var seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen.deinit(self.alloc);

    try queue.append(self.alloc, self.metadata.root_node);
    try seen.put(self.alloc, self.metadata.root_node, {});

    var qi: usize = 0;
    while (qi < queue.items.len) : (qi += 1) {
        if (report.nodes_visited >= max_nodes) {
            report.completed = false;
            break;
        }
        const node_id = queue.items[qi];
        var node = loadNode(self, txn, node_id) catch |err| {
            // A node can disappear mid-sweep when pruning an empty parent
            // collapses part of the tree we already queued.
            if (!isNotFoundGeneric(err)) return err;
            continue;
        };
        defer node.deinit(self.alloc);
        report.nodes_visited += 1;

        if (node.is_leaf) {
            for (node.members) |member_id| {
                const mapped: ?u64 = self.getVecLeaf(txn, member_id) catch |err| blk: {
                    if (!isNotFoundGeneric(err)) return err;
                    break :blk null;
                };
                if (mapped == null or mapped.? != node_id) {
                    try self.putVecLeaf(txn, member_id, node_id);
                    report.vec_leaf_mappings_fixed += 1;
                }
            }
            continue;
        }

        try node.ensureUnbacked(self.alloc);
        var kept = std.ArrayListUnmanaged(u64).empty;
        defer kept.deinit(self.alloc);
        try kept.ensureTotalCapacity(self.alloc, node.children.len);
        var pruned = false;
        for (node.children) |child_id| {
            const child_parent: ?u64 = loadNodeParent(self, txn, child_id) catch |err| blk: {
                if (!isNotFoundGeneric(err)) return err;
                break :blk null;
            };
            if (child_parent == null) {
                report.dangling_children_removed += 1;
                pruned = true;
                continue;
            }
            if (child_parent.? != node_id) {
                if (try nodeReferencesChild(self, txn, child_parent.?, child_id)) {
                    // The recorded parent also references this child: keep
                    // that link, drop this duplicate.
                    report.duplicate_children_removed += 1;
                    pruned = true;
                    continue;
                }
                // Recorded parent is stale; this node actually holds the
                // child, so repoint the child here.
                var child = try loadNode(self, txn, child_id);
                defer child.deinit(self.alloc);
                try child.ensureUnbacked(self.alloc);
                child.parent = node_id;
                try self.saveNode(txn, &child);
                report.parent_pointers_fixed += 1;
            }
            const gop = try seen.getOrPut(self.alloc, child_id);
            if (gop.found_existing) {
                // Second reference to a node we already kept — either a
                // duplicate within this children list or another visited
                // parent legitimately holds it. Drop this occurrence.
                report.duplicate_children_removed += 1;
                pruned = true;
                continue;
            }
            kept.appendAssumeCapacity(child_id);
            try queue.append(self.alloc, child_id);
        }

        if (kept.items.len == 0) {
            if (node_id == self.metadata.root_node) {
                // Convert an emptied root back into an empty leaf, mirroring
                // a freshly created index.
                node.is_leaf = true;
                node.level = 0;
                self.alloc.free(node.children);
                node.children = &.{};
                @memset(node.centroid, 0);
                try self.saveNode(txn, &node);
            } else {
                const parent_id = node.parent;
                try deleteNode(self, txn, node_id);
                report.empty_nodes_removed += 1;
                var parent = loadNode(self, txn, parent_id) catch |err| {
                    if (!isNotFoundGeneric(err)) return err;
                    continue;
                };
                defer parent.deinit(self.alloc);
                try parent.ensureUnbacked(self.alloc);
                if (try removeChildLink(self, &parent, node_id)) {
                    try recomputeInternalCentroid(self, txn, &parent);
                    try self.saveNode(txn, &parent);
                    try collapseSingleChildParents(self, txn, parent_id);
                }
            }
            continue;
        }

        if (pruned) {
            const new_children = try self.alloc.dupe(u64, kept.items);
            self.alloc.free(node.children);
            node.children = new_children;
            try recomputeInternalCentroid(self, txn, &node);
            try self.saveNode(txn, &node);
        }
    }
    return report;
}

/// Standalone repairTreeLinks: opens its own write transaction, flushes
/// metadata (the sweep can change root/node bookkeeping via collapse), and
/// republishes search state — mirroring delete()/batchDelete().
pub fn repairLinks(self: anytype, max_nodes: usize) !TreeLinkRepairReport {
    const publishing = try beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    var txn = try self.beginRuntimeWriteTxn();
    errdefer txn.abort();
    errdefer abortVectorCacheMutationsIfSupported(self);
    const report = try repairTreeLinks(self, &txn, max_nodes);
    try self.flushMetadata(&txn);
    try markPublishSearchStateCommittingIfSupported(self, publishing);
    try txn.commit();
    try finishPublishSearchStateIfSupported(self, publishing);
    return report;
}

fn nodeReferencesChild(self: anytype, txn: anytype, parent_id: u64, child_id: u64) !bool {
    if (parent_id == 0) return false;
    var parent = loadNode(self, txn, parent_id) catch |err| {
        if (!isNotFoundGeneric(err)) return err;
        return false;
    };
    defer parent.deinit(self.alloc);
    if (parent.is_leaf) return false;
    for (parent.children) |cid| {
        if (cid == child_id) return true;
    }
    return false;
}

/// Drops a vector's storage keys (raw vector, vec→leaf mapping, metadata)
/// and invalidates its cache entries. Key deletes are best-effort.
fn deleteVectorKeys(self: anytype, txn: anytype, vector_id: u64) void {
    var vkey_buf: [10]u8 = undefined;
    self.deleteNamespaced(txn, .vecs, hbc.encodeVecKey(&vkey_buf, vector_id)) catch {};
    self.deleteNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&vkey_buf, vector_id)) catch {};
    self.deleteNamespaced(txn, .vecs, hbc.encodeVecMetaKey(&vkey_buf, vector_id)) catch {};
    self.invalidateVectorCache(vector_id);
    self.invalidateMetadataCache(vector_id);
}

pub fn deleteTxn(self: anytype, txn: anytype, vector_id: u64) !void {
    try self.bindTxnLike(txn);
    var leaf_id = self.getVecLeaf(txn, vector_id) catch |err| blk: {
        if (!isNotFoundGeneric(err)) return err;
        break :blk (try findLeafContainingMember(self, txn, self.metadata.root_node, vector_id)) orelse return error.NotFound;
    };

    var leaf = loadNode(self, txn, leaf_id) catch |err| blk: {
        if (!isNotFoundGeneric(err)) return err;
        // Stale vec→leaf mapping into a deleted node. Fall back to a scan;
        // if no reachable leaf holds the vector, it is unreachable garbage —
        // drop its keys instead of failing the delete, and flag a repair.
        noteTreeLinkInconsistencyIfSupported(self);
        const scanned = (try findLeafContainingMember(self, txn, self.metadata.root_node, vector_id)) orelse {
            deleteVectorKeys(self, txn, vector_id);
            self.metadata.active_count -|= 1;
            return;
        };
        leaf_id = scanned;
        break :blk try loadNode(self, txn, scanned);
    };
    defer leaf.deinit(self.alloc);
    try leaf.ensureUnbacked(self.alloc);

    try posting.PostingStore.removeMember(self.alloc, &leaf, vector_id);
    leaf.covering_radius = if (leaf.members.len == 0) 0 else std.math.nan(f32);

    if (leaf.members.len > 0 and shouldDeferPostingCentroidRefresh(self, &leaf)) {
        self.write_profile.posting_lazy_centroid_deferrals += 1;
    } else if (leaf.members.len > 0) {
        try posting.PostingStore.recomputeCentroid(self, txn, &leaf);
    } else {
        @memset(leaf.centroid, 0);
        leaf.covering_radius = 0;
    }

    if (leaf.members.len == 0 and leaf.parent != 0) {
        var parent = loadNode(self, txn, leaf.parent) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            // Dangling parent pointer: delete the empty leaf and let the
            // repair sweep clear whichever node still references it.
            noteTreeLinkInconsistencyIfSupported(self);
            try deleteNode(self, txn, leaf_id);
            deleteVectorKeys(self, txn, vector_id);
            self.metadata.active_count -|= 1;
            return;
        };
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);
        if (try removeChildLink(self, &parent, leaf_id)) {
            try recomputeInternalCentroid(self, txn, &parent);
            try self.saveNode(txn, &parent);
            try deleteNode(self, txn, leaf_id);
            try collapseSingleChildParents(self, txn, leaf.parent);
        } else {
            try deleteNode(self, txn, leaf_id);
        }
    } else {
        try self.saveNode(txn, &leaf);

        if (leaf.parent != 0 and leaf.members.len < minLeafOccupancy(self)) skip_merge: {
            var parent = loadNode(self, txn, leaf.parent) catch |err| {
                if (!isNotFoundGeneric(err)) return err;
                // Dangling parent pointer: the leaf is already saved; skip
                // the merge attempt and flag a repair.
                noteTreeLinkInconsistencyIfSupported(self);
                break :skip_merge;
            };
            defer parent.deinit(self.alloc);
            try parent.ensureUnbacked(self.alloc);
            var best_sibling_id: u64 = 0;
            var best_dist: f32 = std.math.inf(f32);
            for (parent.children) |cid| {
                if (cid == leaf_id) continue;
                var sibling = loadNode(self, txn, cid) catch |err| {
                    if (!isNotFoundGeneric(err)) return err;
                    // Dangling sibling reference; skip it and flag a repair.
                    noteTreeLinkInconsistencyIfSupported(self);
                    continue;
                };
                defer sibling.deinit(self.alloc);
                if (!sibling.is_leaf) continue;
                if (sibling.members.len + leaf.members.len > self.config.leaf_size) continue;
                const dist = vec.distance(leaf.centroid, sibling.centroid, self.config.metric);
                if (dist < best_dist) {
                    best_dist = dist;
                    best_sibling_id = cid;
                }
            }

            if (best_sibling_id != 0) {
                var sibling = try loadNode(self, txn, best_sibling_id);
                defer sibling.deinit(self.alloc);
                try sibling.ensureUnbacked(self.alloc);
                const merged_len = sibling.members.len + leaf.members.len;
                var merged = try self.alloc.alloc(u64, merged_len);
                @memcpy(merged[0..sibling.members.len], sibling.members);
                @memcpy(merged[sibling.members.len..], leaf.members);
                self.alloc.free(sibling.members);
                sibling.members = merged;
                try posting.PostingStore.recomputeCentroid(self, txn, &sibling);
                try self.saveNode(txn, &sibling);
                for (leaf.members) |mid| try self.putVecLeaf(txn, mid, best_sibling_id);

                if (try removeChildLink(self, &parent, leaf_id)) {
                    try recomputeInternalCentroid(self, txn, &parent);
                    try self.saveNode(txn, &parent);
                    try deleteNode(self, txn, leaf_id);
                    try collapseSingleChildParents(self, txn, leaf.parent);
                } else {
                    try deleteNode(self, txn, leaf_id);
                }
            }
        }
    }

    deleteVectorKeys(self, txn, vector_id);
    self.metadata.active_count -= 1;
}

fn findLeafContainingMember(self: anytype, txn: anytype, node_id: u64, vector_id: u64) !?u64 {
    if (node_id == 0) return null;
    var node = loadNode(self, txn, node_id) catch |err| {
        if (isNotFoundGeneric(err)) return null;
        return err;
    };
    defer node.deinit(self.alloc);
    if (node.is_leaf) {
        for (node.members) |member_id| {
            if (member_id == vector_id) return node.id;
        }
        return null;
    }
    for (node.children) |child_id| {
        if (try findLeafContainingMember(self, txn, child_id, vector_id)) |leaf_id| return leaf_id;
    }
    return null;
}

pub fn refreshAncestorSplitRangesCounted(self: anytype, txn: anytype, parent_id: u64) !usize {
    var current_id = parent_id;
    var refreshed: usize = 0;
    while (current_id != 0) {
        var node = try loadNode(self, txn, current_id);
        defer node.deinit(self.alloc);
        try saveNodeSplitRange(self, txn, &node, isNotFoundGeneric);
        refreshed += 1;
        current_id = node.parent;
    }
    return refreshed;
}

pub fn refreshAncestorSplitRanges(self: anytype, txn: anytype, parent_id: u64) !void {
    _ = try refreshAncestorSplitRangesCounted(self, txn, parent_id);
}

pub fn extendAncestorSplitRanges(
    self: anytype,
    txn: anytype,
    parent_id: u64,
    child_range: *const types.NodeSplitRange,
) !void {
    var current_id = parent_id;
    while (current_id != 0) {
        const next_parent = try loadNodeParent(self, txn, current_id);
        const maybe_existing = try loadNodeSplitRange(self, txn, current_id, isNotFoundGeneric);
        if (maybe_existing) |existing_range| {
            var updated = existing_range;
            var changed = false;
            if (std.mem.order(u8, child_range.min_key, updated.min_key) == .lt) {
                self.alloc.free(updated.min_key);
                updated.min_key = try self.alloc.dupe(u8, child_range.min_key);
                changed = true;
            }
            if (std.mem.order(u8, child_range.max_key, updated.max_key) == .gt) {
                self.alloc.free(updated.max_key);
                updated.max_key = try self.alloc.dupe(u8, child_range.max_key);
                changed = true;
            }
            if (changed) {
                defer updated.deinit(self.alloc);
                try putNodeSplitRange(self, txn, current_id, &updated, isNotFoundGeneric);
                current_id = next_parent;
                continue;
            }
            updated.deinit(self.alloc);
            break;
        }

        var cloned = try child_range.clone(self.alloc);
        defer cloned.deinit(self.alloc);
        try putNodeSplitRange(self, txn, current_id, &cloned, isNotFoundGeneric);
        current_id = next_parent;
    }
}

pub fn insert(self: anytype, vector_id: u64, vector_data: []const f32, now_fn_u64: fn () u64, elapsed_fn_u64: fn (u64) u64) !void {
    try insertWithMetadata(self, vector_id, vector_data, "", now_fn_u64, elapsed_fn_u64);
}

pub fn insertWithMetadata(
    self: anytype,
    vector_id: u64,
    vector_data: []const f32,
    metadata_value: []const u8,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const publishing = try beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    var txn = try self.beginRuntimeWriteTxn();
    errdefer txn.abort();
    errdefer abortVectorCacheMutationsIfSupported(self);
    const transformed_vector = try self.alloc.alloc(f32, self.config.dims);
    defer self.alloc.free(transformed_vector);
    try insertWithMetadataTxn(self, &txn, vector_id, vector_data, metadata_value, transformed_vector, now_fn_u64, elapsed_fn_u64);
    try runAutoPostingMaintenanceTxn(self, &txn);
    const flush_start = now_fn_u64();
    try self.flushMetadata(&txn);
    self.write_profile.insert_flush_metadata_ns += elapsed_fn_u64(flush_start);
    const commit_start = now_fn_u64();
    try markPublishSearchStateCommittingIfSupported(self, publishing);
    try txn.commit();
    self.write_profile.insert_commit_ns += elapsed_fn_u64(commit_start);
    try finishPublishSearchStateIfSupported(self, publishing);
}

pub fn insertWithMetadataTxn(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    vector_data: []const f32,
    metadata_value: []const u8,
    transformed_vector: []f32,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    try insertWithMetadataTxnOptions(self, txn, vector_id, vector_data, null, metadata_value, transformed_vector, .{}, now_fn_u64, elapsed_fn_u64);
}

pub fn insertWithMetadataTxnOptions(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    vector_data: []const f32,
    pretransformed_vector: ?[]const f32,
    metadata_value: []const u8,
    transformed_vector: []f32,
    options: anytype,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    try self.bindTxnLike(txn);
    self.write_profile.insert_calls += 1;
    const Options = @TypeOf(options);
    const assume_absent_ids = if (@hasField(Options, "assume_absent_ids")) options.assume_absent_ids else false;
    const centroid_only_routing = if (@hasField(Options, "centroid_only_routing")) options.centroid_only_routing else false;
    const allow_quantized_routing = if (@hasField(Options, "allow_quantized_routing")) options.allow_quantized_routing else !centroid_only_routing;
    const defer_quantized_rebuild = if (@hasField(Options, "defer_quantized_rebuild")) options.defer_quantized_rebuild else false;
    const defer_quantized_rebuild_to_bulk_finish = if (@hasField(Options, "defer_quantized_rebuild_to_bulk_finish")) options.defer_quantized_rebuild_to_bulk_finish else false;
    const coalesce_leaf_writes = if (@hasField(Options, "coalesce_leaf_writes")) options.coalesce_leaf_writes else false;
    const skip_vector_store = if (@hasField(Options, "skip_vector_store")) options.skip_vector_store else false;
    const bulk_ingest = if (@hasField(Options, "bulk_ingest")) options.bulk_ingest else false;
    const defer_leaf_splits_to_batch_finish = if (@hasField(Options, "defer_leaf_splits_to_batch_finish")) options.defer_leaf_splits_to_batch_finish else false;
    const defer_leaf_splits_to_bulk_finish = if (@hasField(Options, "defer_leaf_splits_to_bulk_finish")) options.defer_leaf_splits_to_bulk_finish else false;
    const bulk_rebuild_leaf_min_members = if (@hasField(Options, "bulk_rebuild_leaf_min_members")) options.bulk_rebuild_leaf_min_members else 0;
    const batch_vectors = if (@hasField(Options, "batch_vectors")) options.batch_vectors else null;
    const batch_insert_options: hbc_runtime.BatchInsertOptions = .{
        .defer_quantized_rebuild = defer_quantized_rebuild,
        .defer_quantized_rebuild_to_bulk_finish = defer_quantized_rebuild_to_bulk_finish,
        .centroid_only_routing = centroid_only_routing,
        .allow_quantized_routing = allow_quantized_routing,
        .assume_absent_ids = assume_absent_ids,
        .coalesce_leaf_writes = coalesce_leaf_writes,
        .skip_vector_store = skip_vector_store,
        .bulk_ingest = bulk_ingest,
        .defer_leaf_splits_to_batch_finish = defer_leaf_splits_to_batch_finish,
        .defer_leaf_splits_to_bulk_finish = defer_leaf_splits_to_bulk_finish,
        .suppress_quantized_payload_persist = if (@hasField(Options, "suppress_quantized_payload_persist")) options.suppress_quantized_payload_persist else false,
        .bulk_rebuild_leaf_min_members = bulk_rebuild_leaf_min_members,
        .batch_vectors = batch_vectors,
    };

    const transform_start = now_fn_u64();
    const effective_transformed = if (pretransformed_vector) |existing|
        existing
    else blk: {
        _ = self.transformVector(vector_data, transformed_vector);
        break :blk transformed_vector;
    };
    self.write_profile.insert_transform_ns += elapsed_fn_u64(transform_start);
    var compare_vector_storage: ?[]f32 = null;
    defer if (compare_vector_storage) |buf| self.alloc.free(buf);
    var previous_vector_storage: ?[]f32 = null;
    defer if (previous_vector_storage) |buf| self.alloc.free(buf);
    var previous_transformed_storage: ?[]f32 = null;
    defer if (previous_transformed_storage) |buf| self.alloc.free(buf);

    const existing_leaf_id = if (assume_absent_ids)
        0
    else
        self.getVecLeaf(txn, vector_id) catch |err| blk: {
            if (isNotFoundGeneric(err)) break :blk 0;
            return err;
        };

    if (existing_leaf_id != 0) {
        compare_vector_storage = try self.alloc.alloc(f32, self.config.dims);
        if (try existingVectorMatchesNoOp(self, txn, vector_id, vector_data, metadata_value, compare_vector_storage.?)) {
            self.write_profile.noop_existing_skips += 1;
            return;
        }
        previous_vector_storage = try self.alloc.alloc(f32, self.config.dims);
        previous_transformed_storage = try self.alloc.alloc(f32, self.config.dims);
    }

    const target_leaf_id = blk_leaf: {
        if (existing_leaf_id != 0) {
            const find_leaf_start = now_fn_u64();
            const leaf_id = try posting.CentroidDirectory.findPosting(self, txn, self.metadata.root_node, effective_transformed, allow_quantized_routing);
            self.write_profile.insert_find_leaf_ns += elapsed_fn_u64(find_leaf_start);
            if (existing_leaf_id == leaf_id) {
                if (try tryUpdateExistingVectorInLeafTxnOptions(
                    self,
                    txn,
                    leaf_id,
                    vector_id,
                    vector_data,
                    metadata_value,
                    effective_transformed,
                    previous_vector_storage.?,
                    previous_transformed_storage.?,
                    skip_vector_store,
                    batch_insert_options,
                )) {
                    return;
                }
            } else {
                removeFromLeaf(self, txn, existing_leaf_id, vector_id) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
            }
            break :blk_leaf leaf_id;
        }

        const find_leaf_start = now_fn_u64();
        const leaf_id = try posting.CentroidDirectory.findPosting(self, txn, self.metadata.root_node, effective_transformed, allow_quantized_routing);
        self.write_profile.insert_find_leaf_ns += elapsed_fn_u64(find_leaf_start);
        break :blk_leaf leaf_id;
    };

    const store_start = now_fn_u64();
    // The embedding may be authoritative in an external LSM rather than this
    // index's vector namespace. Invalidate unconditionally so a retained
    // derivative from an earlier generation cannot survive an update merely
    // because cache seeding is bypassed for this write.
    self.invalidateVectorCache(vector_id);
    if (!skip_vector_store) {
        try putVector(self, txn, vector_id, vector_data);
    } else if (shouldSeedRetainedVectorCacheOnSkipStore(self)) {
        _ = self.cacheVector(vector_id, vector_data) catch {};
    }
    if (metadata_value.len > 0) try putMetadata(self, txn, vector_id, metadata_value);
    self.write_profile.insert_store_vector_ns += elapsed_fn_u64(store_start);

    const mutate_start = now_fn_u64();
    var leaf = try loadNode(self, txn, target_leaf_id);
    defer leaf.deinit(self.alloc);
    try leaf.ensureUnbacked(self.alloc);

    _ = try posting.PostingStore.appendMember(self.alloc, &leaf, vector_id);

    const n = leaf.members.len;
    if (shouldDeferPostingCentroidRefresh(self, &leaf)) {
        self.write_profile.posting_lazy_centroid_deferrals += 1;
    } else if (leaf.centroid.len == 0) {
        leaf.centroid = try self.alloc.dupe(f32, effective_transformed);
        normalizeCentroidForMetric(self, leaf.centroid);
        posting.PostingStore.noteCentroidRefreshed(&leaf);
    } else {
        const nf: f32 = @floatFromInt(n);
        for (leaf.centroid, 0..) |*c, i| {
            c.* = c.* * (nf - 1.0) / nf + effective_transformed[i] / nf;
        }
        normalizeCentroidForMetric(self, leaf.centroid);
        posting.PostingStore.noteCentroidRefreshed(&leaf);
    }
    if (self.config.metric == .l2_squared) {
        expandL2RadiusAfterAppend(&leaf, effective_transformed);
    } else {
        leaf.covering_radius = std.math.nan(f32);
    }
    const leaf_overflows = leaf.members.len > self.config.leaf_size;
    const defer_leaf_split = shouldDeferOversizedLeafSplit(self, &leaf, batch_insert_options);
    var save_options = batch_insert_options;
    save_options.suppress_quantized_payload_persist = defer_leaf_split;

    if (metadata_value.len > 0) {
        var range_changed = false;
        var updated_range: types.NodeSplitRange = blk: {
            if (try loadNodeSplitRange(self, txn, target_leaf_id, isNotFoundGeneric)) |existing| {
                var range = existing;
                if (std.mem.order(u8, metadata_value, range.min_key) == .lt) {
                    self.alloc.free(range.min_key);
                    range.min_key = try self.alloc.dupe(u8, metadata_value);
                    range_changed = true;
                }
                if (std.mem.order(u8, metadata_value, range.max_key) == .gt) {
                    self.alloc.free(range.max_key);
                    range.max_key = try self.alloc.dupe(u8, metadata_value);
                    range_changed = true;
                }
                break :blk range;
            }
            range_changed = true;
            break :blk .{
                .min_key = try self.alloc.dupe(u8, metadata_value),
                .max_key = try self.alloc.dupe(u8, metadata_value),
            };
        };
        defer updated_range.deinit(self.alloc);

        try saveExistingNodeBodyWithAddedVectorOptions(self, txn, &leaf, effective_transformed, save_options, now_fn_u64_adapter(now_fn_u64), elapsed_fn_u64_adapter(elapsed_fn_u64));
        if (range_changed) {
            try putNodeSplitRange(self, txn, leaf.id, &updated_range, isNotFoundGeneric);
            try extendAncestorSplitRanges(self, txn, leaf.parent, &updated_range);
        }
    } else {
        const start = now_fn_u64();
        defer {
            self.write_profile.save_node_ns += elapsed_fn_u64(start);
            self.write_profile.save_node_calls += 1;
        }
        try saveExistingNodeBodyWithAddedVectorOptions(self, txn, &leaf, effective_transformed, save_options, now_fn_u64_adapter(now_fn_u64), elapsed_fn_u64_adapter(elapsed_fn_u64));
        const range_start = now_fn_u64();
        try saveNodeSplitRange(self, txn, &leaf, isNotFoundGeneric);
        self.write_profile.save_split_range_ns += elapsed_fn_u64(range_start);
    }

    try self.putVecLeaf(txn, vector_id, target_leaf_id);
    if (existing_leaf_id == 0) self.metadata.active_count += 1;

    if (leaf_overflows) {
        if (defer_leaf_split) {
            try recordDeferredOversizedLeaf(self, leaf.id);
        } else {
            try self.splitLeafWithOptions(txn, &leaf, batch_insert_options);
        }
    }
    self.write_profile.insert_mutate_leaf_ns += elapsed_fn_u64(mutate_start);
}

fn nodeHasMember(members: []const u64, vector_id: u64) bool {
    for (members) |member_id| {
        if (member_id == vector_id) return true;
    }
    return false;
}

fn tryUpdateExistingVectorInLeafTxnOptions(
    self: anytype,
    txn: anytype,
    leaf_id: u64,
    vector_id: u64,
    vector_data: []const f32,
    metadata_value: []const u8,
    effective_transformed: []const f32,
    previous_vector_storage: []f32,
    previous_transformed_storage: []f32,
    skip_vector_store: bool,
    options: anytype,
) !bool {
    var existing_leaf = try loadNode(self, txn, leaf_id);
    defer existing_leaf.deinit(self.alloc);
    if (!nodeHasMember(existing_leaf.members, vector_id)) return false;

    const previous_transformed = blk_previous: {
        const previous_vector = self.getVectorScratch(txn, vector_id, previous_vector_storage) catch |err| switch (err) {
            error.NotFound => break :blk_previous null,
            else => return err,
        };
        _ = self.transformVector(previous_vector, previous_transformed_storage);
        break :blk_previous previous_transformed_storage[0..];
    };
    const store_start = nowNsU64Fixed();
    try storeVectorAndMetadataWithOptions(self, txn, vector_id, vector_data, metadata_value, skip_vector_store);
    self.write_profile.insert_store_vector_ns += elapsedSinceU64Fixed(store_start);
    posting.PostingStore.noteVectorsChanged(&existing_leaf);
    if (shouldDeferPostingCentroidRefresh(self, &existing_leaf)) {
        self.write_profile.posting_lazy_centroid_deferrals += 1;
    } else if (previous_transformed) |old_transformed| {
        const delta_storage = try self.alloc.alloc(f32, self.config.dims);
        defer self.alloc.free(delta_storage);
        for (delta_storage, 0..) |*delta, i| delta.* = effective_transformed[i] - old_transformed[i];
        applyLeafCentroidDelta(self, &existing_leaf, delta_storage) catch {
            try posting.PostingStore.recomputeCentroid(self, txn, &existing_leaf);
        };
    } else {
        try posting.PostingStore.recomputeCentroid(self, txn, &existing_leaf);
    }
    try self.saveNodeWithOptionsMode(txn, &existing_leaf, options, false);
    if (shouldDeferPostingCentroidRefresh(self, &existing_leaf)) {
        if (existing_leaf.parent != 0) self.write_profile.posting_lazy_ancestor_deferrals += 1;
    } else {
        try recomputeAncestorCentroidsWithOptions(self, txn, existing_leaf.parent, options);
    }
    return true;
}

fn tryCoalesceExistingVectorInLeafTxnOptions(
    self: anytype,
    txn: anytype,
    leaf_id: u64,
    vector_id: u64,
    vector_data: []const f32,
    metadata_value: []const u8,
    effective_transformed: []const f32,
    previous_vector_storage: []f32,
    previous_transformed_storage: []f32,
    deferred_recompute_leaf_ids: *std.ArrayListUnmanaged(u64),
    deferred_leaf_centroid_deltas: *std.ArrayListUnmanaged(DeferredLeafCentroidDelta),
    deferred_ancestor_centroid_refresh_ids: *std.ArrayListUnmanaged(u64),
    options: hbc_runtime.BatchInsertOptions,
) !bool {
    if (!options.coalesce_leaf_writes) return false;

    const force_leaf_touch = options.bulk_ingest and options.skip_vector_store;
    if (!force_leaf_touch and try existingVectorMatchesNoOp(self, txn, vector_id, vector_data, metadata_value, previous_vector_storage)) {
        self.write_profile.noop_existing_skips += 1;
        return true;
    }

    const previous_transformed = blk_previous: {
        const previous_vector = self.getVectorScratch(txn, vector_id, previous_vector_storage) catch |err| switch (err) {
            error.NotFound => break :blk_previous null,
            else => return err,
        };
        _ = self.transformVector(previous_vector, previous_transformed_storage);
        break :blk_previous previous_transformed_storage[0..];
    };

    const store_start = nowNsU64Fixed();
    try storeVectorAndMetadataWithOptions(self, txn, vector_id, vector_data, metadata_value, options.skip_vector_store);
    self.write_profile.insert_store_vector_ns += elapsedSinceU64Fixed(store_start);

    if (previous_transformed) |old_transformed| {
        try appendLeafCentroidDelta(self, deferred_leaf_centroid_deltas, leaf_id, old_transformed, effective_transformed);
    } else {
        try appendUniqueU64(self.alloc, deferred_recompute_leaf_ids, leaf_id);
    }
    var leaf = try loadNode(self, txn, leaf_id);
    defer leaf.deinit(self.alloc);
    try appendUniqueU64(self.alloc, deferred_ancestor_centroid_refresh_ids, leaf.parent);
    return true;
}

fn existingVectorMatchesNoOp(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    next_vector: []const f32,
    next_metadata: []const u8,
    scratch: []f32,
) !bool {
    const existing_metadata = try getMetadataInTxn(self, txn, vector_id, isNotFoundGeneric);
    if (existing_metadata) |value| {
        if (!std.mem.eql(u8, value, next_metadata)) return false;
    } else if (next_metadata.len != 0) {
        return false;
    }
    const existing_vector = getVectorScratch(self, txn, vector_id, scratch) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (existing_vector.len != next_vector.len) return false;
    return std.mem.eql(u8, std.mem.sliceAsBytes(existing_vector), std.mem.sliceAsBytes(next_vector));
}

pub fn removeFromLeaf(self: anytype, txn: anytype, leaf_id: u64, vector_id: u64) !void {
    try self.bindTxnLike(txn);
    var leaf = try loadNode(self, txn, leaf_id);
    defer leaf.deinit(self.alloc);
    try leaf.ensureUnbacked(self.alloc);

    try posting.PostingStore.removeMember(self.alloc, &leaf, vector_id);

    if (leaf.members.len > 0 and shouldDeferPostingCentroidRefresh(self, &leaf)) {
        self.write_profile.posting_lazy_centroid_deferrals += 1;
    } else if (leaf.members.len > 0) {
        try posting.PostingStore.recomputeCentroid(self, txn, &leaf);
    } else {
        @memset(leaf.centroid, 0);
    }

    if (leaf.members.len == 0 and leaf.parent != 0) {
        var parent = loadNode(self, txn, leaf.parent) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            // Dangling parent pointer: delete the empty leaf and let the
            // repair sweep clear whatever still references it.
            noteTreeLinkInconsistencyIfSupported(self);
            try deleteNode(self, txn, leaf_id);
            return;
        };
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);
        if (try removeChildLink(self, &parent, leaf_id)) {
            try recomputeInternalCentroid(self, txn, &parent);
            try self.saveNodeWithOptionsMode(txn, &parent, .{}, false);
            try deleteNode(self, txn, leaf_id);
            try collapseSingleChildParents(self, txn, leaf.parent);
        } else {
            try deleteNode(self, txn, leaf_id);
        }
        return;
    }

    try self.saveNodeWithOptionsMode(txn, &leaf, .{}, false);

    if (leaf.parent != 0 and leaf.members.len < minLeafOccupancy(self)) {
        var parent = loadNode(self, txn, leaf.parent) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            // Dangling parent pointer: the leaf is already saved; skip the
            // merge attempt and flag a repair.
            noteTreeLinkInconsistencyIfSupported(self);
            return;
        };
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);
        var best_sibling_id: u64 = 0;
        var best_dist: f32 = std.math.inf(f32);
        for (parent.children) |cid| {
            if (cid == leaf_id) continue;
            var sibling = loadNode(self, txn, cid) catch |err| {
                if (!isNotFoundGeneric(err)) return err;
                // Dangling sibling reference; skip it and flag a repair.
                noteTreeLinkInconsistencyIfSupported(self);
                continue;
            };
            defer sibling.deinit(self.alloc);
            if (!sibling.is_leaf) continue;
            if (sibling.members.len + leaf.members.len > self.config.leaf_size) continue;
            const dist = vec.distance(leaf.centroid, sibling.centroid, self.config.metric);
            if (dist < best_dist) {
                best_dist = dist;
                best_sibling_id = cid;
            }
        }

        if (best_sibling_id != 0) {
            var sibling = try loadNode(self, txn, best_sibling_id);
            defer sibling.deinit(self.alloc);
            try sibling.ensureUnbacked(self.alloc);
            const merged_len = sibling.members.len + leaf.members.len;
            var merged = try self.alloc.alloc(u64, merged_len);
            var merged_owned = true;
            errdefer if (merged_owned) self.alloc.free(merged);
            @memcpy(merged[0..sibling.members.len], sibling.members);
            @memcpy(merged[sibling.members.len..], leaf.members);
            self.alloc.free(sibling.members);
            sibling.members = merged;
            // sibling's deferred destructor owns the replacement after the
            // transfer, including every subsequent error path.
            merged_owned = false;
            try posting.PostingStore.recomputeCentroid(self, txn, &sibling);
            try self.saveNodeWithOptionsMode(txn, &sibling, .{}, false);
            for (leaf.members) |mid| try self.putVecLeaf(txn, mid, best_sibling_id);

            if (try removeChildLink(self, &parent, leaf_id)) {
                try recomputeInternalCentroid(self, txn, &parent);
                try self.saveNodeWithOptionsMode(txn, &parent, .{}, false);
                try deleteNode(self, txn, leaf_id);
                try collapseSingleChildParents(self, txn, leaf.parent);
            } else {
                try deleteNode(self, txn, leaf_id);
            }
        }
    }
}

pub fn splitInternal(self: anytype, txn: anytype, node: *const types.Node) !void {
    try splitInternalWithOptions(self, txn, node, .{}, nowNsI128Fixed, elapsedSinceNsFixed);
}

pub fn splitInternalWithOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try self.bindTxnLike(txn);
    const start = now_fn();
    defer {
        self.write_profile.split_internal_ns += elapsed_fn(start);
        self.write_profile.split_internal_calls += 1;
    }
    const dims = self.config.dims;
    const count = node.children.len;

    const split_workspace_bytes =
        @as(u64, @intCast(count * dims * @sizeOf(f32))) +
        @as(u64, @intCast(dims * @sizeOf(f32))) +
        @as(u64, @intCast(dims * @sizeOf(f32)));
    addApplyWorkspaceBytes(self, split_workspace_bytes);
    defer releaseApplyWorkspaceBytes(self, split_workspace_bytes);

    const vec_data = try self.alloc.alloc(f32, count * dims);
    defer self.alloc.free(vec_data);

    for (node.children, 0..) |child_id, i| {
        var child = try loadNode(self, txn, child_id);
        defer child.deinit(self.alloc);
        if (child.centroid.len > 0) {
            @memcpy(vec_data[i * dims ..][0..dims], child.centroid[0..dims]);
        } else {
            @memset(vec_data[i * dims ..][0..dims], 0);
        }
    }

    var vector_set = vec.Set{
        .dims = dims,
        .count = count,
        .data = vec_data,
    };

    var split = try self.splitVectorSet(&vector_set, node.children);
    defer {
        if (split.c1.len > 0) self.alloc.free(split.c1);
        if (split.g1.len > 0) self.alloc.free(split.g1);
        if (split.c2.len > 0) self.alloc.free(split.c2);
        if (split.g2.len > 0) self.alloc.free(split.g2);
    }

    const n1_id = self.nextNodeId();
    var n1 = types.Node{
        .id = n1_id,
        .is_leaf = false,
        .level = node.level,
        .parent = node.parent,
        .centroid = split.c1,
        .children = split.g1,
        .members = &.{},
    };
    split.c1 = &.{};
    split.g1 = &.{};
    defer n1.deinit(self.alloc);

    const n2_id = self.nextNodeId();
    var n2 = types.Node{
        .id = n2_id,
        .is_leaf = false,
        .level = node.level,
        .parent = node.parent,
        .centroid = split.c2,
        .children = split.g2,
        .members = &.{},
    };
    split.c2 = &.{};
    split.g2 = &.{};
    defer n2.deinit(self.alloc);

    for (n1.children) |child_id| try updateParent(self, txn, child_id, n1_id, nowNsU64Fixed, elapsedSinceU64Fixed);
    for (n2.children) |child_id| try updateParent(self, txn, child_id, n2_id, nowNsU64Fixed, elapsedSinceU64Fixed);
    n1.covering_radius = try computeInternalCoveringRadius(self, txn, &n1);
    n2.covering_radius = try computeInternalCoveringRadius(self, txn, &n2);

    if (node.parent == 0) {
        const new_root_id = self.nextNodeId();
        const root_centroid = try self.alloc.dupe(f32, n1.centroid);
        defer self.alloc.free(root_centroid);
        vec.add(root_centroid, n2.centroid);
        vec.scale(0.5, root_centroid);
        normalizeCentroidForMetric(self, root_centroid);

        n1.parent = new_root_id;
        n2.parent = new_root_id;

        try saveNodeWithOptions(self, txn, &n1, options, now_fn, elapsed_fn);
        try saveNodeWithOptions(self, txn, &n2, options, now_fn, elapsed_fn);

        const root_children = try self.alloc.alloc(u64, 2);
        defer self.alloc.free(root_children);
        root_children[0] = n1_id;
        root_children[1] = n2_id;

        var new_root = types.Node{
            .id = new_root_id,
            .is_leaf = false,
            .level = node.level + 1,
            .parent = 0,
            .centroid = try self.alloc.dupe(f32, root_centroid),
            .children = try self.alloc.dupe(u64, root_children),
            .members = &.{},
        };
        defer new_root.deinit(self.alloc);
        new_root.covering_radius = try computeInternalCoveringRadius(self, txn, &new_root);
        try saveNodeWithOptions(self, txn, &new_root, options, now_fn, elapsed_fn);
        self.metadata.root_node = new_root_id;
    } else {
        try saveNodeWithOptions(self, txn, &n1, options, now_fn, elapsed_fn);
        try saveNodeWithOptions(self, txn, &n2, options, now_fn, elapsed_fn);

        var parent = try loadNode(self, txn, node.parent);
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);

        var new_children = try self.alloc.alloc(u64, parent.children.len + 1);
        var wi: usize = 0;
        for (parent.children) |c| {
            if (c == node.id) {
                new_children[wi] = n1_id;
            } else {
                new_children[wi] = c;
            }
            wi += 1;
        }
        new_children[wi] = n2_id;
        self.alloc.free(parent.children);
        parent.children = new_children;
        try recomputeInternalCentroid(self, txn, &parent);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);

        if (parent.children.len > self.config.branching_factor) {
            try splitInternalWithOptions(self, txn, &parent, options, now_fn, elapsed_fn);
        }
    }
    try deleteNode(self, txn, node.id);
}

pub fn splitLeaf(self: anytype, txn: anytype, leaf: *const types.Node) !void {
    try splitLeafWithOptions(self, txn, leaf, .{}, nowNsI128Fixed, elapsedSinceNsFixed);
}

fn bulkRebuildLeafMinMembers(self: anytype, options: anytype) usize {
    const Options = @TypeOf(options);
    if (comptime @hasField(Options, "bulk_rebuild_leaf_min_members")) {
        if (options.bulk_rebuild_leaf_min_members != 0) return options.bulk_rebuild_leaf_min_members;
    }
    if (comptime @hasField(Options, "bulk_ingest")) {
        if (options.bulk_ingest) {
            const leaf_size: usize = @intCast(self.config.leaf_size);
            return @max(leaf_size * 4, leaf_size + 1);
        }
    }
    return 0;
}

fn shouldBulkRebuildOversizedLeaf(self: anytype, leaf: *const types.Node, options: anytype) bool {
    if (!leaf.is_leaf) return false;
    if (leaf.members.len <= self.config.leaf_size) return false;
    const min_members = bulkRebuildLeafMinMembers(self, options);
    if (min_members == 0) return false;
    return leaf.members.len >= min_members;
}

fn rebuildOversizedLeafAsSubtree(
    self: anytype,
    txn: anytype,
    leaf: *const types.Node,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    const dims = self.config.dims;
    const count = leaf.members.len;

    const matrix_floats = std.math.mul(usize, count, dims) catch return error.BufferTooSmall;
    const matrix_bytes = std.math.mul(usize, matrix_floats, @sizeOf(f32)) catch return error.BufferTooSmall;
    addApplyWorkspaceBytes(self, @intCast(matrix_bytes));
    const matrix = try self.alloc.alloc(f32, matrix_floats);
    defer {
        self.alloc.free(matrix);
        releaseApplyWorkspaceBytes(self, @intCast(matrix_bytes));
    }

    const vector_load_start = now_fn();
    try loadTransformedVectorIdsIntoMatrix(self, txn, leaf.members, matrix, options);
    self.write_profile.split_leaf_vector_load_ns += elapsed_fn(vector_load_start);

    const metadata = try self.alloc.alloc(?[]const u8, count);
    defer self.alloc.free(metadata);
    const lookups = try self.alloc.alloc(FixedKeyLookup, count);
    defer self.alloc.free(lookups);
    const key_views = try self.alloc.alloc([]const u8, count);
    defer self.alloc.free(key_views);
    const values = try self.alloc.alloc(?[]const u8, count);
    defer self.alloc.free(values);
    try getMetadataManySortedInTxnWithScratch(self, txn, leaf.members, metadata, lookups, key_views, values);

    const inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, count);
    defer self.alloc.free(inputs);
    for (leaf.members, 0..) |member_id, i| {
        const transformed = matrix[i * dims ..][0..dims];
        inputs[i] = .{
            .vector_id = member_id,
            .vector = transformed,
            .transformed = transformed,
            .metadata = metadata[i] orelse "",
        };
    }

    const indexes = try self.alloc.alloc(usize, count);
    defer self.alloc.free(indexes);
    for (indexes, 0..) |*index, i| index.* = i;
    const positions = try self.alloc.alloc(u64, count);
    defer self.alloc.free(positions);
    const partitioned_indexes = try self.alloc.alloc(usize, count);
    defer self.alloc.free(partitioned_indexes);
    addApplyWorkspaceBytes(self, @intCast(matrix_bytes));
    const recursive_vec_data = try self.alloc.alloc(f32, matrix_floats);
    defer {
        self.alloc.free(recursive_vec_data);
        releaseApplyWorkspaceBytes(self, @intCast(matrix_bytes));
    }

    var scratch = BulkRecursiveScratch{
        .vec_data = recursive_vec_data,
        .positions = positions,
        .partitioned_indexes = partitioned_indexes,
    };

    var built = try buildBulkSubtreeRecursive(self, txn, inputs, indexes, &scratch, leaf.parent, leaf.level);
    defer built.deinit(self.alloc);

    const finalize_start = now_fn();
    if (leaf.parent == 0) {
        if (leaf.id != self.metadata.root_node) return error.Corrupted;
        try deleteNode(self, txn, leaf.id);
        self.metadata.root_node = built.node_id;
    } else {
        var parent = try loadNode(self, txn, leaf.parent);
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);
        var replaced = false;
        for (parent.children) |*child_id| {
            if (child_id.* == leaf.id) {
                child_id.* = built.node_id;
                replaced = true;
                break;
            }
        }
        if (!replaced) return error.Corrupted;
        try recomputeInternalCentroid(self, txn, &parent);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);
        try deleteNode(self, txn, leaf.id);
        try recomputeAncestorCentroidsWithOptions(self, txn, parent.parent, options);
        _ = try refreshAncestorSplitRangesCounted(self, txn, parent.parent);
    }

    self.write_profile.split_leaf_finalize_ns += elapsed_fn(finalize_start);
    self.write_profile.bulk_leaf_rebuild_calls += 1;
    self.write_profile.bulk_leaf_rebuild_members_total += @intCast(count);
    self.write_profile.bulk_leaf_rebuild_members_max = @max(
        self.write_profile.bulk_leaf_rebuild_members_max,
        @as(u64, @intCast(count)),
    );
}

pub fn splitLeafWithOptions(
    self: anytype,
    txn: anytype,
    leaf: *const types.Node,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try self.bindTxnLike(txn);
    const start = now_fn();
    defer {
        self.write_profile.split_leaf_ns += elapsed_fn(start);
        self.write_profile.split_leaf_calls += 1;
    }
    const dims = self.config.dims;
    const count = leaf.members.len;
    self.write_profile.split_leaf_input_members_total += @intCast(count);
    if (count > self.config.leaf_size) {
        self.write_profile.split_leaf_input_overflow_members_total += @as(u64, @intCast(count - self.config.leaf_size));
    }

    if (shouldBulkRebuildOversizedLeaf(self, leaf, options)) {
        try rebuildOversizedLeafAsSubtree(self, txn, leaf, options, now_fn, elapsed_fn);
        return;
    }

    const matrix_floats = std.math.mul(usize, count, dims) catch return error.BufferTooSmall;
    const matrix_bytes = std.math.mul(usize, matrix_floats, @sizeOf(f32)) catch return error.BufferTooSmall;
    addApplyWorkspaceBytes(self, @intCast(matrix_bytes));
    defer releaseApplyWorkspaceBytes(self, @intCast(matrix_bytes));

    const vec_data = try self.alloc.alloc(f32, matrix_floats);
    defer self.alloc.free(vec_data);
    const vector_load_start = now_fn();
    var used_cached_nonquant = false;
    var quantized_handle = try loadQuantizedReadHandle(
        self,
        txn,
        leaf.id,
        usesNonQuantizedPayload(leaf),
        count,
        isNotFoundGeneric,
    );
    defer if (quantized_handle) |*handle| handle.deinit(self.alloc);
    if (quantized_handle) |*handle| {
        switch (handle.ptr().*) {
            .nonquant => |*set| {
                if (set.vectors.dims == dims and set.vectors.count == count and set.vectors.data.len >= count * dims) {
                    @memcpy(vec_data, set.vectors.data[0 .. count * dims]);
                    used_cached_nonquant = true;
                }
            },
            .rabit => {},
        }
    }
    if (!used_cached_nonquant) {
        try loadTransformedVectorIdsIntoMatrix(self, txn, leaf.members, vec_data, options);
    }
    self.write_profile.split_leaf_vector_load_ns += elapsed_fn(vector_load_start);

    var vector_set = vec.Set{
        .dims = dims,
        .count = count,
        .data = vec_data,
    };

    const partition_start = now_fn();
    var split = try self.splitVectorSet(&vector_set, leaf.members);
    if (self.config.prefer_key_local_leaf_splits) {
        if (try self.maybeBuildKeyLocalLeafSplit(txn, leaf.members, &vector_set, &split)) |replacement| {
            self.alloc.free(split.c1);
            self.alloc.free(split.g1);
            self.alloc.free(split.c2);
            self.alloc.free(split.g2);
            split = replacement;
        }
    }
    self.write_profile.split_leaf_partition_ns += elapsed_fn(partition_start);
    const partition_workspace_bytes =
        @as(u64, @intCast((split.c1.len + split.c2.len) * @sizeOf(f32))) +
        @as(u64, @intCast((split.g1.len + split.g2.len) * @sizeOf(u64)));
    addApplyWorkspaceBytes(self, partition_workspace_bytes);
    defer {
        releaseApplyWorkspaceBytes(self, partition_workspace_bytes);
        self.alloc.free(split.c1);
        self.alloc.free(split.g1);
        self.alloc.free(split.c2);
        self.alloc.free(split.g2);
    }

    const finalize_start = now_fn();
    const leaf_id = leaf.id;
    const leaf_parent = leaf.parent;
    const leaf_level = leaf.level;
    const splitting_root = leaf_parent == 0 and leaf_id == self.metadata.root_node;

    const left_id = leaf_id;
    var left_node = types.Node{
        .id = left_id,
        .is_leaf = true,
        .level = leaf_level,
        .parent = leaf_parent,
        .centroid = split.c1,
        .children = &.{},
        .members = split.g1,
    };
    split.c1 = &.{};
    split.g1 = &.{};
    defer left_node.deinit(self.alloc);

    const right_id = self.nextNodeId();
    var right_node = types.Node{
        .id = right_id,
        .is_leaf = true,
        .level = leaf_level,
        .parent = leaf_parent,
        .centroid = split.c2,
        .children = &.{},
        .members = split.g2,
    };
    split.c2 = &.{};
    split.g2 = &.{};
    defer right_node.deinit(self.alloc);

    const publish_known_quantized_now = !(deferQuantizedRebuild(options) and shouldDeferQuantizedRebuildToBulkFinish(self, options));
    var left_vectors: []f32 = &.{};
    defer if (left_vectors.len > 0) self.alloc.free(left_vectors);
    var right_vectors: []f32 = &.{};
    defer if (right_vectors.len > 0) self.alloc.free(right_vectors);
    if (publish_known_quantized_now or self.config.metric == .l2_squared) {
        left_vectors = try copyNodeMemberVectorsFromSource(self, &left_node, leaf.members, vec_data);
        right_vectors = try copyNodeMemberVectorsFromSource(self, &right_node, leaf.members, vec_data);
    }
    if (self.config.metric == .l2_squared) {
        left_node.covering_radius = l2CoveringRadiusForMatrix(left_node.centroid, left_vectors, left_node.members.len);
        right_node.covering_radius = l2CoveringRadiusForMatrix(right_node.centroid, right_vectors, right_node.members.len);
    }

    if (splitting_root) {
        const new_root_id = self.nextNodeId();
        const root_centroid = try self.alloc.dupe(f32, left_node.centroid);
        defer self.alloc.free(root_centroid);
        vec.add(root_centroid, right_node.centroid);
        vec.scale(0.5, root_centroid);
        normalizeCentroidForMetric(self, root_centroid);

        left_node.parent = new_root_id;
        left_node.level = leaf_level + 1;
        right_node.parent = new_root_id;
        right_node.level = leaf_level + 1;

        if (publish_known_quantized_now) {
            try saveLeafNodeWithKnownVectors(self, txn, &left_node, left_vectors, now_fn, elapsed_fn);
            try saveLeafNodeWithKnownVectors(self, txn, &right_node, right_vectors, now_fn, elapsed_fn);
        } else {
            try saveNodeWithOptions(self, txn, &left_node, options, now_fn, elapsed_fn);
            try saveNodeWithOptions(self, txn, &right_node, options, now_fn, elapsed_fn);
        }

        const root_children = try self.alloc.alloc(u64, 2);
        defer self.alloc.free(root_children);
        root_children[0] = left_id;
        root_children[1] = right_id;

        var new_root = types.Node{
            .id = new_root_id,
            .is_leaf = false,
            .level = leaf_level,
            .parent = 0,
            .centroid = try self.alloc.dupe(f32, root_centroid),
            .children = try self.alloc.dupe(u64, root_children),
            .members = &.{},
        };
        defer new_root.deinit(self.alloc);
        new_root.covering_radius = try computeInternalCoveringRadius(self, txn, &new_root);
        try saveNodeWithOptions(self, txn, &new_root, options, now_fn, elapsed_fn);
        self.metadata.root_node = new_root_id;

        for (right_node.members) |vid| try self.putVecLeaf(txn, vid, right_id);
    } else if (leaf_parent != 0) {
        if (publish_known_quantized_now) {
            try saveLeafNodeWithKnownVectors(self, txn, &left_node, left_vectors, now_fn, elapsed_fn);
            try saveLeafNodeWithKnownVectors(self, txn, &right_node, right_vectors, now_fn, elapsed_fn);
        } else {
            try self.saveNodeWithOptionsMode(txn, &left_node, options, false);
            try saveNodeWithOptions(self, txn, &right_node, options, now_fn, elapsed_fn);
        }

        for (right_node.members) |vid| try self.putVecLeaf(txn, vid, right_id);

        var parent = try loadNode(self, txn, leaf_parent);
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);
        const previous_child_count = parent.children.len;

        var new_children = try self.alloc.alloc(u64, parent.children.len + 1);
        var wi: usize = 0;
        for (parent.children) |c| {
            new_children[wi] = c;
            wi += 1;
        }
        new_children[wi] = right_id;
        self.alloc.free(parent.children);
        parent.children = new_children;
        try updateInternalCentroidForLeafSplit(self, &parent, leaf.centroid, left_node.centroid, right_node.centroid, previous_child_count);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);

        if (parent.children.len > self.config.branching_factor) {
            try splitInternalWithOptions(self, txn, &parent, options, now_fn, elapsed_fn);
        }
    }
    self.write_profile.split_leaf_finalize_ns += elapsed_fn(finalize_start);
}

pub fn rebuildOversizedLeafKmeansWithOptions(
    self: anytype,
    txn: anytype,
    leaf: *const types.Node,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !bool {
    try self.bindTxnLike(txn);
    if (!leaf.is_leaf) return false;
    if (leaf.members.len <= self.config.leaf_size) return false;
    if (self.config.prefer_key_local_leaf_splits) return false;

    const leaf_size = @max(@as(usize, 1), self.config.leaf_size);
    const replacement_count = std.math.divCeil(usize, leaf.members.len, leaf_size) catch unreachable;
    if (replacement_count < 3) return false;

    const start = now_fn();
    defer {
        self.write_profile.split_leaf_ns += elapsed_fn(start);
        self.write_profile.split_leaf_calls += 1;
    }
    self.write_profile.split_leaf_input_members_total += @intCast(leaf.members.len);
    self.write_profile.split_leaf_input_overflow_members_total += @as(u64, @intCast(leaf.members.len - leaf_size));

    const dims: usize = @intCast(self.config.dims);
    const dense_vectors = try self.alloc.alloc(f32, leaf.members.len * dims);
    defer self.alloc.free(dense_vectors);
    const points = try self.alloc.alloc(kmeans.Point, leaf.members.len);
    defer self.alloc.free(points);
    const inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, leaf.members.len);
    defer self.alloc.free(inputs);
    const vector_scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(vector_scratch);
    const transformed_scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(transformed_scratch);

    const vector_load_start = now_fn();
    var used_cached_nonquant = false;
    var quantized_handle = try loadQuantizedReadHandle(
        self,
        txn,
        leaf.id,
        usesNonQuantizedPayload(leaf),
        leaf.members.len,
        isNotFoundGeneric,
    );
    defer if (quantized_handle) |*handle| handle.deinit(self.alloc);
    if (quantized_handle) |*handle| {
        switch (handle.ptr().*) {
            .nonquant => |*set| {
                if (set.vectors.dims == dims and set.vectors.count == leaf.members.len and set.vectors.data.len >= leaf.members.len * dims) {
                    @memcpy(dense_vectors, set.vectors.data[0 .. leaf.members.len * dims]);
                    used_cached_nonquant = true;
                }
            },
            .rabit => {},
        }
    }
    if (!used_cached_nonquant) {
        for (leaf.members, 0..) |member_id, i| {
            const vector_slot = dense_vectors[i * dims ..][0..dims];
            const raw = try self.getVectorViewOrScratch(txn, member_id, vector_scratch);
            const transformed = self.transformVector(raw, transformed_scratch);
            @memcpy(vector_slot, transformed);
        }
    }
    for (leaf.members, 0..) |member_id, i| {
        const vector_slot = dense_vectors[i * dims ..][0..dims];
        points[i] = .{
            .stable_id = member_id,
            .vector = vector_slot,
            .weight = 1,
        };
        inputs[i] = .{
            .vector_id = member_id,
            .vector = vector_slot,
            .transformed = vector_slot,
            .metadata = "",
        };
    }
    self.write_profile.split_leaf_vector_load_ns += elapsed_fn(vector_load_start);

    const assignments = try self.alloc.alloc(usize, leaf.members.len);
    defer self.alloc.free(assignments);
    const distances = try self.alloc.alloc(f32, leaf.members.len);
    defer self.alloc.free(distances);
    const counts = try self.alloc.alloc(usize, replacement_count);
    defer self.alloc.free(counts);
    const centroids = try self.alloc.alloc(f32, replacement_count * dims);
    defer self.alloc.free(centroids);
    const next_centroids = try self.alloc.alloc(f32, replacement_count * dims);
    defer self.alloc.free(next_centroids);
    const entries = try self.alloc.alloc(kmeans.Entry, leaf.members.len);
    defer self.alloc.free(entries);

    const partition_start = now_fn();
    const stats = try kmeans.run(.{
        .dims = dims,
        .metric = self.config.metric,
        .max_iter = self.config.kmeans_max_iter,
        .backend = self.config.kmeans_backend,
        .update_strategy = self.config.kmeans_update_strategy,
        .dense_vectors = dense_vectors,
    }, points, self.rng.intN(leaf.members.len), centroids, next_centroids, assignments, distances, counts, entries);
    recordKmeansRunStats(self, stats);
    self.write_profile.split_leaf_partition_ns += elapsed_fn(partition_start);

    const finalize_start = now_fn();
    var replacement_ids = std.ArrayListUnmanaged(u64).empty;
    defer replacement_ids.deinit(self.alloc);
    try replacement_ids.ensureTotalCapacity(self.alloc, replacement_count);

    const splitting_root = leaf.parent == 0 and leaf.id == self.metadata.root_node;
    const new_root_id = if (splitting_root) self.nextNodeId() else 0;
    const replacement_parent = if (splitting_root) new_root_id else leaf.parent;
    const replacement_level = if (splitting_root) leaf.level + 1 else leaf.level;
    const publish_known_quantized_now = !(deferQuantizedRebuild(options) and shouldDeferQuantizedRebuildToBulkFinish(self, options));
    var first_replacement = true;

    var cluster_start: usize = 0;
    while (cluster_start < entries.len) {
        const cluster = entries[cluster_start].cluster;
        var cluster_end = cluster_start + 1;
        while (cluster_end < entries.len and entries[cluster_end].cluster == cluster) : (cluster_end += 1) {}

        const cluster_len = cluster_end - cluster_start;
        const leaf_groups = try bulk_build.planBalancedGroupSizes(self.alloc, cluster_len, leaf_size);
        errdefer self.alloc.free(leaf_groups);

        var entry_cursor = cluster_start;
        for (leaf_groups) |group_size| {
            const node_id = if (first_replacement) leaf.id else self.nextNodeId();
            first_replacement = false;
            try replacement_ids.append(self.alloc, node_id);

            const centroid = try self.alloc.alloc(f32, dims);
            errdefer self.alloc.free(centroid);
            @memset(centroid, 0);

            var members = try self.alloc.alloc(u64, group_size);
            errdefer self.alloc.free(members);
            var group_vectors = try self.alloc.alloc(f32, group_size * dims);
            defer self.alloc.free(group_vectors);

            for (0..group_size) |i| {
                const input = inputs[entries[entry_cursor + i].point_index];
                members[i] = input.vector_id;
                vec.add(centroid, input.transformed);
                @memcpy(group_vectors[i * dims ..][0..dims], input.transformed);
                try self.putVecLeaf(txn, input.vector_id, node_id);
            }
            vec.scale(1.0 / @as(f32, @floatFromInt(group_size)), centroid);
            normalizeCentroidForMetric(self, centroid);

            var node = types.Node{
                .id = node_id,
                .is_leaf = true,
                .level = replacement_level,
                .parent = replacement_parent,
                .centroid = centroid,
                .children = &.{},
                .members = members,
            };
            defer node.deinit(self.alloc);
            node.covering_radius = if (self.config.metric == .l2_squared)
                l2CoveringRadiusForMatrix(node.centroid, group_vectors, node.members.len)
            else
                std.math.nan(f32);
            if (publish_known_quantized_now) {
                try saveLeafNodeWithKnownVectors(self, txn, &node, group_vectors, now_fn, elapsed_fn);
            } else {
                try saveNodeWithOptions(self, txn, &node, options, now_fn, elapsed_fn);
            }

            entry_cursor += group_size;
        }
        self.alloc.free(leaf_groups);
        cluster_start = cluster_end;
    }

    if (splitting_root) {
        const child_ids = try self.alloc.dupe(u64, replacement_ids.items);
        errdefer self.alloc.free(child_ids);
        const centroid = try self.alloc.alloc(f32, dims);
        errdefer self.alloc.free(centroid);
        @memset(centroid, 0);
        for (inputs) |input| vec.add(centroid, input.transformed);
        vec.scale(1.0 / @as(f32, @floatFromInt(leaf.members.len)), centroid);
        normalizeCentroidForMetric(self, centroid);

        var root = types.Node{
            .id = new_root_id,
            .is_leaf = false,
            .level = leaf.level,
            .parent = 0,
            .centroid = centroid,
            .children = child_ids,
            .members = &.{},
        };
        defer root.deinit(self.alloc);
        root.covering_radius = try computeInternalCoveringRadius(self, txn, &root);
        try saveNodeWithOptions(self, txn, &root, options, now_fn, elapsed_fn);
        self.metadata.root_node = new_root_id;
    } else {
        var parent = try loadNode(self, txn, leaf.parent);
        defer parent.deinit(self.alloc);
        try parent.ensureUnbacked(self.alloc);

        const old_children = parent.children;
        const new_children = try self.alloc.alloc(u64, old_children.len + replacement_ids.items.len - 1);
        var wi: usize = 0;
        var replaced = false;
        for (old_children) |child_id| {
            if (child_id == leaf.id) {
                for (replacement_ids.items) |replacement_id| {
                    new_children[wi] = replacement_id;
                    wi += 1;
                }
                replaced = true;
            } else {
                new_children[wi] = child_id;
                wi += 1;
            }
        }
        if (!replaced) {
            self.alloc.free(new_children);
            return error.Corrupted;
        }
        self.alloc.free(parent.children);
        parent.children = new_children;
        try recomputeInternalCentroid(self, txn, &parent);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);

        if (parent.children.len > self.config.branching_factor) {
            try splitInternalWithOptions(self, txn, &parent, options, now_fn, elapsed_fn);
        }
    }
    self.write_profile.split_leaf_finalize_ns += elapsed_fn(finalize_start);
    return true;
}

pub fn batchInsertWithMetadata(self: anytype, items: []const hbc_runtime.BatchInsertItem, now_fn: fn () u64, elapsed_fn: fn (u64) u64) !void {
    try batchInsertWithMetadataOptions(self, items, .{}, now_fn, elapsed_fn);
}

pub fn batchApply(
    self: anytype,
    writes: []const hbc_runtime.BatchInsertItem,
    deletes: []const u64,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    try batchApplyOptions(self, writes, deletes, .{}, now_fn, elapsed_fn);
}

pub fn batchApplyOptions(
    self: anytype,
    writes: []const hbc_runtime.BatchInsertItem,
    deletes: []const u64,
    options: hbc_runtime.BatchInsertOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (writes.len == 0 and deletes.len == 0) return;
    if (writes.len == 0) {
        if (deletes.len == 1) return self.delete(deletes[0]);

        const Index = comptime childType(@TypeOf(self));
        const publishing = try beginPublishSearchStateIfSupported(self);
        errdefer abortPublishSearchStateIfSupported(self, publishing);
        var batch = if (options.bulk_ingest and comptime @hasDecl(Index, "beginRuntimeBatchTxnOptions"))
            try self.beginRuntimeBatchTxnOptions(options)
        else
            try self.beginRuntimeBatchTxn();
        errdefer batch.abort();
        errdefer abortVectorCacheMutationsIfSupported(self);
        try batchDeleteTxn(self, &batch, deletes);
        try finalizeWriteTxnOptions(self, &batch, options, now_fn, elapsed_fn);
        const commit_start = now_fn();
        try markPublishSearchStateCommittingIfSupported(self, publishing);
        try batch.commit();
        self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
        try finishPublishSearchStateIfSupported(self, publishing);
        return;
    }
    if (deletes.len == 0) return batchInsertWithMetadataOptions(self, writes, options, now_fn, elapsed_fn);

    const Index = comptime childType(@TypeOf(self));
    const publishing = try beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    var batch = if (options.bulk_ingest and comptime @hasDecl(Index, "beginRuntimeBatchTxnOptions"))
        try self.beginRuntimeBatchTxnOptions(options)
    else
        try self.beginRuntimeBatchTxn();
    errdefer batch.abort();
    errdefer abortVectorCacheMutationsIfSupported(self);

    try batchDeleteTxn(self, &batch, deletes);

    var insert_options = options;
    if (!insert_options.assume_absent_ids and
        try batchWritesAreUniqueAndCoveredByDeletes(self.alloc, writes, deletes))
    {
        insert_options.assume_absent_ids = true;
    }
    const grouped = if (writes.len > 1 and insert_options.assume_absent_ids and insert_options.coalesce_leaf_writes)
        try batchInsertAssumeAbsentGroupedTxnOptions(self, &batch, writes, insert_options, now_fn, elapsed_fn)
    else
        false;
    if (!grouped) try batchInsertWithMetadataTxnOptions(self, &batch, writes, insert_options);
    try finalizeWriteTxnOptions(self, &batch, insert_options, now_fn, elapsed_fn);
    const commit_start = now_fn();
    try markPublishSearchStateCommittingIfSupported(self, publishing);
    try batch.commit();
    self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
    try finishPublishSearchStateIfSupported(self, publishing);
    seedRetainedVectorsAfterCommit(self, writes, insert_options);
}

pub fn batchInsertWithMetadataOptions(
    self: anytype,
    items: []const hbc_runtime.BatchInsertItem,
    options: hbc_runtime.BatchInsertOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (items.len == 0) return;

    if (items.len > 1) {
        const Index = comptime childType(@TypeOf(self));
        const publishing = try beginPublishSearchStateIfSupported(self);
        errdefer abortPublishSearchStateIfSupported(self, publishing);
        var batch = if (options.bulk_ingest and comptime @hasDecl(Index, "beginRuntimeBatchTxnOptions"))
            try self.beginRuntimeBatchTxnOptions(options)
        else
            try self.beginRuntimeBatchTxn();
        errdefer batch.abort();
        errdefer abortVectorCacheMutationsIfSupported(self);
        const grouped = if (options.coalesce_leaf_writes)
            try batchInsertAssumeAbsentGroupedTxnOptions(self, &batch, items, options, now_fn, elapsed_fn)
        else
            false;
        if (!grouped) try batchInsertWithMetadataTxnOptions(self, &batch, items, options);
        try finalizeWriteTxnOptions(self, &batch, options, now_fn, elapsed_fn);
        const commit_start = now_fn();
        try markPublishSearchStateCommittingIfSupported(self, publishing);
        try batch.commit();
        self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
        try finishPublishSearchStateIfSupported(self, publishing);
        seedRetainedVectorsAfterCommit(self, items, options);
    } else {
        const publishing = try beginPublishSearchStateIfSupported(self);
        errdefer abortPublishSearchStateIfSupported(self, publishing);
        var txn = try self.beginRuntimeWriteTxn();
        errdefer txn.abort();
        errdefer abortVectorCacheMutationsIfSupported(self);
        try batchInsertWithMetadataTxnOptions(self, &txn, items, options);
        try finalizeWriteTxnOptions(self, &txn, options, now_fn, elapsed_fn);
        const commit_start = now_fn();
        try markPublishSearchStateCommittingIfSupported(self, publishing);
        try txn.commit();
        self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
        try finishPublishSearchStateIfSupported(self, publishing);
        seedRetainedVectorsAfterCommit(self, items, options);
    }
}

const PreparedBatchInsert = struct {
    item_index: usize,
    leaf_id: u64,
};

const BatchRouteProfile = struct {
    internal_nodes: u64 = 0,
    leaf_groups: u64 = 0,
    routed_items: u64 = 0,
    quantized_nodes: u64 = 0,
    exact_child_scores: u64 = 0,
    fallback_nodes: u64 = 0,
};

fn lessPreparedBatchInsert(_: void, lhs: PreparedBatchInsert, rhs: PreparedBatchInsert) bool {
    return if (lhs.leaf_id == rhs.leaf_id)
        lhs.item_index < rhs.item_index
    else
        lhs.leaf_id < rhs.leaf_id;
}

fn lessBatchInsertItemVectorId(items: []const hbc_runtime.BatchInsertItem, lhs: usize, rhs: usize) bool {
    return items[lhs].vector_id < items[rhs].vector_id;
}

fn batchWritesAreUniqueAndCoveredByDeletes(
    alloc: Allocator,
    writes: []const hbc_runtime.BatchInsertItem,
    deletes: []const u64,
) !bool {
    if (writes.len == 0) return true;
    if (deletes.len == 0) return false;

    if (writes.len <= 16 and deletes.len <= 64) {
        for (writes, 0..) |item, i| {
            for (writes[0..i]) |previous| {
                if (previous.vector_id == item.vector_id) return false;
            }
            var found = false;
            for (deletes) |delete_id| {
                if (delete_id == item.vector_id) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    var deleted = std.AutoHashMapUnmanaged(u64, void).empty;
    defer deleted.deinit(alloc);
    try deleted.ensureTotalCapacity(alloc, @intCast(deletes.len));
    for (deletes) |delete_id| {
        try deleted.put(alloc, delete_id, {});
    }

    var seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen.deinit(alloc);
    try seen.ensureTotalCapacity(alloc, @intCast(writes.len));
    for (writes) |item| {
        if (!deleted.contains(item.vector_id)) return false;
        if (seen.contains(item.vector_id)) return false;
        try seen.put(alloc, item.vector_id, {});
    }
    return true;
}

fn appendUniqueU64(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(u64), value: u64) !void {
    if (value == 0) return;
    for (list.items) |existing| {
        if (existing == value) return;
    }
    try list.append(alloc, value);
}

fn queryMeasureForMetric(metric: vec.DistanceMetric, query: []const f32) f32 {
    return switch (metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(query),
        .inner_product => 0,
    };
}

fn chooseInsertChildForVector(
    self: anytype,
    child_ids: []const u64,
    child_nodes: []const *const types.Node,
    query: []const f32,
    query_measure: f32,
    profile: *BatchRouteProfile,
) !u64 {
    var best_child: u64 = 0;
    var best_dist: f32 = std.math.inf(f32);
    for (child_ids, 0..) |child_id, child_index| {
        const child = child_nodes[child_index];
        if (child.centroid.len != query.len) continue;
        profile.exact_child_scores += 1;
        const dist = vec.distanceToQuery(query, query_measure, child.centroid, self.config.metric);
        if (dist < best_dist) {
            best_dist = dist;
            best_child = child_id;
        }
    }
    if (best_child == 0) return error.Corrupted;
    return best_child;
}

fn chooseInsertChildForVectorQuantized(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    child_ids: []const u64,
    query: []const f32,
    query_measure: f32,
    allow_quantized: bool,
    scratch: anytype,
    profile: *BatchRouteProfile,
) !?u64 {
    if (!allow_quantized or !self.config.use_quantization) return null;
    if (child_ids.len == 0) return null;
    if (comptime @hasDecl(@TypeOf(scratch.*), "ensureCapacity")) {
        try scratch.ensureCapacity(self.alloc, child_ids.len);
    }
    if (scratch.distances.len < child_ids.len or scratch.error_bounds.len < child_ids.len) return null;

    var quantized_handle = (try loadQuantizedReadHandle(
        self,
        txn,
        node.id,
        usesNonQuantizedPayload(node),
        child_ids.len,
        isNotFoundGeneric,
    )) orelse {
        profile.fallback_nodes += 1;
        return null;
    };
    defer quantized_handle.deinit(self.alloc);

    const distances = scratch.distances[0..child_ids.len];
    const error_bounds = scratch.error_bounds[0..child_ids.len];
    self.estimateQuantizedDistances(quantized_handle.ptr(), query, query_measure, distances, error_bounds, &scratch.estimate) catch {
        self.invalidateQuantizedCache(node.id);
        profile.fallback_nodes += 1;
        return null;
    };

    var best_child: u64 = 0;
    var best_dist: f32 = std.math.inf(f32);
    for (child_ids, 0..) |child_id, i| {
        if (distances[i] < best_dist) {
            best_dist = distances[i];
            best_child = child_id;
        }
    }
    if (best_child == 0) return null;
    profile.quantized_nodes += 1;
    return best_child;
}

fn routeBatchNodeToLeaves(
    self: anytype,
    txn: anytype,
    node_id: u64,
    transformed_data: []const f32,
    query_measures: []const f32,
    current_item_indexes: []usize,
    next_item_indexes: []usize,
    choices: []usize,
    allow_quantized: bool,
    prepared: []PreparedBatchInsert,
    prepared_count: *usize,
    scratch: anytype,
    profile: *BatchRouteProfile,
) !void {
    if (current_item_indexes.len == 0) return;
    var node_handle = try loadMutationNodeReadHandle(self, txn, node_id);
    defer node_handle.deinit(self.alloc);
    const node = node_handle.ptr();
    if (node.is_leaf or node.children.len == 0) {
        for (current_item_indexes) |item_index| {
            prepared[prepared_count.*] = .{ .item_index = item_index, .leaf_id = node_id };
            prepared_count.* += 1;
        }
        profile.leaf_groups += 1;
        profile.routed_items += @intCast(current_item_indexes.len);
        return;
    }

    profile.internal_nodes += 1;
    const child_ids = try self.alloc.dupe(u64, node.children);
    defer self.alloc.free(child_ids);

    const NodeReadHandle = CachedNodeReadHandle(@TypeOf(self));
    const child_handles = try self.alloc.alloc(NodeReadHandle, child_ids.len);
    defer self.alloc.free(child_handles);
    var child_handle_count: usize = 0;
    defer {
        for (child_handles[0..child_handle_count]) |*handle| {
            handle.deinit(self.alloc);
        }
    }

    const child_nodes = try self.alloc.alloc(*const types.Node, child_ids.len);
    defer self.alloc.free(child_nodes);
    for (child_ids, 0..) |child_id, child_index| {
        child_handles[child_index] = try loadMutationNodeReadHandle(self, txn, child_id);
        child_handle_count += 1;
        child_nodes[child_index] = child_handles[child_index].ptr();
    }

    const dims: usize = @intCast(self.config.dims);
    for (current_item_indexes, 0..) |item_index, local_index| {
        const transformed = transformed_data[item_index * dims ..][0..dims];
        const child_id = (try chooseInsertChildForVectorQuantized(
            self,
            txn,
            node,
            child_ids,
            transformed,
            query_measures[item_index],
            allow_quantized,
            scratch,
            profile,
        )) orelse try chooseInsertChildForVector(
            self,
            child_ids,
            child_nodes,
            transformed,
            query_measures[item_index],
            profile,
        );
        choices[local_index] = std.mem.indexOfScalar(u64, child_ids, child_id) orelse return error.Corrupted;
    }

    const starts = try self.alloc.alloc(usize, child_ids.len);
    defer self.alloc.free(starts);
    const ends = try self.alloc.alloc(usize, child_ids.len);
    defer self.alloc.free(ends);

    var write_index: usize = 0;
    for (child_ids, 0..) |_, child_index| {
        starts[child_index] = write_index;
        for (current_item_indexes, 0..) |item_index, local_index| {
            if (choices[local_index] != child_index) continue;
            if (write_index >= next_item_indexes.len) return error.Corrupted;
            next_item_indexes[write_index] = item_index;
            write_index += 1;
        }
        ends[child_index] = write_index;
    }

    for (child_ids, 0..) |child_id, child_index| {
        const start = starts[child_index];
        const end = ends[child_index];
        if (start == end) continue;
        try routeBatchNodeToLeaves(
            self,
            txn,
            child_id,
            transformed_data,
            query_measures,
            next_item_indexes[start..end],
            current_item_indexes[0 .. end - start],
            choices[0 .. end - start],
            allow_quantized,
            prepared,
            prepared_count,
            scratch,
            profile,
        );
    }
}

fn routeBatchInsertsToLeaves(
    self: anytype,
    txn: anytype,
    transformed_data: []const f32,
    item_count: usize,
    allow_quantized: bool,
    prepared: []PreparedBatchInsert,
    scratch: anytype,
) !BatchRouteProfile {
    var profile = BatchRouteProfile{};
    if (item_count == 0) return profile;

    const current = try self.alloc.alloc(usize, item_count);
    defer self.alloc.free(current);
    const next = try self.alloc.alloc(usize, item_count);
    defer self.alloc.free(next);
    const choices = try self.alloc.alloc(usize, item_count);
    defer self.alloc.free(choices);
    const query_measures = try self.alloc.alloc(f32, item_count);
    defer self.alloc.free(query_measures);

    const dims: usize = @intCast(self.config.dims);
    for (current, 0..) |*slot, i| {
        slot.* = i;
        const transformed = transformed_data[i * dims ..][0..dims];
        query_measures[i] = queryMeasureForMetric(self.config.metric, transformed);
    }

    var prepared_count: usize = 0;
    try routeBatchNodeToLeaves(
        self,
        txn,
        self.metadata.root_node,
        transformed_data,
        query_measures,
        current,
        next,
        choices,
        allow_quantized,
        prepared,
        &prepared_count,
        scratch,
        &profile,
    );
    if (prepared_count != item_count) return error.Corrupted;
    return profile;
}

fn batchInsertAssumeAbsentGroupedTxnOptions(
    self: anytype,
    txn: anytype,
    items: []const hbc_runtime.BatchInsertItem,
    options: hbc_runtime.BatchInsertOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !bool {
    if (!options.assume_absent_ids or items.len < 2) return false;

    try self.bindTxnLike(txn);

    const dims = self.config.dims;
    var transformed_data = try self.alloc.alloc(f32, items.len * dims);
    defer self.alloc.free(transformed_data);
    var prepared = try self.alloc.alloc(PreparedBatchInsert, items.len);
    defer self.alloc.free(prepared);

    var transform_ns: u64 = 0;
    var find_leaf_ns: u64 = 0;
    var store_vector_ns: u64 = 0;
    const allow_quantized_routing = if (@hasField(@TypeOf(options), "allow_quantized_routing"))
        options.allow_quantized_routing
    else
        !options.centroid_only_routing;
    for (items, 0..) |item, i| {
        const transformed = transformed_data[i * dims ..][0..dims];
        const transform_start = now_fn();
        if (item.transformed) |existing| {
            @memcpy(transformed, existing);
        } else {
            _ = self.transformVector(item.vector, transformed);
        }
        transform_ns += elapsed_fn(transform_start);
    }

    var routing_handle = try self.acquireRoutingScratch();
    defer self.releaseRoutingScratch(&routing_handle);
    const find_start = now_fn();
    const route_profile = try routeBatchInsertsToLeaves(
        self,
        txn,
        transformed_data,
        items.len,
        allow_quantized_routing,
        prepared,
        &routing_handle.scratch,
    );
    find_leaf_ns += elapsed_fn(find_start);

    const sorted_item_indexes = try self.alloc.alloc(usize, items.len);
    defer self.alloc.free(sorted_item_indexes);
    for (sorted_item_indexes, 0..) |*idx, i| idx.* = i;
    std.mem.sort(usize, sorted_item_indexes, items, lessBatchInsertItemVectorId);

    const store_start = now_fn();
    for (sorted_item_indexes) |item_idx| {
        self.invalidateVectorCache(items[item_idx].vector_id);
    }
    for (sorted_item_indexes) |item_idx| {
        const item = items[item_idx];
        if (item.metadata.len == 0) continue;
        var key_buf: [10]u8 = undefined;
        try self.appendNamespaced(txn, .vecs, hbc.encodeVecMetaKey(&key_buf, item.vector_id), item.metadata);
    }
    if (!options.skip_vector_store) {
        for (sorted_item_indexes) |item_idx| {
            const item = items[item_idx];
            var key_buf: [10]u8 = undefined;
            try self.appendNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, item.vector_id), std.mem.sliceAsBytes(item.vector));
        }
    } else if (shouldSeedRetainedVectorCacheOnSkipStore(self)) {
        for (sorted_item_indexes) |item_idx| {
            const item = items[item_idx];
            _ = self.cacheVector(item.vector_id, item.vector) catch {};
        }
    }
    store_vector_ns += elapsed_fn(store_start);

    if (prepared.len > 1) std.mem.sort(PreparedBatchInsert, prepared, {}, lessPreparedBatchInsert);

    const fallback_transformed = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(fallback_transformed);
    var mutate_leaf_ns: u64 = 0;
    var grouped_items: usize = 0;
    const centroid_sum = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(centroid_sum);
    var split_candidates = std.ArrayListUnmanaged(u64).empty;
    defer split_candidates.deinit(self.alloc);
    var ancestor_range_refreshes = std.ArrayListUnmanaged(u64).empty;
    defer ancestor_range_refreshes.deinit(self.alloc);
    var fallback_items: usize = 0;
    var grouped_leaf_groups: usize = 0;
    var grouped_split_candidates: usize = 0;
    var grouped_recursive_splits: usize = 0;
    var grouped_split_scan_iterations: usize = 0;
    var grouped_split_queue_peak: usize = 0;
    var grouped_leaf_range_writes: usize = 0;
    var grouped_ancestor_range_refreshes: usize = 0;
    var grouped_ancestor_range_nodes: usize = 0;
    var grouped_node_body_writes: usize = 0;
    var grouped_vec_leaf_writes: usize = 0;
    var deferred_vec_leaf_mappings = std.ArrayListUnmanaged(DeferredVecLeafMapping).empty;
    defer deferred_vec_leaf_mappings.deinit(self.alloc);

    var group_start: usize = 0;
    while (group_start < prepared.len) {
        var group_end = group_start + 1;
        while (group_end < prepared.len and prepared[group_end].leaf_id == prepared[group_start].leaf_id) : (group_end += 1) {}

        var leaf = try loadNode(self, txn, prepared[group_start].leaf_id);
        defer leaf.deinit(self.alloc);
        const group_len = group_end - group_start;
        const post_group_member_count = leaf.members.len + group_len;
        const max_batched_overflow_members: usize = @max(@as(usize, self.config.leaf_size) * 4, @as(usize, self.config.leaf_size) + 1);
        if (group_len < 2 or !leaf.is_leaf or post_group_member_count > max_batched_overflow_members) {
            for (prepared[group_start..group_end]) |entry| {
                const item = items[entry.item_index];
                const transformed = transformed_data[entry.item_index * dims ..][0..dims];
                var fallback_options = options;
                fallback_options.skip_vector_store = true;
                try self.insertWithMetadataTxnOptions(txn, item.vector_id, item.vector, transformed, item.metadata, fallback_transformed, fallback_options);
            }
            fallback_items += group_len;
            group_start = group_end;
            continue;
        }

        const mutate_start = now_fn();
        try leaf.ensureUnbacked(self.alloc);

        const added_member_ids = try self.alloc.alloc(u64, group_len);
        defer self.alloc.free(added_member_ids);
        for (prepared[group_start..group_end], 0..) |entry, j| {
            added_member_ids[j] = items[entry.item_index].vector_id;
        }
        const old_len = try posting.PostingStore.appendMembers(self.alloc, &leaf, added_member_ids);

        const added_vectors = try self.alloc.alloc(f32, group_len * dims);
        defer self.alloc.free(added_vectors);
        var updated_range: ?types.NodeSplitRange = null;
        defer if (updated_range) |*range| range.deinit(self.alloc);
        var range_changed = false;
        @memset(centroid_sum, 0);
        for (prepared[group_start..group_end], 0..) |entry, j| {
            const item = items[entry.item_index];
            const transformed = transformed_data[entry.item_index * dims ..][0..dims];
            @memcpy(added_vectors[j * dims ..][0..dims], transformed);

            for (centroid_sum, 0..) |*sum, dim| sum.* += transformed[dim];

            if (item.metadata.len > 0) {
                if (updated_range == null) {
                    if (try loadNodeSplitRange(self, txn, leaf.id, isNotFoundGeneric)) |existing| {
                        updated_range = existing;
                    } else {
                        updated_range = .{
                            .min_key = try self.alloc.dupe(u8, item.metadata),
                            .max_key = try self.alloc.dupe(u8, item.metadata),
                        };
                        range_changed = true;
                        continue;
                    }
                }

                if (updated_range) |*range| {
                    if (std.mem.order(u8, item.metadata, range.min_key) == .lt) {
                        self.alloc.free(range.min_key);
                        range.min_key = try self.alloc.dupe(u8, item.metadata);
                        range_changed = true;
                    }
                    if (std.mem.order(u8, item.metadata, range.max_key) == .gt) {
                        self.alloc.free(range.max_key);
                        range.max_key = try self.alloc.dupe(u8, item.metadata);
                        range_changed = true;
                    }
                }
            }
        }

        const new_len = leaf.members.len;
        if (shouldDeferPostingCentroidRefresh(self, &leaf)) {
            self.write_profile.posting_lazy_centroid_deferrals += 1;
        } else if (leaf.centroid.len == 0) {
            leaf.centroid = try self.alloc.alloc(f32, dims);
            const denom: f32 = @floatFromInt(group_len);
            for (leaf.centroid, 0..) |*c, dim| c.* = centroid_sum[dim] / denom;
            normalizeCentroidForMetric(self, leaf.centroid);
            posting.PostingStore.noteCentroidRefreshed(&leaf);
        } else {
            const old_f: f32 = @floatFromInt(old_len);
            const new_f: f32 = @floatFromInt(new_len);
            for (leaf.centroid, 0..) |*c, dim| c.* = (c.* * old_f + centroid_sum[dim]) / new_f;
            normalizeCentroidForMetric(self, leaf.centroid);
            posting.PostingStore.noteCentroidRefreshed(&leaf);
        }
        if (self.config.metric == .l2_squared) {
            expandL2RadiusAfterBatchAppend(&leaf, added_vectors, group_len);
        } else {
            leaf.covering_radius = std.math.nan(f32);
        }

        const leaf_overflows = leaf.members.len > self.config.leaf_size;
        const defer_leaf_split = shouldDeferOversizedLeafSplit(self, &leaf, options);
        if (leaf_overflows and !defer_leaf_split) {
            const split_start = now_fn();
            const right_leaf_id = self.metadata.node_count + 1;
            try self.splitLeafWithOptions(txn, &leaf, options);
            mutate_leaf_ns += elapsed_fn(split_start);
            grouped_recursive_splits += 1;
            try split_candidates.append(self.alloc, leaf.id);
            try split_candidates.append(self.alloc, right_leaf_id);
            grouped_split_queue_peak = @max(grouped_split_queue_peak, split_candidates.items.len);
        } else {
            const save_start = now_fn();
            var save_options = options;
            save_options.suppress_quantized_payload_persist = defer_leaf_split;
            try saveExistingNodeBodyWithAddedVectorsOptions(self, txn, &leaf, added_vectors, group_len, save_options, now_fn_u64_adapter(now_fn), elapsed_fn_u64_adapter(elapsed_fn));
            self.write_profile.save_node_ns += elapsed_fn(save_start);
            self.write_profile.save_node_calls += 1;
            grouped_node_body_writes += 1;
        }
        if (updated_range) |*range| {
            if (range_changed and !leaf_overflows) {
                const range_start = now_fn();
                try putNodeSplitRange(self, txn, leaf.id, range, isNotFoundGeneric);
                self.write_profile.save_split_range_ns += elapsed_fn(range_start);
                grouped_leaf_range_writes += 1;
                try appendUniqueU64(self.alloc, &ancestor_range_refreshes, leaf.parent);
            }
        } else if (!leaf_overflows) {
            const range_start = now_fn();
            try saveNodeSplitRange(self, txn, &leaf, isNotFoundGeneric);
            self.write_profile.save_split_range_ns += elapsed_fn(range_start);
            grouped_leaf_range_writes += 1;
        }
        if (leaf_overflows) {
            if (defer_leaf_split) {
                try recordDeferredOversizedLeaf(self, leaf.id);
                for (prepared[group_start..group_end]) |entry| {
                    try self.putVecLeaf(txn, items[entry.item_index].vector_id, leaf.id);
                }
                grouped_vec_leaf_writes += group_len;
            } else {
                try deferred_vec_leaf_mappings.ensureUnusedCapacity(self.alloc, @intCast(group_len));
                for (prepared[group_start..group_end]) |entry| {
                    deferred_vec_leaf_mappings.appendAssumeCapacity(.{
                        .vector_id = items[entry.item_index].vector_id,
                        .leaf_id = leaf.id,
                    });
                }
                grouped_split_candidates += 1;
            }
        } else {
            for (prepared[group_start..group_end]) |entry| {
                try self.putVecLeaf(txn, items[entry.item_index].vector_id, leaf.id);
            }
            grouped_vec_leaf_writes += group_len;
        }
        self.metadata.active_count += @intCast(group_len);
        grouped_leaf_groups += 1;
        grouped_items += group_len;
        mutate_leaf_ns += elapsed_fn(mutate_start);
        group_start = group_end;
    }

    var split_scan_index: usize = 0;
    grouped_split_queue_peak = @max(grouped_split_queue_peak, split_candidates.items.len);
    const max_split_steps = split_candidates.items.len + items.len * 4 + 64;
    while (split_scan_index < split_candidates.items.len) : (split_scan_index += 1) {
        if (split_scan_index > max_split_steps) return error.HBCBatchSplitLimitExceeded;
        grouped_split_scan_iterations += 1;

        const leaf_id = split_candidates.items[split_scan_index];
        var leaf = loadNode(self, txn, leaf_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        defer leaf.deinit(self.alloc);
        if (leaf.members.len > self.config.leaf_size) {
            try appendUniqueU64(self.alloc, &ancestor_range_refreshes, leaf.parent);
            const right_leaf_id = self.metadata.node_count + 1;
            try self.splitLeafWithOptions(txn, &leaf, options);
            try split_candidates.append(self.alloc, leaf_id);
            try split_candidates.append(self.alloc, right_leaf_id);
            grouped_recursive_splits += 1;
            grouped_split_queue_peak = @max(grouped_split_queue_peak, split_candidates.items.len);
        }
    }

    if (deferred_vec_leaf_mappings.items.len > 0) {
        const deferred_start = now_fn();
        grouped_vec_leaf_writes += try putMissingDeferredVecLeafMappings(self, txn, deferred_vec_leaf_mappings.items);
        mutate_leaf_ns += elapsed_fn(deferred_start);
    }

    for (ancestor_range_refreshes.items) |parent_id| {
        const refreshed = refreshAncestorSplitRangesCounted(self, txn, parent_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        grouped_ancestor_range_refreshes += 1;
        grouped_ancestor_range_nodes += refreshed;
    }

    self.write_profile.insert_calls += @intCast(grouped_items);
    self.write_profile.insert_transform_ns += transform_ns;
    self.write_profile.insert_find_leaf_ns += find_leaf_ns;
    self.write_profile.insert_store_vector_ns += store_vector_ns;
    self.write_profile.insert_mutate_leaf_ns += mutate_leaf_ns;
    self.write_profile.batch_route_calls += 1;
    self.write_profile.batch_route_internal_nodes += route_profile.internal_nodes;
    self.write_profile.batch_route_leaf_groups += route_profile.leaf_groups;
    self.write_profile.batch_route_items += route_profile.routed_items;
    self.write_profile.batch_route_quantized_nodes += route_profile.quantized_nodes;
    self.write_profile.batch_route_exact_child_scores += route_profile.exact_child_scores;
    self.write_profile.batch_route_fallback_nodes += route_profile.fallback_nodes;
    self.write_profile.grouped_leaf_groups += @intCast(grouped_leaf_groups);
    self.write_profile.grouped_items += @intCast(grouped_items);
    self.write_profile.grouped_fallback_items += @intCast(fallback_items);
    self.write_profile.grouped_split_candidates += @intCast(grouped_split_candidates);
    self.write_profile.grouped_recursive_splits += @intCast(grouped_recursive_splits);
    self.write_profile.grouped_split_scan_iterations += @intCast(grouped_split_scan_iterations);
    self.write_profile.grouped_split_queue_peak_total += @intCast(grouped_split_queue_peak);
    self.write_profile.grouped_leaf_range_writes += @intCast(grouped_leaf_range_writes);
    self.write_profile.grouped_ancestor_range_refreshes += @intCast(grouped_ancestor_range_refreshes);
    self.write_profile.grouped_ancestor_range_nodes += @intCast(grouped_ancestor_range_nodes);
    self.write_profile.grouped_node_body_writes += @intCast(grouped_node_body_writes);
    self.write_profile.grouped_vec_leaf_writes += @intCast(grouped_vec_leaf_writes);
    return true;
}

pub fn batchInsertWithMetadataTxn(self: anytype, txn: anytype, items: []const hbc_runtime.BatchInsertItem) !void {
    try batchInsertWithMetadataTxnOptions(self, txn, items, .{});
}

const DeferredVecLeafMapping = struct {
    vector_id: u64,
    leaf_id: u64,
};

fn putMissingDeferredVecLeafMappings(self: anytype, txn: anytype, mappings: []const DeferredVecLeafMapping) !usize {
    var writes: usize = 0;
    for (mappings) |mapping| {
        _ = self.getVecLeaf(txn, mapping.vector_id) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            try self.putVecLeaf(txn, mapping.vector_id, mapping.leaf_id);
            writes += 1;
            continue;
        };
    }
    return writes;
}

pub fn bulkBuildWithMetadata(
    self: anytype,
    items: []const hbc_runtime.BatchInsertItem,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    try bulkBuildWithMetadataOptions(self, items, .{}, now_fn, elapsed_fn);
}

pub fn bulkBuildWithMetadataOptions(
    self: anytype,
    items: []const hbc_runtime.BatchInsertItem,
    options: bulk_build.BulkBuildOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (items.len == 0) return;

    const publishing = try beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    var batch = try self.beginRuntimeBatchTxn();
    errdefer batch.abort();
    errdefer abortVectorCacheMutationsIfSupported(self);
    try bulkBuildWithMetadataTxnOptions(self, &batch, items, options, now_fn, elapsed_fn);
    try finalizeWriteTxnOptions(self, &batch, .{}, now_fn, elapsed_fn);
    const commit_start = now_fn();
    try markPublishSearchStateCommittingIfSupported(self, publishing);
    try batch.commit();
    self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
    try finishPublishSearchStateIfSupported(self, publishing);
    seedRetainedVectorsAfterCommit(self, items, options);
}

pub fn bulkBuildWithMetadataTxn(self: anytype, txn: anytype, items: []const hbc_runtime.BatchInsertItem) !void {
    try bulkBuildWithMetadataTxnOptions(self, txn, items, .{}, nowNsU64Fixed, elapsedSinceU64Fixed);
}

pub fn bulkBuildPreparedInputsTxn(self: anytype, txn: anytype, inputs: []const bulk_build.PreparedBulkBuildInput) !void {
    try bulkBuildPreparedInputsTxnOptions(self, txn, inputs, .{}, nowNsU64Fixed, elapsedSinceU64Fixed);
}

pub fn bulkBuildPreparedInputsTxnOptions(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
    options: bulk_build.BulkBuildOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (inputs.len == 0) return;
    try self.bindTxnLike(txn);
    try self.prepareEmptyPreparedBulkBuild(txn, inputs);

    const sorted_indexes = try self.alloc.alloc(usize, inputs.len);
    defer self.alloc.free(sorted_indexes);
    for (sorted_indexes, 0..) |*idx, i| idx.* = i;
    std.mem.sort(usize, sorted_indexes, inputs, struct {
        fn lessThan(ctx: []const bulk_build.PreparedBulkBuildInput, a: usize, b: usize) bool {
            return ctx[a].vector_id < ctx[b].vector_id;
        }
    }.lessThan);

    const store_start = now_fn();
    for (sorted_indexes) |input_idx| {
        self.invalidateVectorCache(inputs[input_idx].vector_id);
    }
    for (sorted_indexes) |input_idx| {
        const input = inputs[input_idx];
        if (input.metadata.len > 0) {
            var key_buf: [10]u8 = undefined;
            try self.appendNamespaced(txn, .vecs, hbc.encodeVecMetaKey(&key_buf, input.vector_id), input.metadata);
        }
    }
    if (!options.skip_vector_store) {
        for (sorted_indexes) |input_idx| {
            const input = inputs[input_idx];
            var key_buf: [10]u8 = undefined;
            try self.appendNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, input.vector_id), std.mem.sliceAsBytes(input.vector));
        }
    } else if (shouldSeedRetainedVectorCacheOnSkipStore(self)) {
        for (sorted_indexes) |input_idx| {
            const input = inputs[input_idx];
            _ = self.cacheVector(input.vector_id, input.vector) catch {};
        }
    }
    self.write_profile.bulk_build_store_ns += elapsed_fn(store_start);

    const build_start = now_fn();
    var built = switch (options.algo orelse self.config.bulk_build_algo) {
        .recursive => try self.buildBulkRecursiveFromInputs(txn, inputs),
        .hilbert_seeded => try self.buildBulkHilbertSeeded(txn, inputs),
        .doc_key_seeded => try self.buildBulkDocKeySeeded(txn, inputs),
        .kmeans => try self.buildBulkKmeansFromInputs(txn, inputs),
    };
    defer built.deinit(self.alloc);
    self.write_profile.bulk_build_tree_ns += elapsed_fn(build_start);
    self.metadata.root_node = built.node_id;
    self.metadata.active_count = @intCast(inputs.len);
}

pub fn bulkBuildWithMetadataTxnOptions(
    self: anytype,
    txn: anytype,
    items: []const hbc_runtime.BatchInsertItem,
    options: bulk_build.BulkBuildOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (items.len == 0) return;
    try self.bindTxnLike(txn);
    try self.prepareEmptyBulkBuild(txn, items);

    var inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, items.len);
    defer self.alloc.free(inputs);
    const dims: usize = @intCast(self.config.dims);
    const all_pretransformed = blk: {
        for (items) |item| {
            if (item.transformed == null) break :blk false;
        }
        break :blk true;
    };
    const transformed_storage = if (all_pretransformed)
        null
    else
        try self.alloc.alloc(f32, items.len * dims);
    defer if (transformed_storage) |storage| self.alloc.free(storage);

    for (items, 0..) |item, i| {
        const transformed = if (item.transformed) |existing|
            existing
        else blk: {
            const storage = transformed_storage.?;
            const transformed_slot = storage[i * dims ..][0..dims];
            break :blk self.transformVector(item.vector, transformed_slot);
        };

        inputs[i] = .{
            .vector_id = item.vector_id,
            .vector = item.vector,
            .transformed = transformed,
            .metadata = item.metadata,
        };
    }

    try bulkBuildPreparedInputsTxnOptions(self, txn, inputs, options, now_fn, elapsed_fn);
}

pub fn buildBulkRecursiveFromInputs(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
) !BuiltBulkNode {
    const indexes = try self.alloc.alloc(usize, inputs.len);
    defer self.alloc.free(indexes);
    for (indexes, 0..) |*index, i| index.* = i;
    const dims: usize = @intCast(self.config.dims);
    const vec_data = try self.alloc.alloc(f32, inputs.len * dims);
    defer self.alloc.free(vec_data);
    const positions = try self.alloc.alloc(u64, inputs.len);
    defer self.alloc.free(positions);
    const partitioned_indexes = try self.alloc.alloc(usize, inputs.len);
    defer self.alloc.free(partitioned_indexes);

    var scratch = BulkRecursiveScratch{
        .vec_data = vec_data,
        .positions = positions,
        .partitioned_indexes = partitioned_indexes,
    };
    return try buildBulkSubtreeRecursive(self, txn, inputs, indexes, &scratch, 0, 0);
}

pub fn buildBulkHilbertSeeded(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
) !BuiltBulkNode {
    const Entry = struct {
        input: bulk_build.PreparedBulkBuildInput,
        embedding: []const u8,
    };

    const entries = try self.alloc.alloc(Entry, inputs.len);
    defer self.alloc.free(entries);
    const hilbert = try self.getHilbert();
    const embedding_len = hilbert.byteLen();
    const embeddings = try self.alloc.alloc(u8, inputs.len * embedding_len);
    defer self.alloc.free(embeddings);
    const coords = try self.alloc.alloc(u32, hilbert.dimension);
    defer self.alloc.free(coords);

    for (inputs, 0..) |input, i| {
        const embedding = embeddings[i * embedding_len ..][0..embedding_len];
        try hilbert.encodeVecBytesInto(input.transformed, coords, embedding);
        entries[i] = .{
            .input = input,
            .embedding = embedding,
        };
    }
    std.mem.sort(Entry, entries, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return std.mem.order(u8, a.embedding, b.embedding) == .lt;
        }
    }.lessThan);

    var current_count: usize = 0;
    var current = try self.alloc.alloc(BuiltBulkNode, entries.len);
    defer {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    }

    const leaf_groups = try bulk_build.planBalancedGroupSizes(self.alloc, entries.len, @max(@as(usize, 1), self.config.leaf_size));
    defer self.alloc.free(leaf_groups);

    var entry_cursor: usize = 0;
    for (leaf_groups) |group_size| {
        const node_id = self.nextNodeId();
        var group_inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, group_size);
        errdefer self.alloc.free(group_inputs);
        for (0..group_size) |i| {
            group_inputs[i] = entries[entry_cursor + i].input;
        }
        current[current_count] = try buildBulkLeaf(self, txn, node_id, group_inputs, 0, 0);
        self.alloc.free(group_inputs);
        current_count += 1;
        entry_cursor += group_size;
    }

    var current_level: u16 = 0;
    while (current_count > 1) {
        current_level += 1;
        const branch_groups = try bulk_build.planBalancedGroupSizes(self.alloc, current_count, @max(@as(usize, 2), self.config.branching_factor));
        errdefer self.alloc.free(branch_groups);

        var next = try self.alloc.alloc(BuiltBulkNode, branch_groups.len);
        var next_count: usize = 0;
        errdefer {
            for (next[0..next_count]) |*node| node.deinit(self.alloc);
            self.alloc.free(next);
        }

        var child_cursor: usize = 0;
        for (branch_groups) |group_size| {
            const node_id = self.nextNodeId();
            var child_ids = try self.alloc.alloc(u64, group_size);
            errdefer self.alloc.free(child_ids);

            const centroid = try self.alloc.alloc(f32, self.config.dims);
            errdefer self.alloc.free(centroid);
            @memset(centroid, 0);

            var merged_range: ?types.NodeSplitRange = null;
            errdefer if (merged_range) |*owned| owned.deinit(self.alloc);

            for (0..group_size) |i| {
                const child = &current[child_cursor + i];
                child_ids[i] = child.node_id;
                addWeightedVector(centroid, child.centroid, child.member_count);
                try self.updateParent(txn, child.node_id, node_id);
                if (merged_range == null) {
                    if (child.range) |range| merged_range = try range.clone(self.alloc);
                } else {
                    var old_range = merged_range;
                    merged_range = try bulk_build.mergeNodeSplitRanges(self.alloc, old_range, child.range);
                    if (old_range) |*owned| owned.deinit(self.alloc);
                }
            }
            const member_count = sumBulkMemberCounts(current[child_cursor .. child_cursor + group_size]);
            vec.scale(1.0 / @as(f32, @floatFromInt(member_count)), centroid);
            normalizeCentroidForMetric(self, centroid);

            var node = types.Node{
                .id = node_id,
                .is_leaf = false,
                .level = current_level,
                .parent = 0,
                .centroid = centroid,
                .children = child_ids,
                .members = &.{},
            };
            node.covering_radius = try computeInternalCoveringRadius(self, txn, &node);
            try self.saveNodeBody(txn, &node);
            try self.putNodeSplitRange(txn, node_id, if (merged_range) |*owned| owned else null);
            self.alloc.free(child_ids);

            next[next_count] = .{
                .node_id = node_id,
                .centroid = centroid,
                .range = merged_range,
                .level = current_level,
                .member_count = member_count,
            };
            next_count += 1;
            child_cursor += group_size;
        }
        self.alloc.free(branch_groups);

        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
        current = next;
        current_count = next_count;
    }

    return .{
        .node_id = current[0].node_id,
        .centroid = try self.alloc.dupe(f32, current[0].centroid),
        .range = if (current[0].range) |range| try range.clone(self.alloc) else null,
        .level = current[0].level,
        .member_count = current[0].member_count,
    };
}

pub fn buildBulkDocKeySeeded(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
) !BuiltBulkNode {
    const Entry = struct {
        input: bulk_build.PreparedBulkBuildInput,
    };

    const entries = try self.alloc.alloc(Entry, inputs.len);
    defer self.alloc.free(entries);
    for (inputs, 0..) |input, i| entries[i] = .{ .input = input };

    std.mem.sort(Entry, entries, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return switch (std.mem.order(u8, a.input.metadata, b.input.metadata)) {
                .lt => true,
                .eq => a.input.vector_id < b.input.vector_id,
                .gt => false,
            };
        }
    }.lessThan);

    var current_count: usize = 0;
    var current = try self.alloc.alloc(BuiltBulkNode, entries.len);
    defer {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    }

    const leaf_groups = try bulk_build.planBalancedGroupSizes(self.alloc, entries.len, @max(@as(usize, 1), self.config.leaf_size));
    defer self.alloc.free(leaf_groups);

    var entry_cursor: usize = 0;
    for (leaf_groups) |group_size| {
        const node_id = self.nextNodeId();
        var group_inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, group_size);
        errdefer self.alloc.free(group_inputs);
        for (0..group_size) |i| {
            group_inputs[i] = entries[entry_cursor + i].input;
        }
        current[current_count] = try buildBulkLeaf(self, txn, node_id, group_inputs, 0, 0);
        self.alloc.free(group_inputs);
        current_count += 1;
        entry_cursor += group_size;
    }

    return try buildBulkParentLevels(self, txn, current, current_count);
}

pub fn buildBulkKmeansFromInputs(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
) !BuiltBulkNode {
    if (inputs.len == 0) return error.TooFewVectors;
    if (inputs.len <= self.config.leaf_size) {
        return try buildBulkLeaf(self, txn, self.nextNodeId(), inputs, 0, 0);
    }

    const leaf_size = @max(@as(usize, 1), self.config.leaf_size);
    const cluster_count = std.math.divCeil(usize, inputs.len, leaf_size) catch unreachable;
    const dims: usize = @intCast(self.config.dims);

    const dense_vectors = try self.alloc.alloc(f32, inputs.len * dims);
    defer self.alloc.free(dense_vectors);
    const points = try self.alloc.alloc(kmeans.Point, inputs.len);
    defer self.alloc.free(points);
    for (points, inputs, 0..) |*point, input, i| {
        const vector = dense_vectors[i * dims ..][0..dims];
        @memcpy(vector, input.transformed);
        point.* = .{
            .stable_id = input.vector_id,
            .vector = vector,
            .weight = 1,
        };
    }

    const assignments = try self.alloc.alloc(usize, inputs.len);
    defer self.alloc.free(assignments);
    const distances = try self.alloc.alloc(f32, inputs.len);
    defer self.alloc.free(distances);
    const counts = try self.alloc.alloc(usize, cluster_count);
    defer self.alloc.free(counts);
    const centroids = try self.alloc.alloc(f32, cluster_count * dims);
    defer self.alloc.free(centroids);
    const next_centroids = try self.alloc.alloc(f32, cluster_count * dims);
    defer self.alloc.free(next_centroids);
    const entries = try self.alloc.alloc(kmeans.Entry, inputs.len);
    defer self.alloc.free(entries);

    const stats = try kmeans.run(.{
        .dims = dims,
        .metric = self.config.metric,
        .max_iter = self.config.kmeans_max_iter,
        .backend = self.config.kmeans_backend,
        .update_strategy = self.config.kmeans_update_strategy,
        .dense_vectors = dense_vectors,
    }, points, self.rng.intN(inputs.len), centroids, next_centroids, assignments, distances, counts, entries);
    recordKmeansRunStats(self, stats);

    var current_count: usize = 0;
    var current = try self.alloc.alloc(BuiltBulkNode, inputs.len);
    defer {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    }

    var cluster_start: usize = 0;
    while (cluster_start < entries.len) {
        const cluster = entries[cluster_start].cluster;
        var cluster_end = cluster_start + 1;
        while (cluster_end < entries.len and entries[cluster_end].cluster == cluster) : (cluster_end += 1) {}

        const cluster_len = cluster_end - cluster_start;
        const leaf_groups = try bulk_build.planBalancedGroupSizes(self.alloc, cluster_len, leaf_size);
        errdefer self.alloc.free(leaf_groups);

        var entry_cursor = cluster_start;
        for (leaf_groups) |group_size| {
            const node_id = self.nextNodeId();
            var group_inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, group_size);
            errdefer self.alloc.free(group_inputs);
            for (0..group_size) |i| {
                group_inputs[i] = inputs[entries[entry_cursor + i].point_index];
            }
            current[current_count] = try buildBulkLeaf(self, txn, node_id, group_inputs, 0, 0);
            self.alloc.free(group_inputs);
            current_count += 1;
            entry_cursor += group_size;
        }
        self.alloc.free(leaf_groups);

        cluster_start = cluster_end;
    }

    return try buildBulkKmeansParentLevels(self, txn, current, current_count);
}

pub fn splitVectorSet(
    self: anytype,
    vectors: *const vec.Set,
    ids: []const u64,
) !SplitResult {
    if (ids.len < 2) return error.TooFewVectors;
    if (self.config.metric == .cosine) try vec.validateUnitVectorSet(vectors);
    return switch (self.config.split_algo) {
        .kmeans => splitVectorSetKmeans(self, vectors, ids),
        .hilbert => splitVectorSetHilbert(self, vectors, ids),
    };
}

pub fn maybeBuildKeyLocalLeafSplit(
    self: anytype,
    txn: anytype,
    member_ids: []const u64,
    vectors: *const vec.Set,
    current: *const SplitResult,
) !?SplitResult {
    if (member_ids.len < 2) return null;

    var entries = try self.alloc.alloc(LeafKeyEntry, member_ids.len);
    defer self.alloc.free(entries);
    for (member_ids, 0..) |member_id, i| {
        const key = (try self.loadMetadataRaw(txn, member_id)) orelse return null;
        entries[i] = .{
            .index = i,
            .member_id = member_id,
            .key = key,
        };
    }
    std.mem.sort(LeafKeyEntry, entries, {}, struct {
        fn lessThan(_: void, a: LeafKeyEntry, b: LeafKeyEntry) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lessThan);

    const count = member_ids.len;
    const min_count = (count * self.config.kmeans_min_balance_pct + 99) / 100;
    const left_count = std.math.clamp(count / 2, min_count, count - min_count);

    const current_score = try splitObjective(self, vectors, member_ids, current);
    const candidate_score = try orderedLeafSplitObjective(self, vectors, entries, left_count);
    if (current_score == 0) return null;
    if (candidate_score > current_score * self.config.key_local_leaf_split_penalty) return null;

    return try buildOrderedLeafSplit(self, vectors, entries, left_count);
}

pub fn saveQuantized(self: anytype, txn: anytype, node_id: u64, qs: *const hbc_runtime.QuantizedSet, now_fn: fn () u64, elapsed_fn: fn (u64) u64) !void {
    const encode_start = now_fn();
    const data = switch (qs.*) {
        .rabit => |*set| try set.encode(self.alloc),
        .nonquant => |*set| try set.encode(self.alloc),
    };
    self.write_profile.quantized_encode_ns += elapsed_fn(encode_start);
    defer self.alloc.free(data);
    var key_buf: [10]u8 = undefined;
    const put_start = now_fn();
    try self.putNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node_id), data);
    self.write_profile.quantized_put_ns += elapsed_fn(put_start);
    try self.cacheQuantized(node_id, qs);
}

pub fn putQuantizedCached(self: anytype, txn: anytype, node_id: u64, qs: *const hbc_runtime.QuantizedSet, now_fn: fn () u64, elapsed_fn: fn (u64) u64) !void {
    const encode_start = now_fn();
    const data = switch (qs.*) {
        .rabit => |*set| try set.encode(self.alloc),
        .nonquant => |*set| try set.encode(self.alloc),
    };
    self.write_profile.quantized_encode_ns += elapsed_fn(encode_start);
    defer self.alloc.free(data);
    var key_buf: [10]u8 = undefined;
    const put_start = now_fn();
    try self.putNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node_id), data);
    self.write_profile.quantized_put_ns += elapsed_fn(put_start);
}

pub fn loadQuantized(self: anytype, txn: anytype, node_id: u64, is_root: bool, expected_count: usize, is_not_found: fn (anyerror) bool) !hbc_runtime.QuantizedSet {
    _ = is_not_found;
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node_id));
    var decoded = if (is_root)
        hbc_runtime.QuantizedSet{ .nonquant = try proto.NonQuantizedVectorSet.decode(self.alloc, data) }
    else
        hbc_runtime.QuantizedSet{ .rabit = try proto.RaBitQuantizedVectorSet.decode(self.alloc, data) };
    errdefer decoded.deinit(self.alloc);
    try validateQuantizedSet(self, &decoded, expected_count);
    return decoded;
}

pub fn getQuantized(self: anytype, txn: anytype, node_id: u64, is_root: bool, expected_count: usize, is_not_found: fn (anyerror) bool) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    return try loadQuantizedReadHandle(self, txn, node_id, is_root, expected_count, is_not_found);
}

pub fn getQuantizedProfiled(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    profile: *search_types.SearchProfile,
    is_not_found: fn (anyerror) bool,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    return try loadQuantizedReadHandleProfiled(self, txn, node_id, is_root, expected_count, profile, now_fn, elapsed_fn, is_not_found);
}

fn loadQuantizedProfiledOwned(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
    is_not_found: fn (anyerror) bool,
) !?hbc_runtime.QuantizedSet {
    if (try self.getCachedQuantizedClone(node_id)) |cached_value| {
        var cached = cached_value;
        validateQuantizedSet(self, &cached, expected_count) catch {
            cached.deinit(self.alloc);
            self.invalidateQuantizedCache(node_id);
            const start = now_fn();
            const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
                if (is_not_found(err) or err == error.Corrupted) return null;
                return err;
            };
            profile.quantized_cache_miss_ns += elapsed_fn(start);
            profile.quantized_cache_misses += 1;
            if (self.cache_enabled) self.cacheQuantized(node_id, &decoded) catch {};
            return decoded;
        };
        return cached;
    }

    const start = now_fn();
    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) return null;
        return err;
    };
    profile.quantized_cache_miss_ns += elapsed_fn(start);
    profile.quantized_cache_misses += 1;
    if (self.cache_enabled) {
        self.cacheQuantized(node_id, &decoded) catch {};
    }
    return decoded;
}

pub fn estimateQuantizedDistances(
    self: anytype,
    qs: *const hbc_runtime.QuantizedSet,
    query: []const f32,
    query_measure: f32,
    distances: []f32,
    error_bounds: []f32,
    scratch: *quantizer_mod.RaBitQuantizer.EstimateScratch,
) !void {
    const count = qs.getCount();
    if (distances.len != count or error_bounds.len != count) return error.Corrupted;
    try validateQuantizedSet(self, qs, count);

    switch (qs.*) {
        .rabit => |*set| try self.quantizer.estimateDistancesWithScratch(set, query, distances, error_bounds, scratch),
        .nonquant => |*set| {
            const dims: usize = @intCast(set.vectors.dims);
            for (0..count) |i| {
                const candidate = set.vectors.data[i * dims ..][0..dims];
                distances[i] = vec.distanceToQuery(query, query_measure, candidate, self.config.metric);
                error_bounds[i] = 0;
            }
        },
    }
}

pub fn refreshQuantized(self: anytype, txn: anytype, node: *const types.Node, now_fn: fn () u64, elapsed_fn: fn (u64) u64) !void {
    return try refreshQuantizedWithOptions(self, txn, node, .{}, now_fn, elapsed_fn);
}

pub fn refreshQuantizedWithOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    options: anytype,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (!self.config.use_quantization) return;
    if (node.centroid.len == 0) return;

    var key_buf: [10]u8 = undefined;
    const count = if (node.is_leaf) node.members.len else node.children.len;
    if (count == 0) {
        self.deleteNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node.id)) catch {};
        self.invalidateQuantizedCache(node.id);
        return;
    }

    const dims: usize = @intCast(self.metadata.dims);
    const vectors = try self.alloc.alloc(f32, count * dims);
    defer self.alloc.free(vectors);

    const load_start = now_fn();
    if (node.is_leaf) {
        posting.PostingStore.loadTransformedVectorsForQuantizedRefresh(self, txn, node, vectors, options) catch {
            self.deleteNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node.id)) catch {};
            self.invalidateQuantizedCache(node.id);
            return;
        };
        const load_elapsed = elapsed_fn(load_start);
        self.write_profile.quantized_vector_load_ns += load_elapsed;
        self.write_profile.quantized_leaf_vector_load_ns += load_elapsed;
    } else {
        for (node.children, 0..) |child_id, i| {
            var child = try loadNode(self, txn, child_id);
            defer child.deinit(self.alloc);
            if (child.centroid.len == 0) {
                @memset(vectors[i * dims ..][0..dims], 0);
                continue;
            }
            @memcpy(vectors[i * dims ..][0..dims], child.centroid[0..dims]);
        }
        const load_elapsed = elapsed_fn(load_start);
        self.write_profile.quantized_vector_load_ns += load_elapsed;
        self.write_profile.quantized_internal_child_load_ns += load_elapsed;
    }

    if (node.is_leaf) {
        try posting.PostingStore.refreshQuantizedPayload(self, txn, node, vectors, now_fn, elapsed_fn);
        return;
    }

    if (try self.getCachedQuantizedClone(node.id)) |cached_value| {
        var cached = cached_value;
        defer cached.deinit(self.alloc);
        switch (cached) {
            .nonquant => |*set| {
                if (!usesNonQuantizedPayload(node)) {
                    const compute_start = now_fn();
                    var fresh: hbc_runtime.QuantizedSet = .{ .rabit = try self.quantizer.quantize(node.centroid, vectors, count) };
                    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                    defer fresh.deinit(self.alloc);
                    const store_start = now_fn();
                    try saveQuantized(self, txn, node.id, &fresh, now_fn, elapsed_fn);
                    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
                    return;
                }
                set.vectors.dims = @intCast(dims);
                set.vectors.count = @intCast(count);
                if (set.vectors.data.len == 0) {
                    set.vectors.data = try self.alloc.alloc(f32, count * dims);
                } else {
                    set.vectors.data = try self.alloc.realloc(set.vectors.data, count * dims);
                }
                @memcpy(set.vectors.data, vectors);
                const store_start = now_fn();
                try putQuantizedCached(self, txn, node.id, &cached, now_fn, elapsed_fn);
                try self.cacheQuantized(node.id, &cached);
                noteMutatedCachedQuantized(self, node.id);
                self.write_profile.quantized_store_ns += elapsed_fn(store_start);
                return;
            },
            .rabit => |*set| {
                if (usesNonQuantizedPayload(node)) {
                    const compute_start = now_fn();
                    var fresh: hbc_runtime.QuantizedSet = .{ .nonquant = .{
                        .vectors = .{
                            .dims = @intCast(dims),
                            .count = @intCast(count),
                            .data = try self.alloc.dupe(f32, vectors),
                        },
                    } };
                    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                    defer fresh.deinit(self.alloc);
                    const store_start = now_fn();
                    try saveQuantized(self, txn, node.id, &fresh, now_fn, elapsed_fn);
                    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
                    return;
                }
                const compute_start = now_fn();
                try self.quantizer.quantizeInto(set, node.centroid, vectors, count);
                self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                const store_start = now_fn();
                try putQuantizedCached(self, txn, node.id, &cached, now_fn, elapsed_fn);
                try self.cacheQuantized(node.id, &cached);
                noteMutatedCachedQuantized(self, node.id);
                self.write_profile.quantized_store_ns += elapsed_fn(store_start);
                return;
            },
        }
    }

    const compute_start = now_fn();
    var qs: hbc_runtime.QuantizedSet = if (usesNonQuantizedPayload(node))
        .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(dims),
                .count = @intCast(count),
                .data = try self.alloc.dupe(f32, vectors),
            },
        } }
    else
        .{ .rabit = try self.quantizer.quantize(node.centroid, vectors, count) };
    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
    defer qs.deinit(self.alloc);
    const store_start = now_fn();
    try saveQuantized(self, txn, node.id, &qs, now_fn, elapsed_fn);
    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
}

pub fn batchInsertWithMetadataTxnOptions(
    self: anytype,
    txn: anytype,
    items: []const hbc_runtime.BatchInsertItem,
    options: hbc_runtime.BatchInsertOptions,
) !void {
    try self.bindTxnLike(txn);
    var transformed_vector_storage = try self.alloc.alloc(f32, self.config.dims);
    const transformed_vector = transformed_vector_storage[0..];
    defer self.alloc.free(transformed_vector);
    const previous_vector_storage = try self.alloc.alloc(f32, self.config.dims);
    defer self.alloc.free(previous_vector_storage);
    const previous_transformed_storage = try self.alloc.alloc(f32, self.config.dims);
    defer self.alloc.free(previous_transformed_storage);
    var deferred_recompute_leaf_ids = std.ArrayListUnmanaged(u64).empty;
    defer deferred_recompute_leaf_ids.deinit(self.alloc);
    var deferred_leaf_centroid_deltas = std.ArrayListUnmanaged(DeferredLeafCentroidDelta).empty;
    defer {
        for (deferred_leaf_centroid_deltas.items) |entry| self.alloc.free(entry.delta_sum);
        deferred_leaf_centroid_deltas.deinit(self.alloc);
    }
    var deferred_ancestor_centroid_refresh_ids = std.ArrayListUnmanaged(u64).empty;
    defer deferred_ancestor_centroid_refresh_ids.deinit(self.alloc);
    for (items) |item| {
        self.write_profile.insert_calls += 1;
        const effective_transformed = blk: {
            const transform_start = nowNsU64Fixed();
            const transformed = if (item.transformed) |existing|
                existing
            else blk_transformed: {
                _ = self.transformVector(item.vector, transformed_vector);
                break :blk_transformed transformed_vector;
            };
            self.write_profile.insert_transform_ns += elapsedSinceU64Fixed(transform_start);
            break :blk transformed;
        };

        if (!options.assume_absent_ids) {
            const existing_leaf_id = self.getVecLeaf(txn, item.vector_id) catch |err| blk: {
                if (isNotFoundGeneric(err)) break :blk 0;
                return err;
            };
            if (existing_leaf_id != 0) {
                if (!options.coalesce_leaf_writes and try existingVectorMatchesNoOp(self, txn, item.vector_id, item.vector, item.metadata, previous_vector_storage)) {
                    self.write_profile.noop_existing_skips += 1;
                    continue;
                }
                const find_leaf_start = nowNsU64Fixed();
                const allow_quantized_routing = if (@hasField(@TypeOf(options), "allow_quantized_routing"))
                    options.allow_quantized_routing
                else
                    !options.centroid_only_routing;
                const leaf_id = try posting.CentroidDirectory.findPosting(self, txn, self.metadata.root_node, effective_transformed, allow_quantized_routing);
                self.write_profile.insert_find_leaf_ns += elapsedSinceU64Fixed(find_leaf_start);
                if (existing_leaf_id == leaf_id) {
                    if (try tryCoalesceExistingVectorInLeafTxnOptions(
                        self,
                        txn,
                        leaf_id,
                        item.vector_id,
                        item.vector,
                        item.metadata,
                        effective_transformed,
                        previous_vector_storage,
                        previous_transformed_storage,
                        &deferred_recompute_leaf_ids,
                        &deferred_leaf_centroid_deltas,
                        &deferred_ancestor_centroid_refresh_ids,
                        options,
                    )) {
                        continue;
                    }
                    if (try tryUpdateExistingVectorInLeafTxnOptions(
                        self,
                        txn,
                        leaf_id,
                        item.vector_id,
                        item.vector,
                        item.metadata,
                        effective_transformed,
                        previous_vector_storage,
                        previous_transformed_storage,
                        options.skip_vector_store,
                        options,
                    )) {
                        continue;
                    }
                }
            }
        }

        try self.insertWithMetadataTxnOptions(txn, item.vector_id, item.vector, item.transformed, item.metadata, transformed_vector, options);
    }

    for (deferred_leaf_centroid_deltas.items) |entry| {
        var leaf = loadNode(self, txn, entry.leaf_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        defer leaf.deinit(self.alloc);
        const mutate_start = nowNsU64Fixed();
        posting.PostingStore.noteVectorsChanged(&leaf);
        if (shouldDeferPostingCentroidRefresh(self, &leaf)) {
            self.write_profile.posting_lazy_centroid_deferrals += 1;
        } else applyLeafCentroidDelta(self, &leaf, entry.delta_sum) catch {
            try posting.PostingStore.recomputeCentroid(self, txn, &leaf);
        };
        try self.saveNodeWithOptionsMode(txn, &leaf, options, false);
        self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
    }

    for (deferred_recompute_leaf_ids.items) |leaf_id| {
        var leaf = loadNode(self, txn, leaf_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        defer leaf.deinit(self.alloc);
        const mutate_start = nowNsU64Fixed();
        posting.PostingStore.noteVectorsChanged(&leaf);
        if (shouldDeferPostingCentroidRefresh(self, &leaf)) {
            self.write_profile.posting_lazy_centroid_deferrals += 1;
        } else {
            try posting.PostingStore.recomputeCentroid(self, txn, &leaf);
        }
        try self.saveNodeWithOptionsMode(txn, &leaf, options, false);
        self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
    }

    for (deferred_ancestor_centroid_refresh_ids.items) |parent_id| {
        if (self.config.lazy_posting_maintenance) {
            if (parent_id != 0) self.write_profile.posting_lazy_ancestor_deferrals += 1;
            continue;
        }
        const mutate_start = nowNsU64Fixed();
        try recomputeAncestorCentroidsWithOptions(self, txn, parent_id, options);
        self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
    }
}

fn storeVectorAndMetadataWithOptions(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    vector_data: []const f32,
    metadata_value: []const u8,
    skip_vector_store: bool,
) !void {
    self.invalidateVectorCache(vector_id);
    if (!skip_vector_store) {
        try putVector(self, txn, vector_id, vector_data);
    } else if (shouldSeedRetainedVectorCacheOnSkipStore(self)) {
        _ = self.cacheVector(vector_id, vector_data) catch {};
    }
    if (metadata_value.len > 0) try putMetadata(self, txn, vector_id, metadata_value);
}

pub fn finalizeWriteTxnOptions(
    self: anytype,
    txn: anytype,
    options: hbc_runtime.BatchInsertOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    try self.bindTxnLike(txn);
    if (deferLeafSplitToBatchFinish(options)) {
        _ = try normalizeDeferredOversizedLeavesForBatchFinish(self, txn, options);
    }
    if (options.defer_quantized_rebuild) {
        if (!shouldDeferQuantizedRebuildToBulkFinish(self, options)) {
            const rebuild_start = now_fn();
            if (!try rebuildDeferredQuantizedNodes(self, txn, options)) {
                try rebuildAllQuantized(self, txn);
            }
            self.write_profile.refresh_quantized_ns += elapsed_fn(rebuild_start);
        }
    }
    try runAutoPostingMaintenanceTxn(self, txn);
    const flush_start = now_fn();
    try self.flushMetadata(txn);
    self.write_profile.insert_flush_metadata_ns += elapsed_fn(flush_start);
    try publishDeferredNodeKeysForBatchFinish(self, txn, options);
}

pub fn rebuildAllQuantized(self: anytype, txn: anytype) !void {
    try rebuildQuantizedSubtree(self, txn, self.metadata.root_node);
}

fn buildBulkSubtreeRecursive(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
    indexes: []usize,
    scratch: *BulkRecursiveScratch,
    parent_id: u64,
    level: u16,
) !BuiltBulkNode {
    if (indexes.len == 0) return error.TooFewVectors;

    const node_id = self.nextNodeId();
    if (indexes.len <= self.config.leaf_size) {
        return try buildBulkLeafIndexed(self, txn, node_id, inputs, indexes, parent_id, level);
    }

    const left_len = try partitionBulkInputIndexesInPlace(self, inputs, indexes, scratch);

    var left = try buildBulkSubtreeRecursive(self, txn, inputs, indexes[0..left_len], scratch, node_id, level + 1);
    defer left.deinit(self.alloc);
    var right = try buildBulkSubtreeRecursive(self, txn, inputs, indexes[left_len..], scratch, node_id, level + 1);
    defer right.deinit(self.alloc);

    const centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(centroid);
    @memset(centroid, 0);
    addWeightedVector(centroid, left.centroid, left.member_count);
    addWeightedVector(centroid, right.centroid, right.member_count);
    const member_count = left.member_count + right.member_count;
    vec.scale(1.0 / @as(f32, @floatFromInt(member_count)), centroid);
    normalizeCentroidForMetric(self, centroid);

    var child_ids = try self.alloc.alloc(u64, 2);
    errdefer self.alloc.free(child_ids);
    child_ids[0] = left.node_id;
    child_ids[1] = right.node_id;

    var node = types.Node{
        .id = node_id,
        .is_leaf = false,
        .level = level,
        .parent = parent_id,
        .centroid = centroid,
        .children = child_ids,
        .members = &.{},
    };
    node.covering_radius = try computeInternalCoveringRadius(self, txn, &node);
    try self.saveNodeBody(txn, &node);
    self.alloc.free(child_ids);

    var range = try bulk_build.mergeNodeSplitRanges(self.alloc, left.range, right.range);
    errdefer if (range) |*owned| owned.deinit(self.alloc);
    try self.putNodeSplitRange(txn, node_id, if (range) |*owned| owned else null);

    return .{
        .node_id = node_id,
        .centroid = centroid,
        .range = range,
        .level = level,
        .member_count = member_count,
    };
}

fn buildBulkLeafIndexed(
    self: anytype,
    txn: anytype,
    node_id: u64,
    inputs: []const bulk_build.PreparedBulkBuildInput,
    indexes: []const usize,
    parent_id: u64,
    level: u16,
) !BuiltBulkNode {
    const centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(centroid);
    @memset(centroid, 0);

    var members = try self.alloc.alloc(u64, indexes.len);
    errdefer self.alloc.free(members);
    var range = try bulk_build.initNodeSplitRangeFromInput(self.alloc, inputs[indexes[0]]);
    errdefer {
        var owned = range;
        owned.deinit(self.alloc);
    }

    for (indexes, 0..) |input_idx, i| {
        const input = inputs[input_idx];
        members[i] = input.vector_id;
        vec.add(centroid, input.transformed);
        try self.putVecLeaf(txn, input.vector_id, node_id);
        try bulk_build.extendNodeSplitRangeFromInput(self.alloc, &range, input);
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(indexes.len)), centroid);
    normalizeCentroidForMetric(self, centroid);

    var node = types.Node{
        .id = node_id,
        .is_leaf = true,
        .level = level,
        .parent = parent_id,
        .centroid = centroid,
        .children = &.{},
        .members = members,
    };
    const leaf_vectors = try self.alloc.alloc(f32, indexes.len * self.config.dims);
    defer self.alloc.free(leaf_vectors);
    for (indexes, 0..) |input_idx, i| {
        const transformed = inputs[input_idx].transformed;
        @memcpy(leaf_vectors[i * self.config.dims ..][0..self.config.dims], transformed);
    }
    try saveLeafNodeBodyWithKnownVectors(self, txn, &node, leaf_vectors, nowNsI128Fixed, elapsedSinceNsFixed);
    try self.putNodeSplitRange(txn, node_id, &range);
    self.alloc.free(members);

    return .{
        .node_id = node_id,
        .centroid = centroid,
        .range = range,
        .level = level,
        .member_count = indexes.len,
    };
}

fn buildBulkParentLevels(
    self: anytype,
    txn: anytype,
    initial_nodes: []BuiltBulkNode,
    initial_count: usize,
) !BuiltBulkNode {
    var current = initial_nodes;
    var current_count = initial_count;
    var owns_current = false;
    defer if (owns_current) {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    };

    var current_level: u16 = if (current_count == 0) 0 else current[0].level;
    while (current_count > 1) {
        current_level += 1;
        const branch_groups = try bulk_build.planBalancedGroupSizes(self.alloc, current_count, @max(@as(usize, 2), self.config.branching_factor));
        errdefer self.alloc.free(branch_groups);

        var next = try self.alloc.alloc(BuiltBulkNode, branch_groups.len);
        var next_count: usize = 0;
        errdefer {
            for (next[0..next_count]) |*node| node.deinit(self.alloc);
            self.alloc.free(next);
        }

        var child_cursor: usize = 0;
        for (branch_groups) |group_size| {
            const node_id = self.nextNodeId();
            var child_ids = try self.alloc.alloc(u64, group_size);
            errdefer self.alloc.free(child_ids);

            const centroid = try self.alloc.alloc(f32, self.config.dims);
            errdefer self.alloc.free(centroid);
            @memset(centroid, 0);

            var merged_range: ?types.NodeSplitRange = null;
            errdefer if (merged_range) |*owned| owned.deinit(self.alloc);

            for (0..group_size) |i| {
                const child = &current[child_cursor + i];
                child_ids[i] = child.node_id;
                addWeightedVector(centroid, child.centroid, child.member_count);
                try self.updateParent(txn, child.node_id, node_id);
                if (merged_range == null) {
                    if (child.range) |range| merged_range = try range.clone(self.alloc);
                } else {
                    var old_range = merged_range;
                    merged_range = try bulk_build.mergeNodeSplitRanges(self.alloc, old_range, child.range);
                    if (old_range) |*owned| owned.deinit(self.alloc);
                }
            }
            const member_count = sumBulkMemberCounts(current[child_cursor .. child_cursor + group_size]);
            vec.scale(1.0 / @as(f32, @floatFromInt(member_count)), centroid);
            normalizeCentroidForMetric(self, centroid);

            var node = types.Node{
                .id = node_id,
                .is_leaf = false,
                .level = current_level,
                .parent = 0,
                .centroid = centroid,
                .children = child_ids,
                .members = &.{},
            };
            node.covering_radius = try computeInternalCoveringRadius(self, txn, &node);
            try self.saveNodeBody(txn, &node);
            try self.putNodeSplitRange(txn, node_id, if (merged_range) |*owned| owned else null);
            self.alloc.free(child_ids);

            next[next_count] = .{
                .node_id = node_id,
                .centroid = centroid,
                .range = merged_range,
                .level = current_level,
                .member_count = member_count,
            };
            next_count += 1;
            child_cursor += group_size;
        }
        self.alloc.free(branch_groups);

        if (owns_current) {
            for (current[0..current_count]) |*node| node.deinit(self.alloc);
            self.alloc.free(current);
        }
        current = next;
        current_count = next_count;
        owns_current = true;
    }

    return .{
        .node_id = current[0].node_id,
        .centroid = try self.alloc.dupe(f32, current[0].centroid),
        .range = if (current[0].range) |range| try range.clone(self.alloc) else null,
        .level = current[0].level,
        .member_count = current[0].member_count,
    };
}

fn buildBulkKmeansParentLevels(
    self: anytype,
    txn: anytype,
    initial_nodes: []BuiltBulkNode,
    initial_count: usize,
) !BuiltBulkNode {
    var current = initial_nodes;
    var current_count = initial_count;
    var owns_current = false;
    defer if (owns_current) {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    };

    var current_level: u16 = if (current_count == 0) 0 else current[0].level;
    while (current_count > 1) {
        current_level += 1;
        const max_group_size = @max(@as(usize, 2), self.config.branching_factor);

        const next = if (current_count <= max_group_size) blk: {
            const out = try self.alloc.alloc(BuiltBulkNode, 1);
            out[0] = try buildBulkParentFromNodeRange(self, txn, current[0..current_count], current_level);
            break :blk out;
        } else try buildBulkKmeansParentLevel(self, txn, current[0..current_count], current_level, max_group_size);
        errdefer {
            for (next) |*node| node.deinit(self.alloc);
            self.alloc.free(next);
        }

        if (owns_current) {
            for (current[0..current_count]) |*node| node.deinit(self.alloc);
            self.alloc.free(current);
        }
        current = next;
        current_count = next.len;
        owns_current = true;
    }

    return .{
        .node_id = current[0].node_id,
        .centroid = try self.alloc.dupe(f32, current[0].centroid),
        .range = if (current[0].range) |range| try range.clone(self.alloc) else null,
        .level = current[0].level,
        .member_count = current[0].member_count,
    };
}

fn buildBulkKmeansParentLevel(
    self: anytype,
    txn: anytype,
    nodes: []BuiltBulkNode,
    level: u16,
    max_group_size: usize,
) ![]BuiltBulkNode {
    const dims: usize = @intCast(self.config.dims);
    const cluster_count = std.math.divCeil(usize, nodes.len, max_group_size) catch unreachable;

    const dense_vectors = try self.alloc.alloc(f32, nodes.len * dims);
    defer self.alloc.free(dense_vectors);
    const points = try self.alloc.alloc(kmeans.Point, nodes.len);
    defer self.alloc.free(points);
    for (points, nodes, 0..) |*point, node, i| {
        const vector = dense_vectors[i * dims ..][0..dims];
        @memcpy(vector, node.centroid);
        point.* = .{
            .stable_id = node.node_id,
            .vector = vector,
            .weight = node.member_count,
        };
    }

    const assignments = try self.alloc.alloc(usize, nodes.len);
    defer self.alloc.free(assignments);
    const distances = try self.alloc.alloc(f32, nodes.len);
    defer self.alloc.free(distances);
    const counts = try self.alloc.alloc(usize, cluster_count);
    defer self.alloc.free(counts);
    const centroids = try self.alloc.alloc(f32, cluster_count * dims);
    defer self.alloc.free(centroids);
    const next_centroids = try self.alloc.alloc(f32, cluster_count * dims);
    defer self.alloc.free(next_centroids);
    const entries = try self.alloc.alloc(kmeans.Entry, nodes.len);
    defer self.alloc.free(entries);

    const stats = try kmeans.run(.{
        .dims = dims,
        .metric = self.config.metric,
        .max_iter = self.config.kmeans_max_iter,
        .backend = self.config.kmeans_backend,
        .update_strategy = self.config.kmeans_update_strategy,
        .dense_vectors = dense_vectors,
    }, points, self.rng.intN(nodes.len), centroids, next_centroids, assignments, distances, counts, entries);
    recordKmeansRunStats(self, stats);

    var out = try self.alloc.alloc(BuiltBulkNode, nodes.len);
    var out_count: usize = 0;
    errdefer {
        for (out[0..out_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(out);
    }

    var cluster_start: usize = 0;
    while (cluster_start < entries.len) {
        const cluster = entries[cluster_start].cluster;
        var cluster_end = cluster_start + 1;
        while (cluster_end < entries.len and entries[cluster_end].cluster == cluster) : (cluster_end += 1) {}

        const cluster_len = cluster_end - cluster_start;
        const groups = try bulk_build.planBalancedGroupSizes(self.alloc, cluster_len, max_group_size);
        errdefer self.alloc.free(groups);

        var entry_cursor = cluster_start;
        for (groups) |group_size| {
            const group = try self.alloc.alloc(usize, group_size);
            errdefer self.alloc.free(group);
            for (0..group_size) |i| {
                group[i] = entries[entry_cursor + i].point_index;
            }
            out[out_count] = try buildBulkParentFromNodeIndexes(self, txn, nodes, group, level);
            self.alloc.free(group);
            out_count += 1;
            entry_cursor += group_size;
        }
        self.alloc.free(groups);

        cluster_start = cluster_end;
    }

    return try self.alloc.realloc(out, out_count);
}

fn buildBulkParentFromNodeRange(
    self: anytype,
    txn: anytype,
    nodes: []BuiltBulkNode,
    level: u16,
) !BuiltBulkNode {
    const indexes = try self.alloc.alloc(usize, nodes.len);
    defer self.alloc.free(indexes);
    for (indexes, 0..) |*index, i| index.* = i;
    return try buildBulkParentFromNodeIndexes(self, txn, nodes, indexes, level);
}

fn buildBulkParentFromNodeIndexes(
    self: anytype,
    txn: anytype,
    nodes: []BuiltBulkNode,
    indexes: []const usize,
    level: u16,
) !BuiltBulkNode {
    const node_id = self.nextNodeId();
    var child_ids = try self.alloc.alloc(u64, indexes.len);
    errdefer self.alloc.free(child_ids);

    const centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(centroid);
    @memset(centroid, 0);

    var merged_range: ?types.NodeSplitRange = null;
    errdefer if (merged_range) |*owned| owned.deinit(self.alloc);

    var member_count: usize = 0;
    for (indexes, 0..) |node_index, i| {
        const child = &nodes[node_index];
        child_ids[i] = child.node_id;
        addWeightedVector(centroid, child.centroid, child.member_count);
        member_count += child.member_count;
        try self.updateParent(txn, child.node_id, node_id);
        if (merged_range == null) {
            if (child.range) |range| merged_range = try range.clone(self.alloc);
        } else {
            var old_range = merged_range;
            merged_range = try bulk_build.mergeNodeSplitRanges(self.alloc, old_range, child.range);
            if (old_range) |*owned| owned.deinit(self.alloc);
        }
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(member_count)), centroid);
    normalizeCentroidForMetric(self, centroid);

    var node = types.Node{
        .id = node_id,
        .is_leaf = false,
        .level = level,
        .parent = 0,
        .centroid = centroid,
        .children = child_ids,
        .members = &.{},
    };
    node.covering_radius = try computeInternalCoveringRadius(self, txn, &node);
    try self.saveNodeBody(txn, &node);
    try self.putNodeSplitRange(txn, node_id, if (merged_range) |*owned| owned else null);
    self.alloc.free(child_ids);

    return .{
        .node_id = node_id,
        .centroid = centroid,
        .range = merged_range,
        .level = level,
        .member_count = member_count,
    };
}

fn buildBulkLeaf(
    self: anytype,
    txn: anytype,
    node_id: u64,
    inputs: []const bulk_build.PreparedBulkBuildInput,
    parent_id: u64,
    level: u16,
) !BuiltBulkNode {
    const centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(centroid);
    @memset(centroid, 0);

    var members = try self.alloc.alloc(u64, inputs.len);
    errdefer self.alloc.free(members);
    var range = try bulk_build.initNodeSplitRangeFromInput(self.alloc, inputs[0]);
    errdefer {
        var owned = range;
        owned.deinit(self.alloc);
    }

    for (inputs, 0..) |input, i| {
        members[i] = input.vector_id;
        vec.add(centroid, input.transformed);
        try self.putVecLeaf(txn, input.vector_id, node_id);
        try bulk_build.extendNodeSplitRangeFromInput(self.alloc, &range, input);
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(inputs.len)), centroid);
    normalizeCentroidForMetric(self, centroid);

    var node = types.Node{
        .id = node_id,
        .is_leaf = true,
        .level = level,
        .parent = parent_id,
        .centroid = centroid,
        .children = &.{},
        .members = members,
    };
    if (self.config.metric == .l2_squared) {
        const radius_matrix = try self.alloc.alloc(f32, inputs.len * self.config.dims);
        defer self.alloc.free(radius_matrix);
        for (inputs, 0..) |input, row| {
            @memcpy(radius_matrix[row * self.config.dims ..][0..self.config.dims], input.transformed);
        }
        node.covering_radius = l2CoveringRadiusForMatrix(node.centroid, radius_matrix, inputs.len);
    }
    try self.saveNodeBody(txn, &node);
    try self.putNodeSplitRange(txn, node_id, &range);
    self.alloc.free(members);

    return .{
        .node_id = node_id,
        .centroid = centroid,
        .range = range,
        .level = level,
        .member_count = inputs.len,
    };
}

fn addWeightedVector(dst: []f32, src: []const f32, count: usize) void {
    const weight: f32 = @floatFromInt(count);
    for (dst, src) |*d, s| {
        d.* += s * weight;
    }
}

fn sumBulkMemberCounts(nodes: []const BuiltBulkNode) usize {
    var total: usize = 0;
    for (nodes) |node| total += node.member_count;
    return total;
}

fn partitionBulkInputIndexesInPlace(
    self: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
    indexes: []usize,
    scratch: *BulkRecursiveScratch,
) !usize {
    const dims: usize = @intCast(self.config.dims);
    const vec_data = scratch.vec_data[0 .. indexes.len * dims];
    const positions = scratch.positions[0..indexes.len];
    const partitioned_indexes = scratch.partitioned_indexes[0..indexes.len];

    for (indexes, 0..) |input_idx, i| {
        const input = inputs[input_idx];
        positions[i] = @intCast(i);
        @memcpy(vec_data[i * dims ..][0..dims], input.transformed);
    }
    var vector_set = vec.Set{
        .dims = self.config.dims,
        .count = indexes.len,
        .data = vec_data,
    };
    const split = try self.splitVectorSet(&vector_set, positions);
    defer {
        if (split.c1.len > 0) self.alloc.free(split.c1);
        if (split.g1.len > 0) self.alloc.free(split.g1);
        if (split.c2.len > 0) self.alloc.free(split.c2);
        if (split.g2.len > 0) self.alloc.free(split.g2);
    }

    for (split.g1, 0..) |position, i| {
        partitioned_indexes[i] = indexes[@intCast(position)];
    }
    for (split.g2, 0..) |position, i| {
        partitioned_indexes[split.g1.len + i] = indexes[@intCast(position)];
    }
    if (split.g1.len == 0 or split.g2.len == 0) return error.UnbalancedBulkSplit;
    @memcpy(indexes, partitioned_indexes);
    return split.g1.len;
}

fn splitVectorSetKmeans(
    self: anytype,
    vectors: *const vec.Set,
    ids: []const u64,
) !SplitResult {
    const dims = self.config.dims;
    const count = ids.len;

    const left_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(left_centroid);
    const right_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(right_centroid);
    const new_left = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(new_left);
    const new_right = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(new_right);
    const assignments = try self.alloc.alloc(u64, count);
    defer self.alloc.free(assignments);
    const temp_dists = try self.alloc.alloc(f32, count);
    defer self.alloc.free(temp_dists);

    const left_idx = self.rng.intN(count);
    @memcpy(left_centroid, vectors.atConst(left_idx));

    var dist_sum: f32 = 0;
    var min_dist: f32 = std.math.inf(f32);
    for (0..count) |i| {
        var d = vec.distance(vectors.atConst(i), left_centroid, self.config.metric);
        if (self.config.metric == .inner_product) {
            const norm = vec.norm(vectors.atConst(i));
            if (norm != 0) d /= norm;
        }
        temp_dists[i] = d;
        dist_sum += d;
        min_dist = @min(min_dist, d);
    }
    dist_sum += @as(f32, @floatFromInt(count)) * -min_dist;
    if (min_dist != 0) {
        for (temp_dists) |*d| d.* -= min_dist;
    }
    if (dist_sum > 0) {
        vec.scale(1.0 / dist_sum, temp_dists);
    }
    var cum: f32 = 0;
    const rnd = self.rng.float32();
    var right_idx: usize = count - 1;
    for (temp_dists, 0..) |p, i| {
        cum += p;
        if (rnd < cum) {
            right_idx = i;
            break;
        }
    }
    @memcpy(right_centroid, vectors.atConst(right_idx));

    const tolerance = calcTolerance(self, vectors, count);
    const max_iter = self.config.kmeans_max_iter;
    for (0..max_iter) |_| {
        assignPartitions(self, vectors, count, left_centroid, right_centroid, assignments, temp_dists);
        calcPartitionCentroids(vectors, count, assignments, new_left, new_right);
        const left_shift = vec.l2SquaredDistance(left_centroid, new_left);
        const right_shift = vec.l2SquaredDistance(right_centroid, new_right);
        @memcpy(left_centroid, new_left);
        @memcpy(right_centroid, new_right);
        if (left_shift + right_shift <= tolerance) break;
    }

    assignPartitions(self, vectors, count, left_centroid, right_centroid, assignments, temp_dists);

    if (self.config.metric == .cosine) {
        _ = vec.normalize(left_centroid);
        _ = vec.normalize(right_centroid);
    }

    var g1_count: usize = 0;
    for (assignments) |a| {
        if (a == 0) g1_count += 1;
    }
    const g2_count = count - g1_count;

    const out_g1 = try self.alloc.alloc(u64, g1_count);
    errdefer self.alloc.free(out_g1);
    const out_g2 = try self.alloc.alloc(u64, g2_count);
    errdefer self.alloc.free(out_g2);

    var left_pos: usize = 0;
    var right_pos: usize = 0;
    for (assignments, 0..) |a, i| {
        if (a == 0) {
            out_g1[left_pos] = ids[i];
            left_pos += 1;
        } else {
            out_g2[right_pos] = ids[i];
            right_pos += 1;
        }
    }

    return .{
        .c1 = left_centroid,
        .g1 = out_g1,
        .c2 = right_centroid,
        .g2 = out_g2,
    };
}

fn splitVectorSetHilbert(
    self: anytype,
    vectors: *const vec.Set,
    ids: []const u64,
) !SplitResult {
    const dims = self.config.dims;
    const count = ids.len;
    const assignments = try self.alloc.alloc(u64, count);
    defer self.alloc.free(assignments);

    const Entry = struct {
        index: usize,
        embedding: []const u8,
    };
    const entries = try self.alloc.alloc(Entry, count);
    defer self.alloc.free(entries);

    const hilbert = try self.getHilbert();
    const embedding_len = hilbert.byteLen();
    const embeddings = try self.alloc.alloc(u8, count * embedding_len);
    defer self.alloc.free(embeddings);
    const coords = try self.alloc.alloc(u32, hilbert.dimension);
    defer self.alloc.free(coords);

    for (0..count) |i| {
        const embedding = embeddings[i * embedding_len ..][0..embedding_len];
        try hilbert.encodeVecBytesInto(vectors.atConst(i), coords, embedding);
        entries[i] = .{
            .index = i,
            .embedding = embedding,
        };
    }

    std.mem.sort(Entry, entries, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return std.mem.order(u8, a.embedding, b.embedding) == .lt;
        }
    }.lessThan);

    const split_point = count / 2;
    for (entries, 0..) |entry, i| {
        assignments[entry.index] = if (i < split_point) 0 else 1;
    }

    const left_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(left_centroid);
    const right_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(right_centroid);
    calcPartitionCentroids(vectors, count, assignments, left_centroid, right_centroid);

    if (self.config.metric == .cosine) {
        _ = vec.normalize(left_centroid);
        _ = vec.normalize(right_centroid);
    }

    var g1 = std.ArrayListUnmanaged(u64).empty;
    var g2 = std.ArrayListUnmanaged(u64).empty;
    for (assignments, 0..) |a, i| {
        if (a == 0) {
            try g1.append(self.alloc, ids[i]);
        } else {
            try g2.append(self.alloc, ids[i]);
        }
    }

    return .{
        .c1 = left_centroid,
        .g1 = try g1.toOwnedSlice(self.alloc),
        .c2 = right_centroid,
        .g2 = try g2.toOwnedSlice(self.alloc),
    };
}

fn assignPartitions(
    self: anytype,
    vectors: *const vec.Set,
    count: usize,
    left_centroid: []const f32,
    right_centroid: []const f32,
    assignments: []u64,
    temp_dists: []f32,
) void {
    const spherical = self.config.metric == .cosine;

    var inv_left_norm: f32 = 1;
    var inv_right_norm: f32 = 1;
    if (spherical) {
        const ln = vec.norm(left_centroid);
        if (ln != 0) inv_left_norm = 1.0 / ln;
        const rn = vec.norm(right_centroid);
        if (rn != 0) inv_right_norm = 1.0 / rn;
    }

    var left_count: usize = 0;
    for (0..count) |i| {
        const v = vectors.atConst(i);
        var left_dist: f32 = undefined;
        var right_dist: f32 = undefined;
        if (spherical) {
            left_dist = -vec.dot(v, left_centroid) * inv_left_norm;
            right_dist = -vec.dot(v, right_centroid) * inv_right_norm;
        } else if (self.config.metric == .inner_product) {
            left_dist = -vec.dot(v, left_centroid);
            right_dist = -vec.dot(v, right_centroid);
        } else {
            left_dist = vec.l2SquaredDistance(v, left_centroid);
            right_dist = vec.l2SquaredDistance(v, right_centroid);
        }
        temp_dists[i] = left_dist - right_dist;
        if (temp_dists[i] < 0) left_count += 1;
    }

    const min_count = (count * self.config.kmeans_min_balance_pct + 99) / 100;
    if (left_count >= min_count and (count - left_count) >= min_count) {
        for (0..count) |i| {
            assignments[i] = if (temp_dists[i] < 0) 0 else 1;
        }
        return;
    }

    const offsets = self.alloc.alloc(usize, count) catch return;
    defer self.alloc.free(offsets);
    for (0..count) |i| offsets[i] = i;

    stableSortOffsetsByDistance(offsets, temp_dists);

    var adj_left = left_count;
    if (adj_left < min_count) {
        adj_left = min_count;
    } else if (count - adj_left < min_count) {
        adj_left = count - min_count;
    }

    for (0..count) |i| {
        if (i < adj_left) {
            assignments[offsets[i]] = 0;
        } else {
            assignments[offsets[i]] = 1;
        }
    }
}

fn stableSortOffsetsByDistance(offsets: []usize, distances: []const f32) void {
    var i: usize = 1;
    while (i < offsets.len) : (i += 1) {
        const key = offsets[i];
        const key_dist = distances[key];
        var j = i;
        while (j > 0 and distances[offsets[j - 1]] > key_dist) : (j -= 1) {
            offsets[j] = offsets[j - 1];
        }
        offsets[j] = key;
    }
}

fn calcPartitionCentroids(
    vectors: *const vec.Set,
    count: usize,
    assignments: []const u64,
    c0: []f32,
    c1: []f32,
) void {
    @memset(c0, 0);
    @memset(c1, 0);
    var n0: usize = 0;
    var n1: usize = 0;
    for (0..count) |i| {
        const v = vectors.atConst(i);
        if (assignments[i] == 0) {
            vec.add(c0, v);
            n0 += 1;
        } else {
            vec.add(c1, v);
            n1 += 1;
        }
    }
    if (n0 > 0) vec.scale(1.0 / @as(f32, @floatFromInt(n0)), c0);
    if (n1 > 0) vec.scale(1.0 / @as(f32, @floatFromInt(n1)), c1);
}

fn calcTolerance(self: anytype, vectors: *const vec.Set, count: usize) f32 {
    if (count < 2) return 0;
    const dims = self.config.dims;

    const means = self.alloc.alloc(f32, dims) catch return 0;
    defer self.alloc.free(means);
    const m2 = self.alloc.alloc(f32, dims) catch return 0;
    defer self.alloc.free(m2);

    @memset(means, 0);
    @memset(m2, 0);

    for (0..count) |i| {
        const v = vectors.atConst(i);
        const sample_index: f32 = @floatFromInt(i + 1);
        for (0..dims) |d| {
            const delta = v[d] - means[d];
            means[d] += delta / sample_index;
            const delta2 = v[d] - means[d];
            m2[d] += delta * delta2;
        }
    }

    var variance_sum: f32 = 0;
    const inv_count_minus_one = 1.0 / @as(f32, @floatFromInt(count - 1));
    for (0..dims) |d| {
        variance_sum += m2[d] * inv_count_minus_one;
    }
    return (variance_sum / @as(f32, @floatFromInt(dims))) * 1e-4;
}

fn buildOrderedLeafSplit(
    self: anytype,
    vectors: *const vec.Set,
    entries: []const LeafKeyEntry,
    left_count: usize,
) !SplitResult {
    const dims = self.config.dims;
    const left_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(left_centroid);
    const right_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(right_centroid);
    @memset(left_centroid, 0);
    @memset(right_centroid, 0);

    const g1 = try self.alloc.alloc(u64, left_count);
    errdefer self.alloc.free(g1);
    const g2 = try self.alloc.alloc(u64, entries.len - left_count);
    errdefer self.alloc.free(g2);

    for (entries[0..left_count], 0..) |entry, i| {
        g1[i] = entry.member_id;
        vec.add(left_centroid, vectors.atConst(entry.index));
    }
    for (entries[left_count..], 0..) |entry, i| {
        g2[i] = entry.member_id;
        vec.add(right_centroid, vectors.atConst(entry.index));
    }

    vec.scale(1.0 / @as(f32, @floatFromInt(g1.len)), left_centroid);
    vec.scale(1.0 / @as(f32, @floatFromInt(g2.len)), right_centroid);
    if (self.config.metric == .cosine) {
        _ = vec.normalize(left_centroid);
        _ = vec.normalize(right_centroid);
    }

    return .{
        .c1 = left_centroid,
        .g1 = g1,
        .c2 = right_centroid,
        .g2 = g2,
    };
}

fn splitObjective(
    self: anytype,
    vectors: *const vec.Set,
    member_ids: []const u64,
    split: *const SplitResult,
) !f32 {
    var left = std.AutoHashMapUnmanaged(u64, void).empty;
    defer left.deinit(self.alloc);
    try left.ensureTotalCapacity(self.alloc, @intCast(split.g1.len));
    for (split.g1) |id| left.putAssumeCapacity(id, {});

    var total: f32 = 0;
    for (member_ids, 0..) |member_id, i| {
        const centroid = if (left.contains(member_id)) split.c1 else split.c2;
        total += vec.distance(vectors.atConst(i), centroid, self.config.metric);
    }
    return total;
}

fn orderedLeafSplitObjective(
    self: anytype,
    vectors: *const vec.Set,
    entries: []const LeafKeyEntry,
    left_count: usize,
) !f32 {
    const dims = self.config.dims;
    const left_centroid = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(left_centroid);
    const right_centroid = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(right_centroid);
    @memset(left_centroid, 0);
    @memset(right_centroid, 0);

    for (entries[0..left_count]) |entry| vec.add(left_centroid, vectors.atConst(entry.index));
    for (entries[left_count..]) |entry| vec.add(right_centroid, vectors.atConst(entry.index));
    vec.scale(1.0 / @as(f32, @floatFromInt(left_count)), left_centroid);
    vec.scale(1.0 / @as(f32, @floatFromInt(entries.len - left_count)), right_centroid);
    if (self.config.metric == .cosine) {
        _ = vec.normalize(left_centroid);
        _ = vec.normalize(right_centroid);
    }

    var total: f32 = 0;
    for (entries[0..left_count]) |entry| total += vec.distance(vectors.atConst(entry.index), left_centroid, self.config.metric);
    for (entries[left_count..]) |entry| total += vec.distance(vectors.atConst(entry.index), right_centroid, self.config.metric);
    return total;
}

fn validateQuantizedSet(self: anytype, qs: *const hbc_runtime.QuantizedSet, expected_count: usize) !void {
    switch (qs.*) {
        .nonquant => |*set| {
            const dims: usize = @intCast(set.vectors.dims);
            if (dims != self.config.dims) return error.Corrupted;
            if (set.getCount() != expected_count) return error.Corrupted;
            if (set.vectors.data.len != expected_count * self.config.dims) return error.Corrupted;
        },
        .rabit => |*set| {
            if (set.metric != self.config.metric) return error.Corrupted;
            if (set.centroid.len != self.config.dims) return error.Corrupted;
            if (set.getCount() != expected_count) return error.Corrupted;

            const expected_width = rabitq.codeWidth(self.config.dims);
            if (set.codes.width != expected_width) return error.Corrupted;
            if (set.codes.count != expected_count) return error.Corrupted;
            if (set.codes.data.len != expected_count * expected_width) return error.Corrupted;
            if (set.code_counts.len != expected_count) return error.Corrupted;
            if (set.centroid_distances.len != expected_count) return error.Corrupted;
            if (set.quantized_dot_products.len != expected_count) return error.Corrupted;
            if (self.config.metric != .l2_squared and set.centroid_dot_products.len != expected_count) {
                return error.Corrupted;
            }
        },
    }
}

fn rebuildQuantizedSubtree(self: anytype, txn: anytype, node_id: u64) !void {
    var node = try loadNode(self, txn, node_id);
    defer node.deinit(self.alloc);
    if (!node.is_leaf) {
        for (node.children) |child_id| {
            try rebuildQuantizedSubtree(self, txn, child_id);
        }
    }
    try self.refreshQuantized(txn, &node);
}

fn deferQuantizedRebuild(options: anytype) bool {
    const Options = @TypeOf(options);
    if (@hasField(Options, "defer_quantized_rebuild")) {
        return @field(options, "defer_quantized_rebuild");
    }
    return false;
}

fn isNotFoundGeneric(err: anyerror) bool {
    return err == error.NotFound;
}

/// Approximate searches tolerate stale topology references so repair can run
/// without turning a partially useful index into an outage. Full-effort
/// searches promise coverage of the published snapshot, so the same missing
/// node must be surfaced instead of silently weakening that contract.
fn handleTraversalNodeLoadError(err: anyerror, coverage_policy: search_types.CoveragePolicy) !void {
    if (!isNotFoundGeneric(err)) return err;
    if (coverage_policy == .complete_snapshot) return error.IncompletePublishedSnapshot;
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

fn now_fn_u64_adapter(now_fn: fn () u64) fn () i128 {
    return struct {
        const inner = now_fn;
        fn call() i128 {
            return @intCast(inner());
        }
    }.call;
}

fn elapsed_fn_u64_adapter(elapsed_fn: fn (u64) u64) fn (i128) u64 {
    return struct {
        const inner = elapsed_fn;
        fn call(start: i128) u64 {
            return inner(@intCast(start));
        }
    }.call;
}
