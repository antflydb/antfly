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

const hilbert_coord_stack_capacity = 2048;
const flat_centroid_search_probe_stack_capacity = 512;
const flat_centroid_posting_prefetch_stack_capacity = 512;
const bulk_parent_child_ids_stack_capacity = 256;

fn debugHitFromApprox(item: search_results.ApproxSearchResult) search_types.DebugHit {
    return .{
        .id = item.vector_id,
        .distance = item.distance,
        .error_bound = item.error_bound,
        .lower_bound = item.distance - item.error_bound,
        .upper_bound = item.distance + item.error_bound,
    };
}

fn deinitOptionalBaseDataBatch(alloc: Allocator, values: []?[]u8) void {
    for (values) |maybe_value| {
        if (maybe_value) |value| alloc.free(value);
    }
    alloc.free(values);
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

    pub fn take(self: *BuiltBulkNode) BuiltBulkNode {
        const out = self.*;
        self.centroid = &.{};
        self.range = null;
        return out;
    }

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
const small_metadata_batch_count = 64;
const small_coalesced_batch_count = 512;

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

fn CachedNodeReadHandle(comptime T: type) type {
    const Index = childType(T);
    const Borrowed = if (comptime @hasDecl(Index, "BorrowedNode")) Index.BorrowedNode else void;
    return union(enum) {
        borrowed: Borrowed,
        owned: types.Node,

        fn ptr(self: *const @This()) *const types.Node {
            return switch (self.*) {
                .borrowed => |*lease| if (Borrowed == void) unreachable else lease.ptr(),
                .owned => |*node| node,
            };
        }

        fn deinit(self: *@This(), alloc: Allocator) void {
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

fn CachedQuantizedReadHandle(comptime T: type) type {
    const Index = childType(T);
    const Borrowed = if (comptime @hasDecl(Index, "BorrowedQuantized")) Index.BorrowedQuantized else void;
    return union(enum) {
        borrowed: Borrowed,
        owned: hbc_runtime.QuantizedSet,

        fn ptr(self: *const @This()) *const hbc_runtime.QuantizedSet {
            return switch (self.*) {
                .borrowed => |*lease| if (Borrowed == void) unreachable else lease.ptr(),
                .owned => |*qs| qs,
            };
        }

        fn deinit(self: *@This(), alloc: Allocator) void {
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

fn cacheSearchNode(self: anytype, node: *const types.Node) !void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "cacheSearchNode")) {
        try self.cacheSearchNode(node);
    } else {
        try self.cacheNode(node);
    }
}

fn borrowCachedVectorHandle(self: anytype, vector_id: u64) ?CachedVectorReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedVector")) {
        if (self.borrowCachedVector(vector_id)) |borrowed| return .{ .borrowed = borrowed };
    }
    return null;
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

fn hasOpenBulkIngestSession(self: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(self))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(self),
    };
    if (comptime @hasField(Index, "bulk_ingest_session_depth")) {
        return self.bulk_ingest_session_depth > 0;
    }
    return false;
}

fn borrowCachedMetadataHandle(self: anytype, vector_id: u64) ?CachedMetadataReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "borrowCachedMetadata")) {
        if (self.borrowCachedMetadata(vector_id)) |borrowed| return .{ .borrowed = borrowed };
    }
    return null;
}

fn loadNodeReadHandleProfiled(
    self: anytype,
    txn: anytype,
    node_id: u64,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !CachedNodeReadHandle(@TypeOf(self)) {
    const lookup_start = now_fn();
    if (try borrowSearchCachedNodeHandle(self, node_id)) |cached| {
        profile.node_cache_lookup_ns += elapsed_fn(lookup_start);
        return cached;
    }
    profile.node_cache_lookup_ns += elapsed_fn(lookup_start);

    const start = now_fn();
    var loaded = try loadSearchNodeFromStorage(self, txn, node_id);
    errdefer loaded.deinit(self.alloc);
    try overlayBaseDeltaMembersOnLoadedNode(self, txn, &loaded);
    try self.cacheSearchNode(&loaded);
    profile.node_cache_miss_ns += elapsed_fn(start);
    profile.node_cache_misses += 1;
    return .{ .owned = loaded };
}

fn loadNodeReadHandle(
    self: anytype,
    txn: anytype,
    node_id: u64,
) !CachedNodeReadHandle(@TypeOf(self)) {
    if (try borrowSearchCachedNodeHandle(self, node_id)) |cached| return cached;

    var loaded = try loadSearchNodeFromStorage(self, txn, node_id);
    errdefer loaded.deinit(self.alloc);
    try overlayBaseDeltaMembersOnLoadedNode(self, txn, &loaded);
    try self.cacheSearchNode(&loaded);
    return .{ .owned = loaded };
}

const QuantizedProfileRole = enum {
    internal,
    leaf,
};

fn noteQuantizedProfileMiss(
    profile: *search_types.SearchProfile,
    role: QuantizedProfileRole,
    elapsed_ns: u64,
) void {
    profile.quantized_cache_miss_ns += elapsed_ns;
    profile.quantized_cache_misses += 1;
    switch (role) {
        .internal => {
            profile.quantized_internal_cache_miss_ns += elapsed_ns;
            profile.quantized_internal_cache_misses += 1;
        },
        .leaf => {
            profile.quantized_leaf_cache_miss_ns += elapsed_ns;
            profile.quantized_leaf_cache_misses += 1;
        },
    }
}

fn loadQuantizedReadHandleProfiled(
    self: anytype,
    txn: anytype,
    node_id: u64,
    is_root: bool,
    expected_count: usize,
    role: QuantizedProfileRole,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
    is_not_found: fn (anyerror) bool,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    const Index = comptime childType(@TypeOf(self));
    const lookup_start = now_fn();
    if (comptime @hasDecl(Index, "borrowCachedQuantized")) {
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
    } else if (try self.getCachedQuantizedClone(node_id)) |cached| {
        profile.quantized_cache_lookup_ns += elapsed_fn(lookup_start);
        validateQuantizedSet(self, &cached, expected_count) catch |err| switch (err) {
            error.Corrupted => self.invalidateQuantizedCache(node_id),
        };
        if (try self.getCachedQuantizedClone(node_id)) |valid| return .{ .owned = valid };
    } else {
        profile.quantized_cache_lookup_ns += elapsed_fn(lookup_start);
    }

    const start = now_fn();
    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) {
            noteQuantizedProfileMiss(profile, role, elapsed_fn(start));
            return null;
        }
        return err;
    };
    noteQuantizedProfileMiss(profile, role, elapsed_fn(start));
    if (self.cache_enabled) {
        self.cacheQuantized(node_id, &decoded) catch {};
    }
    return .{ .owned = decoded };
}

fn loadQuantizedReadHandle(
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

    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) return null;
        return err;
    };
    if (self.cache_enabled) {
        self.cacheQuantized(node_id, &decoded) catch {};
    }
    return .{ .owned = decoded };
}

fn rebuildInternalQuantizedReadHandleForQuery(
    self: anytype,
    txn: anytype,
    node_id: u64,
    uses_nonquantized_payload: bool,
    child_ids: []const u64,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    if (child_ids.len == 0) return null;
    const dims: usize = @intCast(self.metadata.dims);
    const child_vectors = try self.alloc.alloc(f32, child_ids.len * dims);
    var child_vectors_owned = true;
    defer if (child_vectors_owned) self.alloc.free(child_vectors);

    for (child_ids, 0..) |child_id, i| {
        var child_handle = loadNodeReadHandleProfiled(self, txn, child_id, profile, now_fn, elapsed_fn) catch return null;
        defer child_handle.deinit(self.alloc);
        const child = child_handle.ptr();
        if (child.centroid.len < dims) return null;
        @memcpy(child_vectors[i * dims ..][0..dims], child.centroid[0..dims]);
    }

    var rebuilt: hbc_runtime.QuantizedSet = if (uses_nonquantized_payload) blk: {
        child_vectors_owned = false;
        break :blk .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(dims),
                .count = @intCast(child_ids.len),
                .data = child_vectors,
            },
        } };
    } else blk: {
        var node_handle = loadNodeReadHandleProfiled(self, txn, node_id, profile, now_fn, elapsed_fn) catch return null;
        defer node_handle.deinit(self.alloc);
        const node = node_handle.ptr();
        if (node.centroid.len < dims) return null;
        break :blk .{ .rabit = try self.quantizer.quantize(node.centroid[0..dims], child_vectors, child_ids.len) };
    };
    errdefer rebuilt.deinit(self.alloc);

    if (self.cache_enabled) {
        self.cacheQuantized(node_id, &rebuilt) catch {};
    }
    return .{ .owned = rebuilt };
}

fn rebuildLeafQuantizedReadHandleForQuery(
    self: anytype,
    txn: anytype,
    leaf_id: u64,
    uses_nonquantized_payload: bool,
    member_ids: []const u64,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !?CachedQuantizedReadHandle(@TypeOf(self)) {
    if (member_ids.len == 0) return null;
    const dims: usize = @intCast(self.metadata.dims);
    const vector_count = std.math.mul(usize, member_ids.len, dims) catch return null;
    try scratch.ensureVectorFetchCapacity(self.alloc, member_ids.len);
    const vectors = scratch.vector_batch[0..vector_count];
    loadTransformedVectorIdsIntoMatrix(self, txn, member_ids, vectors, .{}) catch return null;

    var rebuilt: hbc_runtime.QuantizedSet = if (uses_nonquantized_payload)
        .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(dims),
                .count = @intCast(member_ids.len),
                .data = try self.alloc.dupe(f32, vectors),
            },
        } }
    else blk: {
        var leaf_handle = loadNodeReadHandleProfiled(self, txn, leaf_id, profile, now_fn, elapsed_fn) catch return null;
        defer leaf_handle.deinit(self.alloc);
        const leaf = leaf_handle.ptr();
        if (leaf.centroid.len < dims) return null;
        break :blk .{ .rabit = try self.quantizer.quantize(leaf.centroid[0..dims], vectors, member_ids.len) };
    };
    errdefer rebuilt.deinit(self.alloc);

    if (self.cache_enabled) {
        self.cacheQuantized(leaf_id, &rebuilt) catch {};
    }
    return .{ .owned = rebuilt };
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
    return deferLeafSplitToBatchFinish(options) or
        shouldDeferLeafSplitToBulkFinish(self, options) or
        options.defer_leaf_splits_to_posting_maintenance;
}

fn shouldRecordDeferredOversizedLeafForSplitFinish(options: hbc_runtime.BatchInsertOptions) bool {
    return !options.defer_leaf_splits_to_posting_maintenance;
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

fn sortFixedKeyLookupsIfNeeded(lookups: []FixedKeyLookup) void {
    if (lookups.len < 2) return;
    for (lookups[1..], 1..) |lookup, i| {
        if (lessFixedKeyLookup({}, lookup, lookups[i - 1])) {
            std.mem.sort(FixedKeyLookup, lookups, {}, lessFixedKeyLookup);
            return;
        }
    }
}

test "fixed key lookup sort skips already sorted keys" {
    var key_1: [10]u8 = undefined;
    var key_2: [10]u8 = undefined;
    var key_3: [10]u8 = undefined;
    _ = hbc.encodeVecKey(&key_1, 1);
    _ = hbc.encodeVecKey(&key_2, 2);
    _ = hbc.encodeVecKey(&key_3, 3);

    var sorted = [_]FixedKeyLookup{
        .{ .item_index = 0, .vector_id = 1, .key = key_1 },
        .{ .item_index = 1, .vector_id = 2, .key = key_2 },
        .{ .item_index = 2, .vector_id = 3, .key = key_3 },
    };
    sortFixedKeyLookupsIfNeeded(sorted[0..]);
    try std.testing.expectEqual(@as(u64, 1), sorted[0].vector_id);
    try std.testing.expectEqual(@as(u64, 2), sorted[1].vector_id);
    try std.testing.expectEqual(@as(u64, 3), sorted[2].vector_id);

    var unsorted = [_]FixedKeyLookup{
        .{ .item_index = 2, .vector_id = 3, .key = key_3 },
        .{ .item_index = 0, .vector_id = 1, .key = key_1 },
        .{ .item_index = 1, .vector_id = 2, .key = key_2 },
    };
    sortFixedKeyLookupsIfNeeded(unsorted[0..]);
    try std.testing.expectEqual(@as(u64, 1), unsorted[0].vector_id);
    try std.testing.expectEqual(@as(u64, 2), unsorted[1].vector_id);
    try std.testing.expectEqual(@as(u64, 3), unsorted[2].vector_id);
}

pub fn loadNode(self: anytype, txn: anytype, node_id: u64) !types.Node {
    if (try self.getCachedNodeClone(node_id)) |cached| return cached;

    var loaded = try self.loadNodeFromStorage(txn, node_id);
    errdefer loaded.deinit(self.alloc);
    try overlayBaseDeltaMembersOnLoadedNode(self, txn, &loaded);
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
    try overlayBaseDeltaMembersOnLoadedNode(self, txn, &loaded);
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
    try posting.PostingStore.deletePostingArtifacts(self, txn, node_id, isNotFoundGeneric);
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
    const packed_value = try self.alloc.dupe(u8, packed_data);
    defer self.alloc.free(packed_value);
    var decoded = try hbc.decodePackedNodeValue(packed_value);
    var header = decoded.header;
    const old_parent = header.parent;
    header.parent = new_parent;
    decoded.header = header;
    _ = try hbc.encodePackedNodeValue(packed_value, decoded.header, decoded.centroid_bytes, decoded.ids_bytes);
    try self.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node_id, .packed_node), packed_value);
    self.invalidateNodeCache(node_id);
    const payload_shape_changed = (old_parent == 0) != (new_parent == 0);
    const base_delta_parent_handled = if (old_parent != new_parent)
        try updateBaseDeltaParentMetadata(self, txn, node_id, decoded, payload_shape_changed)
    else
        false;
    if (self.config.use_quantization and payload_shape_changed) {
        self.invalidateQuantizedCache(node_id);
        if (base_delta_parent_handled) return;
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
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id));
    const view = try vectorViewFromRaw(data, scratch);
    return try self.cacheVector(vector_id, view);
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
    var key_buf: [10]u8 = undefined;
    const key = hbc.encodeVecKey(&key_buf, vector_id);
    const entry = (try cursor.seekAtOrAfter(key)) orelse return error.NotFound;
    if (!std.mem.eql(u8, entry.key, key)) return error.NotFound;
    const view = try vectorViewFromRaw(entry.value, scratch);
    return try self.cacheVector(vector_id, view);
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
    var key_buf: [10]u8 = undefined;
    const data = try self.getNamespaced(txn, .vecs, hbc.encodeVecKey(&key_buf, vector_id));
    const view = try vectorViewFromRaw(data, scratch);
    return try self.cacheVector(vector_id, view);
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

pub fn deleteVecLeaf(self: anytype, txn: anytype, vector_id: u64) !void {
    try posting.AssignmentMap.delete(self, txn, vector_id);
}

pub fn loadMetadataRaw(self: anytype, txn: anytype, vector_id: u64, is_not_found: fn (anyerror) bool) !?[]const u8 {
    if (self.getCachedMetadata(vector_id)) |cached| return cached;
    var key_buf: [10]u8 = undefined;
    const data = self.getNamespaced(txn, .vecs, hbc.encodeVecMetaKey(&key_buf, vector_id)) catch |err| {
        if (is_not_found(err)) return null;
        return err;
    };
    return try self.cacheMetadata(vector_id, data);
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

fn savePackedNodeValueWithLeafMembers(self: anytype, txn: anytype, node: *const types.Node, include_leaf_members: bool) !void {
    const header = hbc.NodeHeader{
        .is_leaf = node.is_leaf,
        .level = node.level,
        .parent = node.parent,
    };
    const centroid_bytes = std.mem.sliceAsBytes(node.centroid);
    const ids_bytes = if (node.is_leaf)
        if (include_leaf_members) std.mem.sliceAsBytes(node.members) else &.{}
    else
        std.mem.sliceAsBytes(node.children);
    const packed_len = hbc.packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    const packed_value = try self.alloc.alloc(u8, packed_len);
    defer self.alloc.free(packed_value);
    const encoded = try hbc.encodePackedNodeValue(packed_value, header, centroid_bytes, ids_bytes);
    var key_buf: [12]u8 = undefined;
    try self.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node.id, .packed_node), encoded);
}

fn savePackedNodeValue(self: anytype, txn: anytype, node: *const types.Node) !void {
    try savePackedNodeValueWithLeafMembers(self, txn, node, true);
}

fn shouldWritePostingBaseDelta(self: anytype) bool {
    return switch (self.config.posting_storage_mode) {
        .shadow_base_delta, .base_delta => true,
        .packed_hbc => false,
    };
}

fn shouldUseBaseDeltaAsCanonicalPosting(self: anytype) bool {
    return self.config.posting_storage_mode == .base_delta;
}

fn shouldRouteExistingUpdatesByCentroidOnly(self: anytype) bool {
    return shouldUseBaseDeltaAsCanonicalPosting(self) and self.config.lazy_posting_maintenance;
}

fn shouldUseCursorTxnForBatchWrites(self: anytype, options: hbc_runtime.BatchInsertOptions) bool {
    return shouldUseBaseDeltaAsCanonicalPosting(self) and !options.assume_absent_ids;
}

fn centroidDirectoryFlagsFromPostingState(state: types.PostingState) u8 {
    var flags: u8 = 0;
    if (state.dirty) flags |= posting.CentroidDirectoryFormat.dirty_flag;
    if (state.centroid_dirty) flags |= posting.CentroidDirectoryFormat.centroid_dirty_flag;
    if (state.payload_dirty) flags |= posting.CentroidDirectoryFormat.payload_dirty_flag;
    return flags;
}

fn saveCentroidDirectoryForLeafWithRadius(self: anytype, txn: anytype, node: *const types.Node, bounds_radius: f32) !void {
    if (!node.is_leaf or !shouldWritePostingBaseDelta(self)) return;
    try posting.PostingStore.saveCentroidDirectoryRecord(self, txn, .{
        .posting_id = node.id,
        .generation = node.posting_state.centroid_version,
        .mutation_version = node.posting_state.mutation_version,
        .payload_version = node.posting_state.payload_version,
        .flags = centroidDirectoryFlagsFromPostingState(node.posting_state),
        .parent = node.parent,
        .level = node.level,
        .member_count = node.members.len,
        .bounds_radius = bounds_radius,
        .centroid = node.centroid,
    });
}

fn shadowSavePostingBaseWithRadiusOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    bounds_radius: f32,
    publish_centroid_directory: bool,
) !void {
    if (!node.is_leaf or !shouldWritePostingBaseDelta(self)) return;
    if (!publish_centroid_directory and shouldUseBaseDeltaAsCanonicalPosting(self)) {
        try posting.PostingStore.saveBase(self, txn, .{
            .posting_id = node.id,
            .generation = node.posting_state.mutation_version,
            .members = node.members,
        });
        return;
    }
    try posting.PostingStore.saveBaseAndCentroidDirectoryRecord(self, txn, .{
        .posting_id = node.id,
        .generation = node.posting_state.mutation_version,
        .members = node.members,
    }, .{
        .posting_id = node.id,
        .generation = node.posting_state.centroid_version,
        .mutation_version = node.posting_state.mutation_version,
        .payload_version = node.posting_state.payload_version,
        .flags = centroidDirectoryFlagsFromPostingState(node.posting_state),
        .parent = node.parent,
        .level = node.level,
        .member_count = node.members.len,
        .bounds_radius = bounds_radius,
        .centroid = node.centroid,
    });
}

fn shadowSavePostingBaseWithRadius(self: anytype, txn: anytype, node: *const types.Node, bounds_radius: f32) !void {
    try shadowSavePostingBaseWithRadiusOptions(self, txn, node, bounds_radius, true);
}

fn shadowSavePostingBase(self: anytype, txn: anytype, node: *const types.Node) !void {
    try shadowSavePostingBaseWithRadius(self, txn, node, 0);
}

fn shadowAppendPostingDelta(
    self: anytype,
    txn: anytype,
    posting_id: posting.PostingId,
    op: posting.PostingDeltaOp,
    vector_id: posting.VectorId,
    sequence: u64,
) !void {
    if (!shouldWritePostingBaseDelta(self)) return;
    try posting.PostingStore.appendDelta(self, txn, posting_id, .{
        .sequence = sequence,
        .op = op,
        .vector_id = vector_id,
    });
}

fn shadowAppendPostingDeltas(
    self: anytype,
    txn: anytype,
    posting_id: posting.PostingId,
    records: []const posting.PostingDeltaRecord,
) !void {
    if (!shouldWritePostingBaseDelta(self)) return;
    try posting.PostingStore.appendDeltaRecords(self, txn, posting_id, records);
}

fn saveCentroidDirectoryForLeaf(self: anytype, txn: anytype, node: *const types.Node) !void {
    try saveCentroidDirectoryForLeafWithRadius(self, txn, node, 0);
}

fn ensurePostingBaseExists(self: anytype, txn: anytype, node: *const types.Node) !void {
    if (!node.is_leaf or !shouldUseBaseDeltaAsCanonicalPosting(self)) return;
    _ = posting.PostingStore.loadBaseHeader(self, txn, node.id, isNotFoundGeneric) catch |err| {
        if (!isNotFoundGeneric(err)) return err;
        try posting.PostingStore.saveBase(self, txn, .{
            .posting_id = node.id,
            .generation = node.posting_state.mutation_version,
            .members = node.members,
        });
        return;
    };
}

fn overlayBaseDeltaMembersOnLoadedNode(self: anytype, txn: anytype, node: *types.Node) !void {
    if (!node.is_leaf or !shouldUseBaseDeltaAsCanonicalPosting(self)) return;
    const materialized = posting.PostingStore.materializeBaseDeltaMembers(self, txn, node.id, isNotFoundGeneric) catch |err| {
        if (isNotFoundGeneric(err)) return;
        return err;
    };
    errdefer self.alloc.free(materialized);

    try node.ensureUnbacked(self.alloc);
    if (node.members.len > 0) self.alloc.free(node.members);
    node.members = materialized;
}

pub fn overlayBaseDeltaMembersForLoadedNode(self: anytype, txn: anytype, node: *types.Node) !void {
    try overlayBaseDeltaMembersOnLoadedNode(self, txn, node);
}

fn saveDeltaBackedLeafNodeValue(self: anytype, txn: anytype, node: *const types.Node) !void {
    if (!node.is_leaf or !shouldUseBaseDeltaAsCanonicalPosting(self)) {
        try savePackedNodeValue(self, txn, node);
        try shadowSavePostingBase(self, txn, node);
        return;
    }
    try ensurePostingBaseExists(self, txn, node);
    try savePackedNodeValueWithLeafMembers(self, txn, node, false);
    try saveCentroidDirectoryForLeaf(self, txn, node);
}

fn postingDeltaSequence(mutation_version: u64, offset: usize) u64 {
    const low_mask: u64 = 0xffff_ffff;
    const high = if (mutation_version > (std.math.maxInt(u64) >> 32))
        std.math.maxInt(u64) & ~low_mask
    else
        mutation_version << 32;
    return high | @min(@as(u64, @intCast(offset)), low_mask);
}

fn updateBaseDeltaParentMetadata(
    self: anytype,
    txn: anytype,
    node_id: u64,
    packed_value: hbc.PackedNodeValue,
    payload_shape_changed: bool,
) !bool {
    if (!shouldUseBaseDeltaAsCanonicalPosting(self)) return false;

    if (payload_shape_changed) {
        var qkey_buf: [10]u8 = undefined;
        self.deleteNamespaced(txn, .quant, hbc.encodeQuantKey(&qkey_buf, node_id)) catch {};
        self.invalidateQuantizedCache(node_id);
    }

    if (!packed_value.header.is_leaf) return true;
    if (packed_value.centroid_bytes.len % @sizeOf(f32) != 0) return error.Corrupted;

    var state = try posting.PostingStore.loadState(self, txn, node_id, isNotFoundGeneric);
    if (payload_shape_changed) {
        state.payload_dirty = true;
        state.dirty = true;
        try posting.PostingStore.saveState(self, txn, node_id, state);
    }

    const centroid = try self.alloc.alloc(f32, packed_value.centroid_bytes.len / @sizeOf(f32));
    defer self.alloc.free(centroid);
    @memcpy(std.mem.sliceAsBytes(centroid), packed_value.centroid_bytes);

    const member_count = blk: {
        if (packed_value.ids_bytes.len != 0) {
            if (packed_value.ids_bytes.len % @sizeOf(u64) != 0) return error.Corrupted;
            break :blk packed_value.ids_bytes.len / @sizeOf(u64);
        }
        const base_header = try posting.PostingStore.loadBaseHeader(self, txn, node_id, isNotFoundGeneric);
        break :blk base_header.member_count;
    };

    try posting.PostingStore.saveCentroidDirectoryRecord(self, txn, .{
        .posting_id = node_id,
        .generation = state.centroid_version,
        .mutation_version = state.mutation_version,
        .payload_version = state.payload_version,
        .flags = centroidDirectoryFlagsFromPostingState(state),
        .parent = packed_value.header.parent,
        .level = packed_value.header.level,
        .member_count = member_count,
        .bounds_radius = 0,
        .centroid = centroid,
    });
    return true;
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
    vectors_owned: ?*bool,
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
    const vector_count = count * dims;
    const can_take_vectors = vectors_owned != null and vectors_owned.?.* and vectors.len == vector_count;

    const compute_start = now_fn();
    var qs: hbc_runtime.QuantizedSet = if (usesNonQuantizedPayload(node))
        .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(dims),
                .count = @intCast(count),
                .data = if (can_take_vectors) @constCast(vectors[0..vector_count]) else try self.alloc.dupe(f32, vectors[0..vector_count]),
            },
        } }
    else
        .{ .rabit = try self.quantizer.quantize(node.centroid, vectors[0..vector_count], count) };
    if (can_take_vectors and usesNonQuantizedPayload(node)) vectors_owned.?.* = false;
    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
    defer qs.deinit(self.alloc);

    const store_start = now_fn();
    try saveQuantized(self, txn, node.id, &qs, now_fn, elapsed_fn);
    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
}

fn leafBoundsRadiusFromKnownVectors(self: anytype, node: *const types.Node, vectors: []const f32) !f32 {
    if (!node.is_leaf) return error.ExpectedLeaf;
    if (node.members.len == 0 or node.centroid.len == 0) return 0;
    const dims: usize = @intCast(self.metadata.dims);
    if (vectors.len < node.members.len * dims) return error.InvalidArgument;
    var radius: f32 = 0;
    for (0..node.members.len) |i| {
        const vector = vectors[i * dims ..][0..dims];
        radius = @max(radius, vec.distance(node.centroid, vector, self.config.metric));
    }
    return radius;
}

fn leafBoundsRadiusFromStorage(self: anytype, txn: anytype, node: *const types.Node) !f32 {
    if (!node.is_leaf) return error.ExpectedLeaf;
    if (node.members.len == 0 or node.centroid.len == 0) return 0;
    const dims: usize = @intCast(self.metadata.dims);
    const vectors = try self.alloc.alloc(f32, node.members.len * dims);
    defer self.alloc.free(vectors);
    try posting.PostingStore.loadTransformedVectorsForQuantizedRefresh(self, txn, node, vectors, .{});
    return try leafBoundsRadiusFromKnownVectors(self, node, vectors);
}

fn saveLeafNodeBodyWithKnownVectors(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    vectors: []const f32,
    vectors_owned: ?*bool,
    publish_centroid_directory: bool,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    if (!node.is_leaf) return error.InvalidArgument;

    const start = now_fn();
    defer {
        self.write_profile.save_node_ns += elapsed_fn(start);
        self.write_profile.save_node_calls += 1;
    }

    if (node.is_leaf and shouldUseBaseDeltaAsCanonicalPosting(self)) {
        const bounds_radius = try leafBoundsRadiusFromKnownVectors(self, node, vectors);
        try savePackedNodeValueWithLeafMembers(self, txn, node, false);
        try shadowSavePostingBaseWithRadiusOptions(self, txn, node, bounds_radius, publish_centroid_directory);
    } else {
        try saveDeltaBackedLeafNodeValue(self, txn, node);
    }
    try refreshQuantizedWithKnownVectors(self, txn, node, vectors, vectors_owned, nowNsU64Fixed, elapsedSinceU64Fixed);
    clearDeferredQuantizedNode(self, node.id);
    var posting_state_to_save = node.posting_state;
    if (node.is_leaf) {
        posting_state_to_save.notePayloadRefreshed();
        try posting.PostingStore.saveState(self, txn, node.id, posting_state_to_save);
        var node_for_cache = node.*;
        node_for_cache.posting_state = posting_state_to_save;
        try self.cacheNode(&node_for_cache);
    } else {
        try self.cacheNode(node);
    }
}

fn saveLeafNodeWithKnownVectors(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    vectors: []const f32,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    try saveLeafNodeBodyWithKnownVectors(self, txn, node, vectors, null, true, now_fn, elapsed_fn);
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

    if (copyMemberVectorsFromSourceOrder(out, node.members, source_ids, source_vectors, dims)) {
        return out;
    }

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

const SplitMemberVectorCopies = struct {
    storage: []f32 = &.{},
    left: []f32 = &.{},
    right: []f32 = &.{},

    pub fn deinit(self: *SplitMemberVectorCopies, alloc: Allocator) void {
        if (self.storage.len > 0) {
            alloc.free(self.storage);
        } else {
            if (self.left.len > 0) alloc.free(self.left);
            if (self.right.len > 0) alloc.free(self.right);
        }
        self.* = .{};
    }
};

fn copySplitMemberVectorsFromSourceOrder(
    self: anytype,
    left_node: *const types.Node,
    right_node: *const types.Node,
    source_ids: []const u64,
    source_vectors: []const f32,
) !?SplitMemberVectorCopies {
    if (!left_node.is_leaf or !right_node.is_leaf) return error.InvalidArgument;
    const dims: usize = @intCast(self.metadata.dims);
    if (source_vectors.len < source_ids.len * dims) return error.InvalidArgument;

    const left_len = left_node.members.len * dims;
    const right_len = right_node.members.len * dims;
    const storage = try self.alloc.alloc(f32, left_len + right_len);
    errdefer self.alloc.free(storage);
    const left = storage[0..left_len];
    const right = storage[left_len .. left_len + right_len];

    var left_index: usize = 0;
    var right_index: usize = 0;
    for (source_ids, 0..) |source_id, source_index| {
        const src = source_vectors[source_index * dims ..][0..dims];
        if (left_index < left_node.members.len and left_node.members[left_index] == source_id) {
            @memcpy(left[left_index * dims ..][0..dims], src);
            left_index += 1;
            continue;
        }
        if (right_index < right_node.members.len and right_node.members[right_index] == source_id) {
            @memcpy(right[right_index * dims ..][0..dims], src);
            right_index += 1;
        }
    }

    if (left_index == left_node.members.len and right_index == right_node.members.len) {
        return .{
            .storage = storage,
            .left = left,
            .right = right,
        };
    }

    self.alloc.free(storage);
    return null;
}

fn copyMemberVectorsFromSourceOrder(
    out: []f32,
    member_ids: []const u64,
    source_ids: []const u64,
    source_vectors: []const f32,
    dims: usize,
) bool {
    var source_index: usize = 0;
    for (member_ids, 0..) |member_id, i| {
        while (source_index < source_ids.len and source_ids[source_index] != member_id) {
            source_index += 1;
        }
        if (source_index == source_ids.len) return false;
        const src = source_vectors[source_index * dims ..][0..dims];
        const dst = out[i * dims ..][0..dims];
        @memcpy(dst, src);
        source_index += 1;
    }
    return true;
}

test "copy member vectors from source order copies ordered subsequences only" {
    const source_ids = [_]u64{ 10, 20, 30 };
    const source_vectors = [_]f32{ 1, 2, 3, 4, 5, 6 };
    var out: [4]f32 = undefined;

    try std.testing.expect(copyMemberVectorsFromSourceOrder(&out, &.{ 10, 30 }, &source_ids, &source_vectors, 2));
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 5, 6 }, &out);

    try std.testing.expect(!copyMemberVectorsFromSourceOrder(&out, &.{ 30, 10 }, &source_ids, &source_vectors, 2));
}

test "copy split member vectors from source order copies both sides in one pass" {
    const MockIndex = struct {
        alloc: Allocator,
        metadata: struct { dims: u32 },
    };

    var mock = MockIndex{
        .alloc = std.testing.allocator,
        .metadata = .{ .dims = 2 },
    };
    var left_members = [_]u64{ 10, 30 };
    var right_members = [_]u64{20};
    const source_ids = [_]u64{ 10, 20, 30 };
    const source_vectors = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const left = types.Node{
        .id = 1,
        .is_leaf = true,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &.{},
        .members = &left_members,
    };
    const right = types.Node{
        .id = 2,
        .is_leaf = true,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &.{},
        .members = &right_members,
    };

    var copies = (try copySplitMemberVectorsFromSourceOrder(&mock, &left, &right, &source_ids, &source_vectors)).?;
    defer copies.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 5, 6 }, copies.left);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, copies.right);

    var reordered_members = [_]u64{ 30, 10 };
    const reordered_left = types.Node{
        .id = 3,
        .is_leaf = true,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = &.{},
        .members = &reordered_members,
    };
    try std.testing.expect(try copySplitMemberVectorsFromSourceOrder(&mock, &reordered_left, &right, &source_ids, &source_vectors) == null);
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

    try saveDeltaBackedLeafNodeValue(self, txn, node);

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

fn saveExistingNodeBodyWithAddedVectorSourceOptions(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    source: anytype,
    options: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
) !void {
    var posting_state_to_save = node.posting_state;
    var posting_payload_refreshed = node.is_leaf and !self.config.use_quantization;
    const defer_posting_payload_refresh = shouldDeferPostingPayloadRefresh(self, node);

    try saveDeltaBackedLeafNodeValue(self, txn, node);

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
        } else if (try updateQuantizedWithAddedVectorSource(self, txn, node, source, now_fn, elapsed_fn, quant_start)) {
            posting_payload_refreshed = node.is_leaf;
        } else {
            _ = try primeDeferredLeafNonQuantCacheWithAddedVectorSource(self, txn, node, source, now_fn, elapsed_fn, quant_start);
            try recordDeferredQuantizedNode(self, node.id);
        }
    } else {
        const quant_start = now_fn();
        if (try updateQuantizedWithAddedVectorSource(self, txn, node, source, now_fn, elapsed_fn, quant_start)) {
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

fn saveDeferredBaseDeltaLeafMutationState(self: anytype, txn: anytype, node: *const types.Node) !void {
    if (!node.is_leaf or !shouldUseBaseDeltaAsCanonicalPosting(self)) return error.InvalidArgument;
    try ensurePostingBaseExists(self, txn, node);
    try posting.PostingStore.saveState(self, txn, node.id, node.posting_state);
    try self.cacheNode(node);
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

    if (added_vector != null) {
        try saveDeltaBackedLeafNodeValue(self, txn, node);
    } else {
        try savePackedNodeValue(self, txn, node);
        try shadowSavePostingBase(self, txn, node);
    }
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
    _ = self.getQuantized(txn, node.id, usesNonQuantizedPayload(node), previous_count) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    const cached = self.getCachedQuantizedPtr(node.id) orelse return false;

    switch (cached.*) {
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
    try self.putQuantizedCached(txn, node.id, cached);
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
        _ = try self.cacheQuantizedOwned(node.id, fresh);
        self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        return true;
    }

    _ = try self.getQuantized(txn, node.id, true, previous_count);
    const cached = self.getCachedQuantizedPtr(node.id) orelse return false;
    switch (cached.*) {
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
    _ = self.getQuantized(txn, node.id, usesNonQuantizedPayload(node), previous_count) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    const cached = self.getCachedQuantizedPtr(node.id) orelse return false;

    switch (cached.*) {
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
    try self.putQuantizedCached(txn, node.id, cached);
    self.write_profile.quantized_store_ns += elapsed_fn(store_start);
    self.write_profile.refresh_quantized_ns += elapsed_fn(compute_start);
    return true;
}

fn resizeQuantizedSlice(comptime T: type, alloc: Allocator, slice: []T, new_len: usize) ![]T {
    if (slice.len == 0) return try alloc.alloc(T, new_len);
    return try alloc.realloc(slice, new_len);
}

fn appendNonQuantVectorsFromSource(self: anytype, set: anytype, source: anytype) !void {
    const added_count = source.count();
    const old_len = set.vectors.data.len;
    const new_len = old_len + source.floatCount();
    set.vectors.dims = @intCast(self.config.dims);
    set.vectors.count += @intCast(added_count);
    if (set.vectors.data.len == 0) {
        set.vectors.data = try self.alloc.alloc(f32, new_len);
    } else {
        set.vectors.data = try self.alloc.realloc(set.vectors.data, new_len);
    }
    var write_offset = old_len;
    for (0..added_count) |i| {
        const transformed = source.vectorAt(i);
        @memcpy(set.vectors.data[write_offset..][0..transformed.len], transformed);
        write_offset += transformed.len;
    }
}

fn quantizeWithSetFromSource(self: anytype, set: *proto.RaBitQuantizedVectorSet, source: anytype) !void {
    const added_count = source.count();
    if (added_count == 0) return;

    const dims: usize = @intCast(self.config.dims);
    const width = rabitq.codeWidth(dims);
    const old_count = set.getCount();
    const new_count = old_count + added_count;

    set.codes.data = try resizeQuantizedSlice(u64, self.alloc, set.codes.data, new_count * width);
    set.codes.count = @intCast(new_count);
    set.codes.width = @intCast(width);
    set.code_counts = try resizeQuantizedSlice(u32, self.alloc, set.code_counts, new_count);
    set.centroid_distances = try resizeQuantizedSlice(f32, self.alloc, set.centroid_distances, new_count);
    set.quantized_dot_products = try resizeQuantizedSlice(f32, self.alloc, set.quantized_dot_products, new_count);

    if (self.quantizer.distance_metric != .l2_squared) {
        set.centroid_dot_products = try resizeQuantizedSlice(f32, self.alloc, set.centroid_dot_products, new_count);
        for (0..added_count) |i| {
            const transformed = source.vectorAt(i);
            set.centroid_dot_products[old_count + i] = vec.dot(transformed, set.centroid);
        }
    }

    const temp_diff_count = source.floatCount();
    var temp_diff_stack: [8192]f32 = undefined;
    const use_temp_diff_stack = temp_diff_count <= temp_diff_stack.len;
    const temp_diffs = if (use_temp_diff_stack)
        temp_diff_stack[0..temp_diff_count]
    else
        try self.alloc.alloc(f32, temp_diff_count);
    defer if (!use_temp_diff_stack) self.alloc.free(temp_diffs);

    for (0..added_count) |i| {
        const transformed = source.vectorAt(i);
        const diff = temp_diffs[i * dims ..][0..dims];
        vec.subTo(diff, transformed, set.centroid);
        const dist = vec.norm(diff);
        set.centroid_distances[old_count + i] = dist;
        if (dist != 0) vec.scale(1.0 / dist, diff);
    }

    rabitq.quantizeVectors(
        temp_diffs,
        set.codes.data[old_count * width ..],
        set.quantized_dot_products[old_count..],
        set.code_counts[old_count..],
        self.quantizer.sqrt_dims_inv,
        added_count,
        dims,
        width,
    );
}

fn updateQuantizedWithAddedVectorSource(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    source: anytype,
    now_fn: fn () i128,
    elapsed_fn: fn (i128) u64,
    compute_start: i128,
) !bool {
    if (!self.config.use_quantization) return false;
    if (!node.is_leaf) return false;
    if (node.centroid.len == 0) return false;
    if (node.members.len == 0) return false;
    const added_count = source.count();
    if (added_count == 0) return true;

    const previous_count = node.members.len - added_count;
    _ = self.getQuantized(txn, node.id, usesNonQuantizedPayload(node), previous_count) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    const cached = self.getCachedQuantizedPtr(node.id) orelse return false;

    switch (cached.*) {
        .nonquant => |*set| try appendNonQuantVectorsFromSource(self, set, source),
        .rabit => |*set| try quantizeWithSetFromSource(self, set, source),
    }
    noteMutatedCachedQuantized(self, node.id);
    self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);

    const store_start = now_fn();
    try self.putQuantizedCached(txn, node.id, cached);
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
        _ = try self.cacheQuantizedOwned(node.id, fresh);
        self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        return true;
    }

    _ = try self.getQuantized(txn, node.id, true, previous_count);
    const cached = self.getCachedQuantizedPtr(node.id) orelse return false;
    switch (cached.*) {
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
            self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
            return true;
        },
        .rabit => return false,
    }
}

fn primeDeferredLeafNonQuantCacheWithAddedVectorSource(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    source: anytype,
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
    const added_count = source.count();
    if (added_count == 0) return true;

    const previous_count = node.members.len - added_count;
    if (previous_count == 0) {
        const vector_data = try self.alloc.alloc(f32, source.floatCount());
        errdefer self.alloc.free(vector_data);
        var write_offset: usize = 0;
        for (0..added_count) |i| {
            const transformed = source.vectorAt(i);
            @memcpy(vector_data[write_offset..][0..transformed.len], transformed);
            write_offset += transformed.len;
        }
        const fresh: hbc_runtime.QuantizedSet = .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(self.config.dims),
                .count = @intCast(added_count),
                .data = vector_data,
            },
        } };
        _ = try self.cacheQuantizedOwned(node.id, fresh);
        self.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        return true;
    }

    _ = try self.getQuantized(txn, node.id, true, previous_count);
    const cached = self.getCachedQuantizedPtr(node.id) orelse return false;
    switch (cached.*) {
        .nonquant => |*set| {
            try appendNonQuantVectorsFromSource(self, set, source);
            noteMutatedCachedQuantized(self, node.id);
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

        for (node.members) |member_id| {
            const metadata = (try loadMetadataRaw(self, txn, member_id, is_not_found)) orelse continue;
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

    for (node.children) |child_id| {
        const child_range = blk: {
            if (try loadNodeSplitRange(self, txn, child_id, is_not_found)) |range| break :blk range;
            var child = try loadNode(self, txn, child_id);
            defer child.deinit(self.alloc);
            const computed = (try computeNodeSplitRange(self, txn, &child, is_not_found)) orelse continue;
            break :blk computed;
        };
        defer {
            var owned = child_range;
            owned.deinit(self.alloc);
        }

        if (min_key == null) {
            min_key = try self.alloc.dupe(u8, child_range.min_key);
            max_key = try self.alloc.dupe(u8, child_range.max_key);
            continue;
        }
        if (std.mem.order(u8, child_range.min_key, min_key.?) == .lt) {
            self.alloc.free(min_key.?);
            min_key = try self.alloc.dupe(u8, child_range.min_key);
        }
        if (std.mem.order(u8, child_range.max_key, max_key.?) == .gt) {
            self.alloc.free(max_key.?);
            max_key = try self.alloc.dupe(u8, child_range.max_key);
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
    var lookups_stack: [small_metadata_batch_count]FixedKeyLookup = undefined;
    const use_stack_lookups = vector_ids.len <= lookups_stack.len;
    const lookups = if (use_stack_lookups)
        lookups_stack[0..vector_ids.len]
    else
        try self.alloc.alloc(FixedKeyLookup, vector_ids.len);
    defer if (!use_stack_lookups) self.alloc.free(lookups);

    var key_views_stack: [small_metadata_batch_count][]const u8 = undefined;
    const use_stack_key_views = vector_ids.len <= key_views_stack.len;
    const key_views = if (use_stack_key_views)
        key_views_stack[0..vector_ids.len]
    else
        try self.alloc.alloc([]const u8, vector_ids.len);
    defer if (!use_stack_key_views) self.alloc.free(key_views);

    var values_stack: [small_metadata_batch_count]?[]const u8 = undefined;
    const use_stack_values = vector_ids.len <= values_stack.len;
    const values = if (use_stack_values)
        values_stack[0..vector_ids.len]
    else
        try self.alloc.alloc(?[]const u8, vector_ids.len);
    defer if (!use_stack_values) self.alloc.free(values);

    try getMetadataManySortedInTxnWithScratch(self, txn, vector_ids, out_metadata, lookups, key_views, values);
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
    if (vector_ids.len != out_metadata.len) return error.InvalidArgument;
    if (lookup_storage.len < vector_ids.len) return error.InvalidArgument;
    if (key_views_storage.len < vector_ids.len) return error.InvalidArgument;
    if (values_storage.len < vector_ids.len) return error.InvalidArgument;
    for (out_metadata) |*slot| slot.* = null;
    if (vector_ids.len == 0) return;

    var lookup_count: usize = 0;
    for (vector_ids, 0..) |vector_id, index| {
        if (self.getCachedMetadata(vector_id)) |cached| {
            out_metadata[index] = cached;
            continue;
        }
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
    sortFixedKeyLookupsIfNeeded(lookups);
    for (lookups, 0..) |*lookup, i| key_views[i] = lookup.key[0..];

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
    for (values, 0..) |maybe_value, i| {
        const value = maybe_value orelse continue;
        out_metadata[lookups[i].item_index] = try self.cacheMetadata(lookups[i].vector_id, value);
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

test "getMetadataManySortedInTxn uses stack scratch for small batches" {
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
        fn getManySorted(_: @This(), _: anytype, _: []const []const u8, values: []?[]const u8) !void {
            values[0] = "one";
            values[1] = "two";
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const index = TestIndex{ .alloc = failing.allocator() };
    var out_metadata: [2]?[]const u8 = .{ null, null };

    try getMetadataManySortedInTxn(index, TestTxn{}, &.{ 1, 2 }, out_metadata[0..]);
    try std.testing.expectEqualStrings("one", out_metadata[0].?);
    try std.testing.expectEqualStrings("two", out_metadata[1].?);
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

fn prefetchFlatProbePostingMembers(
    self: anytype,
    txn: anytype,
    probes: []const spfresh_index.FlatCentroidProbe,
    scratch: anytype,
    profile: *search_types.SearchProfile,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const Index = comptime childType(@TypeOf(self));
    const Scratch = comptime childType(@TypeOf(scratch));
    if (comptime !@hasDecl(Index, "loadPostingBackendBaseDataBatch")) return;
    if (comptime !@hasDecl(Scratch, "cachedCurrentPostingMembers") or
        !@hasDecl(Scratch, "notePostingMemberCacheMiss") or
        !@hasDecl(Scratch, "cachePostingMembers") or
        !@hasDecl(Scratch, "canCachePostingMemberCount") or
        !@hasDecl(Scratch, "postingMemberCacheAdmissionSkip"))
    {
        return;
    }
    if (self.config.posting_storage_mode != .base_delta or self.config.posting_backend != .segments) return;
    if (!scratch.posting_member_cache_admission_enabled or
        scratch.max_posting_member_cache_bytes == 0 or
        scratch.max_posting_member_cache_entry_bytes == 0)
    {
        return;
    }

    var prefetch_stack: [flat_centroid_posting_prefetch_stack_capacity * 2]u64 = undefined;
    const prefetch_storage_len = probes.len * 2;
    const use_stack = prefetch_storage_len <= prefetch_stack.len;
    const prefetch_storage = if (use_stack)
        prefetch_stack[0..prefetch_storage_len]
    else
        try self.alloc.alloc(u64, prefetch_storage_len);
    defer if (!use_stack) self.alloc.free(prefetch_storage);
    const ids = prefetch_storage[0..probes.len];
    const positions = prefetch_storage[probes.len..prefetch_storage_len];

    var prefetch_count: usize = 0;
    for (probes, 0..) |probe, i| {
        if (scratch.cachedCurrentPostingMembers(probe.posting_id, probe.state.mutation_version, 0) != null) continue;
        try scratch.notePostingMemberCacheMiss(self.alloc, probe.posting_id);
        ids[prefetch_count] = probe.posting_id;
        positions[prefetch_count] = @intCast(i);
        prefetch_count += 1;
    }
    if (prefetch_count == 0) return;

    const prefetch_start = now_fn_u64();
    const loaded = try self.loadPostingBackendBaseDataBatch(txn, self.alloc, ids[0..prefetch_count]);
    defer deinitOptionalBaseDataBatch(self.alloc, loaded);

    for (loaded, 0..) |maybe_base_data, i| {
        const base_data = maybe_base_data orelse continue;
        const probe = probes[@intCast(positions[i])];
        const header = try posting.PostingFormat.decodeBaseHeader(base_data);
        if (header.posting_id != probe.posting_id) return error.Corrupted;
        if (header.generation < probe.state.mutation_version) continue;
        if (!scratch.canCachePostingMemberCount(header.member_count)) {
            const result = scratch.postingMemberCacheAdmissionSkip();
            profile.posting_overlay_cache_admission_skips += result.admission_skips;
            profile.posting_overlay_cache_member_bytes = result.member_bytes;
            continue;
        }

        const decode_start = now_fn_u64();
        _ = try posting.PostingFormat.decodeBaseIntoScratch(self.alloc, scratch, base_data);
        profile.posting_base_decode_ns += elapsed_fn_u64(decode_start);
        profile.posting_base_decode_members += @intCast(header.member_count);

        const members = scratch.member_ids[0..header.member_count];
        const result = try scratch.cachePostingMembers(self.alloc, probe.posting_id, header.generation, probe.state.mutation_version, 0, members);
        if (result.inserted) profile.posting_overlay_cache_misses += 1;
        profile.posting_overlay_cache_evictions += result.evictions;
        profile.posting_overlay_cache_admission_skips += result.admission_skips;
        profile.posting_overlay_cache_member_bytes = result.member_bytes;
    }
    profile.posting_overlay_ns += elapsed_fn_u64(prefetch_start);
}

fn resolveFlatCentroidProbeLimit(configured_probe_count: usize, search_width: u32, candidate_limit: usize) usize {
    const dynamic_limit = @max(@as(usize, @intCast(search_width)), @max(candidate_limit, @as(usize, 1)));
    if (configured_probe_count == 0) return dynamic_limit;
    return @min(configured_probe_count, dynamic_limit);
}

fn usesFlatCentroidDirectorySearch(config: types.HBCConfig) bool {
    return config.centroid_directory_mode != .hbc and config.use_quantization;
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
    var profile = search_types.SearchProfile{};
    const total_start = now_fn_u64();
    hbc_runtime.beginSearchEpoch(self);
    defer hbc_runtime.endSearchEpoch(self);
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "publishedActiveCount")) {
        if (self.publishedActiveCount() == 0) {
            const empty = search_results.SearchResults.init(self.alloc, req.k);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{
                .results = empty,
                .profile = profile,
            };
        }
    }
    const setup_start = total_start;
    const txn_start = now_fn_u64();
    var txn = try self.beginRuntimeSearchTxn();
    profile.runtime_txn_ns += elapsed_fn_u64(txn_start);
    defer txn.abort();
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
    const scratch_allocation_count_start = if (scratch_handle.from_cache) scratch.allocationCount() else 0;
    const scratch_allocation_bytes_start = if (scratch_handle.from_cache) scratch.allocationBytes() else 0;
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
    const root_node_id = searchRootNode(self);

    var approx_results = try search_results.ApproxSearchResults.initCapacity(self.alloc, req.k, candidate_limit, candidate_limit);
    errdefer approx_results.deinit();
    profile.setup_ns += elapsed_fn_u64(setup_start);

    if (usesFlatCentroidDirectorySearch(self.config)) {
        const probe_limit = resolveFlatCentroidProbeLimit(self.config.flat_centroid_probe_count, search_width, candidate_limit);
        var probes_stack: [flat_centroid_search_probe_stack_capacity]spfresh_index.FlatCentroidProbe = undefined;
        const use_probes_stack = probe_limit <= probes_stack.len;
        var probes = if (use_probes_stack)
            probes_stack[0..probe_limit]
        else
            try self.alloc.alloc(spfresh_index.FlatCentroidProbe, probe_limit);
        defer if (!use_probes_stack) self.alloc.free(probes);

        const probe_count = try spfresh_index.selectFlatRabitqPostings(
            self,
            &txn,
            transformed_query,
            probe_limit,
            epsilon,
            probes,
            scratch,
            &profile,
            now_fn_u64,
            elapsed_fn_u64,
        );

        try prefetchFlatProbePostingMembers(self, &txn, probes[0..probe_count], scratch, &profile, now_fn_u64, elapsed_fn_u64);

        const packed_bulk_ingest_session = self.config.posting_storage_mode == .packed_hbc and hasOpenBulkIngestSession(self);
        var flat_leaves_scored: usize = 0;
        for (probes[0..probe_count]) |probe| {
            if (packed_bulk_ingest_session) continue;
            profile.nodes_visited += 1;
            if (self.config.posting_storage_mode == .base_delta) {
                const leaf_posting = posting.PostingView{
                    .id = probe.posting_id,
                    .parent = probe.parent,
                    .level = probe.level,
                    .centroid = &.{},
                    .members = &.{},
                    .state = probe.state,
                };
                const member_ids = try posting.PostingStore.copyQueryMemberIds(self, &txn, self.alloc, scratch, leaf_posting, &profile, now_fn_u64, elapsed_fn_u64);
                try @This().scoreLeafMemberIds(self, &txn, leaf_posting.id, leaf_posting.usesNonQuantizedPayload(), leaf_posting.hasFreshStoredPayload(), member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, now_fn_u64, elapsed_fn_u64);
            } else {
                var leaf_handle = loadNodeReadHandleProfiled(self, &txn, probe.posting_id, &profile, now_fn_u64, elapsed_fn_u64) catch |err| {
                    if (isNotFoundGeneric(err)) continue;
                    return err;
                };
                defer leaf_handle.deinit(self.alloc);
                const leaf = leaf_handle.ptr();
                if (!leaf.is_leaf) continue;
                const leaf_posting = try posting.PostingStore.view(leaf);
                const member_ids = try posting.PostingStore.copyQueryMemberIds(self, &txn, self.alloc, scratch, leaf_posting, &profile, now_fn_u64, elapsed_fn_u64);
                try @This().scoreLeafMemberIds(self, &txn, leaf_posting.id, leaf_posting.usesNonQuantizedPayload(), leaf_posting.hasFreshStoredPayload(), member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, now_fn_u64, elapsed_fn_u64);
            }
            profile.leaves_explored += 1;
            flat_leaves_scored += 1;
        }

        if (flat_leaves_scored > 0) {
            if (should_rerank) {
                const reranked = try rerankResults(self, &txn, &approx_results, req.query, exact_query_measure, req, &filter_state, scratch, &profile, now_fn_u64, elapsed_fn_u64);
                approx_results.deinit();
                finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
                profile.total_ns = elapsed_fn_u64(total_start);
                return .{ .results = reranked, .profile = profile };
            }

            var results = try approx_results.toFinalResults();
            approx_results.deinit();
            results.sort();
            if (req.load_metadata) try populateMetadata(self, &txn, &results);
            finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{ .results = results, .profile = profile };
        }

        if (packed_bulk_ingest_session) {
            approx_results.deinit();
            const empty = search_results.SearchResults.init(self.alloc, req.k);
            finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
            profile.total_ns = elapsed_fn_u64(total_start);
            return .{
                .results = empty,
                .profile = profile,
            };
        }
    }

    try pinUpperTreeCacheProfiled(self, &txn, &profile, now_fn_u64, elapsed_fn_u64);

    const candidate_capacity: usize = search_mod.candidateCapacity(search_width, self.metadata.branching_factor);
    var candidates = std.PriorityQueue(types.PriorityItem, void, search_types.candidateLessThan).initContext({});
    defer candidates.deinit(self.alloc);
    try candidates.ensureTotalCapacity(self.alloc, candidate_capacity);

    const root_start = now_fn_u64();
    var root_handle = loadNodeReadHandleProfiled(self, &txn, root_node_id, &profile, now_fn_u64, elapsed_fn_u64) catch |err| switch (err) {
        error.NotFound => {
            approx_results.deinit();
            const empty = search_results.SearchResults.init(self.alloc, req.k);
            finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
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

    {
        const root = root_handle.ptr();
        if (root.is_leaf) {
            const root_posting = try posting.PostingStore.view(root);
            const member_ids = try posting.PostingStore.copyQueryMemberIds(self, &txn, self.alloc, scratch, root_posting, &profile, now_fn_u64, elapsed_fn_u64);
            const leaf_id = root_posting.id;
            const leaf_uses_nonquantized_payload = root_posting.usesNonQuantizedPayload();
            const leaf_has_fresh_stored_payload = root_posting.hasFreshStoredPayload();
            root_handle.deinit(self.alloc);
            root_handle_active = false;
            @This().scoreLeafMemberIds(self, &txn, leaf_id, leaf_uses_nonquantized_payload, leaf_has_fresh_stored_payload, member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, now_fn_u64, elapsed_fn_u64) catch |err| switch (err) {
                error.NotFound => {
                    approx_results.deinit();
                    const empty = search_results.SearchResults.init(self.alloc, req.k);
                    finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
                    profile.total_ns = elapsed_fn_u64(total_start);
                    return .{
                        .results = empty,
                        .profile = profile,
                    };
                },
                else => return err,
            };
            if (should_rerank) {
                const reranked = try rerankResults(self, &txn, &approx_results, req.query, exact_query_measure, req, &filter_state, scratch, &profile, now_fn_u64, elapsed_fn_u64);
                approx_results.deinit();
                finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
                profile.total_ns = elapsed_fn_u64(total_start);
                return .{ .results = reranked, .profile = profile };
            }
            var results = try approx_results.toFinalResults();
            approx_results.deinit();
            results.sort();
            if (req.load_metadata) try populateMetadata(self, &txn, &results);
            finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
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
        try addChildCandidatesFromIds(self, &txn, root_id, root_uses_nonquantized_payload, root_child_ids, transformed_query, transformed_query_measure, &candidates, scratch, &profile, now_fn_u64, elapsed_fn_u64);
    }

    var beam_state = search_mod.BeamSearchState{};
    while (true) {
        const candidate = candidates.pop() orelse break;
        if (search_mod.shouldStopBeamSearch(&beam_state, search_width)) break;
        profile.nodes_visited += 1;

        var node_handle = loadNodeReadHandleProfiled(self, &txn, candidate.id, &profile, now_fn_u64, elapsed_fn_u64) catch continue;
        var node_handle_active = true;
        defer if (node_handle_active) node_handle.deinit(self.alloc);
        const node = node_handle.ptr();
        const allow_dynamic_pruning = self.config.metric != .inner_product;
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
            const member_ids = try posting.PostingStore.copyQueryMemberIds(self, &txn, self.alloc, scratch, leaf_posting, &profile, now_fn_u64, elapsed_fn_u64);
            const leaf_id = leaf_posting.id;
            const leaf_uses_nonquantized_payload = leaf_posting.usesNonQuantizedPayload();
            const leaf_has_fresh_stored_payload = leaf_posting.hasFreshStoredPayload();
            node_handle.deinit(self.alloc);
            node_handle_active = false;
            try @This().scoreLeafMemberIds(self, &txn, leaf_id, leaf_uses_nonquantized_payload, leaf_has_fresh_stored_payload, member_ids, transformed_query, transformed_query_measure, req.query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, now_fn_u64, elapsed_fn_u64);
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
            try addChildCandidatesFromIds(self, &txn, node_id, node_uses_nonquantized_payload, child_ids, transformed_query, transformed_query_measure, &candidates, scratch, &profile, now_fn_u64, elapsed_fn_u64);
        }
    }

    if (should_rerank) {
        const reranked = try rerankResults(self, &txn, &approx_results, req.query, exact_query_measure, req, &filter_state, scratch, &profile, now_fn_u64, elapsed_fn_u64);
        approx_results.deinit();
        finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
        profile.total_ns = elapsed_fn_u64(total_start);
        return .{ .results = reranked, .profile = profile };
    }

    var results = try approx_results.toFinalResults();
    approx_results.deinit();
    results.sort();
    if (req.load_metadata) try populateMetadata(self, &txn, &results);
    finishSearchProfileScratch(&profile, scratch, scratch_allocation_count_start, scratch_allocation_bytes_start);
    profile.total_ns = elapsed_fn_u64(total_start);
    return .{ .results = results, .profile = profile };
}

fn finishSearchProfileScratch(profile: *search_types.SearchProfile, scratch: anytype, allocation_count_start: u64, allocation_bytes_start: u64) void {
    profile.search_scratch_allocations = scratch.allocationCount() -| allocation_count_start;
    profile.search_scratch_allocation_bytes = scratch.allocationBytes() -| allocation_bytes_start;
    profile.search_scratch_retained_bytes = scratch.bytes();
}

fn publishSearchStateIfSupported(self: anytype) void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "shouldPublishSearchStateAfterWrite")) {
        if (!self.shouldPublishSearchStateAfterWrite()) return;
    }
    if (comptime @hasDecl(Index, "refreshPublishedSearchState")) {
        self.refreshPublishedSearchState();
    }
}

fn beginPublishSearchStateIfSupported(self: anytype) bool {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "shouldPublishSearchStateAfterWrite")) {
        if (!self.shouldPublishSearchStateAfterWrite()) return false;
    }
    if (comptime @hasDecl(Index, "beginPublishedSearchStateRefresh")) {
        self.beginPublishedSearchStateRefresh();
        return true;
    }
    return false;
}

fn finishPublishSearchStateIfSupported(self: anytype, publishing: bool) void {
    if (!publishing) {
        publishSearchStateIfSupported(self);
        return;
    }
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "finishPublishedSearchStateRefresh")) {
        self.finishPublishedSearchStateRefresh();
    }
}

fn abortPublishSearchStateIfSupported(self: anytype, publishing: bool) void {
    if (!publishing) return;
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "abortPublishedSearchStateRefresh")) {
        self.abortPublishedSearchStateRefresh();
    }
}

fn searchRootNode(self: anytype) u64 {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "publishedRootNode")) {
        return self.publishedRootNode();
    }
    return self.metadata.root_node;
}

fn pinUpperTreeCacheProfiled(
    self: anytype,
    txn: anytype,
    profile: *search_types.SearchProfile,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const Index = comptime childType(@TypeOf(self));
    if (comptime @hasDecl(Index, "pinUpperTreeCache")) {
        const start = now_fn_u64();
        try self.pinUpperTreeCache(txn);
        profile.upper_tree_pin_ns += elapsed_fn_u64(start);
    }
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
    return try addChildCandidatesFromIds(self, txn, node.id, usesNonQuantizedPayload(node), child_ids, query, query_measure, candidates, scratch, profile, now_fn_u64, elapsed_fn_u64);
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
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const start = now_fn_u64();
    defer profile.child_expand_ns += elapsed_fn_u64(start);
    const child_count = child_ids.len;
    if (self.config.use_quantization) {
        try scratch.ensureDistanceOnlyCapacity(self.alloc, child_count);
        if (try loadQuantizedReadHandleProfiled(self, txn, node_id, uses_nonquantized_payload, child_count, .internal, profile, now_fn_u64, elapsed_fn_u64, isNotFoundGeneric)) |quantized_handle| {
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
                try candidates.push(self.alloc, .{ .id = child_id, .distance = distances[i], .error_bound = 0 });
            }
            return;
        }
        if (try rebuildInternalQuantizedReadHandleForQuery(self, txn, node_id, uses_nonquantized_payload, child_ids, profile, now_fn_u64, elapsed_fn_u64)) |quantized_handle| {
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
                try candidates.push(self.alloc, .{ .id = child_id, .distance = distances[i], .error_bound = 0 });
            }
            return;
        }
    }

    for (child_ids) |child_id| {
        if (try borrowCachedNodeHandle(self, child_id)) |cached_handle| {
            defer {
                var handle = cached_handle;
                handle.deinit(self.alloc);
            }
            const dist = vec.distanceToQuery(query, query_measure, cached_handle.ptr().centroid, self.config.metric);
            try candidates.push(self.alloc, .{ .id = child_id, .distance = dist, .error_bound = 0 });
            continue;
        }

        var child_handle = loadNodeReadHandleProfiled(self, txn, child_id, profile, now_fn_u64, elapsed_fn_u64) catch continue;
        defer child_handle.deinit(self.alloc);
        const dist = vec.distanceToQuery(query, query_measure, child_handle.ptr().centroid, self.config.metric);
        try candidates.push(self.alloc, .{ .id = child_id, .distance = dist, .error_bound = 0 });
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
    const member_ids = try posting.PostingStore.copyQueryMemberIds(self, txn, self.alloc, scratch, leaf_posting, profile, now_fn_u64, elapsed_fn_u64);
    return try @This().scoreLeafMemberIds(self, txn, leaf_posting.id, leaf_posting.usesNonQuantizedPayload(), leaf_posting.hasFreshStoredPayload(), member_ids, approx_query, approx_query_measure, exact_query, exact_query_measure, req, filter_state, results, scratch, profile, now_fn_u64, elapsed_fn_u64);
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
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !void {
    const start = now_fn_u64();
    defer profile.leaf_score_ns += elapsed_fn_u64(start);
    const has_extra_filters = search_runtime.requestHasExtraFilters(req, filter_state);
    if (self.config.use_quantization and leaf_has_fresh_stored_payload) {
        if (try loadQuantizedReadHandleProfiled(self, txn, leaf_id, leaf_uses_nonquantized_payload, member_ids.len, .leaf, profile, now_fn_u64, elapsed_fn_u64, isNotFoundGeneric)) |quantized_handle| {
            defer {
                var handle = quantized_handle;
                handle.deinit(self.alloc);
            }
            const quantized = quantized_handle.ptr();
            profile.approx_leaves_scored += 1;
            const count = member_ids.len;
            try scratch.ensureDistanceOnlyCapacity(self.alloc, count);
            const distances = scratch.distances[0..count];
            const error_bounds = scratch.error_bounds[0..count];
            try self.estimateQuantizedDistances(quantized, approx_query, approx_query_measure, distances, error_bounds, &scratch.estimate);
            if (!has_extra_filters) {
                for (member_ids, 0..) |member_id, i| results.addApproxResult(member_id, distances[i], error_bounds[i]);
            } else {
                for (member_ids, 0..) |member_id, i| {
                    if (!try memberMatchesRequest(self, txn, member_id, distances[i], error_bounds[i], req, filter_state, true)) continue;
                    results.addApproxResult(member_id, distances[i], error_bounds[i]);
                }
            }
            profile.approx_vectors_scored += count;
            return;
        }
        if (try rebuildLeafQuantizedReadHandleForQuery(self, txn, leaf_id, leaf_uses_nonquantized_payload, member_ids, scratch, profile, now_fn_u64, elapsed_fn_u64)) |quantized_handle| {
            defer {
                var handle = quantized_handle;
                handle.deinit(self.alloc);
            }
            const quantized = quantized_handle.ptr();
            profile.approx_leaves_scored += 1;
            const count = member_ids.len;
            try scratch.ensureDistanceOnlyCapacity(self.alloc, count);
            const distances = scratch.distances[0..count];
            const error_bounds = scratch.error_bounds[0..count];
            try self.estimateQuantizedDistances(quantized, approx_query, approx_query_measure, distances, error_bounds, &scratch.estimate);
            if (!has_extra_filters) {
                for (member_ids, 0..) |member_id, i| results.addApproxResult(member_id, distances[i], error_bounds[i]);
            } else {
                for (member_ids, 0..) |member_id, i| {
                    if (!try memberMatchesRequest(self, txn, member_id, distances[i], error_bounds[i], req, filter_state, true)) continue;
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

    try scratch.ensureVectorFetchCapacity(self.alloc, member_ids.len);
    const fetch_member_ids = scratch.member_ids[0..member_ids.len];
    var fetch_count: usize = 0;
    for (member_ids) |member_id| {
        if (borrowCachedVectorHandle(self, member_id)) |cached_handle| {
            var handle = cached_handle;
            defer handle.deinit();
            const dist = vec.distanceToQuery(exact_query, exact_query_measure, handle.view(), self.config.metric);
            if (has_extra_filters and !try memberMatchesRequest(self, txn, member_id, dist, 0, req, filter_state, false)) {
                continue;
            }
            results.addResult(member_id, dist, 0);
            profile.exact_vectors_scored += 1;
            continue;
        }
        fetch_member_ids[fetch_count] = member_id;
        fetch_count += 1;
    }

    if (fetch_count == 0) return;

    const exact_distances = scratch.distances[0..fetch_count];
    var external_scored = false;
    const Index = comptime childType(@TypeOf(self));
    if (indexHasExternalVectorLoader(self) and comptime @hasDecl(Index, "scoreExternalVectorsSortedWithScratch")) {
        external_scored = try self.scoreExternalVectorsSortedWithScratch(
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
            if (!std.math.isFinite(dist)) continue;
            if (has_extra_filters and !try memberMatchesRequest(self, txn, member_id, dist, 0, req, filter_state, false)) {
                continue;
            }
            results.addResult(member_id, dist, 0);
            profile.exact_vectors_scored += 1;
        }
        return;
    }

    const vector_views = scratch.vector_views[0..fetch_count];
    try loadVectorIdsSortedWithScratch(
        self,
        txn,
        fetch_member_ids[0..fetch_count],
        vector_views,
        scratch.lookups,
        scratch.key_views,
        scratch.values,
        scratch.vector,
        scratch.vector_batch,
    );
    const scored_positions = scratch.positions[0..fetch_count];
    var scored_count: usize = 0;
    for (vector_views, 0..) |member_vec, i| {
        if (member_vec.len == 0) continue;
        vector_views[scored_count] = member_vec;
        scored_positions[scored_count] = i;
        scored_count += 1;
    }
    if (scored_count == 0) return;
    search_runtime.exactDistancesToStoredVectors(
        self.config,
        exact_query,
        exact_query_measure,
        vector_views[0..scored_count],
        exact_distances[0..scored_count],
    );
    for (scored_positions[0..scored_count], 0..) |member_index, dist_index| {
        const member_id = fetch_member_ids[member_index];
        const dist = exact_distances[dist_index];
        if (has_extra_filters and !try memberMatchesRequest(self, txn, member_id, dist, 0, req, filter_state, false)) {
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
    const start = now_fn_u64();
    defer profile.rerank_ns += elapsed_fn_u64(start);
    const ranked_items = approx_results.items.items;
    const has_extra_filters = search_runtime.requestHasExtraFilters(req, filter_state);
    try scratch.ensureQueryCapacity(self.alloc, ranked_items.len);

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
        try scratch.ensureDistanceOnlyCapacity(self.alloc, rerank_count);
        try scratch.ensureVectorBatchCapacity(self.alloc, rerank_count);

        const select_start = now_fn_u64();
        const rerank_positions = selectedRerankCandidatePositionsInto(ranked_items, rerank_selection.flags, scratch.positions[0..rerank_count]);
        profile.rerank_select_positions_ns += elapsed_fn_u64(select_start);
        const vector_views = scratch.vector_views[0..rerank_count];
        const exact_distances = scratch.distances[0..rerank_count];

        var external_scored = false;
        const Index = comptime childType(@TypeOf(self));
        if (indexHasExternalVectorLoader(self) and comptime @hasDecl(Index, "scoreExternalRerankVectorsSortedWithScratch")) {
            const score_start = now_fn_u64();
            external_scored = try self.scoreExternalRerankVectorsSortedWithScratch(
                txn,
                ranked_items,
                rerank_positions,
                query,
                query_measure,
                exact_distances,
                scratch.vector_ids,
                scratch.metadata,
                scratch.lookups,
                scratch.key_views,
                scratch.values,
                scratch.vector_batch,
                profile,
            );
            const score_elapsed = elapsed_fn_u64(score_start);
            profile.rerank_prefetch_ns += score_elapsed;
            profile.rerank_vector_load_ns += score_elapsed;
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
            );
            const preload_elapsed = elapsed_fn_u64(preload_start);
            profile.rerank_prefetch_ns += preload_elapsed;
            profile.rerank_vector_load_ns += preload_elapsed;

            var loaded_count: usize = 0;
            for (rerank_positions, 0..) |index, slot| {
                const item = &ranked_items[index];
                const member_vec = vector_views[slot];
                if (member_vec.len == 0) {
                    item.distance = std.math.inf(f32);
                    item.error_bound = 0;
                    continue;
                }
                vector_views[loaded_count] = member_vec;
                loaded_count += 1;
            }
            const dist_start = now_fn_u64();
            search_runtime.exactDistancesToStoredVectors(self.config, query, query_measure, vector_views[0..loaded_count], exact_distances[0..loaded_count]);
            profile.rerank_distance_ns += elapsed_fn_u64(dist_start);
        }

        const apply_start = now_fn_u64();
        var exact_idx: usize = 0;
        for (rerank_positions, 0..) |index, slot| {
            const item = &ranked_items[index];
            if (!std.math.isFinite(item.distance)) continue;
            const dist = if (external_scored) exact_distances[slot] else blk: {
                const value = exact_distances[exact_idx];
                exact_idx += 1;
                break :blk value;
            };
            if (!std.math.isFinite(dist)) {
                item.distance = std.math.inf(f32);
                item.error_bound = 0;
                continue;
            }
            if (has_extra_filters and !try memberMatchesRequest(self, txn, item.vector_id, dist, 0, req, filter_state, false)) {
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

        const resort_start = now_fn_u64();
        search_mod.sortApproxResultsByDistance(ranked_items);
        profile.rerank_resort_ns += elapsed_fn_u64(resort_start);
    }

    const finalize_start = now_fn_u64();
    var exact_results = try search_results.SearchResults.fromSortedApproxSlice(self.alloc, req.k, ranked_items);
    profile.rerank_finalize_ns += elapsed_fn_u64(finalize_start);

    if (req.load_metadata) {
        const metadata_start = now_fn_u64();
        try populateMetadataWithScratch(self, txn, &exact_results, scratch);
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
    if (rerankPositionsAreVectorIdOrdered(ranked_items, used)) return used;
    std.mem.sort(usize, used, ranked_items, struct {
        fn lessThan(items: []const search_results.ApproxSearchResult, a: usize, b: usize) bool {
            return items[a].vector_id < items[b].vector_id;
        }
    }.lessThan);
    return used;
}

fn rerankPositionsAreVectorIdOrdered(
    ranked_items: []const search_results.ApproxSearchResult,
    positions: []const usize,
) bool {
    if (positions.len < 2) return true;
    var previous = ranked_items[positions[0]].vector_id;
    for (positions[1..]) |position| {
        const current = ranked_items[position].vector_id;
        if (previous > current) return false;
        previous = current;
    }
    return true;
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
    try loadRerankVectorsSortedWithScratch(self, txn, ranked_items, rerank_positions, vector_views, vector_ids, lookups, key_views, values, scratch, batch_scratch);
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
) !void {
    if (vector_id_storage.len < rerank_positions.len) return error.InvalidArgument;
    const vector_ids = vector_id_storage[0..rerank_positions.len];
    for (rerank_positions, 0..) |index, slot| vector_ids[slot] = ranked_items[index].vector_id;
    try loadVectorIdsSortedWithScratch(self, txn, vector_ids, vector_views, lookup_storage, key_views_storage, values_storage, scratch, batch_scratch);
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
    const Index = comptime childType(@TypeOf(self));
    std.debug.assert(vector_views.len >= vector_ids.len);
    std.debug.assert(lookup_storage.len >= vector_ids.len);
    std.debug.assert(key_views_storage.len >= vector_ids.len);
    std.debug.assert(values_storage.len >= vector_ids.len);
    for (vector_views[0..vector_ids.len]) |*view| view.* = &.{};
    if (indexHasExternalVectorLoader(self)) {
        if (comptime @hasDecl(Index, "getExternalVectorViewsSortedWithScratch")) {
            if (try self.getExternalVectorViewsSortedWithScratch(
                txn,
                vector_ids,
                vector_views,
                lookup_storage,
                key_views_storage,
                values_storage,
                scratch,
                batch_scratch,
            )) return;
        }
        const dims = scratch.len;
        std.debug.assert(batch_scratch.len >= dims * vector_ids.len);
        for (vector_ids, 0..) |vector_id, slot| {
            const slot_scratch = batch_scratch[slot * dims ..][0..dims];
            vector_views[slot] = self.getVectorInto(txn, vector_id, slot_scratch) catch &.{};
        }
        return;
    }

    if (comptime !txnSupportsGetManySorted(@TypeOf(txn))) {
        const dims = scratch.len;
        std.debug.assert(batch_scratch.len >= dims * vector_ids.len);
        for (vector_ids, 0..) |vector_id, slot| {
            const slot_scratch = batch_scratch[slot * dims ..][0..dims];
            vector_views[slot] = self.getVectorInto(txn, vector_id, slot_scratch) catch &.{};
        }
        return;
    }

    var lookup_count: usize = 0;
    for (vector_ids, 0..) |vector_id, slot| {
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
        var key: [10]u8 = undefined;
        _ = hbc.encodeVecKey(&key, vector_id);
        lookup_storage[lookup_count] = .{
            .item_index = slot,
            .vector_id = vector_id,
            .key = key,
        };
        lookup_count += 1;
    }
    if (lookup_count == 0) return;

    const lookups = lookup_storage[0..lookup_count];
    const key_views = key_views_storage[0..lookup_count];
    const values = values_storage[0..lookup_count];
    sortFixedKeyLookupsIfNeeded(lookups);
    for (lookups, 0..) |*lookup, i| key_views[i] = lookup.key[0..];

    try txn.getManySorted(.vecs, key_views, values);
    for (values, 0..) |maybe_value, i| {
        const value = maybe_value orelse continue;
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
        const view = try vectorViewFromRaw(value, scratch);
        if (builtin.is_test and comptime @hasDecl(Index, "notifyVectorViewLoadForTest")) {
            self.notifyVectorViewLoadForTest(lookups[i].vector_id);
        }
        vector_views[lookups[i].item_index] = try self.cacheVector(lookups[i].vector_id, view);
    }
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

    const small_vector_count = 64;
    const small_vector_scratch_floats = 1024;
    const small_batch_scratch_floats = 4096;

    var missing_ids_stack: [small_vector_count]u64 = undefined;
    const use_stack_missing_ids = vector_ids.len <= missing_ids_stack.len;
    const missing_ids = if (use_stack_missing_ids)
        missing_ids_stack[0..vector_ids.len]
    else
        try self.alloc.alloc(u64, vector_ids.len);
    defer if (!use_stack_missing_ids) self.alloc.free(missing_ids);

    var missing_positions_stack: [small_vector_count]usize = undefined;
    const use_stack_missing_positions = vector_ids.len <= missing_positions_stack.len;
    const missing_positions = if (use_stack_missing_positions)
        missing_positions_stack[0..vector_ids.len]
    else
        try self.alloc.alloc(usize, vector_ids.len);
    defer if (!use_stack_missing_positions) self.alloc.free(missing_positions);

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

    var lookups_stack: [small_vector_count]FixedKeyLookup = undefined;
    const use_stack_lookups = missing_count <= lookups_stack.len;
    const lookups = if (use_stack_lookups)
        lookups_stack[0..missing_count]
    else
        try self.alloc.alloc(FixedKeyLookup, missing_count);
    defer if (!use_stack_lookups) self.alloc.free(lookups);

    var key_views_stack: [small_vector_count][]const u8 = undefined;
    const use_stack_key_views = missing_count <= key_views_stack.len;
    const key_views = if (use_stack_key_views)
        key_views_stack[0..missing_count]
    else
        try self.alloc.alloc([]const u8, missing_count);
    defer if (!use_stack_key_views) self.alloc.free(key_views);

    var values_stack: [small_vector_count]?[]const u8 = undefined;
    const use_stack_values = missing_count <= values_stack.len;
    const values = if (use_stack_values)
        values_stack[0..missing_count]
    else
        try self.alloc.alloc(?[]const u8, missing_count);
    defer if (!use_stack_values) self.alloc.free(values);

    var vector_scratch_stack: [small_vector_scratch_floats]f32 = undefined;
    const use_stack_vector_scratch = dims <= vector_scratch_stack.len;
    const vector_scratch = if (use_stack_vector_scratch)
        vector_scratch_stack[0..dims]
    else
        try self.alloc.alloc(f32, dims);
    defer if (!use_stack_vector_scratch) self.alloc.free(vector_scratch);

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

    var vector_views_stack: [small_vector_count][]const f32 = undefined;
    const use_stack_vector_views = missing_count <= vector_views_stack.len;
    const vector_views = if (use_stack_vector_views)
        vector_views_stack[0..missing_count]
    else
        try self.alloc.alloc([]const f32, missing_count);
    defer if (!use_stack_vector_views) self.alloc.free(vector_views);

    const batch_scratch_floats = std.math.mul(usize, missing_count, dims) catch return error.BufferTooSmall;
    var batch_scratch_stack: [small_batch_scratch_floats]f32 = undefined;
    const use_stack_batch_scratch = batch_scratch_floats <= batch_scratch_stack.len;
    const batch_scratch = if (use_stack_batch_scratch)
        batch_scratch_stack[0..batch_scratch_floats]
    else
        try self.alloc.alloc(f32, batch_scratch_floats);
    const batch_scratch_bytes = std.math.mul(usize, batch_scratch_floats, @sizeOf(f32)) catch return error.BufferTooSmall;
    if (!use_stack_batch_scratch) addApplyWorkspaceBytes(self, @intCast(batch_scratch_bytes));
    defer if (!use_stack_batch_scratch) {
        releaseApplyWorkspaceBytes(self, @intCast(batch_scratch_bytes));
        self.alloc.free(batch_scratch);
    };

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

test "loadTransformedVectorIdsIntoMatrix uses stack scratch for small fallback batches" {
    const TestTxn = struct {};
    const TestConfig = struct {
        dims: usize = 2,
    };
    const TestIndex = struct {
        alloc: std.mem.Allocator,
        config: TestConfig,
        calls: *usize,

        fn hasExternalVectorLoader(_: @This()) bool {
            return true;
        }

        fn transformVector(_: @This(), original: []const f32, transformed: []f32) []const f32 {
            @memcpy(transformed, original);
            return transformed;
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
                const view = batch_scratch[slot * self.config.dims ..][0..self.config.dims];
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

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var calls: usize = 0;
    const index = TestIndex{
        .alloc = failing.allocator(),
        .config = .{},
        .calls = &calls,
    };
    var matrix: [4]f32 = undefined;
    try loadTransformedVectorIdsIntoMatrix(index, TestTxn{}, &.{ 3, 4 }, matrix[0..], .{});

    try std.testing.expectEqual(@as(usize, 1), calls);
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

test "rerank selected positions skip sort when vector ids are already ordered" {
    const ranked_items = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 10, .distance = 0.10, .error_bound = 0.01 },
        .{ .vector_id = 20, .distance = 0.20, .error_bound = 0.01 },
        .{ .vector_id = 30, .distance = 0.30, .error_bound = 0.01 },
        .{ .vector_id = 40, .distance = 0.40, .error_bound = 0.01 },
    };
    const flags = [_]bool{ true, false, true, true };
    var positions: [3]usize = undefined;

    const selected = selectedRerankCandidatePositionsInto(ranked_items[0..], flags[0..], positions[0..]);
    try std.testing.expect(rerankPositionsAreVectorIdOrdered(ranked_items[0..], selected));
    try std.testing.expectEqualSlices(usize, &.{ 0, 2, 3 }, selected);
}

test "rerank selected positions sort when vector ids are out of order" {
    const ranked_items = [_]search_results.ApproxSearchResult{
        .{ .vector_id = 30, .distance = 0.10, .error_bound = 0.01 },
        .{ .vector_id = 10, .distance = 0.20, .error_bound = 0.01 },
        .{ .vector_id = 20, .distance = 0.30, .error_bound = 0.01 },
    };
    const flags = [_]bool{ true, true, true };
    var positions: [3]usize = undefined;

    const selected = selectedRerankCandidatePositionsInto(ranked_items[0..], flags[0..], positions[0..]);
    try std.testing.expect(rerankPositionsAreVectorIdOrdered(ranked_items[0..], selected));
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 0 }, selected);
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

test "flat centroid probe limit follows dynamic search width under configured cap" {
    try std.testing.expectEqual(@as(usize, 64), resolveFlatCentroidProbeLimit(0, 64, 10));
    try std.testing.expectEqual(@as(usize, 10), resolveFlatCentroidProbeLimit(0, 0, 10));
    try std.testing.expectEqual(@as(usize, 1), resolveFlatCentroidProbeLimit(0, 0, 0));
    try std.testing.expectEqual(@as(usize, 32), resolveFlatCentroidProbeLimit(256, 16, 32));
    try std.testing.expectEqual(@as(usize, 256), resolveFlatCentroidProbeLimit(256, 4096, 32));
}

test "flat centroid search probe stack covers default 256 probe window" {
    try std.testing.expect(flat_centroid_search_probe_stack_capacity >= 256);
}

test "flat centroid directory search predicate requires non-hbc quantized mode" {
    try std.testing.expect(!usesFlatCentroidDirectorySearch(.{
        .dims = 2,
        .centroid_directory_mode = .hbc,
        .use_quantization = true,
    }));
    try std.testing.expect(!usesFlatCentroidDirectorySearch(.{
        .dims = 2,
        .centroid_directory_mode = .two_level_rabitq,
        .use_quantization = false,
    }));
    try std.testing.expect(usesFlatCentroidDirectorySearch(.{
        .dims = 2,
        .centroid_directory_mode = .flat_rabitq,
        .use_quantization = true,
    }));
    try std.testing.expect(usesFlatCentroidDirectorySearch(.{
        .dims = 2,
        .centroid_directory_mode = .two_level_rabitq,
        .use_quantization = true,
    }));
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
    try self.bindTxnLike(txn);
    if (comptime txnSupportsGetManySorted(@TypeOf(txn))) {
        try populateMetadataBatched(self, txn, results);
        return;
    }
    for (results.items.items) |*item| {
        if (item.metadata != null) continue;
        const data = (try loadMetadataRaw(self, txn, item.vector_id, isNotFoundGeneric)) orelse continue;
        item.metadata = try self.alloc.dupe(u8, data);
    }
}

fn populateMetadataWithScratch(self: anytype, txn: anytype, results: *search_results.SearchResults, scratch: anytype) !void {
    try self.bindTxnLike(txn);
    if (comptime txnSupportsGetManySorted(@TypeOf(txn))) {
        try scratch.ensureQueryCapacity(self.alloc, results.items.items.len);
        try populateMetadataBatchedWithScratch(self, txn, results, scratch.lookups, scratch.key_views, scratch.values);
        return;
    }
    try populateMetadata(self, txn, results);
}

fn populateMetadataBatched(self: anytype, txn: anytype, results: *search_results.SearchResults) !void {
    var lookups_stack: [small_metadata_batch_count]FixedKeyLookup = undefined;
    const use_stack_lookups = results.items.items.len <= lookups_stack.len;
    const lookups = if (use_stack_lookups)
        lookups_stack[0..results.items.items.len]
    else
        try self.alloc.alloc(FixedKeyLookup, results.items.items.len);
    defer if (!use_stack_lookups) self.alloc.free(lookups);

    var key_views_stack: [small_metadata_batch_count][]const u8 = undefined;
    const use_stack_key_views = results.items.items.len <= key_views_stack.len;
    const key_views = if (use_stack_key_views)
        key_views_stack[0..results.items.items.len]
    else
        try self.alloc.alloc([]const u8, results.items.items.len);
    defer if (!use_stack_key_views) self.alloc.free(key_views);

    var values_stack: [small_metadata_batch_count]?[]const u8 = undefined;
    const use_stack_values = results.items.items.len <= values_stack.len;
    const values = if (use_stack_values)
        values_stack[0..results.items.items.len]
    else
        try self.alloc.alloc(?[]const u8, results.items.items.len);
    defer if (!use_stack_values) self.alloc.free(values);

    try populateMetadataBatchedWithScratch(self, txn, results, lookups, key_views, values);
}

fn populateMetadataBatchedWithScratch(
    self: anytype,
    txn: anytype,
    results: *search_results.SearchResults,
    lookup_storage: []FixedKeyLookup,
    key_views_storage: [][]const u8,
    values_storage: []?[]const u8,
) !void {
    var lookup_count: usize = 0;
    for (results.items.items, 0..) |item, index| {
        if (item.metadata != null) continue;
        if (borrowCachedMetadataHandle(self, item.vector_id)) |cached_handle| {
            var handle = cached_handle;
            defer handle.deinit();
            results.items.items[index].metadata = try self.alloc.dupe(u8, handle.view());
            continue;
        }
        if (self.getCachedMetadata(item.vector_id)) |cached| {
            results.items.items[index].metadata = try self.alloc.dupe(u8, cached);
            continue;
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
    sortFixedKeyLookupsIfNeeded(lookups);
    for (lookups, 0..) |*lookup, i| key_views[i] = lookup.key[0..];

    try txn.getManySorted(.vecs, key_views, values);
    for (values, 0..) |maybe_value, i| {
        const value = maybe_value orelse continue;
        _ = try self.cacheMetadata(lookups[i].vector_id, value);
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
        if (borrowCachedMetadataHandle(self, vector_id)) |cached_handle| {
            var handle = cached_handle;
            defer handle.deinit();
            if (!std.mem.startsWith(u8, handle.view(), req.filter_prefix)) return false;
            return true;
        }
        const metadata = (try loadMetadataRaw(self, txn, vector_id, isNotFoundGeneric)) orelse return false;
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
    posting.PostingStore.noteCentroidRefreshed(leaf);
}

fn applyLeafCentroidReplacementDelta(self: anytype, leaf: *types.Node, old_transformed: []const f32, new_transformed: []const f32) !void {
    if (leaf.members.len == 0) {
        @memset(leaf.centroid, 0);
        return;
    }
    if (leaf.centroid.len != self.config.dims or old_transformed.len != self.config.dims or new_transformed.len != self.config.dims) {
        return error.InvalidArgument;
    }
    const n: f32 = @floatFromInt(leaf.members.len);
    for (leaf.centroid, old_transformed, new_transformed) |*c, old_value, new_value| {
        c.* += (new_value - old_value) / n;
    }
    normalizeCentroidForMetric(self, leaf.centroid);
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
    var txn = try self.beginRuntimeWriteTxn();
    errdefer txn.abort();
    try deleteTxn(self, &txn, vector_id);
    try runAutoPostingMaintenanceTxn(self, &txn);
    try self.flushMetadata(&txn);
    try txn.commit();
    publishSearchStateIfSupported(self);
}

pub fn batchDelete(self: anytype, vector_ids: []const u64) !void {
    if (vector_ids.len == 0) return;
    if (vector_ids.len == 1) return delete(self, vector_ids[0]);

    var batch = try self.beginRuntimeBatchTxn();
    errdefer batch.abort();
    try batchDeleteTxn(self, &batch, vector_ids);
    try runAutoPostingMaintenanceTxn(self, &batch);
    try self.flushMetadata(&batch);
    try batch.commit();
    publishSearchStateIfSupported(self);
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
        if (shouldWritePostingBaseDelta(self)) {
            const delta_records = try self.alloc.alloc(posting.PostingDeltaRecord, group.len);
            defer self.alloc.free(delta_records);
            for (group, 0..) |entry, offset| {
                delta_records[offset] = .{
                    .sequence = postingDeltaSequence(leaf.posting_state.mutation_version, offset),
                    .op = .tombstone,
                    .vector_id = entry.vector_id,
                };
            }
            try shadowAppendPostingDeltas(self, txn, leaf.id, delta_records);
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
    var txn = try self.beginRuntimeWriteTxn();
    errdefer txn.abort();
    const report = try repairTreeLinks(self, &txn, max_nodes);
    try self.flushMetadata(&txn);
    try txn.commit();
    publishSearchStateIfSupported(self);
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
    try shadowAppendPostingDelta(self, txn, leaf.id, .tombstone, vector_id, postingDeltaSequence(leaf.posting_state.mutation_version, 0));

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
                posting.PostingStore.noteMembersChanged(&sibling);
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
    var node = if (shouldUseBaseDeltaAsCanonicalPosting(self))
        self.loadNodeFromStorage(txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) return null;
            return err;
        }
    else
        loadNode(self, txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) return null;
            return err;
        };
    defer node.deinit(self.alloc);

    if (node.is_leaf and shouldUseBaseDeltaAsCanonicalPosting(self)) {
        const contains = posting.PostingStore.containsBaseDeltaMember(self, txn, node.id, vector_id, isNotFoundGeneric) catch |err| {
            if (!isNotFoundGeneric(err)) return err;
            for (node.members) |member_id| {
                if (member_id == vector_id) return node.id;
            }
            return null;
        };
        return if (contains) node.id else null;
    }

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

fn saveNodeSplitRangeAfterMetadataOverwrite(
    self: anytype,
    txn: anytype,
    node: *const types.Node,
    previous_metadata: ?[]const u8,
    next_metadata: []const u8,
    is_not_found: fn (anyerror) bool,
) !void {
    const maybe_existing = try loadNodeSplitRange(self, txn, node.id, is_not_found);
    var range = maybe_existing orelse {
        if (previous_metadata == null and next_metadata.len == 0) return;
        return saveNodeSplitRange(self, txn, node, is_not_found);
    };
    defer range.deinit(self.alloc);

    if (previous_metadata) |old_metadata| {
        const old_touched_min = std.mem.eql(u8, old_metadata, range.min_key);
        const old_touched_max = std.mem.eql(u8, old_metadata, range.max_key);
        if ((old_touched_min or old_touched_max) and !std.mem.eql(u8, old_metadata, next_metadata)) {
            return saveNodeSplitRange(self, txn, node, is_not_found);
        }
    }

    if (next_metadata.len == 0) return;

    var changed = false;
    if (std.mem.order(u8, next_metadata, range.min_key) == .lt) {
        self.alloc.free(range.min_key);
        range.min_key = try self.alloc.dupe(u8, next_metadata);
        changed = true;
    }
    if (std.mem.order(u8, next_metadata, range.max_key) == .gt) {
        self.alloc.free(range.max_key);
        range.max_key = try self.alloc.dupe(u8, next_metadata);
        changed = true;
    }
    if (changed) try putNodeSplitRange(self, txn, node.id, &range, is_not_found);
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
    var txn = try self.beginRuntimeWriteTxn();
    errdefer txn.abort();
    var transformed_vector_stack: [4096]f32 = undefined;
    const use_stack_transformed = self.config.dims <= transformed_vector_stack.len;
    const transformed_vector = if (use_stack_transformed)
        transformed_vector_stack[0..self.config.dims]
    else
        try self.alloc.alloc(f32, self.config.dims);
    defer if (!use_stack_transformed) self.alloc.free(transformed_vector);
    try insertWithMetadataTxn(self, &txn, vector_id, vector_data, metadata_value, transformed_vector, now_fn_u64, elapsed_fn_u64);
    try runAutoPostingMaintenanceTxn(self, &txn);
    const flush_start = now_fn_u64();
    try self.flushMetadata(&txn);
    self.write_profile.insert_flush_metadata_ns += elapsed_fn_u64(flush_start);
    const commit_start = now_fn_u64();
    try txn.commit();
    self.write_profile.insert_commit_ns += elapsed_fn_u64(commit_start);
    publishSearchStateIfSupported(self);
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
    const defer_leaf_splits_to_posting_maintenance = if (@hasField(Options, "defer_leaf_splits_to_posting_maintenance")) options.defer_leaf_splits_to_posting_maintenance else false;
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
        .defer_leaf_splits_to_posting_maintenance = defer_leaf_splits_to_posting_maintenance,
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
    var existing_update_stack: [8192]f32 = undefined;
    var existing_update_heap: ?[]f32 = null;
    defer if (existing_update_heap) |buf| self.alloc.free(buf);
    var previous_vector_storage: []f32 = &.{};
    var previous_transformed_storage: []f32 = &.{};

    const existing_leaf_id = if (assume_absent_ids)
        0
    else
        self.getVecLeaf(txn, vector_id) catch |err| blk: {
            if (isNotFoundGeneric(err)) break :blk 0;
            return err;
        };

    if (existing_leaf_id != 0) {
        const existing_scratch_len = try std.math.mul(usize, self.config.dims, 2);
        const existing_update_scratch = if (existing_scratch_len <= existing_update_stack.len)
            existing_update_stack[0..existing_scratch_len]
        else blk: {
            existing_update_heap = try self.alloc.alloc(f32, existing_scratch_len);
            break :blk existing_update_heap.?;
        };
        previous_vector_storage = existing_update_scratch[0..self.config.dims];
        previous_transformed_storage = existing_update_scratch[self.config.dims..existing_scratch_len];
        if (try existingVectorMatchesNoOp(self, txn, vector_id, vector_data, metadata_value, previous_vector_storage)) {
            self.write_profile.noop_existing_skips += 1;
            return;
        }
    }

    const target_leaf_id = blk_leaf: {
        if (existing_leaf_id != 0) {
            const find_leaf_start = now_fn_u64();
            const existing_update_allow_quantized_routing = if (shouldRouteExistingUpdatesByCentroidOnly(self)) false else allow_quantized_routing;
            const leaf_id = try posting.CentroidDirectory.findPosting(self, txn, self.metadata.root_node, effective_transformed, existing_update_allow_quantized_routing);
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
                    previous_vector_storage,
                    previous_transformed_storage,
                    skip_vector_store,
                    batch_insert_options,
                )) {
                    return;
                }
            } else {
                if (shouldRouteExistingUpdatesByCentroidOnly(self)) {
                    if (try tryLazyRerouteExistingVectorTxnOptions(
                        self,
                        txn,
                        existing_leaf_id,
                        leaf_id,
                        vector_id,
                        vector_data,
                        metadata_value,
                        skip_vector_store,
                    )) {
                        return;
                    }
                    var target_leaf = loadNode(self, txn, leaf_id) catch |err| {
                        if (isNotFoundGeneric(err)) break :blk_leaf existing_leaf_id;
                        return err;
                    };
                    defer target_leaf.deinit(self.alloc);
                    if (!leafCanAcceptMember(self, &target_leaf, vector_id)) {
                        if (try tryUpdateExistingVectorInLeafTxnOptions(
                            self,
                            txn,
                            existing_leaf_id,
                            vector_id,
                            vector_data,
                            metadata_value,
                            effective_transformed,
                            previous_vector_storage,
                            previous_transformed_storage,
                            skip_vector_store,
                            batch_insert_options,
                        )) {
                            return;
                        }
                        break :blk_leaf existing_leaf_id;
                    }
                }
                self.write_profile.existing_vector_reroutes += 1;
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
    try shadowAppendPostingDelta(self, txn, leaf.id, .insert, vector_id, postingDeltaSequence(leaf.posting_state.mutation_version, 0));

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
    const leaf_overflows = leaf.members.len > self.config.leaf_size;
    const defer_lazy_existing_leaf_split = leaf_overflows and existing_leaf_id != 0 and shouldRouteExistingUpdatesByCentroidOnly(self);
    const defer_leaf_split = shouldDeferOversizedLeafSplit(self, &leaf, batch_insert_options);
    var save_options = batch_insert_options;
    save_options.suppress_quantized_payload_persist = defer_leaf_split or defer_lazy_existing_leaf_split;

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
        if (defer_lazy_existing_leaf_split) {
            self.write_profile.posting_lazy_payload_deferrals += 1;
        } else if (defer_leaf_split) {
            if (shouldRecordDeferredOversizedLeafForSplitFinish(batch_insert_options)) {
                try recordDeferredOversizedLeaf(self, leaf.id);
            }
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

fn leafCanAcceptMember(self: anytype, leaf: *const types.Node, vector_id: u64) bool {
    if (!leaf.is_leaf) return false;
    return nodeHasMember(leaf.members, vector_id) or leaf.members.len < self.config.leaf_size;
}

fn tryLazyRerouteExistingVectorTxnOptions(
    self: anytype,
    txn: anytype,
    source_leaf_id: u64,
    target_leaf_id: u64,
    vector_id: u64,
    vector_data: []const f32,
    metadata_value: []const u8,
    skip_vector_store: bool,
) !bool {
    if (!shouldRouteExistingUpdatesByCentroidOnly(self)) return false;
    if (source_leaf_id == 0 or target_leaf_id == 0 or source_leaf_id == target_leaf_id) return false;

    var source_leaf = try loadNode(self, txn, source_leaf_id);
    defer source_leaf.deinit(self.alloc);
    if (!source_leaf.is_leaf or !nodeHasMember(source_leaf.members, vector_id)) return false;
    try source_leaf.ensureUnbacked(self.alloc);

    var target_leaf = try loadNode(self, txn, target_leaf_id);
    defer target_leaf.deinit(self.alloc);
    if (!leafCanAcceptMember(self, &target_leaf, vector_id)) return false;
    try target_leaf.ensureUnbacked(self.alloc);

    const store_start = nowNsU64Fixed();
    try storeVectorAndMetadataWithOptions(self, txn, vector_id, vector_data, metadata_value, skip_vector_store);
    self.write_profile.insert_store_vector_ns += elapsedSinceU64Fixed(store_start);

    const mutate_start = nowNsU64Fixed();
    try posting.PostingStore.removeMember(self.alloc, &source_leaf, vector_id);
    try shadowAppendPostingDelta(self, txn, source_leaf.id, .tombstone, vector_id, postingDeltaSequence(source_leaf.posting_state.mutation_version, 0));

    if (!nodeHasMember(target_leaf.members, vector_id)) {
        _ = try posting.PostingStore.appendMember(self.alloc, &target_leaf, vector_id);
        try shadowAppendPostingDelta(self, txn, target_leaf.id, .insert, vector_id, postingDeltaSequence(target_leaf.posting_state.mutation_version, 0));
    } else {
        posting.PostingStore.noteVectorsChanged(&target_leaf);
        try shadowAppendPostingDelta(self, txn, target_leaf.id, .replace, vector_id, postingDeltaSequence(target_leaf.posting_state.mutation_version, 0));
    }

    try self.putVecLeaf(txn, vector_id, target_leaf.id);
    try posting.PostingStore.saveState(self, txn, source_leaf.id, source_leaf.posting_state);
    try posting.PostingStore.saveState(self, txn, target_leaf.id, target_leaf.posting_state);

    self.invalidateQuantizedCache(source_leaf.id);
    self.invalidateQuantizedCache(target_leaf.id);
    try self.cacheNode(&source_leaf);
    try self.cacheNode(&target_leaf);

    self.write_profile.existing_vector_reroutes += 1;
    self.write_profile.posting_lazy_centroid_deferrals += 2;
    self.write_profile.posting_lazy_payload_deferrals += 2;
    if (source_leaf.parent != 0) self.write_profile.posting_lazy_ancestor_deferrals += 1;
    if (target_leaf.parent != 0) self.write_profile.posting_lazy_ancestor_deferrals += 1;
    self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
    return true;
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
    self.write_profile.existing_same_leaf_updates += 1;

    const previous_transformed = blk_previous: {
        const previous_vector = self.getVectorScratch(txn, vector_id, previous_vector_storage) catch |err| switch (err) {
            error.NotFound => break :blk_previous null,
            else => return err,
        };
        _ = self.transformVector(previous_vector, previous_transformed_storage);
        break :blk_previous previous_transformed_storage[0..];
    };
    var previous_metadata_owned: ?[]u8 = null;
    defer if (previous_metadata_owned) |metadata| self.alloc.free(metadata);
    if (try getMetadataInTxn(self, txn, vector_id, isNotFoundGeneric)) |metadata| {
        previous_metadata_owned = try self.alloc.dupe(u8, metadata);
    }
    const store_start = nowNsU64Fixed();
    try storeVectorAndMetadataWithOptions(self, txn, vector_id, vector_data, metadata_value, skip_vector_store);
    self.write_profile.insert_store_vector_ns += elapsedSinceU64Fixed(store_start);

    if (shouldUseBaseDeltaAsCanonicalPosting(self) and self.config.lazy_posting_maintenance) {
        const mutate_start = nowNsU64Fixed();
        posting.PostingStore.noteVectorsChanged(&existing_leaf);
        try self.putVecLeaf(txn, vector_id, existing_leaf.id);
        if (shouldDeferPostingCentroidRefresh(self, &existing_leaf)) {
            self.write_profile.posting_lazy_centroid_deferrals += 1;
        }
        if (shouldDeferPostingPayloadRefresh(self, &existing_leaf)) {
            self.write_profile.posting_lazy_payload_deferrals += 1;
        }
        if (existing_leaf.parent != 0) {
            self.write_profile.posting_lazy_ancestor_deferrals += 1;
        }
        try posting.PostingStore.saveState(self, txn, existing_leaf.id, existing_leaf.posting_state);
        try saveNodeSplitRangeAfterMetadataOverwrite(self, txn, &existing_leaf, previous_metadata_owned, metadata_value, isNotFoundGeneric);
        self.invalidateQuantizedCache(existing_leaf.id);
        try self.cacheNode(&existing_leaf);
        self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
        return true;
    }

    posting.PostingStore.noteVectorsChanged(&existing_leaf);
    try shadowAppendPostingDelta(self, txn, existing_leaf.id, .replace, vector_id, postingDeltaSequence(existing_leaf.posting_state.mutation_version, 0));
    if (shouldDeferPostingCentroidRefresh(self, &existing_leaf)) {
        self.write_profile.posting_lazy_centroid_deferrals += 1;
    } else if (previous_transformed) |old_transformed| {
        applyLeafCentroidReplacementDelta(self, &existing_leaf, old_transformed, effective_transformed) catch {
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

    if (shouldUseBaseDeltaAsCanonicalPosting(self) and self.config.lazy_posting_maintenance) {
        const vector_matches_stored = try existingVectorMatchesStoredVector(self, txn, vector_id, vector_data, previous_vector_storage);
        var previous_metadata_owned: ?[]u8 = null;
        defer if (previous_metadata_owned) |metadata| self.alloc.free(metadata);
        if (try getMetadataInTxn(self, txn, vector_id, isNotFoundGeneric)) |metadata| {
            previous_metadata_owned = try self.alloc.dupe(u8, metadata);
        }

        const store_start = nowNsU64Fixed();
        if (vector_matches_stored) {
            if (metadata_value.len > 0) try putMetadata(self, txn, vector_id, metadata_value);
        } else {
            try storeVectorAndMetadataWithOptions(self, txn, vector_id, vector_data, metadata_value, options.skip_vector_store);
        }
        self.write_profile.insert_store_vector_ns += elapsedSinceU64Fixed(store_start);

        var leaf = try loadNode(self, txn, leaf_id);
        defer leaf.deinit(self.alloc);
        if (!nodeHasMember(leaf.members, vector_id)) return false;

        const mutate_start = nowNsU64Fixed();
        if (vector_matches_stored) {
            try saveNodeSplitRangeAfterMetadataOverwrite(self, txn, &leaf, previous_metadata_owned, metadata_value, isNotFoundGeneric);
            self.write_profile.existing_same_vector_coalesces += 1;
            self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
            return true;
        }

        posting.PostingStore.noteVectorsChanged(&leaf);
        self.write_profile.posting_lazy_centroid_deferrals += 1;
        self.write_profile.posting_lazy_payload_deferrals += 1;
        try self.putVecLeaf(txn, vector_id, leaf.id);
        if (leaf.parent != 0) self.write_profile.posting_lazy_ancestor_deferrals += 1;
        try posting.PostingStore.saveState(self, txn, leaf.id, leaf.posting_state);
        try saveNodeSplitRangeAfterMetadataOverwrite(self, txn, &leaf, previous_metadata_owned, metadata_value, isNotFoundGeneric);
        self.invalidateQuantizedCache(leaf.id);
        try self.cacheNode(&leaf);
        self.write_profile.existing_same_leaf_updates += 1;
        self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
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
    self.write_profile.existing_same_vector_coalesces += 1;
    return true;
}

fn batchCoalesceBaseDeltaExistingUpdatesTxnOptions(
    self: anytype,
    txn: anytype,
    writes: []const hbc_runtime.BatchInsertItem,
    options: hbc_runtime.BatchInsertOptions,
    processed: []bool,
) !usize {
    if (!options.coalesce_leaf_writes) return 0;
    if (!shouldUseBaseDeltaAsCanonicalPosting(self) or !self.config.lazy_posting_maintenance) return 0;
    if (writes.len < 2) return 0;

    var prepared = std.ArrayListUnmanaged(PreparedExistingBatchUpdate).empty;
    defer prepared.deinit(self.alloc);
    try prepared.ensureTotalCapacity(self.alloc, @intCast(writes.len));

    const dims: usize = @intCast(self.config.dims);
    const scratch_len = try std.math.mul(usize, dims, 2);
    var vector_scratch_stack: [8192]f32 = undefined;
    const use_stack_scratch = scratch_len <= vector_scratch_stack.len;
    const vector_scratch = if (use_stack_scratch)
        vector_scratch_stack[0..scratch_len]
    else
        try self.alloc.alloc(f32, scratch_len);
    defer if (!use_stack_scratch) self.alloc.free(vector_scratch);
    const compare_vector = vector_scratch[0..dims];
    const transformed = vector_scratch[dims..scratch_len];

    var processed_count: usize = 0;
    for (writes, 0..) |item, item_index| {
        const leaf_id = self.getVecLeaf(txn, item.vector_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        if (try existingVectorMatchesNoOp(self, txn, item.vector_id, item.vector, item.metadata, compare_vector)) {
            self.write_profile.noop_existing_skips += 1;
            processed[item_index] = true;
            processed_count += 1;
            continue;
        }
        prepared.appendAssumeCapacity(.{ .item_index = item_index, .leaf_id = leaf_id });
    }
    if (prepared.items.len == 0) return processed_count;

    std.mem.sort(PreparedExistingBatchUpdate, prepared.items, {}, lessPreparedExistingBatchUpdate);

    var target_delta_records = std.ArrayListUnmanaged(TargetPostingDeltaRecord).empty;
    defer target_delta_records.deinit(self.alloc);
    try target_delta_records.ensureTotalCapacity(self.alloc, prepared.items.len);

    var group_start: usize = 0;
    while (group_start < prepared.items.len) {
        var group_end = group_start + 1;
        while (group_end < prepared.items.len and prepared.items[group_end].leaf_id == prepared.items[group_start].leaf_id) : (group_end += 1) {}
        const group = prepared.items[group_start..group_end];
        const leaf_id = group[0].leaf_id;

        var leaf = try loadNode(self, txn, leaf_id);
        defer leaf.deinit(self.alloc);
        try leaf.ensureUnbacked(self.alloc);

        const records = try self.alloc.alloc(posting.PostingDeltaRecord, group.len);
        defer self.alloc.free(records);
        var record_count: usize = 0;
        var same_posting_update_count: usize = 0;

        const store_start = nowNsU64Fixed();
        for (group) |entry| {
            const item = writes[entry.item_index];
            if (!nodeHasMember(leaf.members, item.vector_id)) continue;

            if (try existingVectorMatchesStoredVector(self, txn, item.vector_id, item.vector, compare_vector)) {
                var previous_metadata_owned: ?[]u8 = null;
                defer if (previous_metadata_owned) |metadata| self.alloc.free(metadata);
                if (try getMetadataInTxn(self, txn, item.vector_id, isNotFoundGeneric)) |metadata| {
                    previous_metadata_owned = try self.alloc.dupe(u8, metadata);
                }
                if (item.metadata.len > 0) try putMetadata(self, txn, item.vector_id, item.metadata);
                try saveNodeSplitRangeAfterMetadataOverwrite(self, txn, &leaf, previous_metadata_owned, item.metadata, isNotFoundGeneric);
                self.write_profile.existing_same_vector_coalesces += 1;
                processed[entry.item_index] = true;
                processed_count += 1;
                continue;
            }

            _ = self.transformVector(item.vector, transformed);

            const find_leaf_start = nowNsU64Fixed();
            const allow_quantized_routing = if (shouldRouteExistingUpdatesByCentroidOnly(self))
                false
            else if (@hasField(@TypeOf(options), "allow_quantized_routing"))
                options.allow_quantized_routing
            else
                !options.centroid_only_routing;
            const target_leaf_id = try posting.CentroidDirectory.findPosting(self, txn, self.metadata.root_node, transformed, allow_quantized_routing);
            self.write_profile.insert_find_leaf_ns += elapsedSinceU64Fixed(find_leaf_start);
            const moved_to_target = if (target_leaf_id != leaf.id) moved_blk: {
                var target_leaf = loadNode(self, txn, target_leaf_id) catch |err| {
                    if (isNotFoundGeneric(err)) break :moved_blk false;
                    return err;
                };
                defer target_leaf.deinit(self.alloc);
                if (!leafCanAcceptMember(self, &target_leaf, item.vector_id)) break :moved_blk false;

                try target_leaf.ensureUnbacked(self.alloc);
                try storeVectorAndMetadataWithOptions(self, txn, item.vector_id, item.vector, item.metadata, options.skip_vector_store);
                try posting.PostingStore.removeMember(self.alloc, &leaf, item.vector_id);
                records[record_count] = .{
                    .sequence = postingDeltaSequence(leaf.posting_state.mutation_version, record_count),
                    .op = .tombstone,
                    .vector_id = item.vector_id,
                };
                record_count += 1;
                _ = try posting.PostingStore.appendMember(self.alloc, &target_leaf, item.vector_id);
                target_delta_records.appendAssumeCapacity(.{
                    .posting_id = target_leaf.id,
                    .record = .{
                        .sequence = target_leaf.posting_state.mutation_version,
                        .op = .insert,
                        .vector_id = item.vector_id,
                    },
                });
                try self.putVecLeaf(txn, item.vector_id, target_leaf.id);
                try posting.PostingStore.saveState(self, txn, target_leaf.id, target_leaf.posting_state);
                self.invalidateQuantizedCache(target_leaf.id);
                try self.cacheNode(&target_leaf);
                self.write_profile.existing_vector_reroutes += 1;
                self.write_profile.posting_lazy_centroid_deferrals += 1;
                self.write_profile.posting_lazy_payload_deferrals += 1;
                if (target_leaf.parent != 0) {
                    self.write_profile.posting_lazy_ancestor_deferrals += 1;
                }
                processed[entry.item_index] = true;
                processed_count += 1;
                break :moved_blk true;
            } else false;
            if (moved_to_target) continue;

            var previous_metadata_owned: ?[]u8 = null;
            defer if (previous_metadata_owned) |metadata| self.alloc.free(metadata);
            if (try getMetadataInTxn(self, txn, item.vector_id, isNotFoundGeneric)) |metadata| {
                previous_metadata_owned = try self.alloc.dupe(u8, metadata);
            }
            try storeVectorAndMetadataWithOptions(self, txn, item.vector_id, item.vector, item.metadata, options.skip_vector_store);
            posting.PostingStore.noteVectorsChanged(&leaf);
            try saveNodeSplitRangeAfterMetadataOverwrite(self, txn, &leaf, previous_metadata_owned, item.metadata, isNotFoundGeneric);
            try self.putVecLeaf(txn, item.vector_id, leaf.id);
            same_posting_update_count += 1;
            processed[entry.item_index] = true;
            processed_count += 1;
        }
        self.write_profile.insert_store_vector_ns += elapsedSinceU64Fixed(store_start);

        if (record_count != 0 or same_posting_update_count != 0) {
            const mutate_start = nowNsU64Fixed();
            if (record_count != 0) {
                for (records[0..record_count], 0..) |*record, record_index| {
                    record.sequence = postingDeltaSequence(leaf.posting_state.mutation_version, record_index);
                }
                try shadowAppendPostingDeltas(self, txn, leaf.id, records[0..record_count]);
            }
            const deferral_count = record_count + same_posting_update_count;
            self.write_profile.posting_lazy_centroid_deferrals += @intCast(deferral_count);
            self.write_profile.posting_lazy_payload_deferrals += @intCast(deferral_count);
            if (leaf.parent != 0) {
                self.write_profile.posting_lazy_ancestor_deferrals += @intCast(deferral_count);
            }
            try posting.PostingStore.saveState(self, txn, leaf.id, leaf.posting_state);
            self.invalidateQuantizedCache(leaf.id);
            try self.cacheNode(&leaf);
            self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
        }

        group_start = group_end;
    }

    if (target_delta_records.items.len != 0) {
        const mutate_start = nowNsU64Fixed();
        std.mem.sort(TargetPostingDeltaRecord, target_delta_records.items, {}, lessTargetPostingDeltaRecord);
        var target_group_start: usize = 0;
        while (target_group_start < target_delta_records.items.len) {
            var target_group_end = target_group_start + 1;
            while (target_group_end < target_delta_records.items.len and
                target_delta_records.items[target_group_end].posting_id == target_delta_records.items[target_group_start].posting_id) : (target_group_end += 1)
            {}

            const target_group = target_delta_records.items[target_group_start..target_group_end];
            {
                const target_records = try self.alloc.alloc(posting.PostingDeltaRecord, target_group.len);
                defer self.alloc.free(target_records);
                var target_mutation_version: u64 = 0;
                for (target_group) |entry| {
                    target_mutation_version = @max(target_mutation_version, entry.record.sequence);
                }
                for (target_group, 0..) |entry, target_index| {
                    target_records[target_index] = entry.record;
                    target_records[target_index].sequence = postingDeltaSequence(target_mutation_version, target_index);
                }
                try shadowAppendPostingDeltas(self, txn, target_group[0].posting_id, target_records);
            }
            target_group_start = target_group_end;
        }
        self.write_profile.insert_mutate_leaf_ns += elapsedSinceU64Fixed(mutate_start);
    }

    return processed_count;
}

fn existingVectorMatchesStoredVector(
    self: anytype,
    txn: anytype,
    vector_id: u64,
    next_vector: []const f32,
    scratch: []f32,
) !bool {
    const existing_vector = getVectorScratch(self, txn, vector_id, scratch) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (existing_vector.len != next_vector.len) return false;
    return std.mem.eql(u8, std.mem.sliceAsBytes(existing_vector), std.mem.sliceAsBytes(next_vector));
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
    try shadowAppendPostingDelta(self, txn, leaf.id, .tombstone, vector_id, postingDeltaSequence(leaf.posting_state.mutation_version, 0));

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
            errdefer self.alloc.free(merged);
            @memcpy(merged[0..sibling.members.len], sibling.members);
            @memcpy(merged[sibling.members.len..], leaf.members);
            self.alloc.free(sibling.members);
            sibling.members = merged;
            posting.PostingStore.noteMembersChanged(&sibling);
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

    if (node.parent == 0) {
        const new_root_id = self.nextNodeId();
        var root_centroid = try self.alloc.dupe(f32, n1.centroid);
        errdefer if (root_centroid.len > 0) self.alloc.free(root_centroid);
        vec.add(root_centroid, n2.centroid);
        vec.scale(0.5, root_centroid);
        normalizeCentroidForMetric(self, root_centroid);

        n1.parent = new_root_id;
        n2.parent = new_root_id;

        try saveNodeWithOptions(self, txn, &n1, options, now_fn, elapsed_fn);
        try saveNodeWithOptions(self, txn, &n2, options, now_fn, elapsed_fn);

        var root_children = try self.alloc.alloc(u64, 2);
        errdefer if (root_children.len > 0) self.alloc.free(root_children);
        root_children[0] = n1_id;
        root_children[1] = n2_id;

        var new_root = types.Node{
            .id = new_root_id,
            .is_leaf = false,
            .level = node.level + 1,
            .parent = 0,
            .centroid = root_centroid,
            .children = root_children,
            .members = &.{},
        };
        root_centroid = &.{};
        root_children = &.{};
        defer new_root.deinit(self.alloc);
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
    _ = self.getQuantized(txn, leaf.id, usesNonQuantizedPayload(leaf), count) catch null;
    if (self.getCachedQuantizedPtr(leaf.id)) |cached| {
        switch (cached.*) {
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
    if (options.capacity_balance_leaf_split) {
        try capacityBalanceLeafSplitToMaxMembers(self, &vector_set, leaf.members, &split, self.config.leaf_size);
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

    left_node.posting_state = leaf.posting_state;
    posting.PostingStore.noteMembersChanged(&left_node);
    posting.PostingStore.noteCentroidRefreshed(&left_node);
    right_node.posting_state = leaf.posting_state;
    posting.PostingStore.noteMembersChanged(&right_node);
    posting.PostingStore.noteCentroidRefreshed(&right_node);

    const publish_known_quantized_now = !(deferQuantizedRebuild(options) and shouldDeferQuantizedRebuildToBulkFinish(self, options));
    var vector_copies = SplitMemberVectorCopies{};
    defer vector_copies.deinit(self.alloc);
    if (publish_known_quantized_now) {
        if (try copySplitMemberVectorsFromSourceOrder(self, &left_node, &right_node, leaf.members, vec_data)) |copies| {
            vector_copies = copies;
        } else {
            vector_copies.left = try copyNodeMemberVectorsFromSource(self, &left_node, leaf.members, vec_data);
            vector_copies.right = try copyNodeMemberVectorsFromSource(self, &right_node, leaf.members, vec_data);
        }
    }
    const left_vectors = vector_copies.left;
    const right_vectors = vector_copies.right;

    if (splitting_root) {
        const new_root_id = self.nextNodeId();
        var root_centroid = try self.alloc.dupe(f32, left_node.centroid);
        errdefer if (root_centroid.len > 0) self.alloc.free(root_centroid);
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

        var root_children = try self.alloc.alloc(u64, 2);
        errdefer if (root_children.len > 0) self.alloc.free(root_children);
        root_children[0] = left_id;
        root_children[1] = right_id;

        var new_root = types.Node{
            .id = new_root_id,
            .is_leaf = false,
            .level = leaf_level,
            .parent = 0,
            .centroid = root_centroid,
            .children = root_children,
            .members = &.{},
        };
        root_centroid = &.{};
        root_children = &.{};
        defer new_root.deinit(self.alloc);
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
    const scratch_len = try std.math.mul(usize, dims, 2);
    var stack_scratch: [4096]f32 = undefined;
    const use_stack_scratch = scratch_len <= stack_scratch.len;
    const scratch = if (use_stack_scratch)
        stack_scratch[0..scratch_len]
    else
        try self.alloc.alloc(f32, scratch_len);
    defer if (!use_stack_scratch) self.alloc.free(scratch);
    const vector_scratch = scratch[0..dims];
    const transformed_scratch = scratch[dims..scratch_len];

    const vector_load_start = now_fn();
    var used_cached_nonquant = false;
    _ = self.getQuantized(txn, leaf.id, usesNonQuantizedPayload(leaf), leaf.members.len) catch null;
    if (self.getCachedQuantizedPtr(leaf.id)) |cached| {
        switch (cached.*) {
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
                const point = points[entries[entry_cursor + i].point_index];
                members[i] = point.stable_id;
                vec.add(centroid, point.vector);
                @memcpy(group_vectors[i * dims ..][0..dims], point.vector);
                try self.putVecLeaf(txn, point.stable_id, node_id);
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
        for (points) |point| vec.add(centroid, point.vector);
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
        var batch = if (options.bulk_ingest and comptime @hasDecl(Index, "beginRuntimeBatchTxnOptions"))
            try self.beginRuntimeBatchTxnOptions(options)
        else
            try self.beginRuntimeBatchTxn();
        errdefer batch.abort();
        try batchDeleteTxn(self, &batch, deletes);
        try finalizeWriteTxnOptions(self, &batch, options, now_fn, elapsed_fn);
        const commit_start = now_fn();
        const publishing = beginPublishSearchStateIfSupported(self);
        errdefer abortPublishSearchStateIfSupported(self, publishing);
        try batch.commit();
        self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
        finishPublishSearchStateIfSupported(self, publishing);
        return;
    }
    if (deletes.len == 0) return batchInsertWithMetadataOptions(self, writes, options, now_fn, elapsed_fn);

    const Index = comptime childType(@TypeOf(self));
    var batch = if (options.bulk_ingest and comptime @hasDecl(Index, "beginRuntimeBatchTxnOptions"))
        try self.beginRuntimeBatchTxnOptions(options)
    else
        try self.beginRuntimeBatchTxn();
    errdefer batch.abort();

    try batchDeleteTxn(self, &batch, deletes);

    var processed_write_stack: [small_coalesced_batch_count]bool = undefined;
    var remaining_write_stack: [small_coalesced_batch_count]hbc_runtime.BatchInsertItem = undefined;
    var processed_write_updates: []bool = &.{};
    var remaining_write_storage: []hbc_runtime.BatchInsertItem = &.{};
    var processed_write_on_stack = false;
    var remaining_write_on_stack = false;
    defer if (processed_write_updates.len > 0 and !processed_write_on_stack) self.alloc.free(processed_write_updates);
    defer if (remaining_write_storage.len > 0 and !remaining_write_on_stack) self.alloc.free(remaining_write_storage);

    var effective_writes = writes;
    if (writes.len > 1 and options.coalesce_leaf_writes) {
        processed_write_updates = if (writes.len <= processed_write_stack.len) blk: {
            processed_write_on_stack = true;
            break :blk processed_write_stack[0..writes.len];
        } else try self.alloc.alloc(bool, writes.len);
        @memset(processed_write_updates, false);
        const processed_count = try batchCoalesceBaseDeltaExistingUpdatesTxnOptions(
            self,
            &batch,
            writes,
            options,
            processed_write_updates,
        );
        if (processed_count != 0) {
            const remaining_count = writes.len - processed_count;
            remaining_write_storage = if (remaining_count <= remaining_write_stack.len) blk: {
                remaining_write_on_stack = true;
                break :blk remaining_write_stack[0..remaining_count];
            } else try self.alloc.alloc(hbc_runtime.BatchInsertItem, remaining_count);
            var wi: usize = 0;
            for (writes, 0..) |item, i| {
                if (processed_write_updates[i]) continue;
                remaining_write_storage[wi] = item;
                wi += 1;
            }
            effective_writes = remaining_write_storage;
        }
    }

    var insert_options = options;
    if (!insert_options.assume_absent_ids and
        try batchWritesAreUniqueAndCoveredByDeletes(self.alloc, effective_writes, deletes))
    {
        insert_options.assume_absent_ids = true;
    }
    if (!insert_options.assume_absent_ids and
        effective_writes.len > 1 and
        insert_options.coalesce_leaf_writes and
        try batchWritesAreUniqueAndAbsent(self, &batch, effective_writes))
    {
        insert_options.assume_absent_ids = true;
    }
    const grouped = if (effective_writes.len > 1 and insert_options.assume_absent_ids and insert_options.coalesce_leaf_writes)
        try batchInsertAssumeAbsentGroupedTxnOptions(self, &batch, effective_writes, insert_options, now_fn, elapsed_fn)
    else
        false;
    if (!grouped and effective_writes.len != 0) try batchInsertWithMetadataTxnOptions(self, &batch, effective_writes, insert_options);
    try finalizeWriteTxnOptions(self, &batch, insert_options, now_fn, elapsed_fn);
    const commit_start = now_fn();
    const publishing = beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    try batch.commit();
    self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
    finishPublishSearchStateIfSupported(self, publishing);
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
        if (shouldUseCursorTxnForBatchWrites(self, options)) {
            var txn = try self.beginRuntimeWriteTxn();
            errdefer txn.abort();
            var processed_write_stack: [small_coalesced_batch_count]bool = undefined;
            var remaining_write_stack: [small_coalesced_batch_count]hbc_runtime.BatchInsertItem = undefined;
            var processed_write_updates: []bool = &.{};
            var remaining_write_storage: []hbc_runtime.BatchInsertItem = &.{};
            var processed_write_on_stack = false;
            var remaining_write_on_stack = false;
            defer if (processed_write_updates.len > 0 and !processed_write_on_stack) self.alloc.free(processed_write_updates);
            defer if (remaining_write_storage.len > 0 and !remaining_write_on_stack) self.alloc.free(remaining_write_storage);

            var effective_items = items;
            if (options.coalesce_leaf_writes) {
                processed_write_updates = if (items.len <= processed_write_stack.len) blk: {
                    processed_write_on_stack = true;
                    break :blk processed_write_stack[0..items.len];
                } else try self.alloc.alloc(bool, items.len);
                @memset(processed_write_updates, false);
                const processed_count = try batchCoalesceBaseDeltaExistingUpdatesTxnOptions(
                    self,
                    &txn,
                    items,
                    options,
                    processed_write_updates,
                );
                if (processed_count != 0) {
                    const remaining_count = items.len - processed_count;
                    remaining_write_storage = if (remaining_count <= remaining_write_stack.len) blk: {
                        remaining_write_on_stack = true;
                        break :blk remaining_write_stack[0..remaining_count];
                    } else try self.alloc.alloc(hbc_runtime.BatchInsertItem, remaining_count);
                    var wi: usize = 0;
                    for (items, 0..) |item, i| {
                        if (processed_write_updates[i]) continue;
                        remaining_write_storage[wi] = item;
                        wi += 1;
                    }
                    effective_items = remaining_write_storage;
                }
            }

            if (effective_items.len != 0) {
                var insert_options = options;
                if (!insert_options.assume_absent_ids and
                    insert_options.coalesce_leaf_writes and
                    try batchWritesAreUniqueAndAbsent(self, &txn, effective_items))
                {
                    insert_options.assume_absent_ids = true;
                }
                const grouped = if (insert_options.assume_absent_ids and insert_options.coalesce_leaf_writes)
                    try batchInsertAssumeAbsentGroupedTxnOptions(self, &txn, effective_items, insert_options, now_fn, elapsed_fn)
                else
                    false;
                if (!grouped) try batchInsertWithMetadataTxnOptions(self, &txn, effective_items, insert_options);
            }
            try finalizeWriteTxnOptions(self, &txn, options, now_fn, elapsed_fn);
            const commit_start = now_fn();
            const publishing = beginPublishSearchStateIfSupported(self);
            errdefer abortPublishSearchStateIfSupported(self, publishing);
            try txn.commit();
            self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
            finishPublishSearchStateIfSupported(self, publishing);
            return;
        }

        const Index = comptime childType(@TypeOf(self));
        var batch = if (options.bulk_ingest and comptime @hasDecl(Index, "beginRuntimeBatchTxnOptions"))
            try self.beginRuntimeBatchTxnOptions(options)
        else
            try self.beginRuntimeBatchTxn();
        errdefer batch.abort();
        const grouped = if (options.coalesce_leaf_writes)
            try batchInsertAssumeAbsentGroupedTxnOptions(self, &batch, items, options, now_fn, elapsed_fn)
        else
            false;
        if (!grouped) try batchInsertWithMetadataTxnOptions(self, &batch, items, options);
        try finalizeWriteTxnOptions(self, &batch, options, now_fn, elapsed_fn);
        const commit_start = now_fn();
        const publishing = beginPublishSearchStateIfSupported(self);
        errdefer abortPublishSearchStateIfSupported(self, publishing);
        try batch.commit();
        self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
        finishPublishSearchStateIfSupported(self, publishing);
    } else {
        var txn = try self.beginRuntimeWriteTxn();
        errdefer txn.abort();
        try batchInsertWithMetadataTxnOptions(self, &txn, items, options);
        try finalizeWriteTxnOptions(self, &txn, options, now_fn, elapsed_fn);
        const commit_start = now_fn();
        const publishing = beginPublishSearchStateIfSupported(self);
        errdefer abortPublishSearchStateIfSupported(self, publishing);
        try txn.commit();
        self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
        finishPublishSearchStateIfSupported(self, publishing);
    }
}

const PreparedBatchInsert = struct {
    item_index: usize,
    leaf_id: u64,
};

const PreparedExistingBatchUpdate = struct {
    item_index: usize,
    leaf_id: u64,
};

const TargetPostingDeltaRecord = struct {
    posting_id: posting.PostingId,
    record: posting.PostingDeltaRecord,
};

const PreparedTransformedVectorSource = struct {
    transformed_data: []const f32,
    entries: []const PreparedBatchInsert,
    dims: usize,

    fn count(self: *const PreparedTransformedVectorSource) usize {
        return self.entries.len;
    }

    fn floatCount(self: *const PreparedTransformedVectorSource) usize {
        return self.entries.len * self.dims;
    }

    fn vectorAt(self: *const PreparedTransformedVectorSource, index: usize) []const f32 {
        const item_index = self.entries[index].item_index;
        return self.transformed_data[item_index * self.dims ..][0..self.dims];
    }
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

fn lessPreparedExistingBatchUpdate(_: void, lhs: PreparedExistingBatchUpdate, rhs: PreparedExistingBatchUpdate) bool {
    return if (lhs.leaf_id == rhs.leaf_id)
        lhs.item_index < rhs.item_index
    else
        lhs.leaf_id < rhs.leaf_id;
}

fn lessTargetPostingDeltaRecord(_: void, lhs: TargetPostingDeltaRecord, rhs: TargetPostingDeltaRecord) bool {
    return if (lhs.posting_id == rhs.posting_id)
        lhs.record.sequence < rhs.record.sequence
    else
        lhs.posting_id < rhs.posting_id;
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

fn batchWritesAreUniqueAndAbsent(self: anytype, txn: anytype, writes: []const hbc_runtime.BatchInsertItem) !bool {
    if (writes.len == 0) return true;

    var seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen.deinit(self.alloc);
    try seen.ensureTotalCapacity(self.alloc, @intCast(writes.len));
    for (writes) |item| {
        if (seen.contains(item.vector_id)) return false;
        try seen.put(self.alloc, item.vector_id, {});
        _ = self.getVecLeaf(txn, item.vector_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        return false;
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
    const node = try self.getNodePtr(txn, node_id);
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
    var child_id_stack: [128]u64 = undefined;
    const use_child_id_stack = node.children.len <= child_id_stack.len;
    const child_ids = if (use_child_id_stack)
        child_id_stack[0..node.children.len]
    else
        try self.alloc.dupe(u64, node.children);
    defer if (!use_child_id_stack) self.alloc.free(child_ids);
    if (use_child_id_stack) @memcpy(child_ids, node.children);

    const NodeReadHandle = CachedNodeReadHandle(@TypeOf(self));
    var child_handle_stack: [128]NodeReadHandle = undefined;
    const use_child_handle_stack = child_ids.len <= child_handle_stack.len;
    const child_handles = if (use_child_handle_stack)
        child_handle_stack[0..child_ids.len]
    else
        try self.alloc.alloc(NodeReadHandle, child_ids.len);
    defer if (!use_child_handle_stack) self.alloc.free(child_handles);
    var child_handle_count: usize = 0;
    defer {
        for (child_handles[0..child_handle_count]) |*handle| {
            handle.deinit(self.alloc);
        }
    }

    var child_node_stack: [128]*const types.Node = undefined;
    const use_child_node_stack = child_ids.len <= child_node_stack.len;
    const child_nodes = if (use_child_node_stack)
        child_node_stack[0..child_ids.len]
    else
        try self.alloc.alloc(*const types.Node, child_ids.len);
    defer if (!use_child_node_stack) self.alloc.free(child_nodes);
    for (child_ids, 0..) |child_id, child_index| {
        child_handles[child_index] = try loadNodeReadHandle(self, txn, child_id);
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

    const range_storage_len = try std.math.mul(usize, child_ids.len, 2);
    var range_storage_stack: [512]usize = undefined;
    const use_range_storage_stack = range_storage_len <= range_storage_stack.len;
    const range_storage = if (use_range_storage_stack)
        range_storage_stack[0..range_storage_len]
    else
        try self.alloc.alloc(usize, range_storage_len);
    defer if (!use_range_storage_stack) self.alloc.free(range_storage);
    const starts = range_storage[0..child_ids.len];
    const ends = range_storage[child_ids.len..range_storage_len];

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

    const index_storage_len = try std.math.mul(usize, item_count, 3);
    var index_storage_stack: [3072]usize = undefined;
    const use_index_storage_stack = index_storage_len <= index_storage_stack.len;
    const index_storage = if (use_index_storage_stack)
        index_storage_stack[0..index_storage_len]
    else
        try self.alloc.alloc(usize, index_storage_len);
    defer if (!use_index_storage_stack) self.alloc.free(index_storage);
    const current = index_storage[0..item_count];
    const next = index_storage[item_count .. 2 * item_count];
    const choices = index_storage[2 * item_count .. index_storage_len];

    var query_measure_stack: [1024]f32 = undefined;
    const use_query_measure_stack = item_count <= query_measure_stack.len;
    const query_measures = if (use_query_measure_stack)
        query_measure_stack[0..item_count]
    else
        try self.alloc.alloc(f32, item_count);
    defer if (!use_query_measure_stack) self.alloc.free(query_measures);

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
            self.invalidateVectorCache(item.vector_id);
        }
    } else if (shouldSeedRetainedVectorCacheOnSkipStore(self)) {
        for (sorted_item_indexes) |item_idx| {
            const item = items[item_idx];
            _ = self.cacheVector(item.vector_id, item.vector) catch {};
        }
    }
    store_vector_ns += elapsed_fn(store_start);

    if (prepared.len > 1) std.mem.sort(PreparedBatchInsert, prepared, {}, lessPreparedBatchInsert);

    const paired_scratch_len = try std.math.mul(usize, dims, 2);
    var paired_scratch_stack: [8192]f32 = undefined;
    const use_paired_scratch_stack = paired_scratch_len <= paired_scratch_stack.len;
    const paired_scratch = if (use_paired_scratch_stack)
        paired_scratch_stack[0..paired_scratch_len]
    else
        try self.alloc.alloc(f32, paired_scratch_len);
    defer if (!use_paired_scratch_stack) self.alloc.free(paired_scratch);
    const fallback_transformed = paired_scratch[0..dims];
    var mutate_leaf_ns: u64 = 0;
    var grouped_items: usize = 0;
    const centroid_sum = paired_scratch[dims..paired_scratch_len];
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
        const leaf_size: usize = @intCast(self.config.leaf_size);
        const max_batched_overflow_multiplier: usize = if (options.bulk_ingest and options.defer_leaf_splits_to_posting_maintenance) 8 else 4;
        const max_batched_overflow_members: usize = @max(leaf_size * max_batched_overflow_multiplier, leaf_size + 1);
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
        if (shouldWritePostingBaseDelta(self)) {
            const delta_records = try self.alloc.alloc(posting.PostingDeltaRecord, added_member_ids.len);
            defer self.alloc.free(delta_records);
            for (added_member_ids, 0..) |member_id, offset| {
                delta_records[offset] = .{
                    .sequence = postingDeltaSequence(leaf.posting_state.mutation_version, offset),
                    .op = .insert,
                    .vector_id = member_id,
                };
            }
            try shadowAppendPostingDeltas(self, txn, leaf.id, delta_records);
        }

        const transformed_source = PreparedTransformedVectorSource{
            .transformed_data = transformed_data,
            .entries = prepared[group_start..group_end],
            .dims = dims,
        };
        var updated_range: ?types.NodeSplitRange = null;
        defer if (updated_range) |*range| range.deinit(self.alloc);
        var range_changed = false;
        @memset(centroid_sum, 0);
        for (prepared[group_start..group_end]) |entry| {
            const item = items[entry.item_index];
            const transformed = transformed_data[entry.item_index * dims ..][0..dims];

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
            if (defer_leaf_split and shouldUseBaseDeltaAsCanonicalPosting(self)) {
                try saveDeferredBaseDeltaLeafMutationState(self, txn, &leaf);
            } else {
                try saveExistingNodeBodyWithAddedVectorSourceOptions(self, txn, &leaf, &transformed_source, save_options, now_fn_u64_adapter(now_fn), elapsed_fn_u64_adapter(elapsed_fn));
                self.write_profile.save_node_calls += 1;
                grouped_node_body_writes += 1;
            }
            self.write_profile.save_node_ns += elapsed_fn(save_start);
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
                if (shouldRecordDeferredOversizedLeafForSplitFinish(options)) {
                    try recordDeferredOversizedLeaf(self, leaf.id);
                }
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

    var batch = try self.beginRuntimeBatchTxn();
    errdefer batch.abort();
    try bulkBuildWithMetadataTxnOptions(self, &batch, items, options, now_fn, elapsed_fn);
    try finalizeWriteTxnOptions(self, &batch, .{}, now_fn, elapsed_fn);
    const commit_start = now_fn();
    const publishing = beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    try batch.commit();
    self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
    finishPublishSearchStateIfSupported(self, publishing);
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

pub fn bulkBuildExternalSequentialWithMetadataOptions(
    self: anytype,
    count: usize,
    options: bulk_build.BulkBuildOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (count == 0) return;

    var batch = try self.beginRuntimeBatchTxn();
    errdefer batch.abort();
    try bulkBuildExternalSequentialTxnOptions(self, &batch, count, options, now_fn, elapsed_fn);
    try finalizeWriteTxnOptions(self, &batch, .{}, now_fn, elapsed_fn);
    const commit_start = now_fn();
    const publishing = beginPublishSearchStateIfSupported(self);
    errdefer abortPublishSearchStateIfSupported(self, publishing);
    try batch.commit();
    self.write_profile.insert_commit_ns += elapsed_fn(commit_start);
    finishPublishSearchStateIfSupported(self, publishing);
}

pub fn bulkBuildExternalSequentialTxnOptions(
    self: anytype,
    txn: anytype,
    count: usize,
    options: bulk_build.BulkBuildOptions,
    now_fn: fn () u64,
    elapsed_fn: fn (u64) u64,
) !void {
    if (count == 0) return;
    if (!options.skip_vector_store) return error.InvalidArgument;
    if (!indexHasExternalVectorLoader(self)) return error.InvalidArgument;

    try self.bindTxnLike(txn);
    const empty_inputs: []const bulk_build.PreparedBulkBuildInput = &.{};
    try self.prepareEmptyPreparedBulkBuild(txn, empty_inputs);

    const dims: usize = @intCast(self.config.dims);
    const leaf_size = @max(@as(usize, 1), self.config.leaf_size);
    const target_leaf_size = leaf_size;
    const leaf_count = std.math.divCeil(usize, count, target_leaf_size) catch unreachable;
    var current = try self.alloc.alloc(BuiltBulkNode, leaf_count);
    var current_count: usize = 0;
    var current_owned = true;
    errdefer if (current_owned) {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    };

    const scratch_capacity = @min(count, @max(leaf_size, @as(usize, 4096)));
    const vector_ids = try self.alloc.alloc(u64, scratch_capacity);
    defer self.alloc.free(vector_ids);
    const transformed = try self.alloc.alloc(f32, scratch_capacity * dims);
    defer self.alloc.free(transformed);
    var inputs = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, leaf_size);
    defer self.alloc.free(inputs);

    const build_start = now_fn();
    switch (options.algo orelse self.config.bulk_build_algo) {
        .hilbert_seeded => {
            const Entry = struct {
                embedding_index: usize,
            };
            const SortContext = struct {
                embeddings: []const u8,
                embedding_len: usize,
            };

            const hilbert = try self.getHilbert();
            const embedding_len = hilbert.byteLen();
            var entries = try self.alloc.alloc(Entry, count);
            defer self.alloc.free(entries);
            const embeddings = try self.alloc.alloc(u8, count * embedding_len);
            defer self.alloc.free(embeddings);
            var coords_stack: [hilbert_coord_stack_capacity]u32 = undefined;
            const use_coords_stack = hilbert.dimension <= coords_stack.len;
            const coords = if (use_coords_stack)
                coords_stack[0..hilbert.dimension]
            else
                try self.alloc.alloc(u32, hilbert.dimension);
            defer if (!use_coords_stack) self.alloc.free(coords);

            var offset: usize = 0;
            while (offset < count) {
                const group_size = @min(scratch_capacity, count - offset);
                for (0..group_size) |i| {
                    const vector_id: u64 = @intCast(offset + i + 1);
                    vector_ids[i] = vector_id;
                }
                try loadTransformedVectorIdsIntoMatrix(self, txn, vector_ids[0..group_size], transformed[0 .. group_size * dims], .{});
                for (0..group_size) |i| {
                    const entry_index = offset + i;
                    const embedding = embeddings[entry_index * embedding_len ..][0..embedding_len];
                    try hilbert.encodeVecBytesInto(transformed[i * dims ..][0..dims], coords, embedding);
                    entries[entry_index] = .{
                        .embedding_index = entry_index,
                    };
                }
                offset += group_size;
            }

            std.mem.sort(Entry, entries, SortContext{ .embeddings = embeddings, .embedding_len = embedding_len }, struct {
                fn lessThan(ctx: SortContext, a: Entry, b: Entry) bool {
                    const left_offset = a.embedding_index * ctx.embedding_len;
                    const right_offset = b.embedding_index * ctx.embedding_len;
                    const left = ctx.embeddings[left_offset..][0..ctx.embedding_len];
                    const right = ctx.embeddings[right_offset..][0..ctx.embedding_len];
                    return std.mem.order(u8, left, right) == .lt;
                }
            }.lessThan);

            var entry_offset: usize = 0;
            while (entry_offset < count) {
                const group_size = @min(target_leaf_size, count - entry_offset);
                for (0..group_size) |i| vector_ids[i] = @intCast(entries[entry_offset + i].embedding_index + 1);
                try loadTransformedVectorIdsIntoMatrix(self, txn, vector_ids[0..group_size], transformed[0 .. group_size * dims], .{});
                for (0..group_size) |i| {
                    inputs[i] = .{
                        .vector_id = vector_ids[i],
                        .vector = &.{},
                        .transformed = transformed[i * dims ..][0..dims],
                        .metadata = "",
                    };
                }
                current[current_count] = try buildBulkLeaf(self, txn, self.nextNodeId(), inputs[0..group_size], 0, 0, count <= target_leaf_size);
                current_count += 1;
                entry_offset += group_size;
            }
        },
        .doc_key_seeded => {
            var offset: usize = 0;
            while (offset < count) {
                const group_size = @min(target_leaf_size, count - offset);
                for (0..group_size) |i| {
                    const vector_id: u64 = @intCast(offset + i + 1);
                    vector_ids[i] = vector_id;
                }
                try loadTransformedVectorIdsIntoMatrix(self, txn, vector_ids[0..group_size], transformed[0 .. group_size * dims], .{});
                for (0..group_size) |i| {
                    inputs[i] = .{
                        .vector_id = vector_ids[i],
                        .vector = &.{},
                        .transformed = transformed[i * dims ..][0..dims],
                        .metadata = "",
                    };
                }
                current[current_count] = try buildBulkLeaf(self, txn, self.nextNodeId(), inputs[0..group_size], 0, 0, count <= target_leaf_size);
                current_count += 1;
                offset += group_size;
            }
        },
        .recursive, .kmeans => return error.InvalidArgument,
    }

    var built = if (current_count == 1) blk: {
        const root = current[0].take();
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
        current_owned = false;
        break :blk root;
    } else blk: {
        current_owned = false;
        break :blk try buildBulkParentLevels(self, txn, current, current_count);
    };
    defer built.deinit(self.alloc);
    self.write_profile.bulk_build_tree_ns += elapsed_fn(build_start);
    self.metadata.root_node = built.node_id;
    self.metadata.active_count = @intCast(count);
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
        input_index: usize,
    };
    const SortContext = struct {
        embeddings: []const u8,
        embedding_len: usize,
    };

    const entries = try self.alloc.alloc(Entry, inputs.len);
    defer self.alloc.free(entries);
    const hilbert = try self.getHilbert();
    const embedding_len = hilbert.byteLen();
    const embeddings = try self.alloc.alloc(u8, inputs.len * embedding_len);
    defer self.alloc.free(embeddings);
    var coords_stack: [hilbert_coord_stack_capacity]u32 = undefined;
    const use_coords_stack = hilbert.dimension <= coords_stack.len;
    const coords = if (use_coords_stack)
        coords_stack[0..hilbert.dimension]
    else
        try self.alloc.alloc(u32, hilbert.dimension);
    defer if (!use_coords_stack) self.alloc.free(coords);

    for (inputs, 0..) |input, i| {
        const embedding = embeddings[i * embedding_len ..][0..embedding_len];
        try hilbert.encodeVecBytesInto(input.transformed, coords, embedding);
        entries[i] = .{
            .input_index = i,
        };
    }
    std.mem.sort(Entry, entries, SortContext{ .embeddings = embeddings, .embedding_len = embedding_len }, struct {
        fn lessThan(ctx: SortContext, a: Entry, b: Entry) bool {
            const left_offset = a.input_index * ctx.embedding_len;
            const right_offset = b.input_index * ctx.embedding_len;
            const left = ctx.embeddings[left_offset..][0..ctx.embedding_len];
            const right = ctx.embeddings[right_offset..][0..ctx.embedding_len];
            return std.mem.order(u8, left, right) == .lt;
        }
    }.lessThan);

    const leaf_groups = try bulk_build.planBalancedGroupSizes(self.alloc, entries.len, @max(@as(usize, 1), self.config.leaf_size));
    defer self.alloc.free(leaf_groups);

    var current_count: usize = 0;
    var current = try self.alloc.alloc(BuiltBulkNode, leaf_groups.len);
    defer {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    }

    const group_inputs_scratch = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, @max(@as(usize, 1), self.config.leaf_size));
    defer self.alloc.free(group_inputs_scratch);

    var entry_cursor: usize = 0;
    for (leaf_groups) |group_size| {
        const node_id = self.nextNodeId();
        const group_inputs = group_inputs_scratch[0..group_size];
        for (0..group_size) |i| {
            group_inputs[i] = inputs[entries[entry_cursor + i].input_index];
        }
        current[current_count] = try buildBulkLeaf(self, txn, node_id, group_inputs, 0, 0, leaf_groups.len == 1);
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
            var child_ids_stack: [bulk_parent_child_ids_stack_capacity]u64 = undefined;
            const use_child_ids_stack = group_size <= child_ids_stack.len;
            var child_ids = if (use_child_ids_stack)
                child_ids_stack[0..group_size]
            else
                try self.alloc.alloc(u64, group_size);
            errdefer if (!use_child_ids_stack) self.alloc.free(child_ids);

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
            try self.saveNodeBody(txn, &node);
            try self.putNodeSplitRange(txn, node_id, if (merged_range) |*owned| owned else null);
            if (!use_child_ids_stack) self.alloc.free(child_ids);

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

    return current[0].take();
}

pub fn buildBulkDocKeySeeded(
    self: anytype,
    txn: anytype,
    inputs: []const bulk_build.PreparedBulkBuildInput,
) !BuiltBulkNode {
    const Entry = struct {
        input_index: usize,
    };

    const entries = try self.alloc.alloc(Entry, inputs.len);
    defer self.alloc.free(entries);
    for (entries, 0..) |*entry, i| entry.* = .{ .input_index = i };

    std.mem.sort(Entry, entries, inputs, struct {
        fn lessThan(source: []const bulk_build.PreparedBulkBuildInput, a: Entry, b: Entry) bool {
            const left = source[a.input_index];
            const right = source[b.input_index];
            return switch (std.mem.order(u8, left.metadata, right.metadata)) {
                .lt => true,
                .eq => left.vector_id < right.vector_id,
                .gt => false,
            };
        }
    }.lessThan);

    const leaf_groups = try bulk_build.planBalancedGroupSizes(self.alloc, entries.len, @max(@as(usize, 1), self.config.leaf_size));
    defer self.alloc.free(leaf_groups);

    var current_count: usize = 0;
    var current = try self.alloc.alloc(BuiltBulkNode, leaf_groups.len);
    defer {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    }

    const group_inputs_scratch = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, @max(@as(usize, 1), self.config.leaf_size));
    defer self.alloc.free(group_inputs_scratch);

    var entry_cursor: usize = 0;
    for (leaf_groups) |group_size| {
        const node_id = self.nextNodeId();
        const group_inputs = group_inputs_scratch[0..group_size];
        for (0..group_size) |i| {
            group_inputs[i] = inputs[entries[entry_cursor + i].input_index];
        }
        current[current_count] = try buildBulkLeaf(self, txn, node_id, group_inputs, 0, 0, leaf_groups.len == 1);
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
        return try buildBulkLeaf(self, txn, self.nextNodeId(), inputs, 0, 0, true);
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

    var leaf_group_count: usize = 0;
    var leaf_count_cluster_start: usize = 0;
    while (leaf_count_cluster_start < entries.len) {
        const cluster = entries[leaf_count_cluster_start].cluster;
        var cluster_end = leaf_count_cluster_start + 1;
        while (cluster_end < entries.len and entries[cluster_end].cluster == cluster) : (cluster_end += 1) {}
        leaf_group_count += std.math.divCeil(usize, cluster_end - leaf_count_cluster_start, leaf_size) catch unreachable;
        leaf_count_cluster_start = cluster_end;
    }

    var current_count: usize = 0;
    var current = try self.alloc.alloc(BuiltBulkNode, leaf_group_count);
    defer {
        for (current[0..current_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(current);
    }
    const group_inputs_scratch = try self.alloc.alloc(bulk_build.PreparedBulkBuildInput, leaf_size);
    defer self.alloc.free(group_inputs_scratch);

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
            const group_inputs = group_inputs_scratch[0..group_size];
            for (0..group_size) |i| {
                group_inputs[i] = inputs[entries[entry_cursor + i].point_index];
            }
            current[current_count] = try buildBulkLeaf(self, txn, node_id, group_inputs, 0, 0, leaf_group_count == 1);
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

fn capacityBalanceLeafSplitToMaxMembers(
    self: anytype,
    vectors: *const vec.Set,
    ids: []const u64,
    split: *SplitResult,
    max_members: usize,
) !void {
    if (max_members == 0 or ids.len > max_members * 2) return;
    if (split.g1.len <= max_members and split.g2.len <= max_members) return;

    const count = ids.len;
    const target_left_count = if (split.g1.len > max_members)
        max_members
    else
        count - max_members;
    if (target_left_count == 0 or target_left_count >= count) return;

    var offsets_stack: [1024]usize = undefined;
    const use_offsets_stack = count <= offsets_stack.len;
    const offsets = if (use_offsets_stack)
        offsets_stack[0..count]
    else
        try self.alloc.alloc(usize, count);
    defer if (!use_offsets_stack) self.alloc.free(offsets);

    var margins_stack: [1024]f32 = undefined;
    const use_margins_stack = count <= margins_stack.len;
    const margins = if (use_margins_stack)
        margins_stack[0..count]
    else
        try self.alloc.alloc(f32, count);
    defer if (!use_margins_stack) self.alloc.free(margins);

    var assignments_stack: [1024]u64 = undefined;
    const use_assignments_stack = count <= assignments_stack.len;
    const assignments = if (use_assignments_stack)
        assignments_stack[0..count]
    else
        try self.alloc.alloc(u64, count);
    defer if (!use_assignments_stack) self.alloc.free(assignments);

    const spherical = self.config.metric == .cosine;
    var inv_left_norm: f32 = 1;
    var inv_right_norm: f32 = 1;
    if (spherical) {
        const ln = vec.norm(split.c1);
        if (ln != 0) inv_left_norm = 1.0 / ln;
        const rn = vec.norm(split.c2);
        if (rn != 0) inv_right_norm = 1.0 / rn;
    }

    for (0..count) |i| {
        offsets[i] = i;
        const v = vectors.atConst(i);
        const left_dist = if (spherical)
            -vec.dot(v, split.c1) * inv_left_norm
        else if (self.config.metric == .inner_product)
            -vec.dot(v, split.c1)
        else
            vec.l2SquaredDistance(v, split.c1);
        const right_dist = if (spherical)
            -vec.dot(v, split.c2) * inv_right_norm
        else if (self.config.metric == .inner_product)
            -vec.dot(v, split.c2)
        else
            vec.l2SquaredDistance(v, split.c2);
        margins[i] = left_dist - right_dist;
    }
    stableSortOffsetsByDistance(offsets, margins);

    for (0..count) |i| assignments[i] = 1;
    for (offsets[0..target_left_count]) |offset| assignments[offset] = 0;

    const left_centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(left_centroid);
    const right_centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(right_centroid);
    calcPartitionCentroids(vectors, count, assignments, left_centroid, right_centroid);
    if (self.config.metric == .cosine) {
        _ = vec.normalize(left_centroid);
        _ = vec.normalize(right_centroid);
    }

    const out_g1 = try self.alloc.alloc(u64, target_left_count);
    errdefer self.alloc.free(out_g1);
    const out_g2 = try self.alloc.alloc(u64, count - target_left_count);
    errdefer self.alloc.free(out_g2);
    var left_pos: usize = 0;
    var right_pos: usize = 0;
    for (assignments, 0..) |assignment, i| {
        if (assignment == 0) {
            out_g1[left_pos] = ids[i];
            left_pos += 1;
        } else {
            out_g2[right_pos] = ids[i];
            right_pos += 1;
        }
    }

    self.alloc.free(split.c1);
    self.alloc.free(split.g1);
    self.alloc.free(split.c2);
    self.alloc.free(split.g2);
    split.* = if (out_g1.len >= out_g2.len)
        .{
            .c1 = left_centroid,
            .g1 = out_g1,
            .c2 = right_centroid,
            .g2 = out_g2,
        }
    else
        .{
            .c1 = right_centroid,
            .g1 = out_g2,
            .c2 = left_centroid,
            .g2 = out_g1,
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
        hbc_runtime.QuantizedSet{ .nonquant = proto.NonQuantizedVectorSet.decode(self.alloc, data) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.Corrupted,
        } }
    else
        hbc_runtime.QuantizedSet{ .rabit = proto.RaBitQuantizedVectorSet.decode(self.alloc, data) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.Corrupted,
        } };
    errdefer decoded.deinit(self.alloc);
    try validateQuantizedSet(self, &decoded, expected_count);
    return decoded;
}

pub fn getQuantized(self: anytype, txn: anytype, node_id: u64, is_root: bool, expected_count: usize, is_not_found: fn (anyerror) bool) !?*const hbc_runtime.QuantizedSet {
    if (self.getCachedQuantizedPtr(node_id)) |cached| {
        validateQuantizedSet(self, cached, expected_count) catch |err| switch (err) {
            error.Corrupted => self.invalidateQuantizedCache(node_id),
        };
        if (self.getCachedQuantizedPtr(node_id)) |valid| return valid;
    }

    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) return null;
        return err;
    };
    return try self.cacheQuantizedOwned(node_id, decoded);
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
) !?*const hbc_runtime.QuantizedSet {
    if (self.getCachedQuantizedPtr(node_id)) |cached| {
        validateQuantizedSet(self, cached, expected_count) catch |err| switch (err) {
            error.Corrupted => self.invalidateQuantizedCache(node_id),
        };
        if (self.getCachedQuantizedPtr(node_id)) |valid| return valid;
    }

    const start = now_fn();
    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) {
            profile.quantized_cache_miss_ns += elapsed_fn(start);
            profile.quantized_cache_misses += 1;
            return null;
        }
        return err;
    };
    const cached = try self.cacheQuantizedOwned(node_id, decoded);
    profile.quantized_cache_miss_ns += elapsed_fn(start);
    profile.quantized_cache_misses += 1;
    return cached;
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
    if (try self.getCachedQuantizedClone(node_id)) |cached| {
        validateQuantizedSet(self, &cached, expected_count) catch |err| switch (err) {
            error.Corrupted => self.invalidateQuantizedCache(node_id),
        };
        if (try self.getCachedQuantizedClone(node_id)) |valid| return valid;
    }

    const start = now_fn();
    const decoded = loadQuantized(self, txn, node_id, is_root, expected_count, is_not_found) catch |err| {
        if (is_not_found(err) or err == error.Corrupted) {
            profile.quantized_cache_miss_ns += elapsed_fn(start);
            profile.quantized_cache_misses += 1;
            return null;
        }
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
    var vectors_owned = true;
    defer if (vectors_owned) self.alloc.free(vectors);

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
            var child = if (shouldUseBaseDeltaAsCanonicalPosting(self))
                try self.loadNodeFromStorage(txn, child_id)
            else
                try loadNode(self, txn, child_id);
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
        try posting.PostingStore.refreshQuantizedPayload(self, txn, node, vectors, &vectors_owned, now_fn, elapsed_fn);
        return;
    }

    if (self.getCachedQuantizedPtr(node.id)) |cached| {
        switch (cached.*) {
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
                if (set.vectors.data.len > 0) self.alloc.free(set.vectors.data);
                set.vectors.data = vectors;
                vectors_owned = false;
                noteMutatedCachedQuantized(self, node.id);
                const store_start = now_fn();
                try putQuantizedCached(self, txn, node.id, cached, now_fn, elapsed_fn);
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
                            .data = vectors,
                        },
                    } };
                    vectors_owned = false;
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
                noteMutatedCachedQuantized(self, node.id);
                const store_start = now_fn();
                try putQuantizedCached(self, txn, node.id, cached, now_fn, elapsed_fn);
                self.write_profile.quantized_store_ns += elapsed_fn(store_start);
                return;
            },
        }
    }

    const compute_start = now_fn();
    var qs: hbc_runtime.QuantizedSet = if (usesNonQuantizedPayload(node)) blk: {
        vectors_owned = false;
        break :blk .{ .nonquant = .{
            .vectors = .{
                .dims = @intCast(dims),
                .count = @intCast(count),
                .data = vectors,
            },
        } };
    } else .{ .rabit = try self.quantizer.quantize(node.centroid, vectors, count) };
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
    const dims: usize = @intCast(self.config.dims);
    const scratch_len = try std.math.mul(usize, dims, 3);
    var scratch_stack: [12288]f32 = undefined;
    const use_stack_scratch = scratch_len <= scratch_stack.len;
    const scratch = if (use_stack_scratch)
        scratch_stack[0..scratch_len]
    else
        try self.alloc.alloc(f32, scratch_len);
    defer if (!use_stack_scratch) self.alloc.free(scratch);
    const transformed_vector = scratch[0..dims];
    const previous_vector_storage = scratch[dims .. 2 * dims];
    const previous_transformed_storage = scratch[2 * dims .. scratch_len];
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
                if (options.coalesce_leaf_writes and try existingVectorMatchesStoredVector(self, txn, item.vector_id, item.vector, previous_vector_storage)) {
                    if (try tryCoalesceExistingVectorInLeafTxnOptions(
                        self,
                        txn,
                        existing_leaf_id,
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
                }
                const find_leaf_start = nowNsU64Fixed();
                const allow_quantized_routing = if (shouldRouteExistingUpdatesByCentroidOnly(self))
                    false
                else if (@hasField(@TypeOf(options), "allow_quantized_routing"))
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
    var leaf_vectors_owned = true;
    defer if (leaf_vectors_owned) self.alloc.free(leaf_vectors);
    for (indexes, 0..) |input_idx, i| {
        const transformed = inputs[input_idx].transformed;
        @memcpy(leaf_vectors[i * self.config.dims ..][0..self.config.dims], transformed);
    }
    try saveLeafNodeBodyWithKnownVectors(self, txn, &node, leaf_vectors, &leaf_vectors_owned, true, nowNsI128Fixed, elapsedSinceNsFixed);
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
            var child_ids_stack: [bulk_parent_child_ids_stack_capacity]u64 = undefined;
            const use_child_ids_stack = group_size <= child_ids_stack.len;
            var child_ids = if (use_child_ids_stack)
                child_ids_stack[0..group_size]
            else
                try self.alloc.alloc(u64, group_size);
            errdefer if (!use_child_ids_stack) self.alloc.free(child_ids);

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
            try self.saveNodeBody(txn, &node);
            try self.putNodeSplitRange(txn, node_id, if (merged_range) |*owned| owned else null);
            if (!use_child_ids_stack) self.alloc.free(child_ids);

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

    if (owns_current) return current[0].take();
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

    if (owns_current) return current[0].take();
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

    var output_group_count: usize = 0;
    var count_cluster_start: usize = 0;
    while (count_cluster_start < entries.len) {
        const cluster = entries[count_cluster_start].cluster;
        var cluster_end = count_cluster_start + 1;
        while (cluster_end < entries.len and entries[cluster_end].cluster == cluster) : (cluster_end += 1) {}
        output_group_count += std.math.divCeil(usize, cluster_end - count_cluster_start, max_group_size) catch unreachable;
        count_cluster_start = cluster_end;
    }

    var out = try self.alloc.alloc(BuiltBulkNode, output_group_count);
    var out_count: usize = 0;
    errdefer {
        for (out[0..out_count]) |*node| node.deinit(self.alloc);
        self.alloc.free(out);
    }
    const group_scratch = try self.alloc.alloc(usize, max_group_size);
    defer self.alloc.free(group_scratch);

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
            const group = group_scratch[0..group_size];
            for (0..group_size) |i| {
                group[i] = entries[entry_cursor + i].point_index;
            }
            out[out_count] = try buildBulkParentFromNodeIndexes(self, txn, nodes, group, level);
            out_count += 1;
            entry_cursor += group_size;
        }
        self.alloc.free(groups);

        cluster_start = cluster_end;
    }

    return out[0..out_count];
}

fn buildBulkParentFromNodeRange(
    self: anytype,
    txn: anytype,
    nodes: []BuiltBulkNode,
    level: u16,
) !BuiltBulkNode {
    return try buildBulkParentFromNodeSelection(self, txn, nodes, null, level);
}

fn buildBulkParentFromNodeIndexes(
    self: anytype,
    txn: anytype,
    nodes: []BuiltBulkNode,
    indexes: []const usize,
    level: u16,
) !BuiltBulkNode {
    return try buildBulkParentFromNodeSelection(self, txn, nodes, indexes, level);
}

fn buildBulkParentFromNodeSelection(
    self: anytype,
    txn: anytype,
    nodes: []BuiltBulkNode,
    indexes: ?[]const usize,
    level: u16,
) !BuiltBulkNode {
    const node_id = self.nextNodeId();
    const child_count = if (indexes) |selected| selected.len else nodes.len;
    var child_ids_stack: [bulk_parent_child_ids_stack_capacity]u64 = undefined;
    const use_child_ids_stack = child_count <= child_ids_stack.len;
    var child_ids = if (use_child_ids_stack)
        child_ids_stack[0..child_count]
    else
        try self.alloc.alloc(u64, child_count);
    errdefer if (!use_child_ids_stack) self.alloc.free(child_ids);

    const centroid = try self.alloc.alloc(f32, self.config.dims);
    errdefer self.alloc.free(centroid);
    @memset(centroid, 0);

    var merged_range: ?types.NodeSplitRange = null;
    errdefer if (merged_range) |*owned| owned.deinit(self.alloc);

    var member_count: usize = 0;
    for (0..child_count) |i| {
        const node_index = if (indexes) |selected| selected[i] else i;
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
    try self.saveNodeBody(txn, &node);
    try self.putNodeSplitRange(txn, node_id, if (merged_range) |*owned| owned else null);
    if (!use_child_ids_stack) self.alloc.free(child_ids);

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
    publish_centroid_directory: bool,
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
    const leaf_vectors = try self.alloc.alloc(f32, inputs.len * self.config.dims);
    var leaf_vectors_owned = true;
    defer if (leaf_vectors_owned) self.alloc.free(leaf_vectors);
    for (inputs, 0..) |input, i| {
        @memcpy(leaf_vectors[i * self.config.dims ..][0..self.config.dims], input.transformed);
    }
    try saveLeafNodeBodyWithKnownVectors(self, txn, &node, leaf_vectors, &leaf_vectors_owned, publish_centroid_directory, nowNsI128Fixed, elapsedSinceNsFixed);
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
    const centroid_scratch = try self.alloc.alloc(f32, dims * 2);
    defer self.alloc.free(centroid_scratch);
    const new_left = centroid_scratch[0..dims];
    const new_right = centroid_scratch[dims .. dims * 2];

    var assignments_stack: [1024]u64 = undefined;
    const use_assignments_stack = count <= assignments_stack.len;
    const assignments = if (use_assignments_stack)
        assignments_stack[0..count]
    else
        try self.alloc.alloc(u64, count);
    defer if (!use_assignments_stack) self.alloc.free(assignments);

    var temp_dists_stack: [1024]f32 = undefined;
    const use_temp_dists_stack = count <= temp_dists_stack.len;
    const temp_dists = if (use_temp_dists_stack)
        temp_dists_stack[0..count]
    else
        try self.alloc.alloc(f32, count);
    defer if (!use_temp_dists_stack) self.alloc.free(temp_dists);

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
    var left_flags_stack: [1024]bool = undefined;
    const use_left_flags_stack = count <= left_flags_stack.len;
    const left_flags = if (use_left_flags_stack)
        left_flags_stack[0..count]
    else
        try self.alloc.alloc(bool, count);
    defer if (!use_left_flags_stack) self.alloc.free(left_flags);
    @memset(left_flags, false);

    const Entry = struct {
        index: usize,
    };
    const SortContext = struct {
        embeddings: []const u8,
        embedding_len: usize,
    };
    const entries = try self.alloc.alloc(Entry, count);
    defer self.alloc.free(entries);

    const hilbert = try self.getHilbert();
    const embedding_len = hilbert.byteLen();
    const embeddings = try self.alloc.alloc(u8, count * embedding_len);
    defer self.alloc.free(embeddings);
    var coords_stack: [hilbert_coord_stack_capacity]u32 = undefined;
    const use_coords_stack = hilbert.dimension <= coords_stack.len;
    const coords = if (use_coords_stack)
        coords_stack[0..hilbert.dimension]
    else
        try self.alloc.alloc(u32, hilbert.dimension);
    defer if (!use_coords_stack) self.alloc.free(coords);

    for (0..count) |i| {
        const embedding = embeddings[i * embedding_len ..][0..embedding_len];
        try hilbert.encodeVecBytesInto(vectors.atConst(i), coords, embedding);
        entries[i] = .{
            .index = i,
        };
    }

    std.mem.sort(Entry, entries, SortContext{ .embeddings = embeddings, .embedding_len = embedding_len }, struct {
        fn lessThan(ctx: SortContext, a: Entry, b: Entry) bool {
            const left_offset = a.index * ctx.embedding_len;
            const right_offset = b.index * ctx.embedding_len;
            const left = ctx.embeddings[left_offset..][0..ctx.embedding_len];
            const right = ctx.embeddings[right_offset..][0..ctx.embedding_len];
            return std.mem.order(u8, left, right) == .lt;
        }
    }.lessThan);

    const split_point = count / 2;
    for (entries[0..split_point]) |entry| {
        left_flags[entry.index] = true;
    }

    const left_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(left_centroid);
    const right_centroid = try self.alloc.alloc(f32, dims);
    errdefer self.alloc.free(right_centroid);
    @memset(left_centroid, 0);
    @memset(right_centroid, 0);

    const out_g1 = try self.alloc.alloc(u64, split_point);
    errdefer self.alloc.free(out_g1);
    const out_g2 = try self.alloc.alloc(u64, count - split_point);
    errdefer self.alloc.free(out_g2);

    var left_pos: usize = 0;
    var right_pos: usize = 0;
    for (left_flags, 0..) |is_left, i| {
        if (is_left) {
            out_g1[left_pos] = ids[i];
            left_pos += 1;
            vec.add(left_centroid, vectors.atConst(i));
        } else {
            out_g2[right_pos] = ids[i];
            right_pos += 1;
            vec.add(right_centroid, vectors.atConst(i));
        }
    }

    vec.scale(1.0 / @as(f32, @floatFromInt(out_g1.len)), left_centroid);
    vec.scale(1.0 / @as(f32, @floatFromInt(out_g2.len)), right_centroid);

    if (self.config.metric == .cosine) {
        _ = vec.normalize(left_centroid);
        _ = vec.normalize(right_centroid);
    }

    return .{
        .c1 = left_centroid,
        .g1 = out_g1,
        .c2 = right_centroid,
        .g2 = out_g2,
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

    var offsets_stack: [1024]usize = undefined;
    const use_offsets_stack = count <= offsets_stack.len;
    const offsets = if (use_offsets_stack)
        offsets_stack[0..count]
    else
        self.alloc.alloc(usize, count) catch return;
    defer if (!use_offsets_stack) self.alloc.free(offsets);
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
