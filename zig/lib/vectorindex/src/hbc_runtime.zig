// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const vec = @import("antfly_vector").vector;
const proto = @import("antfly_vector").proto;
const search_runtime = @import("search_runtime.zig");
const types = @import("types.zig");

pub const QuantizedSet = union(enum) {
    rabit: proto.RaBitQuantizedVectorSet,
    nonquant: proto.NonQuantizedVectorSet,

    pub fn getCount(self: *const QuantizedSet) usize {
        return switch (self.*) {
            .rabit => |*set| set.getCount(),
            .nonquant => |*set| set.getCount(),
        };
    }

    pub fn clone(self: *const QuantizedSet, alloc: Allocator) !QuantizedSet {
        return switch (self.*) {
            .rabit => |*set| .{ .rabit = try set.clone(alloc) },
            .nonquant => |*set| .{ .nonquant = try set.clone(alloc) },
        };
    }

    /// Copy an ordered subset without changing its scoring origin or error
    /// metadata. The caller binds row offsets to one posting mutation version;
    /// offsets from another membership revision must never be reused here.
    pub fn selectRows(self: *const QuantizedSet, alloc: Allocator, rows: []const usize) !QuantizedSet {
        const count = switch (self.*) {
            .rabit => |set| set.getCount(),
            .nonquant => |set| std.math.cast(usize, set.vectors.count) orelse return error.InvalidPostingRows,
        };
        for (rows, 0..) |row, i| {
            if (row >= count or (i != 0 and row <= rows[i - 1])) return error.InvalidPostingRows;
        }
        var result: QuantizedSet = switch (self.*) {
            .nonquant => .{ .nonquant = .{} },
            .rabit => .{ .rabit = .{} },
        };
        errdefer result.deinit(alloc);
        switch (self.*) {
            .nonquant => |set| {
                const dims = std.math.cast(usize, set.vectors.dims) orelse return error.InvalidPostingRows;
                if (set.vectors.data.len != try std.math.mul(usize, count, dims)) return error.InvalidPostingRows;
                result.nonquant.vectors = .{
                    .dims = set.vectors.dims,
                    .count = @intCast(rows.len),
                    .data = try selectRowPlane(f32, alloc, set.vectors.data, dims, rows),
                };
            },
            .rabit => |set| {
                const width = std.math.cast(usize, set.codes.width) orelse return error.InvalidPostingRows;
                if (width == 0 or set.codes.count != count or
                    set.codes.data.len != try std.math.mul(usize, count, width) or
                    set.centroid_distances.len != count or set.quantized_dot_products.len != count or
                    (set.centroid_dot_products.len != 0 and set.centroid_dot_products.len != count) or
                    (set.metric != .l2_squared and set.centroid_dot_products.len != count)) return error.InvalidPostingRows;
                const out = &result.rabit;
                out.metric = set.metric;
                out.centroid_norm = set.centroid_norm;
                out.centroid = try alloc.dupe(f32, set.centroid);
                out.codes = .{ .count = @intCast(rows.len), .width = set.codes.width, .data = try selectRowPlane(u64, alloc, set.codes.data, width, rows) };
                out.code_counts = try selectRowPlane(u32, alloc, set.code_counts, 1, rows);
                out.centroid_distances = try selectRowPlane(f32, alloc, set.centroid_distances, 1, rows);
                out.quantized_dot_products = try selectRowPlane(f32, alloc, set.quantized_dot_products, 1, rows);
                if (set.centroid_dot_products.len != 0) out.centroid_dot_products = try selectRowPlane(f32, alloc, set.centroid_dot_products, 1, rows);
            },
        }
        return result;
    }

    fn selectRowPlane(comptime T: type, alloc: Allocator, data: []const T, width: usize, rows: []const usize) ![]T {
        const out = try alloc.alloc(T, try std.math.mul(usize, rows.len, width));
        for (rows, 0..) |row, i| @memcpy(out[i * width ..][0..width], data[row * width ..][0..width]);
        return out;
    }

    pub fn deinit(self: *QuantizedSet, alloc: Allocator) void {
        switch (self.*) {
            .rabit => |*set| set.deinit(alloc),
            .nonquant => |*set| set.deinit(alloc),
        }
        self.* = undefined;
    }
};

test "posting row selection preserves scoring origin and is allocation safe" {
    const alloc = std.testing.allocator;
    const Attempt = struct {
        fn run(a: Allocator, source: *const QuantizedSet) !void {
            var selected = try source.selectRows(a, &.{ 0, 2 });
            defer selected.deinit(a);
            switch (source.*) {
                .nonquant => |set| {
                    try std.testing.expectEqualSlices(f32, set.vectors.data[0..2], selected.nonquant.vectors.data[0..2]);
                    try std.testing.expectEqualSlices(f32, set.vectors.data[4..6], selected.nonquant.vectors.data[2..4]);
                },
                .rabit => |set| {
                    try std.testing.expectEqualSlices(f32, set.centroid, selected.rabit.centroid);
                    try std.testing.expectEqual(set.centroid_norm, selected.rabit.centroid_norm);
                    for ([_]usize{ 0, 2 }, 0..) |row, i| {
                        try std.testing.expectEqualSlices(u64, set.codes.atConst(row), selected.rabit.codes.atConst(i));
                        try std.testing.expectEqual(set.code_counts[row], selected.rabit.code_counts[i]);
                        try std.testing.expectEqual(set.centroid_distances[row], selected.rabit.centroid_distances[i]);
                        try std.testing.expectEqual(set.quantized_dot_products[row], selected.rabit.quantized_dot_products[i]);
                    }
                },
            }
        }
    };
    for ([_]vec.DistanceMetric{ .l2_squared, .cosine, .inner_product }) |metric| {
        var q = try @import("antfly_vector").quantizer.RaBitQuantizer.init(alloc, 2, 42, metric);
        defer q.deinit();
        var source: QuantizedSet = .{ .rabit = try q.quantize(&.{ 0.5, 0.5 }, &.{ 1, 2, 3, 4, 5, 6 }, 3) };
        defer source.deinit(alloc);
        try std.testing.checkAllAllocationFailures(alloc, Attempt.run, .{&source});
        try std.testing.expectError(error.InvalidPostingRows, source.selectRows(alloc, &.{ 1, 1 }));
        try std.testing.expectError(error.InvalidPostingRows, source.selectRows(alloc, &.{ 2, 0 }));
        try std.testing.expectError(error.InvalidPostingRows, source.selectRows(alloc, &.{3}));
        var empty = try source.selectRows(alloc, &.{});
        defer empty.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), empty.getCount());
    }
    var source: QuantizedSet = .{ .nonquant = .{ .vectors = .{ .dims = 2, .count = 3, .data = try alloc.dupe(f32, &.{ 1, 2, 3, 4, 5, 6 }) } } };
    defer source.deinit(alloc);
    try std.testing.checkAllAllocationFailures(alloc, Attempt.run, .{&source});
}

/// One immutable leaf scoring row borrowed from a generation lease. Keeping
/// membership beside the fixed-width candidate plane removes the packed-node
/// lookup and copy from the flat-directory search path. The source generation
/// remains authoritative: adapters decline this view as soon as any leaf
/// membership, posting state, or quantized payload is shadowed by a delta.
pub const NativeLeafScanView = struct {
    member_ids: []const u64,
    quantized: QuantizedSet,
    /// Native base/delta rows under the same complete generation lease. When
    /// present, quantized is only a placeholder and must not be scored/freed.
    row_snapshot: ?*const @import("posting_row_delta.zig").Snapshot = null,
    /// Optional source-space float16 rows owned by the same immutable posting
    /// generation. Rows follow member_ids exactly; per-row metadata makes the
    /// resulting score interval conservative enough to defer authoritative
    /// residual reads until the public top-k boundary is known.
    projections: ?NativeProjectionPlane = null,
    subgroup_plan: ?@import("posting_subgroups.zig").View = null,
};

pub const NativeProjectionPlane = struct {
    dims: usize,
    values: []const f16,
    scales: []const f32,
    error_norms: []const f32,
    decoded_norm_lower_bounds: []const f32,
    /// Source vector payload CRCs. Version-three posting generations omit
    /// this column and remain scoreable, but cannot use residual-only exact
    /// completion without revalidating the projection payload.
    checksums: []const u32 = &.{},
    verification: ?[]std.atomic.Value(u8) = null,
    /// Optional generation-bound locations of the lossless residuals in the
    /// shared exact-vector store. Older posting generations omit this plane;
    /// callers must then resolve the artifact key through the authoritative
    /// directory. A location is only a hint until the exact-vector generation
    /// validates its generation, shard, sequence, projection checksum, and
    /// residual checksum.
    residual_locations: ?NativeResidualLocationPlane = null,

    pub fn validateRow(self: @This(), row: usize) !void {
        if (row >= self.scales.len or row >= self.checksums.len or self.dims == 0 or
            row >= self.values.len / self.dims) return error.InvalidQuantizedDirectory;
        const values = self.values[row * self.dims ..][0..self.dims];
        try validateProjectionPayload(values, self.checksums[row], if (self.verification) |states| &states[row] else null);
    }

    pub fn validFor(self: @This(), count: usize, dims: usize) bool {
        const expected_values = std.math.mul(usize, count, dims) catch return false;
        return self.dims == dims and
            self.values.len == expected_values and
            self.scales.len == count and
            self.error_norms.len == count and
            self.decoded_norm_lower_bounds.len == count and
            (self.checksums.len == 0 or self.checksums.len == count) and
            (self.verification == null or self.verification.?.len == count) and
            (self.residual_locations == null or self.residual_locations.?.validFor(count));
    }
};

/// Memoization belongs to the immutable generation, never to an artifact ID
/// that can be reused by a later mutation. Concurrent first readers may both
/// hash a row; neither waits or holds a lock across mmap faults.
pub fn validateProjectionPayload(values: []const f16, checksum: u32, verification: ?*std.atomic.Value(u8)) !void {
    if (verification) |state| switch (state.load(.acquire)) {
        1 => return,
        2 => return error.QuantizedDirectoryChecksumMismatch,
        else => {},
    };
    const valid = @import("antfly_hash").Crc32.hash(std.mem.sliceAsBytes(values)) == checksum;
    if (verification) |state| state.store(if (valid) 1 else 2, .release);
    if (!valid) return error.QuantizedDirectoryChecksumMismatch;
}

pub const NativeResidualLocation = types.NativeResidualLocation;
pub const NativeResidualLocationPlane = types.NativeResidualLocationPlane;

/// One transient projection returned to an immutable checkpoint builder. The
/// callback owns the bytes only until it returns; the posting codec copies the
/// complete leaf plane before the next callback invocation.
pub const NativeProjectionBuildValue = struct {
    bytes: []const u8 = &.{},
    scale: f32 = 1,
    error_norm: f32 = 0,
    decoded_norm_lower_bound: f32 = 0,
    checksum: u32 = 0,
    residual_location: ?NativeResidualLocation = null,
};

pub const NativeProjectionBuildLoader = *const fn (
    ctx: *anyopaque,
    vector_ids: []const u64,
    metadata: []const ?[]const u8,
    values: []NativeProjectionBuildValue,
    payload_scratch: []u8,
    dims: usize,
    source_sequence: u64,
) anyerror!void;

pub const NativeProjectionBuildBegin = *const fn (ctx: *anyopaque, source_sequence: u64) anyerror!void;
pub const NativeProjectionBuildEnd = *const fn (ctx: *anyopaque) void;

pub const NativeProjectionBuildSource = struct {
    /// Training reads the shared projection, but need not duplicate that
    /// matrix in the posting generation.
    retain_projection_plane: bool = true,
    subgroup_count: u8 = 0,
    ctx: *anyopaque,
    loader: NativeProjectionBuildLoader,
    begin: ?NativeProjectionBuildBegin = null,
    end: ?NativeProjectionBuildEnd = null,
    /// Layout policy, independent of whether the source is available at this
    /// instant. Missing optional acceleration remains retryable serving debt.
    required: bool = false,
};

pub const WriteProfile = struct {
    bulk_build_store_ns: u64 = 0,
    bulk_build_tree_ns: u64 = 0,
    bulk_spool_partition_bytes: u64 = 0,
    bulk_spool_partition_read_bytes: u64 = 0,
    bulk_projection_commit_ns: u64 = 0,
    bulk_spool_compact_rows: u64 = 0,
    bulk_build_leaf_cluster_ns: u64 = 0,
    bulk_build_leaf_training_rows: usize = 0,
    bulk_build_leaf_training_ns: u64 = 0,
    bulk_build_leaf_route_ns: u64 = 0,
    bulk_build_leaf_count: usize = 0,
    bulk_build_leaf_min_members: usize = 0,
    bulk_build_leaf_max_members: usize = 0,
    kmeans_assignment_calls: u64 = 0,
    kmeans_assignment_cpu_calls: u64 = 0,
    kmeans_assignment_metal_calls: u64 = 0,
    kmeans_assignment_points_total: u64 = 0,
    kmeans_assignment_ns: u64 = 0,
    kmeans_assignment_cpu_ns: u64 = 0,
    kmeans_assignment_metal_ns: u64 = 0,
    kmeans_update_calls: u64 = 0,
    kmeans_update_cpu_calls: u64 = 0,
    kmeans_update_metal_calls: u64 = 0,
    kmeans_update_ns: u64 = 0,
    kmeans_update_cpu_ns: u64 = 0,
    kmeans_update_metal_ns: u64 = 0,
    insert_transform_ns: u64 = 0,
    insert_store_vector_ns: u64 = 0,
    insert_find_leaf_ns: u64 = 0,
    insert_find_leaf_calls: u64 = 0,
    centroid_delta_removals: u64 = 0,
    insert_mutate_leaf_ns: u64 = 0,
    insert_flush_metadata_ns: u64 = 0,
    insert_commit_ns: u64 = 0,
    save_node_ns: u64 = 0,
    refresh_quantized_ns: u64 = 0,
    quantized_vector_load_ns: u64 = 0,
    matrix_metadata_lookup_ns: u64 = 0,
    matrix_external_load_ns: u64 = 0,
    quantized_leaf_vector_load_ns: u64 = 0,
    quantized_internal_child_load_ns: u64 = 0,
    quantized_compute_ns: u64 = 0,
    quantized_store_ns: u64 = 0,
    quantized_encode_ns: u64 = 0,
    quantized_put_ns: u64 = 0,
    external_vector_cache_hits: u64 = 0,
    external_vector_cache_misses: u64 = 0,
    centroid_recompute_calls: u64 = 0,
    delete_reused_vector_rows: u64 = 0,
    delete_preserved_vector_rows: u64 = 0,
    delete_native_vector_rows: u64 = 0,
    centroid_recompute_members_total: u64 = 0,
    centroid_recompute_members_max: u64 = 0,
    save_split_range_ns: u64 = 0,
    update_parent_ns: u64 = 0,
    split_leaf_ns: u64 = 0,
    split_leaf_vector_load_ns: u64 = 0,
    split_leaf_partition_ns: u64 = 0,
    emeans_leaf_split_calls: u64 = 0,
    emeans_leaf_split_rebalances: u64 = 0,
    split_leaf_finalize_ns: u64 = 0,
    split_internal_ns: u64 = 0,
    insert_calls: u64 = 0,
    save_node_calls: u64 = 0,
    update_parent_calls: u64 = 0,
    split_leaf_calls: u64 = 0,
    split_internal_calls: u64 = 0,
    deferred_leaf_split_publish_windows: u64 = 0,
    deferred_leaf_split_steps: u64 = 0,
    deferred_leaf_split_window_max_steps: u64 = 0,
    grouped_leaf_groups: u64 = 0,
    grouped_items: u64 = 0,
    grouped_fallback_items: u64 = 0,
    noop_existing_skips: u64 = 0,
    grouped_split_candidates: u64 = 0,
    grouped_recursive_splits: u64 = 0,
    grouped_split_scan_iterations: u64 = 0,
    grouped_split_queue_peak_total: u64 = 0,
    grouped_leaf_range_writes: u64 = 0,
    grouped_ancestor_range_refreshes: u64 = 0,
    grouped_ancestor_range_nodes: u64 = 0,
    grouped_node_body_writes: u64 = 0,
    grouped_vec_leaf_writes: u64 = 0,
    batch_route_calls: u64 = 0,
    batch_route_internal_nodes: u64 = 0,
    batch_route_leaf_groups: u64 = 0,
    batch_route_items: u64 = 0,
    batch_route_quantized_nodes: u64 = 0,
    batch_route_exact_child_scores: u64 = 0,
    batch_route_fallback_nodes: u64 = 0,
    split_leaf_input_members_total: u64 = 0,
    split_leaf_input_overflow_members_total: u64 = 0,
    bulk_leaf_rebuild_calls: u64 = 0,
    bulk_leaf_rebuild_members_total: u64 = 0,
    bulk_leaf_rebuild_members_max: u64 = 0,
    ns_nodes_put_calls: u64 = 0,
    ns_nodes_append_calls: u64 = 0,
    ns_nodes_delete_calls: u64 = 0,
    ns_nodes_key_bytes: u64 = 0,
    ns_nodes_value_bytes: u64 = 0,
    ns_meta_put_calls: u64 = 0,
    ns_meta_append_calls: u64 = 0,
    ns_meta_delete_calls: u64 = 0,
    ns_meta_key_bytes: u64 = 0,
    ns_meta_value_bytes: u64 = 0,
    ns_quant_put_calls: u64 = 0,
    ns_quant_append_calls: u64 = 0,
    ns_quant_delete_calls: u64 = 0,
    ns_quant_key_bytes: u64 = 0,
    ns_quant_value_bytes: u64 = 0,
    ns_vecs_put_calls: u64 = 0,
    ns_vecs_append_calls: u64 = 0,
    ns_vecs_delete_calls: u64 = 0,
    ns_vecs_key_bytes: u64 = 0,
    ns_vecs_value_bytes: u64 = 0,
    posting_maintenance_scanned_nodes: u64 = 0,
    posting_maintenance_scanned_postings: u64 = 0,
    posting_maintenance_dirty_postings: u64 = 0,
    posting_maintenance_repaired_postings: u64 = 0,
    posting_maintenance_centroid_refreshed: u64 = 0,
    posting_maintenance_payload_refreshed: u64 = 0,
    posting_maintenance_ancestor_refresh_roots: u64 = 0,
    posting_maintenance_split_postings: u64 = 0,
    posting_maintenance_merged_postings: u64 = 0,
    posting_maintenance_boundary_reassigned_vectors: u64 = 0,
    posting_lazy_centroid_deferrals: u64 = 0,
    posting_lazy_payload_deferrals: u64 = 0,
    posting_lazy_ancestor_deferrals: u64 = 0,
    range_nodes_examined: u64 = 0,
    range_put_calls: u64 = 0,
    range_delete_calls: u64 = 0,
    range_key_bytes: u64 = 0,
    range_value_bytes: u64 = 0,
};

pub const BatchInsertItem = struct {
    vector_id: u64,
    vector: []const f32,
    transformed: ?[]const f32 = null,
    metadata: []const u8 = "",
};

pub const BatchVectorLookup = struct {
    ptr: *const anyopaque,
    getFn: *const fn (ptr: *const anyopaque, vector_id: u64) ?[]const f32,

    pub fn get(self: BatchVectorLookup, vector_id: u64) ?[]const f32 {
        return self.getFn(self.ptr, vector_id);
    }
};

pub const BatchInsertOptions = struct {
    /// Share one authoritative transformed leaf matrix between centroid and
    /// payload refresh on eager batch deletes. Does not defer either refresh.
    reuse_delete_vectors: bool = false,
    preserve_delete_rows: bool = false,
    defer_quantized_rebuild: bool = false,
    defer_quantized_rebuild_to_bulk_finish: bool = false,
    centroid_only_routing: bool = false,
    allow_quantized_routing: bool = false,
    assume_absent_ids: bool = false,
    coalesce_leaf_writes: bool = false,
    /// Membership edits may rebuild centroids from already-committed external
    /// updates. Such batches must finalize deferred updates from final members
    /// instead of adding old-to-new deltas to an already refreshed mean.
    recompute_coalesced_centroids: bool = false,
    skip_vector_store: bool = false,
    bulk_ingest: bool = false,
    defer_leaf_splits_to_batch_finish: bool = false,
    defer_leaf_splits_to_bulk_finish: bool = false,
    suppress_quantized_payload_persist: bool = false,
    bulk_rebuild_leaf_min_members: usize = 0,
    batch_vectors: ?BatchVectorLookup = null,
};

pub const SearchScratch = search_runtime.SearchScratch;

pub const ScratchHandle = struct {
    scratch: SearchScratch,
    from_cache: bool,
    accounted_bytes: u64 = 0,
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    @import("antfly_platform").sync.lockYielding(mutex);
}

fn nodeCacheValueType(comptime Self: type) type {
    return @FieldType(@FieldType(@typeInfo(Self).pointer.child, "node_cache").KV, "value");
}

fn quantizedCacheValueType(comptime Self: type) type {
    return @FieldType(@FieldType(@typeInfo(Self).pointer.child, "quantized_cache").KV, "value");
}

fn nodeCacheValuePtr(value_ptr: anytype) *types.Node {
    const Value = @TypeOf(value_ptr.*);
    return switch (@typeInfo(Value)) {
        .pointer => &value_ptr.*.node,
        else => value_ptr,
    };
}

fn quantizedCacheValuePtr(value_ptr: anytype) *QuantizedSet {
    const Value = @TypeOf(value_ptr.*);
    return switch (@typeInfo(Value)) {
        .pointer => &value_ptr.*.quantized,
        else => value_ptr,
    };
}

fn deinitNodeCacheValue(alloc: Allocator, value: anytype) void {
    const Value = @TypeOf(value);
    switch (@typeInfo(Value)) {
        .pointer => {
            var node = value.node;
            node.deinit(alloc);
            alloc.destroy(value);
        },
        else => {
            var node = value;
            node.deinit(alloc);
        },
    }
}

fn deinitQuantizedCacheValue(alloc: Allocator, value: anytype) void {
    const Value = @TypeOf(value);
    switch (@typeInfo(Value)) {
        .pointer => {
            var quantized = value.quantized;
            quantized.deinit(alloc);
            alloc.destroy(value);
        },
        else => {
            var quantized = value;
            quantized.deinit(alloc);
        },
    }
}

fn cloneNodeCacheValue(value_ptr: anytype, alloc: Allocator) !types.Node {
    return try nodeCacheValuePtr(value_ptr).clone(alloc);
}

fn cloneQuantizedCacheValue(value_ptr: anytype, alloc: Allocator) !QuantizedSet {
    return try quantizedCacheValuePtr(value_ptr).clone(alloc);
}

fn initNodeCacheValue(self: anytype, node: types.Node) !nodeCacheValueType(@TypeOf(self)) {
    const Value = comptime nodeCacheValueType(@TypeOf(self));
    return switch (@typeInfo(Value)) {
        .pointer => blk: {
            const entry = try self.alloc.create(@typeInfo(Value).pointer.child);
            entry.* = .{ .node = node };
            break :blk entry;
        },
        else => node,
    };
}

fn initQuantizedCacheValue(self: anytype, qs: QuantizedSet) !quantizedCacheValueType(@TypeOf(self)) {
    const Value = comptime quantizedCacheValueType(@TypeOf(self));
    return switch (@typeInfo(Value)) {
        .pointer => blk: {
            const entry = try self.alloc.create(@typeInfo(Value).pointer.child);
            entry.* = .{ .quantized = qs };
            break :blk entry;
        },
        else => qs,
    };
}

pub fn clearNodeCache(self: anytype) void {
    var it = self.node_cache.iterator();
    while (it.next()) |entry| deinitNodeCacheValue(self.alloc, entry.value_ptr.*);
    self.node_cache.deinit(self.alloc);
    self.node_cache = .empty;
    self.node_cache_slots.deinit(self.alloc);
    self.node_cache_slots = .empty;
    @memset(self.node_clock_keys, 0);
    @memset(self.node_clock_states, .empty);
    self.node_clock_hand = 0;
}

pub fn clearQuantizedCache(self: anytype) void {
    var it = self.quantized_cache.iterator();
    while (it.next()) |entry| deinitQuantizedCacheValue(self.alloc, entry.value_ptr.*);
    self.quantized_cache.deinit(self.alloc);
    self.quantized_cache = .empty;
    self.quantized_cache_slots.deinit(self.alloc);
    self.quantized_cache_slots = .empty;
    @memset(self.quantized_clock_keys, 0);
    @memset(self.quantized_clock_states, .empty);
    self.quantized_clock_hand = 0;
}

pub fn clearVectorCache(self: anytype) void {
    var it = self.vector_cache.iterator();
    while (it.next()) |entry| self.alloc.free(entry.value_ptr.*);
    self.vector_cache.deinit(self.alloc);
    self.vector_cache = .empty;
    self.vector_cache_slots.deinit(self.alloc);
    self.vector_cache_slots = .empty;
    @memset(self.vector_clock_keys, 0);
    @memset(self.vector_clock_states, .empty);
    self.vector_clock_hand = 0;
}

pub fn clearMetadataCache(self: anytype) void {
    var it = self.metadata_cache.iterator();
    while (it.next()) |entry| self.alloc.free(entry.value_ptr.*);
    self.metadata_cache.deinit(self.alloc);
    self.metadata_cache = .empty;
    self.metadata_cache_slots.deinit(self.alloc);
    self.metadata_cache_slots = .empty;
    @memset(self.metadata_clock_keys, 0);
    @memset(self.metadata_clock_states, .empty);
    self.metadata_clock_hand = 0;
}

pub fn invalidateNodeCache(self: anytype, node_id: u64) void {
    if (self.node_cache_slots.fetchRemove(node_id)) |removed_slot| {
        self.node_clock_keys[removed_slot.value] = 0;
        self.node_clock_states[removed_slot.value] = .empty;
    }
    if (self.node_cache.fetchRemove(node_id)) |removed| deinitNodeCacheValue(self.alloc, removed.value);
}

pub fn invalidateQuantizedCache(self: anytype, node_id: u64) void {
    if (self.quantized_cache_slots.fetchRemove(node_id)) |removed_slot| {
        self.quantized_clock_keys[removed_slot.value] = 0;
        self.quantized_clock_states[removed_slot.value] = .empty;
    }
    if (self.quantized_cache.fetchRemove(node_id)) |removed| deinitQuantizedCacheValue(self.alloc, removed.value);
}

pub fn invalidateVectorCache(self: anytype, vector_id: u64) void {
    if (self.vector_cache_slots.fetchRemove(vector_id)) |removed_slot| {
        self.vector_clock_keys[removed_slot.value] = 0;
        self.vector_clock_states[removed_slot.value] = .empty;
    }
    if (self.vector_cache.fetchRemove(vector_id)) |removed| self.alloc.free(removed.value);
}

pub fn invalidateMetadataCache(self: anytype, vector_id: u64) void {
    if (self.metadata_cache_slots.fetchRemove(vector_id)) |removed_slot| {
        self.metadata_clock_keys[removed_slot.value] = 0;
        self.metadata_clock_states[removed_slot.value] = .empty;
    }
    if (self.metadata_cache.fetchRemove(vector_id)) |removed| self.alloc.free(removed.value);
}

/// Shared local-cache clock. Occupancy is independent of the full u64 key,
/// and replacing a victim always rebinds the reserved slot before publication.
pub const Clock = struct {
    pub const State = enum(u8) { empty, referenced, cold };
    pub const NoProtection = struct {
        pub fn protects(_: @This(), _: u64) bool {
            return false;
        }
    };
    pub fn victim(keys: []const u64, states: []State, hand: *usize, protection: anytype) ?u64 {
        if (keys.len == 0) return null;
        var scanned: usize = 0;
        while (scanned < keys.len * 2) : (scanned += 1) {
            const slot = hand.*;
            const key = keys[slot];
            if (states[slot] != .empty and !protection.protects(key)) {
                if (states[slot] == .referenced) states[slot] = .cold else {
                    hand.* = (slot + 1) % keys.len;
                    return key;
                }
            }
            hand.* = (slot + 1) % keys.len;
        }
        return null;
    }
    pub fn claim(keys: []u64, states: []State, start: usize, key: u64, reserved: ?usize) ?usize {
        if (keys.len == 0) return null;
        const slot = reserved orelse find_empty: {
            for (0..keys.len) |offset| {
                const candidate = (start + offset) % keys.len;
                if (states[candidate] == .empty) break :find_empty candidate;
            }
            return null;
        };
        std.debug.assert(states[slot] == .empty);
        keys[slot] = key;
        states[slot] = .referenced;
        return slot;
    }
};

fn touchClock(states: []Clock.State, slot_map: anytype, key: u64) void {
    if (slot_map.get(key)) |slot| states[slot] = .referenced;
}

fn evictClockVictim(keys: []u64, states: []Clock.State, hand: *usize) ?u64 {
    return Clock.victim(keys, states, hand, Clock.NoProtection{});
}

fn ensureNodeCacheCapacity(self: anytype, key: u64) ?usize {
    if (self.config.max_cached_nodes == 0) return null;
    if (self.node_cache.contains(key)) return null;
    while (self.node_cache.count() >= self.config.max_cached_nodes) {
        const victim = evictClockVictim(self.node_clock_keys, self.node_clock_states, &self.node_clock_hand) orelse break;
        const slot = self.node_cache_slots.get(victim).?;
        invalidateNodeCache(self, victim);
        return slot;
    }
    return null;
}

fn ensureQuantizedCacheCapacity(self: anytype, key: u64) ?usize {
    if (self.config.max_cached_nodes == 0) return null;
    if (self.quantized_cache.contains(key)) return null;
    while (self.quantized_cache.count() >= self.config.max_cached_nodes) {
        const victim = evictClockVictim(self.quantized_clock_keys, self.quantized_clock_states, &self.quantized_clock_hand) orelse break;
        const slot = self.quantized_cache_slots.get(victim).?;
        invalidateQuantizedCache(self, victim);
        return slot;
    }
    return null;
}

fn ensureVectorCacheCapacity(self: anytype, key: u64) ?usize {
    if (self.config.max_cached_vectors == 0) return null;
    if (self.vector_cache.contains(key)) return null;
    while (self.vector_cache.count() >= self.config.max_cached_vectors) {
        const victim = evictClockVictim(self.vector_clock_keys, self.vector_clock_states, &self.vector_clock_hand) orelse break;
        const slot = self.vector_cache_slots.get(victim).?;
        invalidateVectorCache(self, victim);
        return slot;
    }
    return null;
}

fn ensureMetadataCacheCapacity(self: anytype, key: u64) ?usize {
    if (self.config.max_cached_metadata == 0) return null;
    if (self.metadata_cache.contains(key)) return null;
    while (self.metadata_cache.count() >= self.config.max_cached_metadata) {
        const victim = evictClockVictim(self.metadata_clock_keys, self.metadata_clock_states, &self.metadata_clock_hand) orelse break;
        const slot = self.metadata_cache_slots.get(victim).?;
        invalidateMetadataCache(self, victim);
        return slot;
    }
    return null;
}

pub fn getCachedNodeClone(self: anytype, node_id: u64) !?types.Node {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.node_cache.getPtr(node_id)) |cached| {
        touchClock(self.node_clock_states, self.node_cache_slots, node_id);
        return try cloneNodeCacheValue(cached, self.alloc);
    }
    return null;
}

pub fn getCachedQuantizedClone(self: anytype, node_id: u64) !?QuantizedSet {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.quantized_cache.getPtr(node_id)) |cached| {
        touchClock(self.quantized_clock_states, self.quantized_cache_slots, node_id);
        return try cloneQuantizedCacheValue(cached, self.alloc);
    }
    return null;
}

pub fn getCachedMetadata(self: anytype, vector_id: u64) ?[]const u8 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.metadata_cache.get(vector_id)) |cached| {
        touchClock(self.metadata_clock_states, self.metadata_cache_slots, vector_id);
        return cached;
    }
    return null;
}

pub fn cacheNode(self: anytype, node: *const types.Node) !void {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.config.max_cached_nodes == 0) return;
    try self.node_cache.ensureUnusedCapacity(self.alloc, 1);
    try self.node_cache_slots.ensureUnusedCapacity(self.alloc, 1);
    var cloned = try node.clone(self.alloc);
    var owns_clone = true;
    errdefer if (owns_clone) cloned.deinit(self.alloc);
    const value = try initNodeCacheValue(self, cloned);
    owns_clone = false;
    errdefer deinitNodeCacheValue(self.alloc, value);
    const reserved = ensureNodeCacheCapacity(self, node.id);
    invalidateNodeCache(self, node.id);
    const slot = Clock.claim(self.node_clock_keys, self.node_clock_states, self.node_clock_hand, node.id, reserved) orelse return error.CacheDisabled;
    self.node_cache.putAssumeCapacity(node.id, value);
    self.node_cache_slots.putAssumeCapacity(node.id, slot);
}

pub fn cacheQuantized(self: anytype, node_id: u64, qs: *const QuantizedSet) !void {
    if (self.config.max_cached_nodes == 0) return;
    // The owned helper consumes its argument on success and failure.
    try cacheQuantizedOwned(self, node_id, try qs.clone(self.alloc));
}

pub fn cacheQuantizedOwned(self: anytype, node_id: u64, qs: QuantizedSet) !void {
    var owned = qs;
    var owns_payload = true;
    errdefer if (owns_payload) owned.deinit(self.alloc);
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.config.max_cached_nodes == 0) return error.CacheDisabled;
    try self.quantized_cache.ensureUnusedCapacity(self.alloc, 1);
    try self.quantized_cache_slots.ensureUnusedCapacity(self.alloc, 1);
    const value = try initQuantizedCacheValue(self, owned);
    owns_payload = false;
    errdefer deinitQuantizedCacheValue(self.alloc, value);
    const reserved = ensureQuantizedCacheCapacity(self, node_id);
    invalidateQuantizedCache(self, node_id);
    const slot = Clock.claim(self.quantized_clock_keys, self.quantized_clock_states, self.quantized_clock_hand, node_id, reserved) orelse return error.CacheDisabled;
    self.quantized_cache.putAssumeCapacity(node_id, value);
    self.quantized_cache_slots.putAssumeCapacity(node_id, slot);
}

pub fn cacheVector(self: anytype, vector_id: u64, vector_data: []const f32) ![]const f32 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.active_searches.load(.acquire) > 1 or self.config.max_cached_vectors == 0) return vector_data;
    try self.vector_cache.ensureUnusedCapacity(self.alloc, 1);
    try self.vector_cache_slots.ensureUnusedCapacity(self.alloc, 1);
    const copied = try self.alloc.dupe(f32, vector_data);
    errdefer self.alloc.free(copied);
    const reserved = ensureVectorCacheCapacity(self, vector_id);
    invalidateVectorCache(self, vector_id);
    const slot = Clock.claim(self.vector_clock_keys, self.vector_clock_states, self.vector_clock_hand, vector_id, reserved) orelse return error.CacheDisabled;
    self.vector_cache.putAssumeCapacity(vector_id, copied);
    self.vector_cache_slots.putAssumeCapacity(vector_id, slot);
    return copied;
}

pub fn cacheMetadata(self: anytype, vector_id: u64, metadata: []const u8) ![]const u8 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.active_searches.load(.acquire) > 1 or self.config.max_cached_metadata == 0) return metadata;
    try self.metadata_cache.ensureUnusedCapacity(self.alloc, 1);
    try self.metadata_cache_slots.ensureUnusedCapacity(self.alloc, 1);
    const copied = try self.alloc.dupe(u8, metadata);
    errdefer self.alloc.free(copied);
    const reserved = ensureMetadataCacheCapacity(self, vector_id);
    invalidateMetadataCache(self, vector_id);
    const slot = Clock.claim(self.metadata_clock_keys, self.metadata_clock_states, self.metadata_clock_hand, vector_id, reserved) orelse return error.CacheDisabled;
    self.metadata_cache.putAssumeCapacity(vector_id, copied);
    self.metadata_cache_slots.putAssumeCapacity(vector_id, slot);
    // Callers retain their transaction/request view, not an unleased cache view.
    return metadata;
}

pub fn acquireSearchScratch(self: anytype) !ScratchHandle {
    lockAtomic(&self.scratch_mu);
    if (self.cached_scratch) |scratch| {
        self.cached_scratch = null;
        self.scratch_mu.unlock();
        return .{ .scratch = scratch, .from_cache = true, .accounted_bytes = scratch.bytes() };
    }
    const Index = @TypeOf(self.*);
    if (comptime @hasField(Index, "cached_search_scratches")) {
        if (self.cached_search_scratches.pop()) |scratch| {
            self.scratch_mu.unlock();
            return .{ .scratch = scratch, .from_cache = true, .accounted_bytes = scratch.bytes() };
        }
    }
    self.scratch_mu.unlock();

    const dims: usize = @intCast(self.metadata.dims);
    const branching_factor: usize = @intCast(self.metadata.branching_factor);
    const leaf_size: usize = @intCast(self.metadata.leaf_size);
    const initial_bytes = try SearchScratch.initialBytes(dims, branching_factor, leaf_size);
    const pre_admitted = comptime @hasDecl(Index, "admitNewSearchScratchBytes") and
        @hasDecl(Index, "releaseSearchScratchBytes");
    if (pre_admitted) try self.admitNewSearchScratchBytes(initial_bytes);
    errdefer if (pre_admitted) self.releaseSearchScratchBytes(initial_bytes);
    var scratch = try SearchScratch.init(
        self.alloc,
        dims,
        branching_factor,
        leaf_size,
    );
    errdefer scratch.deinit(self.alloc);
    std.debug.assert(scratch.bytes() == initial_bytes);
    if (!pre_admitted and comptime @hasDecl(Index, "observeSearchWorkspaceBytes")) {
        self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted + scratch.bytes());
    }
    return .{
        .scratch = scratch,
        .from_cache = false,
        .accounted_bytes = scratch.bytes(),
    };
}

pub fn refreshSearchScratchAccounting(self: anytype, handle: *ScratchHandle) void {
    const next = handle.scratch.bytes();
    if (next == handle.accounted_bytes) return;
    if (comptime @hasDecl(@TypeOf(self.*), "reconcileSearchScratchBytes")) {
        self.reconcileSearchScratchBytes(handle, next);
        return;
    }
    if (comptime @hasDecl(@TypeOf(self.*), "observeSearchWorkspaceBytes")) {
        if (next > handle.accounted_bytes) {
            self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted + (next - handle.accounted_bytes));
        } else if (next < handle.accounted_bytes) {
            self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted -| (handle.accounted_bytes - next));
        }
        handle.accounted_bytes = next;
    }
}

pub fn beginSearchEpoch(self: anytype) void {
    _ = self.active_searches.fetchAdd(1, .acq_rel);
}

pub fn endSearchEpoch(self: anytype) void {
    _ = self.active_searches.fetchSub(1, .acq_rel);
}

pub fn releaseSearchScratch(self: anytype, handle: *ScratchHandle) void {
    lockAtomic(&self.scratch_mu);
    if (self.cached_scratch == null) {
        self.cached_scratch = handle.scratch;
        self.scratch_mu.unlock();
        return;
    }
    const Index = @TypeOf(self.*);
    if (comptime @hasField(Index, "cached_search_scratches") and @hasDecl(Index, "searchScratchCacheLimit")) {
        const cached_count = self.cached_search_scratches.items.len + 1;
        if (cached_count < self.searchScratchCacheLimit()) {
            self.cached_search_scratches.append(self.alloc, handle.scratch) catch {
                self.scratch_mu.unlock();
                deinitReleasedSearchScratch(self, handle);
                return;
            };
            self.scratch_mu.unlock();
            return;
        }
    }
    self.scratch_mu.unlock();
    deinitReleasedSearchScratch(self, handle);
}

fn deinitReleasedSearchScratch(self: anytype, handle: *ScratchHandle) void {
    var scratch = handle.scratch;
    const Index = @TypeOf(self.*);
    if (comptime @hasDecl(Index, "releaseSearchScratchBytes")) {
        self.releaseSearchScratchBytes(handle.accounted_bytes);
    } else if (comptime @hasDecl(Index, "observeSearchWorkspaceBytes")) {
        self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted -| handle.accounted_bytes);
    }
    scratch.deinit(self.alloc);
}

pub fn transformVector(self: anytype, original: []const f32, transformed: []f32) []const f32 {
    _ = self.rot.transform(original, transformed);
    if (self.config.metric == .cosine) {
        _ = vec.normalize(transformed);
    }
    return transformed;
}

pub fn nextNodeId(self: anytype) u64 {
    self.metadata.node_count += 1;
    return self.metadata.node_count;
}

test "cache clock rebinds evicted slots and tracks zero keys" {
    var keys: [2]u64 = @splat(0);
    var states: [2]Clock.State = @splat(.empty);
    var hand: usize = 0;
    var slots = std.AutoHashMap(u64, usize).init(std.testing.allocator);
    defer slots.deinit();
    for (0..128) |i| {
        const key: u64 = if (i % 7 == 0) 0 else i;
        var reserved: ?usize = null;
        if (slots.fetchRemove(key)) |old| {
            states[old.value] = .empty;
            reserved = old.value;
        }
        if (reserved == null and slots.count() == 2) {
            const victim = Clock.victim(&keys, &states, &hand, Clock.NoProtection{}).?;
            reserved = slots.fetchRemove(victim).?.value;
            states[reserved.?] = .empty;
        }
        const slot = Clock.claim(&keys, &states, hand, key, reserved).?;
        try slots.put(key, slot);
        try std.testing.expect(slots.count() <= 2);
        for (states, 0..) |state, n| if (state != .empty) {
            try std.testing.expectEqual(@as(?usize, n), slots.get(keys[n]));
        };
    }
    const ProtectZero = struct {
        pub fn protects(_: @This(), key: u64) bool {
            return key == 0;
        }
    };
    const victim = Clock.victim(&keys, &states, &hand, ProtectZero{}).?;
    try std.testing.expect(victim != 0);
}

test "generic cache ownership remains valid at every allocation failure" {
    const Check = struct {
        const Lock = struct {
            pub fn lockExclusive(_: *@This()) void {}
            pub fn unlockExclusive(_: *@This()) void {}
        };
        const NodeEntry = struct { node: types.Node };
        const QuantizedEntry = struct { quantized: QuantizedSet };
        const Cache = struct {
            alloc: Allocator,
            cache_mu: Lock = .{},
            active_searches: std.atomic.Value(usize) = .init(0),
            config: struct { max_cached_nodes: usize = 2, max_cached_vectors: usize = 2, max_cached_metadata: usize = 2 } = .{},
            node_cache: std.AutoHashMapUnmanaged(u64, *NodeEntry) = .empty,
            node_cache_slots: std.AutoHashMapUnmanaged(u64, usize) = .empty,
            node_clock_keys: []u64,
            node_clock_states: []Clock.State,
            node_clock_hand: usize = 0,
            quantized_cache: std.AutoHashMapUnmanaged(u64, *QuantizedEntry) = .empty,
            quantized_cache_slots: std.AutoHashMapUnmanaged(u64, usize) = .empty,
            quantized_clock_keys: []u64,
            quantized_clock_states: []Clock.State,
            quantized_clock_hand: usize = 0,
            vector_cache: std.AutoHashMapUnmanaged(u64, []f32) = .empty,
            vector_cache_slots: std.AutoHashMapUnmanaged(u64, usize) = .empty,
            vector_clock_keys: []u64,
            vector_clock_states: []Clock.State,
            vector_clock_hand: usize = 0,
            metadata_cache: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
            metadata_cache_slots: std.AutoHashMapUnmanaged(u64, usize) = .empty,
            metadata_clock_keys: []u64,
            metadata_clock_states: []Clock.State,
            metadata_clock_hand: usize = 0,
        };
        fn run(a: Allocator) !void {
            var keys: [4][2]u64 = @splat(@splat(0));
            var states: [4][2]Clock.State = @splat(@splat(.empty));
            var cache: Cache = .{ .alloc = a, .node_clock_keys = &keys[0], .node_clock_states = &states[0], .quantized_clock_keys = &keys[1], .quantized_clock_states = &states[1], .vector_clock_keys = &keys[2], .vector_clock_states = &states[2], .metadata_clock_keys = &keys[3], .metadata_clock_states = &states[3] };
            defer clearNodeCache(&cache);
            defer clearQuantizedCache(&cache);
            defer clearVectorCache(&cache);
            defer clearMetadataCache(&cache);
            var coordinates = [_]f32{ 1, 2, 3, 4 };
            for (0..5) |id| {
                var node: types.Node = .{ .id = id, .is_leaf = true, .level = 0, .parent = 0, .centroid = coordinates[0..2], .children = &.{}, .members = &.{} };
                try cacheNode(&cache, &node);
                var quantized: QuantizedSet = .{ .nonquant = .{ .vectors = .{ .dims = 2, .count = 2, .data = &coordinates } } };
                try cacheQuantized(&cache, id, &quantized);
                const owned: QuantizedSet = .{ .nonquant = .{ .vectors = .{ .dims = 2, .count = 2, .data = try a.dupe(f32, &coordinates) } } };
                try cacheQuantizedOwned(&cache, id, owned);
                _ = try cacheVector(&cache, id, coordinates[0..2]);
                _ = try cacheMetadata(&cache, id, "metadata");
                try std.testing.expect(cache.node_cache.count() <= 2 and cache.quantized_cache.count() <= 2 and cache.vector_cache.count() <= 2 and cache.metadata_cache.count() <= 2);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
