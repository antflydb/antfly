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

    pub fn deinit(self: *QuantizedSet, alloc: Allocator) void {
        switch (self.*) {
            .rabit => |*set| set.deinit(alloc),
            .nonquant => |*set| set.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const WriteProfile = struct {
    bulk_build_store_ns: u64 = 0,
    bulk_build_tree_ns: u64 = 0,
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
    insert_mutate_leaf_ns: u64 = 0,
    insert_flush_metadata_ns: u64 = 0,
    insert_commit_ns: u64 = 0,
    save_node_ns: u64 = 0,
    refresh_quantized_ns: u64 = 0,
    quantized_vector_load_ns: u64 = 0,
    quantized_leaf_vector_load_ns: u64 = 0,
    quantized_internal_child_load_ns: u64 = 0,
    quantized_compute_ns: u64 = 0,
    quantized_store_ns: u64 = 0,
    quantized_encode_ns: u64 = 0,
    quantized_put_ns: u64 = 0,
    external_vector_cache_hits: u64 = 0,
    external_vector_cache_misses: u64 = 0,
    centroid_recompute_calls: u64 = 0,
    centroid_recompute_members_total: u64 = 0,
    centroid_recompute_members_max: u64 = 0,
    save_split_range_ns: u64 = 0,
    update_parent_ns: u64 = 0,
    split_leaf_ns: u64 = 0,
    split_leaf_vector_load_ns: u64 = 0,
    split_leaf_partition_ns: u64 = 0,
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
    defer_quantized_rebuild: bool = false,
    defer_quantized_rebuild_to_bulk_finish: bool = false,
    centroid_only_routing: bool = false,
    allow_quantized_routing: bool = false,
    assume_absent_ids: bool = false,
    coalesce_leaf_writes: bool = false,
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
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
        }
    }
}

fn nodeCacheValueType(self: anytype) type {
    return @FieldType(@TypeOf(self.node_cache).KV, "value");
}

fn quantizedCacheValueType(self: anytype) type {
    return @FieldType(@TypeOf(self.quantized_cache).KV, "value");
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

fn initNodeCacheValue(self: anytype, node: types.Node) !nodeCacheValueType(self) {
    const Value = comptime nodeCacheValueType(self);
    return switch (@typeInfo(Value)) {
        .pointer => blk: {
            const entry = try self.alloc.create(@typeInfo(Value).pointer.child);
            entry.* = .{ .node = node };
            break :blk entry;
        },
        else => node,
    };
}

fn initQuantizedCacheValue(self: anytype, qs: QuantizedSet) !quantizedCacheValueType(self) {
    const Value = comptime quantizedCacheValueType(self);
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
    @memset(self.node_clock_refs, false);
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
    @memset(self.quantized_clock_refs, false);
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
    @memset(self.vector_clock_refs, false);
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
    @memset(self.metadata_clock_refs, false);
    self.metadata_clock_hand = 0;
}

pub fn invalidateNodeCache(self: anytype, node_id: u64) void {
    if (self.node_cache_slots.fetchRemove(node_id)) |removed_slot| {
        self.node_clock_keys[removed_slot.value] = 0;
        self.node_clock_refs[removed_slot.value] = false;
    }
    if (self.node_cache.fetchRemove(node_id)) |removed| deinitNodeCacheValue(self.alloc, removed.value);
}

pub fn invalidateQuantizedCache(self: anytype, node_id: u64) void {
    if (self.quantized_cache_slots.fetchRemove(node_id)) |removed_slot| {
        self.quantized_clock_keys[removed_slot.value] = 0;
        self.quantized_clock_refs[removed_slot.value] = false;
    }
    if (self.quantized_cache.fetchRemove(node_id)) |removed| deinitQuantizedCacheValue(self.alloc, removed.value);
}

pub fn invalidateVectorCache(self: anytype, vector_id: u64) void {
    if (self.vector_cache_slots.fetchRemove(vector_id)) |removed_slot| {
        self.vector_clock_keys[removed_slot.value] = 0;
        self.vector_clock_refs[removed_slot.value] = false;
    }
    if (self.vector_cache.fetchRemove(vector_id)) |removed| self.alloc.free(removed.value);
}

pub fn invalidateMetadataCache(self: anytype, vector_id: u64) void {
    if (self.metadata_cache_slots.fetchRemove(vector_id)) |removed_slot| {
        self.metadata_clock_keys[removed_slot.value] = 0;
        self.metadata_clock_refs[removed_slot.value] = false;
    }
    if (self.metadata_cache.fetchRemove(vector_id)) |removed| self.alloc.free(removed.value);
}

fn touchClock(refs: []bool, slot_map: anytype, key: u64) void {
    if (slot_map.get(key)) |slot| refs[slot] = true;
}

fn claimClockSlot(clock_keys: []u64, start_slot: usize, key: u64) ?usize {
    if (clock_keys.len == 0) return null;
    for (0..clock_keys.len) |offset| {
        const slot = (start_slot + offset) % clock_keys.len;
        const slot_key = clock_keys[slot];
        if (slot_key == 0) {
            clock_keys[slot] = key;
            return slot;
        }
    }
    return null;
}

fn evictClockVictim(
    clock_keys: []u64,
    clock_refs: []bool,
    hand: *usize,
) ?u64 {
    if (clock_keys.len == 0) return null;
    var scanned: usize = 0;
    const limit = clock_keys.len * 2;
    while (scanned < limit) : (scanned += 1) {
        const slot = hand.*;
        const key = clock_keys[slot];
        if (key != 0) {
            if (clock_refs[slot]) {
                clock_refs[slot] = false;
            } else {
                hand.* = (slot + 1) % clock_keys.len;
                return key;
            }
        }
        hand.* = (slot + 1) % clock_keys.len;
    }
    return null;
}

fn ensureNodeCacheCapacity(self: anytype, key: u64) ?usize {
    if (self.config.max_cached_nodes == 0) return null;
    if (self.node_cache.contains(key)) return null;
    while (self.node_cache.count() >= self.config.max_cached_nodes) {
        const victim = evictClockVictim(self.node_clock_keys, self.node_clock_refs, &self.node_clock_hand) orelse break;
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
        const victim = evictClockVictim(self.quantized_clock_keys, self.quantized_clock_refs, &self.quantized_clock_hand) orelse break;
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
        const victim = evictClockVictim(self.vector_clock_keys, self.vector_clock_refs, &self.vector_clock_hand) orelse break;
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
        const victim = evictClockVictim(self.metadata_clock_keys, self.metadata_clock_refs, &self.metadata_clock_hand) orelse break;
        const slot = self.metadata_cache_slots.get(victim).?;
        invalidateMetadataCache(self, victim);
        return slot;
    }
    return null;
}

pub fn getCachedNodePtr(self: anytype, node_id: u64) ?*const types.Node {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.node_cache.getPtr(node_id)) |cached| {
        touchClock(self.node_clock_refs, self.node_cache_slots, node_id);
        return nodeCacheValuePtr(cached);
    }
    return null;
}

pub fn getCachedNodeClone(self: anytype, node_id: u64) !?types.Node {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.node_cache.getPtr(node_id)) |cached| {
        touchClock(self.node_clock_refs, self.node_cache_slots, node_id);
        return try cloneNodeCacheValue(cached, self.alloc);
    }
    return null;
}

pub fn getCachedQuantizedPtr(self: anytype, node_id: u64) ?*QuantizedSet {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.quantized_cache.getPtr(node_id)) |cached| {
        touchClock(self.quantized_clock_refs, self.quantized_cache_slots, node_id);
        return quantizedCacheValuePtr(cached);
    }
    return null;
}

pub fn getCachedQuantizedClone(self: anytype, node_id: u64) !?QuantizedSet {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.quantized_cache.getPtr(node_id)) |cached| {
        touchClock(self.quantized_clock_refs, self.quantized_cache_slots, node_id);
        return try cloneQuantizedCacheValue(cached, self.alloc);
    }
    return null;
}

pub fn getCachedVector(self: anytype, vector_id: u64) ?[]const f32 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.vector_cache.get(vector_id)) |cached| {
        touchClock(self.vector_clock_refs, self.vector_cache_slots, vector_id);
        return cached;
    }
    return null;
}

pub fn getCachedMetadata(self: anytype, vector_id: u64) ?[]const u8 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.metadata_cache.get(vector_id)) |cached| {
        touchClock(self.metadata_clock_refs, self.metadata_cache_slots, vector_id);
        return cached;
    }
    return null;
}

pub fn cacheNode(self: anytype, node: *const types.Node) !void {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.active_searches.load(.acquire) > 1) return;
    if (self.config.max_cached_nodes == 0) return;
    const reserved_slot = ensureNodeCacheCapacity(self, node.id);
    invalidateNodeCache(self, node.id);
    const cached_value = try initNodeCacheValue(self, try node.clone(self.alloc));
    errdefer deinitNodeCacheValue(self.alloc, cached_value);
    try self.node_cache.put(self.alloc, node.id, cached_value);
    const slot = reserved_slot orelse claimClockSlot(self.node_clock_keys, self.node_clock_hand, node.id) orelse return error.CacheDisabled;
    self.node_clock_refs[slot] = true;
    try self.node_cache_slots.put(self.alloc, node.id, slot);
}

pub fn cacheNodeOwned(self: anytype, node: types.Node) !*const types.Node {
    var owned = node;
    errdefer owned.deinit(self.alloc);
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.config.max_cached_nodes == 0) return error.CacheDisabled;
    const reserved_slot = ensureNodeCacheCapacity(self, owned.id);
    invalidateNodeCache(self, owned.id);
    const cached_value = try initNodeCacheValue(self, owned);
    errdefer deinitNodeCacheValue(self.alloc, cached_value);
    try self.node_cache.put(self.alloc, owned.id, cached_value);
    const slot = reserved_slot orelse claimClockSlot(self.node_clock_keys, self.node_clock_hand, owned.id) orelse return error.CacheDisabled;
    self.node_clock_refs[slot] = true;
    try self.node_cache_slots.put(self.alloc, owned.id, slot);
    return nodeCacheValuePtr(self.node_cache.getPtr(owned.id).?);
}

pub fn cacheQuantized(self: anytype, node_id: u64, qs: *const QuantizedSet) !void {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.active_searches.load(.acquire) > 1) return;
    if (self.config.max_cached_nodes == 0) return;
    const reserved_slot = ensureQuantizedCacheCapacity(self, node_id);
    invalidateQuantizedCache(self, node_id);
    const cached_value = try initQuantizedCacheValue(self, try qs.clone(self.alloc));
    errdefer deinitQuantizedCacheValue(self.alloc, cached_value);
    try self.quantized_cache.put(self.alloc, node_id, cached_value);
    const slot = reserved_slot orelse claimClockSlot(self.quantized_clock_keys, self.quantized_clock_hand, node_id) orelse return error.CacheDisabled;
    self.quantized_clock_refs[slot] = true;
    try self.quantized_cache_slots.put(self.alloc, node_id, slot);
}

pub fn cacheQuantizedOwned(self: anytype, node_id: u64, qs: QuantizedSet) !*const QuantizedSet {
    var owned = qs;
    errdefer owned.deinit(self.alloc);
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.config.max_cached_nodes == 0) return error.CacheDisabled;
    const reserved_slot = ensureQuantizedCacheCapacity(self, node_id);
    invalidateQuantizedCache(self, node_id);
    const cached_value = try initQuantizedCacheValue(self, owned);
    errdefer deinitQuantizedCacheValue(self.alloc, cached_value);
    try self.quantized_cache.put(self.alloc, node_id, cached_value);
    const slot = reserved_slot orelse claimClockSlot(self.quantized_clock_keys, self.quantized_clock_hand, node_id) orelse return error.CacheDisabled;
    self.quantized_clock_refs[slot] = true;
    try self.quantized_cache_slots.put(self.alloc, node_id, slot);
    return quantizedCacheValuePtr(self.quantized_cache.getPtr(node_id).?);
}

pub fn cacheVector(self: anytype, vector_id: u64, vector_data: []const f32) ![]const f32 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.active_searches.load(.acquire) > 1) return vector_data;
    if (self.config.max_cached_vectors == 0) return vector_data;
    const reserved_slot = ensureVectorCacheCapacity(self, vector_id);
    invalidateVectorCache(self, vector_id);
    try self.vector_cache.put(self.alloc, vector_id, try self.alloc.dupe(f32, vector_data));
    const slot = reserved_slot orelse claimClockSlot(self.vector_clock_keys, self.vector_clock_hand, vector_id) orelse return error.CacheDisabled;
    self.vector_clock_refs[slot] = true;
    try self.vector_cache_slots.put(self.alloc, vector_id, slot);
    return self.vector_cache.get(vector_id).?;
}

pub fn cacheMetadata(self: anytype, vector_id: u64, metadata: []const u8) ![]const u8 {
    self.cache_mu.lockExclusive();
    defer self.cache_mu.unlockExclusive();
    if (self.active_searches.load(.acquire) > 1) return metadata;
    if (self.config.max_cached_metadata == 0) return metadata;
    const reserved_slot = ensureMetadataCacheCapacity(self, vector_id);
    invalidateMetadataCache(self, vector_id);
    try self.metadata_cache.put(self.alloc, vector_id, try self.alloc.dupe(u8, metadata));
    const slot = reserved_slot orelse claimClockSlot(self.metadata_clock_keys, self.metadata_clock_hand, vector_id) orelse return error.CacheDisabled;
    self.metadata_clock_refs[slot] = true;
    try self.metadata_cache_slots.put(self.alloc, vector_id, slot);
    return self.metadata_cache.get(vector_id).?;
}

pub fn acquireSearchScratch(self: anytype) !ScratchHandle {
    lockAtomic(&self.scratch_mu);
    defer self.scratch_mu.unlock();
    if (self.cached_scratch) |scratch| {
        self.cached_scratch = null;
        return .{ .scratch = scratch, .from_cache = true, .accounted_bytes = scratch.bytes() };
    }
    const scratch = try SearchScratch.init(
        self.alloc,
        @intCast(self.metadata.dims),
        @intCast(self.metadata.branching_factor),
        @intCast(self.metadata.leaf_size),
    );
    if (comptime @hasDecl(@TypeOf(self.*), "observeSearchWorkspaceBytes")) {
        self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted + scratch.bytes());
    }
    return .{
        .scratch = scratch,
        .from_cache = false,
        .accounted_bytes = scratch.bytes(),
    };
}

pub fn refreshSearchScratchAccounting(self: anytype, handle: *ScratchHandle) void {
    if (comptime @hasDecl(@TypeOf(self.*), "observeSearchWorkspaceBytes")) {
        const next = handle.scratch.bytes();
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
    defer self.scratch_mu.unlock();
    if (self.cached_scratch == null) {
        self.cached_scratch = handle.scratch;
    } else {
        var scratch = handle.scratch;
        if (comptime @hasDecl(@TypeOf(self.*), "observeSearchWorkspaceBytes")) {
            self.observeSearchWorkspaceBytes(self.search_workspace_bytes_accounted -| handle.accounted_bytes);
        }
        scratch.deinit(self.alloc);
    }
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
