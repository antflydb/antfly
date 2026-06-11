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
const vec = @import("antfly_vector").vector;
const quantizer = @import("antfly_vector").quantizer;
const types = @import("types.zig");
const search_types = @import("search_types.zig");

pub const RerankLookup = struct {
    item_index: usize,
    vector_id: u64,
    key: [10]u8,
};

pub const PostingMemberCacheEntry = struct {
    posting_id: u64,
    mutation_version: u64,
    members: []u64,
};

pub const PostingMemberCacheResult = struct {
    inserted: bool = false,
    admission_skips: u64 = 0,
    evictions: u64 = 0,
    member_bytes: u64 = 0,
};

pub const SearchScratch = struct {
    dims: usize,
    estimate: quantizer.RaBitQuantizer.EstimateScratch,
    transformed_query: []f32,
    centroid: []f32,
    vector: []f32,
    vector_batch: []f32,
    member_ids: []u64,
    vector_ids: []u64,
    metadata: []?[]const u8,
    flags: []bool,
    positions: []usize,
    lookups: []RerankLookup,
    key_views: [][]const u8,
    values: []?[]const u8,
    vector_views: [][]const f32,
    distances: []f32,
    error_bounds: []f32,
    posting_member_cache: std.ArrayListUnmanaged(PostingMemberCacheEntry) = .empty,
    posting_member_cache_slots: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    posting_member_cache_bytes: u64 = 0,
    max_posting_member_cache_bytes: u64 = 0,
    max_posting_member_cache_entry_bytes: u64 = 0,
    posting_member_cache_admission_enabled: bool = true,
    posting_overlay_removed_members: std.AutoHashMapUnmanaged(u64, void) = .empty,
    posting_overlay_appended_positions: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    posting_overlay_appended_ids: []u64 = &.{},
    posting_overlay_appended_live: []bool = &.{},
    posting_overlay_appended_count: usize = 0,

    pub fn init(
        alloc: Allocator,
        dims: usize,
        max_branching: usize,
        max_leaf: usize,
        max_posting_member_cache_bytes: u64,
        max_posting_member_cache_entry_bytes: u64,
    ) !SearchScratch {
        const max_candidates = @max(max_branching, max_leaf);
        const estimate = try quantizer.RaBitQuantizer.EstimateScratch.init(alloc, dims);
        errdefer {
            var tmp = estimate;
            tmp.deinit(alloc);
        }
        const transformed_query = try alloc.alloc(f32, dims);
        errdefer alloc.free(transformed_query);
        const centroid = try alloc.alloc(f32, dims);
        errdefer alloc.free(centroid);
        const vector = try alloc.alloc(f32, dims);
        errdefer alloc.free(vector);
        const vector_batch = try alloc.alloc(f32, dims * max_candidates);
        errdefer alloc.free(vector_batch);
        const member_ids = try alloc.alloc(u64, max_leaf);
        errdefer alloc.free(member_ids);
        const vector_ids = try alloc.alloc(u64, max_candidates);
        errdefer alloc.free(vector_ids);
        const metadata = try alloc.alloc(?[]const u8, max_candidates);
        errdefer alloc.free(metadata);
        const flags = try alloc.alloc(bool, max_candidates);
        errdefer alloc.free(flags);
        const positions = try alloc.alloc(usize, max_candidates);
        errdefer alloc.free(positions);
        const lookups = try alloc.alloc(RerankLookup, max_candidates);
        errdefer alloc.free(lookups);
        const key_views = try alloc.alloc([]const u8, max_candidates);
        errdefer alloc.free(key_views);
        const values = try alloc.alloc(?[]const u8, max_candidates);
        errdefer alloc.free(values);
        const vector_views = try alloc.alloc([]const f32, max_candidates);
        errdefer alloc.free(vector_views);
        const distances = try alloc.alloc(f32, max_candidates);
        errdefer alloc.free(distances);
        const error_bounds = try alloc.alloc(f32, max_candidates);
        return .{
            .dims = dims,
            .estimate = estimate,
            .transformed_query = transformed_query,
            .centroid = centroid,
            .vector = vector,
            .vector_batch = vector_batch,
            .member_ids = member_ids,
            .vector_ids = vector_ids,
            .metadata = metadata,
            .flags = flags,
            .positions = positions,
            .lookups = lookups,
            .key_views = key_views,
            .values = values,
            .vector_views = vector_views,
            .distances = distances,
            .error_bounds = error_bounds,
            .max_posting_member_cache_bytes = max_posting_member_cache_bytes,
            .max_posting_member_cache_entry_bytes = effectivePostingMemberCacheEntryBytes(
                max_posting_member_cache_bytes,
                max_posting_member_cache_entry_bytes,
            ),
        };
    }

    pub fn ensureVectorFetchCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.positions.len < needed) self.positions = try alloc.realloc(self.positions, needed);
        if (self.vector_ids.len < needed) self.vector_ids = try alloc.realloc(self.vector_ids, needed);
        if (self.metadata.len < needed) self.metadata = try alloc.realloc(self.metadata, needed);
        if (self.lookups.len < needed) self.lookups = try alloc.realloc(self.lookups, needed);
        if (self.key_views.len < needed) self.key_views = try alloc.realloc(self.key_views, needed);
        if (self.values.len < needed) self.values = try alloc.realloc(self.values, needed);
        if (self.vector_views.len < needed) self.vector_views = try alloc.realloc(self.vector_views, needed);
        if (self.distances.len < needed) self.distances = try alloc.realloc(self.distances, needed);
        if (self.error_bounds.len < needed) self.error_bounds = try alloc.realloc(self.error_bounds, needed);
        if (self.vector_batch.len < self.dims * needed) self.vector_batch = try alloc.realloc(self.vector_batch, self.dims * needed);
    }

    pub fn ensureDistanceCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.positions.len < needed) self.positions = try alloc.realloc(self.positions, needed);
        if (self.distances.len < needed) self.distances = try alloc.realloc(self.distances, needed);
        if (self.error_bounds.len < needed) self.error_bounds = try alloc.realloc(self.error_bounds, needed);
    }

    pub fn ensureRerankCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.flags.len < needed) self.flags = try alloc.realloc(self.flags, needed);
        try self.ensureVectorFetchCapacity(alloc, needed);
    }

    pub fn ensureMemberIdCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
    }

    pub fn ensurePostingOverlayAppendCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.posting_overlay_appended_ids.len < needed) self.posting_overlay_appended_ids = try alloc.realloc(self.posting_overlay_appended_ids, needed);
        if (self.posting_overlay_appended_live.len < needed) self.posting_overlay_appended_live = try alloc.realloc(self.posting_overlay_appended_live, needed);
    }

    pub fn resetPostingOverlayApply(self: *SearchScratch) void {
        self.posting_overlay_removed_members.clearRetainingCapacity();
        self.posting_overlay_appended_positions.clearRetainingCapacity();
        self.posting_overlay_appended_count = 0;
    }

    pub fn setPostingMemberCacheAdmissionEnabled(self: *SearchScratch, enabled: bool) void {
        self.posting_member_cache_admission_enabled = enabled;
    }

    pub fn trimPostingMemberCache(self: *SearchScratch, alloc: Allocator, target_bytes: u64) u64 {
        var evictions: u64 = 0;
        while (self.posting_member_cache_bytes > target_bytes and self.posting_member_cache.items.len != 0) {
            self.evictPostingMemberCacheEntry(alloc, 0);
            evictions += 1;
        }
        return evictions;
    }

    pub fn clearPostingMemberCache(self: *SearchScratch, alloc: Allocator) u64 {
        return self.trimPostingMemberCache(alloc, 0);
    }

    pub fn cachedPostingMembers(self: *SearchScratch, posting_id: u64, mutation_version: u64) ?[]const u64 {
        const slot = self.posting_member_cache_slots.get(posting_id) orelse return null;
        if (slot >= self.posting_member_cache.items.len) return null;
        if (self.posting_member_cache.items[slot].mutation_version != mutation_version) return null;
        const last = self.posting_member_cache.items.len - 1;
        if (slot != last) {
            std.mem.swap(PostingMemberCacheEntry, &self.posting_member_cache.items[slot], &self.posting_member_cache.items[last]);
            self.posting_member_cache_slots.getPtr(self.posting_member_cache.items[slot].posting_id).?.* = slot;
            self.posting_member_cache_slots.getPtr(self.posting_member_cache.items[last].posting_id).?.* = last;
        }
        return self.posting_member_cache.items[last].members;
    }

    pub fn cachePostingMembers(self: *SearchScratch, alloc: Allocator, posting_id: u64, mutation_version: u64, members: []const u64) !PostingMemberCacheResult {
        const member_bytes = byteLen(members);
        var result = PostingMemberCacheResult{ .member_bytes = self.posting_member_cache_bytes };
        if (!self.posting_member_cache_admission_enabled or self.max_posting_member_cache_bytes == 0 or self.max_posting_member_cache_entry_bytes == 0) {
            result.admission_skips += 1;
            return result;
        }
        if (self.posting_member_cache_slots.get(posting_id)) |i| self.evictPostingMemberCacheEntry(alloc, i);
        if (member_bytes > self.max_posting_member_cache_entry_bytes or member_bytes > self.max_posting_member_cache_bytes) {
            result.admission_skips += 1;
            result.member_bytes = self.posting_member_cache_bytes;
            return result;
        }
        var evictions: u64 = 0;
        while (self.max_posting_member_cache_bytes != 0 and self.posting_member_cache_bytes + member_bytes > self.max_posting_member_cache_bytes and self.posting_member_cache.items.len != 0) {
            self.evictPostingMemberCacheEntry(alloc, 0);
            evictions += 1;
        }
        try self.posting_member_cache_slots.ensureUnusedCapacity(alloc, 1);
        try self.posting_member_cache.append(alloc, .{
            .posting_id = posting_id,
            .mutation_version = mutation_version,
            .members = try alloc.dupe(u64, members),
        });
        self.posting_member_cache_slots.putAssumeCapacity(posting_id, self.posting_member_cache.items.len - 1);
        self.posting_member_cache_bytes += member_bytes;
        result.inserted = true;
        result.evictions = evictions;
        result.member_bytes = self.posting_member_cache_bytes;
        return result;
    }

    fn evictPostingMemberCacheEntry(self: *SearchScratch, alloc: Allocator, index: usize) void {
        const entry = self.posting_member_cache.items[index];
        self.posting_member_cache_bytes -|= byteLen(entry.members);
        _ = self.posting_member_cache_slots.remove(entry.posting_id);
        const last = self.posting_member_cache.items.len - 1;
        if (index != last) {
            self.posting_member_cache.items[index] = self.posting_member_cache.items[last];
            self.posting_member_cache_slots.getPtr(self.posting_member_cache.items[index].posting_id).?.* = index;
        }
        self.posting_member_cache.items.len = last;
        alloc.free(entry.members);
    }

    pub fn bytes(self: *const SearchScratch) u64 {
        const posting_member_cache_bytes =
            byteLen(self.posting_member_cache.items) +
            approximateHashMapBytes(self.posting_member_cache_slots.capacity(), @sizeOf(u64), @sizeOf(usize)) +
            self.posting_member_cache_bytes;
        const posting_overlay_bytes =
            approximateHashMapBytes(self.posting_overlay_removed_members.capacity(), @sizeOf(u64), 0) +
            approximateHashMapBytes(self.posting_overlay_appended_positions.capacity(), @sizeOf(u64), @sizeOf(usize)) +
            byteLen(self.posting_overlay_appended_ids) +
            byteLen(self.posting_overlay_appended_live);
        return estimateScratchBytes(&self.estimate) +
            byteLen(self.transformed_query) +
            byteLen(self.centroid) +
            byteLen(self.vector) +
            byteLen(self.vector_batch) +
            byteLen(self.member_ids) +
            byteLen(self.vector_ids) +
            byteLen(self.metadata) +
            byteLen(self.flags) +
            byteLen(self.positions) +
            byteLen(self.lookups) +
            byteLen(self.key_views) +
            byteLen(self.values) +
            byteLen(self.vector_views) +
            byteLen(self.distances) +
            byteLen(self.error_bounds) +
            posting_member_cache_bytes +
            posting_overlay_bytes;
    }

    pub fn shouldRetain(self: *const SearchScratch, max_retained_bytes: u64) bool {
        return max_retained_bytes == 0 or self.bytes() <= max_retained_bytes;
    }

    pub fn deinit(self: *SearchScratch, alloc: Allocator) void {
        self.estimate.deinit(alloc);
        alloc.free(self.transformed_query);
        alloc.free(self.centroid);
        alloc.free(self.vector);
        alloc.free(self.vector_batch);
        alloc.free(self.member_ids);
        alloc.free(self.vector_ids);
        alloc.free(self.metadata);
        alloc.free(self.flags);
        alloc.free(self.positions);
        alloc.free(self.lookups);
        alloc.free(self.key_views);
        alloc.free(self.values);
        alloc.free(self.vector_views);
        alloc.free(self.distances);
        alloc.free(self.error_bounds);
        for (self.posting_member_cache.items) |entry| alloc.free(entry.members);
        self.posting_member_cache.deinit(alloc);
        self.posting_member_cache_slots.deinit(alloc);
        self.posting_overlay_removed_members.deinit(alloc);
        self.posting_overlay_appended_positions.deinit(alloc);
        alloc.free(self.posting_overlay_appended_ids);
        alloc.free(self.posting_overlay_appended_live);
        self.* = undefined;
    }
};

fn effectivePostingMemberCacheEntryBytes(max_cache_bytes: u64, configured_entry_bytes: u64) u64 {
    if (max_cache_bytes == 0) return 0;
    if (configured_entry_bytes != 0) return @min(configured_entry_bytes, max_cache_bytes);
    return @max(@as(u64, @sizeOf(u64)), max_cache_bytes / 4);
}

fn byteLen(values: anytype) u64 {
    return @as(u64, @intCast(values.len * @sizeOf(std.meta.Child(@TypeOf(values)))));
}

fn approximateHashMapBytes(capacity: usize, comptime key_size: usize, comptime value_size: usize) u64 {
    if (capacity == 0) return 0;
    return @intCast(capacity * (key_size + value_size + 2));
}

test "SearchScratch grows error bounds with vector fetch capacity" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    try scratch.ensureVectorFetchCapacity(alloc, 5);

    try std.testing.expect(scratch.distances.len >= 5);
    try std.testing.expect(scratch.error_bounds.len >= 5);
    try std.testing.expect(scratch.vector_batch.len >= 4 * 5);
}

test "SearchScratch grows distance capacity without vector fetch buffers" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    const vector_batch_len = scratch.vector_batch.len;
    const vector_ids_len = scratch.vector_ids.len;

    try scratch.ensureDistanceCapacity(alloc, 5);

    try std.testing.expect(scratch.positions.len >= 5);
    try std.testing.expect(scratch.distances.len >= 5);
    try std.testing.expect(scratch.error_bounds.len >= 5);
    try std.testing.expectEqual(vector_batch_len, scratch.vector_batch.len);
    try std.testing.expectEqual(vector_ids_len, scratch.vector_ids.len);
}

test "SearchScratch bounds posting member cache and reports evictions" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 4 * @sizeOf(u64), 4 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const first = [_]u64{ 1, 2 };
    const first_result = try scratch.cachePostingMembers(alloc, 1, 1, first[0..]);
    try std.testing.expect(first_result.inserted);
    try std.testing.expectEqual(@as(u64, 0), first_result.evictions);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(u64)), first_result.member_bytes);

    const second = [_]u64{ 3, 4, 5 };
    const second_result = try scratch.cachePostingMembers(alloc, 2, 1, second[0..]);
    try std.testing.expect(second_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), second_result.evictions);
    try std.testing.expectEqual(@as(u64, 3 * @sizeOf(u64)), second_result.member_bytes);
    try std.testing.expect(scratch.cachedPostingMembers(1, 1) == null);
    try std.testing.expect(scratch.cachedPostingMembers(2, 1) != null);

    const oversized = [_]u64{ 6, 7, 8, 9, 10 };
    const oversized_result = try scratch.cachePostingMembers(alloc, 3, 1, oversized[0..]);
    try std.testing.expect(!oversized_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), oversized_result.admission_skips);
    try std.testing.expectEqual(@as(u64, 3 * @sizeOf(u64)), oversized_result.member_bytes);
}

test "SearchScratch posting member cache refreshes hit recency" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 4 * @sizeOf(u64), 4 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const first = [_]u64{ 1, 2 };
    const second = [_]u64{ 3, 4 };
    const third = [_]u64{ 5, 6 };
    _ = try scratch.cachePostingMembers(alloc, 1, 1, first[0..]);
    _ = try scratch.cachePostingMembers(alloc, 2, 1, second[0..]);

    const hot = scratch.cachedPostingMembers(1, 1) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualSlices(u64, first[0..], hot);

    const third_result = try scratch.cachePostingMembers(alloc, 3, 1, third[0..]);
    try std.testing.expect(third_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), third_result.evictions);
    try std.testing.expect(scratch.cachedPostingMembers(1, 1) != null);
    try std.testing.expect(scratch.cachedPostingMembers(2, 1) == null);
    try std.testing.expect(scratch.cachedPostingMembers(3, 1) != null);
}

test "SearchScratch disables posting member cache when byte cap is zero" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    const members = [_]u64{ 1, 2 };
    const result = try scratch.cachePostingMembers(alloc, 1, 1, members[0..]);
    try std.testing.expect(!result.inserted);
    try std.testing.expectEqual(@as(u64, 1), result.admission_skips);
    try std.testing.expectEqual(@as(u64, 0), result.member_bytes);
    try std.testing.expect(scratch.cachedPostingMembers(1, 1) == null);
}

test "SearchScratch skips entries above per posting member cache cap" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 8 * @sizeOf(u64), 2 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const small = [_]u64{ 1, 2 };
    const small_result = try scratch.cachePostingMembers(alloc, 1, 1, small[0..]);
    try std.testing.expect(small_result.inserted);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(u64)), small_result.member_bytes);

    const large = [_]u64{ 3, 4, 5 };
    const large_result = try scratch.cachePostingMembers(alloc, 2, 1, large[0..]);
    try std.testing.expect(!large_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), large_result.admission_skips);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(u64)), large_result.member_bytes);
    try std.testing.expect(scratch.cachedPostingMembers(1, 1) != null);
    try std.testing.expect(scratch.cachedPostingMembers(2, 1) == null);
}

fn estimateScratchBytes(scratch: *const quantizer.RaBitQuantizer.EstimateScratch) u64 {
    return byteLen(scratch.query_diff) +
        byteLen(scratch.q1) +
        byteLen(scratch.q2) +
        byteLen(scratch.q3) +
        byteLen(scratch.q4);
}

pub fn requestHasExtraFilters(
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
) bool {
    return req.filter_prefix.len > 0 or
        req.distance_over != null or
        req.distance_under != null or
        !filter_state.isTrivial();
}

pub fn exactDistanceToStoredVector(
    metric: types.HBCConfig,
    query: []const f32,
    query_measure: f32,
    candidate: []const f32,
) f32 {
    return switch (metric.metric) {
        .cosine => blk: {
            if (query_measure == 0) break :blk 1.0;
            break :blk 1.0 - (vec.dot(query, candidate) / query_measure);
        },
        else => vec.distanceToQuery(query, query_measure, candidate, metric.metric),
    };
}

pub fn exactDistancesToStoredVectors(
    metric: types.HBCConfig,
    query: []const f32,
    query_measure: f32,
    candidates: []const []const f32,
    distances: []f32,
) void {
    std.debug.assert(candidates.len <= distances.len);
    switch (metric.metric) {
        .l2_squared => vec.batchL2SquaredDistance(query, candidates, distances),
        .inner_product => {
            vec.batchDot(query, candidates, distances);
            for (distances[0..candidates.len]) |*distance| distance.* = -distance.*;
        },
        .cosine => {
            if (query_measure == 0) {
                @memset(distances[0..candidates.len], 1.0);
                return;
            }
            vec.batchDot(query, candidates, distances);
            for (distances[0..candidates.len]) |*distance| {
                distance.* = 1.0 - (distance.* / query_measure);
            }
        },
    }
}
