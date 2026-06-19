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
const posting = @import("posting.zig");

const posting_member_cache_miss_count_limit: usize = 256;
const posting_member_cache_reuse_admit_count: u8 = 2;
const posting_delta_tail_cache_slot_count: usize = 4;
const posting_delta_tail_prefetch_min: usize = 0;
const posting_delta_tail_prefetch_default: usize = 3;
const posting_delta_tail_prefetch_max: usize = 8;
const posting_base_header_cache_slot_count: usize = 8;
const posting_base_header_cache_max_header_bytes: usize = 32;

pub const RerankLookup = struct {
    item_index: usize,
    vector_id: u64,
    key: [10]u8,
};

pub const PostingMemberCacheEntry = struct {
    posting_id: u64,
    base_generation: u64,
    mutation_version: u64,
    max_delta_sequence: u64,
    members: []u64,
    score: u32 = 1,
};

pub const PostingMemberCacheResult = struct {
    inserted: bool = false,
    admission_skips: u64 = 0,
    evictions: u64 = 0,
    member_bytes: u64 = 0,
};

pub const PostingDeltaTailCacheView = struct {
    sequences: []const u64,
    ids: []const u64,
    ops: []const u8,
};

pub const PostingBaseHeaderCacheView = struct {
    generation: u64,
    member_count: usize,
};

const PostingBaseHeaderCacheEntry = struct {
    posting_id: u64 = 0,
    valid: bool = false,
    header_len: usize = 0,
    header_bytes: [posting_base_header_cache_max_header_bytes]u8 = undefined,
    generation: u64 = 0,
    member_count: usize = 0,

    fn matches(self: *const PostingBaseHeaderCacheEntry, posting_id: u64, header_bytes: []const u8) bool {
        return self.valid and
            self.posting_id == posting_id and
            self.header_len == header_bytes.len and
            std.mem.eql(u8, self.header_bytes[0..self.header_len], header_bytes);
    }

    fn set(self: *PostingBaseHeaderCacheEntry, posting_id: u64, header_bytes: []const u8, generation: u64, member_count: usize) void {
        self.posting_id = posting_id;
        self.valid = true;
        self.header_len = header_bytes.len;
        @memcpy(self.header_bytes[0..header_bytes.len], header_bytes);
        self.generation = generation;
        self.member_count = member_count;
    }

    fn view(self: *const PostingBaseHeaderCacheEntry) PostingBaseHeaderCacheView {
        return .{
            .generation = self.generation,
            .member_count = self.member_count,
        };
    }
};

const PostingDeltaTailCacheEntry = struct {
    posting_id: u64 = 0,
    valid: bool = false,
    storage: []u64 = &.{},
    sequences: []u64 = &.{},
    ids: []u64 = &.{},
    ops: []u8 = &.{},
    count: usize = 0,

    fn reset(self: *PostingDeltaTailCacheEntry, posting_id: u64) void {
        self.posting_id = posting_id;
        self.count = 0;
        self.valid = true;
    }

    fn append(self: *PostingDeltaTailCacheEntry, alloc: Allocator, sequence: u64, vector_id: u64, op: u8) !void {
        const needed = self.count + 1;
        if (self.sequences.len < needed or self.ids.len < needed or self.ops.len < needed) {
            const current_capacity = @min(self.sequences.len, @min(self.ids.len, self.ops.len));
            const doubled = current_capacity *| 2;
            const capacity = @max(needed, @max(doubled, @as(usize, 8)));
            const replacement = try allocateDeltaTailCacheStorage(alloc, capacity);
            errdefer alloc.free(replacement);
            const views = carveDeltaTailCacheStorage(replacement, capacity);
            @memcpy(views.sequences[0..self.count], self.sequences[0..self.count]);
            @memcpy(views.ids[0..self.count], self.ids[0..self.count]);
            @memcpy(views.ops[0..self.count], self.ops[0..self.count]);
            alloc.free(self.storage);
            self.storage = replacement;
            self.sequences = views.sequences;
            self.ids = views.ids;
            self.ops = views.ops;
        }
        self.sequences[self.count] = sequence;
        self.ids[self.count] = vector_id;
        self.ops[self.count] = op;
        self.count = needed;
    }

    fn view(self: *const PostingDeltaTailCacheEntry) PostingDeltaTailCacheView {
        return .{
            .sequences = self.sequences[0..self.count],
            .ids = self.ids[0..self.count],
            .ops = self.ops[0..self.count],
        };
    }

    fn bytes(self: *const PostingDeltaTailCacheEntry) u64 {
        return byteLen(self.storage);
    }

    fn deinit(self: *PostingDeltaTailCacheEntry, alloc: Allocator) void {
        alloc.free(self.storage);
        self.* = .{};
    }
};

pub const SearchScratch = struct {
    dims: usize,
    estimate: quantizer.RaBitQuantizer.EstimateScratch,
    vector_storage: []f32,
    transformed_query: []f32,
    centroid: []f32,
    vector: []f32,
    vector_batch: []f32,
    member_ids: []u64,
    query_storage: []u64,
    vector_ids: []u64,
    metadata: []?[]const u8,
    flags: []bool,
    positions: []usize,
    lookups: []RerankLookup,
    key_views: [][]const u8,
    values: []?[]const u8,
    vector_views: [][]const f32,
    distance_storage: []f32,
    distances: []f32,
    error_bounds: []f32,
    posting_member_cache: std.ArrayListUnmanaged(PostingMemberCacheEntry) = .empty,
    posting_member_cache_slots: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    posting_member_cache_miss_counts: std.AutoHashMapUnmanaged(u64, u8) = .empty,
    posting_member_cache_bytes: u64 = 0,
    max_posting_member_cache_bytes: u64 = 0,
    max_posting_member_cache_entry_bytes: u64 = 0,
    posting_member_cache_admission_enabled: bool = true,
    posting_overlay_removed_members: std.AutoHashMapUnmanaged(u64, void) = .empty,
    posting_overlay_appended_positions: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    posting_overlay_append_storage: []u64 = &.{},
    posting_overlay_appended_ids: []u64 = &.{},
    posting_overlay_appended_live: []bool = &.{},
    posting_overlay_appended_count: usize = 0,
    posting_delta_records: []posting.PostingDeltaRecord = &.{},
    posting_delta_record_count: usize = 0,
    posting_delta_tail_cache: [posting_delta_tail_cache_slot_count]PostingDeltaTailCacheEntry = [_]PostingDeltaTailCacheEntry{.{}} ** posting_delta_tail_cache_slot_count,
    posting_delta_tail_cache_active_slot: usize = 0,
    posting_delta_tail_cache_next_slot: usize = 0,
    posting_delta_tail_prefetch_limit: usize = posting_delta_tail_prefetch_default,
    posting_delta_tail_prefetch_hits: u32 = 0,
    posting_delta_tail_prefetch_misses: u32 = 0,
    posting_delta_tail_prefetch_decoded_bytes: u64 = 0,
    posting_base_header_cache: [posting_base_header_cache_slot_count]PostingBaseHeaderCacheEntry = [_]PostingBaseHeaderCacheEntry{.{}} ** posting_base_header_cache_slot_count,
    posting_base_header_cache_next_slot: usize = 0,
    scratch_allocation_count: u64 = 0,
    scratch_allocation_bytes: u64 = 0,

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
        const vector_storage_len = try std.math.mul(usize, dims, 3);
        const vector_storage = try alloc.alloc(f32, vector_storage_len);
        errdefer alloc.free(vector_storage);
        const transformed_query = vector_storage[0..dims];
        const centroid = vector_storage[dims .. 2 * dims];
        const vector = vector_storage[2 * dims .. 3 * dims];
        const member_ids = try alloc.alloc(u64, max_leaf);
        errdefer alloc.free(member_ids);
        const query_storage = try allocateQueryStorage(alloc, max_candidates);
        errdefer alloc.free(query_storage);
        const query_views = carveQueryStorage(query_storage, max_candidates);
        const distance_storage = try alloc.alloc(f32, 2 * max_candidates);
        errdefer alloc.free(distance_storage);
        return .{
            .dims = dims,
            .estimate = estimate,
            .vector_storage = vector_storage,
            .transformed_query = transformed_query,
            .centroid = centroid,
            .vector = vector,
            .vector_batch = &.{},
            .member_ids = member_ids,
            .query_storage = query_storage,
            .vector_ids = query_views.vector_ids,
            .metadata = query_views.metadata,
            .flags = query_views.flags,
            .positions = query_views.positions,
            .lookups = query_views.lookups,
            .key_views = query_views.key_views,
            .values = query_views.values,
            .vector_views = query_views.vector_views,
            .distance_storage = distance_storage,
            .distances = distance_storage[0..max_candidates],
            .error_bounds = distance_storage[max_candidates .. 2 * max_candidates],
            .max_posting_member_cache_bytes = max_posting_member_cache_bytes,
            .max_posting_member_cache_entry_bytes = effectivePostingMemberCacheEntryBytes(
                max_posting_member_cache_bytes,
                max_posting_member_cache_entry_bytes,
            ),
            .scratch_allocation_count = initialScratchAllocationCount(.{
                vector_storage,
                member_ids,
                query_storage,
                distance_storage,
            }),
            .scratch_allocation_bytes = byteLen(vector_storage) +
                byteLen(member_ids) +
                byteLen(query_storage) +
                byteLen(distance_storage),
        };
    }

    pub fn allocationCount(self: *const SearchScratch) u64 {
        return self.scratch_allocation_count;
    }

    pub fn allocationBytes(self: *const SearchScratch) u64 {
        return self.scratch_allocation_bytes;
    }

    fn noteScratchAllocation(self: *SearchScratch, byte_count: u64) void {
        if (byte_count == 0) return;
        self.scratch_allocation_count +|= 1;
        self.scratch_allocation_bytes +|= byte_count;
    }

    pub fn ensureVectorFetchCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        try self.ensureQueryStorageCapacity(alloc, needed);
        try self.ensureDistanceStorageCapacity(alloc, needed);
        try self.ensureVectorBatchCapacity(alloc, needed);
    }

    pub fn ensureVectorBatchCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        const vector_count = try std.math.mul(usize, self.dims, needed);
        if (self.vector_batch.len < vector_count) {
            self.vector_batch = try alloc.realloc(self.vector_batch, nextScratchCapacity(self.vector_batch.len, vector_count));
            self.noteScratchAllocation(byteLen(self.vector_batch));
        }
    }

    pub fn ensureDistanceCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        try self.ensureQueryStorageCapacity(alloc, needed);
        try self.ensureDistanceStorageCapacity(alloc, needed);
    }

    pub fn ensureDistanceOnlyCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        try self.ensureDistanceStorageCapacity(alloc, needed);
    }

    fn ensureQueryStorageCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.positions.len >= needed) return;
        const capacity = nextScratchCapacity(self.positions.len, needed);
        const replacement = try allocateQueryStorage(alloc, capacity);
        errdefer alloc.free(replacement);
        const views = carveQueryStorage(replacement, capacity);
        copyQueryStorageViews(views, .{
            .positions = self.positions,
            .vector_ids = self.vector_ids,
            .metadata = self.metadata,
            .flags = self.flags,
            .lookups = self.lookups,
            .key_views = self.key_views,
            .values = self.values,
            .vector_views = self.vector_views,
        });
        alloc.free(self.query_storage);
        self.query_storage = replacement;
        self.noteScratchAllocation(byteLen(self.query_storage));
        self.positions = views.positions;
        self.vector_ids = views.vector_ids;
        self.metadata = views.metadata;
        self.flags = views.flags;
        self.lookups = views.lookups;
        self.key_views = views.key_views;
        self.values = views.values;
        self.vector_views = views.vector_views;
    }

    fn ensureDistanceStorageCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.distances.len >= needed and self.error_bounds.len >= needed) return;
        const old_distance_len = self.distances.len;
        const old_error_bound_len = self.error_bounds.len;
        const capacity = nextScratchCapacity(@min(self.distances.len, self.error_bounds.len), needed);
        const new_storage_len = try std.math.mul(usize, capacity, 2);
        self.distance_storage = try alloc.realloc(self.distance_storage, new_storage_len);
        self.noteScratchAllocation(byteLen(self.distance_storage));
        self.distances = self.distance_storage[0..capacity];
        self.error_bounds = self.distance_storage[capacity .. 2 * capacity];
        if (old_error_bound_len != 0) {
            std.mem.copyBackwards(f32, self.error_bounds[0..old_error_bound_len], self.distance_storage[old_distance_len .. old_distance_len + old_error_bound_len]);
        }
    }

    pub fn ensureRerankCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        try self.ensureVectorFetchCapacity(alloc, needed);
    }

    pub fn ensureMemberIdCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.member_ids.len < needed) {
            self.member_ids = try alloc.realloc(self.member_ids, nextScratchCapacity(self.member_ids.len, needed));
            self.noteScratchAllocation(byteLen(self.member_ids));
        }
    }

    pub fn ensurePostingOverlayAppendCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.posting_overlay_appended_ids.len < needed or self.posting_overlay_appended_live.len < needed) {
            const current_capacity = @min(self.posting_overlay_appended_ids.len, self.posting_overlay_appended_live.len);
            const doubled = current_capacity *| 2;
            const capacity = @max(needed, @max(doubled, @as(usize, 8)));
            const replacement = try allocatePostingOverlayAppendStorage(alloc, capacity);
            errdefer alloc.free(replacement);
            const views = carvePostingOverlayAppendStorage(replacement, capacity);
            @memcpy(views.ids[0..self.posting_overlay_appended_ids.len], self.posting_overlay_appended_ids);
            @memcpy(views.live[0..self.posting_overlay_appended_live.len], self.posting_overlay_appended_live);
            alloc.free(self.posting_overlay_append_storage);
            self.posting_overlay_append_storage = replacement;
            self.posting_overlay_appended_ids = views.ids;
            self.posting_overlay_appended_live = views.live;
            self.noteScratchAllocation(byteLen(self.posting_overlay_append_storage));
        }
    }

    pub fn resetPostingOverlayApply(self: *SearchScratch) void {
        self.posting_overlay_removed_members.clearRetainingCapacity();
        self.posting_overlay_appended_positions.clearRetainingCapacity();
        self.posting_overlay_appended_count = 0;
    }

    pub fn ensureDeltaRecordCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.posting_delta_records.len < needed) {
            self.posting_delta_records = try alloc.realloc(self.posting_delta_records, nextScratchCapacity(self.posting_delta_records.len, needed));
            self.noteScratchAllocation(byteLen(self.posting_delta_records));
        }
    }

    pub fn deltaRecordCount(self: *const SearchScratch) usize {
        return self.posting_delta_record_count;
    }

    pub fn appendDeltaRecordAssumeCapacity(self: *SearchScratch, record: posting.PostingDeltaRecord) void {
        self.posting_delta_records[self.posting_delta_record_count] = record;
        self.posting_delta_record_count += 1;
    }

    pub fn deltaRecords(self: *const SearchScratch) []const posting.PostingDeltaRecord {
        return self.posting_delta_records[0..self.posting_delta_record_count];
    }

    pub fn deltaRecordsMut(self: *SearchScratch) []posting.PostingDeltaRecord {
        return self.posting_delta_records[0..self.posting_delta_record_count];
    }

    pub fn resetDeltaRecords(self: *SearchScratch) void {
        self.posting_delta_record_count = 0;
    }

    pub fn cachedPostingDeltaTail(self: *SearchScratch, posting_id: u64) ?PostingDeltaTailCacheView {
        for (&self.posting_delta_tail_cache) |*entry| {
            if (entry.valid and entry.posting_id == posting_id) return entry.view();
        }
        return null;
    }

    pub fn postingDeltaTailPrefetchLimit(self: *const SearchScratch) usize {
        return self.posting_delta_tail_prefetch_limit;
    }

    pub fn notePostingDeltaTailCacheHit(self: *SearchScratch) void {
        self.posting_delta_tail_prefetch_hits +|= 1;
        self.tunePostingDeltaTailPrefetch();
    }

    pub fn notePostingDeltaTailCacheMiss(self: *SearchScratch) void {
        self.posting_delta_tail_prefetch_misses +|= 1;
        self.tunePostingDeltaTailPrefetch();
    }

    pub fn notePostingDeltaTailPrefetchDecodedBytes(self: *SearchScratch, byte_count: usize) void {
        self.posting_delta_tail_prefetch_decoded_bytes +|= @intCast(byte_count);
    }

    fn tunePostingDeltaTailPrefetch(self: *SearchScratch) void {
        const total = self.posting_delta_tail_prefetch_hits + self.posting_delta_tail_prefetch_misses;
        if (total < 8) return;
        if (self.posting_delta_tail_prefetch_hits * 2 >= self.posting_delta_tail_prefetch_misses and self.posting_delta_tail_prefetch_limit < posting_delta_tail_prefetch_max) {
            self.posting_delta_tail_prefetch_limit += 1;
        } else if (self.posting_delta_tail_prefetch_misses > self.posting_delta_tail_prefetch_hits * 3 and self.posting_delta_tail_prefetch_limit > posting_delta_tail_prefetch_min) {
            self.posting_delta_tail_prefetch_limit -= 1;
        }
        self.posting_delta_tail_prefetch_hits /= 2;
        self.posting_delta_tail_prefetch_misses /= 2;
    }

    pub fn beginPostingDeltaTailCache(self: *SearchScratch, posting_id: u64) void {
        if (self.findPostingDeltaTailCacheSlot(posting_id)) |slot| {
            self.posting_delta_tail_cache_active_slot = slot;
        } else {
            self.posting_delta_tail_cache_active_slot = self.posting_delta_tail_cache_next_slot;
            self.posting_delta_tail_cache_next_slot = (self.posting_delta_tail_cache_next_slot + 1) % self.posting_delta_tail_cache.len;
        }
        self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].reset(posting_id);
    }

    pub fn appendPostingDeltaTailCacheRecord(self: *SearchScratch, alloc: Allocator, sequence: u64, vector_id: u64, op: u8) !void {
        if (!self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].valid) return;
        const old_bytes = self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].bytes();
        try self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].append(alloc, sequence, vector_id, op);
        const new_bytes = self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].bytes();
        if (new_bytes > old_bytes) self.noteScratchAllocation(new_bytes);
    }

    pub fn invalidatePostingDeltaTailCache(self: *SearchScratch) void {
        self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].valid = false;
        self.posting_delta_tail_cache[self.posting_delta_tail_cache_active_slot].count = 0;
    }

    fn findPostingDeltaTailCacheSlot(self: *SearchScratch, posting_id: u64) ?usize {
        for (&self.posting_delta_tail_cache, 0..) |*entry, i| {
            if (entry.valid and entry.posting_id == posting_id) return i;
        }
        return null;
    }

    pub fn cachedPostingBaseHeader(self: *SearchScratch, posting_id: u64, header_bytes: []const u8) ?PostingBaseHeaderCacheView {
        if (header_bytes.len > posting_base_header_cache_max_header_bytes) return null;
        for (&self.posting_base_header_cache) |*entry| {
            if (entry.matches(posting_id, header_bytes)) return entry.view();
        }
        return null;
    }

    pub fn cachePostingBaseHeader(self: *SearchScratch, posting_id: u64, header_bytes: []const u8, generation: u64, member_count: usize) void {
        if (header_bytes.len > posting_base_header_cache_max_header_bytes) return;
        const slot = self.posting_base_header_cache_next_slot;
        self.posting_base_header_cache_next_slot = (self.posting_base_header_cache_next_slot + 1) % self.posting_base_header_cache.len;
        self.posting_base_header_cache[slot].set(posting_id, header_bytes, generation, member_count);
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

    pub fn notePostingMemberCacheMiss(self: *SearchScratch, alloc: Allocator, posting_id: u64) !void {
        if (!self.posting_member_cache_admission_enabled or self.max_posting_member_cache_bytes == 0) return;
        if (self.posting_member_cache_miss_counts.count() >= posting_member_cache_miss_count_limit and !self.posting_member_cache_miss_counts.contains(posting_id)) {
            self.posting_member_cache_miss_counts.clearRetainingCapacity();
        }
        const entry = try self.posting_member_cache_miss_counts.getOrPut(alloc, posting_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = 1;
        } else if (entry.value_ptr.* < posting_member_cache_reuse_admit_count) {
            entry.value_ptr.* += 1;
        }
    }

    pub fn cachedPostingMembers(self: *SearchScratch, posting_id: u64, base_generation: u64, mutation_version: u64, max_delta_sequence: u64) ?[]const u64 {
        const slot = self.posting_member_cache_slots.get(posting_id) orelse return null;
        if (slot >= self.posting_member_cache.items.len) return null;
        if (self.posting_member_cache.items[slot].base_generation != base_generation) return null;
        if (self.posting_member_cache.items[slot].mutation_version != mutation_version) return null;
        if (self.posting_member_cache.items[slot].max_delta_sequence != max_delta_sequence) return null;
        return self.promotePostingMemberCacheHit(slot);
    }

    pub fn cachedCurrentPostingMembers(self: *SearchScratch, posting_id: u64, mutation_version: u64, max_delta_sequence: u64) ?[]const u64 {
        const slot = self.posting_member_cache_slots.get(posting_id) orelse return null;
        if (slot >= self.posting_member_cache.items.len) return null;
        if (self.posting_member_cache.items[slot].base_generation < mutation_version) return null;
        if (self.posting_member_cache.items[slot].mutation_version != mutation_version) return null;
        if (self.posting_member_cache.items[slot].max_delta_sequence != max_delta_sequence) return null;
        return self.promotePostingMemberCacheHit(slot);
    }

    fn promotePostingMemberCacheHit(self: *SearchScratch, slot: usize) []const u64 {
        self.posting_member_cache.items[slot].score = self.posting_member_cache.items[slot].score +| 4;
        const last = self.posting_member_cache.items.len - 1;
        if (slot != last) {
            std.mem.swap(PostingMemberCacheEntry, &self.posting_member_cache.items[slot], &self.posting_member_cache.items[last]);
            self.posting_member_cache_slots.getPtr(self.posting_member_cache.items[slot].posting_id).?.* = slot;
            self.posting_member_cache_slots.getPtr(self.posting_member_cache.items[last].posting_id).?.* = last;
        }
        return self.posting_member_cache.items[last].members;
    }

    pub fn cachePostingMembers(self: *SearchScratch, alloc: Allocator, posting_id: u64, base_generation: u64, mutation_version: u64, max_delta_sequence: u64, members: []const u64) !PostingMemberCacheResult {
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
        const would_evict = self.posting_member_cache_bytes + member_bytes > self.max_posting_member_cache_bytes and self.posting_member_cache.items.len != 0;
        const large_entry = member_bytes > self.max_posting_member_cache_bytes / 2;
        const miss_count = self.posting_member_cache_miss_counts.get(posting_id) orelse 0;
        const candidate_score: u32 = @max(@as(u32, 1), miss_count);
        if (would_evict and large_entry and miss_count < posting_member_cache_reuse_admit_count) {
            result.admission_skips += 1;
            result.member_bytes = self.posting_member_cache_bytes;
            return result;
        }
        var evictions: u64 = 0;
        while (self.max_posting_member_cache_bytes != 0 and self.posting_member_cache_bytes + member_bytes > self.max_posting_member_cache_bytes and self.posting_member_cache.items.len != 0) {
            if (!postingMemberCacheCandidateBeatsVictim(candidate_score, members.len, member_bytes, self.posting_member_cache.items[0])) {
                result.admission_skips += 1;
                result.member_bytes = self.posting_member_cache_bytes;
                return result;
            }
            self.evictPostingMemberCacheEntry(alloc, 0);
            evictions += 1;
        }
        try self.posting_member_cache_slots.ensureUnusedCapacity(alloc, 1);
        const cached_members = try alloc.dupe(u64, members);
        errdefer alloc.free(cached_members);
        self.noteScratchAllocation(byteLen(cached_members));
        try self.posting_member_cache.append(alloc, .{
            .posting_id = posting_id,
            .base_generation = base_generation,
            .mutation_version = mutation_version,
            .max_delta_sequence = max_delta_sequence,
            .members = cached_members,
            .score = candidate_score,
        });
        self.posting_member_cache_slots.putAssumeCapacity(posting_id, self.posting_member_cache.items.len - 1);
        _ = self.posting_member_cache_miss_counts.remove(posting_id);
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

    pub fn trimVectorBatchForRetention(self: *SearchScratch, alloc: Allocator, max_retained_bytes: u64) bool {
        return self.trimColdScratchForRetention(alloc, max_retained_bytes);
    }

    pub fn trimColdScratchForRetention(self: *SearchScratch, alloc: Allocator, max_retained_bytes: u64) bool {
        var trimmed = false;
        if (max_retained_bytes == 0 or self.bytes() <= max_retained_bytes) return false;
        if (self.vector_batch.len != 0) {
            alloc.free(self.vector_batch);
            self.vector_batch = &.{};
            trimmed = true;
        }
        if (self.bytes() <= max_retained_bytes) return trimmed;
        for (&self.posting_delta_tail_cache) |*entry| {
            entry.deinit(alloc);
            trimmed = true;
        }
        if (self.bytes() <= max_retained_bytes) return trimmed;
        self.posting_overlay_removed_members.deinit(alloc);
        self.posting_overlay_removed_members = .empty;
        self.posting_overlay_appended_positions.deinit(alloc);
        self.posting_overlay_appended_positions = .empty;
        alloc.free(self.posting_overlay_append_storage);
        self.posting_overlay_append_storage = &.{};
        self.posting_overlay_appended_ids = &.{};
        self.posting_overlay_appended_live = &.{};
        self.posting_overlay_appended_count = 0;
        alloc.free(self.posting_delta_records);
        self.posting_delta_records = &.{};
        self.posting_delta_record_count = 0;
        trimmed = true;
        if (self.bytes() <= max_retained_bytes) return trimmed;
        alloc.free(self.query_storage);
        self.query_storage = &.{};
        self.positions = &.{};
        self.vector_ids = &.{};
        self.metadata = &.{};
        self.flags = &.{};
        self.lookups = &.{};
        self.key_views = &.{};
        self.values = &.{};
        self.vector_views = &.{};
        if (self.bytes() <= max_retained_bytes) return trimmed;
        alloc.free(self.distance_storage);
        self.distance_storage = &.{};
        self.distances = &.{};
        self.error_bounds = &.{};
        if (self.bytes() <= max_retained_bytes) return trimmed;
        alloc.free(self.member_ids);
        self.member_ids = &.{};
        return trimmed;
    }

    pub fn bytes(self: *const SearchScratch) u64 {
        const posting_member_cache_bytes =
            byteLen(self.posting_member_cache.items) +
            approximateHashMapBytes(self.posting_member_cache_slots.capacity(), @sizeOf(u64), @sizeOf(usize)) +
            approximateHashMapBytes(self.posting_member_cache_miss_counts.capacity(), @sizeOf(u64), @sizeOf(u8)) +
            self.posting_member_cache_bytes;
        const posting_overlay_bytes =
            approximateHashMapBytes(self.posting_overlay_removed_members.capacity(), @sizeOf(u64), 0) +
            approximateHashMapBytes(self.posting_overlay_appended_positions.capacity(), @sizeOf(u64), @sizeOf(usize)) +
            byteLen(self.posting_overlay_append_storage);
        var posting_delta_tail_cache_bytes: u64 = 0;
        for (&self.posting_delta_tail_cache) |*entry| posting_delta_tail_cache_bytes += entry.bytes();
        return estimateScratchBytes(&self.estimate) +
            byteLen(self.vector_storage) +
            byteLen(self.vector_batch) +
            byteLen(self.member_ids) +
            byteLen(self.query_storage) +
            byteLen(self.distance_storage) +
            byteLen(self.posting_delta_records) +
            posting_delta_tail_cache_bytes +
            posting_member_cache_bytes +
            posting_overlay_bytes;
    }

    pub fn shouldRetain(self: *const SearchScratch, max_retained_bytes: u64) bool {
        return max_retained_bytes == 0 or self.bytes() <= max_retained_bytes;
    }

    pub fn deinit(self: *SearchScratch, alloc: Allocator) void {
        self.estimate.deinit(alloc);
        alloc.free(self.vector_storage);
        alloc.free(self.vector_batch);
        alloc.free(self.member_ids);
        alloc.free(self.query_storage);
        alloc.free(self.distance_storage);
        for (self.posting_member_cache.items) |entry| alloc.free(entry.members);
        self.posting_member_cache.deinit(alloc);
        self.posting_member_cache_slots.deinit(alloc);
        self.posting_member_cache_miss_counts.deinit(alloc);
        self.posting_overlay_removed_members.deinit(alloc);
        self.posting_overlay_appended_positions.deinit(alloc);
        alloc.free(self.posting_overlay_append_storage);
        alloc.free(self.posting_delta_records);
        for (&self.posting_delta_tail_cache) |*entry| entry.deinit(alloc);
        self.* = undefined;
    }
};

const QueryStorageViews = struct {
    positions: []usize,
    vector_ids: []u64,
    metadata: []?[]const u8,
    flags: []bool,
    lookups: []RerankLookup,
    key_views: [][]const u8,
    values: []?[]const u8,
    vector_views: [][]const f32,
};

const DeltaTailCacheStorageViews = struct {
    sequences: []u64,
    ids: []u64,
    ops: []u8,
};

const PostingOverlayAppendViews = struct {
    ids: []u64,
    live: []bool,
};

fn allocateQueryStorage(alloc: Allocator, capacity: usize) ![]u64 {
    const bytes = queryStorageByteLen(capacity);
    const words = (bytes + @sizeOf(u64) - 1) / @sizeOf(u64);
    return try alloc.alloc(u64, words);
}

fn allocateDeltaTailCacheStorage(alloc: Allocator, capacity: usize) ![]u64 {
    const bytes = deltaTailCacheStorageByteLen(capacity);
    const words = (bytes + @sizeOf(u64) - 1) / @sizeOf(u64);
    return try alloc.alloc(u64, words);
}

fn allocatePostingOverlayAppendStorage(alloc: Allocator, capacity: usize) ![]u64 {
    const bytes = postingOverlayAppendStorageByteLen(capacity);
    const words = (bytes + @sizeOf(u64) - 1) / @sizeOf(u64);
    return try alloc.alloc(u64, words);
}

fn nextScratchCapacity(current: usize, needed: usize) usize {
    const doubled = current *| 2;
    return @max(needed, @max(doubled, @as(usize, 8)));
}

fn queryStorageByteLen(capacity: usize) usize {
    var offset: usize = 0;
    addQueryStorageSlice(usize, &offset, capacity);
    addQueryStorageSlice(u64, &offset, capacity);
    addQueryStorageSlice(?[]const u8, &offset, capacity);
    addQueryStorageSlice(bool, &offset, capacity);
    addQueryStorageSlice(RerankLookup, &offset, capacity);
    addQueryStorageSlice([]const u8, &offset, capacity);
    addQueryStorageSlice(?[]const u8, &offset, capacity);
    addQueryStorageSlice([]const f32, &offset, capacity);
    return alignForward(offset, @alignOf(u64));
}

fn deltaTailCacheStorageByteLen(capacity: usize) usize {
    var offset: usize = 0;
    addQueryStorageSlice(u64, &offset, capacity);
    addQueryStorageSlice(u64, &offset, capacity);
    addQueryStorageSlice(u8, &offset, capacity);
    return alignForward(offset, @alignOf(u64));
}

fn postingOverlayAppendStorageByteLen(capacity: usize) usize {
    var offset: usize = 0;
    addQueryStorageSlice(u64, &offset, capacity);
    addQueryStorageSlice(bool, &offset, capacity);
    return alignForward(offset, @alignOf(u64));
}

fn addQueryStorageSlice(comptime T: type, offset: *usize, capacity: usize) void {
    comptime std.debug.assert(@alignOf(T) <= @alignOf(u64));
    offset.* = alignForward(offset.*, @alignOf(T));
    offset.* += capacity * @sizeOf(T);
}

fn carveQueryStorage(storage: []u64, capacity: usize) QueryStorageViews {
    const bytes: []align(@alignOf(u64)) u8 = std.mem.sliceAsBytes(storage);
    var offset: usize = 0;
    return .{
        .positions = carveQueryStorageSlice(usize, bytes, &offset, capacity),
        .vector_ids = carveQueryStorageSlice(u64, bytes, &offset, capacity),
        .metadata = carveQueryStorageSlice(?[]const u8, bytes, &offset, capacity),
        .flags = carveQueryStorageSlice(bool, bytes, &offset, capacity),
        .lookups = carveQueryStorageSlice(RerankLookup, bytes, &offset, capacity),
        .key_views = carveQueryStorageSlice([]const u8, bytes, &offset, capacity),
        .values = carveQueryStorageSlice(?[]const u8, bytes, &offset, capacity),
        .vector_views = carveQueryStorageSlice([]const f32, bytes, &offset, capacity),
    };
}

fn carveDeltaTailCacheStorage(storage: []u64, capacity: usize) DeltaTailCacheStorageViews {
    const bytes: []align(@alignOf(u64)) u8 = std.mem.sliceAsBytes(storage);
    var offset: usize = 0;
    return .{
        .sequences = carveQueryStorageSlice(u64, bytes, &offset, capacity),
        .ids = carveQueryStorageSlice(u64, bytes, &offset, capacity),
        .ops = carveQueryStorageSlice(u8, bytes, &offset, capacity),
    };
}

fn carvePostingOverlayAppendStorage(storage: []u64, capacity: usize) PostingOverlayAppendViews {
    const bytes: []align(@alignOf(u64)) u8 = std.mem.sliceAsBytes(storage);
    var offset: usize = 0;
    return .{
        .ids = carveQueryStorageSlice(u64, bytes, &offset, capacity),
        .live = carveQueryStorageSlice(bool, bytes, &offset, capacity),
    };
}

fn carveQueryStorageSlice(comptime T: type, bytes: []align(@alignOf(u64)) u8, offset: *usize, capacity: usize) []T {
    comptime std.debug.assert(@alignOf(T) <= @alignOf(u64));
    offset.* = alignForward(offset.*, @alignOf(T));
    const byte_len = capacity * @sizeOf(T);
    const aligned: []align(@alignOf(T)) u8 = @alignCast(bytes[offset.* .. offset.* + byte_len]);
    const out = std.mem.bytesAsSlice(T, aligned);
    offset.* += byte_len;
    return out;
}

fn copyQueryStorageViews(dst: QueryStorageViews, src: QueryStorageViews) void {
    copySlicePrefix(usize, dst.positions, src.positions);
    copySlicePrefix(u64, dst.vector_ids, src.vector_ids);
    copySlicePrefix(?[]const u8, dst.metadata, src.metadata);
    copySlicePrefix(bool, dst.flags, src.flags);
    copySlicePrefix(RerankLookup, dst.lookups, src.lookups);
    copySlicePrefix([]const u8, dst.key_views, src.key_views);
    copySlicePrefix(?[]const u8, dst.values, src.values);
    copySlicePrefix([]const f32, dst.vector_views, src.vector_views);
}

fn copySlicePrefix(comptime T: type, dst: []T, src: []const T) void {
    @memcpy(dst[0..src.len], src);
}

fn alignForward(value: usize, alignment: usize) usize {
    return (value + alignment - 1) & ~(alignment - 1);
}

fn effectivePostingMemberCacheEntryBytes(max_cache_bytes: u64, configured_entry_bytes: u64) u64 {
    if (max_cache_bytes == 0) return 0;
    if (configured_entry_bytes != 0) return @min(configured_entry_bytes, max_cache_bytes);
    return @max(@as(u64, @sizeOf(u64)), max_cache_bytes / 4);
}

fn byteLen(values: anytype) u64 {
    return @as(u64, @intCast(values.len * @sizeOf(std.meta.Child(@TypeOf(values)))));
}

fn initialScratchAllocationCount(slices: anytype) u64 {
    var count: u64 = 0;
    inline for (slices) |slice| {
        if (slice.len != 0) count += 1;
    }
    return count;
}

fn approximateHashMapBytes(capacity: usize, comptime key_size: usize, comptime value_size: usize) u64 {
    if (capacity == 0) return 0;
    return @intCast(capacity * (key_size + value_size + 2));
}

fn postingMemberCacheCandidateBeatsVictim(candidate_score: u32, candidate_members: usize, candidate_bytes: u64, victim: PostingMemberCacheEntry) bool {
    if (candidate_score >= victim.score) return true;
    const candidate_saved = @as(u128, candidate_score) * @as(u128, candidate_members + 16);
    const victim_saved = @as(u128, victim.score) * @as(u128, victim.members.len + 16);
    const victim_bytes = @as(u128, byteLen(victim.members));
    return candidate_saved * victim_bytes > victim_saved * @as(u128, candidate_bytes);
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

test "SearchScratch slabs fixed vector work buffers" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 12), scratch.vector_storage.len);
    try std.testing.expectEqual(scratch.vector_storage[0..4].ptr, scratch.transformed_query.ptr);
    try std.testing.expectEqual(scratch.vector_storage[4..8].ptr, scratch.centroid.ptr);
    try std.testing.expectEqual(scratch.vector_storage[8..12].ptr, scratch.vector.ptr);
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

test "SearchScratch grows distance-only capacity without query slab" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    const positions_len = scratch.positions.len;
    const vector_ids_len = scratch.vector_ids.len;

    try scratch.ensureDistanceOnlyCapacity(alloc, 5);

    try std.testing.expectEqual(positions_len, scratch.positions.len);
    try std.testing.expectEqual(vector_ids_len, scratch.vector_ids.len);
    try std.testing.expect(scratch.distances.len >= 5);
    try std.testing.expect(scratch.error_bounds.len >= 5);
}

test "SearchScratch grows hot posting buffers geometrically" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    try scratch.ensureMemberIdCapacity(alloc, 3);
    try std.testing.expectEqual(@as(usize, 8), scratch.member_ids.len);

    try scratch.ensureVectorBatchCapacity(alloc, 3);
    try std.testing.expectEqual(@as(usize, 16), scratch.vector_batch.len);

    try scratch.ensureDeltaRecordCapacity(alloc, 1);
    try std.testing.expectEqual(@as(usize, 8), scratch.posting_delta_records.len);
    try scratch.ensureDeltaRecordCapacity(alloc, 9);
    try std.testing.expectEqual(@as(usize, 16), scratch.posting_delta_records.len);
}

test "SearchScratch grows query and distance slabs geometrically" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    try scratch.ensureDistanceCapacity(alloc, 3);
    try std.testing.expectEqual(@as(usize, 8), scratch.positions.len);
    try std.testing.expectEqual(@as(usize, 8), scratch.vector_ids.len);
    try std.testing.expectEqual(@as(usize, 8), scratch.distances.len);
    try std.testing.expectEqual(@as(usize, 8), scratch.error_bounds.len);

    try scratch.ensureDistanceCapacity(alloc, 9);
    try std.testing.expectEqual(@as(usize, 16), scratch.positions.len);
    try std.testing.expectEqual(@as(usize, 16), scratch.vector_ids.len);
    try std.testing.expectEqual(@as(usize, 16), scratch.distances.len);
    try std.testing.expectEqual(@as(usize, 16), scratch.error_bounds.len);
}

test "SearchScratch reports allocation pressure and retained bytes" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    const initial_allocations = scratch.allocationCount();
    const initial_allocation_bytes = scratch.allocationBytes();
    const initial_retained_bytes = scratch.bytes();
    try std.testing.expect(initial_allocations > 0);
    try std.testing.expect(initial_allocation_bytes > 0);
    try std.testing.expect(initial_retained_bytes > 0);

    try scratch.ensureVectorFetchCapacity(alloc, 5);

    try std.testing.expect(scratch.allocationCount() > initial_allocations);
    try std.testing.expect(scratch.allocationBytes() > initial_allocation_bytes);
    try std.testing.expect(scratch.bytes() > initial_retained_bytes);
}

test "SearchScratch trims cold slabs and regrows on demand" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    try scratch.ensureVectorFetchCapacity(alloc, 5);
    try scratch.ensureRerankCapacity(alloc, 5);
    try scratch.ensureMemberIdCapacity(alloc, 5);
    try std.testing.expect(scratch.query_storage.len > 0);
    try std.testing.expect(scratch.distance_storage.len > 0);
    try std.testing.expect(scratch.vector_batch.len > 0);
    try std.testing.expect(scratch.member_ids.len > 0);
    try std.testing.expect(scratch.flags.len > 0);

    try std.testing.expect(scratch.trimColdScratchForRetention(alloc, 1));
    try std.testing.expectEqual(@as(usize, 0), scratch.query_storage.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.positions.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.vector_ids.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.distance_storage.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.distances.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.error_bounds.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.vector_batch.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.member_ids.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.flags.len);

    try scratch.ensureRerankCapacity(alloc, 5);
    try scratch.ensureMemberIdCapacity(alloc, 5);
    try std.testing.expect(scratch.query_storage.len > 0);
    try std.testing.expect(scratch.positions.len >= 5);
    try std.testing.expect(scratch.vector_ids.len >= 5);
    try std.testing.expect(scratch.distance_storage.len >= 10);
    try std.testing.expect(scratch.distances.len >= 5);
    try std.testing.expect(scratch.error_bounds.len >= 5);
    try std.testing.expect(scratch.vector_batch.len >= 20);
    try std.testing.expect(scratch.member_ids.len >= 5);
    try std.testing.expect(scratch.flags.len >= 5);
}

test "SearchScratch caches multiple posting delta tails in a compact ring" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    scratch.beginPostingDeltaTailCache(10);
    try scratch.appendPostingDeltaTailCacheRecord(alloc, 7, 100, 1);
    try scratch.appendPostingDeltaTailCacheRecord(alloc, 8, 101, 2);

    scratch.beginPostingDeltaTailCache(11);
    try scratch.appendPostingDeltaTailCacheRecord(alloc, 9, 200, 3);

    const first = scratch.cachedPostingDeltaTail(10) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualSlices(u64, &.{ 7, 8 }, first.sequences);
    try std.testing.expectEqualSlices(u64, &.{ 100, 101 }, first.ids);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, first.ops);

    const second = scratch.cachedPostingDeltaTail(11) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualSlices(u64, &.{9}, second.sequences);
    try std.testing.expectEqualSlices(u64, &.{200}, second.ids);
    try std.testing.expectEqualSlices(u8, &.{3}, second.ops);

    scratch.beginPostingDeltaTailCache(12);
    scratch.beginPostingDeltaTailCache(13);
    scratch.beginPostingDeltaTailCache(14);
    try std.testing.expect(scratch.cachedPostingDeltaTail(10) == null);
    try std.testing.expect(scratch.cachedPostingDeltaTail(11) != null);
    try std.testing.expect(scratch.cachedPostingDeltaTail(14) != null);
}

test "SearchScratch posting delta tail cache grows geometrically" {
    const alloc = std.testing.allocator;
    var entry = PostingDeltaTailCacheEntry{};
    defer entry.deinit(alloc);

    entry.reset(10);
    var i: usize = 0;
    while (i < 9) : (i += 1) {
        try entry.append(alloc, @intCast(i + 1), @intCast(100 + i), @intCast(i % 3));
    }

    const view = entry.view();
    try std.testing.expectEqual(@as(usize, 9), view.sequences.len);
    try std.testing.expectEqual(@as(usize, 16), entry.sequences.len);
    try std.testing.expectEqual(entry.sequences.len, entry.ids.len);
    try std.testing.expectEqual(entry.sequences.len, entry.ops.len);
    try std.testing.expect(entry.storage.len > entry.sequences.len + entry.ids.len);
    try std.testing.expectEqual(entry.storage[0..16].ptr, entry.sequences.ptr);
    try std.testing.expectEqual(entry.storage[16..32].ptr, entry.ids.ptr);
    try std.testing.expectEqual(@as(u64, 1), view.sequences[0]);
    try std.testing.expectEqual(@as(u64, 9), view.sequences[8]);
    try std.testing.expectEqual(@as(u64, 100), view.ids[0]);
    try std.testing.expectEqual(@as(u64, 108), view.ids[8]);
    try std.testing.expectEqual(@as(u8, 0), view.ops[0]);
    try std.testing.expectEqual(@as(u8, 2), view.ops[8]);
}

test "SearchScratch grows posting overlay append buffers geometrically" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    try scratch.ensurePostingOverlayAppendCapacity(alloc, 1);
    try std.testing.expectEqual(@as(usize, 8), scratch.posting_overlay_appended_ids.len);
    try std.testing.expectEqual(scratch.posting_overlay_appended_ids.len, scratch.posting_overlay_appended_live.len);
    try std.testing.expect(scratch.posting_overlay_append_storage.len > scratch.posting_overlay_appended_ids.len);
    try std.testing.expectEqual(scratch.posting_overlay_append_storage[0..8].ptr, scratch.posting_overlay_appended_ids.ptr);

    try scratch.ensurePostingOverlayAppendCapacity(alloc, 9);
    try std.testing.expectEqual(@as(usize, 16), scratch.posting_overlay_appended_ids.len);
    try std.testing.expectEqual(scratch.posting_overlay_appended_ids.len, scratch.posting_overlay_appended_live.len);
    try std.testing.expect(scratch.posting_overlay_append_storage.len > scratch.posting_overlay_appended_ids.len);
    try std.testing.expectEqual(scratch.posting_overlay_append_storage[0..16].ptr, scratch.posting_overlay_appended_ids.ptr);
}

test "SearchScratch bounds posting member cache and reports evictions" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 4 * @sizeOf(u64), 4 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const first = [_]u64{ 1, 2 };
    const first_result = try scratch.cachePostingMembers(alloc, 1, 10, 1, 0, first[0..]);
    try std.testing.expect(first_result.inserted);
    try std.testing.expectEqual(@as(u64, 0), first_result.evictions);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(u64)), first_result.member_bytes);

    const second = [_]u64{ 3, 4, 5 };
    const second_result = try scratch.cachePostingMembers(alloc, 2, 10, 1, 0, second[0..]);
    try std.testing.expect(second_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), second_result.evictions);
    try std.testing.expectEqual(@as(u64, 3 * @sizeOf(u64)), second_result.member_bytes);
    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 1, 0) == null);
    try std.testing.expect(scratch.cachedPostingMembers(2, 10, 1, 0) != null);

    const oversized = [_]u64{ 6, 7, 8, 9, 10 };
    const oversized_result = try scratch.cachePostingMembers(alloc, 3, 10, 1, 0, oversized[0..]);
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
    _ = try scratch.cachePostingMembers(alloc, 1, 10, 1, 0, first[0..]);
    _ = try scratch.cachePostingMembers(alloc, 2, 10, 1, 0, second[0..]);

    const hot = scratch.cachedPostingMembers(1, 10, 1, 0) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualSlices(u64, first[0..], hot);

    const third_result = try scratch.cachePostingMembers(alloc, 3, 10, 1, 0, third[0..]);
    try std.testing.expect(third_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), third_result.evictions);
    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 1, 0) != null);
    try std.testing.expect(scratch.cachedPostingMembers(2, 10, 1, 0) == null);
    try std.testing.expect(scratch.cachedPostingMembers(3, 10, 1, 0) != null);
}

test "SearchScratch posting member cache key includes base mutation and delta sequence" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 8 * @sizeOf(u64), 8 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const members = [_]u64{ 1, 2, 3 };
    const result = try scratch.cachePostingMembers(alloc, 1, 10, 5, 20, members[0..]);
    try std.testing.expect(result.inserted);

    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 5, 20) != null);
    try std.testing.expect(scratch.cachedPostingMembers(1, 11, 5, 20) == null);
    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 6, 20) == null);
    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 5, 21) == null);
}

test "SearchScratch posting member cache supports current-generation lookup" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 8 * @sizeOf(u64), 8 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const members = [_]u64{ 1, 2, 3 };
    _ = try scratch.cachePostingMembers(alloc, 1, 10, 5, 0, members[0..]);

    try std.testing.expect(scratch.cachedCurrentPostingMembers(1, 5, 0) != null);
    try std.testing.expect(scratch.cachedCurrentPostingMembers(1, 10, 0) != null);
    try std.testing.expect(scratch.cachedCurrentPostingMembers(1, 11, 0) == null);
    try std.testing.expect(scratch.cachedCurrentPostingMembers(1, 5, 1) == null);
    try std.testing.expect(scratch.cachedCurrentPostingMembers(1, 6, 0) == null);
}

test "SearchScratch disables posting member cache when byte cap is zero" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 0, 0);
    defer scratch.deinit(alloc);

    const members = [_]u64{ 1, 2 };
    const result = try scratch.cachePostingMembers(alloc, 1, 10, 1, 0, members[0..]);
    try std.testing.expect(!result.inserted);
    try std.testing.expectEqual(@as(u64, 1), result.admission_skips);
    try std.testing.expectEqual(@as(u64, 0), result.member_bytes);
    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 1, 0) == null);
}

test "SearchScratch skips entries above per posting member cache cap" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2, 8 * @sizeOf(u64), 2 * @sizeOf(u64));
    defer scratch.deinit(alloc);

    const small = [_]u64{ 1, 2 };
    const small_result = try scratch.cachePostingMembers(alloc, 1, 10, 1, 0, small[0..]);
    try std.testing.expect(small_result.inserted);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(u64)), small_result.member_bytes);

    const large = [_]u64{ 3, 4, 5 };
    const large_result = try scratch.cachePostingMembers(alloc, 2, 10, 1, 0, large[0..]);
    try std.testing.expect(!large_result.inserted);
    try std.testing.expectEqual(@as(u64, 1), large_result.admission_skips);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(u64)), large_result.member_bytes);
    try std.testing.expect(scratch.cachedPostingMembers(1, 10, 1, 0) != null);
    try std.testing.expect(scratch.cachedPostingMembers(2, 10, 1, 0) == null);
}

fn estimateScratchBytes(scratch: *const quantizer.RaBitQuantizer.EstimateScratch) u64 {
    return scratch.bytes();
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
