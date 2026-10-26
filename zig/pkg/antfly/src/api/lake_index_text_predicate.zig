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

//! Exact indexed metadata predicates in a pinned native text snapshot.
const std = @import("std");
const local = @import("antfly_local_sources");
const rows = @import("lake_index_sql_rows.zig");
const corpus = @import("lake_index_native_text.zig");
const A = std.mem.Allocator;
const Bitmap = local.encoding_roaring.RoaringBitmap;
const Result = local.storage_db_query_search_exec.IndexedTextPredicate;
const Condition = local.sql_catalog.Condition;
const Graph = local.storage_db_query_graph_exec;
const Compiled = Graph.CompiledPatternFilter;

/// Translate a complete physical selection in bounded tiles before deriving
/// index-space pack/ordinal supersets. Residual predicates still use current rows.
pub fn indexSelection(a: A, source: *local.serverless_query_lake_serving.ServingSource, binding: ?local.serverless_query_lake_serving.ServingSource.RowResolver, selected: *const @import("lake_index_physical_set.zig").Set, allowance: *local.serverless_query_lake_serving.ServingSource.TranslationAllowance) !@import("lake_index_physical_set.zig").Set {
    if (selected.boundedCardinality(@intCast(allowance.remaining_rows)) == null) return error.QueryCandidateBudgetExceeded;
    return indexSelectionCutLimited(a, source.inventory.source_id, source.inventory.snapshot_id, binding, selected, allowance);
}
fn indexSelectionCut(a: A, source_id: []const u8, snapshot_id: []const u8, binding: ?local.serverless_query_lake_serving.ServingSource.RowResolver, selected: *const @import("lake_index_physical_set.zig").Set) !@import("lake_index_physical_set.zig").Set {
    return indexSelectionCutLimited(a, source_id, snapshot_id, binding, selected, null);
}
fn indexSelectionCutLimited(a: A, source_id: []const u8, snapshot_id: []const u8, binding: ?local.serverless_query_lake_serving.ServingSource.RowResolver, selected: *const @import("lake_index_physical_set.zig").Set, allowance: ?*local.serverless_query_lake_serving.ServingSource.TranslationAllowance) !@import("lake_index_physical_set.zig").Set {
    const Set = @import("lake_index_physical_set.zig").Set;
    const RowRef = local.storage_rowsource_types.RowRef;
    var result = Set.init(a);
    errdefer result.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var refs: [256]RowRef = undefined;
    var count: usize = 0;
    var files = selected.files.iterator();
    while (files.next()) |file| {
        var blocks = file.value_ptr.iterator();
        while (blocks.next()) |block| {
            var selected_rows = block.value_ptr.iterator();
            while (selected_rows.next()) |low| {
                refs[count] = .{ .external = .{ .source_id = source_id, .snapshot_id = snapshot_id, .file_id = file.key_ptr.*, .row_group_ordinal = block.key_ptr.group, .row_ordinal = (@as(u64, block.key_ptr.high) << 32) | low } };
                count += 1;
                if (count == refs.len) {
                    const mapped = try indexBatch(scratch.allocator(), binding, refs[0..count], allowance);
                    for (mapped) |ref| try result.addRow(ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal);
                    _ = scratch.reset(.retain_capacity);
                    count = 0;
                }
            }
        }
    }
    const mapped = try indexBatch(scratch.allocator(), binding, refs[0..count], allowance);
    for (mapped) |ref| try result.addRow(ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal);
    return result;
}

fn indexBatch(a: A, binding: ?local.serverless_query_lake_serving.ServingSource.RowResolver, refs: []const local.storage_rowsource_types.RowRef, allowance: ?*local.serverless_query_lake_serving.ServingSource.TranslationAllowance) ![]const local.storage_rowsource_types.RowRef {
    const resolver = binding orelse return refs;
    if (allowance) |limited| {
        const translate = resolver.translate_many_bounded orelse return error.QueryCandidateBudgetExceeded;
        return translate(resolver.ptr, a, refs, false, limited);
    }
    if (resolver.translate_many) |translate| return translate(resolver.ptr, a, refs, false);
    const result = try a.alloc(local.storage_rowsource_types.RowRef, refs.len);
    for (refs, result) |ref, *target| target.* = try resolver.to_index(resolver.ptr, a, ref);
    return result;
}

/// Four independently reusable slots bound decode/rank storage even when the
/// request allocator is an arena. Each slot uses a fixed buffer, not an arena
/// whose freed objects would remain resident in the request's parent arena.
pub const LiveRowsCache = struct {
    const decoded = @import("lake_index_decoded_metadata.zig");
    const FileSlot = struct {
        digest: [32]u8 = @splat(0),
        group: ?corpus.FileGroup = null,
        row_count: u64 = 0,
        shared: ?decoded.Owned(corpus.FileGroup) = null,
        used: u64 = 0,
        fn clear(self: *@This()) void {
            if (self.shared) |owned| owned.release();
            self.* = .{};
        }
    };
    const slot_bytes = 1024 * 1024;
    const Slot = struct {
        buffer: ?[]u8 = null,
        fixed: std.heap.FixedBufferAllocator = undefined,
        bitmap: ?Bitmap = null,
        id: [32]u8 = undefined,
        used: u64 = 0,
        byte_len: u64 = 0,
    };
    /// Stable cache owner; callers may use a different scratch arena per seek.
    backing: ?A = null,
    files: [4]FileSlot = @splat(.{}),
    slots: [4]Slot = @splat(.{}),
    clock: u64 = 0,
    lineage_reads: u64 = 128 * 1024 * 1024,
    lineage_points: u32 = 0,
    lineage_pages: corpus.source_spans.Paged.PageCache = .{},
    identity_shared: ?decoded.Owned(Identities) = null,
    identity_owner: [32]u8 = @splat(0),
    eager_identity: ?Identities = null,
    pub fn deinit(self: *@This(), a: A) void {
        self.lineage_pages.deinit();
        if (self.identity_shared) |owned| owned.release();
        for (&self.files) |*entry| entry.clear();
        for (&self.slots) |*slot| if (slot.buffer) |buffer| (self.backing orelse a).free(buffer);
        self.* = .{};
    }
    fn file(self: *@This(), store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, ref: local.serverless_manifest_artifact_ref.ArtifactRef) !*const FileSlot {
        try cached.context.ensureActive();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&cached.scope);
        hash.update(ref.artifact_id);
        hash.update(ref.checksum);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        self.clock += 1;
        var victim: *FileSlot = &self.files[0];
        for (&self.files) |*slot| {
            if (slot.group != null and std.mem.eql(u8, &slot.digest, &digest)) {
                slot.used = self.clock;
                return slot;
            }
            if (slot.used < victim.used) victim = slot;
        }
        victim.clear();
        const owned = try decoded.acquire(corpus.FileGroup, cached, store, ref, .none, corpus.loadCachedFile);
        errdefer owned.release();
        const count = try corpus.physical.validate(owned.value.rows);
        victim.* = .{ .digest = digest, .group = owned.value.*, .row_count = count, .shared = owned, .used = self.clock };
        return victim;
    }
    fn load(self: *@This(), a: A, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, block: corpus.physical.Block) !*const Bitmap {
        const ref = block.bitmap orelse return error.InvalidNativeLakeTextCorpus;
        try @import("../serverless/artifacts/store.zig").validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
        if (ref.byte_len == 0 or ref.byte_len > 256 * 1024) return error.InvalidNativeLakeTextCorpus;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(ref.artifact_id, &digest, .{});
        self.clock += 1;
        var victim: *Slot = &self.slots[0];
        for (&self.slots) |*slot| {
            if (slot.bitmap != null and std.mem.eql(u8, &slot.id, &digest)) {
                if (slot.byte_len != ref.byte_len or slot.bitmap.?.cardinality() != block.count) return error.InvalidNativeLakeTextCorpus;
                slot.used = self.clock;
                return &slot.bitmap.?;
            }
            if (slot.used < victim.used) victim = slot;
        }
        if (victim.buffer == null) {
            const backing = self.backing orelse a;
            victim.buffer = try backing.alloc(u8, slot_bytes);
            self.backing = backing;
        }
        victim.fixed = .init(victim.buffer.?);
        victim.bitmap = null;
        victim.used = 0;
        const ca = victim.fixed.allocator();
        var lease = try @import("lake_index_aggregate_artifact.zig").readArtifactLease(ca, store, ref, .none, cached);
        defer lease.deinit();
        var bitmap = try Bitmap.fromBytes(ca, lease.bytes());
        if (bitmap.cardinality() != block.count or bitmap.rank(1 << corpus.physical.shift) != block.count) return error.InvalidNativeLakeTextCorpus;
        try bitmap.prepareRead();
        victim.bitmap = bitmap;
        victim.id = digest;
        victim.byte_len = ref.byte_len;
        victim.used = self.clock;
        return &victim.bitmap.?;
    }
};

/// Version 8 maps compressed physical rows directly to pinned native ordinals.
/// Directory storage scales with files/groups/blocks and compressed delete holes.
pub const Identities = struct {
    const Span = struct {
        lower: u32,
        upper: u32,
        rows: []const corpus.physical.Block,
        intervals: []const Mapping = &.{},
        manifest: ?local.serverless_manifest_artifact_ref.ArtifactRef = null,
        expected_rows: ?u32 = null,
        const Mapping = struct { file_lower: u32, lower: u32, upper: u32 };
        fn localOrdinal(self: @This(), ordinal_value: u32) ?u32 {
            if (self.intervals.len == 0) return if (ordinal_value < self.upper - self.lower) self.lower + ordinal_value else null;
            var lo: usize = 0;
            var hi = self.intervals.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const interval = self.intervals[mid];
                if (@as(u64, interval.file_lower) + interval.upper - interval.lower <= ordinal_value) lo = mid + 1 else hi = mid;
            }
            if (lo == self.intervals.len or ordinal_value < self.intervals[lo].file_lower) return null;
            return self.intervals[lo].lower + ordinal_value - self.intervals[lo].file_lower;
        }
        fn original(self: @This(), ordinal_value: u32) !u32 {
            if (ordinal_value < self.lower or ordinal_value >= self.upper) return error.InvalidNativeLakeTextCorpus;
            if (self.intervals.len == 0) return ordinal_value - self.lower;
            var lo: usize = 0;
            var hi = self.intervals.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.intervals[mid].upper <= ordinal_value) lo = mid + 1 else hi = mid;
            }
            if (lo == self.intervals.len) return error.InvalidNativeLakeTextCorpus;
            const interval = self.intervals[lo];
            return interval.file_lower + ordinal_value - interval.lower;
        }
        fn addRange(self: @This(), result: *Bitmap, lower: u32, upper: u64) !void {
            if (self.intervals.len == 0) return result.addRange(try std.math.add(u32, self.lower, lower), @as(u64, self.lower) + upper);
            for (self.intervals) |interval| {
                const start = @max(lower, interval.file_lower);
                const end = @min(upper, @as(u64, interval.file_lower) + interval.upper - interval.lower);
                if (start < end) try result.addRange(interval.lower + start - interval.file_lower, @as(u64, interval.lower) + end - interval.file_lower);
            }
        }
        fn addBitmap(self: @This(), a: A, result: *Bitmap, bitmap: *const Bitmap) !void {
            if (self.intervals.len == 0) {
                var shifted = try bitmap.addOffset(self.lower);
                defer shifted.deinit();
                return result.orWith(&shifted);
            }
            for (self.intervals) |interval| {
                var ranks = try bitmap.sliceRebased(a, interval.file_lower, @as(u64, interval.file_lower) + interval.upper - interval.lower);
                defer ranks.deinit();
                var shifted = try ranks.addOffset(interval.lower);
                defer shifted.deinit();
                try result.orWith(&shifted);
            }
        }
        /// Invert only one physical block at a time, including sparse segment
        /// intervals. A broad file must not create a file-sized bitmap copy.
        fn fileRanks(self: @This(), a: A, selected: *const Bitmap, lower: u32, upper: u64) !Bitmap {
            if (self.intervals.len == 0) return selected.sliceRebased(a, try std.math.add(u32, self.lower, lower), @as(u64, self.lower) + upper);
            var result = Bitmap.init(a);
            errdefer result.deinit();
            var lo: usize = 0;
            var hi = self.intervals.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const interval = self.intervals[mid];
                if (@as(u64, interval.file_lower) + interval.upper - interval.lower <= lower) lo = mid + 1 else hi = mid;
            }
            for (self.intervals[lo..]) |interval| {
                if (interval.file_lower >= upper) break;
                const start = @max(lower, interval.file_lower);
                const end = @min(upper, @as(u64, interval.file_lower) + interval.upper - interval.lower);
                if (start >= end) continue;
                var ranks = try selected.sliceRebased(a, interval.lower + start - interval.file_lower, @as(u64, interval.lower) + end - interval.file_lower);
                defer ranks.deinit();
                var shifted = try ranks.addOffset(start - lower);
                defer shifted.deinit();
                try result.orWith(&shifted);
            }
            return result;
        }
        fn blocks(self: @This(), cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, file_id: []const u8) ![]const corpus.physical.Block {
            const ref = self.manifest orelse return self.rows;
            const held = try cache.file(store, cached, ref);
            const group = held.group.?;
            if (!std.mem.eql(u8, group.file.id, file_id) or held.row_count != (self.expected_rows orelse self.upper - self.lower)) return error.InvalidNativeLakeTextCorpus;
            return group.rows;
        }
    };
    const FileSpan = struct { file: []const u8, span: Span };
    const CompactSpan = struct { source: u32, mapping: Span.Mapping };
    const Deferred = struct { groups: []const corpus.FileGroup, domain: [32]u8, key: [32]u8 };
    deferred: ?*const Deferred = null,
    row_resolver: ?local.serverless_query_lake_serving.ServingSource.RowResolver = null,
    partial_inventory: bool = false,
    files: std.StringHashMapUnmanaged(Span) = .empty,
    spans: []const FileSpan = &.{},
    compact_sources: []const FileSpan = &.{},
    compact_spans: []const CompactSpan = &.{},
    offsets: []const u32,
    segment_ids: []const [32]u8 = &.{},
    tie_rank: ?local.search_scorer.TieRank = null,
    pub fn spanCount(self: Identities) usize {
        if (self.deferred) |value| {
            var count: usize = 0;
            for (value.groups) |group| count += group.sourceLen();
            return count;
        }
        return if (self.compact_spans.len != 0) self.compact_spans.len else self.spans.len;
    }
    fn spanAt(self: Identities, index: usize) FileSpan {
        if (self.compact_spans.len == 0) return self.spans[index];
        const range = &self.compact_spans[index];
        var result = self.compact_sources[range.source];
        result.span.lower = range.mapping.lower;
        result.span.upper = range.mapping.upper;
        result.span.intervals = (&range.mapping)[0..1];
        return result;
    }
    pub fn init(a: A, root: corpus.Root, snapshot: *const local.index.IndexSnapshot) !Identities {
        if (root.version != corpus.metadata_version or (root.file_groups.len == 0 and snapshot.segments.len != 0)) return error.InvalidNativeLakeTextCorpus;
        const offsets = try a.alloc(u32, snapshot.segments.len + 1);
        offsets[0] = 0;
        for (snapshot.segments, 0..) |segment, i| offsets[i + 1] = try std.math.add(u32, offsets[i], segment.reader.doc_count);
        const lazy = for (root.file_groups) |group| {
            if (group.source_paged != null) break true;
        } else false;
        if (lazy) {
            // Serving packs own one segment each. Mixed/legacy coordinates stay
            // on the fully validated mapping path.
            for (root.file_groups) |group| if (group.sourceLen() == 0 or group.segments.len != 1 or group.masked) return error.InvalidNativeLakeTextCorpus;
            if (root.file_groups.len != snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
            const deferred = try a.create(Deferred);
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update(&root.domain);
            for (root.file_groups) |group| {
                for (group.segments) |ref| hash.update(ref.artifact_id);
                for (group.source_pages) |ref| hash.update(ref.artifact_id);
            }
            for (root.manifests) |ref| hash.update(ref.artifact_id);
            var key: [32]u8 = undefined;
            hash.final(&key);
            // Admission storage is transient. Retain only deeply owned lineage
            // descriptors, never its Root slices or an acquired snapshot pointer.
            const groups = try a.alloc(corpus.FileGroup, root.file_groups.len);
            for (groups, root.file_groups) |*copy, group| copy.* = .{
                .file = .{ .id = try a.dupe(u8, group.file.id), .digest = group.file.digest },
                .segments = try corpus.source_spans.cloneRefsIntoArena(a, group.segments),
                .sources = try corpus.source_spans.cloneIntoArena(a, group.sources),
                .source_pages = try corpus.source_spans.cloneRefsIntoArena(a, group.source_pages),
                .source_count = group.source_count,
                .source_view = if (group.source_view) |view| try view.cloneIntoArena(a) else null,
                .source_paged = if (group.source_paged) |paged| try paged.cloneIntoArena(a) else null,
            };
            deferred.* = .{ .groups = groups, .domain = root.domain, .key = key };
            return .{ .offsets = offsets, .partial_inventory = root.partial_inventory, .deferred = deferred };
        }
        for (root.file_groups) |group| if (group.sourceLen() != 0) return initPacked(a, root.domain, root.file_groups, root.partial_inventory, offsets);
        var result: Identities = .{ .offsets = offsets, .partial_inventory = root.partial_inventory };
        const spans = try a.alloc(FileSpan, root.file_groups.len);
        var segment: usize = 0;
        for (root.file_groups, 0..) |group, group_index| {
            const start = segment;
            segment += group.segments.len;
            if (segment > snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
            const entry = try result.files.getOrPut(a, try a.dupe(u8, group.file.id));
            if (entry.found_existing) return error.InvalidNativeLakeTextCorpus;
            if (group.selected.len == 0 and root.routing.len == 0 and try corpus.physical.validate(group.rows) != offsets[segment] - offsets[start]) return error.InvalidNativeLakeTextCorpus;
            const physical_rows = try a.dupe(corpus.physical.Block, group.rows);
            for (physical_rows) |*block| if (block.bitmap) |*ref| {
                ref.artifact_id = try a.dupe(u8, ref.artifact_id);
                ref.checksum = try a.dupe(u8, ref.checksum);
            };
            entry.value_ptr.* = .{ .lower = offsets[start], .upper = offsets[segment], .rows = physical_rows };
            if (group.selected.len != 0) {
                if (group.selected.len != group.segments.len) return error.InvalidNativeLakeTextCorpus;
                const intervals = try a.alloc(Span.Mapping, group.selected.len);
                for (intervals, group.selected, 0..) |*interval, source, index| {
                    if (source.count != offsets[start + index + 1] - offsets[start + index]) return error.InvalidNativeLakeTextCorpus;
                    interval.* = .{ .file_lower = source.lower, .lower = offsets[start + index], .upper = offsets[start + index + 1] };
                }
                entry.value_ptr.intervals = intervals;
            } else if (root.routing.len != 0) {
                const ref = group.physical_manifest orelse root.manifests[group_index];
                entry.value_ptr.manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = try a.dupe(u8, ref.artifact_id), .checksum = try a.dupe(u8, ref.checksum), .byte_len = ref.byte_len };
            }
            spans[group_index] = .{ .file = entry.key_ptr.*, .span = entry.value_ptr.* };
        }
        if (segment != snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
        result.spans = spans;
        return result;
    }
    const TieContext = struct {
        identities: Identities,
        file_ranks: []const u32,
        file_count: usize,
        archive_docs: u32,
        fn rank(raw: *const anyopaque, doc: u32) u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (doc >= self.archive_docs) return (@as(u64, self.file_count) << 32) | (doc - self.archive_docs);
            var lo: usize = 0;
            var hi = self.identities.spanCount();
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.identities.spanAt(mid).span.upper <= doc) lo = mid + 1 else hi = mid;
            }
            if (lo == self.identities.spanCount()) return std.math.maxInt(u64);
            const item = self.identities.spanAt(lo);
            return (@as(u64, self.file_ranks[lo]) << 32) | (item.span.original(doc) catch return std.math.maxInt(u64));
        }
    };
    fn initPacked(a: A, domain: [32]u8, groups: []const corpus.FileGroup, partial_inventory: bool, offsets: []const u32) !Identities {
        var result: Identities = .{ .offsets = offsets, .partial_inventory = partial_inventory };
        const Mapping = Span.Mapping;
        const FileMappings = struct { index: u32, intervals: std.ArrayList(Mapping) = .empty };
        var mappings: std.StringHashMapUnmanaged(FileMappings) = .empty;
        defer {
            var it = mappings.valueIterator();
            while (it.next()) |value| value.intervals.deinit(std.heap.page_allocator);
            mappings.deinit(std.heap.page_allocator);
        }
        var sources: std.ArrayList(FileSpan) = .empty;
        var spans: std.ArrayList(CompactSpan) = .empty;
        var segment: usize = 0;
        for (groups) |group| {
            if (group.sourceLen() == 0 or group.segments.len != 1 or segment >= offsets.len - 1) return error.InvalidNativeLakeTextCorpus;
            if (group.source_view) |view| try view.validateMode(offsets[segment + 1] - offsets[segment], domain, group.masked) else try corpus.source_spans.validateMode(group.sources, offsets[segment + 1] - offsets[segment], domain, group.masked);
            for (0..group.sourceLen()) |index| {
                const src = group.sourceAt(index);
                const lower = try std.math.add(u32, offsets[segment], src.pack_lower);
                const upper = try std.math.add(u32, lower, src.count);
                const list = try mappings.getOrPut(std.heap.page_allocator, src.file.id);
                // The temporary map is unwound even if owning path allocation
                // fails below. Never leave a newly inserted list undefined.
                if (!list.found_existing) list.value_ptr.* = .{ .index = 0 };
                const entry = try result.files.getOrPut(a, src.file.id);
                if (!list.found_existing) {
                    if (entry.found_existing) return error.InvalidNativeLakeTextCorpus;
                    const name = try a.dupe(u8, src.file.id);
                    entry.key_ptr.* = name;
                    const ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = try a.dupe(u8, src.physical.artifact_id), .checksum = try a.dupe(u8, src.physical.checksum), .byte_len = src.physical.byte_len };
                    entry.value_ptr.* = .{ .lower = lower, .upper = upper, .rows = &.{}, .manifest = ref, .expected_rows = src.file_rows };
                    list.value_ptr.* = .{ .index = @intCast(sources.items.len) };
                    try sources.append(a, .{ .file = name, .span = entry.value_ptr.* });
                } else {
                    if (!entry.found_existing) return error.InvalidNativeLakeTextCorpus;
                    const old = entry.value_ptr;
                    if (old.expected_rows != src.file_rows or old.manifest.?.byte_len != src.physical.byte_len or !std.mem.eql(u8, old.manifest.?.artifact_id, src.physical.artifact_id) or !std.mem.eql(u8, old.manifest.?.checksum, src.physical.checksum)) return error.InvalidNativeLakeTextCorpus;
                    old.lower = @min(old.lower, lower);
                    old.upper = @max(old.upper, upper);
                }
                const mapping: Mapping = .{ .file_lower = src.file_lower, .lower = lower, .upper = upper };
                try spans.append(a, .{ .source = list.value_ptr.index, .mapping = mapping });
                try list.value_ptr.intervals.append(std.heap.page_allocator, mapping);
            }
            segment += 1;
        }
        if (segment != offsets.len - 1) return error.InvalidNativeLakeTextCorpus;
        var it = mappings.iterator();
        while (it.next()) |entry| {
            const intervals = entry.value_ptr.intervals.items;
            std.mem.sort(Mapping, intervals, {}, struct {
                fn less(_: void, l: Mapping, r: Mapping) bool {
                    return l.file_lower < r.file_lower;
                }
            }.less);
            for (intervals, 0..) |m, i| if (i != 0 and @as(u64, intervals[i - 1].file_lower) + intervals[i - 1].upper - intervals[i - 1].lower > m.file_lower) return error.InvalidNativeLakeTextCorpus;
            result.files.getPtr(entry.key_ptr.*).?.intervals = try a.dupe(Mapping, intervals);
        }
        result.compact_sources = try sources.toOwnedSlice(a);
        result.compact_spans = try spans.toOwnedSlice(a);
        const hashes = try a.alloc([32]u8, result.files.count());
        var keys = result.files.keyIterator();
        for (hashes) |*hash| hash.* = corpus.term_directory.identity(keys.next().?.*);
        std.mem.sort([32]u8, hashes, {}, struct {
            fn less(_: void, l: [32]u8, r: [32]u8) bool {
                return std.mem.order(u8, &l, &r) == .lt;
            }
        }.less);
        defer a.free(hashes);
        const tie = try a.create(TieContext);
        const ranks = try a.alloc(u32, result.spanCount());
        for (ranks, 0..) |*rank, i| {
            const id = corpus.term_directory.identity(result.spanAt(i).file);
            var lower: usize = 0;
            var upper = hashes.len;
            while (lower < upper) {
                const middle = lower + (upper - lower) / 2;
                if (std.mem.order(u8, &hashes[middle], &id) == .lt) lower = middle + 1 else upper = middle;
            }
            rank.* = @intCast(lower);
        }
        tie.* = .{ .identities = result, .file_ranks = ranks, .file_count = result.files.count(), .archive_docs = offsets[segment] };
        result.tie_rank = .{ .ptr = tie, .rank = TieContext.rank };
        return result;
    }
    /// Stable logical ranking position shares predicate/hydration translation.
    fn materialize(self: Identities, a: A, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, reads: *u64) anyerror!Identities {
        const deferred = self.deferred orelse return self;
        const groups = try a.dupe(corpus.FileGroup, deferred.groups);
        for (groups) |*group| if (group.source_paged != null) {
            group.source_view = try corpus.source_spans.loadView(a, store, group.source_pages, group.source_count, deferred.domain, corpus.source_spans.Paged.queryToken(&cached.context), cached, reads, false);
            group.source_paged = null;
        };
        var result = try initPacked(a, deferred.domain, groups, self.partial_inventory, try a.dupe(u32, self.offsets));
        result.row_resolver = self.row_resolver;
        result.segment_ids = try a.dupe([32]u8, self.segment_ids);
        return result;
    }
    fn eager(self: Identities, a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead) anyerror!Identities {
        _ = a;
        const deferred = self.deferred orelse return self;
        if (cache.eager_identity == null or !std.mem.eql(u8, &cache.identity_owner, &deferred.key)) {
            cache.eager_identity = null;
            cache.lineage_pages.deinit();
            if (cache.identity_shared) |owned| owned.release();
            cache.identity_shared = null;
            var bytes: u64 = 0;
            for (deferred.groups) |group| for (group.source_pages) |ref| {
                bytes = try std.math.add(u64, bytes, ref.byte_len);
            };
            try @import("../serverless/artifacts/store.zig").chargeReadBudget(&cache.lineage_reads, bytes);
            const anchor = for (deferred.groups) |group| {
                if (group.source_paged != null) break group.source_pages[0];
            } else return error.InvalidNativeLakeTextCorpus;
            const Load = struct {
                fn read(out: A, backend: @import("../serverless/artifacts/store.zig").ArtifactStore, _: local.serverless_manifest_artifact_ref.ArtifactRef, _: @import("antfly_cancellation").CancellationToken, access: @import("lake_index_aggregate_artifact.zig").CachedRead, input: Identities) anyerror!Identities {
                    var reads: u64 = 128 * 1024 * 1024;
                    return input.materialize(out, backend, access, &reads);
                }
            };
            // Shared decoded maps own all strings/arrays and retain no query
            // resolver, snapshot pointer or request capability. Every lease is
            // checked with the caller's current authority and publication key.
            var input = self;
            input.row_resolver = null;
            cache.identity_shared = try @import("lake_index_decoded_metadata.zig").acquireInterpretedBounded(Identities, cached, store, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = anchor.artifact_id, .checksum = anchor.checksum, .byte_len = anchor.byte_len }, corpus.source_spans.Paged.queryToken(&cached.context), &("native-paged-lineage-inverse-v1".* ++ deferred.key), input, Load.read, 32 * 1024 * 1024, null);
            cache.eager_identity = cache.identity_shared.?.value.*;
            cache.identity_owner = deferred.key;
        }
        var result = cache.eager_identity.?;
        result.partial_inventory = self.partial_inventory;
        result.row_resolver = self.row_resolver;
        result.segment_ids = self.segment_ids;
        return result;
    }
    fn deferredSpan(self: Identities, a: A, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, page_cache: *corpus.source_spans.Paged.PageCache, doc: u32, reads: *u64) anyerror!struct { span: corpus.source_spans.Span, local: u32 } {
        const groups = self.deferred.?.groups;
        var lower: usize = 0;
        var upper = groups.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            if (self.offsets[middle + 1] <= doc) lower = middle + 1 else upper = middle;
        }
        if (lower == groups.len) return error.InvalidNativeLakeTextCorpus;
        const group = groups[lower];
        const ordinal_value = doc - self.offsets[lower];
        if (group.source_paged) |paged| return .{ .span = try paged.spanCached(a, store, cached, page_cache, ordinal_value, reads), .local = ordinal_value };
        var lo: usize = 0;
        var hi = group.sourceLen();
        while (lo < hi) {
            const middle = lo + (hi - lo) / 2;
            if (@as(u64, group.sourceAt(middle).pack_lower) + group.sourceAt(middle).count <= ordinal_value) lo = middle + 1 else hi = middle;
        }
        if (lo == group.sourceLen()) return error.InvalidNativeLakeTextCorpus;
        return .{ .span = group.sourceAt(lo), .local = ordinal_value };
    }
    /// Admission and physical packing must never define the public tie order.
    pub fn logicalPosition(self: Identities, ordinal_value: u32) !@import("lake_index_text_directory.zig").Position {
        var lo: usize = 0;
        var hi = self.spanCount();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.spanAt(mid).span.upper <= ordinal_value) lo = mid + 1 else hi = mid;
        }
        if (lo == self.spanCount()) return error.InvalidNativeLakeTextCorpus;
        const item = self.spanAt(lo);
        return .{ .file = corpus.term_directory.identity(item.file), .rank = try item.span.original(ordinal_value) };
    }
    /// Translate a query-wide compressed physical tombstone set into just the
    /// admitted native window. Keep each temporary bitmap within one 2^20 block.
    pub fn bitmapForPhysical(self: Identities, a: A, selected: *const @import("lake_index_physical_set.zig").Set, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead) anyerror!Bitmap {
        if (self.deferred != null) {
            var cache: LiveRowsCache = .{ .backing = a };
            defer cache.deinit(a);
            return (try self.eager(a, &cache, store, cached)).bitmapForPhysical(a, selected, store, cached);
        }
        if (self.row_resolver) |resolver| {
            var window_rows: usize = 0;
            for (0..self.spanCount()) |i| {
                const item = self.spanAt(i);
                window_rows +|= item.span.upper - item.span.lower;
            }
            if (window_rows != 0 and selected.boundedCardinality(window_rows) == null)
                return self.bitmapForCurrentCandidates(a, selected, store, cached, resolver);
            var mapped = try indexSelectionCut(a, resolver.current_source, resolver.current_snapshot, resolver, selected);
            defer mapped.deinit();
            var direct = self;
            direct.row_resolver = null;
            // Run the native compressed block/rank kernel in the selected
            // index's coordinate space after bounded batch translation.
            return direct.bitmapForPhysical(a, &mapped, store, cached);
        }
        var result = Bitmap.init(a);
        errdefer result.deinit();
        var cache: LiveRowsCache = .{ .backing = a };
        defer cache.deinit(a);
        var file_it = self.files.iterator();
        while (file_it.next()) |entry| {
            const item: FileSpan = .{ .file = entry.key_ptr.*, .span = entry.value_ptr.* };
            const selected_blocks = selected.files.getPtr(item.file) orelse continue;
            for (try item.span.blocks(&cache, store, cached, item.file)) |block| {
                try cached.context.ensureActive();
                const physical_base = block.high << corpus.physical.shift;
                const bitmap = selected_blocks.getPtr(.{ .group = block.group, .high = @intCast(physical_base >> 32) }) orelse continue;
                const lower: u32 = @truncate(physical_base);
                var sliced = try bitmap.sliceRebased(a, lower, @as(u64, lower) + (1 << corpus.physical.shift));
                defer sliced.deinit();
                if (!sliced.isEmpty()) try self.addBlock(a, a, &cache, store, cached, .{ .file = item.file, .group = block.group, .base = physical_base, .selection = .{ .bitmap = sliced } }, &result);
            }
        }
        return result;
    }
    /// A broad mask is probed from this window's candidates. Each archive row
    /// is translated at most once across disjoint windows; the compressed
    /// query-wide mask never expands into a second archive-sized selection.
    fn bitmapForCurrentCandidates(self: Identities, a: A, selected: *const @import("lake_index_physical_set.zig").Set, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, resolver: local.serverless_query_lake_serving.ServingSource.RowResolver) !Bitmap {
        var result = Bitmap.init(a);
        errdefer result.deinit();
        var direct = self;
        direct.row_resolver = null;
        var live: LiveRowsCache = .{ .backing = a };
        defer live.deinit(a);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var refs: [256]local.storage_rowsource_types.RowRef = undefined;
        var docs: [256]u32 = undefined;
        for (0..self.spanCount()) |i| {
            const item = self.spanAt(i);
            var begin = item.span.lower;
            while (begin < item.span.upper) {
                try cached.context.ensureActive();
                _ = scratch.reset(.retain_capacity);
                const count = @min(refs.len, item.span.upper - begin);
                for (refs[0..count], docs[0..count], begin..) |*ref, *doc, ordinal_| {
                    doc.* = @intCast(ordinal_);
                    ref.* = try direct.physicalRow(scratch.allocator(), &live, store, cached, doc.*, resolver.source, resolver.snapshot);
                }
                const mapped = if (resolver.translate_many) |translate| try translate(resolver.ptr, scratch.allocator(), refs[0..count], true) else blk: {
                    const translated = try scratch.allocator().alloc(local.storage_rowsource_types.RowRef, count);
                    for (translated, refs[0..count]) |*row, ref| row.* = try resolver.from_index(resolver.ptr, scratch.allocator(), ref);
                    break :blk translated;
                };
                for (mapped, docs[0..count]) |ref, doc| if (selected.contains(ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) try result.add(doc);
                begin += @intCast(count);
            }
        }
        return result;
    }
    fn liveRows(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, block: corpus.physical.Block) !*const Bitmap {
        return cache.load(a, store, cached, block);
    }
    fn addBlock(self: Identities, a: A, temporary: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, selected: @import("lake_index_predicate_blocks.zig").Block, result: *Bitmap) !void {
        if (self.row_resolver != null) {
            // An ordered index may be freshly rebuilt while this text index
            // is retained. Its compressed blocks still describe current rows.
            var physical = @import("lake_index_physical_set.zig").Set.init(temporary);
            defer physical.deinit();
            try physical.addBlock(selected);
            var translated = try self.bitmapForPhysical(temporary, &physical, store, cached);
            defer translated.deinit();
            try result.orWith(&translated);
            return;
        }
        const file = self.files.get(selected.file) orelse {
            if (self.partial_inventory) return;
            return error.ExternalLakeSnapshotMismatch;
        };
        const blocks = try file.blocks(cache, store, cached, selected.file);
        const index = corpus.physical.find(blocks, selected.group, selected.base) orelse return;
        const block = blocks[index];
        const base = block.base;
        switch (selected.selection) {
            .interval => |span| {
                if (block.bitmap != null) {
                    const live = try liveRows(a, cache, store, cached, block);
                    const lower: u32 = @intCast(live.rank(span.lower));
                    const upper: u32 = @intCast(live.rank(span.lower + span.count));
                    try file.addRange(result, try std.math.add(u32, base, lower), @as(u64, base) + upper);
                } else {
                    const lower = @max(span.lower, block.lower);
                    const upper = @min(span.lower + span.count, block.lower + block.count);
                    if (upper > lower) try file.addRange(result, try std.math.add(u32, base, lower - block.lower), @as(u64, base) + upper - block.lower);
                }
            },
            .bitmap => |bitmap| {
                var intersection = try bitmap.clone(temporary);
                defer intersection.deinit();
                if (block.bitmap != null) {
                    const live = try liveRows(a, cache, store, cached, block);
                    intersection.andWith(live);
                    var iterator = intersection.iterator();
                    while (iterator.next()) |row| if (file.localOrdinal(try std.math.add(u32, base, @intCast(live.rank(row))))) |ordinal_value| try result.add(ordinal_value);
                } else {
                    var extent = Bitmap.init(temporary);
                    defer extent.deinit();
                    try extent.addRange(block.lower, @as(u64, block.lower) + block.count);
                    intersection.andWith(&extent);
                    var shifted = try intersection.addOffset(base -% block.lower);
                    defer shifted.deinit();
                    try file.addBitmap(temporary, result, &shifted);
                }
            },
        }
    }
    pub fn ordinal(self: Identities, a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, row: local.storage_rowsource_types.RowRef) anyerror!?u32 {
        if (self.deferred != null) return (try self.eager(a, cache, store, cached)).ordinal(a, cache, store, cached, row);
        if (row != .external) return error.InvalidNativeLakeRowIndex;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const mapped = if (self.row_resolver) |resolver| try resolver.to_index(resolver.ptr, scratch.allocator(), row) else row;
        const ref = mapped.external;
        const span = self.files.get(ref.file_id) orelse {
            if (self.partial_inventory) return null;
            return error.ExternalLakeSnapshotMismatch;
        };
        return span.localOrdinal((try sourceRank(a, cache, store, cached, span, mapped)) orelse return null);
    }
    pub fn sourceOrdinal(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, physical: @import("lake_index_aggregate_artifact.zig").ChunkRef, count: u32, row: local.storage_rowsource_types.RowRef) !?u32 {
        const span: Span = .{ .lower = 0, .upper = count, .rows = &.{}, .manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = physical.artifact_id, .checksum = physical.checksum, .byte_len = physical.byte_len }, .expected_rows = count };
        return sourceRank(a, cache, store, cached, span, row);
    }
    pub const SourceBounds = struct { lower: u32, upper: u32 };
    /// Conservative rank bounds from compressed physical selections. Deleted
    /// endpoints can widen a proof but can never exclude a matching live row.
    pub fn sourceBounds(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, physical: @import("lake_index_aggregate_artifact.zig").ChunkRef, count: u32, selected: *const PhysicalSet, file_id: []const u8) !?SourceBounds {
        const span: Span = .{ .lower = 0, .upper = count, .rows = &.{}, .manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = physical.artifact_id, .checksum = physical.checksum, .byte_len = physical.byte_len }, .expected_rows = count };
        const chosen = selected.files.getPtr(file_id) orelse return null;
        var bounds: ?SourceBounds = null;
        const blocks = try span.blocks(cache, store, cached, file_id);
        var selections = chosen.iterator();
        while (selections.next()) |selection| {
            const key = selection.key_ptr.*;
            const first_high = @as(u64, key.high) << (32 - corpus.physical.shift);
            const after_high = (@as(u64, key.high) + 1) << (32 - corpus.physical.shift);
            var lower: usize = 0;
            var upper = blocks.len;
            while (lower < upper) {
                const mid = lower + (upper - lower) / 2;
                if (blocks[mid].group < key.group or (blocks[mid].group == key.group and blocks[mid].high < first_high)) lower = mid + 1 else upper = mid;
            }
            for (blocks[lower..]) |block| {
                if (block.group != key.group or block.high >= after_high) break;
                try cached.context.ensureActive();
                const base = block.high << corpus.physical.shift;
                const bitmap = selection.value_ptr;
                const offset: u32 = @truncate(base);
                try bitmap.prepareRead();
                const first = bitmap.rank(offset + block.lower);
                const last = bitmap.rank(@intCast(@as(u64, offset) + (1 << corpus.physical.shift) - 1)) + @intFromBool(bitmap.contains(@intCast(@as(u64, offset) + (1 << corpus.physical.shift) - 1)));
                if (first == last) continue;
                const low = bitmap.read_rank.?.select(first).? - offset;
                const high = bitmap.read_rank.?.select(last - 1).? - offset;
                const ranks: SourceBounds = if (block.bitmap != null) deleted: {
                    const live = try liveRows(a, cache, store, cached, block);
                    break :deleted .{ .lower = block.base + @as(u32, @intCast(live.rank(low))), .upper = block.base + @as(u32, @intCast(live.rank(high + 1))) };
                } else .{ .lower = block.base + @min(block.count, low - block.lower), .upper = block.base + @min(block.count, high - block.lower + 1) };
                if (ranks.lower == ranks.upper) continue;
                if (bounds) |*value| {
                    value.lower = @min(value.lower, ranks.lower);
                    value.upper = @max(value.upper, ranks.upper);
                } else bounds = ranks;
            }
        }
        return bounds;
    }
    /// Test a pack span using rank/select and bitmap range cardinality. Work is
    /// proportional to immutable blocks/spans, never to matching row count.
    pub fn sourceRangeSelected(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, physical: @import("lake_index_aggregate_artifact.zig").ChunkRef, count: u32, selected: *const PhysicalSet, file_id: []const u8, lower: u32, length: u32, work: *usize) !bool {
        const span: Span = .{ .lower = 0, .upper = count, .rows = &.{}, .manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = physical.artifact_id, .checksum = physical.checksum, .byte_len = physical.byte_len }, .expected_rows = count };
        const chosen = selected.files.getPtr(file_id) orelse return false;
        const upper = @as(u64, lower) + length;
        const blocks = try span.blocks(cache, store, cached, file_id);
        var first_block: usize = 0;
        var end_block = blocks.len;
        while (first_block < end_block) {
            const mid = first_block + (end_block - first_block) / 2;
            if (@as(u64, blocks[mid].base) + blocks[mid].count <= lower) first_block = mid + 1 else end_block = mid;
        }
        for (blocks[first_block..]) |block| {
            try cached.context.ensureActive();
            if (work.* == 65536) return error.QueryCandidateBudgetExceeded;
            work.* += 1;
            if (@as(u64, block.base) + block.count <= lower) continue;
            if (block.base >= upper) break;
            const base = block.high << corpus.physical.shift;
            const bitmap = chosen.getPtr(.{ .group = block.group, .high = @intCast(base >> 32) }) orelse continue;
            const first = @max(lower, block.base) - block.base;
            const last: u32 = @intCast(@min(upper, @as(u64, block.base) + block.count) - block.base);
            const extent = if (block.bitmap != null) deleted: {
                const live = try liveRows(a, cache, store, cached, block);
                break :deleted SourceBounds{ .lower = live.read_rank.?.select(first).?, .upper = live.read_rank.?.select(last - 1).? + 1 };
            } else SourceBounds{ .lower = block.lower + first, .upper = block.lower + last };
            const offset: u32 = @truncate(base);
            if (bitmap.rangeCardinality(offset + extent.lower, @as(u64, offset) + extent.upper) == 0) continue;
            if (block.bitmap == null) return true;
            const live = try liveRows(a, cache, store, cached, block);
            var navigation = bitmap.iterator();
            var target: u64 = extent.lower;
            while (target < extent.upper) {
                try cached.context.ensureActive();
                if (work.* == 65536) return error.QueryCandidateBudgetExceeded;
                work.* += 1;
                const next = navigation.seekTo(offset + @as(u32, @intCast(target))) orelse break;
                target = next - offset;
                if (target >= extent.upper) break;
                // Physical blocks are aligned to whole words. Intersect the
                // two borrowed coordinate systems directly, without bitmap
                // copies or allocation inside a non-fallible set operation.
                const word = bitmap.wordMask(next) & live.wordMask(@intCast(target)) &
                    (@as(u64, std.math.maxInt(u64)) << @as(u6, @truncate(target)));
                if (word != 0 and (target & ~@as(u64, 63)) + @ctz(word) < extent.upper) return true;
                target = (target & ~@as(u64, 63)) + 64;
            }
        }
        return false;
    }
    fn sourceRank(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, span: Span, row: local.storage_rowsource_types.RowRef) !?u32 {
        if (row != .external) return error.InvalidNativeLakeRowIndex;
        const ref = row.external;
        const blocks = try span.blocks(cache, store, cached, ref.file_id);
        const index = corpus.physical.find(blocks, ref.row_group_ordinal, ref.row_ordinal) orelse return null;
        const block = blocks[index];
        const low: u32 = @intCast(ref.row_ordinal & ((1 << corpus.physical.shift) - 1));
        const rank: u32 = if (block.bitmap) |artifact| blk: {
            _ = artifact;
            const live = try liveRows(a, cache, store, cached, block);
            if (!live.contains(low)) return null;
            break :blk @intCast(live.rank(low));
        } else blk: {
            if (low < block.lower or low - block.lower >= block.count) return null;
            break :blk low - block.lower;
        };
        return try std.math.add(u32, block.base, rank);
    }
    /// Binary searches keep native-to-physical lookup logarithmic in files and
    /// physical blocks. Delete holes use the same authenticated rank directory.
    pub fn physicalRow(self: Identities, a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, ordinal_value: u32, source: []const u8, snapshot: []const u8) anyerror!local.storage_rowsource_types.RowRef {
        if (self.deferred) |deferred| {
            // Bound both CPU point work and newly admitted IO. Page reuse
            // cannot let an exhaustive fallback loop forever without switching
            // to the shared complete map; required-read reserve stays intact.
            if ((cache.eager_identity != null and std.mem.eql(u8, &cache.identity_owner, &deferred.key)) or cache.lineage_reads < 96 * 1024 * 1024 or cache.lineage_points >= 4096)
                return (try self.eager(a, cache, store, cached)).physicalRow(a, cache, store, cached, ordinal_value, source, snapshot);
            cache.lineage_points += 1;
            const located = try self.deferredSpan(cache.backing orelse local.sql_memory_budget.reclaimingAllocator(a), store, cached, &cache.lineage_pages, ordinal_value, &cache.lineage_reads);
            const span = located.span;
            if (located.local < span.pack_lower or located.local - span.pack_lower >= span.count) return error.InvalidNativeLakeTextCorpus;
            const rank = try std.math.add(u32, span.file_lower, located.local - span.pack_lower);
            const ref = try sourcePhysicalRow(a, cache, store, cached, span, rank, if (self.row_resolver) |resolver| resolver.source else source, if (self.row_resolver) |resolver| resolver.snapshot else snapshot);
            return if (self.row_resolver) |resolver| try resolver.from_index(resolver.ptr, a, ref) else ref;
        }
        var lo: usize = 0;
        var hi = self.spanCount();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.spanAt(mid).span.upper <= ordinal_value) lo = mid + 1 else hi = mid;
        }
        if (lo == self.spanCount()) return error.InvalidNativeLakeTextCorpus;
        const file = self.spanAt(lo);
        const blocks = try file.span.blocks(cache, store, cached, file.file);
        if (ordinal_value < file.span.lower) return error.InvalidNativeLakeTextCorpus;
        const relative = try file.span.original(ordinal_value);
        lo = 0;
        hi = blocks.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const block = blocks[mid];
            if (@as(u64, block.base) + block.count <= relative) lo = mid + 1 else hi = mid;
        }
        if (lo == blocks.len) return error.InvalidNativeLakeTextCorpus;
        const block = blocks[lo];
        if (relative < block.base) return error.InvalidNativeLakeTextCorpus;
        const rank = relative - block.base;
        const low = if (block.bitmap != null) blk: {
            const live = try liveRows(a, cache, store, cached, block);
            break :blk live.read_rank.?.select(rank) orelse return error.InvalidNativeLakeTextCorpus;
        } else try std.math.add(u32, block.lower, rank);
        const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = if (self.row_resolver) |resolver| resolver.source else source, .snapshot_id = if (self.row_resolver) |resolver| resolver.snapshot else snapshot, .file_id = file.file, .row_group_ordinal = block.group, .row_ordinal = (block.high << corpus.physical.shift) | low } };
        return if (self.row_resolver) |resolver| try resolver.from_index(resolver.ptr, a, ref) else ref;
    }
    /// Resolve one native candidate tile through a single relocation operation.
    pub fn physicalRows(self: Identities, a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, ordinals: []const u32, source: []const u8, snapshot: []const u8) ![]const local.storage_rowsource_types.RowRef {
        if (ordinals.len > 128) return error.InvalidNativeLakeTextCorpus;
        var original = self;
        original.row_resolver = null;
        const refs = try a.alloc(local.storage_rowsource_types.RowRef, ordinals.len);
        // Traverse a tile in native ordinal order, then restore caller order.
        // Nearby ordinals share the bounded leased range-page path, including
        // shuffled and duplicate requested rows across child scratch resets.
        var positions: [128]usize = undefined;
        for (positions[0..ordinals.len], 0..) |*position, i| position.* = i;
        if (self.deferred != null) std.mem.sort(usize, positions[0..ordinals.len], ordinals, struct {
            fn less(values: []const u32, left: usize, right: usize) bool {
                return values[left] < values[right];
            }
        }.less);
        for (positions[0..ordinals.len]) |i| refs[i] = try original.physicalRow(a, cache, store, cached, ordinals[i], if (self.row_resolver) |resolver| resolver.source else source, if (self.row_resolver) |resolver| resolver.snapshot else snapshot);
        if (self.row_resolver) |resolver| {
            if (resolver.translate_many) |translate| return translate(resolver.ptr, a, refs, true);
            for (refs) |*ref| ref.* = try resolver.from_index(resolver.ptr, a, ref.*);
        }
        return refs;
    }
    pub fn sourcePhysicalRow(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, span: corpus.source_spans.Span, ordinal_value: u32, source: []const u8, snapshot: []const u8) anyerror!local.storage_rowsource_types.RowRef {
        const item: FileSpan = .{ .file = span.file.id, .span = .{ .lower = 0, .upper = span.file_rows, .rows = &.{}, .manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = span.physical.artifact_id, .checksum = span.physical.checksum, .byte_len = span.physical.byte_len }, .expected_rows = span.file_rows } };
        const identities: Identities = .{ .offsets = &.{}, .spans = &.{item} };
        return identities.physicalRow(a, cache, store, cached, ordinal_value, source, snapshot);
    }
    /// Invert only selected ordinal blocks. Contiguous extents stay compressed;
    /// delete holes are resolved within their authenticated physical block.
    fn physicalSelection(self: Identities, a: A, selected: *const Bitmap, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead) !PhysicalSet {
        var result = PhysicalSet.init(a);
        errdefer result.deinit();
        var cache: LiveRowsCache = .{ .backing = a };
        defer cache.deinit(a);
        var files = self.files.iterator();
        while (files.next()) |file| {
            const span = file.value_ptr.*;
            if (selected.rangeCardinality(span.lower, span.upper) == 0) continue;
            for (try span.blocks(&cache, store, cached, file.key_ptr.*)) |block| {
                const lower = block.base;
                const upper = @as(u64, lower) + block.count;
                var ranks = try span.fileRanks(a, selected, lower, upper);
                defer ranks.deinit();
                if (ranks.cardinality() == 0) continue;
                var physical = if (block.bitmap != null) blk: {
                    const live = try liveRows(a, &cache, store, cached, block);
                    if (ranks.cardinality() == block.count) break :blk try live.clone(a);
                    var matches = Bitmap.init(a);
                    errdefer matches.deinit();
                    if (ranks.cardinality() < block.count / 8) {
                        var selected_ranks = ranks.iterator();
                        while (selected_ranks.next()) |rank| {
                            try matches.add(live.read_rank.?.select(rank) orelse return error.InvalidNativeLakeTextCorpus);
                        }
                    } else {
                        var rows_it = live.iterator();
                        var rank: u32 = 0;
                        while (rows_it.next()) |row| : (rank += 1) if (ranks.contains(rank)) {
                            try matches.add(row);
                        };
                    }
                    break :blk matches;
                } else try ranks.addOffset(block.lower);
                defer physical.deinit();
                try result.addBlock(.{ .file = file.key_ptr.*, .group = block.group, .base = block.high << corpus.physical.shift, .selection = .{ .bitmap = physical } });
            }
        }
        return result;
    }
};

pub const PhysicalSet = @import("lake_index_physical_set.zig").Set;
pub const Resolver = PredicateResolver(Bitmap);
pub const PhysicalResolver = PredicateResolver(PhysicalSet);
fn PredicateResolver(comptime Set: type) type {
    return struct {
        const Self = @This();
        const PredicateResult = struct { bitmap: Set, exact: bool = true, residual: ?Compiled = null };
        server: *@import("http_server.zig").ApiHttpServer,
        table: local.sql_catalog.Table,
        source: *local.serverless_query_lake_serving.ServingSource,
        context: local.api_operation.RequestContext,
        identities: Identities = .{ .offsets = &.{} },
        store: @import("../serverless/artifacts/store.zig").ArtifactStore,
        store_identity: [32]u8,
        read_context: local.serverless_query_lake_read_context.Context,
        pinned: ?rows.PredicateContext = null,
        allow_partial: bool = true,

        /// Query-owned exact physical membership, adaptive between point probes
        /// and the existing whole-predicate index/scan planner.
        pub const Membership = struct {
            a: A,
            resolver: Self,
            arena: std.heap.ArenaAllocator,
            json: []const u8,
            predicates: std.ArrayList(rows.Predicate) = .empty,
            complete: ?PhysicalSet = null,
            spent: u64 = 0,
            budget: u64 = 0,
            pub fn deinit(self: *@This()) void {
                self.closePredicates();
                if (self.complete) |*set| set.deinit();
                self.arena.deinit();
            }
            fn closePredicates(self: *@This()) void {
                for (self.predicates.items) |*predicate| predicate.deinit();
                self.predicates.clearRetainingCapacity();
            }
            /// Reopen an exact compound predicate rather than consuming the
            /// adaptive membership cursor. Shared original coordinates keep
            /// broad native selections compressed through vector admission.
            pub fn indexSpaceSelection(self: *@This(), a: A, target: ?local.serverless_query_lake_serving.ServingSource.RowResolver) !?PhysicalSet {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const ca = arena.allocator();
                const parsed = try std.json.parseFromSliceLeaky(std.json.Value, ca, self.json, .{});
                const compiled = try Graph.compilePatternFilter(ca, parsed);
                var conditions: std.ArrayList(Condition) = .empty;
                if (!try collectConditions(ca, compiled, &conditions, 0)) return null;
                var predicate = (try rows.tryOpenPredicateWithContext(a, self.resolver.server, self.resolver.table, conditions.items, self.resolver.context, self.resolver.source, self.resolver.pinned)) orelse return null;
                defer predicate.deinit();
                if (!predicate.blocksMatch(target)) return null;
                var result = PhysicalSet.init(a);
                errdefer result.deinit();
                var window = std.heap.ArenaAllocator.init(a);
                defer window.deinit();
                while (true) {
                    try self.resolver.read_context.ensureActive();
                    _ = window.reset(.free_all);
                    const blocks = try predicate.nextBlocks(window.allocator());
                    if (blocks.len == 0) break;
                    for (blocks) |block| try result.addBlock(block);
                }
                return result;
            }
            fn materialize(self: *@This()) !void {
                if (self.complete != null) return;
                // A selective exact driver is cheaper than walking a broad
                // second index or scanning its Parquet column. Probe remaining
                // exact predicates only over these bounded physical candidates.
                if (self.predicates.items.len != 0 and self.predicates.items[0].estimatedRows() <= 4096) {
                    try self.materializeSelective();
                    self.closePredicates();
                    return;
                }
                const resolved = (try self.resolver.resolve(self.a, self.json)) orelse return error.UnsupportedQueryRequest;
                self.complete = resolved.bitmap;
                self.closePredicates();
            }
            fn materializeSelective(self: *@This()) !void {
                var candidates = try self.resolver.consume(self.a, &self.predicates.items[0]);
                errdefer candidates.deinit();
                for (self.predicates.items[1..]) |*predicate| {
                    const count = candidates.boundedCardinality(4096) orelse return error.InvalidNativeLakeRowIndex;
                    if (count == 0) break;
                    if (predicate.membershipWork(count) >= predicate.work or !predicate.canProbeMembership()) {
                        var next = try self.resolver.consume(self.a, predicate);
                        defer next.deinit();
                        candidates.andWith(&next);
                        continue;
                    }
                    var selected = PhysicalSet.init(self.a);
                    defer selected.deinit();
                    var exhausted = false;
                    var files = candidates.files.iterator();
                    outer: while (files.next()) |file| {
                        var blocks = file.value_ptr.iterator();
                        while (blocks.next()) |block| {
                            var rows_it = block.value_ptr.iterator();
                            while (rows_it.next()) |low| {
                                try self.resolver.context.ensureActive();
                                try self.resolver.read_context.ensureActive();
                                if (!predicate.canProbeMembership()) {
                                    exhausted = true;
                                    break :outer;
                                }
                                const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.resolver.source.inventory.source_id, .snapshot_id = self.resolver.source.inventory.snapshot_id, .identity_version = self.resolver.source.inventory.row_identity_version, .file_id = file.key_ptr.*, .row_group_ordinal = block.key_ptr.group, .row_ordinal = (@as(u64, block.key_ptr.high) << 32) | low } };
                                if (try predicate.contains(ref)) try selected.addRow(ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal);
                            }
                        }
                    }
                    if (exhausted) {
                        // Point and tuple walks have independent read budgets;
                        // discard partial probe output before an exact index walk.
                        var next = try self.resolver.consume(self.a, predicate);
                        defer next.deinit();
                        candidates.andWith(&next);
                    } else {
                        candidates.deinit();
                        candidates = selected;
                        selected = PhysicalSet.init(self.a);
                    }
                }
                self.complete = candidates;
            }
            pub fn allows(self: *@This(), ref: local.storage_rowsource_types.RowRef) !bool {
                try self.resolver.context.ensureActive();
                try self.resolver.read_context.ensureActive();
                if (self.complete == null) {
                    var cost: u64 = 0;
                    for (self.predicates.items) |predicate| cost +|= predicate.membershipWork(1);
                    var probe = self.predicates.items.len != 0 and cost <= self.budget -| self.spent;
                    for (self.predicates.items) |predicate| probe = probe and predicate.canProbeMembership();
                    if (probe) {
                        // Reserve the complete conjunction's work before probing;
                        // short circuiting can only spend less than this budget.
                        self.spent +|= cost;
                        for (self.predicates.items) |*predicate| if (!try predicate.contains(ref)) return false;
                        return true;
                    }
                    try self.materialize();
                }
                if (ref != .external) return error.InvalidNativeLakeRowIndex;
                const row = ref.external;
                return self.complete.?.contains(row.file_id, row.row_group_ordinal, row.row_ordinal);
            }
            /// Probe a complete conjunction over one native candidate tile.
            /// Admission is reserved for the whole tile before any predicate
            /// advances, so a declined tile safely materializes the exact set.
            pub fn allowsMany(self: *@This(), refs: []const local.storage_rowsource_types.RowRef, output: []bool) !void {
                if (refs.len != output.len or refs.len > 128) return error.InvalidNativeLakeRowIndex;
                try self.resolver.context.ensureActive();
                try self.resolver.read_context.ensureActive();
                if (self.complete == null) {
                    var cost: u64 = 0;
                    var probe = self.predicates.items.len != 0;
                    for (self.predicates.items) |predicate| {
                        cost +|= predicate.membershipWork(refs.len);
                        probe = probe and predicate.canProbeMembership();
                    }
                    if (probe and cost <= self.budget -| self.spent) {
                        self.spent +|= cost;
                        @memset(output, true);
                        var selected: [128]local.storage_rowsource_types.RowRef = undefined;
                        var positions: [128]usize = undefined;
                        var allowed: [128]bool = undefined;
                        for (self.predicates.items) |*predicate| {
                            var count: usize = 0;
                            for (refs, output, 0..) |ref, keep, i| if (keep) {
                                selected[count] = ref;
                                positions[count] = i;
                                count += 1;
                            };
                            if (count == 0) break;
                            predicate.containsMany(selected[0..count], allowed[0..count]) catch |err| {
                                if (err != error.ArtifactReadBudgetExceeded and err != error.QueryCandidateBudgetExceeded) return err;
                                try self.materialize();
                                return self.allowsMany(refs, output);
                            };
                            for (positions[0..count], allowed[0..count]) |i, keep| output[i] = keep;
                        }
                        return;
                    }
                    try self.materialize();
                }
                for (refs, output) |ref, *keep| {
                    if (ref != .external) return error.InvalidNativeLakeRowIndex;
                    const row = ref.external;
                    keep.* = self.complete.?.contains(row.file_id, row.row_group_ordinal, row.row_ordinal);
                }
            }
        };
        pub fn openMembership(self: Self, a: A, json: []const u8) !Membership {
            if (Set != PhysicalSet) @compileError("adaptive physical membership requires physical coordinates");
            var result: Membership = .{ .a = a, .resolver = self, .arena = .init(a), .json = undefined };
            errdefer result.deinit();
            const ca = result.arena.allocator();
            result.json = try ca.dupe(u8, json);
            const parsed = try std.json.parseFromSliceLeaky(std.json.Value, ca, json, .{});
            const compiled = try Graph.compilePatternFilter(ca, parsed);
            var conditions: std.ArrayList(Condition) = .empty;
            if (try collectConditions(ca, compiled, &conditions, 0)) {
                if (try rows.tryOpenPredicateWithContext(a, self.server, self.table, conditions.items, self.context, self.source, self.pinned)) |predicate| {
                    var owned = predicate;
                    result.predicates.append(ca, owned) catch |err| {
                        owned.deinit();
                        return err;
                    };
                } else {
                    // Independent exact column predicates together prove the
                    // entire conjunction. No residual/superset becomes membership.
                    var names: std.StringHashMapUnmanaged(void) = .empty;
                    for (conditions.items) |condition| {
                        const entry = try names.getOrPut(ca, condition.column);
                        if (entry.found_existing) continue;
                        var subset: std.ArrayList(Condition) = .empty;
                        for (conditions.items) |other| if (std.mem.eql(u8, condition.column, other.column)) try subset.append(ca, other);
                        var predicate = (try rows.tryOpenPredicateWithContext(a, self.server, self.table, subset.items, self.context, self.source, self.pinned)) orelse {
                            result.closePredicates();
                            break;
                        };
                        result.predicates.append(ca, predicate) catch |err| {
                            predicate.deinit();
                            return err;
                        };
                    }
                }
                // Cardinality governs selective ordinal masks independently of
                // cold metadata setup cost. Cheapest predicates short circuit first.
                std.mem.sort(rows.Predicate, result.predicates.items, {}, struct {
                    fn less(_: void, left: rows.Predicate, right: rows.Predicate) bool {
                        return left.estimatedRows() < right.estimatedRows();
                    }
                }.less);
                var work: u64 = 0;
                var probe = result.predicates.items.len != 0;
                for (result.predicates.items) |predicate| {
                    work +|= predicate.work;
                    probe = probe and predicate.hasMembership() and predicate.canProbeMembership();
                }
                result.budget = @min(work, rows.predicateScanWork(self.source, conditions.items) orelse std.math.maxInt(u64));
                const selective = result.predicates.items.len != 0 and result.predicates.items[0].estimatedRows() <= 4096;
                var point_work: u64 = 0;
                for (result.predicates.items) |predicate| point_work +|= predicate.membershipWork(1);
                if (probe and !selective and result.budget > point_work) return result;
            }
            // Selective, unsupported or exhausted plans use the authoritative
            // exact planner. Small selections are ready before sparse navigation.
            try result.materialize();
            return result;
        }

        /// Optional admission proof. A required native conjunct may bound
        /// serving packs even when the complete predicate needs a residual.
        /// Only native index walks participate. Broad proofs require compressed
        /// blocks; row-wise producers retain the selective bound.
        pub fn selectivePredicate(self: Self, a: A, json: []const u8, limit: usize) !?rows.Predicate {
            if (Set != PhysicalSet) @compileError("physical admission requires physical coordinates");
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const sa = scratch.allocator();
            const parsed = try std.json.parseFromSliceLeaky(std.json.Value, sa, json, .{});
            const compiled = try Graph.compilePatternFilter(sa, parsed);
            var conditions: std.ArrayList(Condition) = .empty;
            try requiredConditions(sa, compiled, &conditions, 0);
            if (conditions.items.len == 0 or conditions.items.len > 64) return null;
            var best: ?rows.Predicate = null;
            errdefer if (best) |*predicate| predicate.deinit();
            // Try a compound index first, then independent column indexes.
            // All these predicates are necessary; full membership is still
            // evaluated in the selected text windows, including OR/NOT.
            var names: std.StringHashMapUnmanaged(void) = .empty;
            var condition_index: usize = 0;
            while (condition_index <= conditions.items.len) : (condition_index += 1) {
                try self.context.ensureActive();
                try self.read_context.ensureActive();
                var subset: std.ArrayList(Condition) = .empty;
                const selected = if (condition_index == 0) conditions.items else column_conditions: {
                    const name = conditions.items[condition_index - 1].column;
                    if ((try names.getOrPut(sa, name)).found_existing) continue;
                    for (conditions.items) |condition| if (std.mem.eql(u8, name, condition.column)) try subset.append(sa, condition);
                    break :column_conditions subset.items;
                };
                if (try rows.tryOpenPredicateWithContext(a, self.server, self.table, selected, self.context, self.source, self.pinned)) |candidate| {
                    var predicate = candidate;
                    if ((predicate.estimatedRows() <= 4096 or predicate.hasBlocks()) and predicate.estimatedRows() <= limit and (best == null or predicate.estimatedRows() < best.?.estimatedRows())) {
                        if (best) |*previous| previous.deinit();
                        best = predicate;
                    } else predicate.deinit();
                }
            }
            return best;
        }
        pub fn selectiveCandidates(self: Self, a: A, json: []const u8, limit: usize, allowance: *local.serverless_query_lake_serving.ServingSource.TranslationAllowance) !?PhysicalSet {
            var best = (try self.selectivePredicate(a, json, limit)) orelse return null;
            defer best.deinit();
            if (best.hasBlocks()) {
                var result = try self.consume(a, &best);
                if (result.boundedCardinality(limit) == null) {
                    result.deinit();
                    return null;
                }
                return result;
            }
            var result = PhysicalSet.init(a);
            var keep = false;
            defer if (!keep) result.deinit();
            var window = std.heap.ArenaAllocator.init(a);
            defer window.deinit();
            while (true) {
                try self.read_context.ensureActive();
                _ = window.reset(.free_all);
                const refs = try best.nextPhysicalBounded(window.allocator(), 256, allowance);
                if (refs.len == 0) break;
                for (refs) |ref| try result.addRow(ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal);
                if (result.boundedCardinality(limit) == null) return null;
            }
            keep = true;
            return result;
        }

        pub fn resolve(self: Self, a: A, json: []const u8) !?if (Set == Bitmap) Result else PredicateResult {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{});
            const compiled = try Graph.compilePatternFilter(arena.allocator(), parsed.value);
            if (Set == Bitmap) if (try self.openProducer(a, arena.allocator(), compiled)) |producer| return producer;
            var result = try self.value(a, arena.allocator(), compiled, 0, true);
            if (result) |resolved| {
                const usable = if (Set == Bitmap)
                    self.allow_partial and resolved.bitmap.cardinality() <= local.storage_db_query_search_exec.lateVisibilityExactCandidateBudget()
                else
                    false;
                if (!resolved.exact and !usable) {
                    var partial = resolved;
                    defer partial.bitmap.deinit();
                    if (Set == PhysicalSet) {
                        result = try self.scanSelected(a, partial.residual orelse compiled, &partial.bitmap);
                    } else {
                        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store_identity, .context = self.read_context };
                        var selected = try self.identities.physicalSelection(a, &partial.bitmap, self.store, cached);
                        defer selected.deinit();
                        result = try self.scanSelected(a, partial.residual orelse compiled, &selected);
                    }
                }
            }
            if (Set == Bitmap) return if (result) |resolved| .{ .bitmap = resolved.bitmap, .exact = resolved.exact } else null;
            return result;
        }
        const ProducerOwner = struct {
            a: A,
            resolver: Self,
            predicate: rows.Predicate,
            arena: std.heap.ArenaAllocator,
            conditions: []const Condition,
            work_budget: u64,
            cache: LiveRowsCache = .{},
            materialized: ?Bitmap = null,
            spent_work: u64 = 0,
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.materialized) |*bitmap| bitmap.deinit();
                self.cache.deinit(self.a);
                self.predicate.deinit();
                self.arena.deinit();
                self.a.destroy(self);
            }
            fn materialize(self: *@This()) !void {
                // Preserve the whole-condition planner's scan/intersection
                // alternatives instead of forcing an expensive index walk.
                var exact = self.resolver;
                exact.allow_partial = false;
                if (try exact.planConditions(self.a, self.conditions)) |resolved| {
                    if (!resolved.exact) {
                        var rejected = resolved.bitmap;
                        rejected.deinit();
                        return error.InvalidNativeLakeRowIndex;
                    }
                    self.materialized = resolved.bitmap;
                } else self.materialized = try self.resolver.consume(self.a, &self.predicate);
                try self.materialized.?.prepareRead();
            }
            fn completeMembership(raw: *anyopaque) ?*const Bitmap {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return if (self.materialized) |*bitmap| bitmap else null;
            }
            fn contains(raw: *anyopaque, a: A, ordinal: u32) anyerror!bool {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.resolver.context.ensureActive();
                if (self.materialized) |*bitmap| return bitmap.contains(ordinal);
                var selected = Bitmap.init(a);
                defer selected.deinit();
                try selected.add(0);
                var matched = try produce(raw, a, ordinal, 1, &selected);
                defer matched.deinit();
                return matched.contains(0);
            }
            fn produce(raw: *anyopaque, a: A, offset: u32, count: u32, candidates: ?*const Bitmap) anyerror!Bitmap {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.resolver.context.ensureActive();
                if (self.materialized == null) {
                    const work = if (candidates) |selected| self.predicate.membershipWork(selected.cardinality()) else std.math.maxInt(u64);
                    if (candidates != null and work <= self.work_budget -| self.spent_work) {
                        self.spent_work +|= work;
                        var result = Bitmap.init(a);
                        errdefer result.deinit();
                        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.resolver.server.lake_read_cache, .scope = self.resolver.store_identity, .context = self.resolver.read_context };
                        var iterator = candidates.?.iterator();
                        var tile_budget: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 4 * 1024 * 1024 };
                        var scratch = std.heap.ArenaAllocator.init(tile_budget.allocator());
                        defer scratch.deinit();
                        while (true) {
                            try self.resolver.context.ensureActive();
                            var ordinals: [128]u32 = undefined;
                            var local_ordinals: [128]u32 = undefined;
                            var size: usize = 0;
                            while (size < ordinals.len) {
                                const local_ordinal = iterator.next() orelse break;
                                if (local_ordinal >= count) return error.InvalidNativeLakeTextCorpus;
                                local_ordinals[size] = local_ordinal;
                                ordinals[size] = try std.math.add(u32, offset, local_ordinal);
                                size += 1;
                            }
                            if (size == 0) break;
                            const admitted = tile: {
                                if (!self.predicate.canProbeMembership()) break :tile false;
                                _ = scratch.reset(.free_all);
                                const refs = self.resolver.identities.physicalRows(scratch.allocator(), &self.cache, self.resolver.store, cached, ordinals[0..size], self.resolver.source.inventory.source_id, self.resolver.source.inventory.snapshot_id) catch |err| {
                                    if (err == error.OutOfMemory and tile_budget.isExhausted()) break :tile false;
                                    return err;
                                };
                                var allowed: [128]bool = undefined;
                                self.predicate.containsMany(refs, allowed[0..size]) catch |err| {
                                    if (err == error.ArtifactReadBudgetExceeded or err == error.QueryCandidateBudgetExceeded or (err == error.OutOfMemory and tile_budget.isExhausted())) break :tile false;
                                    return err;
                                };
                                for (local_ordinals[0..size], allowed[0..size]) |ordinal, keep| if (keep) try result.add(ordinal);
                                break :tile true;
                            };
                            if (!admitted) {
                                // Discard all partial probe output before exposing exact fallback.
                                _ = scratch.reset(.free_all);
                                try self.materialize();
                                const slice = try self.materialized.?.sliceRebased(a, offset, @as(u64, offset) + count);
                                result.deinit();
                                return slice;
                            }
                        }
                        return result;
                    }
                    // One full compressed membership is shared by every later
                    // segment/count pass. Point work is bounded by this plan's
                    // estimated cost, including when selective segments precede
                    // a broad segment.
                    try self.materialize();
                }
                return self.materialized.?.sliceRebased(a, offset, @as(u64, offset) + count);
            }
        };
        fn openProducer(self: Self, a: A, pa: A, compiled: Compiled) !?Result {
            // Only a proven exact whole-index conjunction can be deferred.
            // Boolean combinations and residual predicates retain the planner.
            if (self.identities.spanCount() == 0) return null;
            var conditions: std.ArrayList(Condition) = .empty;
            if (!try collectConditions(pa, compiled, &conditions, 0)) return null;
            var predicate = (try rows.tryOpenPredicateWithContext(a, self.server, self.table, conditions.items, self.context, self.source, self.pinned)) orelse return null;
            errdefer predicate.deinit();
            if (!predicate.hasMembership()) {
                predicate.deinit();
                return null;
            }
            // A point-sized metadata predicate is cheaper to materialize than
            // even one reverse-tree probe. Preserve native scorer pushdown.
            if (predicate.estimatedRows() == 0 or predicate.work <= predicate.membershipWork(1)) {
                const bitmap = try self.consume(a, &predicate);
                predicate.deinit();
                return .{ .bitmap = bitmap };
            }
            var owned = std.heap.ArenaAllocator.init(a);
            errdefer owned.deinit();
            const ca = owned.allocator();
            const copied = try ca.alloc(Condition, conditions.items.len);
            for (conditions.items, copied) |condition, *out| {
                out.* = condition;
                out.column = try ca.dupe(u8, condition.column);
                const bytes = try std.json.Stringify.valueAlloc(ca, condition.value, .{});
                out.value = try std.json.parseFromSliceLeaky(std.json.Value, ca, bytes, .{ .allocate = .alloc_always, .parse_numbers = false });
                if (condition.value == .integer or condition.value == .float) out.value = condition.value;
            }
            const work_budget = @min(predicate.work, rows.predicateScanWork(self.source, copied) orelse std.math.maxInt(u64));
            const owner = try a.create(ProducerOwner);
            owner.* = .{ .a = a, .resolver = self, .predicate = predicate, .arena = owned, .conditions = copied, .work_budget = work_budget, .cache = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a) } };
            return .{ .bitmap = Bitmap.init(a), .producer = .{ .ptr = owner, .produce = ProducerOwner.produce, .contains = ProducerOwner.contains, .materialized = ProducerOwner.completeMembership }, .owner = owner, .close = ProducerOwner.close };
        }
        fn value(self: Self, a: A, pa: A, input: Compiled, depth: usize, allow_scan: bool) anyerror!?PredicateResult {
            if (depth > 64) return null;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var conditions: std.ArrayList(Condition) = .empty;
            const scalar_conditions = try collectConditions(arena.allocator(), input, &conditions, depth);
            if (scalar_conditions and allow_scan) {
                if (try self.planConditions(a, conditions.items)) |result| return result;
            }
            if (scalar_conditions) {
                if (try self.index(a, conditions.items, false)) |bitmap| return .{ .bitmap = bitmap };
            }
            // Separate indexes may enforce individual conjuncts. An unresolved
            // child makes this whole predicate residual; OR/NOT never use supersets.
            switch (input) {
                .conjuncts => |children| if (try self.resolveChildren(a, pa, children, true, depth)) |result| return result,
                .disjuncts => |children| if (try self.resolveChildren(a, pa, children, false, depth)) |result| return result,
                .bool_query => |boolean| {
                    // A required OR is indexable when every branch is exact.
                    if (boolean.must.len == 0 and boolean.must_not.len == 0 and boolean.min_should == 1) {
                        if (try self.resolveChildren(a, pa, boolean.should, false, depth)) |result| return result;
                    } else if (boolean.must.len != 0) {
                        if (try self.resolveChildren(a, pa, boolean.must, true, depth)) |resolved| {
                            var result = resolved;
                            errdefer result.bitmap.deinit();
                            result.exact = result.exact and boolean.min_should == 0 and boolean.must_not.len == 0;
                            if (!result.exact) {
                                const must = if (result.residual) |residual| try pa.dupe(Compiled, &.{residual}) else try pa.alloc(Compiled, 0);
                                result.residual = .{ .bool_query = .{ .must = must, .should = boolean.should, .must_not = boolean.must_not, .min_should = boolean.min_should } };
                            }
                            return result;
                        }
                    }
                },
                else => {},
            }
            if (allow_scan and scalar_conditions) {
                if (try self.index(a, conditions.items, true)) |bitmap| return .{ .bitmap = bitmap };
            }
            return if (allow_scan) self.scanExpression(a, input) else null;
        }
        fn resolveChildren(self: Self, a: A, pa: A, input: []const Compiled, conjunction: bool, depth: usize) !?PredicateResult {
            if (input.len == 0) return null;
            var result: ?PredicateResult = null;
            errdefer if (result) |*predicate| predicate.bitmap.deinit();
            var exact = true;
            var residual: std.ArrayList(Compiled) = .empty;
            defer residual.deinit(pa);
            for (input) |child| {
                var next = (try self.value(a, pa, child, depth + 1, false)) orelse {
                    if (conjunction) {
                        exact = false;
                        try residual.append(pa, child);
                        continue;
                    }
                    if (result) |*predicate| predicate.bitmap.deinit();
                    return null;
                };
                if (!conjunction and !next.exact) {
                    next.bitmap.deinit();
                    if (result) |*predicate| predicate.bitmap.deinit();
                    return null;
                }
                exact = exact and next.exact;
                if (!next.exact) residual.append(pa, next.residual orelse child) catch |err| {
                    next.bitmap.deinit();
                    return err;
                };
                if (result) |*predicate| {
                    defer next.bitmap.deinit();
                    if (conjunction) predicate.bitmap.andWith(&next.bitmap) else try predicate.bitmap.orWith(&next.bitmap);
                } else result = next;
            }
            if (result) |*predicate| {
                predicate.exact = exact;
                if (!exact) predicate.residual = .{ .conjuncts = try residual.toOwnedSlice(pa) };
            }
            return result;
        }
        /// Plan exact flat conjunctions as a whole. Candidate predicates stay
        /// unopened for row iteration until a plan wins; their metadata handles
        /// are bounded leases and all rejected handles are closed.
        fn planConditions(self: Self, a: A, conditions: []const Condition) !?PredicateResult {
            var scan = (try rows.tryOpenPredicateScan(a, self.table, conditions, self.context, self.source)) orelse return null;
            defer scan.deinit();
            const Choice = union(enum) { scan, whole, subset: usize, intersection };
            var choice: Choice = .scan;
            var best_work = rows.predicateScanWork(self.source, conditions) orelse std.math.maxInt(u64);
            var whole = try rows.tryOpenPredicateWithContext(a, self.server, self.table, conditions, self.context, self.source, self.pinned);
            defer if (whole) |*predicate| predicate.deinit();
            if (whole) |predicate| if (predicate.work < best_work) {
                choice = .whole;
                best_work = predicate.work;
            };
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const ca = arena.allocator();
            var columns: std.ArrayList([]const u8) = .empty;
            for (conditions) |condition| {
                var found = false;
                for (columns.items) |name| if (std.mem.eql(u8, name, condition.column)) {
                    found = true;
                    break;
                };
                if (!found) try columns.append(ca, condition.column);
            }
            var children: std.ArrayList(rows.Predicate) = .empty;
            defer for (children.items) |*predicate| predicate.deinit();
            var intersection_work: u64 = 0;
            var all_indexed = true;
            // A one-column range has already been considered as a whole.
            if (columns.items.len > 1) for (columns.items) |name| {
                var subset: std.ArrayList(Condition) = .empty;
                for (conditions) |condition| if (std.mem.eql(u8, name, condition.column)) {
                    try subset.append(ca, condition);
                };
                var predicate = (try rows.tryOpenPredicateWithContext(a, self.server, self.table, subset.items, self.context, self.source, self.pinned)) orelse {
                    all_indexed = false;
                    continue;
                };
                children.append(ca, predicate) catch |err| {
                    predicate.deinit();
                    return err;
                };
                const work = predicate.work +| rows.predicateResidualWork(self.source, conditions, predicate.estimatedRows());
                if ((predicate.estimatedRows() == 0 or (Set == Bitmap and self.allow_partial and predicate.estimatedRows() <= local.storage_db_query_search_exec.lateVisibilityExactCandidateBudget())) and work < best_work) {
                    choice = .{ .subset = children.items.len - 1 };
                    best_work = work;
                }
                intersection_work +|= predicate.work;
            };
            if (columns.items.len > 1 and all_indexed and children.items.len == columns.items.len and intersection_work < best_work) choice = .intersection;
            return switch (choice) {
                .scan => .{ .bitmap = try self.consume(a, &scan) },
                .whole => .{ .bitmap = try self.consume(a, &whole.?) },
                // A subset is a proven superset only. Keep the entire compiled
                // predicate residual so ranking/counting evaluate every condition.
                .subset => |position| .{ .bitmap = try self.consume(a, &children.items[position]), .exact = children.items[position].estimatedRows() == 0 },
                .intersection => blk: {
                    var result: ?Set = null;
                    errdefer if (result) |*bitmap| bitmap.deinit();
                    for (children.items) |*predicate| {
                        var next = try self.consume(a, predicate);
                        if (result) |*bitmap| {
                            defer next.deinit();
                            bitmap.andWith(&next);
                        } else result = next;
                    }
                    break :blk .{ .bitmap = result.? };
                },
            };
        }
        fn index(self: Self, a: A, conditions: []const Condition, scan: bool) !?Set {
            var predicate = (if (scan) try rows.tryOpenPredicateScan(a, self.table, conditions, self.context, self.source) else try rows.tryOpenPredicateWithContext(a, self.server, self.table, conditions, self.context, self.source, self.pinned)) orelse return null;
            defer predicate.deinit();
            return try self.consume(a, &predicate);
        }
        fn consume(self: Self, a: A, predicate: *rows.Predicate) !Set {
            var result = Set.init(a);
            errdefer result.deinit();
            var bitmap_cache: LiveRowsCache = .{ .backing = a };
            defer bitmap_cache.deinit(a);
            var window = std.heap.ArenaAllocator.init(a);
            defer window.deinit();
            const direct_blocks = if (Set == Bitmap) predicate.blocksMatch(self.identities.row_resolver) else false;
            if (predicate.hasBlocks() or direct_blocks) {
                // Choose translation once for the complete compressed mask.
                // Per-block candidate probes would rescan the same window
                // once for every block in the archive.
                const mixed_blocks = Set == Bitmap and !direct_blocks and self.identities.row_resolver != null;
                var physical = PhysicalSet.init(a);
                defer physical.deinit();
                while (true) {
                    try self.context.ensureActive();
                    _ = window.reset(.free_all);
                    const blocks = try predicate.nextBlocks(window.allocator());
                    if (blocks.len == 0) break;
                    for (blocks) |block| {
                        if (Set == PhysicalSet) {
                            try result.addBlock(block);
                        } else {
                            if (mixed_blocks) {
                                try physical.addBlock(block);
                                continue;
                            }
                            const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store_identity, .context = self.read_context };
                            var identities = self.identities;
                            if (direct_blocks) identities.row_resolver = null;
                            try identities.addBlock(a, window.allocator(), &bitmap_cache, self.store, cached, block, &result);
                        }
                    }
                }
                if (comptime Set == Bitmap) if (mixed_blocks) {
                    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store_identity, .context = self.read_context };
                    return self.identities.bitmapForPhysical(a, &physical, self.store, cached);
                };
                return result;
            }
            while (true) {
                try self.context.ensureActive();
                _ = window.reset(.retain_capacity);
                const refs = try predicate.next(window.allocator(), 1024);
                if (refs.len == 0) break;
                for (refs) |ref| try self.addMatch(a, &result, &bitmap_cache, ref);
            }
            return result;
        }
        fn addMatch(self: Self, a: A, result: *Set, cache: *LiveRowsCache, ref: local.storage_rowsource_types.RowRef) !void {
            if (ref != .external) return error.InvalidNativeLakeRowIndex;
            const row = ref.external;
            if (!std.mem.eql(u8, row.source_id, self.source.inventory.source_id) or !std.mem.eql(u8, row.snapshot_id, self.source.inventory.snapshot_id)) return error.ExternalLakeSnapshotMismatch;
            if (Set == PhysicalSet) {
                try result.addRow(row.file_id, row.row_group_ordinal, row.row_ordinal);
                return;
            }
            const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store_identity, .context = self.read_context };
            if (try self.identities.ordinal(a, cache, self.store, cached, ref)) |number| try result.add(number);
        }
        fn scanExpression(self: Self, a: A, input: Compiled) !?PredicateResult {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const ca = arena.allocator();
            var fields: std.ArrayList([]const u8) = .empty;
            if (!try @import("lake_index_search_filter.zig").dependencies(ca, self.table, input, &fields)) return null;
            var result = Set.init(a);
            errdefer result.deinit();
            try self.scanInto(a, input, fields.items, null, &result);
            return .{ .bitmap = result };
        }
        fn scanSelected(self: Self, a: A, input: Compiled, selected: *const PhysicalSet) !?PredicateResult {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            if (self.source.hasLazyInventory()) {
                // Physical blocks are keyed by logical file identity. Resolve a
                // bounded file view before assigning scan-local file ordinals;
                // neither publication navigation nor a lazy catalog has a flat
                // inventory until the selected files are explicitly reached.
                const ids = try arena.allocator().alloc([]const u8, selected.files.count());
                var files = selected.files.keyIterator();
                for (ids) |*id| id.* = files.next().?.*;
                var view = try self.source.selectedFilesView(a, ids, self.read_context);
                defer view.closeParallelView(self.source);
                var scoped = self;
                scoped.source = &view;
                return scoped.scanSelected(a, input, selected);
            }
            var fields: std.ArrayList([]const u8) = .empty;
            if (!try @import("lake_index_search_filter.zig").dependencies(arena.allocator(), self.table, input, &fields)) return null;
            var result = Set.init(a);
            errdefer result.deinit();
            const Selection = @typeInfo(@FieldType(local.sql_catalog.Scan, "physical_selection")).optional.child;
            var blocks: std.ArrayList(Selection.Block) = .empty;
            const ca = arena.allocator();
            var files = selected.files.iterator();
            while (files.next()) |file| {
                const file_index = if (self.source.fileMap()) |map| map.get(file.key_ptr.*) orelse return error.ExternalLakeSnapshotMismatch else for (self.source.inventory.files, 0..) |entry, file_position| {
                    if (std.mem.eql(u8, entry.file_id, file.key_ptr.*)) break file_position;
                } else return error.ExternalLakeSnapshotMismatch;
                var entries = file.value_ptr.iterator();
                while (entries.next()) |block| {
                    if (block.value_ptr.isEmpty()) continue;
                    try blocks.append(ca, .{ .file = file_index, .group = block.key_ptr.group, .high = block.key_ptr.high, .rows = block.value_ptr });
                }
            }
            std.mem.sort(Selection.Block, blocks.items, {}, Selection.blockLess);
            try self.scanInto(a, input, fields.items, .{ .blocks = blocks.items }, &result);
            return .{ .bitmap = result };
        }
        fn scanInto(self: Self, a: A, input: Compiled, fields: []const []const u8, selection: @FieldType(local.sql_catalog.Scan, "physical_selection"), result: *Set) !void {
            const cursor = try local.sql_lake_cursor.openPinned(a, self.table, .{ .fields = fields, .physical_selection = selection, .limit = 1024 }, self.context, self.source);
            defer cursor.close(cursor.ptr);
            var row_arena = std.heap.ArenaAllocator.init(a);
            defer row_arena.deinit();
            var page_arena = std.heap.ArenaAllocator.init(a);
            defer page_arena.deinit();
            var bitmap_cache: LiveRowsCache = .{ .backing = a };
            defer bitmap_cache.deinit(a);
            while (true) {
                try self.context.ensureActive();
                _ = page_arena.reset(.retain_capacity);
                const page = try cursor.next_columns.?(cursor.ptr, page_arena.allocator(), 1024);
                try page.validate();
                if (page.native != null) return error.InvalidSqlBackendResponse;
                const mask = if (columnEvaluable(input)) try page_arena.allocator().alloc(bool, page.selection.len) else null;
                if (mask) |matches| try evaluateColumns(page_arena.allocator(), input, page, matches, null);
                for (page.selection, 0..) |position, row| {
                    if (mask) |matches| {
                        if (matches[row]) try self.addMatch(a, result, &bitmap_cache, page.batch.rowRef(position));
                        continue;
                    }
                    _ = row_arena.reset(.retain_capacity);
                    const ra = row_arena.allocator();
                    const ref = page.batch.rowRef(position);
                    const id = try local.storage_rowsource_identity.allocId(ra, ref);
                    var doc: std.json.Value = .{ .object = .empty };
                    try doc.object.put(ra, "_id", .{ .string = id });
                    for (page.batch.columns) |col| {
                        const cell = try page.cell(ra, row, col.name);
                        try doc.object.put(ra, col.name, cell.value);
                    }
                    if (try input.matches(ra, id, doc)) try self.addMatch(a, result, &bitmap_cache, ref);
                }
                if (page.after == null) break;
            }
        }
    };
}

// Execute normalized expression nodes over column pages. Leaf semantics stay in
// the shared search compiler; Boolean masks avoid rebuilding a JSON object/ID
// for each row while preserving the remote document projection semantics.
fn columnEvaluable(input: Compiled) bool {
    return switch (input) {
        .match_all, .match_none => true,
        .doc_id => false,
        .field_matcher => |field| column(field.path) != null,
        .conjuncts, .disjuncts => |children| blk: {
            for (children) |child| if (!columnEvaluable(child)) break :blk false;
            break :blk true;
        },
        .bool_query => |b| blk: {
            for (b.must) |child| if (!columnEvaluable(child)) break :blk false;
            for (b.should) |child| if (!columnEvaluable(child)) break :blk false;
            for (b.must_not) |child| if (!columnEvaluable(child)) break :blk false;
            break :blk true;
        },
    };
}
/// Evaluate selected primitive lanes without constructing per-row JSON cells.
fn evaluateNumeric(comptime T: type, a: A, predicate: Graph.CompiledPatternFilter.FieldPredicate, values: []const T, vector: local.storage_rowsource_types.ColumnVector, page: local.sql_catalog.ColumnPage, mask: []bool, active: ?[]const bool) !bool {
    // The legacy numeric_range operator intentionally compares as f64;
    // standard ranges retain exact integer and mixed-domain scalar semantics.
    if (T == i64 and predicate == .numeric_range) return evaluateNumericAs(i64, f64, a, predicate, values, vector, page, mask, active);
    return evaluateNumericAs(T, T, a, predicate, values, vector, page, mask, active);
}
fn evaluateNumericAs(comptime T: type, comptime C: type, a: A, predicate: Graph.CompiledPatternFilter.FieldPredicate, values: []const T, vector: local.storage_rowsource_types.ColumnVector, page: local.sql_catalog.ColumnPage, mask: []bool, active: ?[]const bool) !bool {
    // Admission errors belong to the scalar evaluator on reached lanes.
    // Inactive branches must not observe malformed bound errors.
    const kernel = (predicate.numericKernel(C) catch return false) orelse return false;
    var null_match: ?bool = null;
    var row: usize = 0;
    while (row < mask.len) : (row += 8) {
        var lanes: [8]C = @splat(0);
        const count = @min(8, mask.len - row);
        for (0..count) |lane| {
            const position = row + lane;
            if (active) |enabled| if (!enabled[position]) continue;
            const index = page.selection[position];
            if (vector.nulls.isNull(index)) continue;
            lanes[lane] = if (T == C) values[index] else @floatFromInt(values[index]);
        }
        const v: @Vector(8, C) = lanes;
        const lower: [8]bool = if (kernel.lower) |bound| (if (kernel.inclusive_lower) v >= @as(@Vector(8, C), @splat(bound)) else v > @as(@Vector(8, C), @splat(bound))) else @splat(true);
        const upper: [8]bool = if (kernel.upper) |bound| (if (kernel.inclusive_upper) v <= @as(@Vector(8, C), @splat(bound)) else v < @as(@Vector(8, C), @splat(bound))) else @splat(true);
        for (0..count) |lane| {
            const position = row + lane;
            mask[position] = false;
            if (active) |enabled| if (!enabled[position]) continue;
            if (vector.nulls.isNull(page.selection[position])) {
                if (null_match == null) null_match = try predicate.matches(a, &.{.null});
                mask[position] = null_match.?;
                continue;
            }
            if (C == f64 and !std.math.isFinite(lanes[lane])) {
                mask[position] = try predicate.matches(a, &.{.{ .float = lanes[lane] }});
            } else mask[position] = lower[lane] and upper[lane];
        }
    }
    return true;
}

fn evaluateColumns(a: A, input: Compiled, page: local.sql_catalog.ColumnPage, mask: []bool, active: ?[]const bool) anyerror!void {
    switch (input) {
        .match_all => for (mask, 0..) |*match, row| {
            match.* = if (active) |lanes| lanes[row] else true;
        },
        .match_none => @memset(mask, false),
        .doc_id => unreachable,
        .field_matcher => |field| {
            const name = column(field.path).?;
            if (page.native == null) if (page.batch.findColumn(name)) |vector| {
                if ((field.predicate == .exists or field.predicate.nullTerm()) and vector.values != .json and vector.values != .vector_f32) {
                    for (mask, page.selection, 0..) |*match, index, row| match.* = (field.predicate == .exists or vector.nulls.isNull(index)) and (if (active) |enabled| enabled[row] else true);
                    return;
                }
                switch (vector.values) {
                    inline .dictionary_bytes, .dictionary_i64, .dictionary_f64 => |dictionary, tag| {
                        // Evaluate only dictionary entries reached by active lanes.
                        // This preserves short-circuit errors and null semantics.
                        const cache = try a.alloc(u2, dictionary.values.len);
                        defer a.free(cache);
                        @memset(cache, 0);
                        var null_match: ?bool = null;
                        var scratch = std.heap.ArenaAllocator.init(a);
                        defer scratch.deinit();
                        for (mask, page.selection, 0..) |*match, index, row| {
                            match.* = false;
                            if (active) |lanes| if (!lanes[row]) continue;
                            if (vector.nulls.isNull(index)) {
                                if (null_match == null) null_match = try field.predicate.matches(scratch.allocator(), &.{.null});
                                match.* = null_match.?;
                                continue;
                            }
                            const id = dictionary.indices[index];
                            if (cache[id] == 0) {
                                _ = scratch.reset(.retain_capacity);
                                const value: std.json.Value = switch (tag) {
                                    .dictionary_bytes => .{ .string = dictionary.values[id] },
                                    .dictionary_i64 => .{ .integer = dictionary.values[id] },
                                    .dictionary_f64 => .{ .float = dictionary.values[id] },
                                    else => unreachable,
                                };
                                cache[id] = if (try field.predicate.matches(scratch.allocator(), &.{value})) 2 else 1;
                            }
                            match.* = cache[id] == 2;
                        }
                        return;
                    },
                    .i64 => |values| {
                        if (try evaluateNumeric(i64, a, field.predicate, values, vector, page, mask, active)) return;
                    },
                    .f64 => |values| {
                        if (try evaluateNumeric(f64, a, field.predicate, values, vector, page, mask, active)) return;
                    },
                    .bool => |values| if (field.predicate.booleanTerm()) |term| {
                        for (mask, page.selection, 0..) |*match, index, row| match.* = (if (active) |enabled| enabled[row] else true) and !vector.nulls.isNull(index) and values[index] == term;
                        return;
                    },
                    else => {},
                }
            };
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            for (mask, 0..) |*match, row| {
                match.* = false;
                if (active) |lanes| if (!lanes[row]) continue;
                _ = scratch.reset(.retain_capacity);
                const cell = try page.cell(scratch.allocator(), row, name);
                match.* = try field.predicate.matches(scratch.allocator(), &.{cell.value});
            }
        },
        .conjuncts, .disjuncts => |children| {
            const conjunction = input == .conjuncts;
            for (mask, 0..) |*match, row| match.* = conjunction and (if (active) |lanes| lanes[row] else true);
            const pending = try a.alloc(bool, mask.len);
            defer a.free(pending);
            const child_mask = try a.alloc(bool, mask.len);
            defer a.free(child_mask);
            for (children) |child| {
                for (pending, mask, 0..) |*lane, match, row| lane.* = (if (active) |lanes| lanes[row] else true) and (if (conjunction) match else !match);
                try evaluateColumns(a, child, page, child_mask, pending);
                for (mask, child_mask) |*match, next| match.* = if (conjunction) match.* and next else match.* or next;
            }
        },
        .bool_query => |b| {
            for (mask, 0..) |*match, row| match.* = if (active) |lanes| lanes[row] else true;
            const child_mask = try a.alloc(bool, mask.len);
            defer a.free(child_mask);
            for (b.must) |child| {
                try evaluateColumns(a, child, page, child_mask, mask);
                for (mask, child_mask) |*match, next| match.* = match.* and next;
            }
            if (b.min_should > 0) {
                const counts = try a.alloc(usize, mask.len);
                defer a.free(counts);
                @memset(counts, 0);
                const pending = try a.alloc(bool, mask.len);
                defer a.free(pending);
                for (b.should) |child| {
                    for (pending, mask, counts) |*lane, match, count| lane.* = match and count < b.min_should;
                    try evaluateColumns(a, child, page, child_mask, pending);
                    for (counts, child_mask) |*count, next| count.* += @intFromBool(next);
                }
                for (mask, counts) |*match, count| match.* = match.* and count >= b.min_should;
            }
            for (b.must_not) |child| {
                try evaluateColumns(a, child, page, child_mask, mask);
                for (mask, child_mask) |*match, next| match.* = match.* and !next;
            }
        },
    }
}

// Plan the shared compiler's normalized IR, never a second JSON grammar.
fn column(path: Compiled.FieldPath) ?[]const u8 {
    return switch (path) {
        .single => |name| name,
        .dotted, .json_pointer => |parts| if (parts.len == 1) parts[0] else null,
    };
}
/// Only required predicates may bound an ordered producer. OR/NOT and
/// unsupported leaves remain membership checks in the native collector.
pub fn requiredConditions(a: A, input: Compiled, out: *std.ArrayList(Condition), depth: usize) anyerror!void {
    if (depth > 64) return;
    switch (input) {
        .conjuncts => |children| for (children) |child| try requiredConditions(a, child, out, depth + 1),
        .bool_query => |b| for (b.must) |child| try requiredConditions(a, child, out, depth + 1),
        .field_matcher => {
            const before = out.items.len;
            if (!try collectConditions(a, input, out, depth)) out.shrinkRetainingCapacity(before);
        },
        else => {},
    }
}
fn collectConditions(a: A, input: Compiled, out: *std.ArrayList(Condition), depth: usize) !bool {
    if (depth > 64) return false;
    switch (input) {
        .conjuncts => |children| {
            if (children.len == 0) return false;
            for (children) |child| if (!try collectConditions(a, child, out, depth + 1)) return false;
            return true;
        },
        .bool_query => |boolean| {
            if (boolean.min_should != 0 or boolean.must_not.len != 0 or boolean.must.len == 0) return false;
            for (boolean.must) |child| if (!try collectConditions(a, child, out, depth + 1)) return false;
            return true;
        },
        .field_matcher => |field| {
            const name = column(field.path) orelse return false;
            if (field.predicate == .term) {
                const value = try field.predicate.term.jsonValue();
                if (value == .null or value == .number_string) return false;
                try out.append(a, .{ .column = name, .op = .eq, .value = value });
                return true;
            }
            const bounds = (try field.predicate.standardBounds()) orelse return false;
            if (bounds.lower) |bound| try out.append(a, .{ .column = name, .op = if (bound.inclusive) .gte else .gt, .value = bound.value });
            if (bounds.upper) |bound| try out.append(a, .{ .column = name, .op = if (bound.inclusive) .lte else .lt, .value = bound.value });
            return true;
        },
        else => return false,
    }
}

test "external lake predicate planner shares term and range aliases with the compiler" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        \\{"conjuncts":[{"term":{"path":"/kind","value":"story"}},{"range":{"path":"/created_at","gte":42,"lt":100}}]}
        ,
        \\{"bool":{"filter":[{"term":{"kind":"story"}},{"range":{"created_at":{"from":42,"to":100,"include_upper":false}}}]}}
        ,
        \\{"conjuncts":[{"term":{"field":"kind","term":"story"}},{"range":{"field":"created_at","min":42,"max":100}}]}
    }) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
        const compiled = try Graph.compilePatternFilter(a, parsed.value);
        var conditions: std.ArrayList(Condition) = .empty;
        try std.testing.expect(try collectConditions(a, compiled, &conditions, 0));
        try std.testing.expectEqual(@as(usize, 3), conditions.items.len);
        try std.testing.expectEqualStrings("kind", conditions.items[0].column);
        try std.testing.expectEqual(Condition.Op.lt, conditions.items[2].op);
    }
}

test "external lake column residual masks match shared document evaluation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Datum = @import("antfly_local_sources").sql_scalar.Datum;
    const values = [_]Datum{ Datum.json(.{ .string = "kept" }), Datum.json(.{ .string = "other" }), Datum.json(.null) };
    const vectors = [_][]const Datum{&values};
    const Batch = @typeInfo(@FieldType(@typeInfo(@FieldType(local.sql_catalog.ColumnPage, "native")).optional.child, "values")).pointer.child;
    const batch: Batch = .{ .vectors = .{ .values = &vectors, .count = 3 } };
    const page: local.sql_catalog.ColumnPage = .{ .native = .{ .values = &batch, .names = &.{"label"} }, .selection = &.{ 2, 0, 1 } };
    for ([_][]const u8{
        \\{"prefix":{"path":"/label","value":"ke"}}
        ,
        \\{"bool":{"should":[{"term":{"label":"kept"}},{"exists":{"field":"label"}}],"minimum_should_match":2}}
        ,
        \\{"bool":{"must_not":[{"term":{"label":"other"}}]}}
    }) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, ca, json, .{});
        const compiled = try Graph.compilePatternFilter(ca, parsed.value);
        const Check = struct {
            fn run(allocator: A, input: Compiled, columns: local.sql_catalog.ColumnPage) !void {
                var actual: [3]bool = undefined;
                try evaluateColumns(allocator, input, columns, &actual, null);
            }
        };
        try std.testing.checkAllAllocationFailures(a, Check.run, .{ compiled, page });
        var mask: [3]bool = undefined;
        try evaluateColumns(ca, compiled, page, &mask, null);
        for (mask, 0..) |match, row| {
            var doc: std.json.Value = .{ .object = .empty };
            try doc.object.put(ca, "label", (try page.cell(ca, row, "label")).value);
            try std.testing.expectEqual(try compiled.matches(ca, "id", doc), match);
        }
    }
}

test "external lake residual dictionary and SIMD kernels preserve active null and selected lanes" {
    const a = std.testing.allocator;
    const row_types = local.storage_rowsource_types;
    var refs: [11]row_types.RowRef = undefined;
    @memset(&refs, .{ .relational_key = "id" });
    const numbers = [_]i64{ 7, -1, undefined, 4, 7, 9, 7, 7, 2, undefined, 7 };
    const indices = [_]u32{ 0, 1, 0, 1, 2, 0, 0, 1, 2, 0, 0 };
    const values = [_][]const u8{ "kept", "other", "keeper" };
    const nulls = [_]u8{ 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0 };
    const floats = [_]f64{ 1, -1, undefined, 4.5, 7, 9, 6, 7, 2, undefined, 8 };
    const booleans = [_]bool{ true, false, undefined, true, false, true, false, true, false, undefined, false };
    const vectors = [_]row_types.ColumnVector{
        .{ .name = "floating", .values = .{ .f64 = &floats }, .nulls = .{ .bytes = &nulls } },
        .{ .name = "flag", .values = .{ .bool = &booleans }, .nulls = .{ .bytes = &nulls } },
        .{ .name = "number", .values = .{ .i64 = &numbers }, .nulls = .{ .bytes = &nulls } },
        .{ .name = "label", .values = .{ .dictionary_bytes = .{ .values = &values, .indices = &indices } }, .nulls = .{ .bytes = &nulls } },
    };
    const page: local.sql_catalog.ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &vectors }, .selection = &.{ 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 } };
    try page.validate();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for ([_][]const u8{
        \\{"term":{"number":7}}
        ,
        \\{"range":{"number":{"gt":2,"lte":7}}}
        ,
        \\{"range":{"number":{"gte":2}}}
        ,
        \\{"range":{"number":{"lt":7}}}
        ,
        \\{"range":{"number":{"gt":2.5,"lte":7}}}
        ,
        \\{"numeric_range":{"path":"/number","min":2,"max":7}}
        ,
        \\{"range":{"floating":{"gte":1,"lt":7}}}
        ,
        \\{"numeric_range":{"path":"/floating","min":1,"max":7,"inclusive_max":true}}
        ,
        \\{"term":{"floating":4.5}}
        ,
        \\{"term":{"flag":true}}
        ,
        \\{"term":{"number":null}}
        ,
        \\{"exists":{"field":"number"}}
        ,
        \\{"prefix":{"path":"/label","value":"ke"}}
        ,
        \\{"bool":{"must":[{"term":{"number":7}}],"must_not":[{"term":{"label":"other"}}]}}
    }) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{});
        const compiled = try Graph.compilePatternFilter(arena.allocator(), parsed.value);
        const Probe = struct {
            fn run(allocator: A, input: Compiled, columns: local.sql_catalog.ColumnPage) !void {
                const active = [_]bool{ true, true, false, true, true, true, false, true, true, true, true };
                var mask: [11]bool = undefined;
                try evaluateColumns(allocator, input, columns, &mask, &active);
                // Inject failures into the column kernel; reference document
                // construction has an independent allocator and arena growth.
                var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
                defer scratch.deinit();
                for (mask, active, 0..) |actual, enabled, row| {
                    _ = scratch.reset(.retain_capacity);
                    const ra = scratch.allocator();
                    var doc: std.json.Value = .{ .object = .empty };
                    for (columns.batch.columns) |vector| try doc.object.put(ra, vector.name, (try columns.cell(ra, row, vector.name)).value);
                    try std.testing.expectEqual(enabled and try input.matches(ra, "id", doc), actual);
                }
            }
        };
        try Probe.run(a, compiled, page);
        try std.testing.checkAllAllocationFailures(a, Probe.run, .{ compiled, page });
    }
}

test "external lake residual numeric kernels preserve nonfinite errors and inactive lanes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const refs = [_]local.storage_rowsource_types.RowRef{.{ .relational_key = "id" }};
    const values = [_]f64{std.math.nan(f64)};
    const vectors = [_]local.storage_rowsource_types.ColumnVector{.{ .name = "value", .values = .{ .f64 = &values } }};
    const page: local.sql_catalog.ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &vectors }, .selection = &.{0} };
    const range_json = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"range\":{\"value\":{\"gte\":1}}}", .{});
    const range = try Graph.compilePatternFilter(arena.allocator(), range_json);
    var mask: [1]bool = undefined;
    try std.testing.expectError(error.SqlNumericOutOfRange, evaluateColumns(a, range, page, &mask, null));
    try evaluateColumns(a, range, page, &mask, &.{false});
    try std.testing.expect(!mask[0]);
    const numeric_json = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"numeric_range\":{\"path\":\"/value\",\"min\":1,\"max\":7}}", .{});
    const numeric = try Graph.compilePatternFilter(arena.allocator(), numeric_json);
    try evaluateColumns(a, numeric, page, &mask, null);
    // The authoritative numeric_range matcher uses unordered comparisons.
    try std.testing.expect(mask[0]);
    for ([_][]const u8{ "{}", "{\"min\":\"bad\"}" }) |json| {
        const malformed = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), json, .{});
        const invalid: Compiled = .{ .field_matcher = .{ .path = .{ .single = "value" }, .predicate = .{ .numeric_range = malformed } } };
        try evaluateColumns(a, invalid, page, &mask, &.{false});
        try std.testing.expect(!mask[0]);
        try std.testing.expectError(error.InvalidArgument, evaluateColumns(a, invalid, page, &mask, null));
    }
}

test "external lake large ordinal selections invert compressed extents and deleted holes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-text-inverse-selection");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } };
    var live = Bitmap.init(a);
    defer live.deinit();
    for ([_]u32{ 5, 7, 9 }) |row| try live.add(row);
    const bytes = try live.toBytes(a);
    defer a.free(bytes);
    var ref = try store.put(bytes);
    defer ref.deinit(a);
    const blocks = [_]corpus.physical.Block{
        .{ .group = 0, .high = 0, .base = 0, .count = 200001, .lower = 3 },
        .{ .group = 2, .high = 1 << 32, .base = 200001, .count = 3, .lower = 5, .bitmap = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len } },
    };
    var identities: Identities = .{ .offsets = &.{} };
    defer identities.files.deinit(a);
    try identities.files.put(a, "selected", .{ .lower = 10, .upper = 200014, .rows = &blocks });
    try identities.files.put(a, "unselected", .{ .lower = 300000, .upper = 500004, .rows = &blocks });
    identities.spans = &.{ .{ .file = "selected", .span = identities.files.get("selected").? }, .{ .file = "unselected", .span = identities.files.get("unselected").? } };
    var live_cache: LiveRowsCache = .{ .backing = a };
    defer live_cache.deinit(a);
    for ([_]u32{ 10, 200010, 200011, 200012, 200013, 300000, 500003 }) |ordinal_value| {
        const row = try identities.physicalRow(a, &live_cache, store, cached, ordinal_value, "source", "snapshot");
        try std.testing.expectEqual(@as(?u32, ordinal_value), try identities.ordinal(a, &live_cache, store, cached, row));
    }
    for ([_]u32{ 9, 200014, 299999, 500004 }) |ordinal_value| try std.testing.expectError(error.InvalidNativeLakeTextCorpus, identities.physicalRow(a, &live_cache, store, cached, ordinal_value, "source", "snapshot"));
    var selected = Bitmap.init(a);
    defer selected.deinit();
    try selected.addRange(10, 200011);
    try selected.add(200012);
    var physical = try identities.physicalSelection(a, &selected, store, cached);
    defer physical.deinit();
    try std.testing.expectEqual(@as(usize, 1), physical.files.count());
    try std.testing.expect(physical.contains("selected", 0, 3));
    try std.testing.expect(physical.contains("selected", 0, 200003));
    try std.testing.expect(!physical.contains("selected", 0, 200004));
    try std.testing.expect(physical.contains("selected", 2, (@as(u64, 1) << 52) + 7));
    try std.testing.expect(!physical.contains("selected", 2, (@as(u64, 1) << 52) + 5));
    try std.testing.expect(!physical.contains("selected", 2, (@as(u64, 1) << 52) + 6));
}

test "external lake live row cache evicts within fixed backing storage and prepares rank select once" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-live-rows-cache");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var serving = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer serving.deinit();
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &serving, .scope = @splat(9), .context = .{ .io = std.testing.io } };
    // A request arena cannot reclaim evicted objects. Exactly four slot
    // allocations must suffice for repeated loads, decodes and rank builds.
    const backing = try a.alloc(u8, 4 * LiveRowsCache.slot_bytes);
    defer a.free(backing);
    var fixed = std.heap.FixedBufferAllocator.init(backing);
    var cache: LiveRowsCache = .{ .backing = fixed.allocator() };
    defer cache.deinit(fixed.allocator());
    for (0..20) |i| {
        var live = Bitmap.init(a);
        defer live.deinit();
        try live.addRange(0, 10000);
        try live.remove(@intCast(i));
        const bytes = try live.toBytes(a);
        defer a.free(bytes);
        var ref = try store.put(bytes);
        defer ref.deinit(a);
        var block: corpus.physical.Block = .{ .group = 0, .high = 0, .base = 0, .lower = 0, .count = 9999, .bitmap = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len } };
        const bitmap = try cache.load(fixed.allocator(), store, cached, block);
        try std.testing.expect(bitmap.read_rank != null);
        try std.testing.expectEqual(@as(?u32, 9999), bitmap.read_rank.?.select(9998));
        try std.testing.expect(!bitmap.contains(@intCast(i)));
        try std.testing.expect(bitmap == try cache.load(fixed.allocator(), store, cached, block));
        if (i == 0) {
            var identities: Identities = .{ .offsets = &.{} };
            defer identities.files.deinit(a);
            try identities.files.put(a, "holes", .{ .lower = 10, .upper = 10009, .rows = &.{block} });
            var selected = Bitmap.init(a);
            defer selected.deinit();
            try selected.add(10);
            try selected.add(10008);
            var physical = try identities.physicalSelection(a, &selected, store, cached);
            defer physical.deinit();
            try std.testing.expect(physical.contains("holes", 0, 1));
            try std.testing.expect(physical.contains("holes", 0, 9999));
            try std.testing.expect(!physical.contains("holes", 0, 0));
            try std.testing.expect(!physical.contains("holes", 0, 9998));
        }
        block.count -= 1;
        try std.testing.expectError(error.InvalidNativeLakeTextCorpus, cache.load(fixed.allocator(), store, cached, block));
    }
    try std.testing.expectEqual(backing.len, fixed.end_index);
}

test "external lake selected segment predicates map ranges and bitmaps across file ordinal gaps" {
    const a = std.testing.allocator;
    const span: Identities.Span = .{ .lower = 0, .upper = 4, .rows = &.{.{ .group = 0, .high = 0, .base = 0, .count = 6, .lower = 10 }}, .intervals = &.{ .{ .file_lower = 0, .lower = 0, .upper = 1 }, .{ .file_lower = 3, .lower = 1, .upper = 4 } } };
    var identities: Identities = .{ .offsets = &.{ 0, 1, 4 }, .partial_inventory = true };
    defer identities.files.deinit(a);
    try identities.files.put(a, "part", span);
    var result = Bitmap.init(a);
    defer result.deinit();
    var cache: LiveRowsCache = .{ .backing = a };
    defer cache.deinit(a);
    // No artifact access is necessary for contiguous physical rows.
    const store: @import("../serverless/artifacts/store.zig").ArtifactStore = undefined;
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = undefined;
    try identities.addBlock(a, a, &cache, store, cached, .{ .file = "part", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 11, .count = 4 } } }, &result);
    try std.testing.expectEqual(@as(u64, 2), result.cardinality());
    try std.testing.expect(result.contains(1) and result.contains(2));
    result.deinit();
    result = Bitmap.init(a);
    var physical = Bitmap.init(a);
    defer physical.deinit();
    for ([_]u32{ 10, 12, 13, 15 }) |row| try physical.add(row);
    try identities.addBlock(a, a, &cache, store, cached, .{ .file = "part", .group = 0, .base = 0, .selection = .{ .bitmap = physical } }, &result);
    try std.testing.expectEqual(@as(u64, 3), result.cardinality());
    try std.testing.expect(result.contains(0) and result.contains(1) and result.contains(3));
    var inverted = try span.fileRanks(a, &result, 0, 6);
    defer inverted.deinit();
    try std.testing.expectEqual(@as(u64, 3), inverted.cardinality());
    try std.testing.expect(inverted.contains(0) and inverted.contains(3) and inverted.contains(5));
    var block = try span.fileRanks(a, &result, 2, 5);
    defer block.deinit();
    try std.testing.expectEqual(@as(u64, 1), block.cardinality());
    try std.testing.expect(block.contains(1));
}

test "external lake bounded WAL physical masks preserve selected gaps and high row bits" {
    const a = std.testing.allocator;
    const high: u64 = 1 << 40;
    const span: Identities.Span = .{ .lower = 0, .upper = 4, .rows = &.{.{ .group = 2, .high = high >> corpus.physical.shift, .base = 0, .count = 6, .lower = 10 }}, .intervals = &.{ .{ .file_lower = 0, .lower = 0, .upper = 1 }, .{ .file_lower = 3, .lower = 1, .upper = 4 } } };
    var identities: Identities = .{ .offsets = &.{ 0, 1, 4 }, .partial_inventory = true, .spans = &.{.{ .file = "part", .span = span }} };
    defer identities.files.deinit(a);
    try identities.files.put(a, "part", span);
    var physical = @import("lake_index_physical_set.zig").Set.init(a);
    defer physical.deinit();
    for ([_]u64{ 10, 12, 13, 15, 100 }) |row| try physical.addRow("part", 2, high + row);
    try physical.addRow("part", 0, high + 10);
    try physical.addRow("unadmitted", 2, high + 10);
    // Contiguous physical extents do not access artifact storage.
    const store: @import("../serverless/artifacts/store.zig").ArtifactStore = undefined;
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = undefined, .scope = @splat(1), .context = .{} };
    var result = try identities.bitmapForPhysical(a, &physical, store, cached);
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 3), result.cardinality());
    try std.testing.expect(result.contains(0) and result.contains(1) and result.contains(3));
}

test "external lake rebuilt predicate blocks translate into retained text coordinates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("mixed-predicate-blocks");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    const store = fs.artifactStore();
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } };
    const Ref = local.storage_rowsource_types.RowRef;
    const Mapping = struct {
        fn reverse(_: *anyopaque, allocator: A, row: Ref) !Ref {
            if (!std.mem.eql(u8, row.external.file_id, "new") or row.external.row_ordinal >= 4) return error.TestUnexpectedResult;
            var result = row;
            result.external.file_id = try allocator.dupe(u8, "old");
            result.external.row_ordinal = 3 - row.external.row_ordinal;
            return result;
        }
        fn forward(raw: *anyopaque, _: A, row: Ref) !Ref {
            const calls: *u8 = @ptrCast(@alignCast(raw));
            calls.* += 1;
            if (!std.mem.eql(u8, row.external.file_id, "old") or row.external.row_ordinal >= 4) return error.TestUnexpectedResult;
            var result = row;
            result.external.file_id = "new";
            result.external.row_ordinal = 3 - row.external.row_ordinal;
            return result;
        }
        fn file(_: *anyopaque, _: A, _: []const u8) !?[]const u8 {
            return error.TestUnexpectedResult;
        }
        fn digest(_: *anyopaque, _: A, _: []const u8) ![]const u8 {
            return error.TestUnexpectedResult;
        }
        fn rank(_: *anyopaque, _: A, _: Ref) !u64 {
            return error.TestUnexpectedResult;
        }
        fn close(_: *anyopaque) void {}
    };
    var marker: u8 = 0;
    var identities: Identities = .{ .offsets = &.{}, .row_resolver = .{ .ptr = &marker, .source = "source", .snapshot = "old-cut", .current_source = "source", .current_snapshot = "new-cut", .rows = 4, .to_index = Mapping.reverse, .from_index = Mapping.forward, .file = Mapping.file, .digest = Mapping.digest, .tie_rank = Mapping.rank, .close = Mapping.close } };
    defer identities.files.deinit(a);
    const blocks = [_]corpus.physical.Block{.{ .group = 0, .high = 0, .base = 0, .count = 4, .lower = 0 }};
    try identities.files.put(a, "old", .{ .lower = 0, .upper = 4, .rows = &blocks });
    var live: LiveRowsCache = .{ .backing = a };
    defer live.deinit(a);
    var result = Bitmap.init(a);
    defer result.deinit();
    try identities.addBlock(a, a, &live, store, cached, .{ .file = "new", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 2 } } }, &result);
    try std.testing.expectEqual(@as(u64, 2), result.cardinality());
    try std.testing.expect(result.contains(2) and result.contains(3));
    var selection = @import("lake_index_physical_set.zig").Set.init(a);
    defer selection.deinit();
    try selection.addRow("new", 0, 1);
    var single = try identities.bitmapForPhysical(a, &selection, store, cached);
    defer single.deinit();
    try std.testing.expectEqual(@as(u64, 1), single.cardinality());
    try std.testing.expect(single.contains(2));
    // A four-row mask over a two-row window must use just two forward probes.
    identities.spans = &.{.{ .file = "old", .span = .{ .lower = 0, .upper = 2, .rows = &blocks } }};
    try selection.addBlock(.{ .file = "new", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 4 } } });
    var broad = try identities.bitmapForPhysical(a, &selection, store, cached);
    defer broad.deinit();
    try std.testing.expectEqual(@as(u64, 2), broad.cardinality());
    try std.testing.expect(broad.contains(0) and broad.contains(1));
    try std.testing.expectEqual(@as(u8, 2), marker);
}

test "external lake text candidate tiles batch shuffled duplicate relocation without scalar map probes" {
    const a = std.testing.allocator;
    const Ref = local.storage_rowsource_types.RowRef;
    const Mapping = struct {
        batches: usize = 0,
        fn one(_: *anyopaque, _: A, _: Ref) !Ref {
            return error.TestUnexpectedResult;
        }
        fn many(raw: *anyopaque, out: A, refs: []const Ref, forward: bool) ![]const Ref {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.batches += 1;
            try std.testing.expect(forward);
            const result = try out.dupe(Ref, refs);
            for (result) |*ref| {
                try std.testing.expectEqualStrings("old", ref.external.snapshot_id);
                ref.external.snapshot_id = "new";
                ref.external.file_id = "current";
                ref.external.row_ordinal = 7 - ref.external.row_ordinal;
            }
            return result;
        }
        fn file(_: *anyopaque, _: A, _: []const u8) !?[]const u8 {
            return error.TestUnexpectedResult;
        }
        fn digest(_: *anyopaque, _: A, _: []const u8) ![]const u8 {
            return error.TestUnexpectedResult;
        }
        fn rank(_: *anyopaque, _: A, _: Ref) !u64 {
            return error.TestUnexpectedResult;
        }
        fn close(_: *anyopaque) void {}
    };
    var mapping: Mapping = .{};
    const blocks = [_]corpus.physical.Block{.{ .group = 0, .high = 0, .base = 0, .count = 8, .lower = 0 }};
    const identities: Identities = .{ .offsets = &.{}, .spans = &.{.{ .file = "original", .span = .{ .lower = 0, .upper = 8, .rows = &blocks } }}, .row_resolver = .{ .ptr = &mapping, .source = "source", .snapshot = "old", .current_source = "source", .current_snapshot = "new", .rows = 8, .from_index = Mapping.one, .to_index = Mapping.one, .translate_many = Mapping.many, .file = Mapping.file, .digest = Mapping.digest, .tie_rank = Mapping.rank, .close = Mapping.close } };
    var live: LiveRowsCache = .{};
    defer live.deinit(a);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = undefined, .scope = @splat(1), .context = .{} };
    const refs = try identities.physicalRows(region.allocator(), &live, undefined, cached, &.{ 7, 1, 1, 3 }, "source", "new");
    try std.testing.expectEqual(@as(usize, 1), mapping.batches);
    for (refs, [_]u64{ 0, 6, 6, 4 }) |ref, row| {
        try std.testing.expectEqual(row, ref.external.row_ordinal);
        try std.testing.expectEqualStrings("new", ref.external.snapshot_id);
    }
}
