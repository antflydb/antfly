// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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

/// Four independently reusable slots bound decode/rank storage even when the
/// request allocator is an arena. Each slot uses a fixed buffer, not an arena
/// whose freed objects would remain resident in the request's parent arena.
pub const LiveRowsCache = struct {
    const slot_bytes = 1024 * 1024;
    const Slot = struct {
        buffer: ?[]u8 = null,
        fixed: std.heap.FixedBufferAllocator = undefined,
        bitmap: ?Bitmap = null,
        id: [32]u8 = undefined,
        used: u64 = 0,
        byte_len: u64 = 0,
    };
    slots: [4]Slot = @splat(.{}),
    clock: u64 = 0,
    pub fn deinit(self: *@This(), a: A) void {
        for (&self.slots) |*slot| if (slot.buffer) |buffer| a.free(buffer);
        self.* = .{};
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
        if (victim.buffer == null) victim.buffer = try a.alloc(u8, slot_bytes);
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
    const Span = struct { lower: u32, upper: u32, rows: []const corpus.physical.Block };
    const FileSpan = struct { file: []const u8, span: Span };
    files: std.StringHashMapUnmanaged(Span) = .empty,
    spans: []const FileSpan = &.{},
    offsets: []const u32,
    pub fn init(a: A, root: corpus.Root, snapshot: *const local.index.IndexSnapshot) !Identities {
        if (root.version != corpus.metadata_version or (root.file_groups.len == 0 and snapshot.segments.len != 0)) return error.InvalidNativeLakeTextCorpus;
        const offsets = try a.alloc(u32, snapshot.segments.len + 1);
        offsets[0] = 0;
        for (snapshot.segments, 0..) |segment, i| offsets[i + 1] = try std.math.add(u32, offsets[i], segment.reader.doc_count);
        var result: Identities = .{ .offsets = offsets };
        const spans = try a.alloc(FileSpan, root.file_groups.len);
        var segment: usize = 0;
        for (root.file_groups, 0..) |group, group_index| {
            const start = segment;
            segment += group.segments.len;
            if (segment > snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
            const entry = try result.files.getOrPut(a, try a.dupe(u8, group.file.id));
            if (entry.found_existing) return error.InvalidNativeLakeTextCorpus;
            if (try corpus.physical.validate(group.rows) != offsets[segment] - offsets[start]) return error.InvalidNativeLakeTextCorpus;
            const physical_rows = try a.dupe(corpus.physical.Block, group.rows);
            for (physical_rows) |*block| if (block.bitmap) |*ref| {
                ref.artifact_id = try a.dupe(u8, ref.artifact_id);
                ref.checksum = try a.dupe(u8, ref.checksum);
            };
            entry.value_ptr.* = .{ .lower = offsets[start], .upper = offsets[segment], .rows = physical_rows };
            spans[group_index] = .{ .file = entry.key_ptr.*, .span = entry.value_ptr.* };
        }
        if (segment != snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
        result.spans = spans;
        return result;
    }
    fn liveRows(a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, block: corpus.physical.Block) !*const Bitmap {
        return cache.load(a, store, cached, block);
    }
    fn addBlock(self: Identities, a: A, temporary: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, selected: @import("lake_index_predicate_blocks.zig").Block, result: *Bitmap) !void {
        const file = self.files.get(selected.file) orelse return error.ExternalLakeSnapshotMismatch;
        const index = corpus.physical.find(file.rows, selected.group, selected.base) orelse return;
        const block = file.rows[index];
        const base = try std.math.add(u32, file.lower, block.base);
        switch (selected.selection) {
            .interval => |span| {
                if (block.bitmap != null) {
                    const live = try liveRows(a, cache, store, cached, block);
                    const lower: u32 = @intCast(live.rank(span.lower));
                    const upper: u32 = @intCast(live.rank(span.lower + span.count));
                    try result.addRange(try std.math.add(u32, base, lower), @as(u64, base) + upper);
                } else {
                    const lower = @max(span.lower, block.lower);
                    const upper = @min(span.lower + span.count, block.lower + block.count);
                    if (upper > lower) try result.addRange(try std.math.add(u32, base, lower - block.lower), @as(u64, base) + upper - block.lower);
                }
            },
            .bitmap => |bitmap| {
                var intersection = try bitmap.clone(temporary);
                defer intersection.deinit();
                if (block.bitmap != null) {
                    const live = try liveRows(a, cache, store, cached, block);
                    intersection.andWith(live);
                    var iterator = intersection.iterator();
                    while (iterator.next()) |row| try result.add(try std.math.add(u32, base, @intCast(live.rank(row))));
                } else {
                    var extent = Bitmap.init(temporary);
                    defer extent.deinit();
                    try extent.addRange(block.lower, @as(u64, block.lower) + block.count);
                    intersection.andWith(&extent);
                    var shifted = try intersection.addOffset(base -% block.lower);
                    defer shifted.deinit();
                    try result.orWith(&shifted);
                }
            },
        }
    }
    pub fn ordinal(self: Identities, a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, row: local.storage_rowsource_types.RowRef) !?u32 {
        if (row != .external) return error.InvalidNativeLakeRowIndex;
        const ref = row.external;
        const span = self.files.get(ref.file_id) orelse return error.ExternalLakeSnapshotMismatch;
        const index = corpus.physical.find(span.rows, ref.row_group_ordinal, ref.row_ordinal) orelse return null;
        const block = span.rows[index];
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
        return try std.math.add(u32, span.lower, try std.math.add(u32, block.base, rank));
    }
    /// Binary searches keep native-to-physical lookup logarithmic in files and
    /// physical blocks. Delete holes use the same authenticated rank directory.
    pub fn physicalRow(self: Identities, a: A, cache: *LiveRowsCache, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, ordinal_value: u32, source: []const u8, snapshot: []const u8) !local.storage_rowsource_types.RowRef {
        var lo: usize = 0;
        var hi = self.spans.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.spans[mid].span.upper <= ordinal_value) lo = mid + 1 else hi = mid;
        }
        if (lo == self.spans.len) return error.InvalidNativeLakeTextCorpus;
        const file = self.spans[lo];
        if (ordinal_value < file.span.lower) return error.InvalidNativeLakeTextCorpus;
        const relative = ordinal_value - file.span.lower;
        lo = 0;
        hi = file.span.rows.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const block = file.span.rows[mid];
            if (@as(u64, block.base) + block.count <= relative) lo = mid + 1 else hi = mid;
        }
        if (lo == file.span.rows.len) return error.InvalidNativeLakeTextCorpus;
        const block = file.span.rows[lo];
        if (relative < block.base) return error.InvalidNativeLakeTextCorpus;
        const rank = relative - block.base;
        const low = if (block.bitmap != null) blk: {
            const live = try liveRows(a, cache, store, cached, block);
            break :blk live.read_rank.?.select(rank) orelse return error.InvalidNativeLakeTextCorpus;
        } else try std.math.add(u32, block.lower, rank);
        return .{ .external = .{ .source_id = source, .snapshot_id = snapshot, .file_id = file.file, .row_group_ordinal = block.group, .row_ordinal = (block.high << corpus.physical.shift) | low } };
    }
    /// Invert only selected ordinal blocks. Contiguous extents stay compressed;
    /// delete holes are resolved within their authenticated physical block.
    fn physicalSelection(self: Identities, a: A, selected: *const Bitmap, store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead) !PhysicalSet {
        var result = PhysicalSet.init(a);
        errdefer result.deinit();
        var cache: LiveRowsCache = .{};
        defer cache.deinit(a);
        var files = self.files.iterator();
        while (files.next()) |file| {
            const span = file.value_ptr.*;
            if (selected.rangeCardinality(span.lower, span.upper) == 0) continue;
            for (span.rows) |block| {
                const lower = try std.math.add(u32, span.lower, block.base);
                const upper = @as(u64, lower) + block.count;
                if (selected.rangeCardinality(lower, upper) == 0) continue;
                var ranks = try selected.sliceRebased(a, lower, upper);
                defer ranks.deinit();
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
                    if (64 *| count >= predicate.work or !predicate.canProbeMembership()) {
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
                                const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.resolver.source.inventory.source_id, .snapshot_id = self.resolver.source.inventory.snapshot_id, .file_id = file.key_ptr.*, .row_group_ordinal = block.key_ptr.group, .row_ordinal = (@as(u64, block.key_ptr.high) << 32) | low } };
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
                    const cost = std.math.mul(u64, 64, self.predicates.items.len) catch std.math.maxInt(u64);
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
                if (probe and !selective and result.budget > 64 *| result.predicates.items.len) return result;
            }
            // Selective, unsupported or exhausted plans use the authoritative
            // exact planner. Small selections are ready before sparse navigation.
            try result.materialize();
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
            const point_work: u64 = 64;
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
            fn produce(raw: *anyopaque, a: A, offset: u32, count: u32, candidates: ?*const Bitmap) anyerror!Bitmap {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.resolver.context.ensureActive();
                if (self.materialized == null) {
                    const work = if (candidates) |selected| std.math.mul(u64, selected.cardinality(), point_work) catch std.math.maxInt(u64) else std.math.maxInt(u64);
                    if (candidates != null and work <= self.work_budget -| self.spent_work) {
                        self.spent_work +|= work;
                        var result = Bitmap.init(a);
                        errdefer result.deinit();
                        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.resolver.server.lake_read_cache, .scope = self.resolver.store_identity, .context = self.resolver.read_context };
                        var iterator = candidates.?.iterator();
                        var checked: usize = 0;
                        while (iterator.next()) |local_ordinal| : (checked += 1) {
                            if (checked % 64 == 0) try self.resolver.context.ensureActive();
                            if (local_ordinal >= count) return error.InvalidNativeLakeTextCorpus;
                            if (!self.predicate.canProbeMembership()) {
                                try self.materialize();
                                const slice = try self.materialized.?.sliceRebased(a, offset, @as(u64, offset) + count);
                                result.deinit();
                                return slice;
                            }
                            const ordinal_value = try std.math.add(u32, offset, local_ordinal);
                            const row = try self.resolver.identities.physicalRow(self.a, &self.cache, self.resolver.store, cached, ordinal_value, self.resolver.source.inventory.source_id, self.resolver.source.inventory.snapshot_id);
                            if (try self.predicate.contains(row)) try result.add(local_ordinal);
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
            if (self.identities.spans.len == 0) return null;
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
            if (predicate.estimatedRows() == 0 or predicate.work <= ProducerOwner.point_work) {
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
            owner.* = .{ .a = a, .resolver = self, .predicate = predicate, .arena = owned, .conditions = copied, .work_budget = work_budget };
            return .{ .bitmap = Bitmap.init(a), .producer = .{ .ptr = owner, .produce = ProducerOwner.produce, .materialized = ProducerOwner.completeMembership }, .owner = owner, .close = ProducerOwner.close };
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
            var bitmap_cache: LiveRowsCache = .{};
            defer bitmap_cache.deinit(a);
            var window = std.heap.ArenaAllocator.init(a);
            defer window.deinit();
            if (predicate.hasBlocks()) {
                while (true) {
                    try self.context.ensureActive();
                    _ = window.reset(.free_all);
                    const blocks = try predicate.nextBlocks(window.allocator());
                    if (blocks.len == 0) break;
                    for (blocks) |block| {
                        if (Set == PhysicalSet) {
                            try result.addBlock(block);
                        } else {
                            const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store_identity, .context = self.read_context };
                            try self.identities.addBlock(a, window.allocator(), &bitmap_cache, self.store, cached, block, &result);
                        }
                    }
                }
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
            var bitmap_cache: LiveRowsCache = .{};
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
                        if (matches[row]) try self.addMatch(a, result, &bitmap_cache, page.batch.row_refs[position]);
                        continue;
                    }
                    _ = row_arena.reset(.retain_capacity);
                    const ra = row_arena.allocator();
                    const ref = page.batch.row_refs[position];
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
    var live_cache: LiveRowsCache = .{};
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
    var cache: LiveRowsCache = .{};
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
