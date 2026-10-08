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

/// Version 8 maps compressed physical rows directly to pinned native ordinals.
/// Directory storage scales with files/groups/blocks and compressed delete holes.
pub const Identities = struct {
    const Span = struct { lower: u32, upper: u32, rows: []const corpus.physical.Block };
    files: std.StringHashMapUnmanaged(Span) = .empty,
    offsets: []const u32,
    pub fn init(a: A, root: corpus.Root, snapshot: *const local.index.IndexSnapshot) !Identities {
        if (root.version != corpus.metadata_version or (root.file_groups.len == 0 and snapshot.segments.len != 0)) return error.InvalidNativeLakeTextCorpus;
        const offsets = try a.alloc(u32, snapshot.segments.len + 1);
        offsets[0] = 0;
        for (snapshot.segments, 0..) |segment, i| offsets[i + 1] = try std.math.add(u32, offsets[i], segment.reader.doc_count);
        var result: Identities = .{ .offsets = offsets };
        var segment: usize = 0;
        for (root.file_groups) |group| {
            const start = segment;
            segment += group.segments.len;
            if (segment > snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
            const entry = try result.files.getOrPut(a, try a.dupe(u8, group.file.id));
            if (entry.found_existing) return error.InvalidNativeLakeTextCorpus;
            if (try corpus.physical.validate(group.rows) != offsets[segment] - offsets[start]) return error.InvalidNativeLakeTextCorpus;
            const encoded_rows = try std.json.Stringify.valueAlloc(a, group.rows, .{});
            defer a.free(encoded_rows);
            const physical_rows = try std.json.parseFromSliceLeaky([]const corpus.physical.Block, a, encoded_rows, .{ .allocate = .alloc_always });
            entry.value_ptr.* = .{ .lower = offsets[start], .upper = offsets[segment], .rows = physical_rows };
        }
        if (segment != snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
        return result;
    }
    fn liveRows(a: A, cache: *std.StringHashMapUnmanaged(Bitmap), store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, block: corpus.physical.Block) !*const Bitmap {
        const artifact = block.bitmap orelse return error.InvalidNativeLakeTextCorpus;
        const entry = try cache.getOrPut(a, artifact.artifact_id);
        if (!entry.found_existing) {
            const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, artifact, .none, cached);
            entry.value_ptr.* = try Bitmap.fromBytes(a, bytes);
            if (entry.value_ptr.cardinality() != block.count or entry.value_ptr.rank(1 << corpus.physical.shift) != block.count) return error.InvalidNativeLakeTextCorpus;
        }
        return entry.value_ptr;
    }
    fn addBlock(self: Identities, a: A, temporary: A, cache: *std.StringHashMapUnmanaged(Bitmap), store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, selected: @import("lake_index_predicate_blocks.zig").Block, result: *Bitmap) !void {
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
    fn ordinal(self: Identities, a: A, cache: *std.StringHashMapUnmanaged(Bitmap), store: @import("../serverless/artifacts/store.zig").ArtifactStore, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, row: local.storage_rowsource_types.RowRef) !?u32 {
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
};

pub const PhysicalSet = @import("lake_index_physical_set.zig").Set;
pub const Resolver = PredicateResolver(Bitmap);
pub const PhysicalResolver = PredicateResolver(PhysicalSet);
fn PredicateResolver(comptime Set: type) type {
    return struct {
        const Self = @This();
        const PredicateResult = if (Set == Bitmap) Result else struct { bitmap: Set, exact: bool = true };
        server: *@import("http_server.zig").ApiHttpServer,
        table: local.sql_catalog.Table,
        source: *local.serverless_query_lake_serving.ServingSource,
        context: local.api_operation.RequestContext,
        identities: Identities = .{ .offsets = &.{} },
        store: @import("../serverless/artifacts/store.zig").ArtifactStore,
        store_identity: [32]u8,
        read_context: local.serverless_query_lake_read_context.Context,

        pub fn resolve(self: Self, a: A, json: []const u8) !?PredicateResult {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{});
            const compiled = try Graph.compilePatternFilter(arena.allocator(), parsed.value);
            const result = try self.value(a, compiled, 0, true);
            if (Set == PhysicalSet) if (result) |resolved| {
                if (!resolved.exact) {
                    var partial = resolved;
                    partial.bitmap.deinit();
                    return self.scanExpression(a, compiled);
                }
            };
            return result;
        }
        fn value(self: Self, a: A, input: Compiled, depth: usize, allow_scan: bool) anyerror!?PredicateResult {
            if (depth > 64) return null;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var conditions: std.ArrayList(Condition) = .empty;
            const scalar_conditions = try collectConditions(arena.allocator(), input, &conditions, depth);
            if (scalar_conditions) {
                if (try self.index(a, conditions.items, false)) |bitmap| return .{ .bitmap = bitmap };
            }
            // Separate indexes may enforce individual conjuncts. An unresolved
            // child makes this whole predicate residual; OR/NOT never use supersets.
            switch (input) {
                .conjuncts => |children| if (try self.resolveChildren(a, children, true, depth)) |result| return result,
                .disjuncts => |children| if (try self.resolveChildren(a, children, false, depth)) |result| return result,
                .bool_query => |boolean| {
                    // A required OR is indexable when every branch is exact.
                    if (boolean.must.len == 0 and boolean.must_not.len == 0 and boolean.min_should == 1) {
                        if (try self.resolveChildren(a, boolean.should, false, depth)) |result| return result;
                    } else if (boolean.must.len != 0) {
                        if (try self.resolveChildren(a, boolean.must, true, depth)) |resolved| {
                            var result = resolved;
                            result.exact = result.exact and boolean.min_should == 0 and boolean.must_not.len == 0;
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
        fn resolveChildren(self: Self, a: A, input: []const Compiled, conjunction: bool, depth: usize) !?PredicateResult {
            if (input.len == 0) return null;
            var result: ?PredicateResult = null;
            errdefer if (result) |*predicate| predicate.bitmap.deinit();
            var exact = true;
            for (input) |child| {
                var next = (try self.value(a, child, depth + 1, false)) orelse {
                    if (conjunction) {
                        exact = false;
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
                if (result) |*predicate| {
                    defer next.bitmap.deinit();
                    if (conjunction) predicate.bitmap.andWith(&next.bitmap) else try predicate.bitmap.orWith(&next.bitmap);
                } else result = next;
            }
            if (result) |*predicate| predicate.exact = exact;
            return result;
        }
        fn index(self: Self, a: A, conditions: []const Condition, scan: bool) !?Set {
            var predicate = (if (scan) try rows.tryOpenPredicateScan(a, self.table, conditions, self.context, self.source) else try rows.tryOpenPredicate(a, self.server, self.table, conditions, self.context, self.source)) orelse return null;
            defer predicate.deinit();
            var result = Set.init(a);
            errdefer result.deinit();
            var lookup = std.heap.ArenaAllocator.init(a);
            defer lookup.deinit();
            var bitmap_cache: std.StringHashMapUnmanaged(Bitmap) = .empty;
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
                            try self.identities.addBlock(lookup.allocator(), window.allocator(), &bitmap_cache, self.store, cached, block, &result);
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
                for (refs) |ref| try self.addMatch(lookup.allocator(), &result, &bitmap_cache, ref);
            }
            return result;
        }
        fn addMatch(self: Self, a: A, result: *Set, cache: *std.StringHashMapUnmanaged(Bitmap), ref: local.storage_rowsource_types.RowRef) !void {
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
            const cursor = try local.sql_lake_cursor.openPinned(a, self.table, .{ .fields = fields.items, .limit = 1024 }, self.context, self.source);
            defer cursor.close(cursor.ptr);
            var result = Set.init(a);
            errdefer result.deinit();
            var row_arena = std.heap.ArenaAllocator.init(a);
            defer row_arena.deinit();
            var page_arena = std.heap.ArenaAllocator.init(a);
            defer page_arena.deinit();
            var lookup = std.heap.ArenaAllocator.init(a);
            defer lookup.deinit();
            var bitmap_cache: std.StringHashMapUnmanaged(Bitmap) = .empty;
            while (true) {
                try self.context.ensureActive();
                _ = page_arena.reset(.retain_capacity);
                const page = try cursor.next_columns.?(cursor.ptr, page_arena.allocator(), 1024);
                try page.validate();
                if (page.native != null) return error.InvalidSqlBackendResponse;
                for (page.selection, 0..) |position, row| {
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
                    if (try input.matches(ra, id, doc)) try self.addMatch(lookup.allocator(), &result, &bitmap_cache, ref);
                }
                if (page.after == null) break;
            }
            return .{ .bitmap = result };
        }
    };
}

// Plan the shared compiler's normalized IR, never a second JSON grammar.
fn column(path: Compiled.FieldPath) ?[]const u8 {
    return switch (path) {
        .single => |name| name,
        .dotted, .json_pointer => |parts| if (parts.len == 1) parts[0] else null,
    };
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
