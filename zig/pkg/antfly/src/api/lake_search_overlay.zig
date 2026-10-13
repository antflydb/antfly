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

//! Query-owned accepted-WAL suffix. Stable keys resolve visibility before
//! native ranking and pagination; one composed corpus supplies global BM25.
const std = @import("std");
const local = @import("antfly_local_sources");
const ingestion = @import("../serverless/lake_ingestion.zig");
const search = local.storage_db_query_search_exec;
const mapper = local.storage_db_document_mapper;
const A = std.mem.Allocator;
const V = std.json.Value;
const Bitmap = local.encoding_roaring.RoaringBitmap;
pub const Overlay = struct {
    pending: ingestion.Pending,
    rows: std.StringHashMapUnmanaged(V) = .empty,
    key_filter: []const u8,
    physical: ?@import("lake_index_physical_set.zig").Set = null,
    pub fn init(a: A, pending: ingestion.Pending) !Overlay {
        var result: Overlay = .{ .pending = pending, .key_filter = "" };
        var disjunction: std.ArrayList(V) = .empty;
        for (pending.changes) |change| {
            var conjunction: std.ArrayList(V) = .empty;
            var identity: std.ArrayList(V) = .empty;
            for (pending.key_fields) |key| {
                const value = change.row.object.get(key) orelse return error.InvalidWal;
                try identity.append(a, value);
                const clause = try std.json.Stringify.valueAlloc(a, .{ .term = .{ .path = try std.fmt.allocPrint(a, "/{s}", .{key}), .value = value } }, .{});
                try conjunction.append(a, try std.json.parseFromSliceLeaky(V, a, clause, .{}));
            }
            const clause = try std.json.Stringify.valueAlloc(a, .{ .conjuncts = conjunction.items }, .{});
            try disjunction.append(a, try std.json.parseFromSliceLeaky(V, a, clause, .{}));
            if (change.op == .upsert) {
                const id = try std.fmt.allocPrint(a, "wal1:{s}", .{local.serverless_external_source_mod.lake_catalog.types.digestHex(try std.json.Stringify.valueAlloc(a, identity.items, .{}))});
                try result.rows.put(a, id, change.row);
            }
        }
        result.key_filter = try std.json.Stringify.valueAlloc(a, .{ .disjuncts = disjunction.items }, .{});
        return result;
    }
    pub fn row(self: *const Overlay, key: []const u8) ?V {
        return self.rows.get(key);
    }
    /// Build the recent corpus once, independently of archive admission. Its
    /// immutable statistics are added to published base statistics, matching
    /// compose's existing BM25 contract (tombstones do not subtract statistics).
    pub fn recent(self: *const Overlay, a: A, name: []const u8, analysis: local.introducer.TextAnalysisConfig, schema: ?local.storage_schema.TableSchema, field: ?[]const u8, context: local.serverless_query_lake_read_context.Context) !search.PinnedTextSource {
        var writer = try local.index.IndexWriter.init(a);
        defer writer.deinit();
        try self.appendRecent(a, &writer, analysis, schema, field, context);
        return .{ .snapshot = writer.acquireSnapshot(), .name = name, .text_analysis = analysis, .runtime_schema = schema, .selected_field = field };
    }
    fn appendRecent(self: *const Overlay, a: A, writer: *local.index.IndexWriter, analysis: local.introducer.TextAnalysisConfig, schema: ?local.storage_schema.TableSchema, field: ?[]const u8, context: local.serverless_query_lake_read_context.Context) !void {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(scratch, analysis, schema, null, field);
        const ids = try scratch.alloc([]const u8, self.rows.count());
        var iterator = self.rows.keyIterator();
        for (ids) |*id| id.* = iterator.next().?.*;
        std.mem.sort([]const u8, ids, {}, struct {
            fn less(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.lessThan(u8, l, r);
            }
        }.less);
        for (ids) |id| {
            try context.ensureActive();
            try builder.appendSourceDoc(.{ .key = id, .root = self.rows.get(id).?, .stored_data = "", .typed_source = null });
        }
        try context.ensureActive();
        const encoded = try mapper.buildTextSegmentsFromProjectionBatch(a, builder.batch(), analysis, .{ .target_segment_bytes = 8 * 1024 * 1024, .target_build_memory_bytes = 32 * 1024 * 1024, .store_document_source = false });
        defer mapper.freeTextSegments(a, encoded);
        for (encoded) |bytes| {
            try context.ensureActive();
            try writer.addSegment(bytes);
        }
    }
    /// Transfers base only on success. Global frequencies come from immutable
    /// preparation, while exact per-segment bounds keep their original owner.
    pub fn window(base: search.PinnedTextSource, excluded: *const Bitmap, collection: @import("lake_index_native_text.zig").Collection, preparation: *const local.search_search.TextNavigationPlan, context: local.serverless_query_lake_read_context.Context) !search.PinnedTextSource {
        const a = base.snapshot.inventoryAllocator();
        var writer = try local.index.IndexWriter.init(a);
        defer writer.deinit();
        try shareMasked(a, &writer, base, excluded, context);
        const fields = try a.alloc(local.index.IndexWriter.CollectionField, collection.fields.len);
        defer a.free(fields);
        for (fields, collection.fields) |*out, field| out.* = .{ .name = field.name, .total_length = field.total_length };
        try writer.sealImmutableCollection(collection.documents, fields);
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .base = base, .preparation = preparation, .context = context };
        const snapshot = try writer.acquireSnapshotWithReadCapability(.{ .ptr = owner, .check = Owner.check, .io = context.io });
        snapshot.text_statistics = .{ .ptr = owner, .check = Owner.check, .frequencies = Owner.frequencies, .summary = Owner.summary };
        return .{ .snapshot = snapshot, .read_context = owner, .name = base.name, .text_analysis = base.text_analysis, .runtime_schema = base.runtime_schema, .selected_field = base.selected_field, .provider_metadata = base.provider_metadata, .owner = owner, .release_owner = Owner.release };
    }
    fn shareMasked(a: A, writer: *local.index.IndexWriter, base: search.PinnedTextSource, excluded: *const Bitmap, context: local.serverless_query_lake_read_context.Context) !void {
        const sources = try a.alloc(local.index.IndexWriter.ImmutableSegment, base.snapshot.segments.len);
        defer a.free(sources);
        const masks = try a.alloc(Bitmap, sources.len);
        defer a.free(masks);
        var made: usize = 0;
        defer for (masks[0..made]) |*mask| mask.deinit();
        var offset: u32 = 0;
        var iterator = excluded.iterator();
        var next = iterator.next();
        for (sources, masks, base.snapshot.segments, 0..) |*source, *mask, segment, ordinal| {
            try context.ensureActive();
            source.* = .{ .snapshot = base.snapshot, .ordinal = ordinal, .target_id = ordinal + 1 };
            mask.* = Bitmap.init(a);
            made += 1;
            while (next) |doc| {
                if (doc >= @as(u64, offset) + segment.reader.doc_count) break;
                if (doc >= offset) try mask.add(doc - offset);
                next = iterator.next();
            }
            offset = try std.math.add(u32, offset, segment.reader.doc_count);
        }
        try writer.shareMaskedImmutableSegments(sources, masks);
    }
    /// Corpus owners and range capability outlive the composed facade.
    const Owner = struct {
        a: A,
        base: search.PinnedTextSource,
        preparation: ?*const local.search_search.TextNavigationPlan = null,
        context: local.serverless_query_lake_read_context.Context = .{},
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.context.ensureActive();
            try self.base.snapshot.checkScoringReadContext();
        }
        fn frequencies(raw: *anyopaque, _: A, field: []const u8, terms: []const []const u8, out: []u32) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.context.ensureActive();
            try self.preparation.?.preparedFrequencies(field, terms, out);
        }
        fn summary(raw: *anyopaque, a: A, segment: usize, field: []const u8, term: []const u8, avg: f32, config: local.section_inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.context.ensureActive();
            if (self.base.snapshot.text_statistics) |provider| return provider.summary(provider.ptr, a, segment, field, term, avg, config);
            return .{ .frequency = self.base.snapshot.segments[segment].reader.doc_count, .tf_upper = std.math.inf(f32) };
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.a;
            var base = self.base;
            a.destroy(self);
            base.deinit();
        }
    };
    /// The input pin transfers only on success. Tombstones have private shared
    /// cells, so archive readers cannot see another query's pending deletes.
    pub fn compose(self: *const Overlay, base: search.PinnedTextSource, excluded: *const Bitmap, context: local.serverless_query_lake_read_context.Context) !search.PinnedTextSource {
        const a = base.snapshot.inventoryAllocator();
        var writer = try local.index.IndexWriter.init(a);
        defer writer.deinit();
        try shareMasked(a, &writer, base, excluded, context);
        try self.appendRecent(a, &writer, base.text_analysis, base.runtime_schema, base.selected_field, context);
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .base = base };
        const snapshot = if (base.snapshot.read_context_check) |check|
            try writer.acquireSnapshotWithReadCapability(.{ .ptr = base.snapshot.read_context.?, .check = check, .io = base.snapshot.read_context_io })
        else if (base.read_context) |read_context|
            try writer.acquireSnapshotWithReadContext(read_context)
        else
            writer.acquireSnapshot();
        return .{ .snapshot = snapshot, .read_context = base.read_context, .name = base.name, .text_analysis = base.text_analysis, .runtime_schema = base.runtime_schema, .selected_field = base.selected_field, .owner = owner, .release_owner = Owner.release };
    }
};
/// Coverage is an attribute of each snapshot, not the current table watermark.
/// Pre-native snapshots have no WAL changes. Older native snapshots missing the
/// proof fail closed instead of silently double-applying an accepted prefix.
pub fn snapshotCoverage(metadata: V, snapshot_id: []const u8) !u64 {
    const catalog = local.serverless_external_source_mod.lake_catalog;
    if (try emptySnapshot(metadata, snapshot_id)) return 0;
    const id = try std.fmt.parseInt(i64, snapshot_id, 10);
    for ((try catalog.metadata.get(metadata, "snapshots")).array.items) |snapshot| {
        if (try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id")) != id) continue;
        const summary = (try catalog.metadata.get(snapshot, "summary")).object;
        if (summary.get("antfly.wal.coverage")) |value| return std.fmt.parseInt(u64, try catalog.metadata.str(value), 10);
        if (!summary.contains("antfly.batch-id") and !summary.contains("antfly.compaction")) return 0;
        if (try catalog.metadata.int(try catalog.metadata.get(metadata, "current-snapshot-id")) == id) {
            const value = (try catalog.metadata.get(metadata, "properties")).object.get("antfly.wal.coverage") orelse return error.LakeOverlayCoverageUnavailable;
            return std.fmt.parseInt(u64, try catalog.metadata.str(value), 10);
        }
        return error.LakeOverlayCoverageUnavailable;
    }
    return error.ExternalLakeSnapshotMismatch;
}

/// Pending rows reconstruct only native transitions. An unpublished external
/// commit must rebuild its archive index before this overlay can serve it.
pub fn requireNativeAncestry(metadata: V, base_id: []const u8) !void {
    const m = local.serverless_external_source_mod.lake_catalog.metadata;
    if (try emptySnapshot(metadata, base_id)) {
        if ((try m.get(metadata, "snapshots")).array.items.len == 0) return;
        return error.LakeOverlayCoverageUnavailable;
    }
    const base = try std.fmt.parseInt(i64, base_id, 10);
    var current = try m.int(try m.get(metadata, "current-snapshot-id"));
    const snapshots = (try m.get(metadata, "snapshots")).array.items;
    for (0..snapshots.len + 1) |_| {
        if (current == base) return;
        var found = false;
        for (snapshots) |snapshot| {
            if (try m.int(try m.get(snapshot, "snapshot-id")) != current) continue;
            const summary = (try m.get(snapshot, "summary")).object;
            if (!summary.contains("antfly.wal.coverage") or (!summary.contains("antfly.batch-id") and !summary.contains("antfly.compaction"))) return error.LakeOverlayCoverageUnavailable;
            current = try m.int(snapshot.object.get("parent-snapshot-id") orelse return error.LakeOverlayCoverageUnavailable);
            found = true;
            break;
        }
        if (!found) break;
    }
    return error.LakeOverlayCoverageUnavailable;
}

test "external lake overlays reject unpublished external ancestry and cycles" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a,
        \\{"current-snapshot-id":3,"snapshots":[{"snapshot-id":3,"parent-snapshot-id":2,"summary":{"antfly.wal.coverage":"2","antfly.compaction":"c"}},{"snapshot-id":2,"parent-snapshot-id":1,"summary":{"antfly.wal.coverage":"1","antfly.batch-id":"b"}},{"snapshot-id":1,"summary":{"operation":"append"}}]}
    , .{});
    try requireNativeAncestry(root, "1");
    try std.testing.expectEqual(@as(u64, 2), try snapshotCoverage(root, "3"));
    try std.testing.expectEqual(@as(u64, 0), try snapshotCoverage(root, "1"));
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, requireNativeAncestry(root, "0"));
    var changed = root;
    var summary = changed.object.get("snapshots").?.array.items[1].object.get("summary").?;
    _ = summary.object.swapRemove("antfly.wal.coverage");
    try changed.object.get("snapshots").?.array.items[1].object.put(a, "summary", summary);
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, requireNativeAncestry(changed, "1"));
}

fn emptySnapshot(metadata: V, id: []const u8) !bool {
    if (!std.mem.startsWith(u8, id, "empty:")) return false;
    const m = local.serverless_external_source_mod.lake_catalog.metadata;
    const uuid = try m.str(try m.get(metadata, "table-uuid"));
    const identity = id["empty:".len..];
    if (identity.len != uuid.len + 1 + 64 or !std.mem.startsWith(u8, identity, uuid) or identity[uuid.len] != ':') return error.ExternalLakeSnapshotMismatch;
    return true;
}

test "external lake overlay recognizes the synthetic empty baseline without assuming ancestry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a, "{\"table-uuid\":\"table-1\",\"current-snapshot-id\":null,\"snapshots\":[]}", .{});
    const id = "empty:table-1:" ++ "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    try std.testing.expectEqual(@as(u64, 0), try snapshotCoverage(root, id));
    try requireNativeAncestry(root, id);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, snapshotCoverage(root, "empty:other-1:" ++ "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
}

test "external lake bounded WAL windows preserve composed BM25 and private masks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var archive: Overlay = .{ .pending = undefined, .key_filter = "" };
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    // Independent source segments exercise per-window document ordinals.
    for (0..67) |i| {
        const id = try std.fmt.allocPrint(sa, "base-{d:0>3}", .{i});
        const row = try std.json.parseFromSliceLeaky(V, sa, if (i % 3 == 0) "{\"body\":\"common rare rare\"}" else "{\"body\":\"common\"}", .{});
        archive.rows.clearRetainingCapacity();
        try archive.rows.put(sa, id, row);
        try archive.appendRecent(a, &writer, .{}, null, "body", .{});
    }
    var changes: Overlay = .{ .pending = undefined, .key_filter = "" };
    try changes.rows.put(sa, "wal1:new", try std.json.parseFromSliceLeaky(V, sa, "{\"body\":\"common recent recent recent\"}", .{}));
    try changes.rows.put(sa, "wal1:replacement", try std.json.parseFromSliceLeaky(V, sa, "{\"body\":\"common rare\"}", .{}));
    var base = try (search.PinnedTextSource{ .snapshot = writer.acquireSnapshot(), .name = "text", .text_analysis = .{}, .runtime_schema = null, .selected_field = "body" }).share(a);
    defer base.deinit();
    var excluded = Bitmap.init(a);
    defer excluded.deinit();
    try excluded.add(0);
    try excluded.add(65);
    var reference = try changes.compose(try base.clone(), &excluded, .{});
    defer reference.deinit();
    const Stats = struct {
        fn average(raw: *anyopaque, field: []const u8) f32 {
            const snapshot: *local.index.IndexSnapshot = @ptrCast(@alignCast(raw));
            return snapshot.textAvgDocLen(field);
        }
        fn frequencies(raw: *anyopaque, alloc: A, field: []const u8, terms: []const []const u8, output: []u32) !void {
            const snapshot: *local.index.IndexSnapshot = @ptrCast(@alignCast(raw));
            try snapshot.termDocFreqs(alloc, field, terms, output);
        }
    };
    var collection_fields: std.ArrayList(@import("lake_index_native_text.zig").Collection.Field) = .empty;
    var totals = reference.snapshot.global_total_field_len.iterator();
    while (totals.next()) |field| try collection_fields.append(sa, .{ .name = field.key_ptr.*, .total_length = field.value_ptr.* });
    const collection: @import("lake_index_native_text.zig").Collection = .{ .documents = reference.snapshot.scoringDocCount(), .fields = collection_fields.items };
    var recent_pin = try (try changes.recent(a, "text", .{}, null, "body", .{})).share(a);
    defer recent_pin.deinit();
    const queries = [_]local.search_search.SearchQuery{
        .{ .term = .{ .field = "body", .term = "common" } },
        .{ .match = .{ .field = "body", .text = "rare recent" } },
        .{ .phrase = .{ .field = "body", .text = "rare rare" } },
        .{ .bool_query = .{ .must = &.{.{ .term = .{ .field = "body", .term = "common", .boost = 2 } }}, .should = &.{.{ .term = .{ .field = "body", .term = "rare", .boost = 3 } }} } },
    };
    for (queries) |query| {
        const prepared = (try local.search_search.TextNavigationPlan.prepare(a, query, .{ .ptr = reference.snapshot, .count = collection.documents, .average = Stats.average, .frequencies = Stats.frequencies }, .{})).?;
        defer prepared.deinit();
        var expected = try local.search_search.execute(a, reference.snapshot, .{ .query = query, .k = 100, .include_stored = false });
        defer expected.deinit();
        var seen: usize = 0;
        for (0..68) |window_index| {
            var pin: search.PinnedTextSource = undefined;
            var mask = Bitmap.init(a);
            defer mask.deinit();
            if (window_index == 67) {
                pin = try recent_pin.clone();
            } else {
                var window_writer = try local.index.IndexWriter.init(a);
                defer window_writer.deinit();
                try window_writer.shareImmutableSegments(&.{.{ .snapshot = base.snapshot, .ordinal = window_index, .target_id = 1 }});
                pin = .{ .snapshot = window_writer.acquireSnapshot(), .name = "text", .text_analysis = .{}, .runtime_schema = null, .selected_field = "body" };
                if (excluded.contains(@intCast(window_index))) try mask.add(0);
            }
            const window = Overlay.window(pin, &mask, collection, prepared, .{}) catch |err| {
                pin.deinit();
                return err;
            };
            var owned = window;
            defer owned.deinit();
            try std.testing.expectEqual(collection.documents, window.snapshot.scoringDocCount());
            var result = try local.search_search.execute(a, window.snapshot, .{ .query = query, .k = 100, .include_stored = false, .text_navigation_plan = prepared });
            defer result.deinit();
            for (result.hits) |hit| {
                const offset: u32 = if (window_index == 67) 67 else @intCast(window_index);
                const original = offset + hit.doc_id;
                const matched = for (expected.hits) |candidate| {
                    if (candidate.doc_id == original) break candidate;
                } else return error.UnexpectedWindowMatch;
                try std.testing.expectEqual(matched.score, hit.score);
                seen += 1;
            }
        }
        try std.testing.expectEqual(expected.hits.len, seen);
    }
    // Another query retains the unmasked archive generation.
    try std.testing.expectEqual(@as(u32, 67), base.snapshot.liveDocCount());
}

test "external lake masked overlays retain physical allocation ownership and query cancellation" {
    const a = std.testing.allocator;
    var writer = try local.index.IndexWriter.init(a);
    var writer_open = true;
    defer if (writer_open) writer.deinit();
    var segment = local.segment.SegmentWriter.init(a);
    defer segment.deinit();
    try segment.addStoredDoc("one", "{}");
    try segment.addStoredDoc("two", "{}");
    const bytes = try segment.build();
    defer a.free(bytes);
    try writer.addSegment(bytes);
    const Control = struct {
        cancelled: bool = false,
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.cancelled) return error.Canceled;
        }
    };
    var control: Control = .{};
    const facade = try writer.acquireSnapshotWithReadCapability(.{ .ptr = &control, .check = Control.check });
    var base: search.PinnedTextSource = .{ .snapshot = facade, .read_context = &control, .name = "text", .text_analysis = .{}, .runtime_schema = null };
    var transferred = false;
    defer if (!transferred) base.deinit();
    try std.testing.expect(facade.alloc.ptr != a.ptr or facade.alloc.vtable != a.vtable);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const overlay = try Overlay.init(arena.allocator(), .{ .lsn = 1, .key_fields = &.{"id"}, .changes = &.{} });
    var mask = Bitmap.init(a);
    defer mask.deinit();
    try mask.add(1);
    var joined = try overlay.compose(base, &mask, .{});
    transferred = true;
    defer joined.deinit();
    try std.testing.expect(joined.snapshot.read_context_check != null);
    try std.testing.expectEqual(@as(u32, 1), joined.snapshot.segments[0].shared.deleted_count.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), base.snapshot.segments[0].shared.deleted_count.load(.acquire));
    writer.deinit();
    writer_open = false;
    try joined.snapshot.checkScoringReadContext();
    try std.testing.expect(control.checks > 0);
    control.cancelled = true;
    try std.testing.expectError(error.Canceled, joined.snapshot.checkScoringReadContext());
}
