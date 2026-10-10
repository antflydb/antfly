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
    /// Corpus owners and range capability outlive the composed facade.
    const Owner = struct {
        a: A,
        base: search.PinnedTextSource,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.base.deinit();
            self.a.destroy(self);
        }
    };
    /// The input pin transfers only on success. Tombstones have private shared
    /// cells, so archive readers cannot see another query's pending deletes.
    pub fn compose(self: *const Overlay, base: search.PinnedTextSource, excluded: *const Bitmap, context: local.serverless_query_lake_read_context.Context) !search.PinnedTextSource {
        const a = base.snapshot.alloc;
        var writer = try local.index.IndexWriter.init(a);
        defer writer.deinit();
        const sources = try a.alloc(local.index.IndexWriter.ImmutableSegment, base.snapshot.segments.len);
        defer a.free(sources);
        const masks = try a.alloc(Bitmap, sources.len);
        defer a.free(masks);
        var made: usize = 0;
        defer for (masks[0..made]) |*mask| mask.deinit();
        var offset: u32 = 0;
        var excluded_iterator = excluded.iterator();
        var next_excluded = excluded_iterator.next();
        for (sources, masks, base.snapshot.segments, 0..) |*source, *mask, segment, ordinal| {
            try context.ensureActive();
            source.* = .{ .snapshot = base.snapshot, .ordinal = ordinal, .target_id = ordinal + 1 };
            mask.* = Bitmap.init(a);
            made += 1;
            while (next_excluded) |doc| {
                if (doc >= @as(u64, offset) + segment.reader.doc_count) break;
                if (doc >= offset) try mask.add(doc - offset);
                next_excluded = excluded_iterator.next();
            }
            offset = try std.math.add(u32, offset, segment.reader.doc_count);
        }
        try writer.shareMaskedImmutableSegments(sources, masks);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(scratch, base.text_analysis, base.runtime_schema, null, base.selected_field);
        // Sort stable identities so ties and document ordinals survive restart.
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
        const encoded = try mapper.buildTextSegmentsFromProjectionBatch(a, builder.batch(), base.text_analysis, .{ .target_segment_bytes = 8 * 1024 * 1024, .target_build_memory_bytes = 32 * 1024 * 1024, .store_document_source = false });
        defer mapper.freeTextSegments(a, encoded);
        for (encoded) |bytes| {
            try context.ensureActive();
            try writer.addSegment(bytes);
        }
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .base = base };
        const snapshot = if (base.read_context) |read_context| try writer.acquireSnapshotWithReadContext(read_context) else writer.acquireSnapshot();
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
