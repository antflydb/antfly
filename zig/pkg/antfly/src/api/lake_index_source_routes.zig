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

//! Paged inverse routing: immutable source file/rank -> serving segment.
//! Ordered row consumers seek one predecessor; no file-sized segment fan-out.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const predicates = @import("lake_index_text_predicate.zig");
const A = std.mem.Allocator;
pub const Route = struct {
    segment: [32]u8,
    file: [32]u8,
    physical: artifacts.ChunkRef,
    file_rows: u32,
    file_lower: u32,
    count: u32,
};
pub fn mutate(a: A, changes: *corpus.text_directory.Mutations, group: corpus.FileGroup, deleting: bool) !void {
    if (group.sources.len == 0 or group.segments.len != 1) return error.InvalidNativeLakeTextCorpus;
    const segment = corpus.term_directory.identity(group.segments[0].artifact_id);
    for (group.sources) |span| {
        var key: [36]u8 = undefined;
        key[0..32].* = corpus.term_directory.identity(span.file.id);
        std.mem.writeInt(u32, key[32..36], span.file_lower, .big);
        const bytes = try std.json.Stringify.valueAlloc(a, Route{ .segment = segment, .file = key[0..32].*, .physical = span.physical, .file_rows = span.file_rows, .file_lower = span.file_lower, .count = span.count }, .{});
        defer a.free(bytes);
        try changes.addPrefix(&key, if (deleting) null else bytes, bytes.len);
    }
}
pub fn lookup(a: A, storage: tree.Store, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: @import("antfly_cancellation").CancellationToken, live: *predicates.LiveRowsCache, row: local.storage_rowsource_types.RowRef) !?[32]u8 {
    try cached.context.ensureActive();
    try token.check();
    if (root.source_routes_version != 1 or row != .external) return error.InvalidNativeLakeTextCorpus;
    const file = row.external.file_id;
    const prefix = corpus.term_directory.identity(file);
    const initial: Route = blk: {
        var cursor = try tree.Cursor.init(a, storage, root.source_routes, &prefix, null);
        defer cursor.deinit();
        const entry = (try cursor.next()) orelse return null;
        if (entry.key.len != 40 or !std.mem.eql(u8, entry.key[0..32], &prefix)) return null;
        break :blk try decode(a, root, entry.key, entry.value, file);
    };
    const rank = (try predicates.Identities.sourceOrdinal(a, live, store, cached, initial.physical, initial.file_rows, row)) orelse return null;
    var upper: [41]u8 = @splat(0);
    upper[0..32].* = prefix;
    std.mem.writeInt(u32, upper[32..36], rank, .big);
    var cursor = try tree.Cursor.initReverse(a, storage, root.source_routes, &prefix, &upper);
    defer cursor.deinit();
    const entry = (try cursor.next()) orelse return null;
    const route = try decode(a, root, entry.key, entry.value, file);
    if (rank < route.file_lower or rank - route.file_lower >= route.count) return null;
    return route.segment;
}
/// Turn a bounded physical predicate proof into serving-pack admission.
/// The retained predicate is rechecked during ranking; this is only a superset.
pub fn select(a: A, storage: tree.Store, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: @import("antfly_cancellation").CancellationToken, selected: *const @import("lake_index_physical_set.zig").Set, source: []const u8, snapshot: []const u8) ![]const [32]u8 {
    if (selected.boundedCardinality(4096) != null) return selectPoints(a, storage, root, store, cached, token, selected, source, snapshot);
    if (selected.files.count() > 256) return error.QueryCandidateBudgetExceeded;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var live: predicates.LiveRowsCache = .{};
    defer live.deinit(a);
    var ids: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer ids.deinit(a);
    var work: usize = 0;
    var routes: usize = 0;
    var files = selected.files.keyIterator();
    while (files.next()) |file| {
        _ = scratch.reset(.free_all);
        const sa = scratch.allocator();
        const prefix = corpus.term_directory.identity(file.*);
        var start = try tree.Cursor.init(sa, storage, root.source_routes, &prefix, null);
        defer start.deinit();
        const initial = (try start.next()) orelse continue;
        if (initial.key.len != 40 or !std.mem.eql(u8, initial.key[0..32], &prefix)) continue;
        const first = try decode(sa, root, initial.key, initial.value, file.*);
        const bounds = (try predicates.Identities.sourceBounds(sa, &live, store, cached, first.physical, first.file_rows, selected, file.*)) orelse continue;
        var fence: [41]u8 = @splat(0);
        fence[0..32].* = prefix;
        std.mem.writeInt(u32, fence[32..36], bounds.lower, .big);
        var predecessor = try tree.Cursor.initReverse(sa, storage, root.source_routes, &prefix, &fence);
        defer predecessor.deinit();
        const prior = (try predecessor.next()) orelse return error.InvalidNativeLakeTextCorpus;
        const lower = try sa.dupe(u8, prior.key);
        std.mem.writeInt(u32, fence[32..36], bounds.upper, .big);
        var cursor = try tree.Cursor.init(sa, storage, root.source_routes, lower, fence[0..36]);
        defer cursor.deinit();
        while (try cursor.next()) |entry| {
            try cached.context.ensureActive();
            try token.check();
            if (routes == 4096) return error.QueryCandidateBudgetExceeded;
            routes += 1;
            const route = try decode(sa, root, entry.key, entry.value, file.*);
            if (route.file_rows != first.file_rows or !std.mem.eql(u8, route.physical.artifact_id, first.physical.artifact_id)) return error.InvalidNativeLakeTextCorpus;
            if (!try predicates.Identities.sourceRangeSelected(sa, &live, store, cached, route.physical, route.file_rows, selected, file.*, route.file_lower, route.count, &work)) continue;
            try ids.put(a, route.segment, {});
            // Admission is optional: stop before proving a near-full inventory.
            if (root.segment_count != 0 and ids.count() * 4 >= @as(usize, root.segment_count) * 3) return error.QueryCandidateBudgetExceeded;
        }
    }
    const result = try a.alloc([32]u8, ids.count());
    var it = ids.keyIterator();
    for (result) |*id| id.* = it.next().?.*;
    std.mem.sort([32]u8, result, {}, struct {
        fn less(_: void, l: [32]u8, r: [32]u8) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.less);
    return result;
}
fn selectPoints(a: A, storage: tree.Store, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: @import("antfly_cancellation").CancellationToken, selected: *const @import("lake_index_physical_set.zig").Set, source: []const u8, snapshot: []const u8) ![]const [32]u8 {
    if (selected.boundedCardinality(4096) == null) return error.QueryCandidateBudgetExceeded;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var live: predicates.LiveRowsCache = .{};
    defer live.deinit(a);
    var ids: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer ids.deinit(a);
    var files = selected.files.iterator();
    while (files.next()) |file| {
        var blocks = file.value_ptr.iterator();
        while (blocks.next()) |block| {
            var rows = block.value_ptr.iterator();
            while (rows.next()) |low| {
                try cached.context.ensureActive();
                try token.check();
                _ = scratch.reset(.retain_capacity);
                const row: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = source, .snapshot_id = snapshot, .file_id = file.key_ptr.*, .row_group_ordinal = block.key_ptr.group, .row_ordinal = (@as(u64, block.key_ptr.high) << 32) | low } };
                if (try lookup(scratch.allocator(), storage, root, store, cached, token, &live, row)) |id| try ids.put(a, id, {});
            }
        }
    }
    const result = try a.alloc([32]u8, ids.count());
    var it = ids.keyIterator();
    for (result) |*id| id.* = it.next().?.*;
    std.mem.sort([32]u8, result, {}, struct {
        fn less(_: void, left: [32]u8, right: [32]u8) bool {
            return std.mem.lessThan(u8, &left, &right);
        }
    }.less);
    return result;
}
fn decode(a: A, root: corpus.Root, key: []const u8, bytes: []const u8, file: []const u8) !Route {
    if (key.len != 40 or std.mem.readInt(u32, key[36..40], .big) != 0) return error.InvalidNativeLakeTextCorpus;
    const route = try std.json.parseFromSliceLeaky(Route, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &route.file, &corpus.term_directory.identity(file)) or std.mem.readInt(u32, key[32..36], .big) != route.file_lower or route.count == 0 or @as(u64, route.file_lower) + route.count > route.file_rows) return error.InvalidNativeLakeTextCorpus;
    try corpus.text_directory.validateRef(route.physical, root.domain);
    return route;
}

test "external lake bounded WAL compressed predicate spans route broad rows across delete holes and high coordinates" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("compressed-predicate-routes");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var cancellation: @import("antfly_cancellation").CancellationToken = .none;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*@import("antfly_cancellation").CancellationToken, @ptrCast(@alignCast(raw))).check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &cancellation, .checkpoint = Check.call };
    defer manager.deinit();
    var serving = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer serving.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &serving, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    for ([_]usize{ 2, 4 }) |pack_count| for ([_]bool{ false, true }) |holes| {
        var mutations = corpus.text_directory.Mutations.init(a, &manager, pages.store());
        defer mutations.deinit();
        var live = local.encoding_roaring.RoaringBitmap.init(a);
        defer live.deinit();
        try live.addRange(0, 10000);
        try live.addRange(20000, 50000);
        const bitmap = try live.toBytes(ca);
        var bitmap_object = try store.put(bitmap);
        defer bitmap_object.deinit(a);
        const file: @import("lake_index_native_state.zig").File = .{ .id = if (holes) "holes" else "contiguous", .digest = @splat(7) };
        const group: corpus.FileGroup = .{ .file = file, .segments = &.{}, .rows = &.{.{ .group = 7, .high = 1 << 20, .base = 0, .lower = 0, .count = 40000, .bitmap = if (holes) .{ .artifact_id = bitmap_object.artifact_id, .checksum = bitmap_object.checksum, .byte_len = bitmap_object.byte_len } else null }} };
        var physical = try store.put(try std.json.Stringify.valueAlloc(ca, group, .{}));
        defer physical.deinit(a);
        var refs: [4]artifacts.ChunkRef = undefined;
        for (refs[0..pack_count], 0..) |*ref, i| {
            var object = try store.put(try std.fmt.allocPrint(ca, "pack-{s}-{d}", .{ file.id, i }));
            defer object.deinit(a);
            ref.* = .{ .artifact_id = try ca.dupe(u8, object.artifact_id), .checksum = try ca.dupe(u8, object.checksum), .byte_len = object.byte_len };
            try mutate(ca, &mutations, .{ .file = file, .segments = &.{ref.*}, .sources = &.{.{ .file = file, .physical = .{ .artifact_id = physical.artifact_id, .checksum = physical.checksum, .byte_len = physical.byte_len }, .file_rows = 40000, .file_lower = @intCast(i * (40000 / pack_count)), .pack_lower = 0, .count = @intCast(40000 / pack_count) }} }, false);
        }
        const root: corpus.Root = .{ .domain = pages.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .segment_count = @intCast(pack_count), .source_routes_version = 1, .source_routes = try mutations.finish(null) };
        var selected = @import("lake_index_physical_set.zig").Set.init(a);
        defer selected.deinit();
        try selected.addBlock(.{ .file = file.id, .group = 7, .base = @as(u64, 1) << 40, .selection = .{ .interval = .{ .lower = if (holes) 30000 else 20000, .count = 10000 } } });
        if (holes) try selected.addBlock(.{ .file = file.id, .group = 7, .base = @as(u64, 1) << 40, .selection = .{ .interval = .{ .lower = 15000, .count = 4000 } } });
        const found = try select(ca, pages.store(), root, store, cached, .none, &selected, "source", "snapshot");
        try std.testing.expectEqual(@as(usize, 1), found.len);
        try std.testing.expectEqual(corpus.term_directory.identity(refs[pack_count / 2].artifact_id), found[0]);
        try selected.addBlock(.{ .file = file.id, .group = 7, .base = @as(u64, 1) << 40, .selection = .{ .interval = .{ .lower = 0, .count = 50000 } } });
        try std.testing.expectError(error.QueryCandidateBudgetExceeded, select(ca, pages.store(), root, store, cached, .none, &selected, "source", "snapshot"));
    };
}
