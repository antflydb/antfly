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
