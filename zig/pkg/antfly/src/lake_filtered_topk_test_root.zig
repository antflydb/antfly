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

//! Focused native filtering and tie-pagination qualification without HTTP routes.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
test {
    _ = @import("api/lake_index_ordered_rows.zig");
    _ = @import("api/lake_index_text_query.zig");
    _ = @import("antfly_local_sources").search_search;
    _ = @import("antfly_local_sources").sparse_sparse;
}

test "external lake warm tie pagination distinct tuples retain sequential reads" {
    const std = @import("std");
    const local = @import("antfly_local_sources");
    const ordered = @import("api/lake_index_ordered_rows.zig");
    const public = @import("api/lake_index_public_order.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("review-distinct-public-ties");
    defer directory.cleanup();
    var fs = try @import("serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(5), .attempt = @splat(1) };
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const count = 5000;
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, 1);
    files[0] = .{ .file_id = @constCast("file"), .object_uri = @constCast("file://fixture"), .byte_len = 1, .row_count = count, .row_groups = &.{} };
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = files };
    var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 512 * 1024);
    defer sort.deinit();
    for (0..count) |row| {
        const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = "file", .row_group_ordinal = 0, .row_ordinal = row } };
        var key: [24]u8 = undefined;
        std.mem.writeInt(u64, key[0..8], row, .big);
        @memcpy(key[8..], &ordered.coordinate(0, ref.external));
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = row });
    }
    const artifact = try ordered.publish(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{}, .none);
    const root = try ordered.loadRoot(ca, store, artifact, .none, null);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: @import("api/lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    for ([_]bool{ false, true }) |reverse| {
        var reader: ordered.Reader = undefined;
        try reader.initCached(a, &store, root, @splat(3), "", null, .none, cached);
        defer reader.deinit();
        var cursor = try public.Cursor.init(a, &reader, "", null, null, null, false, reverse);
        defer cursor.deinit();
        const page = try cursor.next(ca, count);
        try std.testing.expectEqual(@as(usize, count), page.len);
        for (page, 0..) |ref, i| try std.testing.expectEqual(@as(u64, if (reverse) count - i - 1 else i), ref.external.row_ordinal);
    }
}
