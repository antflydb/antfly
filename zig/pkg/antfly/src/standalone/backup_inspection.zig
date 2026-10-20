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

//! Resolve standalone Lite tables through the metadata owner. Physical table
//! catalogs alone can lag a server definition or omit a newly created table.
const std = @import("std");
const local = @import("antfly_local_sources");
const staging = local.storage_lite_restore_staging;
const backend_mod = local.storage_lite_backend;
const metadata = @import("../metadata/storage/raft_apply_store.zig");

pub fn inspectInputJsonAlloc(alloc: std.mem.Allocator, input: []const u8, destination: ?[]const u8, options: staging.preflight.Options) ![]u8 {
    return staging.inspectInputJsonAllocWithLiteInspector(alloc, input, destination, options, inspectLite);
}

fn inspectLite(alloc: std.mem.Allocator, io: std.Io, backend: *backend_mod.Handle, stores: *staging.StoreInspection) !void {
    backend.requireEmbeddedCatalogOwnership() catch |err| switch (err) {
        error.StandaloneLiteCatalogRequiresApi => return inspectStandalone(alloc, backend, stores),
        else => return err,
    };
    return staging.inspectEmbeddedLite(alloc, io, backend, stores);
}

fn inspectStandalone(alloc: std.mem.Allocator, backend: *backend_mod.Handle, stores: *staging.StoreInspection) !void {
    var catalog = try metadata.RaftApplyStore.init(alloc, .{
        .root_dir = "",
        .read_only = true,
        .borrowed_store = try backend.runtimeStoreForNamespace("system/metadata"),
    });
    defer catalog.deinit();
    if (try catalog.standaloneRevision() == 0) return error.UnsupportedLiteInspectionLayout;
    const group = local.common_group_ids.main_metadata_group_id;
    const projection = try catalog.captureInspectionCatalog(alloc, group, stores.options.max_definitions, stores.options.max_metadata_bytes, stores.options.cancellation);
    defer catalog.freeTables(alloc, projection.tables);
    defer catalog.freeRanges(alloc, projection.ranges);
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(alloc);
    for (projection.tables) |table| {
        try stores.options.cancellation.check();
        // A mixed-engine table or active physical migration cannot be fully
        // inspected from this file alone. No partial compatible verdicts.
        if (table.storage.engine != .local or table.storage_migration != null or table.lake_index_catalog_json.len != 0) return error.UnsupportedLiteInspectionLayout;
        var count: usize = 0;
        for (projection.ranges) |range| {
            if (range.table_id != table.table_id) continue;
            count += 1;
            if ((try seen.getOrPut(alloc, range.group_id)).found_existing) return error.UnsupportedLiteInspectionLayout;
            const namespace = try std.fmt.allocPrint(alloc, "group-{d}/table-db", .{range.group_id});
            defer alloc.free(namespace);
            try stores.inspectNamespace(backend, namespace, &.{ table.indexes_json, table.replication_sources_json });
        }
        if (count != 1) return error.UnsupportedLiteInspectionLayout;
    }
    if (seen.count() != projection.ranges.len) return error.UnsupportedLiteInspectionLayout;
}

// Exercise the durable metadata owner and physical namespaces together. The
// producer exists only in authoritative metadata, as it can before reconcile.
test "backup preflight standalone Lite visits every table and authoritative producer" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "standalone.aflite" });
    defer a.free(path);
    {
        var backend = try backend_mod.Handle.openOrCreate(a, path, .{ .no_sync = true });
        defer backend.deinit();
        try backend.adoptEmbeddedRootAsNamespace("group-101/table-db");
        var catalog = try metadata.RaftApplyStore.init(a, .{ .root_dir = "", .borrowed_store = try backend.runtimeStoreForNamespace("system/metadata") });
        defer catalog.deinit();
        try catalog.updateStandaloneCatalog(local.common_group_ids.main_metadata_group_id, 0, .{
            .replace = true,
            .auxiliary_json = "{}",
            .tables = &.{
                .{ .table_id = 11, .name = "default" },
                .{ .table_id = 12, .name = "later", .indexes_json = "{\"vec\":{\"name\":\"vec\",\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}" },
            },
            .ranges = &.{
                .{ .table_id = 11, .group_id = 101, .range_id = 101, .doc_identity_shard_id = 101, .doc_identity_range_id = 101, .start_key = "" },
                .{ .table_id = 12, .group_id = 102, .range_id = 102, .doc_identity_shard_id = 102, .doc_identity_range_id = 102, .start_key = "" },
            },
        });
        const key = try local.storage_internal_keys.documentKeyAlloc(a, "row1");
        defer a.free(key);
        for ([_][]const u8{ "group-101/table-db", "group-102/table-db" }) |namespace| {
            const store = try backend.runtimeStoreForNamespace(namespace);
            var write = try store.beginWrite();
            errdefer write.abort();
            try write.put(key, "{\"body\":\"later namespace\"}");
            try write.commit();
        }
    }
    const destination = "{\"version\":1,\"configuration_id\":\"test\",\"requirements\":[]}";
    const json = try inspectInputJsonAlloc(a, path, destination, .{});
    defer a.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    const report = parsed.value;
    try std.testing.expect(report.object.get("completeness").?.object.get("scan_complete").?.bool);
    try std.testing.expectEqual(@as(i64, 2), report.object.get("inventory").?.object.get("documents").?.integer);
    try std.testing.expectEqual(@as(i64, 2), report.object.get("inventory").?.object.get("shards").?.integer);
    try std.testing.expect(report.object.get("inventory").?.object.get("requirements").?.array.items.len > 0);
    try std.testing.expect(!std.mem.eql(u8, "compatible", report.object.get("compatibility").?.object.get("verdict").?.string));
    // A generic embedded-only reader must never silently fall back to root.
    const generic = try staging.inspectInputJsonAlloc(a, path, destination, .{});
    defer a.free(generic);
    try std.testing.expect(std.mem.indexOf(u8, generic, "unsupported_lite_layout") != null);
    const byte_limited = try inspectInputJsonAlloc(a, path, destination, .{ .max_metadata_bytes = 1 });
    defer a.free(byte_limited);
    try std.testing.expect(std.mem.indexOf(u8, byte_limited, "limit_exceeded") != null);
    const limited = try inspectInputJsonAlloc(a, path, destination, .{ .max_definitions = 1 });
    defer a.free(limited);
    try std.testing.expect(std.mem.indexOf(u8, limited, "limit_exceeded") != null);
}

test "backup preflight standalone marker without authoritative metadata is incomplete" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "missing-catalog.aflite" });
    defer a.free(path);
    {
        var backend = try backend_mod.Handle.openOrCreate(a, path, .{ .no_sync = true });
        defer backend.deinit();
        try backend.markStandaloneArtifact();
    }
    const json = try inspectInputJsonAlloc(a, path, "{\"version\":1,\"configuration_id\":\"test\",\"requirements\":[]}", .{});
    defer a.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    try std.testing.expect(!parsed.value.object.get("completeness").?.object.get("scan_complete").?.bool);
    try std.testing.expectEqualStrings("unsupported_lite_layout", parsed.value.object.get("completeness").?.object.get("failure_code").?.string);
    try std.testing.expectEqualStrings("unknown", parsed.value.object.get("compatibility").?.object.get("verdict").?.string);
}
