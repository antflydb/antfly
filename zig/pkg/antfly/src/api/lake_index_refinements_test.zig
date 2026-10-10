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

const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");

test "external lake standalone object provider uses durable journal continuations" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-provider-journal");
    defer directory.cleanup();
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{directory.path()});
    defer a.free(uri);
    const Provider = @import("../serverless/artifacts/object_store.zig").ObjectStore;
    const scope = try stores.UploadScope.forPublication(@splat(8), 1, std.testing.io);
    {
        var provider = try Provider.initFileUri(a, uri);
        defer provider.deinit();
        var store = provider.artifactStore();
        for (0..19) |index| {
            var bytes: [32]u8 = undefined;
            var ref = try store.putScoped(scope, try std.fmt.bufPrint(&bytes, "payload-{d}", .{index}), .none);
            ref.deinit(a);
        }
    }
    const Visitor = struct {
        store: *stores.ArtifactStore,
        count: usize = 0,
        token: ?[]u8 = null,
        fn visit(raw: *anyopaque, _: stores.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Object files retain provider framing; discovery never bypasses it.
            const bytes = try self.store.getAlloc(id);
            defer std.testing.allocator.free(bytes);
            try std.testing.expect(std.mem.startsWith(u8, bytes, "payload-"));
            try self.store.delete(id);
            self.count += 1;
        }
        fn checkpoint(raw: *anyopaque, token: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owned = try std.testing.allocator.dupe(u8, token);
            if (self.token) |prior| std.testing.allocator.free(prior);
            self.token = owned;
        }
    };
    var token: ?[]u8 = null;
    defer if (token) |value| a.free(value);
    var total: usize = 0;
    for (0..8) |_| {
        var provider = try Provider.initFileUri(a, uri);
        defer provider.deinit();
        var store = provider.artifactStore();
        var visitor: Visitor = .{ .store = &store };
        var paused = false;
        store.visitScopedUploads(scope.domain, .{ .ptr = &visitor, .visit = Visitor.visit, .checkpoint = Visitor.checkpoint, .continuation = token, .max_entries = 3 }, .none) catch |err| {
            if (err != error.ArtifactEnumerationPaused) return err;
            paused = true;
        };
        if (visitor.token) |next| {
            if (token) |prior| a.free(prior);
            token = next;
        }
        try std.testing.expectEqual(@as(usize, 0), provider.local_inventory.?.journal_backfills.load(.monotonic));
        total += visitor.count;
        if (!paused) break;
    }
    try std.testing.expectEqual(@as(usize, 19), total);
}

test "external lake verified immutable file hints feed real Parquet reads" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("verified-file-hint-reader");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "n", .values = &.{ 7, 11 } }});
    defer a.free(bytes);
    var upload = try client.putObject("antfly", "part.parquet", bytes, .{});
    upload.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"object_mutability\":\"immutable\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var verified = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer verified.deinit();
    _ = try @import("lake_index_coverage.zig").pin(&verified, .{});
    var by_id: @import("lake_index_coverage.zig").FileMap = .empty;
    defer by_id.deinit(a);
    for (verified.inventory.files) |*file| try by_id.put(a, file.file_id, file);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    source.lazy_versions = true;
    source.verified_files = &by_id;
    var stream = try local.serverless_query_lake_stream.Stream.init(a, &source, &.{"n"}, &.{}, .{}, .{});
    defer stream.deinit();
    const batch = (try stream.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.rowCount());
    try std.testing.expect(source.pinned_files[0]);
    try std.testing.expectEqualStrings(verified.inventory.files[0].etag, source.inventory.files[0].etag);
    try std.testing.expectEqualStrings(verified.inventory.files[0].version_id, source.inventory.files[0].version_id);
    try std.testing.expectEqual(null, try stream.next());
}

test "external lake overlapping text generations share every physical segment once" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("multi-generation-text-sharing");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .converted_type = 0, .values = &.{"needle"} }});
    defer a.free(data);
    for ([_][]const u8{ "one.parquet", "two.parquet", "three.parquet" }) |name| {
        var upload = try client.putObject("antfly", name, data, .{});
        upload.deinit(a);
    }
    const schema_json = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}}}}}}}}}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(artifact_path);
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifact_fs.deinit();
    var store = artifact_fs.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const publication = @import("lake_index_publication.zig");
    var table: local.common_topology_records.TableRecord = .{ .table_id = 9, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"}}" };
    table.lake_index_catalog_json = try publication.begin(ca, std.testing.io, table, &source, @splat(7), .{}, 100, 20);
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var dummy: u8 = 0;
    const published = try publication.build(ca, &store, table, &source, @splat(7), .{ .io = std.testing.io }, .none, .{ .ptr = &dummy, .now_ms = Clock.now });
    var parsed = try local.metadata_lake_index_catalog.parse(a, published);
    defer parsed.deinit();
    try @import("lake_index_directory.zig").hydrate(parsed.arena.allocator(), store, &parsed.value.published.?, .none, null);
    const declaration = parsed.value.published.?.declarations[0];
    const full = try @import("lake_index_native_text.zig").loadRoot(ca, store, declaration.artifact, .none, null);
    try std.testing.expectEqual(@as(usize, 3), full.segments.len);
    var read_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer read_cache.deinit();
    var cache: @import("lake_index_native_text_cache.zig").Cache = .{};
    defer cache.deinit();
    var held: [3]?local.storage_db_query_search_exec.PinnedTextSource = @splat(null);
    defer for (&held) |*entry| if (entry.*) |*value| value.deinit();
    var reserved: u64 = 0;
    for (full.segments) |segment| reserved += segment.byte_len;
    for ([_][2]usize{ .{ 0, 1 }, .{ 0, 2 }, .{ 1, 2 } }, 0..) |pair, phase| {
        var root = full;
        root.manifests = &.{};
        // Keep the delete-aware directory aligned with each overlapping
        // generation; current metadata never admits unbound native ordinals.
        const groups = [_]@import("lake_index_native_text.zig").FileGroup{ full.file_groups[pair[0]], full.file_groups[pair[1]] };
        root.file_groups = &groups;
        const segments = [_]@import("lake_index_aggregate_artifact.zig").ChunkRef{ full.segments[pair[0]], full.segments[pair[1]] };
        root.segments = &segments;
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        const scope = try stores.UploadScope.forPublication(root.domain, 100 + phase, std.testing.io);
        const upload = try store.putScoped(scope, bytes, .none);
        defer a.free(upload.artifact_id);
        defer a.free(upload.checksum);
        var ref = declaration.artifact;
        ref.artifact_id = upload.artifact_id;
        ref.checksum = upload.checksum;
        ref.byte_len = upload.byte_len;
        reserved += upload.byte_len;
        held[phase] = try cache.acquire(std.testing.io, store, ref, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
        try std.testing.expectEqual(@as(u32, 2), held[phase].?.snapshot.liveDocCount());
    }
    try std.testing.expect(held[0].?.snapshot.segments[0].shared == held[1].?.snapshot.segments[0].shared);
    try std.testing.expect(held[0].?.snapshot.segments[1].shared == held[2].?.snapshot.segments[0].shared);
    try std.testing.expect(held[1].?.snapshot.segments[1].shared == held[2].?.snapshot.segments[1].shared);
    try std.testing.expectEqual(reserved, cache.bytes);
}

test "external lake inventory reclamation preserves live legacy and unfenced attempts" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("fenced-inventory-reclamation");
    defer directory.cleanup();
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{directory.path()});
    defer a.free(uri);
    var provider = try @import("../serverless/artifacts/object_store.zig").ObjectStore.initFileUri(a, uri);
    defer provider.deinit();
    var store = provider.artifactStore();
    const domain: [32]u8 = @splat(7);
    var paths: [5][]u8 = undefined;
    var owned: usize = 0;
    defer for (paths[0..owned]) |path| a.free(path);
    for ([_]u64{ 1, 2, 2, 3, 4 }, 0..) |fence, index| {
        const scope = try stores.UploadScope.forPublication(domain, fence, std.testing.io);
        var ref = try store.putScoped(scope, "payload", .none);
        defer ref.deinit(a);
        if (index != 2) try store.delete(ref.artifact_id);
        paths[index] = try std.fs.path.join(a, &.{ provider.local_inventory.?.inventory_root, &std.fmt.bytesToHex(&domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        owned += 1;
    }
    try store.reclaimRetiredScopedInventory(domain, 2, 4, .none);
    for (paths, 0..) |path, index| {
        if (index == 1 or index == 3) {
            try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
        } else try std.Io.Dir.cwd().access(std.testing.io, path, .{});
    }
    try store.reclaimRetiredScopedInventory(domain, 2, 4, .none);
    provider.read_only = true;
    try std.testing.expectError(error.ArtifactStoreReadOnly, store.reclaimRetiredScopedInventory(domain, 2, 4, .none));
}
