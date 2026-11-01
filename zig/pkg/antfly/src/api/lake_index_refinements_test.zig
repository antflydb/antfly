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
    const Lazy = struct {
        files: *const @import("lake_index_coverage.zig").FileMap,
        reads: usize = 0,
        closes: usize = 0,
        fn read(raw: *anyopaque, alloc: std.mem.Allocator, file: local.serverless_external_source_types.FileEntry, context: local.serverless_query_lake_read_context.Context) !local.serverless_query_lake_serving.ServingSource.FileVersions {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try context.ensureActive();
            self.reads += 1;
            const found = self.files.get(file.file_id) orelse return error.ExternalLakeSnapshotMismatch;
            const etag = try alloc.dupe(u8, found.etag);
            errdefer alloc.free(etag);
            return .{ .etag = etag, .version_id = try alloc.dupe(u8, found.version_id) };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
    };
    var lazy: Lazy = .{ .files = &by_id };
    {
        var deferred_source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer deferred_source.deinit();
        deferred_source.lazy_versions = true;
        deferred_source.verified_selector = .{ .ptr = &lazy, .read = Lazy.read, .close = Lazy.close };
        var deferred_stream = try local.serverless_query_lake_stream.Stream.init(a, &deferred_source, &.{"n"}, &.{}, .{}, .{});
        defer deferred_stream.deinit();
        try std.testing.expectEqual(@as(usize, 2), (try deferred_stream.next()).?.rowCount());
        try std.testing.expectEqual(null, try deferred_stream.next());
        try std.testing.expectEqual(@as(usize, 1), lazy.reads);
    }
    try std.testing.expectEqual(@as(usize, 1), lazy.closes);
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
    const declaration = for (parsed.value.published.?.declarations) |candidate| {
        if (std.mem.eql(u8, candidate.name, "body_text")) break candidate;
    } else return error.TestUnexpectedResult;
    const full = try @import("lake_index_native_text.zig").loadRoot(ca, store, declaration.artifact, .none, null);
    try std.testing.expectEqual(@as(usize, 3), full.segments.len);
    var read_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer read_cache.deinit();
    {
        const native = @import("lake_index_native_text.zig");
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &read_cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
        const wire_directory = try native.loadRootDirectory(ca, store, declaration.artifact, .none, cached);
        try std.testing.expectEqual(@as(u8, 3), wire_directory.inventory_version);
        try std.testing.expectEqual(@as(usize, 0), wire_directory.manifests.len);
        const directory_root = try native.text_directory.withManifests(ca, wire_directory, store, cached, .none);
        try std.testing.expectEqual(@as(usize, 0), directory_root.segments.len);
        try std.testing.expect(directory_root.serving_inventory == null);
        try std.testing.expect(directory_root.serving_routes == null);
        try std.testing.expect(directory_root.segment_files == null);
        try std.testing.expect(directory_root.directories.files != null);
        {
            const Cache = @import("lake_index_native_text_cache.zig").Cache;
            const Gate = struct {
                entered: std.Io.Event = .unset,
                released: std.Io.Event = .unset,
                fn check(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.entered.set(std.testing.io);
                    try self.released.wait(std.testing.io);
                }
            };
            const Acquire = struct {
                fn run(owner: *Cache, st: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, root: native.Root, schema: []const u8, read: @import("lake_index_aggregate_artifact.zig").CachedRead, started: ?*std.Io.Event) anyerror!local.storage_db_query_search_exec.PinnedTextSource {
                    if (started) |event| event.set(std.testing.io);
                    return owner.acquire(std.testing.io, st, ref, root, schema, read, .{ .io = std.testing.io }, .none);
                }
            };
            var gate: Gate = .{};
            defer gate.released.set(std.testing.io);
            var owner: Cache = .{};
            defer owner.deinit();
            var blocked = cached;
            blocked.context.checkpoint = .{ .ptr = &gate, .check = Gate.check };
            var leader = try std.testing.io.concurrent(Acquire.run, .{ &owner, store, declaration.artifact, directory_root, schema_json, blocked, @as(?*std.Io.Event, null) });
            defer {
                gate.released.set(std.testing.io);
                if (leader.any_future != null) {
                    var value = leader.cancel(std.testing.io) catch null;
                    if (value) |*source_| source_.deinit();
                }
            }
            try gate.entered.wait(std.testing.io);
            var started: std.Io.Event = .unset;
            var waiter = try std.testing.io.concurrent(Acquire.run, .{ &owner, store, declaration.artifact, directory_root, schema_json, cached, @as(?*std.Io.Event, &started) });
            defer if (waiter.any_future != null) {
                var value = waiter.cancel(std.testing.io) catch null;
                if (value) |*source_| source_.deinit();
            };
            try started.wait(std.testing.io);
            try std.testing.io.sleep(.fromMilliseconds(20), .awake);
            try std.testing.expectEqual(@as(u64, 1), owner.inventory_builds.load(.monotonic));
            try std.testing.expectError(error.Canceled, waiter.cancel(std.testing.io));
            gate.released.set(std.testing.io);
            var admitted = try leader.await(std.testing.io);
            defer admitted.deinit();
            var resident = try owner.acquire(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none);
            defer resident.deinit();
            try std.testing.expectEqual(@as(u64, 1), owner.inventory_builds.load(.monotonic));
            try std.testing.expectEqual(@as(usize, 0), owner.inventory_budget.live);
            try std.testing.expectEqual(@as(u32, 3), resident.snapshot.liveDocCount());
            // A canceled constructor relinquishes admission; a healthy waiter
            // retries with its own authority rather than inheriting the error.
            var second_gate: Gate = .{};
            defer second_gate.released.set(std.testing.io);
            var healthy_read = cached;
            healthy_read.scope = @splat(33);
            var canceled_read = healthy_read;
            canceled_read.context.checkpoint = .{ .ptr = &second_gate, .check = Gate.check };
            var constructor = try std.testing.io.concurrent(Acquire.run, .{ &owner, store, declaration.artifact, directory_root, schema_json, canceled_read, @as(?*std.Io.Event, null) });
            defer if (constructor.any_future != null) {
                var value = constructor.cancel(std.testing.io) catch null;
                if (value) |*source_| source_.deinit();
            };
            try second_gate.entered.wait(std.testing.io);
            var joining: std.Io.Event = .unset;
            var healthy = try std.testing.io.concurrent(Acquire.run, .{ &owner, store, declaration.artifact, directory_root, schema_json, healthy_read, @as(?*std.Io.Event, &joining) });
            defer if (healthy.any_future != null) {
                var value = healthy.cancel(std.testing.io) catch null;
                if (value) |*source_| source_.deinit();
            };
            try joining.wait(std.testing.io);
            try std.testing.expectError(error.Canceled, constructor.cancel(std.testing.io));
            var recovered = try healthy.await(std.testing.io);
            defer recovered.deinit();
            try std.testing.expectEqual(@as(u32, 3), recovered.snapshot.liveDocCount());
            try std.testing.expectEqual(@as(u64, 3), owner.inventory_builds.load(.monotonic));
        }
        {
            var capped: @import("lake_index_native_text_cache.zig").Cache = .{};
            defer capped.deinit();
            capped.inventory_budget.limit = 1;
            try std.testing.expectError(error.NativeLakeTextCacheBusy, capped.acquire(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none));
            try std.testing.expectEqual(@as(usize, 0), capped.admissions.count());
            try std.testing.expectEqual(@as(usize, 0), capped.inventory_budget.live);
            try std.testing.expectError(error.NativeLakeTextCacheBusy, capped.acquireSelected(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none, &.{native.term_directory.identity(full.file_groups[0].segments[0].artifact_id)}));
            try std.testing.expectEqual(@as(usize, 0), capped.admissions.count());
            try std.testing.expectEqual(@as(usize, 0), capped.inventory_budget.live);
        }
        {
            var owner: @import("lake_index_native_text_cache.zig").Cache = .{};
            defer owner.deinit();
            const chosen = [_][32]u8{native.term_directory.identity(full.file_groups[1].segments[0].artifact_id)};
            var selected = try owner.acquireSelected(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none, &chosen);
            defer selected.deinit();
            var complete = try owner.acquire(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none);
            defer complete.deinit();
            const tokens = try local.search_analysis.default_analyzer.analyze(a, "needle");
            defer local.search_analysis.Analyzer.freeTokens(a, tokens);
            try std.testing.expectEqual(@as(usize, 1), tokens.len);
            const term = tokens[0].term;
            try std.testing.expectEqual(@as(u32, 1), selected.snapshot.liveDocCount());
            try std.testing.expectEqual(@as(u32, 3), selected.snapshot.scoringDocCount());
            try std.testing.expectEqual(@as(u32, 3), try selected.snapshot.termDocFreq(a, "body", term));
            try std.testing.expectEqual(complete.snapshot.textAvgDocLen("body"), selected.snapshot.textAvgDocLen("body"));
            const request: local.search_search.SearchRequest = .{ .query = .{ .term = .{ .field = "body", .term = term } }, .k = 10 };
            // Compare actual search scores: native lake identities use their
            // physical-row sidecar rather than embedded document ordinals.
            var oracle = try local.search_search.execute(a, complete.snapshot, request);
            defer oracle.deinit();
            try std.testing.expectEqual(@as(usize, 3), oracle.hits.len);
            var result = try local.search_search.execute(a, selected.snapshot, request);
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.hits.len);
            try std.testing.expectEqual(oracle.hits[0].score, result.hits[0].score);
            // A different file subset gets its own corpus and statistics facade.
            const other = [_][32]u8{native.term_directory.identity(full.file_groups[2].segments[0].artifact_id)};
            var different = try owner.acquireSelected(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none, &other);
            defer different.deinit();
            try std.testing.expect(different.snapshot.inventory_owner != selected.snapshot.inventory_owner);
            var none = try owner.acquireSelected(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none, &.{});
            defer none.deinit();
            try std.testing.expectEqual(@as(u32, 0), none.snapshot.liveDocCount());
            try std.testing.expectEqual(@as(u32, 3), none.snapshot.scoringDocCount());
            try std.testing.expectEqual(@as(u64, 4), owner.inventory_builds.load(.monotonic));
        }
        var legacy = directory_root;
        legacy.serving_inventory = null;
        const first = try native.admitInventory(ca, legacy, store, cached, .none);
        defer first.deinit();
        const second = try native.admitInventory(ca, legacy, store, cached, .none);
        defer second.deinit();
        try std.testing.expectEqual(full.segments.len, second.root.segments.len);
        for (first.manifests, second.manifests) |left, right| try std.testing.expect(left.?.value == right.?.value);
        {
            const manifest = directory_root.manifests[0];
            const bytes = try store.getAlloc(manifest.artifact_id);
            defer a.free(bytes);
            const scope = (try stores.uploadScopeFromArtifactId(manifest.artifact_id)).?;
            try store.delete(manifest.artifact_id);
            defer {
                var restored = store.putScoped(scope, bytes, .none) catch unreachable;
                restored.deinit(a);
            }
            var fresh = cached;
            fresh.scope = @splat(19);
            try std.testing.expectError(error.FileNotFound, native.loadFileManifest(ca, store, manifest, directory_root.domain, .none, null));
            const paged = try native.admitInventory(ca, directory_root, store, fresh, .none);
            defer paged.deinit();
            try std.testing.expectEqual(full.segments.len, paged.root.segments.len);
            try std.testing.expectEqual(@as(usize, 0), paged.manifests.len);
            // Cold compact admission never touches missing detailed manifests.
            // Binding and hydration use independent per-segment and physical
            // descriptors, so missing maintenance manifests do not affect reads.
            var selective_cache: @import("lake_index_native_text_cache.zig").Cache = .{};
            defer selective_cache.deinit();
            var selective = try selective_cache.acquire(std.testing.io, store, declaration.artifact, directory_root, schema_json, fresh, fresh.context, .none);
            defer selective.deinit();
            try std.testing.expectEqual(@as(usize, 0), selective.snapshot.bound_segments.count());
            for (selective.snapshot.segments) |segment| {
                try std.testing.expect(segment.reader.deferred != null);
                try std.testing.expect(segment.reader.native == null);
            }
            try std.testing.expectEqual(@as(u32, 1), (try selective.snapshot.readSegment(0)).reader.doc_count);
            try std.testing.expectEqual(@as(usize, 1), selective.snapshot.bound_segments.count());
            for (full.segments, paged.root.segments) |expected, actual| try std.testing.expectEqualStrings(expected.artifact_id, actual.artifact_id);
        }
        {
            const refs = try ca.dupe(@import("lake_index_aggregate_artifact.zig").ChunkRef, directory_root.manifests);
            refs[0].byte_len += 1;
            var inconsistent = directory_root;
            inconsistent.manifests = refs;
            try std.testing.expectError(error.InvalidNativeLakeTextCorpus, native.admitInventory(ca, inconsistent, store, cached, .none));
        }
        var directory_cache: @import("lake_index_native_text_cache.zig").Cache = .{};
        defer directory_cache.deinit();
        var cold = try directory_cache.acquire(std.testing.io, store, declaration.artifact, directory_root, schema_json, cached, cached.context, .none);
        defer cold.deinit();
        // A resident corpus must not revisit even a missing file manifest.
        // Root authentication remains the caller's prerequisite; this sentinel
        // exercises the cache-hit admission path rather than serving a root.
        var missing = directory_root.manifests[0];
        missing.byte_len = 0;
        var sentinel = directory_root;
        sentinel.manifests = &.{missing};
        var warm = try directory_cache.acquire(std.testing.io, store, declaration.artifact, sentinel, schema_json, cached, cached.context, .none);
        defer warm.deinit();
        try std.testing.expectEqual(@as(u32, 3), warm.snapshot.liveDocCount());
        try std.testing.expect(cold.snapshot.segments[0].shared == warm.snapshot.segments[0].shared);
        {
            // Reissued manifests may have the same digest in a new upload
            // attempt. Physical reader reuse must route through the current
            // generation, whose reader lease retains that exact object.
            const scope = try stores.UploadScope.forPublication(directory_root.domain, 55, std.testing.io);
            const admitted = try native.admitServingInventory(ca, a, directory_root, store, cached, .none);
            defer admitted.deinit();
            const original = admitted.root.routing[0][0].descriptor.?;
            const manifest_bytes = try store.getAlloc(original.artifact_id);
            defer a.free(manifest_bytes);
            var reissued = try store.putScoped(scope, manifest_bytes, .none);
            defer reissued.deinit(a);
            var alias_root = admitted.root;
            const routes = try ca.dupe([]const native.SegmentRouting, admitted.root.routing);
            const first_routes = try ca.dupe(native.SegmentRouting, routes[0]);
            first_routes[0].descriptor = .{ .artifact_id = reissued.artifact_id, .checksum = reissued.checksum, .byte_len = reissued.byte_len };
            routes[0] = first_routes;
            alias_root.routing = routes;
            const root_bytes = try std.json.Stringify.valueAlloc(ca, alias_root, .{});
            var uploaded_root = try store.putScoped(scope, root_bytes, .none);
            defer uploaded_root.deinit(a);
            var alias_ref = declaration.artifact;
            alias_ref.artifact_id = uploaded_root.artifact_id;
            alias_ref.checksum = uploaded_root.checksum;
            alias_ref.byte_len = uploaded_root.byte_len;
            var alias = try directory_cache.acquire(std.testing.io, store, alias_ref, alias_root, schema_json, cached, cached.context, .none);
            defer alias.deinit();
            try std.testing.expect(alias.snapshot.segments[0].shared == cold.snapshot.segments[0].shared);
            try store.delete(reissued.artifact_id);
            var restored = false;
            defer if (!restored) {
                var replacement = store.putScoped(scope, manifest_bytes, .none) catch unreachable;
                replacement.deinit(a);
            };
            // The original descriptor is still available. Binding must fail
            // rather than silently inherit the physical owner's old manifest.
            try std.testing.expectError(error.FileNotFound, alias.snapshot.readSegment(0));
            try std.testing.expectEqual(@as(usize, 0), alias.snapshot.bound_segments.count());
            var replacement = try store.putScoped(scope, manifest_bytes, .none);
            replacement.deinit(a);
            restored = true;
            try std.testing.expectEqual(@as(u32, 1), (try alias.snapshot.readSegment(0)).reader.doc_count);
        }
        var expired = cached;
        expired.context.deadline_ns = 1;
        try std.testing.expectError(error.DeadlineExceeded, native.admitInventory(ca, directory_root, store, expired, .none));
    }
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

test "external lake global frontier compact coverage survives absent inventory and short cursor ownership" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("compact-coverage-owner");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "n", .values = &.{ 7, 11 } }});
    defer a.free(data);
    var upload = try client.putObject("antfly", "part.parquet", data, .{});
    upload.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"object_mutability\":\"immutable\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{{\"type\":\"integer\"}}}}}}}}}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var built_source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer built_source.deinit();
    // Simulate a freshly discovered immutable metadata plan; real Iceberg
    // qualification is covered by the independent-writer E2E fixtures.
    built_source.plan_identity = @splat(9);
    var store = try @import("lake_index_store.zig").Store.openNative(a, null, null, false, .standalone, directory.path());
    var store_closed = false;
    defer if (!store_closed) store.deinit();
    var artifacts = store.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const publication = @import("lake_index_publication.zig");
    var table: local.common_topology_records.TableRecord = .{ .table_id = 9, .name = "lake", .schema_json = schema_json, .indexes_json = "{}" };
    table.lake_index_catalog_json = try publication.begin(ca, std.testing.io, table, &built_source, store.identity, .{}, 100, 20);
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var dummy: u8 = 0;
    const published_json = try publication.build(ca, &artifacts, table, &built_source, store.identity, .{ .io = std.testing.io }, .none, .{ .ptr = &dummy, .now_ms = Clock.now });
    var catalog = try local.metadata_lake_index_catalog.parse(a, published_json);
    defer catalog.deinit();
    const published = catalog.value.published.?;
    const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
    const Harness = struct {
        arena: std.heap.ArenaAllocator,
        state: lifecycle.State,
        fn read(raw: *anyopaque, alloc: std.mem.Allocator, _: u64, _: local.api_operation.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return lifecycle.encode(alloc, self.state);
        }
        fn mutate(raw: *anyopaque, _: u64, revision: u64, mutation: lifecycle.Mutation, _: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (revision != self.state.revision) return error.CatalogGenerationChanged;
            self.state = try mutation.apply(self.arena.allocator(), self.state);
        }
    };
    var harness: Harness = .{ .arena = .init(a), .state = .{} };
    defer harness.arena.deinit();
    harness.state = try harness.state.synchronize(harness.arena.allocator(), catalog.value);
    const leases = @import("lake_index_reader_lease.zig");
    var pool: leases.Pool = .{};
    defer pool.deinit(std.testing.io);
    const lease = try pool.acquire(std.testing.io, .{ .ptr = &harness, .context = .{}, .read = Harness.read, .mutate = Harness.mutate }, table.table_id, published.generation, .{});
    var lease_closed = false;
    defer if (!lease_closed) lease.deinit();
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    source.plan_identity = @splat(9);
    source.lazy_versions = true;
    // Selection must use the authenticated compact root on a cold cache.
    try artifacts.delete(published.inventory.artifact_id);
    const sql_table: local.sql_catalog.Table = .{ .id = table.table_id, .physical_name = "lake", .schema_version = 1, .columns = &.{}, .external_base_source = binding, .external_indexes = .{ .catalog_json = published_json, .indexes_json = "{}", .schema_json = schema_json, .desired = published.signature.desired } };
    var selected = (try @import("lake_index_selection.zig").selectCachedWithLease(a, sql_table, &source, &store, lease.readContext(), .required, &cache, lease)).?;
    selected.deinit();
    try std.testing.expect(source.verified_selector != null);
    try std.testing.expect(source.verified_files == null);
    lease.deinit();
    lease_closed = true;
    store.deinit();
    store_closed = true;
    var stream = try local.serverless_query_lake_stream.Stream.init(a, &source, &.{"n"}, &.{}, .{}, .{});
    defer stream.deinit();
    try std.testing.expectEqual(@as(usize, 2), (try stream.next()).?.rowCount());
    try std.testing.expectEqual(null, try stream.next());
}

test "external lake global frontier retained compacted roots detect v10 and v11 before serving and GC" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("legacy-compact-version");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@import("lake_index_publication.zig").uploadDomainWithNamespace(1, @splat(1), null), 1, std.testing.io);
    const text = @import("lake_index_native_text.zig");
    for ([_]u16{ 10, 11 }) |version| {
        var child: text.Root = .{ .version = version, .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .collection = .{ .documents = 0, .fields = &.{} } };
        var upload = try store.putWithCancellation(try std.json.Stringify.valueAlloc(ca, child, .{}), .none);
        defer upload.deinit(a);
        const chunk: @import("lake_index_aggregate_artifact.zig").ChunkRef = .{ .artifact_id = upload.artifact_id, .checksum = upload.checksum, .byte_len = upload.byte_len };
        const decoded = try text.loadRootDirectoryAuto(ca, store, chunk, .none, null);
        try std.testing.expectEqual(version, decoded.metadata_version);
        child.version = text.metadata_version;
        child.compacted = chunk;
        _ = try text.servingRoot(ca, store, child, .none, null);
        var parent = try store.putWithCancellation(try std.json.Stringify.valueAlloc(ca, child, .{}), .none);
        defer parent.deinit(a);
        var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 1, .authority = undefined, .store = store, .identity = @splat(1), .context = .{}, .audit_only = true, .token = @splat(1), .remaining_reads = 16 * 1024 * 1024 };
        defer {
            var keys = collector.marked.keyIterator();
            while (keys.next()) |key| a.free(key.*);
            collector.marked.deinit(a);
        }
        try collector.markArtifact(ca, .{ .kind = .text_segment, .metadata_version = text.metadata_version, .artifact_id = parent.artifact_id, .checksum = parent.checksum, .byte_len = parent.byte_len });
        try std.testing.expect(collector.marked.contains(upload.artifact_id));
    }
}

test "external lake global frontier GC upgrades maintenance and already marked manifests" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-gc-mode-upgrade");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const identity: [32]u8 = @splat(4);
    const namespace: [32]u8 = @splat(8);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(4, identity, namespace);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    var child = try store.put("reader-held posting segment");
    defer child.deinit(a);
    const text = @import("lake_index_native_text.zig");
    const group: text.FileGroup = .{ .file = .{ .id = "part", .digest = @splat(7) }, .segments = &.{.{ .artifact_id = child.artifact_id, .checksum = child.checksum, .byte_len = child.byte_len }} };
    var manifest = try store.put(try std.json.Stringify.valueAlloc(ca, group, .{}));
    defer manifest.deinit(a);
    const ref: @import("lake_index_aggregate_artifact.zig").ChunkRef = .{ .artifact_id = manifest.artifact_id, .checksum = manifest.checksum, .byte_len = manifest.byte_len };
    // A compacted publication needs only canonical maintenance edges. An older
    // reader-held publication needs the complete serving graph of the same ID.
    for ([_]bool{ false, true }) |maintenance_first| {
        var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 4, .authority = undefined, .store = store, .identity = identity, .context = .{}, .token = @splat(1), .namespace = namespace, .audit_only = true, .remaining_reads = 1024 * 1024 };
        defer {
            var keys = collector.marked.keyIterator();
            while (keys.next()) |key| a.free(key.*);
            collector.marked.deinit(a);
        }
        _ = try collector.mark(ref);
        if (maintenance_first) {
            try collector.markTextManifestMode(ca, ref, false, true);
            try std.testing.expect(!collector.marked.contains(child.artifact_id));
        }
        try collector.markTextManifestMode(ca, ref, false, false);
        try collector.markTextManifestMode(ca, ref, false, true);
        try std.testing.expect(collector.marked.contains(child.artifact_id));
        const remaining = collector.remaining_reads;
        try collector.markTextManifestMode(ca, ref, false, false);
        try std.testing.expectEqual(remaining, collector.remaining_reads);
        try std.testing.expectEqual(@as(u32, 2), collector.marked.count());
    }
}

test "external lake bounded window profiles stored highlight consumer owns output across cursor teardown and allocation faults" {
    const a = std.testing.allocator;
    const Column = local.section_source_column;
    const Search = local.storage_db_query_search_exec;
    const body = try a.alloc(u8, 8192);
    defer a.free(body);
    @memset(body, '.');
    @memcpy(body[4092..][0.."é needle running".len], "é needle running");
    var memory = local.segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try Column.Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = body }, &.{&local.search_analysis.default_analyzer});
    try std.testing.expect(try writer.finish());
    const view = try local.segment_source.View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len);
    const Run = struct {
        fn run(alloc: std.mem.Allocator, source: local.segment_source.View, text: []const u8) !void {
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const sa = scratch.allocator();
            const queries = [_]Search.HighlightQuery{
                .{ .query = .{ .match = .{ .field = "body", .text = "needle" } }, .text_analysis = .{}, .runtime_schema = .{ .full_text_documents = &.{.{ .name = "_default", .fields = &.{.{ .path = "body", .emitted_name = "body", .analyzer = "standard" }} }} } },
                .{ .query = .{ .prefix = .{ .field = "body", .prefix = "run" } }, .text_analysis = .{}, .runtime_schema = .{ .full_text_documents = &.{.{ .name = "_default", .fields = &.{.{ .path = "body", .emitted_name = "body", .analyzer = "standard" }} }} } },
            };
            var prepared = try Search.PreparedHighlights.init(alloc, .{ .fields = &.{"body"}, .max_fragments = 1, .fragment_size = 64 }, &queries);
            defer prepared.deinit();
            var output: Search.PreparedHighlights.Accumulator = .{};
            defer output.deinit(alloc);
            {
                var cursor = Column.Cursor.init(try Column.Reader.init(alloc, source));
                defer cursor.deinit();
                const value = (try cursor.text(0)).?;
                try std.testing.expect(try prepared.appendStoredSourceField(alloc, sa, &output, null, "body", value));
            }
            var reference: local.storage_db_types.SearchHit = .{ .id = try alloc.dupe(u8, "row") };
            defer reference.deinit(alloc);
            var parsed: std.json.Value = .{ .object = .empty };
            try parsed.object.put(sa, "body", .{ .string = text });
            try prepared.attachValue(alloc, sa, &reference, parsed);
            const expected = try std.json.Stringify.valueAlloc(alloc, reference.highlights, .{});
            defer alloc.free(expected);
            const found = try std.json.Stringify.valueAlloc(alloc, output.fields.items, .{});
            defer alloc.free(found);
            try std.testing.expectEqualStrings(expected, found);
            try std.testing.expectEqual(@as(usize, 1), output.fields.items.len);
        }
    };
    try Run.run(a, view, body);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ view, body });
}
