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

//! Catalog control and source-fixture facade for one fenced lake build.
//! The coordinator commits the returned state with a full definition CAS;
//! successful uploads alone never make an index ready.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const records = local.common_topology_records;
const serving = local.serverless_query_lake_serving;
const coverage = @import("lake_index_coverage.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const SourceImplementation = @import("lake_execution_backend").implementation;
pub const Clock = @import("lake_execution_values.zig").Clock;
/// Stable upload/collection namespace, isolated from other native tables and
/// every serverless collector even when they share an underlying object store.
pub fn uploadDomain(table_id: u64, store_identity: catalog.Digest) catalog.Digest {
    return uploadDomainWithNamespace(table_id, store_identity, null);
}
pub fn uploadDomainWithNamespace(table_id: u64, store_identity: catalog.Digest, namespace: ?catalog.Digest) catalog.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-lake-index-upload-domain-v1");
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, table_id, .little);
    hash.update(&id);
    hash.update(&store_identity);
    if (namespace) |owner| {
        hash.update("native-table-incarnation-v1");
        hash.update(&owner);
    }
    return hash.finalResult();
}
pub fn signatureFor(a: A, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context) !catalog.Signature {
    try context.ensureActive();
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return error.InvalidExternalTableBinding;
    defer binding.deinit(a);
    const resolved = try coverage.pin(source, context);
    return .{ .desired = catalog.desiredFingerprint(table), .source = resolved.source, .credentials = try source.credentialIdentity(binding.binding), .store = store_identity };
}
pub fn begin(a: A, io: std.Io, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, now_ms: u64, lease_ms: u64) ![]u8 {
    return beginWithLocator(a, io, table, source, store_identity, context, now_ms, lease_ms, null);
}
pub fn beginWithLocator(a: A, io: std.Io, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, now_ms: u64, lease_ms: u64, locator: ?catalog.StoreLocator) ![]u8 {
    const signature = try signatureFor(a, table, source, store_identity, context);
    var current = try catalog.parse(a, table.lake_index_catalog_json);
    defer current.deinit();
    if (current.value.namespace == null) {
        var namespace: catalog.Digest = undefined;
        io.random(&namespace);
        if (std.mem.allEqual(u8, &namespace, 0)) return error.InvalidArtifactUploadScope;
        current.value.namespace = namespace;
    }
    const generation = std.math.add(u64, current.value.generation, 1) catch return error.LakeIndexGenerationExhausted;
    const scope = try stores.UploadScope.forPublication(uploadDomainWithNamespace(table.table_id, store_identity, current.value.namespace), generation, io);
    var next = try current.value.begin(signature, scope.attempt, now_ms, lease_ms);
    next.pending.?.store_locator = locator;
    return catalog.encode(a, next);
}
/// The table is the already committed pending record. Native callers must own
/// its lease through the end of upload and use that exact record for the final
/// CAS. All source reads, including deletion preparation, share its coverage.
pub fn build(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock) ![]u8 {
    return buildWithLease(a, artifact_store, table, source, store_identity, context, cancellation, clock, null);
}
pub const Lease = @import("lake_execution_values.zig").Lease;
pub fn buildWithLease(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease) ![]u8 {
    return buildWithLeaseAndEmbedding(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, null, null);
}
pub fn buildWithLeaseAndEmbedding(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, memo: ?@import("lake_vector_enrichment.zig").Memo) ![]u8 {
    return buildWithReuse(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, .{});
}
pub const BuildPolicy = @import("lake_execution_values.zig").BuildPolicy;
/// Explicit generation repair must bypass every same-signature reuse shortcut.
pub fn buildWithReuse(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, memo: ?@import("lake_vector_enrichment.zig").Memo, policy: BuildPolicy) ![]u8 {
    return SourceImplementation.buildWithReuse(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, policy);
}
// Source fixtures exercise recovery directly without rooting builders in the
// compiled API owner. Production construction uses the execution operation.
fn priorInventory(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, token: Cancellation) !?local.serverless_external_source_types.Inventory {
    return SourceImplementation.priorInventory(a, store, ref, token);
}

test "external lake native publication builds scoped text artifacts and fences expired completion" {
    const BuildProgress = SourceImplementation.BuildProgress;
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-native-publication");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "first value", "second value" } }});
    defer a.free(data);
    var put = try client.putObject("antfly", "part.parquet", data, .{});
    put.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\",\"row_identity\":{{\"key_columns\":[\"body\"]}}}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const artifact_root = try std.fs.path.join(a, &.{ directory.path(), "native-artifacts" });
    defer a.free(artifact_root);
    var fs_artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_root);
    defer fs_artifacts.deinit();
    var artifact_store = fs_artifacts.artifactStore();
    const store_identity: catalog.Digest = @splat(4);
    const table: records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]}}" };
    const pending_bytes = try begin(a, std.testing.io, table, &source, store_identity, .{}, 100, 20);
    defer a.free(pending_bytes);
    var pending = table;
    pending.lake_index_catalog_json = pending_bytes;
    try std.testing.expect(try catalog.transitionAllowed(a, table, pending));
    const TestClock = struct {
        now: u64 = 101,
        fn read(raw: *const anyopaque) !u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.now;
        }
    };
    var time: TestClock = .{};
    const clock: Clock = .{ .ptr = &time, .now_ms = TestClock.read };
    const published_bytes = try build(a, &artifact_store, pending, &source, store_identity, .{ .io = std.testing.io }, .none, clock);
    defer a.free(published_bytes);
    const declined_bytes = try buildWithReuse(a, &artifact_store, pending, &source, store_identity, .{ .io = std.testing.io }, .none, clock, null, null, null, .{ .identity_spill_bytes = 0 });
    defer a.free(declined_bytes);
    var declined = try catalog.parse(a, declined_bytes);
    defer declined.deinit();
    try std.testing.expect(declined.value.published.?.row_identity == null);
    try std.testing.expect(declined.value.published.?.directory.?.count > 0);
    var published = pending;
    published.lake_index_catalog_json = published_bytes;
    try std.testing.expect(try catalog.transitionAllowed(a, pending, published));
    var parsed = try catalog.parse(a, published_bytes);
    defer parsed.deinit();
    try @import("lake_index_directory.zig").hydrate(parsed.arena.allocator(), artifact_store, &parsed.value.published.?, .none, null);
    // A generation repair must bypass same-cut identity and native directory
    // reuse, even when the old publication's signature remains current.
    var repairing = published;
    repairing.lake_index_catalog_json = try begin(a, std.testing.io, published, &source, store_identity, .{}, 100, 20);
    defer a.free(repairing.lake_index_catalog_json);
    const repaired_bytes = try buildWithReuse(a, &artifact_store, repairing, &source, store_identity, .{ .io = std.testing.io }, .none, clock, null, null, null, .{ .allow_reuse = false });
    defer a.free(repaired_bytes);
    var repaired = try catalog.parse(a, repaired_bytes);
    defer repaired.deinit();
    try @import("lake_index_directory.zig").hydrate(repaired.arena.allocator(), artifact_store, &repaired.value.published.?, .none, null);
    try std.testing.expect(repaired.value.published.?.generation > parsed.value.published.?.generation);
    try std.testing.expect(!std.mem.eql(u8, repaired.value.published.?.declarations[0].artifact.artifact_id, parsed.value.published.?.declarations[0].artifact.artifact_id));

    // Replacing the same current declaration must not retain save history.
    // Before bounded ownership this exhausted 1 MiB after 116 saves.
    const Checkpoints = struct {
        count: usize = 0,
        last_declarations: usize = 0,
        fn save(raw: *anyopaque, saved: catalog.Publication) !void {
            try saved.validate();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            self.last_declarations = @intCast(saved.directory.?.count);
        }
    };
    var checkpoint_budget: local.sql_memory_budget = .{ .backing = a, .limit = 1024 * 1024 };
    {
        var checkpoint_store = artifact_store;
        checkpoint_store.upload_scope = try stores.uploadScopeFromArtifactId(parsed.value.published.?.directory.?.artifact_id);
        var checkpoint_saved = parsed.value.published.?;
        checkpoint_saved.declarations = &.{};
        checkpoint_saved.directory = null;
        var checkpoints: Checkpoints = .{};
        var progress: BuildProgress = .{ .a = checkpoint_budget.allocator(), .store = &checkpoint_store, .cancellation = .none, .clock = clock, .lease = .{ .ptr = &checkpoints, .snapshot = undefined, .save_checkpoint = Checkpoints.save }, .saved = checkpoint_saved };
        defer progress.deinit();
        for (0..1000) |_| try BuildProgress.save(&progress, parsed.value.published.?.declarations[0]);
        try std.testing.expectEqual(@as(usize, 1000), checkpoints.count);
        try std.testing.expectEqual(@as(usize, 1), progress.declarations.items.len);
        var batch: [16]local.serverless_segment_sidecar_manifest.DeclaredArtifact = undefined;
        var names: [16][32]u8 = undefined;
        var batch_objects: [16]?stores.ArtifactMetadata = @splat(null);
        defer for (&batch_objects) |*object| if (object.*) |*value| value.deinit(a);
        const original = parsed.value.published.?.declarations[0].artifact;
        const payload = try artifact_store.getVerifiedAllocWithCancellation(original.artifact_id, original.byte_len, original.checksum, .none);
        defer a.free(payload);
        for (&batch, &names, 0..) |*declaration, *name, index| {
            declaration.* = parsed.value.published.?.declarations[0];
            declaration.name = try std.fmt.bufPrint(name, "batch-{d}", .{index});
            declaration.artifact.name = declaration.name;
            // Distinct completed indexes require distinct scoped identities;
            // reusing one artifact ID under different names is forbidden.
            const scope = checkpoint_store.upload_scope.?;
            batch_objects[index] = try checkpoint_store.putScoped(try stores.UploadScope.forPublication(scope.domain, scope.fencingToken(), std.testing.io), payload, .none);
            declaration.artifact.artifact_id = batch_objects[index].?.artifact_id;
            declaration.artifact.checksum = batch_objects[index].?.checksum;
            declaration.artifact.byte_len = batch_objects[index].?.byte_len;
        }
        // One completed set has one durable checkpoint containing every
        // declaration, alongside the earlier resumable declaration.
        try BuildProgress.saveBatch(&progress, &batch);
        try std.testing.expectEqual(@as(usize, 1001), checkpoints.count);
        try std.testing.expectEqual(@as(usize, 17), checkpoints.last_declarations);
        try std.testing.expectEqual(@as(usize, 17), progress.declarations.items.len);
        progress.io = std.testing.io;
        const Save = struct {
            fn run(owner: *BuildProgress, completed: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) anyerror!void {
                try BuildProgress.saveBatch(owner, completed);
            }
        };
        var first = try std.testing.io.concurrent(Save.run, .{ &progress, batch[0..8] });
        defer first.cancel(std.testing.io) catch {};
        var second = try std.testing.io.concurrent(Save.run, .{ &progress, batch[8..] });
        defer second.cancel(std.testing.io) catch {};
        try first.await(std.testing.io);
        try second.await(std.testing.io);
        try std.testing.expectEqual(@as(usize, 1003), checkpoints.count);
        try std.testing.expectEqual(@as(usize, 17), checkpoints.last_declarations);
    }
    try std.testing.expectEqual(@as(usize, 0), checkpoint_budget.live);

    const publication = parsed.value.published.?;
    {
        var statistics_arena = std.heap.ArenaAllocator.init(a);
        defer statistics_arena.deinit();
        const navigation_root = try @import("lake_source_navigation.zig").load(statistics_arena.allocator(), artifact_store, publication.source_navigation.?, .none, null);
        // A pre-epoch binding remains legacy through publication. Version 3
        // carries costing without switching its public row identity to v2.
        try std.testing.expectEqual(@as(u8, 3), navigation_root.version);
        try std.testing.expectEqual(@as(u8, 1), binding.binding.row_identity_version);
        try std.testing.expectEqual(binding.binding.row_identity_version, source.inventory.row_identity_version);
        try std.testing.expectEqual(binding.binding.row_identity_version, navigation_root.public_identity_version);
        try std.testing.expectEqual(@as(usize, 1), navigation_root.costing.len);
        try std.testing.expectEqualStrings("body", navigation_root.costing[0].name);
        try std.testing.expectEqual(@as(?u64, 2), navigation_root.costing[0].statistics.population_rows);
        try std.testing.expectEqual(@as(?u64, 2), navigation_root.costing[0].statistics.non_null_rows);
    }
    try std.testing.expect(parsed.value.pending == null);
    try std.testing.expect(publication.declarations.len > 0);
    for (publication.declarations) |declaration| {
        if (declaration.artifact.kind != .text_segment) continue;
        try std.testing.expectEqual(local.serverless_manifest_artifact_ref.ArtifactKind.text_segment, declaration.artifact.kind);
        const upload = (try stores.uploadScopeFromArtifactId(declaration.artifact.artifact_id)).?;
        try std.testing.expectEqual(@as(u64, 1), upload.fencingToken());
        try std.testing.expectEqual(uploadDomainWithNamespace(table.table_id, store_identity, publication.namespace), upload.domain);
        const loaded = try artifact_store.getVerifiedAllocWithCancellation(declaration.artifact.artifact_id, declaration.artifact.byte_len, declaration.artifact.checksum, .none);
        defer a.free(loaded);
        try std.testing.expect(loaded.len > 0);
        const native_text = @import("lake_index_native_text.zig");
        var text_arena = std.heap.ArenaAllocator.init(a);
        defer text_arena.deinit();
        const root = try native_text.loadRoot(text_arena.allocator(), artifact_store, declaration.artifact, .none, null);
        try std.testing.expect(root.stored_projection);
        try std.testing.expectEqualStrings("body", root.binding.column_bindings[0]);
        var writer = try native_text.loadWriter(a, artifact_store, root, .none, null, null);
        defer writer.deinit();
        const snapshot = writer.acquireSnapshot();
        defer snapshot.release();
        const results = try snapshot.search(a, "body", &.{"first"}, 10);
        defer a.free(results.hits);
        try std.testing.expectEqual(@as(u32, 1), results.total_count);
        const stored = (try snapshot.storedDocDecompressed(a, results.hits[0].doc_id)).?;
        defer a.free(stored.data);
        var parsed_source = try std.json.parseFromSlice(std.json.Value, a, stored.data, .{});
        defer parsed_source.deinit();
        try std.testing.expect(std.mem.indexOf(u8, parsed_source.value.object.get("body").?.string, "first") != null);
        var cache_io = std.Io.Threaded.init(a, .{});
        defer cache_io.deinit();
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        const cache_root = try std.fs.path.join(a, &.{ directory.path(), "text-read-cache" });
        defer a.free(cache_root);
        try cache.ensurePersistent(cache_io.io(), cache_root, .{}, .{});
        var cached_segments: native_text.CachedSegments = .{ .store = artifact_store, .seekable = root.seekable, .cache = .{ .cache = &cache, .scope = @splat(4), .context = .{ .io = cache_io.io() } } };
        {
            var cold = try native_text.loadWriter(a, artifact_store, root, .none, null, cached_segments.loader());
            defer cold.deinit();
            const cold_snapshot = cold.acquireSnapshot();
            defer cold_snapshot.release();
            try std.testing.expectEqual(snapshot.liveDocCount(), cold_snapshot.liveDocCount());
        }
        cache.persistent.?.flush();
        var warm = try native_text.loadWriter(a, artifact_store, root, .none, null, cached_segments.loader());
        defer warm.deinit();
        const warm_snapshot = warm.acquireSnapshot();
        defer warm_snapshot.release();
        for (warm_snapshot.segments) |segment| try std.testing.expect(segment.reader.native != null);
        const warm_results = try warm_snapshot.search(a, "body", &.{"first"}, 10);
        defer a.free(warm_results.hits);
        try std.testing.expectEqual(results.total_count, warm_results.total_count);
        try std.testing.expectEqual(results.hits[0].score, warm_results.hits[0].score);
        var corpora: @import("lake_index_native_text_cache.zig").Cache = .{ .max_entries = 1 };
        defer corpora.deinit();
        const other_schema = try std.fmt.allocPrint(a, "{s} ", .{schema_json});
        defer a.free(other_schema);
        {
            var first = try corpora.acquire(cache_io.io(), artifact_store, declaration.artifact, root, schema_json, cached_segments.cache, .{ .io = cache_io.io() }, .none);
            defer first.deinit();
            var second = try corpora.acquire(cache_io.io(), artifact_store, declaration.artifact, root, schema_json, cached_segments.cache, .{ .io = cache_io.io() }, .none);
            defer second.deinit();
            try std.testing.expect(first.snapshot != second.snapshot);
            for (first.snapshot.segments, second.snapshot.segments) |left, right| try std.testing.expect(left.shared == right.shared);
            try std.testing.expectError(error.NativeLakeTextCacheBusy, corpora.acquire(cache_io.io(), artifact_store, declaration.artifact, root, other_schema, cached_segments.cache, .{ .io = cache_io.io() }, .none));
        }
        // Once both readers drain the idle corpus can be replaced without
        // retaining two native metadata/statistics allocations.
        var replaced = try corpora.acquire(cache_io.io(), artifact_store, declaration.artifact, root, other_schema, cached_segments.cache, .{ .io = cache_io.io() }, .none);
        defer replaced.deinit();
        try std.testing.expectEqual(snapshot.liveDocCount(), replaced.snapshot.liveDocCount());
        try std.testing.expectEqual(@as(usize, 1), corpora.entries.count());
        try std.testing.expectError(error.DeadlineExceeded, corpora.acquire(cache_io.io(), artifact_store, declaration.artifact, root, schema_json, cached_segments.cache, .{ .deadline_ns = 0 }, .none));
        var tiny: @import("lake_index_native_text_cache.zig").Cache = .{ .max_bytes = 1 };
        defer tiny.deinit();
        try std.testing.expectError(error.NativeLakeTextCorpusTooLarge, tiny.acquire(cache_io.io(), artifact_store, declaration.artifact, root, schema_json, cached_segments.cache, .{ .io = cache_io.io() }, .none));
        try std.testing.expectEqual(@as(usize, 0), tiny.entries.count());
        var heap_limited: @import("lake_index_native_text_cache.zig").Cache = .{ .heap_budget = .{ .backing = a, .limit = 1 } };
        defer heap_limited.deinit();
        try std.testing.expectError(error.NativeLakeTextCacheBusy, heap_limited.acquire(cache_io.io(), artifact_store, declaration.artifact, root, schema_json, cached_segments.cache, .{ .io = cache_io.io() }, .none));
        // Failed single-flight state can be retried without leaking the first
        // schema arena or retaining a partially installed native snapshot.
        try std.testing.expectError(error.NativeLakeTextCacheBusy, heap_limited.acquire(cache_io.io(), artifact_store, declaration.artifact, root, schema_json, cached_segments.cache, .{ .io = cache_io.io() }, .none));
        try std.testing.expectEqual(@as(usize, 0), heap_limited.heap_budget.live);
    }
    // A mixed algebraic/text append must replay only the new Parquet file.
    var appended = try client.putObject("antfly", "part-2.parquet", data, .{});
    appended.deinit(a);
    var next_source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer next_source.deinit();
    // This regression exercises physical append reuse. The copied rows have
    // duplicate logical keys, so use the explicit physical baseline above;
    // losing key evidence after logical construction correctly requires rebase.
    var physical_published = published;
    physical_published.lake_index_catalog_json = declined_bytes;
    const next_pending_bytes = try begin(a, std.testing.io, physical_published, &next_source, store_identity, .{}, 102, 20);
    defer a.free(next_pending_bytes);
    var next_pending = physical_published;
    next_pending.lake_index_catalog_json = next_pending_bytes;
    const Denied = struct {
        var base: local.storage_object_storage.ObjectStorage = undefined;
        fn get(_: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: local.storage_object_storage.GetOptions) !local.storage_object_storage.GetResult {
            if (std.mem.eql(u8, key, "part.parquet")) return error.UnexpectedUnchangedParquetRead;
            var copy = base;
            copy.allocator = alloc;
            return copy.getObject(bucket, key, options);
        }
    };
    Denied.base = next_source.scanner.object_reader.client;
    var denied = Denied.base.vtable.*;
    denied.get_object = Denied.get;
    next_source.scanner.object_reader.client.vtable = &denied;
    time.now = 103;
    const next_bytes = try buildWithReuse(a, &artifact_store, next_pending, &next_source, store_identity, .{ .io = std.testing.io }, .none, clock, null, null, null, .{ .identity_spill_bytes = 0 });
    defer a.free(next_bytes);
    var next = try catalog.parse(a, next_bytes);
    defer next.deinit();
    try @import("lake_index_directory.zig").hydrate(next.arena.allocator(), artifact_store, &next.value.published.?, .none, null);
    try std.testing.expectEqual(@as(usize, 2), next.value.published.?.declarations.len);
    try std.testing.expectEqual(@as(usize, 3), next.value.published.?.file_contributions.len);
    const aggregate = @import("lake_index_aggregate_artifact.zig");
    for (next.value.published.?.declarations) |declaration| if (declaration.artifact.kind == .algebraic_segment) {
        const recipe = try aggregate.loadRecipe(next.arena.allocator(), artifact_store, declaration.artifact, .none);
        const reader = try aggregate.Reader.open(a, artifact_store, declaration.artifact, recipe, .none);
        defer reader.cursor().close(reader);
        const values = (try reader.cursor().next(reader, next.arena.allocator(), 8)).?;
        var count = try local.sql_aggregate_partial.read(a, values[0], 0, .{ .kind = .count });
        defer count.deinit();
        try std.testing.expectEqual(@as(u64, 4), count.count);
    };
    time.now = 120;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, build(a, &artifact_store, pending, &source, store_identity, .{}, .none, clock));
    var changed = pending;
    changed.indexes_json = "{}";
    time.now = 101;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, build(a, &artifact_store, changed, &source, store_identity, .{}, .none, clock));
}

test "external lake bounded WAL prior inventory proofs recover missing corrupt and canceled reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("prior-inventory-recovery");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = &.{} };
    const bytes = try local.serverless_external_source_mod.encodeInventoryAlloc(ca, inventory);
    var good = try store.put(bytes);
    defer good.deinit(a);
    const ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .external_base_source, .artifact_id = good.artifact_id, .checksum = good.checksum, .byte_len = good.byte_len };
    try std.testing.expect((try priorInventory(ca, store, ref, .none)) != null);
    var bad = try store.put("invalid inventory bytes");
    defer bad.deinit(a);
    try std.testing.expect((try priorInventory(ca, store, .{ .kind = .external_base_source, .artifact_id = bad.artifact_id, .checksum = bad.checksum, .byte_len = bad.byte_len }, .none)) == null);
    try store.delete(good.artifact_id);
    try std.testing.expect((try priorInventory(ca, store, ref, .none)) == null);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, priorInventory(ca, store, ref, Cancellation.fromAtomic(&canceled)));
}

test "external lake logical identity publishes native indexes once across reordered Parquet rewrites and rebuilds updates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-logical-publication");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"id":{{"type":"integer"}},"body":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    const artifact_root = try std.fs.path.join(a, &.{ directory.path(), "native-artifacts" });
    defer a.free(artifact_root);
    var fs_artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_root);
    defer fs_artifacts.deinit();
    var artifact_store = fs_artifacts.artifactStore();
    const store_identity: catalog.Digest = @splat(4);
    var table: records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]}}" };
    const Fixed = struct {
        now: u64 = 101,
        fn read(raw: *const anyopaque) !u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.now;
        }
    };
    var marker: Fixed = .{};
    var previous: ?std.json.Parsed(catalog.State) = null;
    defer if (previous) |*state| state.deinit();
    var catalog_bytes: []u8 = &.{};
    defer a.free(catalog_bytes);
    var original_directory: ?[]u8 = null;
    defer if (original_directory) |bytes_| a.free(bytes_);
    var namespace_schema: ?[]u8 = null;
    defer if (namespace_schema) |bytes_| a.free(bytes_);
    var original_namespace: ?catalog.Digest = null;
    var original_contribution: ?catalog.Digest = null;
    var original_snapshot: ?[]u8 = null;
    defer if (original_snapshot) |bytes_| a.free(bytes_);
    var original_location: ?local.storage_rowsource_types.RowRef = null;
    var saved = std.heap.ArenaAllocator.init(a);
    defer saved.deinit();
    const names: []const []const u8 = &.{ "old.parquet", "new.parquet", "third.parquet", "updated.parquet", "append.parquet", "removed.parquet", "namespace.parquet", "duplicate.parquet" };
    for (0..8) |step| {
        marker.now = 101 + step * 100;
        if (step == 3) {
            // Changing one definition must keep the unchanged aggregate's
            // original counted contribution graph, as well as its artifact.
            table.indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":false},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]}}";
        }
        if (step == 6) {
            namespace_schema = try std.mem.replaceOwned(u8, a, schema_json, "\"key_columns\":[\"id\"]", "\"key_columns\":[\"body\"]");
            table.schema_json = namespace_schema.?;
            binding.deinit(a);
            binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)).?;
        }
        if (step != 0) try client.deleteObject("antfly", names[step - 1], .{});
        const ids: []const i64 = if (step == 7) &.{ 3, 1, 1 } else if (step == 5 or step == 6) &.{ 3, 2 } else if (step == 4) &.{ 3, 1, 2, 4 } else if (step == 0 or step == 2) &.{ 1, 2, 3 } else &.{ 3, 1, 2 };
        const bodies: []const []const u8 = if (step == 7) &.{ "last", "updated text", "updated text" } else if (step == 5 or step == 6) &.{ "last", "same text" } else if (step == 4) &.{ "last", "updated text", "same text", "appended" } else if (step == 0 or step == 2) &.{ "same text", "same text", "last" } else if (step == 1) &.{ "last", "same text", "same text" } else &.{ "last", "updated text", "same text" };
        const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "id", .field_id = 1, .values = ids }}, &.{.{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = bodies }});
        defer a.free(data);
        var put = try client.putObject("antfly", names[step], data, .{});
        put.deinit(a);
        var source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        const pending = try begin(a, std.testing.io, table, &source, store_identity, .{}, marker.now - 1, 100000);
        defer a.free(pending);
        var attempt = table;
        attempt.lake_index_catalog_json = pending;
        const completed = try build(a, &artifact_store, attempt, &source, store_identity, .{ .io = std.testing.io }, .none, .{ .ptr = &marker, .now_ms = Fixed.read });
        a.free(catalog_bytes);
        catalog_bytes = completed;
        table.lake_index_catalog_json = completed;
        if (previous) |*state| state.deinit();
        previous = try catalog.parse(a, completed);
        try @import("lake_index_directory.zig").hydrate(previous.?.arena.allocator(), artifact_store, &previous.?.value.published.?, .none, null);
        const publication = previous.?.value.published.?;
        if (step == 7) {
            // Optional identity evidence never turns a duplicate business key
            // into a table outage or authorizes stale logical reuse.
            try std.testing.expect(publication.row_identity == null);
            try std.testing.expect(!std.mem.eql(u8, original_directory.?, publication.directory.?.artifact_id));
            continue;
        }
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const root = try @import("lake_row_identity.zig").load(arena.allocator(), artifact_store, publication.row_identity.?, .none, null);
        try @import("lake_row_identity_reader.zig").validate(arena.allocator(), root, publication, root.domain, &source);
        try std.testing.expectEqual(@as(u64, if (step == 4) 4 else if (step == 5 or step == 6) 2 else 3), root.current.rows);
        try std.testing.expectEqual(@as(usize, 1), publication.contribution_roots.len);
        if (step == 6) try std.testing.expect(!std.mem.eql(u8, &original_namespace.?, &root.namespace));
        if (step == 3) try std.testing.expectEqualSlices(u8, &original_contribution.?, &publication.contribution_roots[0]);
        if (step == 0) {
            original_contribution = publication.contribution_roots[0];
            original_namespace = root.namespace;
            original_directory = try a.dupe(u8, publication.directory.?.artifact_id);
            original_snapshot = try a.dupe(u8, source.inventory.snapshot_id);
            var reads: u64 = 4 * 1024 * 1024;
            var writes: u64 = 0;
            var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = root.domain, .artifacts = &artifact_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
            var resolver: @import("lake_row_identity.zig").Reader = .{ .root = root, .pages = &pages };
            const logical = (try resolver.toIndex(arena.allocator(), source.inventory.files[0].file_id, 0, 0)).?;
            original_location = try resolver.reference(saved.allocator(), root.spaceForIndex("body_text"), logical);
            try std.testing.expect(root.native_space != null);
            try std.testing.expect(root.indexes == null);
        } else if (step < 3) {
            try std.testing.expectEqualStrings(original_directory.?, publication.directory.?.artifact_id);
            try std.testing.expectEqualStrings(original_snapshot.?, root.spaceForIndex("body_text").snapshot);
            var reads: u64 = 4 * 1024 * 1024;
            var writes: u64 = 0;
            var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = root.domain, .artifacts = &artifact_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
            var resolver: @import("lake_row_identity.zig").Reader = .{ .root = root, .pages = &pages };
            const old = original_location.?.external;
            const row = (try resolver.fromIndex(arena.allocator(), old.file_id, old.row_group_ordinal, old.row_ordinal)).?;
            try std.testing.expectEqual(@as(u64, if (step == 1) 1 else 0), row.coordinate.row);
        } else {
            try std.testing.expect(!std.mem.eql(u8, original_directory.?, publication.directory.?.artifact_id));
            try std.testing.expectEqualStrings(root.current.snapshot, root.spaceForIndex("body_text").snapshot);
            const aggregate_name = try @import("lake_index_names.zig").materialization(arena.allocator(), "stats", "rows");
            try std.testing.expectEqualStrings(if (step == 3) original_snapshot.? else root.current.snapshot, root.spaceForIndex(aggregate_name).snapshot);
        }
    }
}

test "external lake definition refresh retains evidence and publishes added renamed removed indexes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-definition-refresh");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "id", .field_id = 1, .values = &.{ 1, 2, 3 } }}, &.{.{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = &.{ "first value", "second value", "last" } }});
    defer a.free(data);
    var uploaded = try client.putObject("antfly", "part.parquet", data, .{});
    uploaded.deinit(a);
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"id":{{"type":"integer"}},"body":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    const artifacts_path = try std.fs.path.join(a, &.{ directory.path(), "indexes" });
    defer a.free(artifacts_path);
    var artifacts_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifacts_path);
    defer artifacts_fs.deinit();
    var store = artifacts_fs.artifactStore();
    const configs = [_][]const u8{
        \\{"text":{"type":"full_text","field":"body","store_source":false},"stats":{"type":"algebraic","materializations":[{"name":"rows","op":"count"}]}}
        ,
        \\{"text":{"type":"full_text","field":"body","store_source":false},"extra":{"type":"full_text","field":"body","store_source":false},"stats":{"type":"algebraic","materializations":[{"name":"rows","op":"count"}]}}
        ,
        \\{"text":{"type":"full_text","field":"body","store_source":false},"renamed":{"type":"full_text","field":"body","store_source":false},"stats":{"type":"algebraic","materializations":[{"name":"rows","op":"count"}]}}
        ,
        \\{"text":{"type":"full_text","field":"body","store_source":false},"stats":{"type":"algebraic","materializations":[{"name":"rows","op":"count"}]}}
    };
    const Fixed = struct {
        now: u64 = 100,
        fn read(raw: *const anyopaque) !u64 {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).now;
        }
    };
    var time: Fixed = .{};
    var bytes_: []u8 = &.{};
    defer a.free(bytes_);
    var table: records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = schema, .indexes_json = configs[0] };
    var roots = std.heap.ArenaAllocator.init(a);
    defer roots.deinit();
    var prior: ?@import("lake_row_identity.zig").Root = null;
    var original_text: ?[]u8 = null;
    defer if (original_text) |id| a.free(id);
    for (configs, 0..) |config, step| {
        table.indexes_json = config;
        time.now += 100;
        var source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        if (prior) |old| {
            store.upload_scope = .{ .domain = old.domain, .attempt = @splat(7) };
            var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
            const navigation = @import("lake_source_navigation.zig");
            const old_nav = try navigation.load(roots.allocator(), store, old.current.navigation.?, .none, null);
            var next_nav = try navigation.build(a, roots.allocator(), &store, &provider, old_nav, .none);
            defer next_nav.deinit();
            provider.native_delta = next_nav.delta;
            const Denied = struct {
                fn get(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: local.storage_object_storage.GetOptions) !local.storage_object_storage.GetResult {
                    return error.UnexpectedDefinitionOnlyParquetRead;
                }
            };
            var vtable = source.scanner.object_reader.client.vtable.*;
            const original = source.scanner.object_reader.client.vtable;
            vtable.get_object = Denied.get;
            source.scanner.object_reader.client.vtable = &vtable;
            defer source.scanner.object_reader.client.vtable = original;
            var captured: ?*@import("lake_row_payloads.zig").Capture = null;
            defer if (captured) |capture| capture.deinit();
            const base = try binding.binding.toManifestBaseSource(source.inventory.snapshot_id, "inventory");
            const rebound = (try @import("lake_row_identity_build.zig").build(a, roots.allocator(), table, &store, &provider, old.cut, base, next_nav.ref, old, .none, &captured)).?;
            try std.testing.expect(captured == null);
            try std.testing.expectEqual(old.current.by_id, rebound.current.by_id);
            try std.testing.expectEqual(old.current.input_versions, rebound.current.input_versions);
            try std.testing.expectEqualSlices(u8, &old.values_recipe, &rebound.values_recipe);
            try std.testing.expectEqualSlices(u8, &catalog.desiredFingerprint(table), &rebound.definition.?);
        }
        const pending = try begin(a, std.testing.io, table, &source, @splat(4), .{}, time.now - 1, 100000);
        defer a.free(pending);
        var attempt = table;
        attempt.lake_index_catalog_json = pending;
        const complete = try build(a, &store, attempt, &source, @splat(4), .{ .io = std.testing.io }, .none, .{ .ptr = &time, .now_ms = Fixed.read });
        a.free(bytes_);
        bytes_ = complete;
        table.lake_index_catalog_json = complete;
        var state = try catalog.parse(a, complete);
        defer state.deinit();
        try @import("lake_index_directory.zig").hydrate(state.arena.allocator(), store, &state.value.published.?, .none, null);
        const published = state.value.published.?;
        try std.testing.expectEqual(@as(usize, if (step == 1 or step == 2) 3 else 2), published.declarations.len);
        var found_extra = false;
        var found_renamed = false;
        for (published.declarations) |declaration| {
            if (std.mem.eql(u8, declaration.name, "text")) {
                if (original_text) |id| try std.testing.expectEqualStrings(id, declaration.artifact.artifact_id) else original_text = try a.dupe(u8, declaration.artifact.artifact_id);
            }
            found_extra = found_extra or std.mem.eql(u8, declaration.name, "extra");
            found_renamed = found_renamed or std.mem.eql(u8, declaration.name, "renamed");
        }
        try std.testing.expectEqual(step == 1, found_extra);
        try std.testing.expectEqual(step == 2, found_renamed);
        prior = try @import("lake_row_identity.zig").load(roots.allocator(), store, published.row_identity.?, .none, null);
        try @import("lake_row_identity_reader.zig").validate(roots.allocator(), prior.?, published, prior.?.domain, &source);
    }
}
