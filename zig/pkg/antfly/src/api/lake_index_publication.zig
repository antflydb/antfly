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

//! Artifact construction for one native catalog-fenced lake index attempt.
//! The coordinator commits the returned state with a full definition CAS;
//! successful uploads alone never make an index ready.
const native_platform = @import("antfly_platform");
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const records = local.common_topology_records;
const serving = local.serverless_query_lake_serving;
const coverage = @import("lake_index_coverage.zig");
const rebuild = @import("../serverless/build/lake_rebuild.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Clock = struct {
    ptr: *const anyopaque,
    now_ms: *const fn (*const anyopaque) anyerror!u64,
};
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
pub const Lease = struct { ptr: *anyopaque, snapshot: *const fn (*anyopaque, A) anyerror!local.common_topology_records.TableRecord };
pub fn buildWithLease(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease) ![]u8 {
    try context.ensureActive();
    try cancellation.check();
    var current = try catalog.parse(a, table.lake_index_catalog_json);
    defer current.deinit();
    const attempt = current.value.pending orelse return error.LakeIndexPublicationFenceChanged;
    const started = try clock.now_ms(clock.ptr);
    if (started < attempt.started_at_ms or started >= attempt.lease_expires_at_ms) return error.LakeIndexPublicationFenceChanged;
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return error.InvalidExternalTableBinding;
    defer binding.deinit(a);
    const pinned = try coverage.pin(source, context);
    const signature: catalog.Signature = .{ .desired = catalog.desiredFingerprint(table), .source = pinned.source, .credentials = try source.credentialIdentity(binding.binding), .store = store_identity };
    if (!std.meta.eql(signature, attempt.signature)) return error.LakeIndexPublicationFenceChanged;
    const scope: stores.UploadScope = .{ .domain = uploadDomainWithNamespace(table.table_id, store_identity, current.value.namespace), .attempt = attempt.token };
    try scope.validate();
    if (scope.fencingToken() != attempt.generation) return error.LakeIndexPublicationFenceChanged;
    var scoped = artifact_store.*;
    scoped.allocator = a;
    scoped.upload_scope = scope;
    const inventory_bytes = try local.serverless_external_source_mod.encodeInventoryAlloc(a, source.inventory);
    defer a.free(inventory_bytes);
    var inventory_artifact = try scoped.putScoped(scope, inventory_bytes, cancellation);
    defer inventory_artifact.deinit(a);
    const inventory: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .external_base_source, .artifact_id = inventory_artifact.artifact_id, .checksum = inventory_artifact.checksum, .byte_len = inventory_artifact.byte_len };
    const base_source = try binding.binding.toManifestBaseSource(source.inventory.snapshot_id, inventory.artifact_id);
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = source, .context = context, .expected_delete_objects = pinned.delete_objects };
    // Same-label source replacements and credential/store changes prohibit
    // reuse even if a legacy sidecar's binding happens to look identical.
    var reusable_directory = true;
    if (current.value.published) |*previous| {
        if (std.meta.eql(previous.namespace, current.value.namespace) and std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) {
            @import("lake_index_directory.zig").hydrateLazy(current.arena.allocator(), scoped, previous, cancellation, null) catch |err| switch (err) {
                error.FileNotFound, error.NotFound, error.ArtifactIntegrityMismatch => reusable_directory = false,
                else => return err,
            };
        }
    }
    const reusable: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (current.value.published) |published|
        if (reusable_directory and std.meta.eql(published.namespace, current.value.namespace) and std.mem.eql(u8, &published.signature.source, &signature.source) and
            std.mem.eql(u8, &published.signature.credentials, &signature.credentials) and
            std.mem.eql(u8, &published.signature.store, &signature.store)) published.declarations else &.{}
    else
        &.{};
    const candidates: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (current.value.published) |published|
        if (reusable_directory and std.meta.eql(published.namespace, current.value.namespace) and std.mem.eql(u8, &published.signature.credentials, &signature.credentials) and std.mem.eql(u8, &published.signature.store, &signature.store)) published.declarations else &.{}
    else
        &.{};
    // Native exact reducers own algebraic publication; do not produce narrow
    // legacy i64 folds alongside them or expose those as SQL materializations.
    var native_arena = std.heap.ArenaAllocator.init(a);
    defer native_arena.deinit();
    const na = native_arena.allocator();
    const previous_contributions = if (current.value.published) |previous| if (reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) previous.file_contributions else &.{} else &.{};
    const contributions_api = @import("lake_index_contributions.zig");
    var contribution_index: contributions_api.Index = undefined;
    const previous_root = if (current.value.published) |previous| if (reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) if (previous.contribution_index) |bytes| try std.json.parseFromSliceLeaky(@import("../serverless/graph_segment/page_tree.zig").Ref, na, bytes, .{}) else null else null else null;
    try contribution_index.init(a, scoped, previous_root, cancellation);
    const previous_ownership = if (current.value.published) |previous| previous.contribution_ownership_version else 0;
    contribution_index.counted = true;
    contribution_index.migrating = previous_root != null and previous_ownership == 0;
    contribution_index.prior_roots = if (previous_root != null and previous_ownership == 1) current.value.published.?.contribution_roots else &.{};
    defer contribution_index.deinit();
    // The authenticated inventory supplies the file delta once per refresh.
    // Counted ownership proves all prior files belong to the unchanged recipes;
    // replay need not point-probe every file/recipe in the contribution tree.
    if (previous_root != null and previous_ownership == 1) {
        const previous = current.value.published.?;
        if (std.mem.eql(u8, &previous.signature.desired, &signature.desired) and source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true)) {
            if (previous.inventory.byte_len > (local.serverless_external_source_mod.codec.DecodeLimits{}).max_artifact_bytes) return error.ExternalSourceInventoryTooLarge;
            const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(na, scoped, .{ .artifact_id = previous.inventory.artifact_id, .checksum = previous.inventory.checksum, .byte_len = previous.inventory.byte_len }, cancellation, null);
            const old_inventory = try local.serverless_external_source_mod.decodeInventoryAlloc(na, bytes);
            if (old_inventory.deleted_row_groups.len == 0) {
                for (old_inventory.files) |file| try contribution_index.covered_files.put(a, @import("lake_index_native_aggregates.zig").inventoryFileIdentity(old_inventory, file), {});
                contribution_index.has_file_coverage = true;
            }
        }
    }
    const replay_api = @import("lake_index_build_replay.zig");
    const replay_columns = try replay_api.columnsForBuild(a, na, table, &provider, base_source);
    var replay = replay_api.Replay.init(a, &provider, replay_columns);
    defer replay.deinit();
    if (current.value.published) |previous| if (candidates.len != 0 and std.mem.eql(u8, &previous.signature.desired, &signature.desired)) {
        replay.only_files = try replay_api.changedFilesIndexed(a, na, &provider, scoped, candidates, previous_contributions, cancellation, &contribution_index);
    };
    if (replay_columns.len != 0) provider.replay = &replay;
    var legacy_indexes = try std.json.parseFromSliceLeaky(std.json.Value, na, table.indexes_json, .{ .allocate = .alloc_always });
    if (legacy_indexes != .object) return error.InvalidTableIndexMetadata;
    var index_position: usize = 0;
    while (index_position < legacy_indexes.object.count()) {
        const config = legacy_indexes.object.values()[index_position];
        const is_native = if (config == .object) if (config.object.get("type")) |kind| kind == .string and (std.mem.eql(u8, kind.string, "algebraic") or std.mem.eql(u8, kind.string, "full_text") or std.mem.eql(u8, kind.string, "embeddings")) else false else false;
        if (is_native) _ = legacy_indexes.object.orderedRemove(legacy_indexes.object.keys()[index_position]) else index_position += 1;
    }
    const legacy_json = try std.json.Stringify.valueAlloc(na, legacy_indexes, .{});
    var manifest = try rebuild.reconcileResolvedExternalSourceSidecarsWithRuntimeAlloc(a, &scoped, provider.provider(), base_source, source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .read_schema_json = table.read_schema_json, .indexes_json = legacy_json }, reusable, cancellation, .{ .published_generation = attempt.generation, .edge_generation = attempt.generation, .computed_at_ms = started }, .{}, scope);
    defer manifest.deinit(a);
    const native = try @import("lake_index_native_aggregates.zig").buildIndexed(a, na, table, source, &scoped, &provider, cancellation, reusable, previous_contributions, &contribution_index);
    const native_declarations = native.declarations;
    const ordered = try @import("lake_index_native_rows.zig").buildIncremental(a, na, table, source, &scoped, &provider, cancellation, reusable, candidates);
    const text = try @import("lake_index_native_text.zig").buildIncremental(a, na, table, source, base_source, &scoped, &provider, cancellation, reusable, candidates);
    const sparse = try @import("lake_index_native_sparse.zig").buildIncremental(a, na, table, source, base_source, &scoped, &provider, cancellation, reusable, candidates);
    const dense = try @import("lake_index_native_dense.zig").buildIncremental(a, na, table, source, base_source, &scoped, &provider, cancellation, reusable, candidates);
    const declarations = try na.alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, manifest.artifacts.len + native_declarations.len + ordered.len + text.len + sparse.len + dense.len);
    @memcpy(declarations[0..manifest.artifacts.len], manifest.artifacts);
    @memcpy(declarations[manifest.artifacts.len .. manifest.artifacts.len + native_declarations.len], native_declarations);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len ..][0..ordered.len], ordered);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len + ordered.len ..][0..text.len], text);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len + ordered.len + text.len ..][0..sparse.len], sparse);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len + ordered.len + text.len + sparse.len ..], dense);
    try cancellation.check();
    const verified = try coverage.pin(source, context);
    if (!std.meta.eql(pinned, verified)) return error.ExternalLakeIndexSourceChanged;
    try context.ensureActive();
    const directory = try @import("lake_index_directory.zig").publishIndexed(a, &scoped, declarations, native.contributions, &contribution_index, cancellation);
    defer a.free(directory.artifact_id);
    defer a.free(directory.checksum);
    try context.ensureActive();
    const completed = try clock.now_ms(clock.ptr);
    const publication: catalog.Publication = .{ .reader_protocol = attempt.reader_protocol, .store_locator = attempt.store_locator, .namespace = current.value.namespace, .generation = attempt.generation, .token = attempt.token, .signature = signature, .published_at_ms = completed, .base_source = base_source, .inventory = inventory, .directory = directory };
    const latest = if (lease) |owner| try owner.snapshot(owner.ptr, a) else table;
    defer if (lease != null) a.free(latest.lake_index_catalog_json);
    var final_state = try catalog.parse(a, latest.lake_index_catalog_json);
    defer final_state.deinit();
    return catalog.encode(a, try final_state.value.publish(publication, completed));
}

test "external lake native publication builds scoped text artifacts and fences expired completion" {
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
    const schema_json = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
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
    const table: records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]}}" };
    const pending_bytes = try begin(a, native_platform.testing.io, table, &source, store_identity, .{}, 100, 20);
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
    const published_bytes = try build(a, &artifact_store, pending, &source, store_identity, .{ .io = native_platform.testing.io }, .none, clock);
    defer a.free(published_bytes);
    var published = pending;
    published.lake_index_catalog_json = published_bytes;
    try std.testing.expect(try catalog.transitionAllowed(a, pending, published));
    var parsed = try catalog.parse(a, published_bytes);
    defer parsed.deinit();
    try @import("lake_index_directory.zig").hydrate(parsed.arena.allocator(), artifact_store, &parsed.value.published.?, .none, null);
    const publication = parsed.value.published.?;
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
        try std.testing.expectEqualStrings("body", root.binding.column_bindings[0]);
        var writer = try native_text.loadWriter(a, artifact_store, root, .none, null, null);
        defer writer.deinit();
        const snapshot = writer.acquireSnapshot();
        defer snapshot.release();
        const results = try snapshot.search(a, "body", &.{"first"}, 10);
        defer a.free(results.hits);
        try std.testing.expectEqual(@as(u32, 1), results.total_count);
        var cache_io = native_platform.Threaded.init(a, .{});
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
        for (warm_snapshot.segments) |segment| try std.testing.expect(segment.reader.postings_loader != null);
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
    const next_pending_bytes = try begin(a, native_platform.testing.io, published, &next_source, store_identity, .{}, 102, 20);
    defer a.free(next_pending_bytes);
    var next_pending = published;
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
    const next_bytes = try build(a, &artifact_store, next_pending, &next_source, store_identity, .{ .io = native_platform.testing.io }, .none, clock);
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
        var count = try local.sql_aggregate_partial.decode(a, values[0].aggregates[0], .{ .kind = .count });
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
