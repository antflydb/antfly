// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Artifact construction for one native catalog-fenced lake index attempt.
//! The coordinator commits the returned state with a full definition CAS;
//! successful uploads alone never make an index ready.
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
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-lake-index-upload-domain-v1");
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, table_id, .little);
    hash.update(&id);
    hash.update(&store_identity);
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
    const signature = try signatureFor(a, table, source, store_identity, context);
    var current = try catalog.parse(a, table.lake_index_catalog_json);
    defer current.deinit();
    const generation = std.math.add(u64, current.value.generation, 1) catch return error.LakeIndexGenerationExhausted;
    const scope = try stores.UploadScope.forPublication(uploadDomain(table.table_id, store_identity), generation, io);
    return catalog.encode(a, try current.value.begin(signature, scope.attempt, now_ms, lease_ms));
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
    const scope: stores.UploadScope = .{ .domain = uploadDomain(table.table_id, store_identity), .attempt = attempt.token };
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
    if (current.value.published) |*previous| {
        if (std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) {
            try @import("lake_index_directory.zig").hydrate(current.arena.allocator(), scoped, previous, cancellation, null);
        }
    }
    const reusable: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (current.value.published) |published|
        if (std.mem.eql(u8, &published.signature.source, &signature.source) and
            std.mem.eql(u8, &published.signature.credentials, &signature.credentials) and
            std.mem.eql(u8, &published.signature.store, &signature.store)) published.declarations else &.{}
    else
        &.{};
    // Native exact reducers own algebraic publication; do not produce narrow
    // legacy i64 folds alongside them or expose those as SQL materializations.
    var native_arena = std.heap.ArenaAllocator.init(a);
    defer native_arena.deinit();
    const na = native_arena.allocator();
    var legacy_indexes = try std.json.parseFromSliceLeaky(std.json.Value, na, table.indexes_json, .{ .allocate = .alloc_always });
    if (legacy_indexes != .object) return error.InvalidTableIndexMetadata;
    var index_position: usize = 0;
    while (index_position < legacy_indexes.object.count()) {
        const config = legacy_indexes.object.values()[index_position];
        const is_algebraic = if (config == .object) if (config.object.get("type")) |kind| kind == .string and std.mem.eql(u8, kind.string, "algebraic") else false else false;
        if (is_algebraic) _ = legacy_indexes.object.orderedRemove(legacy_indexes.object.keys()[index_position]) else index_position += 1;
    }
    const legacy_json = try std.json.Stringify.valueAlloc(na, legacy_indexes, .{});
    var manifest = try rebuild.reconcileResolvedExternalSourceSidecarsWithRuntimeAlloc(a, &scoped, provider.provider(), base_source, source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .read_schema_json = table.read_schema_json, .indexes_json = legacy_json }, reusable, cancellation, .{ .published_generation = attempt.generation, .edge_generation = attempt.generation, .computed_at_ms = started }, .{}, scope);
    defer manifest.deinit(a);
    const previous_contributions = if (current.value.published) |previous| if (std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) previous.file_contributions else &.{} else &.{};
    const native = try @import("lake_index_native_aggregates.zig").buildIncremental(a, na, table, source, &scoped, &provider, cancellation, reusable, previous_contributions);
    const native_declarations = native.declarations;
    const declarations = try na.alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, manifest.artifacts.len + native_declarations.len);
    @memcpy(declarations[0..manifest.artifacts.len], manifest.artifacts);
    @memcpy(declarations[manifest.artifacts.len..], native_declarations);
    try cancellation.check();
    const verified = try coverage.pin(source, context);
    if (!std.meta.eql(pinned, verified)) return error.ExternalLakeIndexSourceChanged;
    try context.ensureActive();
    const directory = try @import("lake_index_directory.zig").publishWithContributions(a, &scoped, declarations, native.contributions, cancellation);
    defer a.free(directory.artifact_id);
    defer a.free(directory.checksum);
    try context.ensureActive();
    const completed = try clock.now_ms(clock.ptr);
    const publication: catalog.Publication = .{ .generation = attempt.generation, .token = attempt.token, .signature = signature, .published_at_ms = completed, .base_source = base_source, .inventory = inventory, .directory = directory };
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
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .values = &.{ "first value", "second value" } }});
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
    const table: records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"}}" };
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
    const published_bytes = try build(a, &artifact_store, pending, &source, store_identity, .{}, .none, clock);
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
        try std.testing.expectEqual(local.serverless_manifest_artifact_ref.ArtifactKind.text_segment, declaration.artifact.kind);
        const upload = (try stores.uploadScopeFromArtifactId(declaration.artifact.artifact_id)).?;
        try std.testing.expectEqual(@as(u64, 1), upload.fencingToken());
        try std.testing.expectEqual(uploadDomain(table.table_id, store_identity), upload.domain);
        const loaded = try artifact_store.getVerifiedAllocWithCancellation(declaration.artifact.artifact_id, declaration.artifact.byte_len, declaration.artifact.checksum, .none);
        defer a.free(loaded);
        try std.testing.expect(loaded.len > 0);
    }
    time.now = 120;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, build(a, &artifact_store, pending, &source, store_identity, .{}, .none, clock));
    var changed = pending;
    changed.indexes_json = "{}";
    time.now = 101;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, build(a, &artifact_store, changed, &source, store_identity, .{}, .none, clock));
}
