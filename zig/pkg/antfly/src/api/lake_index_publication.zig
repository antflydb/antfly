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
pub const Lease = struct {
    ptr: *anyopaque,
    snapshot: *const fn (*anyopaque, A) anyerror!local.common_topology_records.TableRecord,
    save_checkpoint: ?*const fn (*anyopaque, catalog.Publication) anyerror!void = null,
    discard_checkpoint: ?*const fn (*anyopaque) anyerror!void = null,
};
const BuildProgress = struct {
    a: A,
    io: ?std.Io = null,
    mutex: std.Io.Mutex = .init,
    store: *stores.ArtifactStore,
    cancellation: Cancellation,
    clock: Clock,
    lease: Lease,
    saved: catalog.Publication,
    declarations: std.ArrayList(std.json.Parsed(local.serverless_segment_sidecar_manifest.DeclaredArtifact)) = .empty,
    fn deinit(self: *BuildProgress) void {
        for (self.declarations.items) |*declaration| declaration.deinit();
        self.declarations.deinit(self.a);
    }
    fn remember(self: *BuildProgress, declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        // Each current declaration owns a reclaimable region, independently
        // of builder/file arenas and of declarations it replaces.
        const bytes = try std.json.Stringify.valueAlloc(self.a, declaration, .{});
        defer self.a.free(bytes);
        var owned = try std.json.parseFromSlice(local.serverless_segment_sidecar_manifest.DeclaredArtifact, self.a, bytes, .{ .allocate = .alloc_always });
        errdefer owned.deinit();
        const slot = for (self.declarations.items, 0..) |old, index| {
            if (std.mem.eql(u8, old.value.name, owned.value.name)) break index;
        } else null;
        if (slot) |index| {
            self.declarations.items[index].deinit();
            self.declarations.items[index] = owned;
        } else try self.declarations.append(self.a, owned);
    }
    fn save(raw: *anyopaque, declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        return saveBatch(raw, &.{declaration});
    }
    fn saveBatch(raw: *anyopaque, completed: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        if (completed.len == 0) return;
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.io) |io| try self.mutex.lock(io);
        defer if (self.io) |io| self.mutex.unlock(io);
        for (completed) |declaration| {
            try self.cancellation.check();
            try self.remember(declaration);
        }
        return self.persist();
    }
    fn saveNavigation(raw: *anyopaque, ref: local.serverless_manifest_artifact_ref.ArtifactRef) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.io) |io| try self.mutex.lock(io);
        defer if (self.io) |io| self.mutex.unlock(io);
        self.saved.source_navigation = ref;
        try self.persist();
    }
    fn persist(self: *@This()) !void {
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const declarations = try scratch.allocator().alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, self.declarations.items.len);
        for (declarations, self.declarations.items) |*value, owned| value.* = owned.value;
        var saved = self.saved;
        saved.directory = try @import("lake_index_directory.zig").publish(scratch.allocator(), self.store, declarations, self.cancellation);
        const maps = @import("../serverless/artifacts/cloud_generations.zig");
        const writer = if (self.store.providerStore()) |object| object.cloud_writer else null;
        const map_root = if (writer) |w| try w.finish(self.cancellation) else null;
        defer if (map_root) |root| maps.freeRef(writer.?.a, root);
        // A checkpoint is a closed subset of finished declarations. Preserve
        // selector completeness so restart does not turn an incremental build
        // into a migration audit; it carries no publication certificate.
        if (map_root) |root| saved.cloud_generations = .{ .root = root, .complete = writer.?.complete and !writer.?.disabled, .vacuum = writer.?.vacuum_progress };
        saved.published_at_ms = try self.clock.now_ms(self.clock.ptr);
        try self.lease.save_checkpoint.?(self.lease.ptr, saved);
    }
};
pub fn buildWithLease(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease) ![]u8 {
    return buildWithLeaseAndEmbedding(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, null, null);
}
pub fn buildWithLeaseAndEmbedding(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, memo: ?@import("lake_vector_enrichment.zig").Memo) ![]u8 {
    return buildAttempt(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, true) catch |err| {
        switch (err) {
            error.LakeIndexArtifactUnavailable, error.CloudGenerationMapUnavailable, error.ArtifactIntegrityMismatch => {},
            else => return err,
        }
        try context.ensureActive();
        try cancellation.check();
        // Rebuild once without any derived reuse. Keep the serving publication
        // in authority until its replacement commits, and never replay an
        // ambiguous checkpoint CAS. Source failures do not enter this path.
        if (lease) |owner| {
            const latest = try owner.snapshot(owner.ptr, a);
            defer a.free(latest.lake_index_catalog_json);
            var saved = try catalog.parse(a, latest.lake_index_catalog_json);
            defer saved.deinit();
            if (saved.value.checkpoint != null) {
                const discard = owner.discard_checkpoint orelse return err;
                try discard(owner.ptr);
            }
            const fresh = try owner.snapshot(owner.ptr, a);
            defer a.free(fresh.lake_index_catalog_json);
            return buildAttempt(a, artifact_store, fresh, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, false);
        }
        return buildAttempt(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, false);
    };
}
/// Prior inventory is an optional planning proof. Missing/corrupt derived
/// metadata migrates through full native reconciliation; operational failures
/// retain their normal cancellation/retry semantics.
fn priorInventory(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, token: Cancellation) !?local.serverless_external_source_types.Inventory {
    try token.check();
    if (ref.byte_len == 0 or ref.byte_len > (local.serverless_external_source_mod.codec.DecodeLimits{}).max_artifact_bytes) return null;
    const bytes = @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, null) catch |err| switch (err) {
        error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.CloudGenerationMapUnavailable, error.LakeIndexArtifactUnavailable => return null,
        else => return err,
    };
    defer a.free(bytes);
    try token.check();
    const inventory = @import("lake_source_navigation.zig").decodeInventory(a, store, bytes, token) catch |err| switch (err) {
        error.InvalidNativeLakeFileState, error.InvalidGraphPage, error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.CloudGenerationMapUnavailable, error.LakeIndexArtifactUnavailable => return null,
        else => return err, // paged inventory reads retain operational failures
    };
    return inventory;
}
fn buildAttempt(backing: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, memo: ?@import("lake_vector_enrichment.zig").Memo, reuse: bool) ![]u8 {
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = backing };
    const a = allocations.allocator();
    try context.ensureActive();
    try cancellation.check();
    var current = try catalog.parse(a, table.lake_index_catalog_json);
    defer current.deinit();
    const reuse_published = reuse and (if (current.value.checkpoint) |saved| saved.reuse_published else true);
    if (!reuse_published) current.value.published = null;
    if (!reuse) current.value.checkpoint = null;
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
    const cloud_api = @import("lake_cloud_publication.zig");
    const maps = @import("../serverless/artifacts/cloud_generations.zig");
    const cloud_object = artifact_store.providerStore();
    if (cloud_object) |object| object.cloud_io = context.io;
    const cloud_seed_candidate = if (!reuse) @as(?catalog.Publication, null) else if (current.value.checkpoint) |saved| if (saved.cloud_generations != null and std.meta.eql(saved.signature, signature) and std.meta.eql(saved.namespace, current.value.namespace)) @as(?catalog.Publication, saved) else current.value.published else current.value.published;
    const cloud_seed = if (cloud_seed_candidate) |seed_| if (std.meta.eql(seed_.namespace, current.value.namespace) and std.mem.eql(u8, &seed_.signature.store, &store_identity)) @as(?catalog.Publication, seed_) else null else null;
    var cloud_binding: ?cloud_api.Binding = if (cloud_object != null and cloud_seed != null) try cloud_api.Binding.init(std.heap.page_allocator, cloud_object.?, cloud_seed.?, cancellation) else null;
    defer if (cloud_binding) |*held| held.deinit();
    // Fresh/checkpoint artifacts may not belong to the previous closure. A
    // builder verifies and records them; a query's complete binding stays strict.
    if (cloud_binding) |*held| held.reader.complete = false;
    var cloud_memory: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 64 * 1024 * 1024 };
    var map_alloc: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = cloud_memory.allocator() };
    var cloud_writer: ?maps.Writer = if (cloud_object) |object| if (object.fs_client == null and object.client.vtable.version_retention != null) .{ .a = map_alloc.allocator(), .io = context.io, .vacuum_progress = if (cloud_seed) |seed_| if (seed_.cloud_generations) |cloud| cloud.vacuum else .{} else .{}, .reader = .{ .io = context.io, .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .shared = maps.sharedNavigation(object.cloud_cache_scope), .root = if (cloud_binding) |held| try maps.cloneRef(map_alloc.allocator(), held.reader.root.?) else null }, .scope = scope, .qualified_retention = object.cloud_retention_qualified, .cancellation = cancellation, .complete = cloud_api.seedComplete(cloud_seed, current.value.checkpoint, signature, current.value.namespace, !reuse or (current.value.published == null and current.value.checkpoint == null)) } else null else null;
    const previous_writer = if (cloud_object) |object| object.cloud_writer else null;
    if (cloud_writer) |*writer| cloud_object.?.cloud_writer = writer;
    defer if (cloud_writer) |*writer| {
        cloud_object.?.cloud_writer = previous_writer;
        writer.deinit();
    };
    // The descriptor and source navigation negotiate the same identity epoch.
    // Identity belongs to the pinned source binding, never the index build.
    try source.ensureInventory(context);
    try source.ensureCanonicalPlan(context);
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = source, .context = context, .expected_delete_objects = pinned.delete_objects, .embedding_options = embedding_options, .vector_memo = memo };
    // Same-label source replacements and credential/store changes prohibit
    // reuse even if a legacy sidecar's binding happens to look identical.
    var reusable_directory = true;
    if (current.value.published) |*previous| {
        if (std.meta.eql(previous.namespace, current.value.namespace) and std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) {
            @import("lake_index_directory.zig").hydrateLazy(current.arena.allocator(), scoped, previous, cancellation, null) catch |err| switch (err) {
                error.FileNotFound, error.NotFound, error.ArtifactIntegrityMismatch, error.LakeIndexArtifactUnavailable => reusable_directory = false,
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
    const published_candidates: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (current.value.published) |published|
        if (reusable_directory and std.meta.eql(published.namespace, current.value.namespace) and std.mem.eql(u8, &published.signature.credentials, &signature.credentials) and std.mem.eql(u8, &published.signature.store, &signature.store)) published.declarations else &.{}
    else
        &.{};
    // Native exact reducers own algebraic publication; do not produce narrow
    // legacy i64 folds alongside them or expose those as SQL materializations.
    var native_arena = std.heap.ArenaAllocator.init(a);
    defer native_arena.deinit();
    const na = native_arena.allocator();
    var candidate_list: std.ArrayList(local.serverless_segment_sidecar_manifest.DeclaredArtifact) = .empty;
    if (current.value.checkpoint) |*saved| {
        if (std.meta.eql(saved.signature, signature) and std.meta.eql(saved.namespace, current.value.namespace)) {
            @import("lake_index_directory.zig").hydrateLazy(current.arena.allocator(), scoped, saved, cancellation, null) catch |err| switch (err) {
                error.FileNotFound, error.NotFound, error.ArtifactIntegrityMismatch, error.LakeIndexArtifactUnavailable => {
                    // A restored catalog may reference collected derived work.
                    // Clear authority before rebuilding; transient reads and
                    // ambiguous CAS outcomes must still stop this attempt.
                    try context.ensureActive();
                    try cancellation.check();
                    const owner = lease orelse return err;
                    const discard = owner.discard_checkpoint orelse return err;
                    try discard(owner.ptr);
                    current.value.checkpoint = null;
                },
                else => return err,
            };
            if (current.value.checkpoint) |available| try candidate_list.appendSlice(na, available.declarations);
        }
    }
    for (published_candidates) |declaration| {
        const present = for (candidate_list.items) |saved| {
            if (std.mem.eql(u8, declaration.name, saved.name)) break true;
        } else false;
        if (!present) try candidate_list.append(na, declaration);
    }
    const candidates = candidate_list.items;
    const navigation_api = @import("lake_source_navigation.zig");
    var prior_navigation: ?navigation_api.Root = null;
    if (current.value.published) |previous| {
        if (reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and
            std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and
            std.mem.eql(u8, &previous.signature.store, &signature.store))
            if (previous.source_navigation) |ref| {
                prior_navigation = navigation_api.load(na, scoped, ref, cancellation, null) catch |err| switch (err) {
                    error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.InvalidNativeLakeFileState, error.CloudGenerationMapUnavailable, error.LakeIndexArtifactUnavailable => null,
                    else => return err,
                };
            };
    }
    var navigation = try navigation_api.buildProven(a, na, &scoped, &provider, prior_navigation, cancellation, pinned.data);
    defer navigation.deinit();
    provider.native_delta = navigation.delta;
    const inventory_bytes = try navigation_api.encodeInventory(a, &provider, navigation.root);
    defer a.free(inventory_bytes);
    var inventory_artifact = try scoped.putScoped(scope, inventory_bytes, cancellation);
    defer inventory_artifact.deinit(a);
    const inventory: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .external_base_source, .metadata_version = navigation_api.inventoryVersion(inventory_bytes), .artifact_id = inventory_artifact.artifact_id, .checksum = inventory_artifact.checksum, .byte_len = inventory_artifact.byte_len };
    const base_source = try binding.binding.toManifestBaseSource(source.inventory.snapshot_id, inventory.artifact_id);

    var progress: BuildProgress = .{ .a = a, .io = context.io, .store = &scoped, .cancellation = cancellation, .clock = clock, .lease = lease orelse .{ .ptr = undefined, .snapshot = undefined }, .saved = .{ .reader_protocol = attempt.reader_protocol, .reuse_published = reuse_published, .store_locator = attempt.store_locator, .namespace = current.value.namespace, .generation = attempt.generation, .token = attempt.token, .signature = signature, .published_at_ms = started, .base_source = base_source, .inventory = inventory, .source_navigation = navigation.ref } };
    defer progress.deinit();
    if (lease) |owner| if (owner.save_checkpoint != null) {
        if (current.value.checkpoint) |saved| for (saved.declarations) |declaration| try progress.remember(declaration);
        provider.progress = .{ .ptr = &progress, .save = BuildProgress.save, .save_batch = BuildProgress.saveBatch };
    };
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
    if (prior_navigation) |prior| {
        if (previous_root != null and previous_ownership == 1) contribution_index.predecessor = .{ .source = prior.source, .schema = prior.schema, .snapshot = prior.snapshot, .count = prior.count };
    }
    // The authenticated inventory supplies the file delta once per refresh.
    // Counted ownership proves all prior files belong to the unchanged recipes;
    // replay need not point-probe every file/recipe in the contribution tree.
    if (current.value.published) |previous| {
        if (reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and
            std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and
            std.mem.eql(u8, &previous.signature.store, &signature.store) and
            (prior_navigation == null or previous_root != null) and
            source.inventory.deleted_row_groups.len == 0 and
            (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true))
        {
            if (!contribution_index.acceptsDelta(navigation.delta)) {
                if (try priorInventory(na, scoped, previous.inventory, cancellation)) |old_inventory| {
                    if (old_inventory.deleted_row_groups.len == 0) {
                        provider.previous_inventory = old_inventory;
                        if (previous_root != null and previous_ownership == 1 and std.mem.eql(u8, &previous.signature.desired, &signature.desired)) {
                            for (old_inventory.files) |file| try contribution_index.covered_files.put(a, @import("lake_index_native_aggregates.zig").inventoryFileIdentity(old_inventory, file), {});
                            contribution_index.has_file_coverage = true;
                        }
                    }
                }
            }
        }
    }
    const replay_api = @import("lake_index_build_replay.zig");
    const replay_columns = try replay_api.columnsForBuild(a, na, table, &provider, base_source);
    var replay = replay_api.Replay.init(a, &provider, replay_columns);
    defer replay.deinit();
    if (replay_columns.len != 0 and provider.replay_recommended) if (current.value.published) |previous| if (candidates.len != 0 and std.mem.eql(u8, &previous.signature.desired, &signature.desired)) {
        replay.only_files = try replay_api.changedFilesIndexed(a, na, &provider, scoped, candidates, previous_contributions, cancellation, &contribution_index);
    };
    if (replay_columns.len != 0 and provider.replay_recommended) provider.replay = &replay;
    var statistics_prior = prior_navigation;
    if (current.value.checkpoint) |saved| if (std.meta.eql(saved.signature, signature) and std.meta.eql(saved.namespace, current.value.namespace)) if (saved.source_navigation) |ref| {
        const root: ?navigation_api.Root = navigation_api.load(na, scoped, ref, cancellation, null) catch |err| switch (err) {
            error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.InvalidNativeLakeFileState, error.CloudGenerationMapUnavailable, error.LakeIndexArtifactUnavailable => null,
            else => return err,
        };
        if (root) |value| if (value.coverage != null and std.mem.eql(u8, &value.coverage.?.data, &pinned.data)) {
            statistics_prior = value;
        };
    };
    var costing = try navigation_api.CostingBuilder.init(a, na, &scoped, &provider, navigation.ref, statistics_prior, replay_columns, cancellation);
    defer costing.deinit();
    try costing.start();
    if (lease) |owner| {
        if (owner.save_checkpoint != null) costing.checkpoint = .{ .ptr = &progress, .save = BuildProgress.saveNavigation };
    }
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
    const builders = @import("lake_index_publication_builders.zig");
    const built = (try builders.build(.{ .a = a, .out = na, .table = table, .store = &scoped, .provider = &provider, .base = base_source, .reusable = reusable, .candidates = candidates, .contributions = previous_contributions, .index = &contribution_index, .token = cancellation })) orelse serial: {
        const native = @import("lake_index_native_aggregates.zig").buildIndexed(a, na, table, source, &scoped, &provider, cancellation, reusable, previous_contributions, &contribution_index) catch |err| return constructionFailure(err, "aggregate_build", attempt.generation);
        // Aggregate construction already completes the entire set before this
        // boundary. Publishing every growing prefix writes quadratic directory
        // bytes and metadata commits without saving any earlier build work.
        provider.checkpointBatch(native.declarations) catch |err| return constructionFailure(err, "aggregate_checkpoint", attempt.generation);
        const ordered = @import("lake_index_native_rows.zig").buildIncremental(a, na, table, source, &scoped, &provider, cancellation, reusable, candidates) catch |err| return constructionFailure(err, "ordered_build", attempt.generation);
        for (ordered) |declaration| provider.checkpoint(declaration) catch |err| return constructionFailure(err, "ordered_checkpoint", attempt.generation);
        const text = @import("lake_index_native_text.zig").buildIncremental(a, na, table, source, base_source, &scoped, &provider, cancellation, reusable, candidates) catch |err| return constructionFailure(err, "text_build", attempt.generation);
        for (text) |declaration| provider.checkpoint(declaration) catch |err| return constructionFailure(err, "text_checkpoint", attempt.generation);
        const sparse = @import("lake_index_native_sparse.zig").buildIncremental(a, na, table, source, base_source, &scoped, &provider, cancellation, reusable, candidates) catch |err| return constructionFailure(err, "sparse_build", attempt.generation);
        const dense = @import("lake_index_native_dense.zig").buildIncremental(a, na, table, source, base_source, &scoped, &provider, cancellation, reusable, candidates) catch |err| return constructionFailure(err, "dense_build", attempt.generation);
        break :serial builders.Result{ .native = native, .ordered = ordered, .text = text, .sparse = sparse, .dense = dense };
    };
    if (built.contribution_root) |root| {
        contribution_index.root = root;
        contribution_index.counted = true;
        contribution_index.prior_roots = built.contribution_roots;
        for (built.contribution_roots) |key| try contribution_index.includeRoot(key);
    }
    const native = built.native;
    const native_declarations = native.declarations;
    const ordered = built.ordered;
    const text = built.text;
    const sparse = built.sparse;
    const dense = built.dense;
    const declarations = try na.alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, manifest.artifacts.len + native_declarations.len + ordered.len + text.len + sparse.len + dense.len);
    @memcpy(declarations[0..manifest.artifacts.len], manifest.artifacts);
    @memcpy(declarations[manifest.artifacts.len .. manifest.artifacts.len + native_declarations.len], native_declarations);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len ..][0..ordered.len], ordered);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len + ordered.len ..][0..text.len], text);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len + ordered.len + text.len ..][0..sparse.len], sparse);
    @memcpy(declarations[manifest.artifacts.len + native_declarations.len + ordered.len + text.len + sparse.len ..], dense);
    navigation.ref = try costing.finish();
    progress.saved.source_navigation = navigation.ref;
    try cancellation.check();
    const verified = try coverage.pin(source, context);
    if (!std.meta.eql(pinned, verified)) return error.ExternalLakeIndexSourceChanged;
    try context.ensureActive();
    const directory = try @import("lake_index_directory.zig").publishIndexed(a, &scoped, declarations, native.contributions, &contribution_index, cancellation);
    defer a.free(directory.artifact_id);
    defer a.free(directory.checksum);
    try context.ensureActive();
    // All uploads in this attempt are complete before its namespace evidence
    // is sealed. Certificates are advisory; failure retains the full audit.
    scoped.sealClosures(cancellation) catch {
        try context.ensureActive();
        try cancellation.check();
    };
    var completed = try clock.now_ms(clock.ptr);
    var publication: catalog.Publication = .{ .reader_protocol = attempt.reader_protocol, .store_locator = attempt.store_locator, .namespace = current.value.namespace, .generation = attempt.generation, .token = attempt.token, .signature = signature, .published_at_ms = completed, .base_source = base_source, .inventory = inventory, .source_navigation = navigation.ref, .directory = directory };
    if (cloud_writer) |*writer| try cloud_api.finish(na, writer, cloud_object.?, &publication, declarations, table.table_id, store_identity, context, cancellation, cloud_binding != null and cloud_object.?.cloud_certificate_until_ms != 0 and current.value.checkpoint == null);
    completed = try clock.now_ms(clock.ptr);
    publication.published_at_ms = completed;
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
    var published = pending;
    published.lake_index_catalog_json = published_bytes;
    try std.testing.expect(try catalog.transitionAllowed(a, pending, published));
    var parsed = try catalog.parse(a, published_bytes);
    defer parsed.deinit();
    try @import("lake_index_directory.zig").hydrate(parsed.arena.allocator(), artifact_store, &parsed.value.published.?, .none, null);
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
    const next_pending_bytes = try begin(a, std.testing.io, published, &next_source, store_identity, .{}, 102, 20);
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
    const next_bytes = try build(a, &artifact_store, next_pending, &next_source, store_identity, .{ .io = std.testing.io }, .none, clock);
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

fn constructionFailure(err: anyerror, stage: []const u8, generation: u64) anyerror {
    if (err != error.Canceled and err != error.Cancelled) std.log.warn("native lake construction deferred stage={s} generation={d} err={s}", .{ stage, generation, @errorName(err) });
    return err;
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
