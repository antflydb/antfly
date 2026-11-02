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

//! Complete physical lake publication execution. Owned by serverless in compiled products.
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
const values = @import("lake_execution_values.zig");
const Clock = values.Clock;
const Lease = values.Lease;
const BuildPolicy = values.BuildPolicy;
const uploadDomainWithNamespace = @import("lake_index_publication.zig").uploadDomainWithNamespace;
pub const BuildProgress = struct {
    a: A,
    io: ?std.Io = null,
    mutex: std.Io.Mutex = .init,
    store: *stores.ArtifactStore,
    cancellation: Cancellation,
    clock: Clock,
    lease: Lease,
    saved: catalog.Publication,
    declarations: std.ArrayList(std.json.Parsed(local.serverless_segment_sidecar_manifest.DeclaredArtifact)) = .empty,
    pub fn deinit(self: *BuildProgress) void {
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
    pub fn save(raw: *anyopaque, declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        return saveBatch(raw, &.{declaration});
    }
    pub fn saveBatch(raw: *anyopaque, completed: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
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
pub fn buildWithReuse(a: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, memo: ?@import("lake_vector_enrichment.zig").Memo, policy: BuildPolicy) ![]u8 {
    return buildAttempt(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, policy.allow_reuse, policy.identity_spill_bytes) catch |err| {
        switch (err) {
            error.LakeIndexArtifactUnavailable, error.CloudGenerationMapUnavailable, error.ArtifactIntegrityMismatch, error.InvalidLogicalRowIdentity, error.LogicalRowIdentityAdmissionDeclined => {},
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
            return buildAttempt(a, artifact_store, fresh, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, false, if (err == error.LogicalRowIdentityAdmissionDeclined) 0 else policy.identity_spill_bytes);
        }
        return buildAttempt(a, artifact_store, table, source, store_identity, context, cancellation, clock, lease, embedding_options, memo, false, if (err == error.LogicalRowIdentityAdmissionDeclined) 0 else policy.identity_spill_bytes);
    };
}
/// Prior inventory is an optional planning proof. Missing/corrupt derived
/// metadata migrates through full native reconciliation; operational failures
/// retain their normal cancellation/retry semantics.
pub fn priorInventory(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, token: Cancellation) !?local.serverless_external_source_types.Inventory {
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
fn buildAttempt(backing: A, artifact_store: *stores.ArtifactStore, table: records.TableRecord, source: *serving.ServingSource, store_identity: catalog.Digest, context: Context, cancellation: Cancellation, clock: Clock, lease: ?Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, memo: ?@import("lake_vector_enrichment.zig").Memo, reuse: bool, identity_spill_bytes: u64) ![]u8 {
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
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = source, .context = context, .expected_delete_objects = pinned.delete_objects, .embedding_options = embedding_options, .vector_memo = memo, .identity_spill_bytes = identity_spill_bytes };
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
    var reusable: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (current.value.published) |published|
        if (reusable_directory and std.meta.eql(published.namespace, current.value.namespace) and std.mem.eql(u8, &published.signature.source, &signature.source) and
            std.mem.eql(u8, &published.signature.credentials, &signature.credentials) and
            std.mem.eql(u8, &published.signature.store, &signature.store)) published.declarations else &.{}
    else
        &.{};
    var published_candidates: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (current.value.published) |published|
        if (reusable_directory and std.meta.eql(published.namespace, current.value.namespace) and std.mem.eql(u8, &published.signature.credentials, &signature.credentials) and std.mem.eql(u8, &published.signature.store, &signature.store)) published.declarations else &.{}
    else
        &.{};
    // Native exact reducers own algebraic publication; do not produce narrow
    // legacy i64 folds alongside them or expose those as SQL materializations.
    var native_arena = std.heap.ArenaAllocator.init(a);
    defer native_arena.deinit();
    const na = native_arena.allocator();
    const row_identity_api = @import("lake_row_identity.zig");
    var previous_identity: ?row_identity_api.Root = null;
    if (published_candidates.len != 0) if (current.value.published.?.row_identity) |ref| {
        previous_identity = row_identity_api.load(na, scoped, ref, cancellation, null) catch |err| switch (err) {
            error.FileNotFound, error.NotFound, error.ObjectNotFound, error.CloudGenerationMapUnavailable, error.LakeIndexArtifactUnavailable => null,
            else => return err,
        };
        if (previous_identity) |identity| {
            const previous = current.value.published.?;
            if (!std.mem.eql(u8, &identity.domain, &scope.domain) or !std.mem.eql(u8, &identity.cut, &previous.signature.source) or !std.mem.eql(u8, &identity.definitionFingerprint(), &previous.signature.desired) or !std.mem.eql(u8, &identity.artifacts, &try row_identity_api.declarationDigest(a, previous.declarations))) return error.InvalidLogicalRowIdentity;
        }
    };
    // Physical per-file builders cannot treat a relocated source file as the
    // original contribution. Rebase through construction when values change;
    // pure relocation takes the verified whole-cut reuse path below.
    const mapped_predecessor = (previous_identity != null and previous_identity.?.hasRelocation()) or (current.value.published != null and current.value.published.?.row_identity != null and previous_identity == null);
    if (mapped_predecessor) {
        reusable = &.{};
        published_candidates = &.{};
    }

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
    const previous_contributions = if (current.value.published) |previous| if (!mapped_predecessor and reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) previous.file_contributions else &.{} else &.{};
    const contributions_api = @import("lake_index_contributions.zig");
    var contribution_index: contributions_api.Index = undefined;
    const previous_root = if (current.value.published) |previous| if (!mapped_predecessor and reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and std.mem.eql(u8, &previous.signature.credentials, &signature.credentials) and std.mem.eql(u8, &previous.signature.store, &signature.store)) if (previous.contribution_index) |bytes| try std.json.parseFromSliceLeaky(@import("../serverless/graph_segment/page_tree.zig").Ref, na, bytes, .{}) else null else null else null;
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
        if (!mapped_predecessor and reusable_directory and std.meta.eql(previous.namespace, current.value.namespace) and
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
    const identity_unchanged = previous_identity != null and current.value.published != null and std.meta.eql(current.value.published.?.signature, signature) and relocatableFormats(current.value.published.?.declarations);
    var captured_rows: ?*@import("lake_row_payloads.zig").Capture = null;
    defer if (captured_rows) |capture| capture.deinit();
    var row_identity = if (identity_unchanged) previous_identity else @import("lake_row_identity_build.zig").build(a, na, table, &scoped, &provider, signature.source, base_source, navigation.ref, previous_identity, cancellation, &captured_rows) catch |err| switch (err) {
        // An optional identity declaration is not an enforced SQL constraint.
        // Insufficient identity evidence keeps ordinary complete construction.
        error.NullLogicalRowKey, error.DuplicateLogicalRowKey, error.LogicalRowIdentityAdmissionDeclined => null,
        else => return err,
    };
    var fragments: ?*@import("lake_row_fragments.zig").Build = null;
    defer if (fragments) |value| value.deinit();
    if (!identity_unchanged) if (row_identity) |identity| {
        // Legacy sidecars/enrichment dependencies retain the physical builder.
        const configs = try std.json.parseFromSliceLeaky(std.json.Value, na, table.indexes_json, .{});
        const reusable_inputs = @import("lake_row_input_recipes.zig").nativeFragmentsEligible(configs);
        if (reusable_inputs) fragments = try @import("lake_row_fragments.zig").prepareWithCapture(a, &scoped, table, source, &provider, identity, previous_identity, captured_rows);
    };
    var fragment_replay: ?replay_api.Replay = null;
    defer if (fragment_replay) |*value| value.deinit();
    const native_provider = if (fragments) |value| &value.provider else &provider;
    if (fragments) |value| {
        const projection = try replay_api.columnsForBuild(a, na, table, native_provider, base_source);
        fragment_replay = replay_api.Replay.init(a, native_provider, projection);
        native_provider.replay = &fragment_replay.?;
        // Contribution ownership follows the logical namespace, independently
        // of changes to the table-wide desired fingerprint. Each reducer still
        // requires its own exact predecessor census before applying a delta.
        if (previous_identity != null and previous_identity.?.native_space != null and current.value.published != null and std.mem.eql(u8, &previous_identity.?.namespace, &value.root.namespace)) {
            published_candidates = current.value.published.?.declarations;
            contribution_index.deinit();
            const previous = current.value.published.?;
            const root = if (previous.contribution_index) |bytes| try std.json.parseFromSliceLeaky(@import("../serverless/graph_segment/page_tree.zig").Ref, na, bytes, .{}) else null;
            try contribution_index.init(a, scoped, root, cancellation);
            contribution_index.counted = true;
            contribution_index.migrating = root != null and previous.contribution_ownership_version == 0;
            contribution_index.prior_roots = previous.contribution_roots;
            // Bind each reduction to its original authenticated input census.
            // Retained aggregates can legitimately own different older cuts.
            for (previous.declarations) |declaration| if (declaration.artifact.kind == .algebraic_segment) {
                const view = previous_identity.?.spaceForIndex(declaration.name);
                const ref = view.navigation orelse continue;
                const bound = try navigation_api.load(na, scoped, ref, cancellation, null);
                try contribution_index.predecessors.put(a, declaration.name, .{ .source = bound.source, .schema = bound.schema, .snapshot = bound.snapshot, .count = bound.count, .cut = try navigation_api.cutIdentity(a, bound) });
            };
        }
    }
    const native_candidates = if (fragments != null) published_candidates else candidates;
    var relocated = identity_unchanged;
    var retained_declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = &.{};
    if (row_identity) |*identity| if (previous_identity) |previous| {
        const published = current.value.published.?;
        const native_only = relocatableFormats(published.declarations);
        if (!identity_unchanged and native_only and published.directory != null) {
            var reads: u64 = 0;
            var writes: u64 = 0;
            var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &scoped, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .cancellation = cancellation, .streaming = true };
            pages.admitPublication();
            var bindings = try row_identity_api.proveIndexReuse(a, na, &pages, previous, identity.*, published.declarations);
            if (bindings.len != 0) {
                for (bindings, 0..) |*binding_, index| {
                    var prior_translation: ?row_identity_api.Translation = null;
                    for (bindings[0..index]) |earlier| if (earlier.translation != null and earlier.space.by_id != null and binding_.space.by_id != null and earlier.space.by_id.?.eql(binding_.space.by_id.?)) {
                        prior_translation = earlier.translation;
                        break;
                    };
                    var previous_translation: ?row_identity_api.Translation = null;
                    for (previous.reused) |old_binding| if (old_binding.translation != null and old_binding.space.by_id != null and binding_.space.by_id != null and old_binding.space.by_id.?.eql(binding_.space.by_id.?)) {
                        previous_translation = old_binding.translation;
                        break;
                    };
                    binding_.translation = if (prior_translation) |translation| translation else try @import("lake_row_translation_delta.zig").build(a, &pages, binding_.space, identity.current, if (previous_translation != null) previous.current else null, previous_translation, context, identity_spill_bytes);
                }
                var prospective = identity.*;
                prospective.reused = bindings;
                prospective.artifacts = previous.artifacts;
                try prospective.reseal(a);
                const encoded = try std.json.Stringify.valueAlloc(a, prospective, .{});
                defer a.free(encoded);
                if (encoded.len > row_identity_api.max_root_bytes) bindings = &.{};
            }
            if (bindings.len != 0) {
                identity.reused = bindings;
                identity.artifacts = previous.artifacts;
                try identity.reseal(a);
                const names = try na.alloc([]const u8, bindings.len);
                const retained = try na.alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, bindings.len);
                for (bindings, names, retained) |binding_, *name, *declaration| {
                    name.* = binding_.name;
                    declaration.* = for (published.declarations) |entry| {
                        if (std.mem.eql(u8, entry.name, binding_.name)) break entry;
                    } else return error.InvalidLogicalRowIdentity;
                }
                provider.skip_indexes = names;
                retained_declarations = retained;
                relocated = retained.len == published.declarations.len and try completeNativeReuse(a, table.indexes_json, identity.current.input_recipes, retained);
            } else if (previous.reused.len == 0) {
                const configurations = try std.json.parseFromSliceLeaky(std.json.Value, na, table.indexes_json, .{});
                const captured_inputs = for (configurations.object.values()) |config| {
                    if (!@import("lake_row_input_recipes.zig").configurationReusable(config)) break false;
                } else true;
                if (captured_inputs) relocated = try row_identity_api.proveReuse(a, &pages, previous, identity);
            }
        }
    };
    if (fragments != null) {
        // Prior roots seed incremental reductions; only retained declarations
        // authorize keeping them live in the next generation. Keeping replaced
        // roots creates duplicate recipe seeds after membership changes.
        const retain_aggregate = for (retained_declarations) |declaration| {
            if (declaration.artifact.kind == .algebraic_segment) break true;
        } else false;
        if (retain_aggregate) for (contribution_index.prior_roots) |key| {
            const value = (try contribution_index.lookup(na, key)) orelse return error.InvalidLakeIndexCatalog;
            for (retained_declarations) |declaration| if (declaration.artifact.kind == .algebraic_segment and std.mem.eql(u8, declaration.name, value.name)) {
                try contribution_index.includeRoot(key);
                break;
            };
        };
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
    var manifest = if (relocated) rebuild.ReconciledManifest{ .artifacts = try a.alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, 0) } else try rebuild.reconcileResolvedExternalSourceSidecarsWithRuntimeAlloc(a, &scoped, provider.provider(), base_source, source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .read_schema_json = table.read_schema_json, .indexes_json = legacy_json }, reusable, cancellation, .{ .published_generation = attempt.generation, .edge_generation = attempt.generation, .computed_at_ms = started }, .{}, scope);
    defer manifest.deinit(a);
    if (!relocated) if (fragments) |value| try @import("lake_row_fragments.zig").prepareCapturedSpool(value, &provider, &scoped, captured_rows);
    const builders = @import("lake_index_publication_builders.zig");
    native_provider.skip_indexes = provider.skip_indexes;
    const built = if (relocated) builders.Result{} else (try builders.build(.{ .a = a, .out = na, .table = table, .store = &scoped, .provider = native_provider, .base = base_source, .reusable = reusable, .candidates = native_candidates, .contributions = previous_contributions, .index = &contribution_index, .token = cancellation })) orelse serial: {
        const native = @import("lake_index_native_aggregates.zig").buildIndexed(a, na, table, native_provider.source, &scoped, native_provider, cancellation, reusable, previous_contributions, &contribution_index) catch |err| return constructionFailure(err, "aggregate_build", attempt.generation);
        // Aggregate construction already completes the entire set before this
        // boundary. Publishing every growing prefix writes quadratic directory
        // bytes and metadata commits without saving any earlier build work.
        native_provider.checkpointBatch(native.declarations) catch |err| return constructionFailure(err, "aggregate_checkpoint", attempt.generation);
        const ordered = @import("lake_index_native_rows.zig").buildIncremental(a, na, table, native_provider.source, &scoped, native_provider, cancellation, reusable, native_candidates) catch |err| return constructionFailure(err, "ordered_build", attempt.generation);
        for (ordered) |declaration| native_provider.checkpoint(declaration) catch |err| return constructionFailure(err, "ordered_checkpoint", attempt.generation);
        const text = @import("lake_index_native_text.zig").buildIncremental(a, na, table, native_provider.source, base_source, &scoped, native_provider, cancellation, reusable, native_candidates) catch |err| return constructionFailure(err, "text_build", attempt.generation);
        for (text) |declaration| native_provider.checkpoint(declaration) catch |err| return constructionFailure(err, "text_checkpoint", attempt.generation);
        const sparse = @import("lake_index_native_sparse.zig").buildIncremental(a, na, table, native_provider.source, base_source, &scoped, native_provider, cancellation, reusable, native_candidates) catch |err| return constructionFailure(err, "sparse_build", attempt.generation);
        const dense = @import("lake_index_native_dense.zig").buildIncremental(a, na, table, native_provider.source, base_source, &scoped, native_provider, cancellation, reusable, native_candidates) catch |err| return constructionFailure(err, "dense_build", attempt.generation);
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
    const declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = if (relocated) current.value.published.?.declarations else assembled: {
        const result = try na.alloc(local.serverless_segment_sidecar_manifest.DeclaredArtifact, manifest.artifacts.len + native_declarations.len + ordered.len + text.len + sparse.len + dense.len + retained_declarations.len);
        @memcpy(result[0..manifest.artifacts.len], manifest.artifacts);
        @memcpy(result[manifest.artifacts.len .. manifest.artifacts.len + native_declarations.len], native_declarations);
        @memcpy(result[manifest.artifacts.len + native_declarations.len ..][0..ordered.len], ordered);
        @memcpy(result[manifest.artifacts.len + native_declarations.len + ordered.len ..][0..text.len], text);
        @memcpy(result[manifest.artifacts.len + native_declarations.len + ordered.len + text.len ..][0..sparse.len], sparse);
        @memcpy(result[manifest.artifacts.len + native_declarations.len + ordered.len + text.len + sparse.len ..][0..dense.len], dense);
        @memcpy(result[result.len - retained_declarations.len ..], retained_declarations);
        break :assembled result;
    };
    if (fragments) |value| if (row_identity) |*identity| {
        identity.native_space = value.space;
        identity.indexes = null;
        const bindings = try na.alloc(row_identity_api.ReusedIndex, declarations.len);
        var reads: u64 = 0;
        var writes: u64 = 0;
        var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &scoped, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .cancellation = cancellation, .streaming = true };
        pages.admitPublication();
        var prior_translation: ?row_identity_api.Translation = null;
        if (previous_identity) |previous| if (previous.native_space) |space| {
            for (previous.reused) |entry| if (entry.translation != null and entry.space.by_id != null and space.by_id != null and entry.space.by_id.?.eql(space.by_id.?)) {
                prior_translation = entry.translation;
                break;
            };
        };
        const translation = try @import("lake_row_translation_delta.zig").buildWithOriginal(a, &pages, value.space, identity.current, if (prior_translation != null) previous_identity.?.current else null, if (prior_translation != null) previous_identity.?.native_space else null, prior_translation, context, identity_spill_bytes);
        for (bindings, declarations) |*binding_, declaration| {
            const retained = for (identity.reused) |entry| {
                if (std.mem.eql(u8, entry.name, declaration.name)) break entry;
            } else null;
            binding_.* = retained orelse .{ .name = declaration.name, .space = @import("lake_row_fragments.zig").indexSpace(value, declaration.name), .translation = translation };
        }
        identity.reused = bindings;
    };
    navigation.ref = try costing.finish();
    progress.saved.source_navigation = navigation.ref;
    try cancellation.check();
    const verified = try coverage.pin(source, context);
    if (!std.meta.eql(pinned, verified)) return error.ExternalLakeIndexSourceChanged;
    try context.ensureActive();
    const directory: catalog.DirectoryRef = if (relocated) copied: {
        var retained = current.value.published.?.directory.?;
        retained.artifact_id = try a.dupe(u8, retained.artifact_id);
        errdefer a.free(retained.artifact_id);
        retained.checksum = try a.dupe(u8, retained.checksum);
        break :copied retained;
    } else try @import("lake_index_directory.zig").publishIndexed(a, &scoped, declarations, native.contributions, &contribution_index, cancellation);
    defer a.free(directory.artifact_id);
    defer a.free(directory.checksum);
    try context.ensureActive();
    const row_identity_ref = if (row_identity) |*identity| published: {
        identity.version = row_identity_api.metadata_version;
        identity.artifacts = try row_identity_api.declarationDigest(a, declarations);
        try identity.reseal(a);
        break :published try row_identity_api.publish(na, &scoped, identity.*, cancellation);
    } else null;
    // All uploads in this attempt are complete before its namespace evidence
    // is sealed. Certificates are advisory; failure retains the full audit.
    scoped.sealClosures(cancellation) catch {
        try context.ensureActive();
        try cancellation.check();
    };
    var completed = try clock.now_ms(clock.ptr);
    var publication: catalog.Publication = .{ .reader_protocol = attempt.reader_protocol, .store_locator = attempt.store_locator, .namespace = current.value.namespace, .generation = attempt.generation, .token = attempt.token, .signature = signature, .published_at_ms = completed, .base_source = base_source, .inventory = inventory, .source_navigation = navigation.ref, .row_identity = row_identity_ref, .directory = directory };
    if (cloud_writer) |*writer| try cloud_api.finish(na, writer, cloud_object.?, &publication, declarations, table.table_id, store_identity, context, cancellation, cloud_binding != null and cloud_object.?.cloud_certificate_until_ms != 0 and current.value.checkpoint == null);
    completed = try clock.now_ms(clock.ptr);
    publication.published_at_ms = completed;
    const latest = if (lease) |owner| try owner.snapshot(owner.ptr, a) else table;
    defer if (lease != null) a.free(latest.lake_index_catalog_json);
    var final_state = try catalog.parse(a, latest.lake_index_catalog_json);
    defer final_state.deinit();
    return catalog.encode(a, try final_state.value.publish(publication, completed));
}

/// Directory reuse covers the current desired declaration plan, not just the
/// predecessor's surviving subset. Added/removed/renamed declarations assemble
/// a fresh directory while unchanged artifacts remain individually reusable.
fn completeNativeReuse(a: A, configs_json: []const u8, desired: []const @import("lake_row_input_recipes.zig").Recipe, retained: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) !bool {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const configs = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), configs_json, .{});
    const consumers = try @import("lake_row_input_recipes.zig").consumers(arena.allocator(), desired);
    if (!@import("lake_row_input_recipes.zig").nativeFragmentsEligible(configs) or consumers.len != retained.len) return false;
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(a);
    for (retained) |declaration| try names.put(a, declaration.name, {});
    for (consumers) |recipe| if (!names.contains(recipe.name)) return false;
    return names.count() == consumers.len;
}

/// Relocation cannot keep a root whose serving/build format has advanced.
fn relocatableFormats(declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) bool {
    for (declarations) |declaration| {
        if (declaration.artifact.kind == .algebraic_segment and @import("lake_index_aggregate_artifact.zig").supportsMetadataVersion(declaration.artifact.metadata_version)) continue;
        const version: u16 = switch (declaration.artifact.kind) {
            .ordered_row_index => @import("lake_index_ordered_rows.zig").metadata_version,
            .text_segment => @import("lake_index_native_text.zig").metadata_version,
            .vector_segment => @import("lake_index_native_dense.zig").metadata_version,
            .sparse_segment => @import("lake_index_native_sparse.zig").metadata_version,
            .algebraic_segment => @import("lake_index_aggregate_artifact.zig").metadata_version,
            else => return false,
        };
        if (declaration.artifact.metadata_version != version) return false;
    }
    return true;
}

fn constructionFailure(err: anyerror, stage: []const u8, generation: u64) anyerror {
    if (err != error.Canceled and err != error.Cancelled) std.log.warn("native lake construction deferred stage={s} generation={d} err={s}", .{ stage, generation, @errorName(err) });
    return err;
}
