// Copyright 2026 Antfly, Inc.
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
const scraping = @import("antfly_scraping");

const common_secrets = @import("../../common/secrets.zig");
const metadata_api = @import("../../metadata/api.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const metadata_table_provisioner = @import("../../metadata/table_provisioner.zig");
const asset_producer_mod = @import("../../storage/db/enrichment/asset_producer.zig");
const document_extraction_mod = @import("../../storage/db/enrichment/document_extraction.zig");
const asset_producer_runtime = @import("../../asset_producer_runtime.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const db_mod = @import("../../storage/db/mod.zig");
const db_embedder = @import("../../storage/db/enrichment/embedder.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const coverage_policy_mod = @import("../coverage_policy.zig");
const indexes_api = @import("../indexes.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const table_write_core = @import("core.zig");
const table_write_index_config = @import("index_config.zig");

const backend_current_root_generation = table_write_core.backend_current_root_generation;
const StartupCatchUpMetadata = table_write_core.StartupCatchUpMetadata;
const local_schema_json_key = db_mod.local_schema_json_key;
const extractIndexConfigJson = table_write_index_config.extractIndexConfigJson;
const parseIndexKind = table_write_index_config.parseIndexKind;

pub const ManagedDbOpenMode = enum {
    default,
    default_async,
    writer_no_replay,
    startup_catch_up,
    restore_repair,
    query_readonly,
    status_only,
};

pub const ManagedDbOpenOptions = struct {
    drain_resolver_backfill: bool = true,
    inference_api_url: ?[]const u8 = null,
    ha_write_gate: ?db_mod.HAWriteGate = null,
    ha_async_effect_mirror: ?db_mod.HAAsyncEffectMirror = null,
    ha_async_batch_mirror: ?db_mod.HAAsyncBatchMirror = null,
    ha_async_metadata_mirror: ?db_mod.HAAsyncMetadataMirror = null,
    staged_generation: ?*const db_mod.generation_lifecycle.StagedGeneration = null,
    identity_validation: StartupCatchUpMetadata.IdentityValidation = .exact,
};

pub const ManagedDbEnrichmentSet = struct {
    dense: ?db_embedder.DenseEmbedder = null,
    sparse: ?db_embedder.SparseEmbedder = null,
    asset_runtime: ?*asset_producer_runtime.Runtime = null,
    generated: bool = false,

    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        if (self.dense) |owned| owned.deinit(allocator);
        if (self.sparse) |owned| owned.deinit(allocator);
        if (self.asset_runtime) |runtime| {
            runtime.deinit();
            allocator.destroy(runtime);
        }
    }

    pub fn enabled(self: @This()) bool {
        return self.dense != null or self.sparse != null or self.asset_runtime != null or self.generated;
    }

    pub fn config(self: @This()) db_mod.enrichment_runtime.Config {
        return .{
            .dense_embedder = self.dense,
            .sparse_embedder = self.sparse,
            .asset_producer = if (self.asset_runtime) |runtime| runtime.ownedProducer() else null,
            .enable_without_producers = self.generated,
        };
    }

    pub fn forgetTransferred(self: *@This()) void {
        self.dense = null;
        self.sparse = null;
        self.asset_runtime = null;
        self.generated = false;
    }

    fn takeConfig(self: *@This()) db_mod.enrichment_runtime.Config {
        const owned = self.config();
        self.forgetTransferred();
        return owned;
    }
};

pub fn createManagedDbEnrichments(
    allocator: std.mem.Allocator,
    raw_indexes_json: []const u8,
    runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_provider: ?managed_embedder.AntflyProvider,
    inference_api_url: ?[]const u8,
    store: ?*common_secrets.FileStore,
    remote: ?*const scraping.RemoteContentConfig,
) !ManagedDbEnrichmentSet {
    const asset_runtime = if (try indexesJsonNeedsAssetProducer(allocator, raw_indexes_json)) blk: {
        const io = if (runtime) |backend| backend.io() orelse return error.MissingBackendRuntimeIo else return error.MissingBackendRuntimeIo;
        break :blk try asset_producer_runtime.Runtime.createOwned(allocator, io, .{
            .antfly_provider = local_provider,
            .secret_store = store,
        });
    } else null;
    errdefer if (asset_runtime) |owned| {
        owned.deinit();
        allocator.destroy(owned);
    };
    const dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote, .inference_api_url = inference_api_url });
    errdefer if (dense) |owned| owned.deinit(allocator);
    const sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote, .inference_api_url = inference_api_url });
    errdefer if (sparse) |owned| owned.deinit(allocator);
    const generated = try indexesJsonHasGeneratedEnrichment(allocator, raw_indexes_json);
    return .{
        .dense = dense,
        .sparse = sparse,
        .asset_runtime = asset_runtime,
        .generated = generated,
    };
}

pub const TableManagedMetadata = struct {
    indexes_json: ?[]u8,
    schema_json: ?[]u8,

    pub fn deinit(self: TableManagedMetadata, alloc: std.mem.Allocator) void {
        if (self.indexes_json) |value| alloc.free(value);
        if (self.schema_json) |value| alloc.free(value);
    }
};

pub fn managedDbOpenModeDrainsResolverBackfill(mode: ManagedDbOpenMode) bool {
    // default_async is the Raft apply path. Resolver/promotion catch-up can
    // issue cross-shard writes whose completion requires future Raft applies,
    // so waiting here creates a cyclic dependency and wedges every group on
    // this apply thread. DB.open already starts the background workers that
    // drain the same backlog without blocking replicated application.
    return mode != .default_async;
}

test "managed db open modes never drain resolver backfill on raft apply" {
    try std.testing.expect(!managedDbOpenModeDrainsResolverBackfill(.default_async));
    try std.testing.expect(managedDbOpenModeDrainsResolverBackfill(.default));
    try std.testing.expect(managedDbOpenModeDrainsResolverBackfill(.writer_no_replay));
    try std.testing.expect(managedDbOpenModeDrainsResolverBackfill(.startup_catch_up));
    try std.testing.expect(managedDbOpenModeDrainsResolverBackfill(.restore_repair));
    try std.testing.expect(managedDbOpenModeDrainsResolverBackfill(.query_readonly));
    try std.testing.expect(managedDbOpenModeDrainsResolverBackfill(.status_only));
}

pub fn haMirrorForManagedDbOpenMode(mode: ManagedDbOpenMode, mirror: ?db_mod.HAAsyncEffectMirror) ?db_mod.HAAsyncEffectMirror {
    return switch (mode) {
        .default, .default_async, .writer_no_replay => mirror,
        .startup_catch_up, .restore_repair, .query_readonly, .status_only => null,
    };
}

pub fn loadLocalTableSchemaJson(alloc: std.mem.Allocator, db: *db_mod.DB) !?[]u8 {
    return try db.getSchemaJson(alloc);
}

pub fn applyLocalTableSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: []const u8,
) !void {
    if (schema_json.len == 0) return;
    const previous_schema_json = try loadLocalTableSchemaJson(alloc, db);
    defer if (previous_schema_json) |value| alloc.free(value);
    const marker_changed = if (previous_schema_json) |value|
        !std.mem.eql(u8, value, schema_json)
    else
        true;
    try db.applyTableSchemaJson(alloc, schema_json, .{
        .persist_local_schema_json = marker_changed,
    });
}

pub fn corruptEmbeddingArtifactInDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    doc_key: []const u8,
    index_name: []const u8,
) !bool {
    const dense_name = db.core.index_manager.denseEmbeddingName(index_name);
    const sparse_name = db.core.index_manager.sparseEmbeddingName(index_name);
    const candidate_names = [_]?[]const u8{
        index_name,
        dense_name,
        sparse_name,
    };
    const prefix = try db_mod.internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "embedding");
    defer alloc.free(prefix);
    const artifacts = try db.core.scanStorePrefix(alloc, prefix);
    defer db_mod.docstore.DocStore.freeResults(alloc, artifacts);

    var fallback_key: ?[]const u8 = null;
    for (artifacts) |entry| {
        if (!db_mod.internal_keys.isEmbeddingArtifactKey(entry.key)) continue;
        if (fallback_key == null) fallback_key = entry.key;
        for (candidate_names) |candidate_opt| {
            const candidate = candidate_opt orelse continue;
            if (!db_mod.internal_keys.matchesEmbeddingArtifactName(entry.key, candidate)) continue;
            try db.core.store.put(entry.key, "bad-artifact");
            return true;
        }
    }

    if (fallback_key) |artifact_key| {
        try db.core.store.put(artifact_key, "bad-artifact");
        return true;
    }

    const injection_names = [_]?[]const u8{
        dense_name,
        sparse_name,
        index_name,
    };
    for (injection_names) |candidate_opt| {
        const candidate = candidate_opt orelse continue;
        const artifact_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, candidate);
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
        return true;
    }

    return false;
}

pub const ManagedIndexCreateCatchUp = enum {
    complete,
    retry,
    delegated,
};

pub fn catchUpManagedIndexCreate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
    delegate_durable_generation_repair: bool,
) !ManagedIndexCreateCatchUp {
    if (db.core.index_manager.get(index_name)) |cfg| {
        if (cfg.kind == .graph) {
            try db.core.index_manager.syncAll(true);
            return .complete;
        }
    }

    var generation_repair = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = index_name,
        .limit = 1,
    }, .{
        .defer_durable_index_repair_execution = delegate_durable_generation_repair,
    });
    defer generation_repair.deinit(alloc);
    if (generation_repair.debt_remaining) {
        return if (delegate_durable_generation_repair) .delegated else .retry;
    }

    const requires_enrichment_replay = try db.core.indexRequiresEnrichmentReplay(index_name);
    if (requires_enrichment_replay) {
        if (db.enrichment_runtime != null) {
            _ = try db.reprocessGeneratedEnrichmentFromStoredDocs(alloc, managedIndexEmbeddingArtifactName(db, index_name));
        } else {
            _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, db, index_name);
        }
    }

    try db.runUntilIdle();

    if (try db.hasPendingDenseArtifactRebuild(alloc)) {
        _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try db.runUntilIdle();
    }
    if (requires_enrichment_replay) {
        const debt_remaining = try repairManagedEmbeddingArtifactsForIndex(alloc, db, index_name);
        if (debt_remaining) try markManagedIndexRepairRequired(alloc, db, index_name);
    }
    try drainManagedIndexReplayUntilConverged(alloc, db, index_name);
    try db.core.index_manager.syncAll(true);
    return .complete;
}

pub fn catchUpManagedIndexCreateSynchronously(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !void {
    while (true) {
        switch (try catchUpManagedIndexCreate(alloc, db, index_name, false)) {
            .complete => return,
            .retry => continue,
            .delegated => return error.ManagedIndexRepairDelegatedWithoutOwner,
        }
    }
}

test "managed structural catch-up delegates durable generation repair without rebuilding inline" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/managed-structural-repair-delegation",
        .{tmp.sub_path},
    );
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    _ = (try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqual(
        ManagedIndexCreateCatchUp.delegated,
        try catchUpManagedIndexCreate(alloc, &db, "full_text_index_v1", true),
    );

    const summary = try db.indexRepairIntentSummary(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.runnable);
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const index = for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, "full_text_index_v1")) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), index.doc_count);
}

const ManagedIndexReplayPosition = struct {
    applied_sequence: u64,
    target_sequence: u64,

    fn caughtUp(self: @This()) bool {
        return self.applied_sequence >= self.target_sequence;
    }
};

fn managedIndexReplayPosition(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !ManagedIndexReplayPosition {
    const replay_debt = try db.listDerivedReplayDebt(alloc);
    defer {
        for (replay_debt) |*status| status.deinit(alloc);
        alloc.free(replay_debt);
    }
    for (replay_debt) |status| {
        if (!std.mem.eql(u8, status.index_name, index_name)) continue;
        return .{
            .applied_sequence = status.applied_sequence,
            .target_sequence = status.target_sequence,
        };
    }
    return error.IndexNotFound;
}

fn drainManagedIndexReplayUntilConverged(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !void {
    const max_passes: usize = 16;
    var previous: ?ManagedIndexReplayPosition = null;
    var pass: usize = 0;
    while (pass < max_passes) : (pass += 1) {
        try db.runUntilIdle();
        const current = try managedIndexReplayPosition(alloc, db, index_name);
        if (current.caughtUp()) return;

        if (previous) |last| {
            if (current.applied_sequence <= last.applied_sequence and
                current.target_sequence <= last.target_sequence)
            {
                return error.ManagedIndexReplayDidNotConverge;
            }
        }
        previous = current;
        try db.catchUpPendingDerivedReplay();
    }
    return error.ManagedIndexReplayDidNotConverge;
}

fn managedIndexEmbeddingArtifactName(db: *db_mod.DB, index_name: []const u8) ?[]const u8 {
    if (db.core.index_manager.denseEmbeddingName(index_name)) |name| return name;
    if (db.core.index_manager.sparseEmbeddingName(index_name)) |name| return name;
    return index_name;
}

fn repairManagedEmbeddingArtifactsForIndex(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !bool {
    const max_passes: usize = 4;
    var pass: usize = 0;
    while (pass < max_passes) : (pass += 1) {
        var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
            .artifact_kind = .embedding,
            .index_name = index_name,
            .limit = 1024,
        });
        defer repair.deinit(alloc);

        if (repair.reprocessed == 0 and repair.repaired == 0) return repair.has_more or repair.debt_remaining;
        db.catchUpPendingDerivedReplay() catch |err| switch (err) {
            error.ArtifactRepairRequired => {},
            else => return err,
        };
        try db.runUntilIdle();
        if (!repair.has_more and !repair.debt_remaining) return false;
    }
    var remaining = try db.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = .embedding,
        .index_name = index_name,
        .limit = 1,
    });
    defer remaining.deinit(alloc);
    return remaining.issues.len != 0 or remaining.has_more;
}

fn markManagedIndexRepairRequired(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !void {
    var checkpoint = try db.core.loadProjectionCheckpoint(alloc, index_name);
    checkpoint.status = .repair_required;
    try db.core.saveProjectionCheckpoint(index_name, checkpoint);
}

fn managedIndexReplayDebtRequired(replay_debt: anytype, index_name: []const u8) bool {
    for (replay_debt) |status| {
        if (!std.mem.eql(u8, status.index_name, index_name)) continue;
        return status.catch_up_required;
    }
    return false;
}

fn managedIndexCoverageIncomplete(alloc: std.mem.Allocator, db: *db_mod.DB, index_name: []const u8) !bool {
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const primary_doc_count = try db.primaryDocCount(alloc);
    if (primary_doc_count == 0) return false;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, index_name)) continue;
        return item.doc_count < primary_doc_count;
    }
    return false;
}

pub fn seedManagedIndexReplayFromStoredDocsIfNeeded(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !bool {
    const replay_debt = try db.listDerivedReplayDebt(alloc);
    defer {
        for (replay_debt) |*status| status.deinit(alloc);
        alloc.free(replay_debt);
    }
    if (managedIndexReplayDebtRequired(replay_debt, index_name)) return false;
    if (!try managedIndexCoverageIncomplete(alloc, db, index_name)) return false;
    _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
    return true;
}

pub fn reconcileLocalTableIndexes(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
) !void {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, catalog, table_name, group_id, backend_runtime);
        db.close();
    }
}

pub fn dropLocalTableIndex(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    index_name: []const u8,
) !void {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try db_mod.DB.open(alloc, path, .{
            .backend_runtime = backend_runtime,
        });
        defer db.close();
        _ = db.deleteIndex(index_name) catch |err| switch (err) {
            error.IndexNotFound => {},
            else => return err,
        };
    }
}

pub fn replayManagedIndexForTableIfNeeded(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    var managed_visibility_changed = false;
    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, catalog, table_name, group_id, backend_runtime);
        defer db.close();
        if (!try db.core.indexRequiresEnrichmentReplay(index_name)) continue;
        managed_visibility_changed = true;

        _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, &db, index_name);

        if (try db.hasPendingDenseArtifactRebuild(alloc)) {
            _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
            try db.runUntilIdle();
        }
    }
    return managed_visibility_changed;
}

pub fn drainManagedDbBeforeClose(db: *db_mod.DB) !void {
    // Provisioned writes open a managed DB per request, so queued enrichment
    // and primary-store writes must be flushed before query DBs reopen.
    try db.runUntilIdle();
    try db.forceFlushPrimaryStoreForVisibility();
    try db.core.index_manager.syncAll(false);
}

pub fn isTransientReplayVisibilityError(err: anyerror) bool {
    return err == error.WriterLocked or
        err == error.ReplayDocumentNotVisible or
        err == error.AutoBulkIngestBusy;
}

pub fn loadTableIndexesJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.indexes_json);
}

pub fn loadTableManagedMetadata(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?TableManagedMetadata {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    const indexes_json = if (table.indexes_json.len == 0) null else try alloc.dupe(u8, table.indexes_json);
    errdefer if (indexes_json) |value| alloc.free(value);
    const schema_json = if (table.schema_json.len == 0) null else try alloc.dupe(u8, table.schema_json);
    return .{
        .indexes_json = indexes_json,
        .schema_json = schema_json,
    };
}

pub fn loadTableIdentityNamespaceForGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !?doc_identity.Namespace {
    _ = alloc;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    for (snapshot.ranges) |range| {
        if (range.table_id != table.table_id or range.group_id != group_id) continue;
        return tableIdentityNamespaceForRange(table.*, range);
    }
    return null;
}

fn tableIdentityNamespaceForRange(
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
) doc_identity.Namespace {
    return tableIdentityNamespaceForRangeId(table.table_id, range);
}

pub fn tableIdentityNamespaceForRangeId(
    table_id: u64,
    range: metadata_table_manager.RangeRecord,
) doc_identity.Namespace {
    return .{
        .table_id = table_id,
        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
        .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
    };
}

pub const RaftSnapshotCatalogContract = struct {
    metadata_group_id: u64,
    metadata_incarnation: metadata_api.MetadataClusterIncarnation,
    table_id: u64,
    table_name: []u8,
    schema_json: []u8,
    indexes_json: []u8,
    range: metadata_table_manager.RangeRecord,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.schema_json);
        alloc.free(self.indexes_json);
        metadata_table_manager.freeRange(alloc, self.range);
        self.* = undefined;
    }

    pub fn publicationContract(self: *const @This()) metadata_api.CatalogPublicationContract {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .table_id = self.table_id,
            .table_name = self.table_name,
            .schema_json = self.schema_json,
            .indexes_json = self.indexes_json,
            .range = self.range,
        };
    }
};

pub fn captureRaftSnapshotCatalogContract(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    group_id: u64,
    expected_table_id: u64,
    expected_table_name: []const u8,
) !RaftSnapshotCatalogContract {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const range = metadata_mod.findAdminRange(&snapshot, group_id) orelse return error.CatalogChanged;
    if (range.table_id != expected_table_id) return error.CatalogChanged;
    const table = metadata_mod.findAdminTable(&snapshot, expected_table_id) orelse return error.CatalogChanged;
    if (!std.mem.eql(u8, table.name, expected_table_name)) return error.CatalogChanged;
    const metadata_incarnation = snapshot.status.metadata_incarnation orelse return error.MetadataIncarnationUnavailable;

    const table_name = try alloc.dupe(u8, table.name);
    errdefer alloc.free(table_name);
    const schema_json = try alloc.dupe(u8, table.schema_json);
    errdefer alloc.free(schema_json);
    const indexes_json = try alloc.dupe(u8, table.indexes_json);
    errdefer alloc.free(indexes_json);
    const owned_range = try metadata_table_manager.cloneRange(alloc, range.*);
    return .{
        .metadata_group_id = snapshot.status.metadata_group_id,
        .metadata_incarnation = metadata_incarnation,
        .table_id = expected_table_id,
        .table_name = table_name,
        .schema_json = schema_json,
        .indexes_json = indexes_json,
        .range = owned_range,
    };
}

pub fn findTableRecord(tables: []const metadata_table_manager.TableRecord, table_id: u64) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

pub fn findRangeRecord(ranges: []const metadata_table_manager.RangeRecord, group_id: u64) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.group_id == group_id) return range;
    }
    return null;
}

pub fn validateProvisionedDbIdentityNamespaceExpected(expected: ?doc_identity.Namespace, db: *const db_mod.DB) !void {
    return validateProvisionedDbIdentityNamespaceWithPolicy(expected, .exact, db);
}

pub fn validateProvisionedDbIdentityNamespaceWithPolicy(
    expected: ?doc_identity.Namespace,
    validation: StartupCatchUpMetadata.IdentityValidation,
    db: *const db_mod.DB,
) !void {
    const namespace = expected orelse return;
    const valid = switch (validation) {
        .exact => db.core.identity_namespace.eql(namespace),
        .reassign_same_table => db.core.identity_namespace.table_id ==
            namespace.table_id,
    };
    if (!valid) return error.DocIdentityNamespaceMismatch;
}

pub fn validateSplitReplicationForApply(req: db_mod.types.BatchRequest, group_id: u64) !?doc_identity.Namespace {
    const replication = req.split_replication orelse {
        if (req.split_checkpoint) |checkpoint| {
            if (checkpoint.kind != .source_ack) return error.MissingSplitReplicationContext;
        }
        return null;
    };
    if (replication.transition_id == 0 or replication.attempt_epoch == 0 or
        replication.source_group_id == replication.destination_group_id or
        replication.destination_group_id != group_id or
        replication.identity_namespace.table_id == 0 or
        replication.identity_namespace.shard_id == 0 or
        replication.identity_namespace.range_id == 0 or
        req.split_transition != null)
    {
        return error.InvalidBatchRequest;
    }
    if (req.split_checkpoint) |checkpoint| {
        if (checkpoint.kind == .source_ack or
            replication.operation != .checkpoint or
            checkpoint.transition_id != replication.transition_id or
            checkpoint.attempt_epoch != replication.attempt_epoch or
            checkpoint.source_group_id != replication.source_group_id or
            checkpoint.destination_group_id != replication.destination_group_id or
            checkpoint.delta_sequence != replication.sequence)
        {
            return error.InvalidBatchRequest;
        }
        if (req.deletes.len != 0 or req.transforms.len != 0 or req.graph_writes.len != 0 or
            req.graph_deletes.len != 0 or req.predicates.len != 0 or req.writes.len != 0)
        {
            return error.InvalidBatchRequest;
        }
    } else if (replication.operation == .checkpoint) {
        return error.MissingSplitReplicationCheckpoint;
    }
    return replication.identity_namespace;
}

pub fn validateSplitCheckpointGroup(checkpoint: ?db_mod.types.SplitReplicationCheckpoint, group_id: u64) !void {
    const value = checkpoint orelse return;
    if (value.transition_id == 0) return error.InvalidBatchRequest;
    switch (value.kind) {
        .destination_begin, .destination_complete => if (value.destination_group_id != group_id) return error.InvalidBatchRequest,
        .source_ack => if (value.source_group_id != group_id) return error.InvalidBatchRequest,
    }
}

pub fn validateSplitReplicationIdentityAgainstCatalog(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    replication: db_mod.types.SplitReplicationContext,
) !void {
    const source_namespace = (try loadTableIdentityNamespaceForGroup(
        alloc,
        catalog,
        table_name,
        replication.source_group_id,
    )) orelse return error.MissingIdentityNamespace;
    if (!source_namespace.eql(replication.identity_namespace)) return error.DocIdentityNamespaceMismatch;
}

pub fn openManagedDbForReplicatedApply(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    ha_write_gate: ?db_mod.HAWriteGate,
    ha_async_mirror: ?db_mod.HAAsyncEffectMirror,
    split_identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    const namespace = split_identity_namespace orelse
        return try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(
            alloc,
            path,
            catalog,
            table_name,
            group_id,
            backend_runtime,
            ha_write_gate,
            ha_async_mirror,
        );
    const indexes_json = try loadTableIndexesJson(alloc, catalog, table_name);
    defer if (indexes_json) |value| alloc.free(value);
    const effective_ha_mirror = haMirrorForManagedDbOpenMode(.default_async, ha_async_mirror);
    var db = if (indexes_json) |value|
        try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
            alloc,
            path,
            value,
            null,
            null,
            backend_current_root_generation,
            null,
            .default_async,
            backend_runtime,
            null,
            null,
            null,
            namespace,
            .{
                .drain_resolver_backfill = false,
                .ha_write_gate = ha_write_gate,
                .ha_async_effect_mirror = effective_ha_mirror,
                .ha_async_batch_mirror = effective_ha_mirror,
                .ha_async_metadata_mirror = effective_ha_mirror,
            },
        )
    else
        try db_mod.DB.open(alloc, path, .{
            .backend_runtime = backend_runtime,
            .identity_namespace = namespace,
            .prefer_existing_identity_namespace = true,
            .ha_write_gate = ha_write_gate,
            .ha_async_effect_mirror = effective_ha_mirror,
            .ha_async_batch_mirror = effective_ha_mirror,
            .ha_async_metadata_mirror = effective_ha_mirror,
            .open_mode = .writer_no_replay,
            .index_open_parallelism = 1,
        });
    errdefer db.close();
    try validateProvisionedDbIdentityNamespaceExpected(namespace, &db);
    return db;
}

pub fn validateProvisionedDbIdentityNamespace(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    db: *const db_mod.DB,
) !void {
    const expected = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    try validateProvisionedDbIdentityNamespaceExpected(expected, db);
}

pub fn loadTableSchemaJson(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    return try alloc.dupe(u8, table.schema_json);
}

pub fn validateTableWritesAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: anytype,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try enforceNativeDocumentWritesPreflightForSchema(parsed_schema, writes);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, writes);
}

fn freeOwnedBatchWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(@constCast(writes));
}

const SchemaValidationWriteState = struct {
    const Kind = enum { write, delete };

    const Entry = struct {
        key: []u8,
        kind: Kind,
        value: ?[]u8 = null,

        fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            alloc.free(self.key);
            if (self.value) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    entries: std.ArrayListUnmanaged(Entry) = .empty,

    fn deinit(self: *SchemaValidationWriteState, alloc: std.mem.Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(alloc);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    fn findIndex(self: *const SchemaValidationWriteState, key: []const u8) ?usize {
        for (self.entries.items, 0..) |entry, i| {
            if (std.mem.eql(u8, entry.key, key)) return i;
        }
        return null;
    }

    fn applyBorrowedWrite(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        const owned_value = try alloc.dupe(u8, value);
        errdefer alloc.free(owned_value);
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .write;
            entry.value = owned_value;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .write,
            .value = owned_value,
        });
    }

    fn applyDelete(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8) !void {
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .delete;
            entry.value = null;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .delete,
        });
    }

    fn applyOwnedWrite(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8, value: []u8) !void {
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .write;
            entry.value = value;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .write,
            .value = value,
        });
    }

    fn baseValue(self: *const SchemaValidationWriteState, key: []const u8) ?[]const u8 {
        const idx = self.findIndex(key) orelse return null;
        const entry = self.entries.items[idx];
        return switch (entry.kind) {
            .write => entry.value.?,
            .delete => null,
        };
    }

    fn hasRequestState(self: *const SchemaValidationWriteState, key: []const u8) bool {
        return self.findIndex(key) != null;
    }

    fn toOwnedWrites(self: *const SchemaValidationWriteState, alloc: std.mem.Allocator) ![]db_mod.types.BatchWrite {
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.kind == .write) count += 1;
        }

        var out = try alloc.alloc(db_mod.types.BatchWrite, count);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            alloc.free(out);
        }

        var i: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.kind != .write) continue;
            {
                const owned_key = try alloc.dupe(u8, entry.key);
                errdefer alloc.free(owned_key);
                const owned_value = try alloc.dupe(u8, entry.value.?);
                errdefer alloc.free(owned_value);
                out[i] = .{
                    .key = owned_key,
                    .value = owned_value,
                };
            }
            filled += 1;
            i += 1;
        }
        return out;
    }
};

fn resolveWritesForSchemaValidation(
    alloc: std.mem.Allocator,
    schema: anytype,
    db: *db_mod.DB,
    base_writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) ![]db_mod.types.BatchWrite {
    var state = SchemaValidationWriteState{};
    defer state.deinit(alloc);

    for (base_writes) |write| {
        try state.applyBorrowedWrite(alloc, write.key, write.value);
    }

    for (deletes) |key| {
        try state.applyDelete(alloc, key);
    }

    for (transforms) |transform| {
        const has_request_state = state.hasRequestState(transform.key);
        const existing_from_request = state.baseValue(transform.key);
        const existing_from_db = if (!has_request_state) try db.get(alloc, transform.key) else null;
        defer if (existing_from_db) |body| alloc.free(body);
        const existing = existing_from_request orelse existing_from_db;
        const resolved = db_mod.transform.resolveDocumentTransform(alloc, existing, transform) catch |err| switch (err) {
            error.InvalidArgument, error.UnsupportedTransformOperation => return error.InvalidBatchRequest,
            else => return err,
        } orelse continue;
        var resolved_owned = true;
        errdefer if (resolved_owned) alloc.free(resolved);

        const resolved_for_validation = if (!has_request_state) blk: {
            const stripped = stripGeneratedDocumentFieldsForValidationAlloc(alloc, schema, resolved) catch |err| {
                return err;
            };
            resolved_owned = false;
            alloc.free(resolved);
            break :blk stripped;
        } else resolved;
        var validation_owned = true;
        if (has_request_state) resolved_owned = false;
        errdefer if (validation_owned) alloc.free(resolved_for_validation);

        try state.applyOwnedWrite(alloc, transform.key, resolved_for_validation);
        validation_owned = false;
        resolved_owned = false;
    }

    return try state.toOwnedWrites(alloc);
}

fn documentSchemaHasGeneratedField(schema: anytype, field_name: []const u8) bool {
    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            if (property.generated == null) continue;
            if (std.mem.eql(u8, property.name, field_name)) return true;
        }
    }
    return false;
}

fn transformPathTopLevelField(path: []const u8) []const u8 {
    if (std.mem.indexOfAny(u8, path, "./")) |idx| return path[0..idx];
    return path;
}

fn rejectGeneratedDocumentTransformTargets(schema: anytype, transforms: []const db_mod.types.DocumentTransform) !void {
    if (schema.storage_mode != .document) return;
    for (transforms) |transform| {
        for (transform.operations) |operation| {
            if (documentSchemaHasGeneratedField(schema, transformPathTopLevelField(operation.path))) return error.InvalidBatchRequest;
        }
    }
}

fn stripGeneratedDocumentFieldsForValidationAlloc(
    alloc: std.mem.Allocator,
    schema: anytype,
    value_json: []const u8,
) ![]u8 {
    if (schema.storage_mode != .document) return try alloc.dupe(u8, value_json);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{ .allocate = .alloc_always }) catch return error.InvalidBatchRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBatchRequest;

    var removed = false;
    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            if (property.generated == null) continue;
            if (parsed.value.object.fetchSwapRemove(property.name)) |_| removed = true;
        }
    }
    if (!removed) return try alloc.dupe(u8, value_json);
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn transactionWritesToBatchWrites(
    alloc: std.mem.Allocator,
    writes: []const db_mod.types.TransactionWrite,
) ![]db_mod.types.BatchWrite {
    var out = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    errdefer out.deinit(alloc);
    for (writes) |write| {
        if (db_mod.internal_keys.isInternalPhysicalTableDataKey(write.key)) continue;
        try out.append(alloc, .{
            .key = write.key,
            .value = write.value,
        });
    }
    return try out.toOwnedSlice(alloc);
}

pub fn validateTableBatchAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try enforceNativeDocumentBatchPreflightForSchema(parsed_schema, .{
        .writes = writes,
        .deletes = deletes,
        .transforms = transforms,
    });
    try rejectGeneratedDocumentTransformTargets(parsed_schema, transforms);

    const effective_writes = try resolveWritesForSchemaValidation(alloc, parsed_schema, db, writes, deletes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

pub fn validateTransactionAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstLocalSchema(alloc, db, batch_writes, deletes, transforms);
}

pub fn validateTableWritesAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    writes: anytype,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try loadTableSchemaJson(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try enforceNativeDocumentWritesPreflightForSchema(parsed_schema, writes);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, writes);
}

pub fn validateTableBatchAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const schema_json = (try loadTableSchemaJson(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    try validateTableBatchAgainstSchemaJson(alloc, db, schema_json, writes, deletes, transforms);
}

pub fn validateTableBatchAgainstSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: ?[]const u8,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const raw_schema_json = schema_json orelse return;
    if (raw_schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, raw_schema_json);
    defer parsed_schema.deinit(alloc);
    try enforceNativeDocumentBatchPreflightForSchema(parsed_schema, .{
        .writes = writes,
        .deletes = deletes,
        .transforms = transforms,
    });
    try rejectGeneratedDocumentTransformTargets(parsed_schema, transforms);

    const effective_writes = try resolveWritesForSchemaValidation(alloc, parsed_schema, db, writes, deletes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

fn enforceNativeDocumentWritesPreflightForSchema(schema: anytype, writes: anytype) !void {
    const empty_deletes = [_][]const u8{};
    const empty_transforms = [_]db_mod.types.DocumentTransform{};
    try enforceNativeDocumentBatchPreflightForSchema(schema, .{
        .writes = writes,
        .deletes = empty_deletes[0..],
        .transforms = empty_transforms[0..],
    });
}

fn enforceNativeDocumentBatchPreflightForSchema(schema: anytype, req: anytype) !void {
    if (schema.storage_mode != .document) return;
    try db_mod.document_write.enforceNativeDocumentBatchPreflight(req);
}

test "managed table batch validation preflights native document writes before schema validation" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-document-write-preflight", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const Hook = struct {
        operations: [3]db_mod.document_write.DocumentWriteOperation = undefined,
        len: usize = 0,

        fn run(ptr: *anyopaque, req: db_mod.document_write.DocumentWritePreflight) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.operations[self.len] = req.operation;
            self.len += 1;
        }
    };

    var hook = Hook{};
    db_mod.document_write.test_preflight_hook = .{ .ptr = &hook, .run = Hook.run };
    defer db_mod.document_write.test_preflight_hook = null;

    const schema_json =
        \\{"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}}}}}}
    ;
    const writes = [_]db_mod.types.BatchWrite{.{
        .key = "doc:a",
        .value = "{\"title\":\"alpha\"}",
    }};
    const deletes = [_][]const u8{"doc:b"};
    const transform_ops = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "status",
        .value_json = "\"updated\"",
    }};
    const transforms = [_]db_mod.types.DocumentTransform{.{
        .key = "doc:a",
        .operations = transform_ops[0..],
        .upsert = true,
    }};

    try validateTableBatchAgainstSchemaJson(alloc, &db, schema_json, writes[0..], deletes[0..], transforms[0..]);

    try std.testing.expectEqual(@as(usize, 3), hook.len);
    try std.testing.expectEqual(db_mod.document_write.DocumentWriteOperation.full_document_insert, hook.operations[0]);
    try std.testing.expectEqual(db_mod.document_write.DocumentWriteOperation.exact_id_delete, hook.operations[1]);
    try std.testing.expectEqual(db_mod.document_write.DocumentWriteOperation.document_patch, hook.operations[2]);
}

pub fn validateTransactionAgainstCatalogSchema(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstCatalogSchema(alloc, catalog, db, table_name, batch_writes, deletes, transforms);
}

pub fn openManagedDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !db_mod.DB {
    return try openManagedDbForTableWithRuntime(alloc, path, catalog, table_name, null);
}

pub fn openManagedDbForTableWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbForTableWithCacheAndRuntime(alloc, path, catalog, table_name, null, null, backend_current_root_generation, null, backend_runtime);
}

pub fn openManagedDbForTableGroupWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, catalog, table_name, group_id, backend_runtime, null, null);
}

pub fn openManagedDbForTableGroupWithRuntimeAndHAWriteGate(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    ha_write_gate: ?db_mod.HAWriteGate,
    ha_async_mirror: ?db_mod.HAAsyncEffectMirror,
) !db_mod.DB {
    return try openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate(alloc, path, catalog, table_name, group_id, null, null, backend_current_root_generation, null, backend_runtime, ha_write_gate, ha_async_mirror);
}

pub fn openManagedDbForTableWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbForTableWithCacheAndRuntime(alloc, path, catalog, table_name, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, null);
}

pub fn openManagedDbForTableWithIndexesJson(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
) !db_mod.DB {
    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, null, null, backend_current_root_generation, null, null);
}

pub fn openManagedDbForTableWithCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse return try db_mod.DB.open(alloc, path, .{
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
    });
    defer alloc.free(indexes_json);

    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, backend_runtime);
}

pub fn openManagedDbForTableGroupWithCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate(alloc, path, catalog, table_name, group_id, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, backend_runtime, null, null);
}

pub fn openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    ha_write_gate: ?db_mod.HAWriteGate,
    ha_async_mirror: ?db_mod.HAAsyncEffectMirror,
) !db_mod.DB {
    const effective_ha_mirror = haMirrorForManagedDbOpenMode(.default, ha_async_mirror);
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const metadata = try loadTableManagedMetadata(alloc, catalog, table_name);
    defer if (metadata) |managed| {
        if (managed.indexes_json) |value| alloc.free(value);
        if (managed.schema_json) |value| alloc.free(value);
    };
    const indexes_json = if (metadata) |managed| managed.indexes_json else null;
    const schema_json = if (metadata) |managed| managed.schema_json else null;
    if (indexes_json == null) {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
            .ha_write_gate = ha_write_gate,
            .ha_async_effect_mirror = effective_ha_mirror,
            .ha_async_batch_mirror = effective_ha_mirror,
            .ha_async_metadata_mirror = effective_ha_mirror,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        if (schema_json) |value| {
            if (value.len > 0) try applyLocalTableSchemaJson(alloc, &db, value);
        }
        return db;
    }

    var db = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
        alloc,
        path,
        indexes_json.?,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .default,
        backend_runtime,
        null,
        null,
        null,
        identity_namespace,
        .{
            .ha_write_gate = ha_write_gate,
            .ha_async_effect_mirror = effective_ha_mirror,
            .ha_async_batch_mirror = effective_ha_mirror,
            .ha_async_metadata_mirror = effective_ha_mirror,
        },
    );
    errdefer db.close();
    if (schema_json) |value| {
        if (value.len > 0) try applyLocalTableSchemaJson(alloc, &db, value);
    }
    return db;
}

pub fn openManagedDbForTableWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, null);
}

pub fn openManagedDbForTableWithIndexesJsonAndCacheAndRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: ?[]const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const raw_indexes_json = indexes_json orelse return try db_mod.DB.open(alloc, path, .{
        .lsm_cache = lsm_cache,
        .hbc_cache = hbc_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
    });
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(alloc, path, raw_indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, .default, backend_runtime);
}

pub fn openManagedDbForStatusWithCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    };
    defer alloc.free(indexes_json);

    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .status_only,
        backend_runtime,
        identity_namespace,
    );
}

pub fn openManagedDbForStatusWithCacheMode(
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    mode: ManagedDbOpenMode,
) !db_mod.DB {
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse {
        var db = try db_mod.DB.open(alloc, path, .{
            .lsm_cache = lsm_cache,
            .hbc_cache = hbc_cache,
            .lsm_root_generation = lsm_root_generation,
            .resource_manager = resource_manager,
            .backend_runtime = backend_runtime,
            .open_mode = switch (mode) {
                .query_readonly => .query_readonly,
                else => .status_only,
            },
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = identity_namespace != null,
        });
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    };
    defer alloc.free(indexes_json);

    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        identity_namespace,
    );
}

pub fn openManagedDbForStatusWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .status_only,
        backend_runtime,
    );
}

pub fn openManagedDbWithIndexesJsonAndCache(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        .default_async,
    );
}

pub fn openManagedDbWithIndexesJson(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCache(alloc, path, indexes_json, null, null, backend_current_root_generation, null);
}

pub fn openManagedDbWithIndexesJsonAndCacheMode(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        null,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        null,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        null,
        null,
        null,
        identity_namespace,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntfly(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        antfly_provider,
        secret_store,
        remote_content,
        null,
    );
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?doc_identity.Namespace,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
        alloc,
        path,
        indexes_json,
        lsm_cache,
        hbc_cache,
        lsm_root_generation,
        resource_manager,
        mode,
        backend_runtime,
        antfly_provider,
        secret_store,
        remote_content,
        identity_namespace,
        .{},
    );
}

pub fn reconfigureManagedDbEnrichments(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    inference_api_url: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !void {
    var enrichments = try createManagedDbEnrichments(
        db.runtime_alloc,
        indexes_json,
        backend_runtime,
        antfly_provider,
        inference_api_url,
        secret_store,
        remote_content,
    );
    errdefer enrichments.deinit(db.runtime_alloc);
    // An empty replacement is meaningful: dropping the last managed producer
    // must retire the old provider instead of leaving an unused runtime alive.
    try db.reconfigureEnrichmentRuntime(enrichments.takeConfig());
    try reconcileDbArtifactEnrichmentsFromIndexesJson(alloc, db, indexes_json);
}

pub fn applyIndexCreateToCachedDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    index_name: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    inference_api_url: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !void {
    var lookup = (try indexes_api.lookupSingleIndexConfig(alloc, indexes_json, index_name)) orelse return error.InvalidTableIndexMetadata;
    defer lookup.deinit();

    const kind = try parseIndexKind(lookup.config);
    const config_json = try extractIndexConfigJson(alloc, index_name, lookup.config);
    defer alloc.free(config_json);

    const owned_name = try alloc.dupe(u8, index_name);
    defer alloc.free(owned_name);
    try reconfigureManagedDbEnrichments(
        alloc,
        db,
        indexes_json,
        backend_runtime,
        antfly_provider,
        inference_api_url,
        secret_store,
        remote_content,
    );
    db.addIndex(.{
        .name = owned_name,
        .kind = kind,
        .config_json = config_json,
        .coverage_generation = coverage_policy_mod.incarnation(lookup.config) orelse 0,
    }) catch |err| switch (err) {
        error.IndexAlreadyExists => {},
        else => return err,
    };
}

fn reconcileDbArtifactEnrichmentsFromIndexesJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !void {
    const enrichments = try indexes_api.collectArtifactEnrichmentsFromTableIndexesJson(alloc, indexes_json);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
    indexes_api.sortArtifactEnrichmentsByDependency(enrichments);
    for (enrichments) |cfg| {
        _ = try db.upsertEnrichment(cfg);
    }
}

pub fn existingPrimaryBackend() @TypeOf((db_mod.OpenOptions{}).primary_backend) {
    return switch ((db_mod.OpenOptions{}).primary_backend) {
        .lsm => |default_options| blk: {
            var options = default_options;
            options.backend.create_if_missing = false;
            break :blk .{ .lsm = options };
        },
        else => unreachable,
    };
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?doc_identity.Namespace,
    options: ManagedDbOpenOptions,
) !db_mod.DB {
    const mode_consumes_enrichments = switch (mode) {
        .default, .default_async, .writer_no_replay, .startup_catch_up, .restore_repair => true,
        .query_readonly, .status_only => false,
    };

    var enrichments = if (mode_consumes_enrichments)
        try createManagedDbEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, options.inference_api_url, secret_store, remote_content)
    else
        ManagedDbEnrichmentSet{};
    errdefer enrichments.deinit(alloc);

    const openDb = struct {
        fn run(
            allocator: std.mem.Allocator,
            db_path: []const u8,
            enrichment_cfg: ?db_mod.enrichment_runtime.Config,
            cache: ?*lsm_backend.Cache,
            vector_cache: ?*hbc_mod.Cache,
            root_generation: u64,
            manager: ?*resource_manager_mod.ResourceManager,
            open_mode: ManagedDbOpenMode,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
            namespace: ?doc_identity.Namespace,
            open_options: ManagedDbOpenOptions,
        ) !db_mod.DB {
            const base: db_mod.OpenOptions = .{
                .lsm_cache = cache,
                .hbc_cache = vector_cache,
                .lsm_root_generation = root_generation,
                .resource_manager = manager,
                .backend_runtime = runtime,
                .secret_store = store,
                .remote_content = remote,
                .identity_namespace = namespace,
                .prefer_existing_identity_namespace = namespace != null,
                .enrichment = enrichment_cfg,
                .ha_write_gate = open_options.ha_write_gate,
                .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
            };
            return switch (open_mode) {
                .default => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, base)
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                    }),
                .default_async, .writer_no_replay => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .open_mode = .writer_no_replay,
                        .index_open_parallelism = 1,
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .open_mode = .writer_no_replay,
                        .index_open_parallelism = 1,
                    }),
                .startup_catch_up => try db_mod.DB.open(allocator, db_path, .{
                    .lsm_cache = cache,
                    .hbc_cache = vector_cache,
                    .lsm_root_generation = root_generation,
                    .resource_manager = manager,
                    .backend_runtime = runtime,
                    .secret_store = store,
                    .remote_content = remote,
                    .identity_namespace = namespace,
                    .prefer_existing_identity_namespace = namespace != null,
                    .ha_write_gate = open_options.ha_write_gate,
                    .open_mode = .writer_no_replay,
                    .start_index_workers = false,
                    .enrichment = if (enrichment_cfg) |configured| blk: {
                        var bounded = configured;
                        bounded.inline_retry_max_attempts = 1;
                        break :blk bounded;
                    } else null,
                    .start_optional_runtimes = enrichment_cfg != null,
                    .start_optional_runtime_workers = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
                    .staged_generation = open_options.staged_generation,
                }),
                .restore_repair => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = false,
                        .start_optional_runtime_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                        .staged_generation = open_options.staged_generation,
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = false,
                        .start_optional_runtimes = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                        .staged_generation = open_options.staged_generation,
                    }),
                .query_readonly => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .query_readonly,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .query_readonly,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
                .status_only => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
            };
        }
    }.run;

    var db = blk: {
        const enrichment_cfg = if (enrichments.enabled()) enrichments.takeConfig() else null;
        const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
        break :blk opened;
    };
    var db_open = true;
    errdefer if (db_open) db.close();

    try validateProvisionedDbIdentityNamespaceWithPolicy(
        identity_namespace,
        options.identity_validation,
        &db,
    );
    if (mode == .status_only or mode == .query_readonly) return db;

    if ((mode == .startup_catch_up or mode == .restore_repair) and
        db.core.index_manager.hasLoadFailures())
    {
        return db;
    }

    const summary = try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, &db, indexes_json, .{
        .drain_resolver_backfill = options.drain_resolver_backfill,
    });
    if (summary.indexManagerCatalogChanged()) {
        db.close();
        db_open = false;
        enrichments = if (mode_consumes_enrichments)
            try createManagedDbEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, options.inference_api_url, secret_store, remote_content)
        else
            ManagedDbEnrichmentSet{};
        db = blk: {
            const enrichment_cfg = if (enrichments.enabled()) enrichments.takeConfig() else null;
            const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
            break :blk opened;
        };
        db_open = true;
        try validateProvisionedDbIdentityNamespaceWithPolicy(
            identity_namespace,
            options.identity_validation,
            &db,
        );
    }

    if ((mode == .default or mode == .default_async) and summary.indexes_added > 0) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        }
    }
    return db;
}

pub fn indexesJsonNeedsAssetProducer(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return try jsonValueNeedsAssetProducer(alloc, parsed.value);
}

pub fn indexesJsonHasGeneratedEnrichment(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return try jsonValueHasGeneratedEnrichment(alloc, parsed.value);
}

fn jsonValueHasGeneratedEnrichment(alloc: std.mem.Allocator, value: std.json.Value) anyerror!bool {
    switch (value) {
        .object => |object| {
            if (object.get("kind")) |kind| {
                if (kind == .string and (std.mem.eql(u8, kind.string, "asset") or std.mem.eql(u8, kind.string, "chunk"))) return true;
            }
            if (object.get("generator") != null or object.get("chunker") != null) return true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (try jsonValueHasGeneratedEnrichment(alloc, entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try jsonValueHasGeneratedEnrichment(alloc, item)) return true;
            }
            return false;
        },
        .string => |raw| {
            return try jsonStringHasGeneratedEnrichment(alloc, raw);
        },
        else => return false,
    }
}

fn jsonStringHasGeneratedEnrichment(alloc: std.mem.Allocator, raw: []const u8) anyerror!bool {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!jsonStringLooksStructured(trimmed)) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch return false;
    defer parsed.deinit();
    return try jsonValueHasGeneratedEnrichment(alloc, parsed.value);
}

fn jsonValueNeedsAssetProducer(alloc: std.mem.Allocator, value: std.json.Value) anyerror!bool {
    switch (value) {
        .object => |object| {
            if (try objectIsModelBackedAssetEnrichment(alloc, object)) return true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (try jsonValueNeedsAssetProducer(alloc, entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try jsonValueNeedsAssetProducer(alloc, item)) return true;
            }
            return false;
        },
        .string => |raw| {
            return try jsonStringNeedsAssetProducer(alloc, raw);
        },
        else => return false,
    }
}

fn jsonStringNeedsAssetProducer(alloc: std.mem.Allocator, raw: []const u8) anyerror!bool {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!jsonStringLooksStructured(trimmed)) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch return false;
    defer parsed.deinit();
    return try jsonValueNeedsAssetProducer(alloc, parsed.value);
}

fn jsonStringLooksStructured(trimmed: []const u8) bool {
    return trimmed.len >= 2 and
        ((trimmed[0] == '{' and trimmed[trimmed.len - 1] == '}') or
            (trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']'));
}

fn objectIsModelBackedAssetEnrichment(alloc: std.mem.Allocator, object: std.json.ObjectMap) !bool {
    const kind = object.get("kind") orelse return false;
    if (kind != .string or !std.mem.eql(u8, kind.string, "asset")) return false;
    const producer_value = object.get("producer_json") orelse return false;
    const producer_json = switch (producer_value) {
        .string => |raw| raw,
        .object, .array => try std.json.Stringify.valueAlloc(alloc, producer_value, .{}),
        else => return false,
    };
    const owns_producer_json = producer_value != .string;
    defer if (owns_producer_json) alloc.free(@constCast(producer_json));
    var producer_cfg = asset_producer_mod.parseProducerConfig(alloc, producer_json) catch return false;
    defer producer_cfg.deinit(alloc);
    return switch (producer_cfg.type) {
        .copy => false,
        .document_extraction => blk: {
            var config = document_extraction_mod.parseConfig(alloc, producer_cfg.config_json) catch return false;
            defer config.deinit(alloc);
            break :blk config.ocr_enabled or config.transcription_enabled;
        },
        .generator, .reader, .transcriber, .extractor => true,
    };
}

test "provisioning detects model backed graph shorthand assets inside config_json strings" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try indexesJsonNeedsAssetProducer(alloc,
        \\[{
        \\  "name":"relations_graph",
        \\  "kind":"graph",
        \\  "config_json":"{\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"body\",\"producer_json\":{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\"}}}}"
        \\}]
    ));
}

test "provisioning does not require asset producer for copy graph shorthand assets inside config_json strings" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!(try indexesJsonNeedsAssetProducer(alloc,
        \\[{
        \\  "name":"relations_graph",
        \\  "kind":"graph",
        \\  "config_json":"{\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"relations\"}}"
        \\}]
    )));
    try std.testing.expect(try indexesJsonHasGeneratedEnrichment(alloc,
        \\[{
        \\  "name":"relations_graph",
        \\  "kind":"graph",
        \\  "config_json":"{\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"field\":\"relations\"}}"
        \\}]
    ));
}

test "provisioning detects generated embedding chunkers inside index metadata" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try indexesJsonHasGeneratedEnrichment(alloc,
        \\{"semantic_chunked_idx":{"type":"embeddings","field":"body","dimension":3,"chunker":{"provider":"antfly","model":"fixed-bert-tokenizer","store_chunks":false,"full_text_index":{},"text":{"target_tokens":4,"overlap_tokens":1,"separator":" "}}}}
    ));
    try std.testing.expect(try indexesJsonHasGeneratedEnrichment(alloc,
        \\[{
        \\  "name":"semantic_chunked_idx",
        \\  "type":"embeddings",
        \\  "config_json":"{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunker\":{\"provider\":\"antfly\",\"model\":\"fixed-bert-tokenizer\",\"store_chunks\":false}}}"
        \\}]
    ));
}

test "managed db full_index materializes graph shorthand document extraction assets" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-graph-document-artifact/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var backend_runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer backend_runtime.deinit();

    const indexes_json =
        \\{
        \\  "document_units_graph":{
        \\    "type":"graph",
        \\    "source":{"kind":"artifact","artifact":"document_units_v1","path":"$.edges[*]","format":"extraction_relation"},
        \\    "artifact":{
        \\      "name":"document_units_v1",
        \\      "kind":"asset",
        \\      "field":"url",
        \\      "content_type":"application/json",
        \\      "producer_json":{"type":"document_extraction","config":{"source":{"filename_field":"filename","content_type_field":"mime_type","version_field":"version"}}}
        \\    },
        \\    "edge_types":[{"name":"mentions"}]
        \\  }
        \\}
    ;
    var db = try openManagedDbWithIndexesJsonAndCacheModeWithRuntime(
        alloc,
        path,
        indexes_json,
        null,
        null,
        0,
        null,
        .default,
        backend_runtime.ptr(),
    );
    defer db.close();

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a/with/slash",
            .value = "{\"filename\":\"alpha.txt\",\"mime_type\":\"text/plain\",\"version\":\"1\",\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
    });

    var manifest = (try db.getDocumentArtifactManifest(alloc, "doc:a/with/slash", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer manifest.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a/with/slash", manifest.document_id);
    try std.testing.expectEqualStrings("document_units_v1", manifest.artifact_name);
    try std.testing.expectEqual(@as(u32, 1), manifest.unit_count);
    try std.testing.expectEqualStrings("converged", manifest.merge_status);
}

test "managed startup catch-up open disables optional runtimes and workers" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        null,
        null,
        0,
        null,
        .startup_catch_up,
    );
    defer db.close();

    try std.testing.expect(!db.start_index_workers);
    try std.testing.expect(db.enrichment_runtime == null);
    try std.testing.expect(db.resolution_runtime == null);
    try std.testing.expect(db.promotion_runtime == null);
    try std.testing.expect(db.ttl_runtime == null);
    try std.testing.expect(db.transaction_runtime == null);
    try std.testing.expect(db.text_merge_runtime == null);
}
