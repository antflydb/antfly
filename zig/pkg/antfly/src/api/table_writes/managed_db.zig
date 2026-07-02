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
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const table_write_core = @import("core.zig");
const table_write_index_config = @import("index_config.zig");

const backend_current_root_generation = table_write_core.backend_current_root_generation;
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
};

pub const TableManagedMetadata = struct {
    indexes_json: ?[]u8,
    schema_json: ?[]u8,

    pub fn deinit(self: TableManagedMetadata, alloc: std.mem.Allocator) void {
        if (self.indexes_json) |value| alloc.free(value);
        if (self.schema_json) |value| alloc.free(value);
    }
};

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
    try db.applyTableSchemaJson(alloc, schema_json, .{});
}

pub fn rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len == 0) "{}" else indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    var it = root.iterator();
    while (it.next()) |entry| {
        const index_name = entry.key_ptr.*;
        if (!std.mem.startsWith(u8, index_name, "full_text_index_v")) continue;
        if ((try parseIndexKind(entry.value_ptr.*)) != .full_text) continue;
        const current = findDbIndexStats(stats.indexes, index_name) orelse continue;
        if (current.doc_count != 0) continue;

        _ = try db.deleteIndex(index_name);
        const config_json = try extractIndexConfigJson(alloc, index_name, entry.value_ptr.*);
        defer alloc.free(config_json);
        try db.addIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = config_json,
        });
    }
}

fn findDbIndexStats(indexes: []const db_mod.types.DBIndexStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return index;
    }
    return null;
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

pub fn catchUpManagedIndexCreate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
) !void {
    if (try db.core.indexRequiresEnrichmentReplay(index_name)) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        } else {
            _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, db, index_name);
        }
    }

    try db.runUntilIdle();
    try db.catchUpPendingDerivedReplay();
    try db.runUntilIdle();

    if (try db.hasPendingDenseArtifactRebuild(alloc)) {
        _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try db.runUntilIdle();
        try db.catchUpPendingDerivedReplay();
        try db.runUntilIdle();
    }
    try db.core.index_manager.syncAll(false);
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
    return err == error.WriterLocked or err == error.ReplayDocumentNotVisible;
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
    const indexes_json = try alloc.dupe(u8, table.indexes_json);
    errdefer alloc.free(indexes_json);
    const schema_json = try alloc.dupe(u8, table.schema_json);
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
        return .{
            .table_id = table.table_id,
            .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
            .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
        };
    }
    return null;
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
    const namespace = expected orelse return;
    if (!db.core.identity_namespace.eql(namespace)) return error.DocIdentityNamespaceMismatch;
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
            error.InvalidArgument => return error.InvalidBatchRequest,
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
    var out = try alloc.alloc(db_mod.types.BatchWrite, writes.len);
    for (writes, 0..) |write, i| {
        out[i] = .{
            .key = write.key,
            .value = write.value,
        };
    }
    return out;
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
    const EnrichmentSet = struct {
        dense: ?db_embedder.DenseEmbedder = null,
        sparse: ?db_embedder.SparseEmbedder = null,
        asset_runtime: ?*asset_producer_runtime.Runtime = null,
        generated: bool = false,

        fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (self.dense) |owned| owned.deinit(allocator);
            if (self.sparse) |owned| owned.deinit(allocator);
            if (self.asset_runtime) |runtime| {
                runtime.deinit();
                allocator.destroy(runtime);
            }
        }

        fn enabled(self: @This()) bool {
            return self.dense != null or self.sparse != null or self.asset_runtime != null or self.generated;
        }

        fn config(self: @This()) db_mod.enrichment_runtime.Config {
            return .{
                .dense_embedder = self.dense,
                .sparse_embedder = self.sparse,
                .asset_producer = if (self.asset_runtime) |runtime| runtime.ownedProducer() else null,
                .enable_without_producers = self.generated,
            };
        }
    };

    const createEnrichments = struct {
        fn run(
            allocator: std.mem.Allocator,
            raw_indexes_json: []const u8,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            local_provider: ?managed_embedder.AntflyProvider,
            inference_api_url: ?[]const u8,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
        ) !EnrichmentSet {
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
            return .{
                .dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote, .inference_api_url = inference_api_url }),
                .sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .secret_store = store, .remote_content = remote, .inference_api_url = inference_api_url }),
                .asset_runtime = asset_runtime,
                .generated = try indexesJsonHasGeneratedEnrichment(allocator, raw_indexes_json),
            };
        }
    }.run;

    var enrichments = if (mode == .startup_catch_up)
        EnrichmentSet{}
    else
        try createEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, options.inference_api_url, secret_store, remote_content);
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
                    .start_optional_runtimes = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
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
                        .start_index_workers = true,
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
                        .open_mode = .writer_no_replay,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
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
        const enrichment_cfg = if (enrichments.enabled()) enrichments.config() else null;
        const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
        enrichments.dense = null;
        enrichments.sparse = null;
        enrichments.asset_runtime = null;
        break :blk opened;
    };
    var db_open = true;
    errdefer if (db_open) db.close();

    try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
    if (mode == .status_only) return db;

    const summary = try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, &db, indexes_json, .{
        .drain_resolver_backfill = options.drain_resolver_backfill,
    });
    if (summary.indexManagerCatalogChanged()) {
        db.close();
        db_open = false;
        enrichments = if (mode == .startup_catch_up)
            EnrichmentSet{}
        else
            try createEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, options.inference_api_url, secret_store, remote_content);
        db = blk: {
            const enrichment_cfg = if (enrichments.enabled()) enrichments.config() else null;
            const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
            enrichments.dense = null;
            enrichments.sparse = null;
            enrichments.asset_runtime = null;
            break :blk opened;
        };
        db_open = true;
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
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
