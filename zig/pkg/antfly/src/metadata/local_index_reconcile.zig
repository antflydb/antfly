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

pub const std = @import("std");
pub const stored_destination_authorization = @import("../api/stored_destination_authorization.zig");
pub const db_mod = @import("../storage/db/selected_root.zig").db;
pub const internal_keys = @import("../storage/internal_keys.zig");
pub const managed_embedder = @import("../inference/managed_embedder.zig");
pub const coverage_policy = @import("../api/coverage_policy.zig");
pub const table_index_config = @import("../api/table_index_config.zig");
pub const indexes_api = @import("../api/local_indexes.zig");
pub const enrichment_config_validation = @import("../storage/db/enrichment/config_validation.zig");
pub const ProvisionSummary = @import("antfly_provision_contract").ProvisionSummary;

pub const ReconcileDbIndexOptions = struct {
    /// Hidden restore owners admit physical projections while empty. External
    /// enrichment/resolution producers remain disabled until publication.
    restore_build_only: bool = false,
    drain_resolver_backfill: bool = true,
    embedding_options: managed_embedder.InitOptions = .{},
    source_table: []const u8 = "",
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
};

pub fn dbIndexReconciliationCanMutate(db: *const db_mod.DB) bool {
    return db.open_mode != .query_readonly and db.open_mode != .status_only;
}

pub fn reconcileDbIndexesWithOptions(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    options: ReconcileDbIndexOptions,
) !ProvisionSummary {
    if (options.restore_build_only) {
        if (!dbIndexReconciliationCanMutate(db)) return error.ReadOnly;
        const removed = try removeMissingIndexes(alloc, db, indexes_json);
        const indexes = try ensureIndexes(alloc, db, indexes_json);
        try db.syncIndexes(true);
        return .{ .indexes_added = indexes.added, .indexes_removed = removed + indexes.removed, .indexes_pending = indexes.pending };
    }
    var desired_enrichments = std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig).empty;
    defer {
        for (desired_enrichments.items) |*cfg| cfg.deinit(alloc);
        desired_enrichments.deinit(alloc);
    }
    try collectDesiredEnrichmentsFromJson(alloc, indexes_json, options.embedding_options, &desired_enrichments);
    try indexes_api.validateArtifactEnrichmentConfigs(alloc, desired_enrichments.items);
    dedupeDesiredEnrichments(alloc, &desired_enrichments);
    indexes_api.sortArtifactEnrichmentsByDependency(desired_enrichments.items);

    // Read/query opens attach to already-persisted index state only. Metadata-driven
    // materialization is owned by writable provisioners so stale readers never
    // race the single-writer root contract.
    if (!dbIndexReconciliationCanMutate(db)) return .{};

    const enrichment_summary = try ensureEnrichments(db, desired_enrichments.items);
    const resolver_summary = try ensureResolversWithOptions(alloc, db, indexes_json, .{
        .drain_backfill = options.drain_resolver_backfill,
        .source_table = options.source_table,
        .destination_authorizer = options.destination_authorizer,
    });
    const missing_indexes_removed = try removeMissingIndexes(alloc, db, indexes_json);
    const index_summary = try ensureIndexes(alloc, db, indexes_json);
    const enrichments_removed = try removeMissingEnrichments(alloc, db, desired_enrichments.items);
    const indexes_removed = missing_indexes_removed + index_summary.removed;
    if (index_summary.added > 0 or indexes_removed > 0 or enrichment_summary.changed() or enrichments_removed > 0 or resolver_summary.changed()) {
        const pending = db.pendingWorkStats();
        if (pending.enrichment.error_count == 0) {
            // Reconciliation persists catalog/applied-sequence state through the
            // primary store. Avoid forcing every newly-created empty index WAL
            // during create-table; repair/replay paths force-sync real index
            // mutations after applying data.
            try db.core.index_manager.syncAll(false);
        }
    }
    return .{
        .groups_considered = 0,
        .dbs_opened = 0,
        .indexes_added = index_summary.added,
        .indexes_removed = indexes_removed,
        .indexes_pending = index_summary.pending,
        .enrichments_added = enrichment_summary.added,
        .enrichments_updated = enrichment_summary.updated,
        .enrichments_removed = enrichments_removed,
        .resolvers_added = resolver_summary.added,
        .resolvers_updated = resolver_summary.updated,
        .resolvers_removed = resolver_summary.removed,
    };
}

/// Reconcile one catalog index without applying sibling index, resolver, or
/// table-schema changes carried by the same metadata snapshot.
/// This is the storage boundary used by online index DDL after its foreground
/// write-capability barrier has completed (or partially completed).
pub fn reconcileDbIndexTargetWithOptions(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    index_name: []const u8,
    options: ReconcileDbIndexOptions,
) !ProvisionSummary {
    if (!dbIndexReconciliationCanMutate(db)) return .{};
    if (index_name.len == 0 or indexes_api.isReservedIndexMetadataEntry(index_name))
        return error.InvalidTableIndexMetadata;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };

    var desired_enrichments = std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig).empty;
    defer {
        for (desired_enrichments.items) |*cfg| cfg.deinit(alloc);
        desired_enrichments.deinit(alloc);
    }
    try collectDesiredEnrichmentsFromJson(alloc, indexes_json, options.embedding_options, &desired_enrichments);
    try indexes_api.validateArtifactEnrichmentConfigs(alloc, desired_enrichments.items);
    dedupeDesiredEnrichments(alloc, &desired_enrichments);
    indexes_api.sortArtifactEnrichmentsByDependency(desired_enrichments.items);

    var target_enrichments = std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig).empty;
    defer {
        for (target_enrichments.items) |*cfg| cfg.deinit(alloc);
        target_enrichments.deinit(alloc);
    }

    const current = try db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, current);
    var target_summary: IndexEnsureSummary = .{};
    var target_value: ?std.json.Value = null;
    var target_array_form = false;

    if (object.get("indexes")) |indexes_value| {
        const items = switch (indexes_value) {
            .array => |array| array.items,
            else => return error.InvalidTableIndexMetadata,
        };
        for (items) |item| {
            const name = try indexDefinitionName(item);
            if (!std.mem.eql(u8, name, index_name)) continue;
            if (target_value != null) return error.InvalidTableIndexMetadata;
            target_value = item;
            target_array_form = true;
        }
    } else {
        if (object.get(index_name)) |config_value| {
            target_value = config_value;
        }
    }

    if (target_value) |value| {
        try indexes_api.collectArtifactEnrichmentsFromValueWithOptions(alloc, value, options.embedding_options, &target_enrichments);
        dedupeDesiredEnrichments(alloc, &target_enrichments);
        indexes_api.sortArtifactEnrichmentsByDependency(target_enrichments.items);
    }
    const enrichment_summary = try ensureEnrichments(db, target_enrichments.items);

    if (target_value) |value| {
        try ensureIndexDefinition(
            alloc,
            db,
            current,
            &target_summary,
            index_name,
            try parseIndexKind(value),
            if (target_array_form) indexDefinitionConfigValue(value) else value,
            target_array_form,
        );
    } else {
        if (try db.deleteIndex(index_name)) target_summary.removed += 1;
    }
    // Removing every persisted enrichment absent from the current catalog is
    // target-safe: it never applies a sibling addition or update, while also
    // making deletion retryable after a crash between index and enrichment
    // retirement. Definitions still referenced by another index or by the
    // table-level enrichment catalog remain in desired_enrichments.
    const enrichments_removed = try removeAbsentEnrichments(alloc, db, desired_enrichments.items);
    if (target_summary.added > 0 or target_summary.removed > 0 or enrichment_summary.changed() or enrichments_removed > 0) {
        const pending = db.pendingWorkStats();
        // Targeted DDL cannot wait on checkpoints or maintenance owned by a
        // sibling index. Deletion already retires its durable generation; only
        // an installed target has index state to sync here.
        if (pending.enrichment.error_count == 0 and target_value != null and target_summary.pending == 0)
            try db.core.index_manager.syncIndexByName(index_name, false);
    }
    return .{
        .indexes_added = target_summary.added,
        .indexes_removed = target_summary.removed,
        .indexes_pending = target_summary.pending,
        .enrichments_added = enrichment_summary.added,
        .enrichments_updated = enrichment_summary.updated,
        .enrichments_removed = enrichments_removed,
    };
}

pub fn removeMissingIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const current = try db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, current);

    var removed: usize = 0;
    for (current) |cfg| {
        if (try desiredIndexContains(object, cfg.name)) continue;
        if (try db.deleteIndex(cfg.name)) removed += 1;
    }
    return removed;
}

pub const IndexEnsureSummary = struct {
    added: usize = 0,
    removed: usize = 0,
    pending: usize = 0,
};

pub fn ensureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !IndexEnsureSummary {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const current = try db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, current);

    var summary: IndexEnsureSummary = .{};
    if (object.get("indexes")) |indexes_value| {
        const items = switch (indexes_value) {
            .array => |array| array.items,
            else => return error.InvalidTableIndexMetadata,
        };
        for (items) |item| {
            const name = try indexDefinitionName(item);
            const kind = try parseIndexKind(item);
            const config_value = indexDefinitionConfigValue(item);
            try ensureIndexDefinition(alloc, db, current, &summary, name, kind, config_value, true);
        }
        return summary;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        // Reserved top-level sections are handled by their own reconcilers, not
        // by the index reconciler.
        if (indexes_api.isReservedIndexMetadataEntry(entry.key_ptr.*)) continue;
        const kind = try parseIndexKind(entry.value_ptr.*);
        try ensureIndexDefinition(alloc, db, current, &summary, entry.key_ptr.*, kind, entry.value_ptr.*, false);
    }
    return summary;
}

pub fn ensureIndexDefinition(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    current: []const db_mod.types.IndexConfig,
    summary: *IndexEnsureSummary,
    name: []const u8,
    kind: db_mod.types.IndexKind,
    config_value: std.json.Value,
    storage_config: bool,
) !void {
    const config_json = if (storage_config)
        try extractStoredIndexConfigJson(alloc, config_value)
    else
        try extractIndexConfigJsonForKind(alloc, name, kind, config_value);
    defer alloc.free(config_json);
    const configured_coverage_generation = coverage_policy.incarnation(config_value) orelse
        internal_keys.derivedCoverageGeneration(config_json);
    const desired = db_mod.types.IndexConfig{
        .name = name,
        .kind = kind,
        .config_json = config_json,
        // New catalog records carry a random incarnation. v0.2 records may
        // predate that field, so derive the same deterministic fallback used
        // by public readiness and storage-open parsing.
        .coverage_generation = configured_coverage_generation,
    };
    const existing = findIndexConfig(current, name);
    if (existing) |existing_cfg| {
        if (existing_cfg.kind == kind and indexKindConfigReconcileDeferred(kind)) {
            if (kind == .graph or
                (existing_cfg.coverage_generation == desired.coverage_generation and
                    try indexConfigsEqual(alloc, existing_cfg, desired))) return;
        }
    }
    if (existing) |existing_cfg| {
        if (try indexConfigsEqual(alloc, existing_cfg, desired)) {
            if (db_mod.DB.indexKindSupportsManagedGenerationRepair(desired.kind)) {
                if (try db.materializeManagedIndexAdmission(alloc, desired.name) != null) {
                    summary.pending += 1;
                }
            }
            return;
        }
        if (try db.deleteIndex(desired.name)) {
            summary.removed += 1;
            summary.pending += 1;
            // Retirement publishes a durable cleanup tombstone. Re-admitting
            // the same artifact namespace in this pass would either race the
            // owner or require an unbounded request-thread corpus scan. The
            // cleanup owner advances the tombstone in bounded pages and the
            // next idempotent reconcile admits the desired generation.
            return;
        }
    }
    const admitted = db_mod.types.IndexConfig{
        .name = desired.name,
        .kind = desired.kind,
        .config_json = desired.config_json,
        .coverage_generation = desired.coverage_generation,
    };
    const repair_id = db.admitManagedIndex(admitted) catch |err| switch (err) {
        error.IndexArtifactCleanupPending => {
            summary.pending += 1;
            return;
        },
        else => return err,
    };
    if (repair_id != null) summary.pending += 1;
    summary.added += 1;
}

pub fn desiredIndexContains(object: std.json.ObjectMap, name: []const u8) !bool {
    if (object.get("indexes")) |indexes_value| {
        const items = switch (indexes_value) {
            .array => |array| array.items,
            else => return error.InvalidTableIndexMetadata,
        };
        for (items) |item| {
            if (std.mem.eql(u8, try indexDefinitionName(item), name)) return true;
        }
        return false;
    }
    if (indexes_api.isReservedIndexMetadataEntry(name)) return false;
    return object.contains(name);
}

pub fn indexDefinitionName(value: std.json.Value) ![]const u8 {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const name_value = object.get("name") orelse return error.InvalidTableIndexMetadata;
    return switch (name_value) {
        .string => |name| if (name.len > 0) name else error.InvalidTableIndexMetadata,
        else => error.InvalidTableIndexMetadata,
    };
}

pub fn indexDefinitionConfigValue(value: std.json.Value) std.json.Value {
    const object = switch (value) {
        .object => |object| object,
        else => return value,
    };
    return object.get("config") orelse value;
}

pub fn findIndexConfig(configs: []const db_mod.types.IndexConfig, name: []const u8) ?db_mod.types.IndexConfig {
    for (configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

pub fn indexConfigsEqual(alloc: std.mem.Allocator, a: db_mod.types.IndexConfig, b: db_mod.types.IndexConfig) !bool {
    if (a.kind != b.kind) return false;
    if (a.kind == .full_text) return fullTextIndexConfigsEqual(alloc, a.config_json, b.config_json);
    if (a.kind == .algebraic) return algebraicIndexConfigsEqual(alloc, a.config_json, b.config_json);
    if ((a.kind == .dense_vector or a.kind == .sparse_vector) and
        a.coverage_generation != b.coverage_generation) return false;
    return std.mem.eql(u8, a.config_json, b.config_json);
}

pub fn indexKindConfigReconcileDeferred(kind: db_mod.types.IndexKind) bool {
    return switch (kind) {
        .dense_vector, .sparse_vector, .graph => true,
        .full_text, .algebraic => false,
    };
}

pub fn fullTextIndexConfigsEqual(alloc: std.mem.Allocator, a_json: []const u8, b_json: []const u8) !bool {
    var a_parsed = try std.json.parseFromSlice(std.json.Value, alloc, a_json, .{});
    defer a_parsed.deinit();
    var b_parsed = try std.json.parseFromSlice(std.json.Value, alloc, b_json, .{});
    defer b_parsed.deinit();
    return jsonValuesEqualIgnoringTopLevelEnrichments(a_parsed.value, b_parsed.value, true);
}

pub fn algebraicIndexConfigsEqual(alloc: std.mem.Allocator, a_json: []const u8, b_json: []const u8) !bool {
    var a_parsed = try std.json.parseFromSlice(std.json.Value, alloc, a_json, .{});
    defer a_parsed.deinit();
    var b_parsed = try std.json.parseFromSlice(std.json.Value, alloc, b_json, .{});
    defer b_parsed.deinit();
    return jsonValuesEqualIgnoringTopLevelEnrichments(a_parsed.value, b_parsed.value, false);
}

pub fn jsonValuesEqualIgnoringTopLevelEnrichments(a: std.json.Value, b: std.json.Value, top_level: bool) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => |value| value == b.bool,
        .integer => |value| value == b.integer,
        .float => |value| value == b.float,
        .number_string => |value| std.mem.eql(u8, value, b.number_string),
        .string => |value| std.mem.eql(u8, value, b.string),
        .array => |array| blk: {
            if (array.items.len != b.array.items.len) break :blk false;
            for (array.items, b.array.items) |a_item, b_item| {
                if (!jsonValuesEqualIgnoringTopLevelEnrichments(a_item, b_item, false)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            const b_object = b.object;
            var a_count: usize = 0;
            var a_it = object.iterator();
            while (a_it.next()) |entry| {
                if (top_level and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                a_count += 1;
                const b_value = b_object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqualIgnoringTopLevelEnrichments(entry.value_ptr.*, b_value, false)) break :blk false;
            }
            var b_count: usize = 0;
            var b_it = b_object.iterator();
            while (b_it.next()) |entry| {
                if (top_level and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                b_count += 1;
            }
            break :blk a_count == b_count;
        },
    };
}

pub fn collectDesiredEnrichmentsFromJson(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    embedding_options: managed_embedder.InitOptions,
    out: *std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig),
) !void {
    {
        const collected = try indexes_api.collectArtifactEnrichmentsFromTableIndexesJsonWithOptions(alloc, indexes_json, embedding_options);
        errdefer db_mod.types.freeEnrichmentConfigs(alloc, collected);
        try out.appendSlice(alloc, collected);
        alloc.free(collected);
    }
}

pub const EnrichmentEnsureSummary = struct {
    added: usize = 0,
    updated: usize = 0,

    pub fn changed(self: EnrichmentEnsureSummary) bool {
        return self.added > 0 or self.updated > 0;
    }
};

pub fn ensureEnrichments(db: *db_mod.DB, desired: []const db_mod.types.EnrichmentConfig) !EnrichmentEnsureSummary {
    var summary: EnrichmentEnsureSummary = .{};
    for (desired) |cfg| {
        switch (try db.upsertEnrichment(cfg)) {
            .added => summary.added += 1,
            .updated => summary.updated += 1,
            .unchanged => {},
        }
    }
    return summary;
}

pub fn dedupeDesiredEnrichments(
    alloc: std.mem.Allocator,
    desired: *std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig),
) void {
    var i: usize = 0;
    while (i < desired.items.len) {
        const cfg = desired.items[i];
        var duplicate = false;
        for (desired.items[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, cfg.name)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            i += 1;
            continue;
        }
        var removed = desired.orderedRemove(i);
        removed.deinit(alloc);
    }
}

pub fn removeMissingEnrichments(alloc: std.mem.Allocator, db: *db_mod.DB, desired: []const db_mod.types.EnrichmentConfig) !usize {
    const existing = try db.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, existing);

    var removed: usize = 0;
    var i = existing.len;
    while (i > 0) {
        i -= 1;
        const cfg = existing[i];
        if (findEnrichmentByName(desired, cfg.name)) |desired_cfg| {
            if (try enrichmentConfigsEqual(alloc, cfg, desired_cfg)) continue;
        }
        if (db.deleteEnrichment(cfg.kind, cfg.name)) |deleted| {
            if (deleted) removed += 1;
        } else |err| switch (err) {
            error.EnrichmentInUse => continue,
            else => return err,
        }
    }
    return removed;
}

pub fn removeAbsentEnrichments(alloc: std.mem.Allocator, db: *db_mod.DB, desired: []const db_mod.types.EnrichmentConfig) !usize {
    const existing = try db.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, existing);

    var removed: usize = 0;
    var i = existing.len;
    while (i > 0) {
        i -= 1;
        const cfg = existing[i];
        // Target reconciliation may observe a newer sibling definition. Its
        // named operation must not delete the old sibling config merely because
        // that config still needs a whole-table update; only catalog absence is
        // globally safe cleanup.
        if (findEnrichmentByName(desired, cfg.name) != null) continue;
        if (db.deleteEnrichment(cfg.kind, cfg.name)) |deleted| {
            if (deleted) removed += 1;
        } else |err| switch (err) {
            error.EnrichmentInUse => continue,
            else => return err,
        }
    }
    return removed;
}

pub fn findEnrichmentByName(
    configs: []const db_mod.types.EnrichmentConfig,
    name: []const u8,
) ?db_mod.types.EnrichmentConfig {
    for (configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

pub fn enrichmentConfigsEqual(alloc: std.mem.Allocator, a: db_mod.types.EnrichmentConfig, b: db_mod.types.EnrichmentConfig) !bool {
    return a.kind == b.kind and
        std.mem.eql(u8, a.name, b.name) and
        std.mem.eql(u8, a.field, b.field) and
        std.mem.eql(u8, a.template, b.template) and
        std.mem.eql(u8, a.source_artifact_name, b.source_artifact_name) and
        a.expected_dims == b.expected_dims and
        a.chunk_size == b.chunk_size and
        a.chunk_overlap == b.chunk_overlap and
        std.mem.eql(u8, a.chunker_json, b.chunker_json) and
        a.full_text_index == b.full_text_index and
        std.mem.eql(u8, a.content_type, b.content_type) and
        try enrichment_config_validation.producerJsonValuesEqual(alloc, a.producer_json, b.producer_json) and
        std.meta.eql(a.execution, b.execution);
}

pub const ResolverReconcileSummary = struct {
    added: usize = 0,
    updated: usize = 0,
    removed: usize = 0,
    unchanged: usize = 0,

    pub fn changed(self: @This()) bool {
        return self.added > 0 or self.updated > 0 or self.removed > 0;
    }
};

pub const EnsureResolverOptions = struct {
    drain_backfill: bool = true,
    source_table: []const u8 = "",
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
};

pub fn ensureResolversWithOptions(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    options: EnsureResolverOptions,
) !ResolverReconcileSummary {
    try stored_destination_authorization.authorizeIndexesJson(
        alloc,
        indexes_json,
        options.source_table,
        options.destination_authorizer,
    );
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    var desired = std.ArrayListUnmanaged(db_mod.ResolverConfig).empty;
    defer {
        for (desired.items) |*cfg| cfg.deinit(alloc);
        desired.deinit(alloc);
    }
    try collectDesiredResolvers(alloc, parsed.value, &desired);

    var summary: ResolverReconcileSummary = .{};
    for (desired.items) |cfg| {
        const result = try db.upsertResolverWithResultOptions(cfg, .{
            .drain_backfill = options.drain_backfill,
        });
        switch (result) {
            .inserted => summary.added += 1,
            .updated_backfill_required => summary.updated += 1,
            .updated_no_backfill => summary.unchanged += 1,
        }
    }

    const existing = try db.listResolvers(alloc);
    defer {
        for (existing) |*cfg| cfg.deinit(alloc);
        alloc.free(existing);
    }
    for (existing) |cfg| {
        if (desiredResolverContains(desired.items, cfg.name)) continue;
        const removed = if (options.drain_backfill)
            try db.removeResolver(cfg.name)
        else
            try db.removeResolverWithoutDrain(cfg.name);
        if (removed) summary.removed += 1;
    }
    return summary;
}

pub fn desiredResolverContains(desired: []const db_mod.ResolverConfig, name: []const u8) bool {
    for (desired) |cfg| {
        if (std.mem.eql(u8, cfg.name, name)) return true;
    }
    return false;
}

pub fn collectDesiredResolvers(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.ResolverConfig),
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("resolvers")) |resolvers| {
                if (resolvers == .array) {
                    for (resolvers.array.items) |item| {
                        if (item != .object) continue;
                        const parsed = try std.json.parseFromValue(db_mod.ResolverConfig, alloc, item, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        // `parsed.value` is owned by the parse arena; clone with
                        // `alloc` so `out`'s entries free correctly (and so they
                        // outlive the arena).
                        defer parsed.deinit();
                        try out.append(alloc, try db_mod.ResolverConfig.clone(alloc, parsed.value));
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "resolvers")) continue;
                try collectDesiredResolvers(alloc, entry.value_ptr.*, out);
            }
        },
        .array => |array| {
            for (array.items) |item| try collectDesiredResolvers(alloc, item, out);
        },
        else => {},
    }
}

pub fn parseIndexKind(value: std.json.Value) !db_mod.types.IndexKind {
    if (value != .object) return .full_text;
    const type_value = value.object.get("type") orelse {
        if (looksLikeStoredAlgebraicIndexConfig(value)) return .algebraic;
        return .full_text;
    };
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
    if (std.mem.eql(u8, type_value.string, "embeddings")) {
        const sparse = try embeddingIndexSparseFlag(value);
        return if (sparse) .sparse_vector else .dense_vector;
    }
    return error.UnsupportedCreateTableRequest;
}

pub fn embeddingIndexSparseFlag(value: std.json.Value) !bool {
    if (value != .object) return false;
    if (value.object.get("sparse")) |sparse_value| {
        return switch (sparse_value) {
            .bool => sparse_value.bool,
            else => error.InvalidCreateTableRequest,
        };
    }
    const config_value = value.object.get("config") orelse return false;
    const config_object = switch (config_value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    const sparse_value = config_object.get("sparse") orelse return false;
    return switch (sparse_value) {
        .bool => sparse_value.bool,
        else => error.InvalidCreateTableRequest,
    };
}

pub fn looksLikeStoredAlgebraicIndexConfig(value: std.json.Value) bool {
    if (value != .object) return false;
    if (value.object.get("schema_version") == null and
        (value.object.get("version") == null or value.object.get("table") == null)) return false;
    return value.object.get("group_fields") != null or
        value.object.get("measure_fields") != null or
        value.object.get("time_fields") != null or
        value.object.get("materializations") != null;
}

pub fn extractIndexConfigJsonForKind(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    kind: db_mod.types.IndexKind,
    value: std.json.Value,
) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    switch (kind) {
        .dense_vector, .sparse_vector => return try managed_embedder.translateEmbeddingsIndexConfigJson(alloc, index_name, value),
        else => {},
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (skipPublicIndexMetadataField(kind, entry.key_ptr.*)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn extractStoredIndexConfigJson(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub fn skipPublicIndexMetadataField(kind: db_mod.types.IndexKind, field: []const u8) bool {
    return table_index_config.isCatalogMetadataField(kind, field);
}

pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}
