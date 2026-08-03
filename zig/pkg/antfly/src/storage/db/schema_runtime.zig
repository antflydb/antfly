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
const builtin = @import("builtin");

const algebraic_ir = @import("algebraic/ir.zig");
const db_config = @import("config.zig");
const db_internal = @import("internal.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_api_mod = @import("../../schema/mod.zig");
const schema_mod = @import("../schema.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

pub const local_schema_json_key = "\x00\x00__metadata__:schema_json";
pub const local_lite_sql_table_record_json_key = "\x00\x00__metadata__:lite_sql_table_record_json";

pub const ApplyTableSchemaOptions = struct {
    persist_local_schema_json: bool = true,
    reload_algebraic_schema_configs: bool = true,
};

fn deleteKeysWithPrefixFromStore(alloc: Allocator, store: *docstore_mod.DocStore, prefix: []const u8) !void {
    const keys = try store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, keys);
    if (keys.len == 0) return;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (keys) |item| {
        try deletes.append(alloc, item.key);
    }
    try store.putBatch(&.{}, deletes.items);
}

fn deleteDerivedCoverageForIndex(alloc: Allocator, store: *docstore_mod.DocStore, index_name: []const u8) !void {
    const prefix = try internal_keys.derivedCoverageOutcomePrefixAlloc(alloc, index_name);
    defer alloc.free(prefix);
    try deleteKeysWithPrefixFromStore(alloc, store, prefix);
}

pub const SchemaRewriteJobExecutionResult = struct {
    report: relational_store_mod.RowRewriteReport,
    progress_row_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.progress_row_key);
        self.* = undefined;
    }

    pub fn finishRequest(self: @This(), job: metadata_table_manager.SchemaRewriteJobRecord) metadata_table_manager.SchemaRewriteJobFinishRequest {
        return .{
            .job_id = job.job_id,
            .lease_owner = job.lease_owner,
            .completed_row_count = self.report.scanned_rows,
            .progress_row_key = self.progress_row_key,
        };
    }
};

pub const SchemaRewriteJobDrainOptions = struct {
    worker_id: []const u8 = "antfly-schema-rewrite-worker",
    group_id: ?u64 = null,
    now_ms: ?u64 = null,
    lease_ttl_ms: u64 = 60_000,
    max_jobs: usize = 1,
};

pub fn Impl(comptime DB: type) type {
    return struct {
        const InstalledIndex = struct {
            applied: u64,
            needs_enrichment_replay: bool,
            managed_admission_pending: bool = false,
            post_commit_error: ?anyerror = null,
        };

        const IndexAdmissionMode = enum {
            ordinary,
            managed_full_text,
        };

        var test_fail_managed_index_delete_after_catalog_commit = false;

        var test_fail_managed_index_repair_cleanup_after_catalog_commit = false;

        pub var test_fail_index_activation_after_catalog_commit = false;

        pub fn setTestFailManagedIndexDeleteAfterCatalogCommit(value: bool) void {
            if (!builtin.is_test) return;
            test_fail_managed_index_delete_after_catalog_commit = value;
        }

        pub fn setTestFailManagedIndexRepairCleanupAfterCatalogCommit(value: bool) void {
            if (!builtin.is_test) return;
            test_fail_managed_index_repair_cleanup_after_catalog_commit = value;
        }

        pub fn managedAdmissionVisibilitySummary(self: *DB) !doc_identity.VisibilitySummary {
            // Admission is a rare structural mutation and must be based on the
            // primary store that commits the catalog marker. The runtime cache is
            // published only after HA mirrors complete, so a mirror failure can
            // legitimately leave it behind the durable primary state.
            if (try doc_identity.visibilitySummaryFromStore(self.core.store)) |summary| return summary;
            const empty = (try doc_identity.loadAllNewTrustedStateForNamespace(
                self.core.store,
                self.core.identity_namespace,
            )) orelse return error.InvalidDocIdentity;
            return empty.visibility_summary;
        }

        pub fn installIndexWhileEnrichmentQuiesced(
            self: *DB,
            cfg: types.IndexConfig,
            admission_mode: IndexAdmissionMode,
        ) !InstalledIndex {
            self.core.lockApply();
            defer self.core.unlockApply();
            try DB.SchemaRuntimeCallbacks.remove_orphaned_index_repair_intent_for_fresh_admission(self, self.alloc, cfg.name);
            if (admission_mode == .managed_full_text and cfg.kind != .full_text) return error.InvalidArgument;
            const summary = try Self.managedAdmissionVisibilitySummary(self);
            const managed_admission_pending = admission_mode == .managed_full_text and summary.live_ordinals != 0;
            const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(self.alloc, cfg.name);
            defer self.alloc.free(admission_key);
            const admission_value = DB.SchemaRuntimeCallbacks.encode_managed_index_admission_value(
                types.indexAdmissionConfigHash(cfg),
                summary.live_ordinals,
                @max(summary.max_created_generation, summary.max_deleted_generation),
                self.core.nextDerivedSequence(),
                managed_admission_pending,
            );
            const admission_write: index_manager_mod.IndexManager.AtomicCatalogMutation = .{ .put = .{
                .key = admission_key,
                .value = &admission_value,
            } };
            var catalog_committed = false;
            // Allocate the service gate before committing admission. Once the
            // catalog and marker become durable, allocation failure must not
            // create an ungated window in the current process.
            try self.core.index_manager.markRepairUnavailable(cfg.name);
            errdefer if (!catalog_committed) {
                self.core.index_manager.clearRepairUnavailable(cfg.name);
            };

            // Managed empty-source admission still bypasses ordinary synchronous
            // backfill. Its O(1) identity proof establishes an already-complete
            // generation at the current replay head without scanning tombstones.
            var applied = switch (admission_mode) {
                .ordinary => try self.core.addIndex(cfg, admission_write),
                .managed_full_text => try self.core.addManagedIndex(cfg, admission_write),
            };
            catalog_committed = true;
            // Publish outbox work only after the catalog/marker transaction is
            // durable. Any later failure leaves a generation that the scheduler
            // can drain without relying on the failed request to reach its tail.
            if (managed_admission_pending) DB.SchemaRuntimeCallbacks.request_managed_admission_materialization(self);

            var post_commit_error: ?anyerror = if (builtin.is_test and test_fail_index_activation_after_catalog_commit)
                error.TestPostCommitIndexActivation
            else
                null;
            if (admission_mode == .managed_full_text and !managed_admission_pending) {
                applied = self.core.nextDerivedSequence();
            }
            if (post_commit_error == null) {
                self.core.saveAppliedSequence(cfg.name, applied) catch |err| {
                    post_commit_error = err;
                };
            }
            if (post_commit_error == null) {
                DB.SchemaRuntimeCallbacks.initialize_dense_artifact_target_counter_if_needed(self, cfg) catch |err| {
                    post_commit_error = err;
                };
            }
            if (post_commit_error == null) {
                DB.SchemaRuntimeCallbacks.save_index_status_snapshot(self, cfg.name, applied) catch |err| {
                    post_commit_error = err;
                };
            }
            if (cfg.kind == .algebraic) {
                DB.SchemaRuntimeCallbacks.hydrate_algebraic_observation_status_for_index_best_effort(self, cfg.name);
            }
            var needs_enrichment_replay = false;
            if (post_commit_error == null) {
                needs_enrichment_replay = self.core.indexRequiresEnrichmentReplay(cfg.name) catch |err| blk: {
                    post_commit_error = err;
                    break :blk false;
                };
            }
            return .{
                .applied = applied,
                .needs_enrichment_replay = needs_enrichment_replay,
                .managed_admission_pending = managed_admission_pending,
                .post_commit_error = post_commit_error,
            };
        }

        pub fn restartEnrichmentAfterStructuralMutation(self: *DB, operation: []const u8, index_name: []const u8) !void {
            self.async_context.enrichment_desired_running.store(true, .release);
            db_internal.lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
            const runtime = self.async_context.enrichment_runtime orelse {
                self.async_context.enrichment_lifecycle_mutex.unlock();
                return;
            };
            const start_result = if (!runtime.isStarted()) DB.SchemaRuntimeCallbacks.start_enrichment_runtime_for_lifecycle(runtime) else {};
            self.async_context.enrichment_lifecycle_mutex.unlock();
            start_result catch |err| {
                // The durable restart supervisor owns recovery from this point;
                // preserve the diagnostic without classifying a retryable outage
                // as an unrecoverable process failure.
                std.log.warn("failed to restart enrichment runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
                DB.SchemaRuntimeCallbacks.schedule_enrichment_restart_context(
                    self.async_context,
                    self.backend_runtime.durable_jobs,
                    self.repair_cleanup_owner_id,
                );
                DB.notifyQueryVisibilityHook(self.async_context, .status);
                return err;
            };
        }

        fn finalizeCompletedIndexAdmission(self: *DB, index_name: []const u8) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            const key = try internal_keys.managedIndexAdmissionKeyAlloc(self.alloc, index_name);
            defer self.alloc.free(key);
            self.core.store.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.core.index_manager.clearManagedAdmissionSnapshotForIndex(index_name);
            self.core.index_manager.clearRepairUnavailable(index_name);
            DB.notifyQueryVisibilityHook(self.async_context, .index_repair_cleared);
        }

        fn recoverCommittedIndexAdmission(self: *DB, cfg: types.IndexConfig, activation_err: anyerror) ?u128 {
            // Admission remains fail-closed until the durable repair marker is
            // materialized, so this is degraded-but-recoverable state.
            std.log.warn("index catalog admission committed with pending activation index={s} err={s}", .{ cfg.name, @errorName(activation_err) });
            DB.SchemaRuntimeCallbacks.request_managed_admission_materialization(self);
            DB.SchemaRuntimeCallbacks.drain_managed_index_admissions(self, self.alloc) catch |materialize_err| {
                // The primary-store marker remains authoritative and startup will
                // retry projection into the repair scheduler.
                std.log.err("committed index activation marker remains pending index={s} err={s}", .{ cfg.name, @errorName(materialize_err) });
                return null;
            };
            return DB.SchemaRuntimeCallbacks.index_repair_id_for_index(self, self.alloc, cfg.name) catch |lookup_err| {
                std.log.err("committed index activation repair lookup failed index={s} err={s}", .{ cfg.name, @errorName(lookup_err) });
                return null;
            };
        }

        fn completeInstalledIndexAdmission(
            self: *DB,
            cfg: types.IndexConfig,
            installed: InstalledIndex,
            restart_enrichment: bool,
        ) !?u128 {
            const repair_id = if (installed.managed_admission_pending) repair_blk: {
                // Drain the complete outbox, not only this index. A prior admission
                // may have committed before a transient checkpoint failure.
                try DB.SchemaRuntimeCallbacks.drain_managed_index_admissions(self, self.alloc);
                break :repair_blk try DB.SchemaRuntimeCallbacks.index_repair_id_for_index(self, self.alloc, cfg.name);
            } else null;

            var worker_applied = installed.applied;
            if (installed.needs_enrichment_replay) {
                // A newly admitted index owns a fresh coverage generation. Establish
                // its worker baseline only after current artifact reconstruction.
                const rebuilt = switch (cfg.kind) {
                    .dense_vector => try DB.SchemaRuntimeCallbacks.rebuild_dense_index_for_target_coverage_context(self.async_context, cfg.name, 2048),
                    .sparse_vector => try DB.SchemaRuntimeCallbacks.rebuild_sparse_index_from_stored_embedding_artifacts_context(self.async_context, cfg.name, 2048),
                    else => 0,
                };
                const generation_baseline = self.core.nextDerivedSequence();
                if (rebuilt > 0 or generation_baseline > worker_applied) {
                    worker_applied = generation_baseline;
                    try self.core.saveAppliedSequence(cfg.name, worker_applied);
                    try DB.SchemaRuntimeCallbacks.save_index_status_snapshot(self, cfg.name, worker_applied);
                }
                if (self.enrichment_runtime != null) {
                    const refs = try DB.SchemaRuntimeCallbacks.replay_generated_enrichments_from_stored_docs(self, self.alloc);
                    if (refs == 0) {
                        const target_sequence = self.core.nextDerivedSequence();
                        try self.core.saveAppliedSequence(cfg.name, target_sequence);
                        try Self.markEnrichmentAppliedIfNoPendingThrough(self, target_sequence);
                    }
                }
            }

            if (self.start_index_workers) {
                try self.executor.addWorker(cfg.name, .{ .name = cfg.name, .kind = cfg.kind }, worker_applied);
            }
            if (restart_enrichment) try Self.restartEnrichmentAfterStructuralMutation(self, "index creation", cfg.name);
            if (self.start_index_workers) {
                const current_target = self.core.nextDerivedSequence();
                if (current_target > worker_applied) self.executor.notifyIndexes(current_target, &.{cfg.name});
            }
            if (!installed.managed_admission_pending) try Self.finalizeCompletedIndexAdmission(self, cfg.name);
            return repair_id;
        }

        fn addIndexWithAdmission(self: *DB, cfg: types.IndexConfig, admission_mode: IndexAdmissionMode) !?u128 {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            // Generated artifact namespaces can be shared across differently named
            // indexes. Cleanup is durable and owner-driven; never turn index
            // admission into an unbounded corpus scan. Metadata reconciliation can
            // retry after the conflicting generation's tombstone is retired.
            try DB.SchemaRuntimeCallbacks.reject_conflicting_retired_index_cleanup_for_admission(self, cfg);
            const restart_enrichment = DB.SchemaRuntimeCallbacks.quiesce_enrichment_for_structural_mutation(self);
            var enrichment_restarted = false;
            errdefer if (restart_enrichment and !enrichment_restarted) {
                Self.restartEnrichmentAfterStructuralMutation(self, "failed index creation", cfg.name) catch |restart_err| {
                    std.log.err("failed to restore enrichment runtime after index creation error index={s} err={s}", .{ cfg.name, @errorName(restart_err) });
                };
            };
            const installed = try Self.installIndexWhileEnrichmentQuiesced(self, cfg, admission_mode);
            if (installed.post_commit_error) |activation_err| {
                if (restart_enrichment) Self.restartEnrichmentAfterStructuralMutation(self, "pending index activation", cfg.name) catch {};
                enrichment_restarted = true;
                return Self.recoverCommittedIndexAdmission(self, cfg, activation_err);
            }

            const repair_id = Self.completeInstalledIndexAdmission(self, cfg, installed, restart_enrichment) catch |activation_err| {
                if (restart_enrichment) Self.restartEnrichmentAfterStructuralMutation(self, "pending index activation", cfg.name) catch {};
                enrichment_restarted = true;
                return Self.recoverCommittedIndexAdmission(self, cfg, activation_err);
            };
            enrichment_restarted = true;
            return repair_id;
        }

        pub fn admitManagedFullTextIndex(self: *DB, cfg: types.IndexConfig) !?u128 {
            if (cfg.kind != .full_text) return error.InvalidArgument;
            return try Self.addIndexWithAdmission(self, cfg, .managed_full_text);
        }

        const Self = @This();

        fn currentTimeNs() u64 {
            return db_internal.currentTimeNs();
        }

        fn recordForeignKeyIntegrityProgressLocked(
            self: *DB,
            alloc: Allocator,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            report: relational_store_mod.ForeignKeyIntegrityReport,
        ) !void {
            try self.relationalIntegrityRecordForeignKeyIntegrityProgressLocked(alloc, mode, constraint_name, lower_doc_key, upper_doc_key, report);
        }

        pub fn setSchemaAfterGate(self: *DB, table_schema: schema_mod.TableSchema) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            try self.core.setSchema(table_schema);
            Self.refreshRuntimeSideEffects(self);
            try DB.SchemaRuntimeCallbacks.mirror_ha_schema_metadata_commit(self, table_schema);
        }

        pub fn setSchemaReplicatedApply(self: *DB, table_schema: schema_mod.TableSchema) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            try self.core.setSchema(table_schema);
            Self.refreshRuntimeSideEffects(self);
        }

        /// Apply table metadata schema JSON to the DB runtime and all schema-derived
        /// local artifacts. This is the single production entry point for table
        /// schema application so write-cache reconciliation, metadata provisioning,
        /// and crash recovery keep algebraic sidecars in the same lifecycle state.
        pub fn applyTableSchemaJsonAfterGate(
            self: *DB,
            alloc: Allocator,
            schema_json: []const u8,
            options: ApplyTableSchemaOptions,
        ) !void {
            if (schema_json.len == 0) return;

            var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
            defer parsed_schema.deinit(alloc);

            const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
            defer schema_mod.freeSchema(alloc, runtime_schema);

            self.core.lockApply();
            defer self.core.unlockApply();

            try Self.validateTableSchemaCompatibilityLocked(self, alloc, runtime_schema);
            if (options.reload_algebraic_schema_configs) {
                try Self.stageAlgebraicSchemaConfigsPending(self, schema_json);
            }
            try Self.migrateRelationalRowsForSchemaTransitionLocked(self, alloc, runtime_schema);
            try Self.migrateRelationalConstraintsForSchemaTransitionLocked(self, alloc, runtime_schema);
            if (options.persist_local_schema_json) {
                try Self.setSchemaWithLocalSchemaJsonAfterGate(self, runtime_schema, schema_json);
            } else {
                try Self.setSchemaAfterGate(self, runtime_schema);
            }
            if (options.reload_algebraic_schema_configs) {
                try Self.completePendingAlgebraicSchemaRebuilds(self);
            }
        }

        /// Refresh schema-derived algebraic index configs for callers that have
        /// already applied the runtime schema. This remains a structural mutation:
        /// config swaps, sidecar clears, and rebuild replay are serialized with
        /// normal apply work just like `applyTableSchemaJson`.
        pub fn reloadAlgebraicSchemaConfigsAfterGate(self: *DB, schema_json: []const u8) !void {
            if (schema_json.len == 0) return;

            self.core.lockApply();
            defer self.core.unlockApply();

            try self.core.index_manager.reloadAlgebraicSchemaConfigs(self.core.store, schema_json);
        }

        pub fn stageAlgebraicSchemaConfigsPending(self: *DB, schema_json: []const u8) !void {
            try self.core.index_manager.stageAlgebraicSchemaConfigsPending(self.core.store, schema_json);
        }

        pub fn completePendingAlgebraicSchemaRebuilds(self: *DB) !void {
            try self.core.index_manager.completePendingAlgebraicSchemaRebuilds(self.core.store);
        }

        pub fn setSchemaWithLocalSchemaJsonAfterGate(self: *DB, table_schema: schema_mod.TableSchema, schema_json: []const u8) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            const metadata_puts = [_]schema_mod.SchemaMetadataPut{.{
                .key = local_schema_json_key,
                .value = schema_json,
            }};
            try Self.setSchemaWithMetadataNoMirror(self, table_schema, metadata_puts[0..]);
            try DB.SchemaRuntimeCallbacks.mirror_ha_schema_json_metadata_commit(self, table_schema, schema_json);
        }

        pub fn setSchemaWithLocalSchemaJsonReplicatedApply(self: *DB, table_schema: schema_mod.TableSchema, schema_json: []const u8) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            const metadata_puts = [_]schema_mod.SchemaMetadataPut{.{
                .key = local_schema_json_key,
                .value = schema_json,
            }};
            try Self.setSchemaWithMetadataNoMirror(self, table_schema, metadata_puts[0..]);
        }

        pub fn setSchemaWithLocalLiteSqlTableRecordJsonAfterGate(
            self: *DB,
            table_schema: schema_mod.TableSchema,
            schema_json: []const u8,
            table_record_json: []const u8,
        ) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            const metadata_puts = [_]schema_mod.SchemaMetadataPut{
                .{
                    .key = local_schema_json_key,
                    .value = schema_json,
                },
                .{
                    .key = local_lite_sql_table_record_json_key,
                    .value = table_record_json,
                },
            };
            try Self.setSchemaWithMetadataNoMirror(self, table_schema, metadata_puts[0..]);
            try DB.SchemaRuntimeCallbacks.mirror_ha_lite_sql_table_metadata_commit(self, table_schema, schema_json, table_record_json);
        }

        pub fn setSchemaWithLocalLiteSqlTableRecordJsonReplicatedApply(
            self: *DB,
            table_schema: schema_mod.TableSchema,
            schema_json: []const u8,
            table_record_json: []const u8,
        ) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            const metadata_puts = [_]schema_mod.SchemaMetadataPut{
                .{
                    .key = local_schema_json_key,
                    .value = schema_json,
                },
                .{
                    .key = local_lite_sql_table_record_json_key,
                    .value = table_record_json,
                },
            };
            try Self.setSchemaWithMetadataNoMirror(self, table_schema, metadata_puts[0..]);
        }

        fn setSchemaWithMetadataNoMirror(
            self: *DB,
            table_schema: schema_mod.TableSchema,
            metadata_puts: []const schema_mod.SchemaMetadataPut,
        ) !void {
            try self.core.setSchemaWithMetadata(table_schema, metadata_puts[0..]);
            Self.refreshRuntimeSideEffects(self);
        }

        pub fn getSchemaJson(self: *DB, alloc: Allocator) !?[]u8 {
            return try self.core.getStoreValue(alloc, local_schema_json_key);
        }

        pub fn applyLiteSqlTableRecordAfterGate(self: *DB, alloc: Allocator, table: metadata_table_manager.TableRecord) !void {
            const table_record_json = try std.json.Stringify.valueAlloc(alloc, table, .{});
            defer alloc.free(table_record_json);

            var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, table.schema_json);
            defer parsed_schema.deinit(alloc);

            const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
            defer schema_mod.freeSchema(alloc, runtime_schema);

            self.core.lockApply();
            defer self.core.unlockApply();

            try Self.validateTableSchemaCompatibilityLocked(self, alloc, runtime_schema);
            try Self.stageAlgebraicSchemaConfigsPending(self, table.schema_json);
            try Self.migrateRelationalRowsForSchemaTransitionLocked(self, alloc, runtime_schema);
            try Self.migrateRelationalConstraintsForSchemaTransitionLocked(self, alloc, runtime_schema);
            try Self.setSchemaWithLocalLiteSqlTableRecordJsonAfterGate(self, runtime_schema, table.schema_json, table_record_json);
            try Self.completePendingAlgebraicSchemaRebuilds(self);
        }

        pub fn getLiteSqlTableRecordAlloc(self: *DB, alloc: Allocator) !?metadata_table_manager.TableRecord {
            const raw = (try self.core.getStoreValue(alloc, local_lite_sql_table_record_json_key)) orelse return null;
            defer alloc.free(raw);
            var parsed = try std.json.parseFromSlice(metadata_table_manager.TableRecord, alloc, raw, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            return try metadata_table_manager.cloneTable(alloc, parsed.value);
        }

        pub fn listAlgebraicMaterializationStates(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicMaterializationState {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            var out = std.ArrayListUnmanaged(types.AlgebraicMaterializationState).empty;
            errdefer {
                for (out.items) |*state| state.deinit(alloc);
                out.deinit(alloc);
            }

            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                if (index_name) |filter| {
                    if (!std.mem.eql(u8, entry.config.name, filter)) continue;
                }

                const persisted = try entry.index.scanPersistedMaterializationStates(self.core.store);
                defer {
                    for (persisted) |*state| state.deinit(entry.index.alloc);
                    if (persisted.len > 0) entry.index.alloc.free(persisted);
                }

                for (persisted) |state| {
                    const owned_index_name = try alloc.dupe(u8, entry.config.name);
                    errdefer alloc.free(owned_index_name);
                    const owned_recommendation = try alloc.dupe(u8, state.recommendation);
                    errdefer alloc.free(owned_recommendation);
                    const owned_lifecycle = try alloc.dupe(u8, @tagName(state.lifecycle));
                    errdefer alloc.free(owned_lifecycle);
                    try out.append(alloc, .{
                        .index_name = owned_index_name,
                        .recommendation = owned_recommendation,
                        .lifecycle = owned_lifecycle,
                        .observation_count = state.observation_count,
                    });
                }
            }

            return try out.toOwnedSlice(alloc);
        }

        pub fn listAlgebraicQueryObservations(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicQueryObservation {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            var out = std.ArrayListUnmanaged(types.AlgebraicQueryObservation).empty;
            errdefer {
                for (out.items) |*observation| observation.deinit(alloc);
                out.deinit(alloc);
            }

            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                if (index_name) |filter| {
                    if (!std.mem.eql(u8, entry.config.name, filter)) continue;
                }

                const persisted = try entry.index.scanPersistedQueryObservations(self.core.store);
                defer {
                    for (persisted) |*observation| observation.deinit(entry.index.alloc);
                    if (persisted.len > 0) entry.index.alloc.free(persisted);
                }

                for (persisted) |observation| {
                    const owned_index_name = try alloc.dupe(u8, entry.config.name);
                    errdefer alloc.free(owned_index_name);
                    const owned_shape = try alloc.dupe(u8, observation.shape);
                    errdefer alloc.free(owned_shape);
                    const owned_reason = try alloc.dupe(u8, observation.reason);
                    errdefer alloc.free(owned_reason);
                    const owned_recommendation = if (observation.recommendation) |value| try alloc.dupe(u8, value) else null;
                    errdefer if (owned_recommendation) |value| alloc.free(value);
                    const owned_lifecycle = try alloc.dupe(u8, @tagName(observation.lifecycle));
                    errdefer alloc.free(owned_lifecycle);
                    try out.append(alloc, .{
                        .index_name = owned_index_name,
                        .shape = owned_shape,
                        .count = observation.count,
                        .reason = owned_reason,
                        .recommendation = owned_recommendation,
                        .lifecycle = owned_lifecycle,
                    });
                }
            }

            return try out.toOwnedSlice(alloc);
        }

        pub fn evaluateAlgebraicAdaptiveCandidates(self: *DB) !u64 {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try Self.evaluateAlgebraicAdaptiveCandidatesLocked(self);
        }

        fn evaluateAlgebraicAdaptiveCandidatesLocked(self: *DB) !u64 {
            const target_sequence = self.core.nextDerivedSequence();
            var changed: u64 = 0;
            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                changed += try entry.index.evaluateAdaptiveCandidates(self.core.store, target_sequence);
                // Promote + backfill recurring cardinality observations into HLL
                // sketches here (leader-gated), not on the read path.
                changed += try entry.index.evaluateHllCardinalityCandidates(self.core.store);
            }
            return changed;
        }

        pub fn runAlgebraicAdaptiveWork(self: *DB) !u64 {
            self.core.lockApply();
            defer self.core.unlockApply();
            const target_sequence = self.core.nextDerivedSequence();
            var changed: u64 = 0;
            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                changed += try entry.index.runAdaptiveWork(self.core.store, target_sequence);
            }
            return changed;
        }

        pub fn listAlgebraicAdaptiveCandidates(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicAdaptiveCandidate {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            var out = std.ArrayListUnmanaged(types.AlgebraicAdaptiveCandidate).empty;
            errdefer {
                for (out.items) |*candidate| candidate.deinit(alloc);
                out.deinit(alloc);
            }

            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                if (index_name) |filter| {
                    if (!std.mem.eql(u8, entry.config.name, filter)) continue;
                }
                const persisted = try entry.index.scanPersistedAdaptiveCandidates(self.core.store);
                defer {
                    for (persisted) |*candidate| candidate.deinit(entry.index.alloc);
                    if (persisted.len > 0) entry.index.alloc.free(persisted);
                }
                for (persisted) |candidate| {
                    const owned_index_name = try alloc.dupe(u8, entry.config.name);
                    errdefer alloc.free(owned_index_name);
                    const owned_recommendation = try alloc.dupe(u8, candidate.recommendation);
                    errdefer alloc.free(owned_recommendation);
                    const owned_materialization_id = try alloc.dupe(u8, candidate.materialization_id);
                    errdefer alloc.free(owned_materialization_id);
                    const owned_lifecycle = try alloc.dupe(u8, @tagName(candidate.lifecycle));
                    errdefer alloc.free(owned_lifecycle);
                    const owned_decision = try alloc.dupe(u8, candidate.decision);
                    errdefer alloc.free(owned_decision);
                    try out.append(alloc, .{
                        .index_name = owned_index_name,
                        .recommendation = owned_recommendation,
                        .materialization_id = owned_materialization_id,
                        .lifecycle = owned_lifecycle,
                        .observation_count = candidate.observation_count,
                        .estimated_scan_rows_saved = candidate.estimated_scan_rows_saved,
                        .estimated_write_cost = candidate.estimated_write_cost,
                        .estimated_doc_rows = candidate.estimated_doc_rows,
                        .estimated_bucket_cardinality = candidate.estimated_bucket_cardinality,
                        .estimated_tensor_rows = candidate.estimated_tensor_rows,
                        .estimated_storage_bytes = candidate.estimated_storage_bytes,
                        .estimated_write_amplification = candidate.estimated_write_amplification,
                        .score = candidate.score,
                        .decision = owned_decision,
                        .idle_miss_count = candidate.idle_miss_count,
                        .generation = candidate.generation,
                    });
                }
            }
            return try out.toOwnedSlice(alloc);
        }

        pub fn listAlgebraicAdaptiveProgress(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicAdaptiveProgress {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            var out = std.ArrayListUnmanaged(types.AlgebraicAdaptiveProgress).empty;
            errdefer {
                for (out.items) |*progress| progress.deinit(alloc);
                out.deinit(alloc);
            }

            for (self.core.index_manager.algebraic_indexes.items) |*entry| {
                if (index_name) |filter| {
                    if (!std.mem.eql(u8, entry.config.name, filter)) continue;
                }
                const persisted = try entry.index.scanPersistedAdaptiveProgress(self.core.store);
                defer {
                    for (persisted) |*progress| progress.deinit(entry.index.alloc);
                    if (persisted.len > 0) entry.index.alloc.free(persisted);
                }
                for (persisted) |progress| {
                    const owned_index_name = try alloc.dupe(u8, entry.config.name);
                    errdefer alloc.free(owned_index_name);
                    const owned_recommendation = try alloc.dupe(u8, progress.recommendation);
                    errdefer alloc.free(owned_recommendation);
                    const owned_materialization_id = try alloc.dupe(u8, progress.materialization_id);
                    errdefer alloc.free(owned_materialization_id);
                    const owned_lifecycle = try alloc.dupe(u8, @tagName(progress.lifecycle));
                    errdefer alloc.free(owned_lifecycle);
                    try out.append(alloc, .{
                        .index_name = owned_index_name,
                        .recommendation = owned_recommendation,
                        .materialization_id = owned_materialization_id,
                        .lifecycle = owned_lifecycle,
                        .target_sequence = progress.target_sequence,
                        .applied_sequence = progress.applied_sequence,
                        .rows_processed = progress.rows_processed,
                        .target_rows = progress.target_rows,
                    });
                }
            }
            return try out.toOwnedSlice(alloc);
        }

        const StoredGeneratedReplayBatch = struct {
            writes: []types.BatchWrite = &.{},
            extracted: []mapper.ExtractedWrite = &.{},
            next_lower: ?[]u8 = null,

            fn deinit(self: *@This(), alloc: Allocator) void {
                for (self.writes) |write| {
                    alloc.free(@constCast(write.key));
                    alloc.free(@constCast(write.value));
                }
                if (self.writes.len > 0) alloc.free(self.writes);
                for (self.extracted) |*item| item.deinit(alloc);
                if (self.extracted.len > 0) alloc.free(self.extracted);
                if (self.next_lower) |buf| alloc.free(buf);
                self.* = .{};
            }
        };

        fn collectStoredGeneratedReplayBatch(
            self: *DB,
            alloc: Allocator,
            lower: []const u8,
            lower_exclusive: bool,
            limit: usize,
        ) !StoredGeneratedReplayBatch {
            const ScanState = struct {
                alloc: Allocator,
                relational_base_rows: bool,
                limit: usize,
                writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
                extracted: std.ArrayListUnmanaged(mapper.ExtractedWrite) = .empty,
                last_store_key: ?[]u8 = null,

                fn deinitPartial(state: *@This()) void {
                    for (state.writes.items) |write| {
                        state.alloc.free(@constCast(write.key));
                        state.alloc.free(@constCast(write.value));
                    }
                    state.writes.deinit(state.alloc);
                    for (state.extracted.items) |*item| item.deinit(state.alloc);
                    state.extracted.deinit(state.alloc);
                    if (state.last_store_key) |key| state.alloc.free(key);
                }

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (!db_internal.isBaseDocumentStoreKeyForMode(state.relational_base_rows, key)) return .@"continue";
                    const raw_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(state.alloc, key)) orelse return .@"continue";
                    errdefer state.alloc.free(raw_key);
                    const doc_json = if (state.relational_base_rows)
                        try mapper.materializeRelationalRowValueAlloc(state.alloc, value)
                    else
                        try mapper.materializeDocumentValueAlloc(state.alloc, value);
                    errdefer state.alloc.free(doc_json);
                    try state.writes.append(state.alloc, .{
                        .key = raw_key,
                        .value = doc_json,
                    });
                    var extracted = try mapper.extractWrite(state.alloc, raw_key, doc_json);
                    errdefer extracted.deinit(state.alloc);
                    const next_last_store_key = try state.alloc.dupe(u8, key);
                    errdefer state.alloc.free(next_last_store_key);
                    try state.extracted.append(state.alloc, extracted);
                    if (state.last_store_key) |last| state.alloc.free(last);
                    state.last_store_key = next_last_store_key;
                    if (state.writes.items.len >= state.limit) return .stop;
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = alloc,
                .relational_base_rows = self.relationalColumnsForStore() != null,
                .limit = limit,
            };
            errdefer state.deinitPartial();
            try self.core.store.scanWithContext(lower, "", .{ .lower_exclusive = lower_exclusive }, &state, ScanState.scanEntry);

            var next_lower: ?[]u8 = null;
            if (state.last_store_key) |last| {
                next_lower = try alloc.dupe(u8, last);
                alloc.free(last);
                state.last_store_key = null;
            }

            return .{
                .writes = try state.writes.toOwnedSlice(alloc),
                .extracted = try state.extracted.toOwnedSlice(alloc),
                .next_lower = next_lower,
            };
        }

        pub fn replayGeneratedEnrichmentsFromStoredDocs(self: *DB, alloc: Allocator) !usize {
            if (self.enrichment_runtime == null) return 0;

            const chunk_size: usize = 128;
            const initial_lower = try self.core.documentRangeLowerAlloc("");
            defer self.core.alloc.free(initial_lower);
            var lower = try alloc.dupe(u8, initial_lower);
            defer alloc.free(lower);
            var lower_exclusive = false;

            var generated_ref_count: usize = 0;
            var latest_sequence: u64 = 0;
            while (true) {
                var replay_batch = try Self.collectStoredGeneratedReplayBatch(self, alloc, lower, lower_exclusive, chunk_size);
                defer replay_batch.deinit(alloc);
                if (replay_batch.writes.len == 0) break;

                var pending_batch = derived_types.DerivedBatch{};
                defer derived_types.deinitDerivedBatch(alloc, &pending_batch);
                try DB.SchemaRuntimeCallbacks.append_generated_enrichments(self, &pending_batch, .{
                    .writes = replay_batch.writes,
                    .sync_level = .write,
                }, replay_batch.extracted, true);
                if (pending_batch.generated_enrichment_refs.len != 0) {
                    generated_ref_count += pending_batch.generated_enrichment_refs.len;
                    const sequence = try DB.SchemaRuntimeCallbacks.append_derived_batch_record(self, pending_batch);
                    latest_sequence = @max(latest_sequence, sequence);
                }

                const next_lower = replay_batch.next_lower orelse break;
                replay_batch.next_lower = null;
                alloc.free(lower);
                lower = next_lower;
                lower_exclusive = true;
            }
            if (latest_sequence != 0) {
                self.executor.notifySequence(latest_sequence);
                if (self.enrichment_runtime) |runtime| runtime.notifySequence(latest_sequence);
                DB.SchemaRuntimeCallbacks.notify_resolver_replay_runtimes(self, latest_sequence);
            }
            return generated_ref_count;
        }

        pub fn reprocessGeneratedEnrichmentFromStoredDocs(
            self: *DB,
            alloc: Allocator,
            artifact_name: ?[]const u8,
        ) !usize {
            if (self.enrichment_runtime == null) return 0;

            const chunk_size: usize = 128;
            const initial_lower = try self.core.documentRangeLowerAlloc("");
            defer self.core.alloc.free(initial_lower);
            var lower = try alloc.dupe(u8, initial_lower);
            defer alloc.free(lower);
            var lower_exclusive = false;

            const force_artifact = [_][]const u8{artifact_name orelse ""};
            const force_artifacts: []const []const u8 = if (artifact_name != null)
                force_artifact[0..]
            else
                &.{};
            var reprocessed: usize = 0;
            while (true) {
                var replay_batch = try Self.collectStoredGeneratedReplayBatch(self, alloc, lower, lower_exclusive, chunk_size);
                defer replay_batch.deinit(alloc);
                if (replay_batch.writes.len == 0) break;

                try DB.WritePathCallbacks.batch_internal(self, .{
                    .writes = replay_batch.writes,
                    .sync_level = .write,
                }, null, .{
                    .force_generated_artifact_names = force_artifacts,
                    .force_all_generated_artifacts = artifact_name == null,
                    .admission_prechecked = true,
                });
                reprocessed += replay_batch.writes.len;

                const next_lower = replay_batch.next_lower orelse break;
                replay_batch.next_lower = null;
                alloc.free(lower);
                lower = next_lower;
                lower_exclusive = true;
            }
            return reprocessed;
        }

        fn markEnrichmentAppliedIfNoPendingThrough(self: *DB, sequence: u64) !void {
            if (sequence == 0) return;
            const runtime = self.enrichment_runtime orelse return;
            const runtime_stats = runtime.stats();
            if (runtime_stats.applied_sequence >= sequence) return;
            if (try DB.SchemaRuntimeCallbacks.no_pending_enrichment_replay_through(self, runtime_stats.applied_sequence, sequence)) {
                try runtime.markAppliedThrough(sequence);
            }
        }

        pub fn addIndex(self: *DB, cfg: types.IndexConfig) !void {
            _ = try Self.addIndexWithAdmission(self, cfg, .ordinary);
        }

        pub fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) !void {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            try self.core.addEnrichment(cfg);
        }

        pub fn upsertEnrichment(self: *DB, cfg: types.EnrichmentConfig) !index_manager_mod.IndexManager.EnrichmentUpsertResult {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.upsertEnrichment(cfg);
        }

        pub fn getEnrichment(self: *DB, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) !?types.EnrichmentConfig {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.getEnrichment(alloc, kind, name);
        }

        pub fn listIndexes(self: *DB, alloc: Allocator) ![]types.IndexConfig {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.listIndexes(alloc);
        }

        pub fn listEnrichments(self: *DB, alloc: Allocator) ![]types.EnrichmentConfig {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.core.listEnrichments(alloc);
        }

        fn deleteIndexWhileEnrichmentQuiesced(self: *DB, name: []const u8) !bool {
            self.executor.removeWorker(name);
            self.core.lockApply();
            defer self.core.unlockApply();
            const repair_id = try DB.SchemaRuntimeCallbacks.prepare_index_repair_for_deletion(self, self.alloc, name);
            defer if (repair_id != null) DB.SchemaRuntimeCallbacks.end_index_repair_lease(self, name);
            const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(self.alloc, name);
            defer self.alloc.free(admission_key);
            const removed = try self.core.deleteManagedIndex(name, admission_key);
            if (removed) {
                if (builtin.is_test and test_fail_managed_index_delete_after_catalog_commit)
                    return error.TestManagedIndexDeleteCrash;
                self.core.index_manager.clearManagedAdmissionSnapshotForIndex(name);
                if (repair_id) |id| {
                    const repair_cleanup = if (builtin.is_test and test_fail_managed_index_repair_cleanup_after_catalog_commit)
                        error.TestPostCommitRepairCleanup
                    else
                        DB.SchemaRuntimeCallbacks.remove_index_repair_intent_and_pin(self, self.alloc, id);
                    repair_cleanup catch |err| {
                        std.log.warn("index removal committed with stale repair intent name={s} err={s}", .{ name, @errorName(err) });
                    };
                }
                DB.SchemaRuntimeCallbacks.delete_dense_artifact_counter_metadata_context(self.alloc, self.core.store, name) catch |err| {
                    std.log.warn("index removal committed with stale dense coverage counters name={s} err={s}", .{ name, @errorName(err) });
                };
                deleteDerivedCoverageForIndex(self.core.alloc, self.core.store, name) catch |err| {
                    std.log.warn("index removal committed with stale derived coverage counters name={s} err={s}", .{ name, @errorName(err) });
                };
            }
            return removed;
        }

        pub fn deleteIndex(self: *DB, name: []const u8) !bool {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            const restart_enrichment = DB.SchemaRuntimeCallbacks.quiesce_enrichment_for_structural_mutation(self);
            const removed = Self.deleteIndexWhileEnrichmentQuiesced(self, name) catch |delete_err| {
                if (restart_enrichment) Self.restartEnrichmentAfterStructuralMutation(self, "failed index deletion", name) catch |restart_err| {
                    std.log.err("failed to restore enrichment runtime after index deletion error index={s} err={s}", .{ name, @errorName(restart_err) });
                };
                return delete_err;
            };
            if (removed) DB.SchemaRuntimeCallbacks.schedule_generated_artifact_cleanup(self);
            if (restart_enrichment) Self.restartEnrichmentAfterStructuralMutation(self, "index deletion", name) catch |restart_err| {
                std.log.err("failed to restart enrichment runtime after committed index deletion index={s} err={s}", .{ name, @errorName(restart_err) });
            };
            return removed;
        }

        pub fn deleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8) !bool {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.deleteEnrichment(kind, name);
        }

        pub fn validateTableSchemaCompatibilityLocked(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            if (self.core.schema) |current_schema| {
                return validateRuntimeTableSchemaTransition(current_schema, next_schema);
            }

            const durable_schema = try schema_mod.loadSchema(self.core.store, alloc);
            defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
            if (durable_schema) |current_schema| {
                return validateRuntimeTableSchemaTransition(current_schema, next_schema);
            }

            try Self.validateFirstTableSchemaApplyAgainstExistingRows(self, alloc, next_schema);
        }

        pub fn migrateRelationalRowsForSchemaTransitionLocked(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            if (self.core.schema) |current_schema| {
                return try Self.migrateRelationalRowsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }

            const durable_schema = try schema_mod.loadSchema(self.core.store, alloc);
            defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
            if (durable_schema) |current_schema| {
                return try Self.migrateRelationalRowsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }
        }

        pub fn migrateRelationalConstraintsForSchemaTransitionLocked(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            if (self.core.schema) |current_schema| {
                return try Self.migrateRelationalConstraintsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }

            const durable_schema = try schema_mod.loadSchema(self.core.store, alloc);
            defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
            if (durable_schema) |current_schema| {
                return try Self.migrateRelationalConstraintsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }
        }

        fn migrateRelationalRowsFromCurrentLocked(
            self: *DB,
            alloc: Allocator,
            current_schema: schema_mod.TableSchema,
            next_schema: schema_mod.TableSchema,
        ) !void {
            if (current_schema.storage_mode != .relational or next_schema.storage_mode != .relational) return;

            var sets = std.ArrayListUnmanaged(relational_store_mod.RowRewriteSet).empty;
            defer sets.deinit(alloc);
            var owned_rows = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_rows.items) |row| alloc.free(row);
                owned_rows.deinit(alloc);
            }
            var decoded_rows = std.ArrayListUnmanaged(relational_row_codec.Row).empty;
            defer {
                for (decoded_rows.items) |*row| row.deinit(alloc);
                decoded_rows.deinit(alloc);
            }

            var generated_columns = std.ArrayListUnmanaged(schema_mod.RelationalColumn).empty;
            defer generated_columns.deinit(alloc);

            for (next_schema.relational_columns) |column| {
                if (relationalColumnPathExists(current_schema.relational_columns, column.path)) continue;
                if (try relationalColumnHasUniqueDroppedRenameSource(current_schema.relational_columns, next_schema.relational_columns, column)) continue;
                if (column.generated != null) {
                    try generated_columns.append(alloc, column);
                    continue;
                }
                const default_value = column.default_value orelse continue;
                if (default_value.kind != .literal) return error.InvalidSchemaUpdateRequest;
                if (column.nullable and jsonLiteralIsNull(default_value.value_json)) continue;
                const row_value = try relationalDefaultColumnRowValueAlloc(alloc, column, default_value.value_json);
                errdefer alloc.free(row_value);
                try owned_rows.append(alloc, row_value);
                var decoded = try relational_row_codec.deserialize(alloc, row_value);
                errdefer decoded.deinit(alloc);
                if (decoded.cells.len != 1) return error.InvalidSchemaUpdateRequest;
                try decoded_rows.append(alloc, decoded);
                const stable_decoded = &decoded_rows.items[decoded_rows.items.len - 1];
                try sets.append(alloc, .{
                    .cell = stable_decoded.cells[0],
                    .only_if_missing = true,
                });
            }

            const column_index_policy = relational_store_mod.ColumnIndexPolicy.fromSchema(next_schema);
            if (sets.items.len != 0) {
                _ = try relational_store_mod.rewriteRowsInRangeWithColumnIndexPolicy(
                    alloc,
                    self.core.store,
                    .{ .sets = sets.items },
                    self.getRange().start,
                    self.getRange().end,
                    column_index_policy,
                );
            }
            if (generated_columns.items.len != 0) {
                try Self.backfillRelationalGeneratedColumnsLocked(self, alloc, generated_columns.items, column_index_policy);
            }
        }

        fn backfillRelationalGeneratedColumnsLocked(
            self: *DB,
            alloc: Allocator,
            generated_columns: []const schema_mod.RelationalColumn,
            column_index_policy: relational_store_mod.ColumnIndexPolicy,
        ) !void {
            const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, self.getRange().start, self.getRange().end);
            defer relational_store_mod.freeRows(alloc, rows);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer writes.deinit(alloc);
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(alloc);
            var owned_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_keys.items) |key| alloc.free(key);
                owned_keys.deinit(alloc);
            }
            var owned_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_values.items) |value| alloc.free(value);
                owned_values.deinit(alloc);
            }

            for (rows) |row| {
                const row_json = mapper.materializeRelationalRowValueAlloc(alloc, row.row_value) catch return error.InvalidRowsRequest;
                defer alloc.free(row_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;

                var row_sets = std.ArrayListUnmanaged(relational_store_mod.RowRewriteSet).empty;
                defer row_sets.deinit(alloc);
                var row_owned_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (row_owned_values.items) |value| alloc.free(value);
                    row_owned_values.deinit(alloc);
                }
                var row_decoded = std.ArrayListUnmanaged(relational_row_codec.Row).empty;
                defer {
                    for (row_decoded.items) |*decoded| decoded.deinit(alloc);
                    row_decoded.deinit(alloc);
                }

                for (generated_columns) |column| {
                    const generated = column.generated orelse continue;
                    const value_json = self.relationalRowsGeneratedColumnValueJsonAlloc(alloc, parsed.value, generated) catch return error.InvalidRowsRequest;
                    defer alloc.free(value_json);
                    const generated_row = try relationalDefaultColumnRowValueAlloc(alloc, column, value_json);
                    errdefer alloc.free(generated_row);
                    try row_owned_values.append(alloc, generated_row);
                    var decoded = try relational_row_codec.deserialize(alloc, generated_row);
                    errdefer decoded.deinit(alloc);
                    if (decoded.cells.len != 1) return error.InvalidSchemaUpdateRequest;
                    try row_decoded.append(alloc, decoded);
                    const stable_decoded = &row_decoded.items[row_decoded.items.len - 1];
                    try row_sets.append(alloc, .{
                        .cell = stable_decoded.cells[0],
                        .only_if_missing = true,
                    });
                }

                const rewritten = try relational_store_mod.rewriteRowValueWithPlanAlloc(alloc, row.row_value, .{ .sets = row_sets.items });
                if (rewritten) |new_row| {
                    var new_row_owned = true;
                    errdefer if (new_row_owned) alloc.free(new_row);
                    try relational_store_mod.appendUpsertWithColumnIndexPolicy(alloc, self.core.store, &writes, &deletes, &owned_keys, &owned_values, row.doc_key, new_row, column_index_policy);
                    try owned_values.append(alloc, new_row);
                    new_row_owned = false;
                }
            }

            if (writes.items.len > 0 or deletes.items.len > 0) try self.core.store.putBatch(writes.items, deletes.items);
        }

        fn migrateRelationalConstraintsFromCurrentLocked(
            self: *DB,
            alloc: Allocator,
            current_schema: schema_mod.TableSchema,
            next_schema: schema_mod.TableSchema,
        ) !void {
            if (current_schema.storage_mode != .relational or next_schema.storage_mode != .relational) return;

            var unique_to_build = std.ArrayListUnmanaged(schema_mod.UniqueConstraint).empty;
            defer unique_to_build.deinit(alloc);
            var unique_to_delete = std.ArrayListUnmanaged(schema_mod.UniqueConstraint).empty;
            defer unique_to_delete.deinit(alloc);
            var foreign_keys_to_build = std.ArrayListUnmanaged(schema_mod.ForeignKey).empty;
            defer foreign_keys_to_build.deinit(alloc);
            var foreign_keys_to_delete = std.ArrayListUnmanaged(schema_mod.ForeignKey).empty;
            defer foreign_keys_to_delete.deinit(alloc);
            var checks_to_validate = std.ArrayListUnmanaged(schema_mod.RelationalCheck).empty;
            defer checks_to_validate.deinit(alloc);

            for (next_schema.unique_constraints) |constraint| {
                if (findUniqueConstraintByName(current_schema.unique_constraints, constraint.name)) |current_constraint| {
                    if (current_constraint.validation_state != .enforced and constraint.validation_state == .enforced) {
                        try unique_to_build.append(alloc, constraint);
                    }
                } else if (constraint.validation_state == .enforced) {
                    try unique_to_build.append(alloc, constraint);
                }
            }
            for (current_schema.unique_constraints) |constraint| {
                if (findUniqueConstraintByName(next_schema.unique_constraints, constraint.name) == null) {
                    try unique_to_delete.append(alloc, constraint);
                }
            }
            for (next_schema.foreign_keys) |foreign_key| {
                if (findForeignKeyByName(current_schema.foreign_keys, foreign_key.name)) |current_foreign_key| {
                    if (current_foreign_key.validation_state != .enforced and foreign_key.validation_state == .enforced) {
                        try foreign_keys_to_build.append(alloc, foreign_key);
                    }
                } else if (foreign_key.validation_state == .enforced) {
                    try foreign_keys_to_build.append(alloc, foreign_key);
                }
            }
            for (current_schema.foreign_keys) |foreign_key| {
                if (findForeignKeyByName(next_schema.foreign_keys, foreign_key.name) == null) {
                    try foreign_keys_to_delete.append(alloc, foreign_key);
                }
            }
            for (next_schema.checks) |check| {
                if (findRelationalCheckByName(current_schema.checks, check.name)) |current_check| {
                    if (current_check.validation_state != .enforced and check.validation_state == .enforced) {
                        try checks_to_validate.append(alloc, check);
                    }
                } else if (check.validation_state == .enforced) {
                    try checks_to_validate.append(alloc, check);
                }
            }

            if (unique_to_build.items.len > 0) {
                try relational_store_mod.rebuildUniqueConstraintRowsInRange(
                    alloc,
                    self.core.store,
                    next_schema.relational_columns,
                    next_schema.periods,
                    unique_to_build.items,
                    next_schema.relational_indexes,
                    self.getRange().start,
                    self.getRange().end,
                );
                _ = try relational_store_mod.repairColumnBackedIndexesFromRowsInRangeWithColumnIndexPolicy(
                    alloc,
                    self.core.store,
                    self.getRange().start,
                    self.getRange().end,
                    relational_store_mod.ColumnIndexPolicy.fromSchema(next_schema),
                );
                try relational_store_mod.rebuildOrderedTupleUniqueConstraintsFromRowsInRange(
                    alloc,
                    self.core.store,
                    next_schema.relational_columns,
                    next_schema.unique_constraints,
                    next_schema.relational_indexes,
                    self.getRange().start,
                    self.getRange().end,
                );
            }
            if (foreign_keys_to_build.items.len > 0) {
                const validate_report = try relational_store_mod.reconcileForeignKeyRefsInRangeWithPrimaryKeyAndIndexes(
                    alloc,
                    self.core.store,
                    next_schema.default_type,
                    next_schema.relational_columns,
                    next_schema.periods,
                    foreign_keys_to_build.items,
                    next_schema.primary_key,
                    next_schema.unique_constraints,
                    next_schema.relational_indexes,
                    self.getRange().start,
                    self.getRange().end,
                    .validate,
                );
                try Self.recordForeignKeyIntegrityProgressLocked(self, alloc, .validate, null, self.getRange().start, self.getRange().end, validate_report);
                if (validate_report.missing_parent_rows != 0) return error.ForeignKeyViolation;

                const repair_report = try relational_store_mod.reconcileForeignKeyRefsInRangeWithPrimaryKeyAndIndexes(
                    alloc,
                    self.core.store,
                    next_schema.default_type,
                    next_schema.relational_columns,
                    next_schema.periods,
                    foreign_keys_to_build.items,
                    next_schema.primary_key,
                    next_schema.unique_constraints,
                    next_schema.relational_indexes,
                    self.getRange().start,
                    self.getRange().end,
                    .repair,
                );
                try Self.recordForeignKeyIntegrityProgressLocked(self, alloc, .repair, null, self.getRange().start, self.getRange().end, repair_report);
                if (repair_report.missing_parent_rows != 0) return error.ForeignKeyViolation;
            }
            if (checks_to_validate.items.len > 0) {
                try Self.validateRelationalChecksInRangeLocked(
                    self,
                    alloc,
                    checks_to_validate.items,
                    self.getRange().start,
                    self.getRange().end,
                );
            }
            if (foreign_keys_to_delete.items.len > 0) {
                try relational_store_mod.deleteForeignKeyRefRows(alloc, self.core.store, foreign_keys_to_delete.items);
            }
            if (unique_to_delete.items.len > 0) {
                try relational_store_mod.deleteUniqueConstraintRows(alloc, self.core.store, unique_to_delete.items);
            }
        }

        fn schemaRewriteRowValueAlloc(
            self: *DB,
            alloc: Allocator,
            row_value: []const u8,
            target_column: schema_mod.RelationalColumn,
            expression: schema_mod.RelationalRowsExpression,
        ) !?[]u8 {
            const row_json = mapper.materializeRelationalRowValueAlloc(alloc, row_value) catch return error.InvalidRowsRequest;
            defer alloc.free(row_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidRowsRequest;

            const value_json = self.relationalRowsExpressionValueJsonAlloc(alloc, parsed.value, expression) catch return error.InvalidRowsRequest;
            defer alloc.free(value_json);
            var parsed_value = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed_value.deinit();

            if (parsed_value.value == .null) {
                if (!target_column.nullable) return error.InvalidRowsRequest;
                return try relational_store_mod.rewriteRowValueWithPlanAlloc(alloc, row_value, .{ .drops = &.{target_column.path} });
            }

            const target_row = try relationalDefaultColumnRowValueAlloc(alloc, target_column, value_json);
            defer alloc.free(target_row);
            var decoded = try relational_row_codec.deserialize(alloc, target_row);
            defer decoded.deinit(alloc);
            if (decoded.cells.len != 1) return error.InvalidRowsRequest;
            return try relational_store_mod.rewriteRowValueWithPlanAlloc(alloc, row_value, .{ .sets = &.{.{ .cell = decoded.cells[0] }} });
        }

        fn validateFirstTableSchemaApplyAgainstExistingRows(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            const existing_mode = try detectExistingPhysicalStorageMode(alloc, self.core.store);
            switch (existing_mode) {
                .empty => return,
                .document => if (next_schema.storage_mode != .document) return error.InvalidSchemaUpdateRequest,
                .relational => if (next_schema.storage_mode != .relational) return error.InvalidSchemaUpdateRequest,
                .mixed => return error.InvalidSchemaUpdateRequest,
            }
        }

        pub fn drainSchemaRewriteJobsForIdle(
            self: *DB,
            alloc: Allocator,
            service: anytype,
            options: SchemaRewriteJobDrainOptions,
        ) !usize {
            if (options.max_jobs == 0) return 0;
            const ServiceType = @TypeOf(service);
            const ServiceDeclType = switch (@typeInfo(ServiceType)) {
                .pointer => |pointer| pointer.child,
                else => ServiceType,
            };
            if (comptime !@hasDecl(ServiceDeclType, "listProjectedSchemaRewriteJobs") or
                !@hasDecl(ServiceDeclType, "beginSchemaRewriteJob") or
                !@hasDecl(ServiceDeclType, "finishSchemaRewriteJob") or
                !@hasDecl(ServiceDeclType, "invalidateSchemaRewriteJob"))
            {
                return error.UnsupportedOperation;
            }

            const now_ms = options.now_ms orelse @divTrunc(Self.currentTimeNs(), std.time.ns_per_ms);
            const lease_expires_at_ms = now_ms +| options.lease_ttl_ms;
            if (options.worker_id.len == 0 or lease_expires_at_ms <= now_ms) return error.InvalidSchemaRewriteJobLease;

            const jobs = try service.listProjectedSchemaRewriteJobs(alloc);
            defer if (comptime @hasDecl(ServiceDeclType, "freeProjectedSchemaRewriteJobs")) {
                service.freeProjectedSchemaRewriteJobs(alloc, jobs);
            } else {
                for (jobs) |job| metadata_table_manager.freeSchemaRewriteJob(alloc, job);
                alloc.free(jobs);
            };

            var progressed: usize = 0;
            for (jobs) |job| {
                if (progressed >= options.max_jobs) break;
                if (options.group_id) |group_id| {
                    if (job.group_id != group_id) continue;
                }
                if (!schemaRewriteJobClaimableForDrain(job, now_ms)) continue;

                service.beginSchemaRewriteJob(.{
                    .job_id = job.job_id,
                    .lease_owner = options.worker_id,
                    .now_ms = now_ms,
                    .lease_expires_at_ms = lease_expires_at_ms,
                }) catch |err| switch (err) {
                    error.SchemaRewriteJobClaimBusy,
                    error.SchemaRewriteJobNotDeclared,
                    error.UnknownSchemaRewriteJob,
                    => continue,
                    else => return err,
                };

                var running_job = job;
                running_job.state = metadata_table_manager.schema_rewrite_running;
                running_job.lease_owner = options.worker_id;
                running_job.lease_expires_at_ms = lease_expires_at_ms;

                var result = Self.executeClaimedSchemaRewriteJob(self, alloc, running_job) catch |err| {
                    try service.invalidateSchemaRewriteJob(.{
                        .job_id = running_job.job_id,
                        .lease_owner = options.worker_id,
                        .last_error = @errorName(err),
                    });
                    progressed += 1;
                    continue;
                };
                defer result.deinit(alloc);
                try service.finishSchemaRewriteJob(result.finishRequest(running_job));
                progressed += 1;
            }
            return progressed;
        }

        pub fn executeClaimedSchemaRewriteJob(
            self: *DB,
            alloc: Allocator,
            job: metadata_table_manager.SchemaRewriteJobRecord,
        ) !SchemaRewriteJobExecutionResult {
            const validate_constraints = std.mem.eql(u8, job.action, "validate") and std.mem.eql(u8, job.reason, "constraints");
            const rewrite_rows = std.mem.eql(u8, job.action, "rewrite") and std.mem.eql(u8, job.reason, "row_images");
            if (!validate_constraints and !rewrite_rows) return error.InvalidSchemaRewriteJob;
            if (!std.mem.eql(u8, job.state, metadata_table_manager.schema_rewrite_running)) return error.SchemaRewriteJobNotRunning;
            if (job.lease_owner.len == 0) return error.SchemaRewriteJobLeaseMismatch;
            const has_expression_rewrite = job.expression != null or job.target_column.len != 0;
            const has_row_rewrite = job.rewrite_renames.len != 0 or job.rewrite_drops.len != 0;
            const has_full_row_rewrite = job.full_row_rewrite;
            if (validate_constraints) {
                if (has_expression_rewrite or has_row_rewrite or has_full_row_rewrite) return error.InvalidSchemaRewriteJob;
            } else if (has_expression_rewrite) {
                if (job.target_column.len == 0 or job.expression == null) return error.InvalidSchemaRewriteExpression;
                if (has_row_rewrite or has_full_row_rewrite) return error.InvalidSchemaRewriteExpression;
            } else if (has_row_rewrite and has_full_row_rewrite) {
                return error.InvalidSchemaRewriteExpression;
            } else if (!has_row_rewrite and !has_full_row_rewrite) {
                return error.InvalidSchemaRewriteExpression;
            }
            const local_range = self.getRange();
            if (!std.mem.eql(u8, job.start_row_key, local_range.start) or !optionalStringsEqual(job.end_row_key, if (local_range.end.len == 0) null else local_range.end)) {
                return error.SchemaRewriteJobRangeMismatch;
            }

            self.core.lockApply();
            defer self.core.unlockApply();

            const local_schema_json = self.core.store.get(alloc, local_schema_json_key) catch |err| switch (err) {
                error.NotFound => return error.InvalidSchemaRewriteGeneration,
                else => return err,
            };
            defer alloc.free(local_schema_json);
            const current_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(local_schema_json);
            if (job.schema_generation != current_generation) return error.InvalidSchemaRewriteGeneration;

            const runtime_schema = self.core.schema orelse return error.InvalidSchemaUpdateRequest;
            if (runtime_schema.storage_mode != .relational or runtime_schema.relational_columns.len == 0) return error.InvalidSchemaUpdateRequest;
            const column_index_policy = relational_store_mod.ColumnIndexPolicy.fromSchema(runtime_schema);

            if (validate_constraints) {
                const report = try Self.validateRelationalSchemaConstraintsForJobLocked(self, alloc, runtime_schema, local_range.start, local_range.end);
                const progress_row_key = try alloc.dupe(u8, local_range.end);
                return .{ .report = report, .progress_row_key = progress_row_key };
            }

            if (has_expression_rewrite) {
                const expression = job.expression.?;
                const target_column = relationalRowsFindColumn(runtime_schema.relational_columns, job.target_column) orelse return error.InvalidSchemaRewriteExpression;
                try self.validateRelationalRowsExpressionAgainstSchema(runtime_schema, expression);

                const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, local_range.start, local_range.end);
                defer relational_store_mod.freeRows(alloc, rows);

                var report: relational_store_mod.RowRewriteReport = .{ .scanned_rows = @intCast(rows.len) };
                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer writes.deinit(alloc);
                var deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer deletes.deinit(alloc);
                var owned_keys = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_keys.items) |key| alloc.free(key);
                    owned_keys.deinit(alloc);
                }
                var owned_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_values.items) |value| alloc.free(value);
                    owned_values.deinit(alloc);
                }

                for (rows) |row| {
                    const rewritten = try Self.schemaRewriteRowValueAlloc(self, alloc, row.row_value, target_column, expression);
                    if (rewritten) |new_row| {
                        var new_row_owned = true;
                        errdefer if (new_row_owned) alloc.free(new_row);
                        try relational_store_mod.appendUpsertWithColumnIndexPolicy(
                            alloc,
                            self.core.store,
                            &writes,
                            &deletes,
                            &owned_keys,
                            &owned_values,
                            row.doc_key,
                            new_row,
                            column_index_policy,
                        );
                        try owned_values.append(alloc, new_row);
                        new_row_owned = false;
                        report.rewritten_rows += 1;
                    } else {
                        report.unchanged_rows += 1;
                    }
                }

                if (writes.items.len > 0 or deletes.items.len > 0) try self.core.store.putBatch(writes.items, deletes.items);
                const progress_row_key = try alloc.dupe(u8, local_range.end);
                return .{ .report = report, .progress_row_key = progress_row_key };
            }

            if (has_full_row_rewrite) {
                const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, local_range.start, local_range.end);
                defer relational_store_mod.freeRows(alloc, rows);

                var report: relational_store_mod.RowRewriteReport = .{ .scanned_rows = @intCast(rows.len) };
                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer writes.deinit(alloc);
                var deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer deletes.deinit(alloc);
                var owned_keys = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_keys.items) |key| alloc.free(key);
                    owned_keys.deinit(alloc);
                }
                var owned_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_values.items) |value| alloc.free(value);
                    owned_values.deinit(alloc);
                }

                for (rows) |row| {
                    const new_row = try alloc.dupe(u8, row.row_value);
                    var new_row_owned = true;
                    errdefer if (new_row_owned) alloc.free(new_row);
                    try relational_store_mod.appendUpsertWithColumnIndexPolicy(
                        alloc,
                        self.core.store,
                        &writes,
                        &deletes,
                        &owned_keys,
                        &owned_values,
                        row.doc_key,
                        new_row,
                        column_index_policy,
                    );
                    try relational_store_mod.appendColumnBackedIndexWritesForRowWithColumnIndexPolicy(
                        alloc,
                        &writes,
                        &owned_keys,
                        &owned_values,
                        row.doc_key,
                        new_row,
                        column_index_policy,
                    );
                    try owned_values.append(alloc, new_row);
                    new_row_owned = false;
                    report.rewritten_rows += 1;
                }

                if (writes.items.len > 0 or deletes.items.len > 0) try self.core.store.putBatch(writes.items, deletes.items);
                const progress_row_key = try alloc.dupe(u8, local_range.end);
                return .{ .report = report, .progress_row_key = progress_row_key };
            }

            const renames = try alloc.alloc(relational_store_mod.RowRewriteRename, job.rewrite_renames.len);
            defer alloc.free(renames);
            for (job.rewrite_renames, 0..) |rename, i| {
                renames[i] = .{ .old_path = rename.old_path, .new_path = rename.new_path };
            }
            const plan = relational_store_mod.RowRewritePlan{
                .renames = renames,
                .drops = job.rewrite_drops,
            };
            const report = try relational_store_mod.rewriteRowsInRangeWithColumnIndexPolicy(
                alloc,
                self.core.store,
                plan,
                local_range.start,
                local_range.end,
                column_index_policy,
            );
            const progress_row_key = try alloc.dupe(u8, local_range.end);
            return .{ .report = report, .progress_row_key = progress_row_key };
        }

        pub fn validateRelationalSchemaConstraintsForJobLocked(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !relational_store_mod.RowRewriteReport {
            if (runtime_schema.storage_mode != .relational) return error.InvalidSchemaUpdateRequest;

            const unique_report = try self.relationalIntegrityReconcileUniqueConstraintRowsInRangeLocked(
                lower_doc_key,
                upper_doc_key,
                .validate,
            );
            if (!unique_report.valid()) return error.UniqueConstraintViolation;

            const foreign_key_report = try self.relationalIntegrityReconcileForeignKeyRefsInRangeLocked(
                null,
                lower_doc_key,
                upper_doc_key,
                .validate,
            );
            if (!foreign_key_report.valid()) return error.ForeignKeyViolation;

            var checks_to_validate = std.ArrayListUnmanaged(schema_mod.RelationalCheck).empty;
            defer checks_to_validate.deinit(alloc);
            for (runtime_schema.checks) |check| {
                if (check.validation_state == .enforced) try checks_to_validate.append(alloc, check);
            }
            try Self.validateRelationalChecksInRangeLocked(
                self,
                alloc,
                checks_to_validate.items,
                lower_doc_key,
                upper_doc_key,
            );

            const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, lower_doc_key, upper_doc_key);
            defer relational_store_mod.freeRows(alloc, rows);
            return .{
                .scanned_rows = @intCast(rows.len),
                .unchanged_rows = @intCast(rows.len),
            };
        }

        pub fn validateRelationalChecksInRangeLocked(
            self: *DB,
            alloc: Allocator,
            checks: []const schema_mod.RelationalCheck,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !void {
            if (checks.len == 0) return;
            const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, lower_doc_key, upper_doc_key);
            defer relational_store_mod.freeRows(alloc, rows);

            for (rows) |row| {
                const row_json = mapper.materializeRelationalRowValueAlloc(alloc, row.row_value) catch return error.InvalidRowsRequest;
                defer alloc.free(row_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                for (checks) |check| {
                    const passes = Self.relationalCheckPassesExistingRow(self, alloc, parsed.value, check) catch |err| switch (err) {
                        error.InvalidQueryRequest => return error.InvalidRowsRequest,
                        else => return err,
                    };
                    if (!passes) {
                        return error.InvalidRowsRequest;
                    }
                }
            }
        }

        fn relationalCheckPassesExistingRow(
            self: *DB,
            alloc: Allocator,
            row: std.json.Value,
            check: schema_mod.RelationalCheck,
        ) !bool {
            if (check.expression) |condition| return try self.relationalRowsExpressionConditionMatches(alloc, row, condition);
            return try self.relationalRowsQueryPredicatePasses(alloc, row, check);
        }

        pub fn validateRuntimeSchemaFeatureLevel(table_schema: schema_mod.TableSchema) !void {
            if (table_schema.storage_mode != .relational) return;
            for (table_schema.foreign_keys) |foreign_key| {
                if (foreign_key.validation_state == .validating or foreign_key.validation_state == .invalid) return error.InvalidSchemaUpdateRequest;
            }
        }

        pub fn clearDenseHbcCaches(self: *DB) void {
            self.core.index_manager.clearDenseHbcCaches();
        }

        pub fn relationalColumnsForStore(self: *DB) ?[]const schema_mod.RelationalColumn {
            const schema = self.core.schema orelse return null;
            if (schema.storage_mode != .relational) return null;
            return schema.relational_columns;
        }

        pub fn relationalIndexesForStore(self: *DB) []const schema_mod.RelationalIndex {
            const schema = self.core.schema orelse return &.{};
            if (schema.storage_mode != .relational) return &.{};
            return schema.relational_indexes;
        }

        pub fn relationalColumnIndexPolicyForStore(self: *DB) relational_store_mod.ColumnIndexPolicy {
            const schema = self.core.schema orelse return relational_store_mod.ColumnIndexPolicy.empty();
            if (schema.storage_mode != .relational) return relational_store_mod.ColumnIndexPolicy.empty();
            return relational_store_mod.ColumnIndexPolicy.fromSchema(schema);
        }

        pub fn saveIndexStatusSnapshot(self: *DB, index_name: []const u8, sequence: u64) !void {
            return try db_internal.saveIndexStatusSnapshot(self.alloc, self.core.store, self.core.index_manager, index_name, sequence);
        }

        pub fn rebuildRelationalSecondaryIndexInRange(
            self: *DB,
            index_name: []const u8,
            index_generation: u64,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !relational_store_mod.SecondaryIndexRebuildReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try relational_store_mod.rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                index_name,
                index_generation,
                lower_doc_key,
                upper_doc_key,
                Self.relationalColumnIndexPolicyForStore(self),
            );
        }

        pub fn rebuildRelationalSecondaryIndexPageInRange(
            self: *DB,
            index_name: []const u8,
            index_generation: u64,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            max_rows: usize,
        ) !relational_store_mod.SecondaryIndexRebuildPage {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try relational_store_mod.rebuildColumnIndexFromRowsInSpanPageWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                index_name,
                index_generation,
                lower_doc_key,
                upper_doc_key,
                Self.relationalColumnIndexPolicyForStore(self),
                max_rows,
            );
        }

        pub fn repairRelationalColumnBackedIndexesInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !relational_store_mod.ColumnBackedIndexRepairReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try relational_store_mod.repairColumnBackedIndexesFromRowsInRangeWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                lower_doc_key,
                upper_doc_key,
                Self.relationalColumnIndexPolicyForStore(self),
            );
        }

        pub fn hasIndex(self: *DB, name: []const u8) bool {
            return self.core.hasIndex(name);
        }

        pub fn refreshRuntimeSideEffects(self: *DB) void {
            const maybe_relational_columns = Self.relationalColumnsForStore(self);
            const relational_columns = maybe_relational_columns orelse &.{};
            const relational_base_rows = maybe_relational_columns != null;
            self.async_context.relational_base_rows = relational_base_rows;
            if (self.ttl_cleanup_context) |ctx| ctx.batch.relational_base_rows = relational_base_rows;
            if (self.enrichment_runtime) |runtime| runtime.setRelationalBaseRows(relational_base_rows);
            if (self.transaction_recovery_identity_context) |ctx| {
                ctx.relational_base_rows = relational_base_rows;
                ctx.relational_columns = relational_columns;
            }
        }
    };
}

fn schemaRewriteJobClaimableForDrain(job: metadata_table_manager.SchemaRewriteJobRecord, now_ms: u64) bool {
    if (std.mem.eql(u8, job.state, metadata_table_manager.schema_rewrite_declared)) return true;
    return std.mem.eql(u8, job.state, metadata_table_manager.schema_rewrite_running) and
        job.lease_expires_at_ms != 0 and
        job.lease_expires_at_ms <= now_ms;
}

fn validateRuntimeTableSchemaTransition(current_schema: schema_mod.TableSchema, next_schema: schema_mod.TableSchema) !void {
    if (current_schema.storage_mode != next_schema.storage_mode) return error.InvalidSchemaUpdateRequest;
    if (next_schema.storage_mode != .relational) return;
    try validateRelationalColumnCatalogTransition(current_schema.relational_columns, next_schema.relational_columns);
    if (!schema_mod.primaryKeyCatalogsEqual(current_schema.primary_key, next_schema.primary_key)) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (!schema_mod.relationalPeriodCatalogsEqual(current_schema.periods, next_schema.periods)) {
        return error.InvalidSchemaUpdateRequest;
    }
    try validateConstraintCatalogTransition(current_schema, next_schema);
}

fn validateRelationalColumnCatalogTransition(current_columns: []const schema_mod.RelationalColumn, next_columns: []const schema_mod.RelationalColumn) !void {
    for (next_columns) |column| {
        if (findRelationalColumnByPath(current_columns, column.path)) |current_column| {
            if (!schema_mod.relationalColumnCatalogsEqual(&.{current_column.*}, &.{column})) {
                return error.InvalidSchemaUpdateRequest;
            }
            continue;
        }
        if (try relationalColumnHasUniqueDroppedRenameSource(current_columns, next_columns, column)) continue;
        try validateNewRelationalColumnTransition(current_columns, next_columns, column);
    }
}

fn findRelationalColumnByPath(columns: []const schema_mod.RelationalColumn, path: []const u8) ?*const schema_mod.RelationalColumn {
    for (columns) |*column| {
        if (std.mem.eql(u8, column.path, path)) return column;
    }
    return null;
}

fn relationalColumnPathExists(columns: []const schema_mod.RelationalColumn, path: []const u8) bool {
    return findRelationalColumnByPath(columns, path) != null;
}

fn relationalColumnHasUniqueDroppedRenameSource(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    target_column: schema_mod.RelationalColumn,
) !bool {
    var source_index: ?usize = null;
    for (current_columns, 0..) |current_column, index| {
        if (relationalColumnPathExists(next_columns, current_column.path)) continue;
        if (!schema_mod.relationalColumnDefinitionsEqual(current_column, target_column)) continue;
        if (source_index != null) return error.InvalidSchemaUpdateRequest;
        source_index = index;
    }
    const matched_source_index = source_index orelse return false;

    var use_count: usize = 0;
    for (next_columns) |next_column| {
        if (relationalColumnPathExists(current_columns, next_column.path)) continue;
        if (schema_mod.relationalColumnDefinitionsEqual(current_columns[matched_source_index], next_column)) {
            use_count += 1;
        }
    }
    if (use_count != 1) return error.InvalidSchemaUpdateRequest;
    return true;
}

fn validateNewRelationalColumnTransition(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    column: schema_mod.RelationalColumn,
) !void {
    if (column.generated) |generated| {
        try validateAppendedGeneratedColumnTransition(current_columns, next_columns, generated);
        return;
    }
    if (column.default_value) |default_value| {
        if (default_value.kind != .literal) return error.InvalidSchemaUpdateRequest;
        return;
    }
    if (!column.nullable) return error.InvalidSchemaUpdateRequest;
}

fn relationalColumnMatchesGeneratedSource(column: schema_mod.RelationalColumn, field: []const u8, normalized: []const u8) bool {
    return std.mem.eql(u8, field, column.name) or
        std.mem.eql(u8, field, column.path) or
        std.mem.eql(u8, normalized, column.name) or
        std.mem.eql(u8, normalized, column.path);
}

fn validateGeneratedColumnBackfillSource(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    field: []const u8,
) !void {
    const normalized = if (std.mem.startsWith(u8, field, "/")) field[1..] else field;
    for (current_columns) |column| {
        if (relationalColumnMatchesGeneratedSource(column, field, normalized)) {
            return;
        }
    }
    for (next_columns) |column| {
        if (relationalColumnPathExists(current_columns, column.path)) continue;
        if (!relationalColumnMatchesGeneratedSource(column, field, normalized)) continue;
        if (column.generated != null) return error.InvalidSchemaUpdateRequest;
        const default_value = column.default_value orelse return error.InvalidSchemaUpdateRequest;
        if (default_value.kind != .literal) return error.InvalidSchemaUpdateRequest;
        if (jsonLiteralIsNull(default_value.value_json)) return error.InvalidSchemaUpdateRequest;
        return;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn validateAppendedGeneratedColumnTransition(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    generated: schema_mod.RelationalGeneratedValue,
) !void {
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const field = generated.field orelse return error.InvalidSchemaUpdateRequest;
            try validateGeneratedColumnBackfillSource(current_columns, next_columns, field);
        },
        .concat, .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSchemaUpdateRequest;
            for (generated.fields) |field| {
                try validateGeneratedColumnBackfillSource(current_columns, next_columns, field);
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSchemaUpdateRequest;
            try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, expression);
        },
    }
}

fn validateGeneratedColumnExpressionBackfillSources(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    expression: schema_mod.RelationalRowsExpression,
) error{InvalidSchemaUpdateRequest}!void {
    if (expression.kind == .field) {
        try validateGeneratedColumnBackfillSource(current_columns, next_columns, expression.field);
    }
    for (expression.operands) |operand| try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, operand);
    for (expression.case_branches) |branch| {
        try validateGeneratedColumnExpressionConditionBackfillSources(current_columns, next_columns, branch.when);
        try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, branch.then);
    }
    for (expression.case_else) |case_else| try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, case_else);
}

fn validateGeneratedColumnExpressionConditionBackfillSources(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    condition: schema_mod.RelationalRowsExpressionCondition,
) error{InvalidSchemaUpdateRequest}!void {
    try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, condition.lhs);
    for (condition.rhs) |rhs| try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, rhs);
}

fn validateConstraintCatalogTransition(current_schema: schema_mod.TableSchema, next_schema: schema_mod.TableSchema) !void {
    for (current_schema.unique_constraints) |constraint| {
        if (findUniqueConstraintByName(next_schema.unique_constraints, constraint.name)) |next_constraint| {
            if (!uniqueConstraintsSameDefinition(constraint, next_constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_schema.unique_constraints) |constraint| {
        if (findUniqueConstraintByName(current_schema.unique_constraints, constraint.name)) |current_constraint| {
            if (!uniqueConstraintsSameDefinition(current_constraint, constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (current_schema.foreign_keys) |foreign_key| {
        if (findForeignKeyByName(next_schema.foreign_keys, foreign_key.name)) |next_foreign_key| {
            if (!foreignKeysSameDefinition(foreign_key, next_foreign_key)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_schema.foreign_keys) |foreign_key| {
        if (findForeignKeyByName(current_schema.foreign_keys, foreign_key.name)) |current_foreign_key| {
            if (!foreignKeysSameDefinition(current_foreign_key, foreign_key)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (current_schema.checks) |check| {
        if (findRelationalCheckByName(next_schema.checks, check.name)) |next_check| {
            if (!schema_mod.relationalChecksEqual(check, next_check)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_schema.checks) |check| {
        if (findRelationalCheckByName(current_schema.checks, check.name)) |current_check| {
            if (!schema_mod.relationalChecksEqual(current_check, check)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn relationalDefaultColumnRowValueAlloc(
    alloc: Allocator,
    column: schema_mod.RelationalColumn,
    value_json: []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    try writer.print("{f}:", .{std.json.fmt(column.path, .{})});
    try writer.writeAll(value_json);
    try writer.writeByte('}');
    const json = try out.toOwnedSlice();
    defer alloc.free(json);
    const columns = [_]schema_mod.RelationalColumn{column};
    return try mapper.buildRelationalRowValueAlloc(alloc, json, columns[0..]);
}

fn jsonLiteralIsNull(value_json: []const u8) bool {
    return std.mem.eql(u8, std.mem.trim(u8, value_json, " \t\r\n"), "null");
}

const ExistingPhysicalStorageMode = enum {
    empty,
    document,
    relational,
    mixed,
};

fn detectExistingPhysicalStorageMode(alloc: Allocator, store: anytype) !ExistingPhysicalStorageMode {
    const lower = [_]u8{internal_keys.user_namespace};
    const upper = [_]u8{internal_keys.user_namespace + 1};
    const rows = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, rows);

    var saw_document = false;
    var saw_relational = false;
    for (rows) |entry| {
        if (internal_keys.isPrimaryDocumentKey(entry.key)) {
            saw_document = true;
        } else if (internal_keys.isRelationalRowKey(entry.key)) {
            saw_relational = true;
        }
        if (saw_document and saw_relational) return .mixed;
    }

    if (saw_document) return .document;
    if (saw_relational) return .relational;
    return .empty;
}

fn relationalRowsFindColumn(columns: []const schema_mod.RelationalColumn, name: []const u8) ?schema_mod.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, name) or std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

pub fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

pub fn findUniqueConstraintByName(unique_constraints: []const schema_mod.UniqueConstraint, name: []const u8) ?schema_mod.UniqueConstraint {
    for (unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn findForeignKeyByName(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) ?schema_mod.ForeignKey {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
    }
    return null;
}

fn findRelationalCheckByName(checks: []const schema_mod.RelationalCheck, name: []const u8) ?schema_mod.RelationalCheck {
    for (checks) |check| {
        if (std.mem.eql(u8, check.name, name)) return check;
    }
    return null;
}

fn uniqueConstraintsSameDefinition(a: schema_mod.UniqueConstraint, b: schema_mod.UniqueConstraint) bool {
    return std.mem.eql(u8, a.name, b.name) and
        stringSlicesEqual(a.columns, b.columns) and
        uniqueExpressionSlicesEqual(a.expressions, b.expressions) and
        stringSlicesEqual(a.include_columns, b.include_columns) and
        optionalStringsEqual(a.without_overlaps_period, b.without_overlaps_period) and
        a.nulls_not_distinct == b.nulls_not_distinct and
        a.deferrable == b.deferrable and
        a.timing == b.timing and
        uniquePredicateSlicesEqual(a.where, b.where) and
        relationalRowsExpressionConditionSlicesEqual(a.where_expressions, b.where_expressions);
}

pub fn uniqueConstraintCanBackForeignKey(constraint: schema_mod.UniqueConstraint) bool {
    return constraint.where.len == 0 and
        constraint.where_expressions.len == 0 and
        constraint.expressions.len == 0 and
        constraint.without_overlaps_period == null;
}

fn uniqueExpressionSlicesEqual(a: []const schema_mod.UniqueExpression, b: []const schema_mod.UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (left.expression == null or right.expression == null) {
            if (left.expression != null or right.expression != null) return false;
        } else if (!relationalRowsExpressionEqual(left.expression.?, right.expression.?)) return false;
    }
    return true;
}

fn uniquePredicateSlicesEqual(a: []const schema_mod.UniquePredicate, b: []const schema_mod.UniquePredicate) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (!optionalStringsEqual(left.value_json, right.value_json)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionSlicesEqual(
    a: []const schema_mod.RelationalRowsExpressionCondition,
    b: []const schema_mod.RelationalRowsExpressionCondition,
) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!relationalRowsExpressionConditionEqual(left, right)) return false;
    }
    return true;
}

fn foreignKeysSameDefinition(a: schema_mod.ForeignKey, b: schema_mod.ForeignKey) bool {
    return std.mem.eql(u8, a.name, b.name) and
        stringSlicesEqual(a.child_columns, b.child_columns) and
        optionalStringsEqual(a.child_period, b.child_period) and
        std.mem.eql(u8, a.parent_table, b.parent_table) and
        stringSlicesEqual(a.parent_columns, b.parent_columns) and
        optionalStringsEqual(a.parent_period, b.parent_period) and
        a.on_delete == b.on_delete and
        a.on_update == b.on_update and
        a.timing == b.timing and
        a.deferrable == b.deferrable and
        a.match == b.match;
}

pub fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionEqual(
    lhs: schema_mod.RelationalRowsExpressionCondition,
    rhs: schema_mod.RelationalRowsExpressionCondition,
) bool {
    if (lhs.op != rhs.op or lhs.rhs.len != rhs.rhs.len) return false;
    if (!relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
    for (lhs.rhs, rhs.rhs) |lhs_rhs, rhs_rhs| {
        if (!relationalRowsExpressionEqual(lhs_rhs, rhs_rhs)) return false;
    }
    return true;
}

fn relationalRowsExpressionEqual(
    lhs: schema_mod.RelationalRowsExpression,
    rhs: schema_mod.RelationalRowsExpression,
) bool {
    if (lhs.kind != rhs.kind or
        lhs.field_source != rhs.field_source or
        lhs.json_as_text != rhs.json_as_text or
        lhs.cast_type != rhs.cast_type or
        !std.mem.eql(u8, lhs.field, rhs.field) or
        !std.mem.eql(u8, lhs.value_json, rhs.value_json) or
        !std.mem.eql(u8, lhs.json_path, rhs.json_path) or
        lhs.operands.len != rhs.operands.len or
        lhs.case_branches.len != rhs.case_branches.len or
        lhs.case_else.len != rhs.case_else.len)
    {
        return false;
    }
    for (lhs.operands, rhs.operands) |lhs_operand, rhs_operand| {
        if (!relationalRowsExpressionEqual(lhs_operand, rhs_operand)) return false;
    }
    for (lhs.case_branches, rhs.case_branches) |lhs_branch, rhs_branch| {
        if (!relationalRowsExpressionConditionEqual(lhs_branch.when, rhs_branch.when)) return false;
        if (!relationalRowsExpressionEqual(lhs_branch.then, rhs_branch.then)) return false;
    }
    for (lhs.case_else, rhs.case_else) |lhs_fallback, rhs_fallback| {
        if (!relationalRowsExpressionEqual(lhs_fallback, rhs_fallback)) return false;
    }
    return true;
}

test "db schema checks execute json row expressions in storage" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","metadata"],"additionalProperties":false}}}}
    ;
    const schema_json_checks =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","metadata"],"additionalProperties":false}}},"checks":[{"name":"metadata_source_present","expression":{"lhs":{"op":"json_path_exists","args":[{"field":"metadata"}],"path":"source"},"op":"eq","rhs":{"value":true}}},{"name":"metadata_source_text","expression":{"lhs":{"op":"json_extract","args":[{"field":"metadata"}],"path":"source","as_text":true},"op":"eq","rhs":{"value":"api"}}},{"name":"metadata_flags_array","expression":{"lhs":{"op":"json_typeof","args":[{"op":"json_extract","args":[{"field":"metadata"}],"path":"flags"}]},"op":"eq","rhs":{"value":"array"}}},{"name":"metadata_flags_nonempty","expression":{"lhs":{"op":"json_array_length","args":[{"op":"json_extract","args":[{"field":"metadata"}],"path":"flags"}]},"op":"gte","rhs":{"value":1}}}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"row:a\",\"metadata\":{\"source\":\"api\",\"flags\":[\"hot\"]}}" },
            .{ .key = "row:b", .value = "{\"id\":\"row:b\",\"metadata\":{\"flags\":[]}}" },
        },
    });

    try std.testing.expectError(error.InvalidRowsRequest, db.applyTableSchemaJson(alloc, schema_json_checks, .{}));
    try db.batch(.{
        .writes = &.{.{ .key = "row:b", .value = "{\"id\":\"row:b\",\"metadata\":{\"source\":\"api\",\"flags\":[\"cold\"]}}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_json_checks, .{});
    const after_json_checks = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), after_json_checks.checks.len);
    try std.testing.expect(after_json_checks.checks[0].expression != null);
}

test "db schema checks execute temporal and case row expressions in storage" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"created_at_ns":{"type":"datetime"}},"required":["id","status","created_at_ns"],"additionalProperties":false}}}}
    ;
    const schema_temporal_checks =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"created_at_ns":{"type":"datetime"}},"required":["id","status","created_at_ns"],"additionalProperties":false}}},"checks":[{"name":"created_hour_trunc","expression":{"lhs":{"op":"date_trunc","args":[{"value":"hour"},{"field":"created_at_ns"}]},"op":"eq","rhs":{"value":3600000000000}}},{"name":"created_hour_bin","expression":{"lhs":{"op":"date_bin","args":[{"op":"interval_ns","args":[{"value":3600000000000}]},{"field":"created_at_ns"},{"value":0}]},"op":"eq","rhs":{"value":3600000000000}}},{"name":"created_hour_part","expression":{"lhs":{"op":"date_part","args":[{"value":"hour"},{"field":"created_at_ns"}]},"op":"eq","rhs":{"value":1}}},{"name":"case_status_hour","expression":{"lhs":{"op":"case","cases":[{"when":{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"active"}},"then":{"op":"date_part","args":[{"value":"hour"},{"field":"created_at_ns"}]}}],"else":{"value":0}},"op":"eq","rhs":{"value":1}}}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"row:a\",\"status\":\"active\",\"created_at_ns\":3700000000000}" },
            .{ .key = "row:b", .value = "{\"id\":\"row:b\",\"status\":\"inactive\",\"created_at_ns\":7300000000000}" },
        },
    });

    try std.testing.expectError(error.InvalidRowsRequest, db.applyTableSchemaJson(alloc, schema_temporal_checks, .{}));
    try db.batch(.{
        .writes = &.{.{ .key = "row:b", .value = "{\"id\":\"row:b\",\"status\":\"active\",\"created_at_ns\":3700000000000}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_temporal_checks, .{});
    const after_temporal_checks = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), after_temporal_checks.checks.len);
    try std.testing.expect(after_temporal_checks.checks[3].expression != null);
}

test "db schema apply validates added check constraints against existing rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_bad_amount_check =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_nonnegative","field":"amount","op":"gte","value":0}]}
    ;
    const schema_status_check =
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"status_active_lower","expression":{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"eq","rhs":{"value":"active"}}}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"row:a\",\"amount\":-1,\"status\":\"ACTIVE\"}" },
            .{ .key = "row:b", .value = "{\"id\":\"row:b\",\"amount\":2,\"status\":\"ACTIVE\"}" },
        },
    });

    try std.testing.expectError(error.InvalidRowsRequest, db.applyTableSchemaJson(alloc, schema_bad_amount_check, .{}));
    const after_failed_check = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), after_failed_check.checks.len);

    try db.batch(.{
        .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"row:a\",\"amount\":1,\"status\":\"ACTIVE\"}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_status_check, .{});
    const after_expression_check = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), after_expression_check.checks.len);
    try std.testing.expect(after_expression_check.checks[0].expression != null);

    const local_schema_json = try db.core.store.get(alloc, local_schema_json_key);
    defer alloc.free(local_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, local_schema_json, "status_active_lower") != null);
}

test "db schema apply validates unvalidated check promotion" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_unvalidated =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_nonnegative","field":"amount","op":"gte","value":0,"validation_state":"unvalidated"},{"name":"status_active_lower","expression":{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"eq","rhs":{"value":"active"}},"validation_state":"unvalidated"}]}
    ;
    const schema_enforced =
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_nonnegative","field":"amount","op":"gte","value":0,"validation_state":"enforced"},{"name":"status_active_lower","expression":{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"eq","rhs":{"value":"active"}},"validation_state":"enforced"}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"row:a\",\"amount\":-1,\"status\":\"ACTIVE\"}" },
            .{ .key = "row:b", .value = "{\"id\":\"row:b\",\"amount\":2,\"status\":\"pending\"}" },
        },
    });
    try db.applyTableSchemaJson(alloc, schema_unvalidated, .{});
    const unvalidated_schema = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.RelationalCheckValidationState.unvalidated, unvalidated_schema.checks[0].validation_state);
    try std.testing.expectEqual(schema_mod.RelationalCheckValidationState.unvalidated, unvalidated_schema.checks[1].validation_state);

    try std.testing.expectError(error.InvalidRowsRequest, db.applyTableSchemaJson(alloc, schema_enforced, .{}));
    const after_failed_enforce = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.RelationalCheckValidationState.unvalidated, after_failed_enforce.checks[0].validation_state);

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"row:a\",\"amount\":1,\"status\":\"ACTIVE\"}" },
            .{ .key = "row:b", .value = "{\"id\":\"row:b\",\"amount\":2,\"status\":\"active\"}" },
        },
    });
    try db.applyTableSchemaJson(alloc, schema_enforced, .{});
    const enforced_schema = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.RelationalCheckValidationState.enforced, enforced_schema.checks[0].validation_state);
    try std.testing.expectEqual(schema_mod.RelationalCheckValidationState.enforced, enforced_schema.checks[1].validation_state);
}

test "db schema apply validates unvalidated unique promotion" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_unvalidated =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"],"validation_state":"unvalidated"}]}
    ;
    const schema_enforced =
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"],"validation_state":"enforced"}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "user:ada", .value = "{\"id\":\"user:ada\",\"email\":\"dup@example.test\"}" },
            .{ .key = "user:grace", .value = "{\"id\":\"user:grace\",\"email\":\"dup@example.test\"}" },
        },
    });
    try db.applyTableSchemaJson(alloc, schema_unvalidated, .{});
    const unvalidated_schema = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.UniqueConstraintValidationState.unvalidated, unvalidated_schema.unique_constraints[0].validation_state);

    try std.testing.expectError(error.UniqueConstraintViolation, db.applyTableSchemaJson(alloc, schema_enforced, .{}));
    const after_failed_enforce = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.UniqueConstraintValidationState.unvalidated, after_failed_enforce.unique_constraints[0].validation_state);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:grace", .value = "{\"id\":\"user:grace\",\"email\":\"grace@example.test\"}" },
        },
    });
    try db.applyTableSchemaJson(alloc, schema_enforced, .{});
    const enforced_schema = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.UniqueConstraintValidationState.enforced, enforced_schema.unique_constraints[0].validation_state);
}

test "db schema runtime persists lite sql table record with local schema metadata" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const indexes_json =
        \\{"algebraic_index_v0":{"kind":"algebraic","source":"lite_sql"}}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.applyLiteSqlTableRecord(alloc, .{
            .table_id = 42,
            .name = "usage_records",
            .database_name = "tenant_db",
            .namespace_name = "billing",
            .placement_role = "data",
            .desired_replica_count = 1,
            .schema_json = schema_json,
            .indexes_json = indexes_json,
        });

        const local_schema_json = (try db.getSchemaJson(alloc)) orelse return error.TestUnexpectedResult;
        defer alloc.free(local_schema_json);
        try std.testing.expectEqualStrings(schema_json, local_schema_json);

        const table_record = (try db.getLiteSqlTableRecordAlloc(alloc)) orelse return error.TestUnexpectedResult;
        defer metadata_table_manager.freeTable(alloc, table_record);
        try std.testing.expectEqual(@as(u64, 42), table_record.table_id);
        try std.testing.expectEqualStrings("usage_records", table_record.name);
        try std.testing.expectEqualStrings("tenant_db", table_record.database_name);
        try std.testing.expectEqualStrings("billing", table_record.namespace_name);
        try std.testing.expectEqualStrings(schema_json, table_record.schema_json);
        try std.testing.expectEqualStrings(indexes_json, table_record.indexes_json);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{ .open_mode = .status_only });
        defer reopened.close();

        const local_schema_json = (try reopened.getSchemaJson(alloc)) orelse return error.TestUnexpectedResult;
        defer alloc.free(local_schema_json);
        try std.testing.expectEqualStrings(schema_json, local_schema_json);

        const table_record = (try reopened.getLiteSqlTableRecordAlloc(alloc)) orelse return error.TestUnexpectedResult;
        defer metadata_table_manager.freeTable(alloc, table_record);
        try std.testing.expectEqual(@as(u64, 42), table_record.table_id);
        try std.testing.expectEqualStrings("usage_records", table_record.name);
        try std.testing.expectEqualStrings("tenant_db", table_record.database_name);
        try std.testing.expectEqualStrings("billing", table_record.namespace_name);
        try std.testing.expectEqualStrings(schema_json, table_record.schema_json);
        try std.testing.expectEqualStrings(indexes_json, table_record.indexes_json);

        try std.testing.expectError(error.ReadOnly, reopened.applyLiteSqlTableRecord(alloc, .{
            .table_id = 43,
            .name = "denied_records",
            .database_name = "tenant_db",
            .namespace_name = "billing",
            .placement_role = "data",
            .desired_replica_count = 1,
            .schema_json = schema_json,
            .indexes_json = indexes_json,
        }));
    }
}

test "db direct schema apply rejects storage mode switches" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const document_schema =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}}}
    ;
    const relational_schema =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;

    try db.applyTableSchemaJson(alloc, document_schema, .{});
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.applyTableSchemaJson(alloc, relational_schema, .{}));

    const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
    defer schema_mod.freeSchema(alloc, durable_schema);
    try std.testing.expectEqual(schema_mod.StorageMode.document, durable_schema.storage_mode);
}

test "db direct schema apply rejects relational base column changes" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const relational_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    const changed_columns_schema =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"keyword"}},"required":["title"],"additionalProperties":false}}}}
    ;

    try db.applyTableSchemaJson(alloc, relational_schema, .{});
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.applyTableSchemaJson(alloc, changed_columns_schema, .{}));

    const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
    defer schema_mod.freeSchema(alloc, durable_schema);
    try std.testing.expectEqual(schema_mod.StorageMode.relational, durable_schema.storage_mode);
    try std.testing.expectEqual(schema_mod.AntflyType.numeric, durable_schema.relational_columns[1].field_type);
}

test "db direct schema apply appends literal default and generated columns through row rewrite" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword","default":"ACTIVE"},"note":{"type":"keyword"},"title_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"title"}]}}},"status_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"status"}]}}},"status_expr_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"status"}]}}}},"required":["title"],"additionalProperties":false}}},"relational_indexes":[{"name":"status","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"]},{"name":"title_lc","owner_kind":"relational_column","owner_name":"title_lc","access_method":"ordered_tuple","columns":["title_lc"],"keys":[{"column":"title_lc"}],"lifecycle":"ready","generation":1,"schema_fingerprint":"secondary-index-v1:title_lc","generation_record":{"generation":1,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}},{"name":"status_expr_lc","owner_kind":"relational_column","owner_name":"status_expr_lc","access_method":"ordered_tuple","columns":["status_expr_lc"],"keys":[{"column":"status_expr_lc"}],"lifecycle":"ready","generation":1,"schema_fingerprint":"secondary-index-v1:status_expr_lc","generation_record":{"generation":1,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"one\",\"amount\":3}" }},
    });

    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const materialized = (try db.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"title\":\"one\",\"amount\":3,\"status\":\"ACTIVE\",\"title_lc\":\"one\",\"status_lc\":\"active\",\"status_expr_lc\":\"active\"}", materialized);

    const status_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status", "doc:a");
    defer alloc.free(status_index);
    const status_index_value = try db.core.store.get(alloc, status_index);
    defer alloc.free(status_index_value);

    const note_column = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:a", "note");
    defer alloc.free(note_column);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, note_column));

    const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
    defer schema_mod.freeSchema(alloc, durable_schema);
    try std.testing.expectEqual(@as(usize, 7), durable_schema.relational_columns.len);
    const raw_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "doc:a") orelse return error.TestUnexpectedResult;
    defer alloc.free(raw_row);
    const title_lc_tuple = try relational_store_mod.orderedTupleValueForIndexKeysAlloc(alloc, raw_row, durable_schema.relational_indexes[1].keys, durable_schema.relational_columns);
    defer alloc.free(title_lc_tuple);
    const title_lc_docs = try relational_store_mod.scanOrderedTupleDocKeysAlloc(alloc, db.core.store, "title_lc", title_lc_tuple, "", "");
    defer relational_store_mod.freeDocKeys(alloc, title_lc_docs);
    try std.testing.expectEqual(@as(usize, 1), title_lc_docs.len);
    try std.testing.expectEqualStrings("doc:a", title_lc_docs[0]);

    const status_expr_lc_tuple = try relational_store_mod.orderedTupleValueForIndexKeysAlloc(alloc, raw_row, durable_schema.relational_indexes[2].keys, durable_schema.relational_columns);
    defer alloc.free(status_expr_lc_tuple);
    const status_expr_lc_docs = try relational_store_mod.scanOrderedTupleDocKeysAlloc(alloc, db.core.store, "status_expr_lc", status_expr_lc_tuple, "", "");
    defer relational_store_mod.freeDocKeys(alloc, status_expr_lc_docs);
    try std.testing.expectEqual(@as(usize, 1), status_expr_lc_docs.len);
    try std.testing.expectEqualStrings("doc:a", status_expr_lc_docs[0]);

    const durable_generated = durable_schema.relational_columns[6].generated orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.RelationalGeneratedOp.expression, durable_generated.op);
    const durable_expression = durable_generated.expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.RelationalRowsExpressionKind.lower, durable_expression.kind);
}

test "db direct schema apply rejects generated columns without deterministic backfill sources" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"}},"required":["title"],"additionalProperties":false}}}}
    ;
    const missing_source_schema =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"note":{"type":"keyword"},"note_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"note"}]}}}},"required":["title"],"additionalProperties":false}}}}
    ;
    const generated_source_schema =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"title_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"title"}]}}},"title_lc_md5":{"type":"keyword","generated":{"op":"expression","expression":{"op":"md5","args":[{"field":"title_lc"}]}}}},"required":["title"],"additionalProperties":false}}}}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.applyTableSchemaJson(alloc, missing_source_schema, .{}));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.applyTableSchemaJson(alloc, generated_source_schema, .{}));
}

test "db repairs relational column backed indexes from authoritative packed rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"orders_status_amount_idx","owner_kind":"relational_column","owner_name":"status","access_method":"ordered_tuple","columns":["status"],"keys":[{"column":"status"},{"column":"amount"}],"lifecycle":"ready","generation":7,"schema_fingerprint":"secondary-index-v1:status_amount","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}},{"name":"amount","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"]}]}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "doc:a", .value = "{\"id\":\"a\",\"status\":\"active\",\"amount\":1}" },
        .{ .key = "doc:b", .value = "{\"id\":\"b\",\"status\":\"active\",\"amount\":2}" },
    } });

    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    const ordered_index = blk: {
        for (runtime_schema.relational_indexes) |index| {
            if (std.mem.eql(u8, index.name, "orders_status_amount_idx")) break :blk index;
        }
        return error.TestUnexpectedResult;
    };

    const row_b = try relational_store_mod.getRawAlloc(alloc, db.core.store, "doc:b") orelse return error.TestUnexpectedResult;
    defer alloc.free(row_b);
    const tuple_b = try relational_store_mod.orderedTupleValueForIndexKeysAlloc(alloc, row_b, ordered_index.keys, runtime_schema.relational_columns);
    defer alloc.free(tuple_b);

    const doc_b_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_b, "doc:b");
    defer alloc.free(doc_b_forward);
    const doc_b_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:b", "orders_status_amount_idx", tuple_b);
    defer alloc.free(doc_b_reverse);
    const amount_b_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:b");
    defer alloc.free(amount_b_index);
    const amount_b_reverse = try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, "doc:b", "amount");
    defer alloc.free(amount_b_reverse);

    const orphan_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_b, "doc:z");
    defer alloc.free(orphan_forward);
    const orphan_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:z", "orders_status_amount_idx", tuple_b);
    defer alloc.free(orphan_reverse);
    const forward_only_orphan = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_b, "doc:y");
    defer alloc.free(forward_only_orphan);

    try db.core.store.putBatch(&.{
        .{ .key = orphan_forward, .value = "" },
        .{ .key = orphan_reverse, .value = "" },
        .{ .key = forward_only_orphan, .value = "" },
    }, &.{ doc_b_forward, doc_b_reverse, amount_b_index, amount_b_reverse });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, doc_b_forward));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, amount_b_index));

    const report = try db.repairRelationalColumnBackedIndexesInRange("doc:a", "doc:zz");
    try std.testing.expectEqual(@as(u64, 2), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 2), report.indexed_rows);
    try std.testing.expect(report.written_entries >= 6);

    const repaired_forward = try db.core.store.get(alloc, doc_b_forward);
    defer alloc.free(repaired_forward);
    const repaired_reverse = try db.core.store.get(alloc, doc_b_reverse);
    defer alloc.free(repaired_reverse);
    const repaired_amount = try db.core.store.get(alloc, amount_b_index);
    defer alloc.free(repaired_amount);
    const repaired_amount_reverse = try db.core.store.get(alloc, amount_b_reverse);
    defer alloc.free(repaired_amount_reverse);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, orphan_forward));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, orphan_reverse));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, forward_only_orphan));
}

test "db first relational schema apply rejects existing document rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing", .value = "{\"title\":\"already document mode\"}" }},
    });

    const relational_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"required":["title"],"additionalProperties":false}}}}
    ;

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.applyTableSchemaJson(alloc, relational_schema, .{}));
    const schema = try schema_mod.loadSchema(db.core.store, alloc);
    defer if (schema) |loaded| schema_mod.freeSchema(alloc, loaded);
    try std.testing.expect(schema == null);
}

test "db executes claimed schema rewrite job expressions over relational rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}},"relational_indexes":[{"name":"status_key","owner_kind":"relational_column","owner_name":"status_key","access_method":"scalar_column","columns":["status_key"]}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\"}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const expression = types.RelationalRowsExpression{
        .kind = .lower,
        .operands = &.{.{
            .kind = .field,
            .field = "status",
        }},
    };
    const job = metadata_table_manager.SchemaRewriteJobRecord{
        .job_id = 44,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .state = metadata_table_manager.schema_rewrite_running,
        .target_column = "status_key",
        .expression = expression,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 10_000,
    };

    var result = try db.executeClaimedSchemaRewriteJob(alloc, job);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), result.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), result.report.rewritten_rows);
    try std.testing.expectEqualStrings("", result.progress_row_key);
    const finish = result.finishRequest(job);
    try std.testing.expectEqual(@as(u64, 1), finish.completed_row_count);
    try std.testing.expectEqualStrings("worker-a", finish.lease_owner);

    const materialized = (try db.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status_key\":\"active\"") != null);

    const status_key_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status_key", "doc:a");
    defer alloc.free(status_key_index);
    const status_key_index_value = try db.core.store.get(alloc, status_key_index);
    defer alloc.free(status_key_index_value);

    var unclaimed = job;
    unclaimed.state = metadata_table_manager.schema_rewrite_declared;
    try std.testing.expectError(error.SchemaRewriteJobNotRunning, db.executeClaimedSchemaRewriteJob(alloc, unclaimed));

    var stale = job;
    stale.schema_generation = 99;
    try std.testing.expectError(error.InvalidSchemaRewriteGeneration, db.executeClaimedSchemaRewriteJob(alloc, stale));
}

test "db executes claimed schema validation jobs over relational rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id","status","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id","status","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_key","columns":["email"],"validation_state":"enforced"}],"checks":[{"name":"users_status_known","field":"status","op":"eq","value":"active","validation_state":"enforced"}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"id\":\"a\",\"status\":\"active\",\"email\":\"a@example.test\"}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const job = metadata_table_manager.SchemaRewriteJobRecord{
        .job_id = 47,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
        .action = "validate",
        .reason = "constraints",
        .start_row_key = "",
        .end_row_key = null,
        .state = metadata_table_manager.schema_rewrite_running,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 10_000,
    };

    var result = try db.executeClaimedSchemaRewriteJob(alloc, job);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), result.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 0), result.report.rewritten_rows);
    try std.testing.expectEqual(@as(u64, 1), result.report.unchanged_rows);
    try std.testing.expectEqualStrings("", result.progress_row_key);
    const finish = result.finishRequest(job);
    try std.testing.expectEqual(@as(u64, 1), finish.completed_row_count);

    var invalid_payload = job;
    invalid_payload.target_column = "status";
    try std.testing.expectError(error.InvalidSchemaRewriteJob, db.executeClaimedSchemaRewriteJob(alloc, invalid_payload));
}

test "db executes claimed full schema rewrite jobs over relational rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}},"relational_indexes":[{"name":"status","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"]}]}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}},"relational_indexes":[{"name":"title","owner_kind":"relational_column","owner_name":"title","access_method":"scalar_column","columns":["title"]},{"name":"status","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"]}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\"}" }},
    });

    const status_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status", "doc:a");
    defer alloc.free(status_index);
    const initial_status_index_value = try db.core.store.get(alloc, status_index);
    defer alloc.free(initial_status_index_value);

    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const job = metadata_table_manager.SchemaRewriteJobRecord{
        .job_id = 46,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .state = metadata_table_manager.schema_rewrite_running,
        .full_row_rewrite = true,
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 10_000,
    };

    var result = try db.executeClaimedSchemaRewriteJob(alloc, job);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), result.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), result.report.rewritten_rows);
    try std.testing.expectEqual(@as(u64, 0), result.report.renamed_cells);
    try std.testing.expectEqual(@as(u64, 0), result.report.dropped_cells);

    const materialized = (try db.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"title\":\"one\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status\":\"ACTIVE\"") != null);

    const title_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "title", "doc:a");
    defer alloc.free(title_index);
    const title_index_value = try db.core.store.get(alloc, title_index);
    defer alloc.free(title_index_value);
    const rewritten_status_index_value = try db.core.store.get(alloc, status_index);
    defer alloc.free(rewritten_status_index_value);
}

test "db executes claimed schema rewrite row plans over relational rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"legacy_status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"state":{"type":"keyword"}},"required":["title","state"],"additionalProperties":false}}},"relational_indexes":[{"name":"state","owner_kind":"relational_column","owner_name":"state","access_method":"scalar_column","columns":["state"]}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\",\"legacy_status\":\"old\"}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const job = metadata_table_manager.SchemaRewriteJobRecord{
        .job_id = 45,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .state = metadata_table_manager.schema_rewrite_running,
        .rewrite_renames = &.{.{ .old_path = "status", .new_path = "state" }},
        .rewrite_drops = &.{"legacy_status"},
        .lease_owner = "worker-a",
        .lease_expires_at_ms = 10_000,
    };

    var result = try db.executeClaimedSchemaRewriteJob(alloc, job);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), result.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), result.report.rewritten_rows);
    try std.testing.expectEqual(@as(u64, 1), result.report.renamed_cells);
    try std.testing.expectEqual(@as(u64, 1), result.report.dropped_cells);

    const materialized = (try db.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"state\":\"ACTIVE\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"legacy_status\"") == null);

    const state_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "state", "doc:a");
    defer alloc.free(state_index);
    const state_index_value = try db.core.store.get(alloc, state_index);
    defer alloc.free(state_index_value);
    const old_status_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status", "doc:a");
    defer alloc.free(old_status_index);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_status_index));
}

test "db drains metadata schema rewrite jobs through claim and finish lifecycle" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\"}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const TestSchemaRewriteService = struct {
        manager: metadata_table_manager.TableManager,

        fn deinit(self: *@This()) void {
            self.manager.deinit();
        }

        pub fn listProjectedSchemaRewriteJobs(self: *@This(), allocator: std.mem.Allocator) ![]metadata_table_manager.SchemaRewriteJobRecord {
            return try self.manager.listSchemaRewriteJobs(allocator);
        }

        pub fn freeProjectedSchemaRewriteJobs(self: *@This(), allocator: std.mem.Allocator, records: []metadata_table_manager.SchemaRewriteJobRecord) void {
            self.manager.freeSchemaRewriteJobs(allocator, records);
        }

        pub fn beginSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobBeginRequest) !void {
            try self.manager.beginSchemaRewriteJob(request);
        }

        pub fn finishSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
            try self.manager.finishSchemaRewriteJob(request);
        }

        pub fn invalidateSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
            try self.manager.invalidateSchemaRewriteJob(request);
        }
    };

    var service = TestSchemaRewriteService{ .manager = metadata_table_manager.TableManager.init(alloc) };
    defer service.deinit();
    try service.manager.upsertTable(.{
        .table_id = 7,
        .name = "usage_records",
        .schema_json = schema_v2,
    });
    try service.manager.upsertSchemaRewriteJob(.{
        .job_id = 44,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .target_column = "status_key",
        .expression = .{
            .kind = .lower,
            .operands = &.{.{
                .kind = .field,
                .field = "status",
            }},
        },
    });

    try std.testing.expectEqual(@as(usize, 1), try db.drainSchemaRewriteJobsForIdle(alloc, &service, .{
        .worker_id = "worker-a",
        .group_id = 9001,
        .now_ms = 1000,
        .lease_ttl_ms = 5000,
        .max_jobs = 4,
    }));
    try std.testing.expectEqual(@as(usize, 0), try db.drainSchemaRewriteJobsForIdle(alloc, &service, .{
        .worker_id = "worker-a",
        .group_id = 9001,
        .now_ms = 2000,
        .lease_ttl_ms = 5000,
        .max_jobs = 4,
    }));

    const jobs = try service.manager.listSchemaRewriteJobs(alloc);
    defer service.manager.freeSchemaRewriteJobs(alloc, jobs);
    try std.testing.expectEqual(@as(usize, 1), jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_ready, jobs[0].state);
    try std.testing.expectEqual(@as(u64, 1), jobs[0].completed_row_count);
    try std.testing.expectEqualStrings("", jobs[0].progress_row_key);
    try std.testing.expectEqualStrings("", jobs[0].last_error);

    const materialized = (try db.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status_key\":\"active\"") != null);
}

test "db schema rewrite drain skips stale claim races and continues" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\"}" }},
    });
    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const TestSchemaRewriteService = struct {
        manager: metadata_table_manager.TableManager,
        raced_job_id: u64,
        race_claims: usize = 0,

        fn deinit(self: *@This()) void {
            self.manager.deinit();
        }

        pub fn listProjectedSchemaRewriteJobs(self: *@This(), allocator: std.mem.Allocator) ![]metadata_table_manager.SchemaRewriteJobRecord {
            return try self.manager.listSchemaRewriteJobs(allocator);
        }

        pub fn freeProjectedSchemaRewriteJobs(self: *@This(), allocator: std.mem.Allocator, records: []metadata_table_manager.SchemaRewriteJobRecord) void {
            self.manager.freeSchemaRewriteJobs(allocator, records);
        }

        pub fn beginSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobBeginRequest) !void {
            if (request.job_id == self.raced_job_id) {
                self.race_claims += 1;
                return error.SchemaRewriteJobClaimBusy;
            }
            try self.manager.beginSchemaRewriteJob(request);
        }

        pub fn finishSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
            try self.manager.finishSchemaRewriteJob(request);
        }

        pub fn invalidateSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
            try self.manager.invalidateSchemaRewriteJob(request);
        }
    };

    const generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2);
    var service = TestSchemaRewriteService{
        .manager = metadata_table_manager.TableManager.init(alloc),
        .raced_job_id = 41,
    };
    defer service.deinit();
    try service.manager.upsertTable(.{
        .table_id = 7,
        .name = "usage_records",
        .schema_json = schema_v2,
    });
    try service.manager.upsertSchemaRewriteJob(.{
        .job_id = 41,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = generation,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .target_column = "status_key",
        .expression = .{
            .kind = .lower,
            .operands = &.{.{ .kind = .field, .field = "status" }},
        },
    });
    try service.manager.upsertSchemaRewriteJob(.{
        .job_id = 42,
        .table_id = 7,
        .group_id = 9001,
        .schema_generation = generation,
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = "",
        .end_row_key = null,
        .target_column = "status_key",
        .expression = .{
            .kind = .lower,
            .operands = &.{.{ .kind = .field, .field = "status" }},
        },
    });

    try std.testing.expectEqual(@as(usize, 1), try db.drainSchemaRewriteJobsForIdle(alloc, &service, .{
        .worker_id = "worker-a",
        .group_id = 9001,
        .now_ms = 1000,
        .lease_ttl_ms = 5000,
        .max_jobs = 1,
    }));
    try std.testing.expectEqual(@as(usize, 1), service.race_claims);

    const jobs = try service.manager.listSchemaRewriteJobs(alloc);
    defer service.manager.freeSchemaRewriteJobs(alloc, jobs);
    try std.testing.expectEqual(@as(usize, 2), jobs.len);
    var raced_state: []const u8 = "";
    var completed_state: []const u8 = "";
    for (jobs) |job| {
        if (job.job_id == 41) raced_state = job.state;
        if (job.job_id == 42) completed_state = job.state;
    }
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_declared, raced_state);
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_ready, completed_state);

    const materialized = (try db.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status_key\":\"active\"") != null);
}

test "db table schema apply stages algebraic pending before durable schema swap" {
    const DB = @import("mod.zig").DB;
    const algebraic_mod = @import("algebraic/mod.zig");

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_v1 =
        \\{"version":1,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"old_field":{"type":"keyword"},"new_field":{"type":"keyword"}}}}}}
    ;
    const schema_v2 =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"new_field":{"type":"keyword"}}}}}}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        var parsed_v1 = try schema_api_mod.parseValidatedTableSchema(alloc, schema_v1);
        defer parsed_v1.deinit(alloc);
        const runtime_v1 = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_v1);
        defer schema_mod.freeSchema(alloc, runtime_v1);
        try db.setSchema(runtime_v1);

        const config_v1 = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, "docs", schema_v1);
        defer alloc.free(config_v1);
        try db.addIndex(.{
            .name = "alg",
            .kind = .algebraic,
            .config_json = config_v1,
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:1", .value = "{\"old_field\":\"legacy\",\"new_field\":\"fresh\"}" }},
            .sync_level = .full_index,
        });
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, db.core.store, "old_field"));

        // Simulate a crash after the algebraic catalog has been marked pending
        // for schema v2, but before the runtime schema is durably swapped from
        // v1 to v2.
        try db.schemaRuntimeStageAlgebraicSchemaConfigsPending(schema_v2);
        const staged = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u32, 2), staged.index.config().schema_version);
        try std.testing.expectEqualStrings("rebuild_required", staged.index.config().capability_lifecycle_status);

        const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
        defer schema_mod.freeSchema(alloc, durable_schema);
        try std.testing.expectEqual(@as(u32, 1), durable_schema.version);
    }

    {
        var readonly = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
        });
        defer readonly.close();

        const entry = readonly.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u32, 2), entry.index.config().schema_version);
        try std.testing.expectEqualStrings("rebuild_required", entry.index.config().capability_lifecycle_status);
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, readonly.core.store, "old_field"));
    }

    {
        var writer = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer writer.close();

        // The writable open sees durable schema v1 and pending algebraic config
        // v2, so it must leave the rebuild pending until the schema write lands.
        const pending = writer.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("rebuild_required", pending.index.config().capability_lifecycle_status);
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, writer.core.store, "old_field"));

        try writer.applyTableSchemaJson(alloc, schema_v2, .{});
        const current = writer.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u32, 2), current.index.config().schema_version);
        try std.testing.expectEqualStrings("current", current.index.config().capability_lifecycle_status);
        try std.testing.expect(!try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, writer.core.store, "old_field"));
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, writer.core.store, "new_field"));

        const durable_schema = (try schema_mod.loadSchema(writer.core.store, alloc)) orelse return error.TestUnexpectedResult;
        defer schema_mod.freeSchema(alloc, durable_schema);
        try std.testing.expectEqual(@as(u32, 2), durable_schema.version);

        const local_schema_json = try writer.core.store.get(alloc, local_schema_json_key);
        defer alloc.free(local_schema_json);
        try std.testing.expectEqualStrings(schema_v2, local_schema_json);
    }
}

test "db staged algebraic pending waits for first durable schema" {
    const DB = @import("mod.zig").DB;
    const algebraic_mod = @import("algebraic/mod.zig");

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_v1 =
        \\{"version":1,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"old_field":{"type":"keyword"},"new_field":{"type":"keyword"}}}}}}
    ;
    const schema_v2 =
        \\{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"new_field":{"type":"keyword"}}}}}}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        const config_v1 = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, "docs", schema_v1);
        defer alloc.free(config_v1);
        try db.addIndex(.{
            .name = "alg",
            .kind = .algebraic,
            .config_json = config_v1,
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:1", .value = "{\"old_field\":\"legacy\",\"new_field\":\"fresh\"}" }},
            .sync_level = .full_index,
        });
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, db.core.store, "old_field"));

        try db.schemaRuntimeStageAlgebraicSchemaConfigsPending(schema_v2);
        const staged = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u32, 2), staged.index.config().schema_version);
        try std.testing.expectEqualStrings("rebuild_required", staged.index.config().capability_lifecycle_status);
        const missing_schema = try schema_mod.loadSchema(db.core.store, alloc);
        defer if (missing_schema) |schema| schema_mod.freeSchema(alloc, schema);
        try std.testing.expect(missing_schema == null);
    }

    {
        var writer = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer writer.close();

        const pending = writer.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u32, 2), pending.index.config().schema_version);
        try std.testing.expectEqualStrings("rebuild_required", pending.index.config().capability_lifecycle_status);
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, writer.core.store, "old_field"));

        try writer.applyTableSchemaJson(alloc, schema_v2, .{});
        const current = writer.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u32, 2), current.index.config().schema_version);
        try std.testing.expectEqualStrings("current", current.index.config().capability_lifecycle_status);
        try std.testing.expect(!try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, writer.core.store, "old_field"));
        try std.testing.expect(try index_manager_mod.storeHasDocFactScalarKeyContaining(alloc, writer.core.store, "new_field"));

        const durable_schema = (try schema_mod.loadSchema(writer.core.store, alloc)) orelse return error.TestUnexpectedResult;
        defer schema_mod.freeSchema(alloc, durable_schema);
        try std.testing.expectEqual(@as(u32, 2), durable_schema.version);

        const local_schema_json = try writer.core.store.get(alloc, local_schema_json_key);
        defer alloc.free(local_schema_json);
        try std.testing.expectEqualStrings(schema_v2, local_schema_json);
    }
}

test "db schema runtime algebraic adaptive stats report engine-owned observation status" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "schema_version": 42,
        \\  "capability_fingerprint": "cap:v1",
        \\  "capability_lifecycle_status": "stale",
        \\  "capability_change_added_fields": 1,
        \\  "capability_change_removed_fields": 2,
        \\  "capability_change_changed_type_fields": 3,
        \\  "skipped_dynamic_fields": 4,
        \\  "skipped_complex_fields": 5,
        \\  "skipped_unbounded_fields": 6,
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"},{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "adaptive": {"observe": true, "lazy_materialization": true, "min_observations": 1, "min_estimated_scan_rows_saved": 1},
        \\  "materializations": []
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "alg",
            .kind = .algebraic,
            .config_json = cfg,
        });

        const entry = db.core.index_manager.algebraicIndex("alg").?;
        const constraints = [_]algebraic_ir.Constraint{.{ .field = "tenant", .value = "t1" }};
        entry.index.recordObservedQueryShapeWithStore(db.core.store, .{
            .kind = .terms,
            .aggregation_name = "amount_by_customer",
            .bucket_field = "customer",
            .constraints = constraints[0..],
            .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
        }, "no_materialization");

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_observed_query_shape_count);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_recommendation_count);
        try std.testing.expect(stats.indexes[0].algebraic_last_observed_query_shape != null);
        try std.testing.expect(stats.indexes[0].algebraic_last_recommended_materialization != null);
        try std.testing.expect(std.mem.indexOf(u8, stats.indexes[0].algebraic_last_recommended_materialization.?, "recommendation:v1") != null);

        const materialization_states = try db.listAlgebraicMaterializationStates(alloc, "alg");
        defer types.freeAlgebraicMaterializationStates(alloc, materialization_states);
        try std.testing.expectEqual(@as(usize, 1), materialization_states.len);
        try std.testing.expectEqualStrings("alg", materialization_states[0].index_name);
        try std.testing.expectEqualStrings("recommended", materialization_states[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), materialization_states[0].observation_count);
        try std.testing.expect(std.mem.indexOf(u8, materialization_states[0].recommendation, "recommendation:v1") != null);

        const observations = try db.listAlgebraicQueryObservations(alloc, "alg");
        defer types.freeAlgebraicQueryObservations(alloc, observations);
        try std.testing.expectEqual(@as(usize, 1), observations.len);
        try std.testing.expectEqualStrings("alg", observations[0].index_name);
        try std.testing.expectEqual(@as(u64, 1), observations[0].count);
        try std.testing.expectEqualStrings("no_materialization", observations[0].reason);
        try std.testing.expectEqualStrings("recommended", observations[0].lifecycle);
        try std.testing.expect(observations[0].recommendation != null);
        try std.testing.expect(std.mem.indexOf(u8, observations[0].shape, "shape:v1") != null);

        try std.testing.expectEqual(@as(u64, 1), try db.evaluateAlgebraicAdaptiveCandidates());
        const backfilling_states = try db.listAlgebraicMaterializationStates(alloc, "alg");
        defer types.freeAlgebraicMaterializationStates(alloc, backfilling_states);
        try std.testing.expectEqual(@as(usize, 1), backfilling_states.len);
        try std.testing.expectEqualStrings("backfilling", backfilling_states[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), backfilling_states[0].observation_count);

        const backfilling_observations = try db.listAlgebraicQueryObservations(alloc, "alg");
        defer types.freeAlgebraicQueryObservations(alloc, backfilling_observations);
        try std.testing.expectEqual(@as(usize, 1), backfilling_observations.len);
        try std.testing.expectEqualStrings("backfilling", backfilling_observations[0].lifecycle);

        const missing_states = try db.listAlgebraicMaterializationStates(alloc, "missing");
        defer types.freeAlgebraicMaterializationStates(alloc, missing_states);
        try std.testing.expectEqual(@as(usize, 0), missing_states.len);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer reopened.close();

        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_observed_query_shape_count);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_recommendation_count);
        try std.testing.expect(stats.indexes[0].algebraic_last_observed_query_shape != null);
        try std.testing.expect(stats.indexes[0].algebraic_last_recommended_materialization != null);

        const materialization_states = try reopened.listAlgebraicMaterializationStates(alloc, null);
        defer types.freeAlgebraicMaterializationStates(alloc, materialization_states);
        try std.testing.expectEqual(@as(usize, 1), materialization_states.len);
        try std.testing.expectEqualStrings("alg", materialization_states[0].index_name);
        try std.testing.expectEqualStrings("backfilling", materialization_states[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), materialization_states[0].observation_count);

        const observations = try reopened.listAlgebraicQueryObservations(alloc, null);
        defer types.freeAlgebraicQueryObservations(alloc, observations);
        try std.testing.expectEqual(@as(usize, 1), observations.len);
        try std.testing.expectEqualStrings("alg", observations[0].index_name);
        try std.testing.expectEqualStrings("backfilling", observations[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), observations[0].count);
    }
}

test "db schema runtime algebraic evaluates policy-gated adaptive candidates" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "schema_version": 42,
        \\  "capability_fingerprint": "cap:v1",
        \\  "capability_lifecycle_status": "stale",
        \\  "capability_change_added_fields": 1,
        \\  "capability_change_removed_fields": 2,
        \\  "capability_change_changed_type_fields": 3,
        \\  "skipped_dynamic_fields": 4,
        \\  "skipped_complex_fields": 5,
        \\  "skipped_unbounded_fields": 6,
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"},{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "adaptive": {"observe": true, "lazy_materialization": true, "dematerialization": true, "min_observations": 1, "min_estimated_scan_rows_saved": 1, "dematerialize_after_observation_misses": 2},
        \\  "materializations": []
        \\}
    ;
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });

    const entry = db.core.index_manager.algebraicIndex("alg").?;
    const constraints = [_]algebraic_ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    entry.index.recordObservedQueryShapeWithStore(db.core.store, .{
        .kind = .terms,
        .aggregation_name = "amount_by_customer",
        .bucket_field = "customer",
        .constraints = constraints[0..],
        .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
    }, "no_materialization");

    try std.testing.expectEqual(@as(u64, 1), try db.evaluateAlgebraicAdaptiveCandidates());
    const candidates = try db.listAlgebraicAdaptiveCandidates(alloc, "alg");
    defer types.freeAlgebraicAdaptiveCandidates(alloc, candidates);
    try std.testing.expectEqual(@as(usize, 1), candidates.len);
    try std.testing.expect(std.mem.startsWith(u8, candidates[0].materialization_id, "am_"));
    try std.testing.expectEqualStrings("backfilling", candidates[0].lifecycle);
    try std.testing.expectEqualStrings("auto_backfill_started", candidates[0].decision);
    try std.testing.expect(candidates[0].estimated_scan_rows_saved >= 1);
    try std.testing.expect(candidates[0].estimated_write_cost >= 1);
    try std.testing.expect(candidates[0].estimated_write_amplification > 1);

    const progress = try db.listAlgebraicAdaptiveProgress(alloc, "alg");
    defer types.freeAlgebraicAdaptiveProgress(alloc, progress);
    try std.testing.expectEqual(@as(usize, 1), progress.len);
    try std.testing.expectEqualStrings(candidates[0].materialization_id, progress[0].materialization_id);
    try std.testing.expectEqualStrings("backfilling", progress[0].lifecycle);

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqual(@as(u32, 42), stats.indexes[0].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", stats.indexes[0].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("stale", stats.indexes[0].algebraic_capability_lifecycle_status.?);
    try std.testing.expect(!stats.indexes[0].algebraic_planner_lifecycle_ready);
    try std.testing.expectEqualStrings("capability_lifecycle_not_ready", stats.indexes[0].algebraic_planner_lifecycle_blocking_reason.?);
    try std.testing.expectEqual(@as(u32, 1), stats.indexes[0].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 2), stats.indexes[0].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 3), stats.indexes[0].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 4), stats.indexes[0].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 5), stats.indexes[0].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 6), stats.indexes[0].algebraic_skipped_unbounded_fields);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_candidate_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_progress_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_backfilling_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_decision_history_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_policy_drift_count);
    const top_candidate = stats.indexes[0].algebraic_top_candidate orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(candidates[0].recommendation, top_candidate.recommendation);
    try std.testing.expectEqualStrings(candidates[0].materialization_id, top_candidate.materialization_id);
    try std.testing.expectEqualStrings("backfilling", top_candidate.lifecycle);
    try std.testing.expectEqualStrings("auto_backfill_started", top_candidate.decision);
    try std.testing.expectEqual(candidates[0].score, top_candidate.score);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes[0].algebraic_candidate_decision_history.len);
    try std.testing.expectEqualStrings("auto_backfill_started", stats.indexes[0].algebraic_candidate_decision_history[0].decision);
    const active_progress = stats.indexes[0].algebraic_active_progress orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(progress[0].recommendation, active_progress.recommendation);
    try std.testing.expectEqualStrings(progress[0].materialization_id, active_progress.materialization_id);
    try std.testing.expectEqualStrings("backfilling", active_progress.lifecycle);
    try std.testing.expectEqual(progress[0].target_sequence, active_progress.target_sequence);
}

test "db schema runtime enrichment catalog delete rejects referenced definitions" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
        .chunk_overlap = 2,
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.chunk, "body_chunks_v1"));
}

test "db schema runtime enrichment catalog delete rejects asset referenced by chunk enrichment" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    });

    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.asset, "document_units_v1"));
}

test "db schema runtime enrichment catalog upsert rejects replacing referenced asset with chunk" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.upsertEnrichment(.{
        .name = "document_units_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    }));

    var asset = (try db.getEnrichment(alloc, .asset, "document_units_v1")).?;
    defer asset.deinit(alloc);
    try std.testing.expectEqual(types.EnrichmentKind.asset, asset.kind);
}

test "db schema runtime enrichment catalog upsert rejects replacing referenced chunk with asset" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    });
    try db.addEnrichment(.{
        .name = "document_dense_v1",
        .kind = .embedding,
        .source_artifact_name = "document_chunks_v1",
        .field = "text",
        .expected_dims = 3,
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    }));

    var chunk = (try db.getEnrichment(alloc, .chunk, "document_chunks_v1")).?;
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(types.EnrichmentKind.chunk, chunk.kind);
}

test "db schema runtime enrichment catalog upsert rejects replacing indexed chunk with asset" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    });
    try db.addIndex(.{
        .name = "body_text",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.upsertEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    }));

    var chunk = (try db.getEnrichment(alloc, .chunk, "body_chunks_v1")).?;
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(types.EnrichmentKind.chunk, chunk.kind);
}

test "db schema runtime enrichment catalog add rejects duplicate names across enrichment kinds" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_artifact_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try std.testing.expectError(error.EnrichmentAlreadyExists, db.addEnrichment(.{
        .name = "document_artifact_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    }));
}

test "db schema runtime enrichment catalog add allows unrelated definitions after field sparse index" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
}

test "db schema runtime index inspection lists graph indexes" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try std.testing.expect(db.hasIndex("graph_v1"));
    try std.testing.expect(!db.hasIndex("missing_graph"));

    const indexes = try db.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, indexes);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqualStrings("graph_v1", indexes[0].name);
    try std.testing.expectEqual(types.IndexKind.graph, indexes[0].kind);
}

test "db schema runtime delete index clears derived coverage rows" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_delete",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2}",
    });
    try db.addIndex(.{
        .name = "dv_keep",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2}",
    });

    const delete_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "dv_delete", 7, "doc:a");
    defer alloc.free(delete_key);
    const delete_counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, "dv_delete", 7, "skipped");
    defer alloc.free(delete_counter_key);
    const keep_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "dv_keep", 7, "doc:a");
    defer alloc.free(keep_key);

    var counter_value: [8]u8 = undefined;
    try db.core.store.putBatch(&.{
        .{ .key = delete_key, .value = "skipped" },
        .{ .key = delete_counter_key, .value = internal_keys.encodeDerivedCoverageOutcomeCount(&counter_value, 1) },
        .{ .key = keep_key, .value = "skipped" },
    }, &.{});

    try std.testing.expect(try db.deleteIndex("dv_delete"));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, delete_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, delete_counter_key));

    const kept = try db.core.store.get(alloc, keep_key);
    defer alloc.free(kept);
    try std.testing.expectEqualStrings("skipped", kept);
}

test "db schema runtime managed index deletion commits catalog absence with marker removal" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const SchemaRuntime = Impl(DB);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try std.testing.expect((try db.admitManagedFullTextIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = "{}",
        })) != null);

        SchemaRuntime.setTestFailManagedIndexDeleteAfterCatalogCommit(true);
        defer SchemaRuntime.setTestFailManagedIndexDeleteAfterCatalogCommit(false);
        try std.testing.expectError(error.TestManagedIndexDeleteCrash, db.deleteIndex(index_name));
        SchemaRuntime.setTestFailManagedIndexDeleteAfterCatalogCommit(false);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.get(index_name) == null);
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
}

test "db schema runtime managed index deletion remains successful after post-commit cleanup failure" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const SchemaRuntime = Impl(DB);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const old_repair_id = (try db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;

    index_manager_mod.test_inject_index_removal_cleanup_error = error.TestPostCommitCleanup;
    defer index_manager_mod.test_inject_index_removal_cleanup_error = null;
    index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(std.math.maxInt(u32), .release);
    defer index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    SchemaRuntime.setTestFailManagedIndexRepairCleanupAfterCatalogCommit(true);
    defer SchemaRuntime.setTestFailManagedIndexRepairCleanupAfterCatalogCommit(false);
    try std.testing.expect(try db.deleteIndex(index_name));
    index_manager_mod.test_inject_index_removal_cleanup_error = null;
    SchemaRuntime.setTestFailManagedIndexRepairCleanupAfterCatalogCommit(false);

    try std.testing.expect(db.core.index_manager.get(index_name) == null);
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, admission_key));
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));

    try std.testing.expectError(error.IndexArtifactCleanupPending, db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    }));
    index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    const new_repair_id = (try db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;
    try std.testing.expect(new_repair_id != old_repair_id);
    try std.testing.expect(db.core.index_manager.get(index_name) != null);
}
