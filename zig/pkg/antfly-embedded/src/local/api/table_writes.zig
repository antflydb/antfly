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

pub const physical_local_write = if (@import("storage_source_options").control_only) struct {} else @import("antfly_source_root").antfly_sources.local_write;
pub const std = @import("std");
pub const storage_source_options = @import("storage_source_options");
pub const control_only_storage_sources = storage_source_options.control_only;
pub const backups_api = @import("backups.zig");
pub const db_mod = if (control_only_storage_sources)
    @import("../storage/db/control_root.zig")
else
    @import("antfly_source_root").antfly_sources.selected_db;
pub const transactions_mod = @import("../storage/transactions.zig");
pub const backend_types = @import("../storage/backend_types.zig");
pub const portable_backup = @import("../storage/portable_backup.zig");
pub const table_catalog = @import("routing_budget.zig");
pub const table_write_source = @import("table_write_source.zig");
pub const table_index_config = @import("table_index_config.zig");
pub const tables_api = @import("tables.zig");
pub const runtime_status = @import("runtime_status.zig");
pub const nativeSnapshotAttemptTokenAlloc = physical_local_write.nativeSnapshotAttemptTokenAlloc;

pub const createNativeSnapshotAttemptMarker = physical_local_write.createNativeSnapshotAttemptMarker;

pub const reclaimStaleNativeSnapshotAttempts = physical_local_write.reclaimStaleNativeSnapshotAttempts;

pub const applyGraphMetricActionToDb = physical_local_write.applyGraphMetricActionToDb;
pub const runGraphMetricMaintenanceOrActionJsonAlloc = physical_local_write.runGraphMetricMaintenanceOrActionJsonAlloc;

pub const distributed_txn = @import("transaction_contract.zig");
pub const platform_time = @import("antfly_platform").time;
pub const Io = std.Io;

pub var txn_id_nonce: @import("antfly_platform").atomic.Value(u64) = .init(0);
pub fn repairNativeRestoreProjectionsUntilCompleteWithIo(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    io: Io,
    cancellation: db_mod.types.CancellationToken,
) !void {
    var cancel_ctx = db_mod.types.RepairCancellation{ .token = cancellation };
    var attempts: usize = 0;
    while (true) {
        try cancellation.check();
        attempts += 1;
        if (try db.repairNativeRestoreProjectionIntentsStep(alloc, .{
            .cancel_check = cancel_ctx.check(),
        })) break;
        io.sleep(Io.Duration.fromMilliseconds(100), .awake) catch |err| switch (err) {
            error.Canceled => Io.recancel(io),
        };
    }
    try db.syncIndexes(true);
    std.log.info("native restore projection repair complete attempts={d}", .{attempts});
}

pub const TableWriteSource = table_write_source.TableWriteSource;
pub fn ensurePreDecisionContextActive(context: distributed_txn.PreDecisionContext) !void {
    try context.cancellation.check();
    if (context.deadline_ns) |deadline_ns| {
        // This check is deliberately adjacent to mutation admission. Keep its
        // error distinct from generic storage and transport deadlines so only
        // this proven pre-proposal outcome may authorize replica failover.
        const now_ns = (table_catalog.RoutingBudget{ .io = context.deadline_io }).nowNs();
        if (now_ns >= deadline_ns) return error.PreDecisionDeadlineExceeded;
    }
}

pub const BoundTableWriteSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,
    /// Optional physical owner identity for callers that bind one routed group.
    /// Namespace shard IDs are document identity, not storage group identity.
    owner_group_id: ?u64 = null,

    pub fn init(table_name: []const u8, db: *db_mod.DB) BoundTableWriteSource {
        return .{
            .table_name = table_name,
            .db = db,
        };
    }

    pub fn activeDb(self: *BoundTableWriteSource) !*db_mod.DB {
        if (self.db.isClosed()) return error.StorageUnavailable;
        return self.db;
    }

    pub fn source(self: *BoundTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_table = createTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .put_artifact_enrichment = putArtifactEnrichment,
                .delete_artifact_enrichment = deleteArtifactEnrichment,
                .drop_index = dropIndex,
                .graph_metric_action = graphMetricAction,
                .graph_metric_action_with_cancellation = graphMetricActionWithCancellation,
                .graph_metric_maintenance_group_local = graphMetricMaintenanceGroupLocal,
                .backup_table = backupTable,
                .backup_pin_control = backupPinControl,
                .restore_table = restoreTable,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_cancellation = commitTransactionWithCancellation,
                .commit_batch = commitBatch,
                .commit_batch_with_cancellation = commitBatchWithCancellation,
                .commit_transaction_with_id = commitTransactionWithId,
                .commit_transaction_with_id_with_cancellation = commitTransactionWithIdAndCancellation,
                .acknowledge_transaction_commit = acknowledgeTransactionCommit,
                .batch = batch,
                .begin_bulk_ingest = beginBulkIngest,
                .finish_bulk_ingest = finishBulkIngest,
                .abort_bulk_ingest = abortBulkIngest,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_begin_group_local_with_pre_decision_context = txnBeginGroupLocalWithPreDecisionContext,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_prepare_group_local_with_pre_decision_context = txnPrepareGroupLocalWithPreDecisionContext,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_resolve_group_local_with_cancellation = txnResolveGroupLocalWithCancellation,
                .txn_status_group_local = txnStatusGroupLocal,
                .txn_acknowledge_group_local = txnAcknowledgeGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .reprocess_document_artifact = reprocessDocumentArtifact,
                .reprocess_document_artifact_range = reprocessDocumentArtifactRange,
                .list_artifact_repair_issues = listArtifactRepairIssues,
                .repair_artifact_issues = repairArtifactIssues,
                .repair_artifact_issues_controlled = repairArtifactIssuesControlled,
                .list_artifact_repair_issues_group_local = listArtifactRepairIssuesGroupLocal,
                .vector_migration_group_local = vectorMigrationGroupLocal,
                .repair_artifact_issues_group_local = repairArtifactIssuesGroupLocal,
                .repair_artifact_issues_group_local_controlled = repairArtifactIssuesGroupLocalControlled,
                .update_document_artifact_child_range_placement = updateDocumentArtifactChildRangePlacement,
                .apply_document_artifact_child_range_batch = applyDocumentArtifactChildRangeBatch,
                .apply_document_artifact_child_range_batch_group_local = applyDocumentArtifactChildRangeBatchGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
            },
        };
    }

    pub fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const db = try self.activeDb();
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
        items[0] = .{
            .group_id = 0,
            .stats = try db.runtimeStatusStatsConsistent(alloc),
        };
        return .{ .items = items };
    }

    pub fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!try corruptEmbeddingArtifactInDb(alloc, try self.activeDb(), doc_key, index_name)) return error.NotFound;
    }

    pub fn reprocessDocumentArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).reprocessDocumentArtifact(alloc, doc_key, artifact_name);
    }

    pub fn reprocessDocumentArtifactRange(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).reprocessDocumentArtifactRange(alloc, artifact_name, req);
    }

    pub fn listArtifactRepairIssues(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !?db_mod.types.ArtifactRepairListResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).listArtifactRepairIssuesPage(alloc, req);
    }

    pub fn repairArtifactIssues(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
    ) !?db_mod.types.ArtifactRepairResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).repairArtifactIssuesWithRequest(alloc, req);
    }

    pub fn repairArtifactIssuesControlled(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
        options: db_mod.types.ArtifactRepairRunOptions,
    ) !?db_mod.types.ArtifactRepairResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).repairArtifactIssuesWithRequestOptions(alloc, req, options);
    }

    pub fn vectorMigrationGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request_json: []const u8) !?[]u8 {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        _ = group_id;
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        var command = try std.json.parseFromSlice(@import("../common/vector_migration.zig").Command, alloc, request_json, .{});
        defer command.deinit();
        return try (try self.activeDb()).vectorMigrationCommand(alloc, command.value);
    }

    pub fn listArtifactRepairIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !?db_mod.types.ArtifactRepairListResult {
        _ = group_id;
        return try listArtifactRepairIssues(ptr, alloc, table_name, req);
    }

    pub fn repairArtifactIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
    ) !?db_mod.types.ArtifactRepairResult {
        _ = group_id;
        return try repairArtifactIssues(ptr, alloc, table_name, req);
    }

    pub fn repairArtifactIssuesGroupLocalControlled(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
        options: db_mod.types.ArtifactRepairRunOptions,
    ) !?db_mod.types.ArtifactRepairResult {
        _ = group_id;
        return try repairArtifactIssuesControlled(ptr, alloc, table_name, req, options);
    }

    pub fn updateDocumentArtifactChildRangePlacement(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).updateDocumentArtifactChildRangePlacement(alloc, doc_key, artifact_name, update);
    }

    pub fn applyDocumentArtifactChildRangeBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        return try applyDocumentArtifactChildRangeBatchGroupLocal(ptr, alloc, group_id, table_name, doc_key, artifact_name, child_batch);
    }

    pub fn applyDocumentArtifactChildRangeBatchGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        _: []const u8,
        _: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try (try self.activeDb()).applyDocumentArtifactChildRangeBatch(child_batch);
    }

    pub fn createTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const db = try self.activeDb();

        const raw_indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        try db.configureTableStorage(req.storage orelse db.local_execution.table_storage);
        const schema_json = tables_api.effectiveSchemaJson(req.schema_json);
        const expanded_indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(alloc, table_name, raw_indexes_json, schema_json);
        defer alloc.free(expanded_indexes_json);
        const indexes_json = expanded_indexes_json;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |object| object,
            else => return error.InvalidCreateTableRequest,
        };

        var it = object.iterator();
        while (it.next()) |entry| {
            const kind = try parseIndexKind(entry.value_ptr.*);
            const config_json = try extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
            defer alloc.free(config_json);
            try db.addIndex(.{
                .name = entry.key_ptr.*,
                .kind = kind,
                .config_json = config_json,
            });
        }

        try applyLocalTableSchemaJson(alloc, db, schema_json);
    }

    pub fn updateSchema(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try applyLocalTableSchemaJson(alloc, try self.activeDb(), schema_json);
    }

    pub fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const db = try self.activeDb();
        try validateTableBatchAgainstLocalSchema(alloc, db, req.writes, req.deletes, req.transforms);
        try db.batch(req);
    }

    pub fn beginBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try (try self.activeDb()).beginBulkIngestSession();
    }

    pub fn finishBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try (try self.activeDb()).finishBulkIngestSessionWithOptions(options);
    }

    pub fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return;
        const db = self.activeDb() catch return;
        db.abortBulkIngestSession();
    }

    pub fn backupPinControl(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: @import("../storage/db/native_backup_seal.zig").Request, control: backups_api.BackupOperationControl) !?[]u8 {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try executeBackupPinControl(alloc, try self.activeDb(), group_id, request, control);
    }

    pub fn backupTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (plan.target_group_id) |group_id| {
            if (group_id != 0) return error.NotFound;
        }
        try plan.ensureActive();
        const db = try self.activeDb();
        if (plan.format == .portable) {
            return try exportPortableBackupShardWithSeal(alloc, db, plan.backup_root, plan.backup_id, 0, plan.io, try @import("backup_contract.zig").sealedHandleForGroup(plan.sealed_handles, 0), plan.cancellation);
        }

        const snapshot_io = plan.io orelse db.backend_runtime.filesystemIo() orelse
            return error.BackendRuntimeIoUnavailable;
        try reclaimStaleNativeSnapshotAttempts(alloc, snapshot_io, db.core.path);
        const snapshot_token = try nativeSnapshotAttemptTokenAlloc(alloc, snapshot_io, plan.backup_id, "local");
        defer alloc.free(snapshot_token);
        var snapshot_attempt = try createNativeSnapshotAttemptMarker(
            alloc,
            snapshot_io,
            db.core.path,
            snapshot_token,
            platform_time.realtimeNs(),
        );
        defer snapshot_attempt.deinit();
        _ = if (try @import("backup_contract.zig").sealedHandleForGroup(plan.sealed_handles, 0)) |handle|
            try db.exportBackupCohort(handle.handle, snapshot_token, plan.cancellation)
        else if (plan.relational_cohort_fence) |cohort|
            try db.snapshotRelationalCohort(snapshot_token, cohort, plan.cancellation)
        else
            try db.snapshotNativeWithCancellation(snapshot_token, plan.cancellation);

        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db.core.path, snapshot_token });
        defer alloc.free(snapshot_root);
        defer deleteLocalNativeSnapshot(snapshot_io, snapshot_root);
        const dest_root = try backups_api.shardSnapshotPath(alloc, plan.backup_root, plan.backup_id, 0);
        defer alloc.free(dest_root);
        const rel_path = try backups_api.shardSnapshotRelPath(alloc, plan.backup_id, 0);
        errdefer alloc.free(rel_path);
        const byte_range = db.getRange();
        const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
        shards[0] = .{
            .group_id = 0,
            .start_key = try alloc.dupe(u8, byte_range.start),
            .end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null,
            .snapshot_path = rel_path,
        };
        errdefer shards[0].deinit(alloc);
        var integrity = try backups_api.copyNativeDirectoryWithIntegrityUsingIo(
            alloc,
            snapshot_io,
            snapshot_root,
            dest_root,
            plan.cancellation,
        );
        shards[0].artifact_size_bytes = integrity.size_bytes;
        shards[0].artifact_sha256 = integrity.sha256;
        integrity = undefined;
        var native_manifest_integrity = try backups_api.nativeGenerationManifestIntegrityAllocWithCancellation(
            alloc,
            snapshot_io,
            dest_root,
            plan.cancellation,
        );
        shards[0].native_manifest_size_bytes = native_manifest_integrity.size_bytes;
        shards[0].native_manifest_sha256 = native_manifest_integrity.sha256;
        native_manifest_integrity = undefined;
        return shards;
    }

    pub fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try backups_api.validateSingleRangeRestoreManifestLayout(plan.manifest);
        try backups_api.validateRestoreManifest(alloc, plan.manifest, plan.manifest.backup_id);
        if (plan.reconcile_only) return error.RestoreIdentityMismatch;
        const db = try self.activeDb();

        const native_restore_plan: ?db_mod.NativeRestoreOpenPlan = if (plan.manifest.format == .native) blk: {
            const resolved = try db_mod.DB.resolveNativeRestoreOpenPlan(db.core.path, .{
                .primary_backend = db.primary_backend,
                .backend_runtime = db.backend_runtime,
            });
            // External namespaces require a backend-owned stage/promote
            // capability. Fail before integrity-scanning corpus bytes or
            // closing the currently serving DB.
            if (resolved.physicalRootMode() != .filesystem_managed)
                return error.NativeBackupStorageBackendUnsupported;
            break :blk resolved;
        } else null;

        const shard = &plan.manifest.shards[0];
        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ plan.backup_root, shard.snapshot_path });
        defer alloc.free(snapshot_root);
        try backups_api.verifyRestorableShardArtifactIntegrityWithCancellation(
            alloc,
            plan.io,
            plan.manifest.format,
            snapshot_root,
            shard,
            plan.cancellation,
        );

        const db_path = try alloc.dupe(u8, db.core.path);
        defer alloc.free(db_path);
        const primary_backend = db.primary_backend;
        var owned_backend_runtime = db.owned_backend_runtime;
        db.owned_backend_runtime = null;
        errdefer if (owned_backend_runtime) |*runtime| runtime.deinit();
        const backend_runtime = if (owned_backend_runtime) |*runtime|
            runtime.runtime
        else
            db.backend_runtime;
        const identity_namespace = db.core.identity_namespace;

        db.close();
        const recovery_open_options: db_mod.OpenOptions = .{
            .primary_backend = primary_backend,
            .backend_runtime = backend_runtime,
            .identity_namespace = identity_namespace,
        };
        // A restore publishes the snapshot's document-identity generation. The
        // pre-restore namespace is valid only when reopening the old generation
        // after a failed publication attempt.
        const restored_open_options: db_mod.OpenOptions = .{
            .primary_backend = primary_backend,
            .backend_runtime = backend_runtime,
        };
        var effective_recovery_open_options = recovery_open_options;
        var effective_restored_open_options = restored_open_options;
        if (native_restore_plan) |resolved| {
            effective_recovery_open_options = try resolved.optionsForTarget(db_path);
            effective_recovery_open_options.identity_namespace = identity_namespace;
            effective_restored_open_options = try resolved.optionsForTarget(db_path);
        }
        const publication_outcome = restoreBoundTableGeneration(
            alloc,
            snapshot_root,
            db_path,
            effective_restored_open_options,
            native_restore_plan,
            plan,
        ) catch |restore_err| {
            self.db.* = db_mod.DB.open(alloc, db_path, effective_recovery_open_options) catch |reopen_err| {
                std.log.err("bound restore recovery failed phase=reopen restore_class={s} reopen_class={s}", .{
                    @errorName(restore_err),
                    @errorName(reopen_err),
                });
                return reopen_err;
            };
            self.db.owned_backend_runtime = owned_backend_runtime;
            owned_backend_runtime = null;
            return restore_err;
        };
        self.db.* = db_mod.DB.open(alloc, db_path, effective_restored_open_options) catch |reopen_err| {
            std.log.err("bound restore recovery failed phase=published_reopen class={s}", .{@errorName(reopen_err)});
            return reopen_err;
        };
        self.db.owned_backend_runtime = owned_backend_runtime;
        owned_backend_runtime = null;
        if (publication_outcome == .durability_uncertain) return error.GenerationDurabilityUncertain;
    }

    pub fn restoreBoundTableGeneration(
        alloc: std.mem.Allocator,
        snapshot_root: []const u8,
        live_path: []const u8,
        open_options: db_mod.OpenOptions,
        native_restore_plan: ?db_mod.NativeRestoreOpenPlan,
        plan: backups_api.TableRestorePlan,
    ) !db_mod.generation_lifecycle.PublicationOutcome {
        try plan.cancellation.check();
        const backend_runtime = open_options.backend_runtime orelse return error.MissingBackendRuntime;
        const restore_io = plan.io orelse backend_runtime.filesystemIo() orelse return error.MissingBackendRuntimeIo;
        var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithRuntimeAndIo(
            live_path,
            open_options.backend_runtime,
            restore_io,
        );
        defer transition.deinit();
        var staged = try transition.beginStaging();
        defer staged.deinit();
        const candidate_open_options = if (native_restore_plan) |resolved|
            try resolved.optionsForStagedGeneration(&staged)
        else
            open_options;

        switch (plan.manifest.format) {
            .portable => {
                var staged_open_options = open_options;
                staged_open_options.staged_generation = &staged;
                var restored = try db_mod.DB.open(alloc, staged.path(), staged_open_options);
                defer restored.close();
                try importPortableBackupFileWithOptions(alloc, restored.core.store, snapshot_root, restore_io, .{
                    .unpublished_staging = true,
                    .cancellation = plan.cancellation,
                    .progress_context = plan.progress_context,
                    .progress_fn = plan.progress_fn,
                });
                try restored.reloadSchemaForInternalRestore();
                try plan.cancellation.check();
                _ = try restored.rebuildDenseIndexesForTargetCoverage(alloc);
                try plan.cancellation.check();
                _ = try restored.rebuildSparseIndexesForTargetCoverage(alloc);
                try plan.cancellation.check();
                try restored.rebuildGraphIndexesForTargetCoverage(alloc);
                try restored.syncIndexes(true);
            },
            .native => {
                const restored_native_generation = try db_mod.DB.restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(
                    &staged,
                    alloc,
                    restore_io,
                    snapshot_root,
                    staged.path(),
                    candidate_open_options,
                    plan.cancellation,
                );
                if (!restored_native_generation) {
                    // Legacy native backups contain only the primary store.
                    // Complete their derived indexes in the restore job without
                    // imposing a foreground deadline or inventing a Raft repair
                    // identity for this process-local database.
                    var staged_open_options = candidate_open_options;
                    staged_open_options.staged_generation = &staged;
                    var restored = try db_mod.DB.open(alloc, staged.path(), staged_open_options);
                    defer restored.close();
                    try plan.cancellation.check();
                    _ = try restored.rebuildDenseIndexesForTargetCoverage(alloc);
                    try plan.cancellation.check();
                    _ = try restored.rebuildSparseIndexesForTargetCoverage(alloc);
                    try plan.cancellation.check();
                    try restored.rebuildGraphIndexesForTargetCoverage(alloc);
                    try restored.syncIndexes(true);
                } else {
                    // Native validation may have retained healthy projections
                    // while creating durable intents for only the damaged or
                    // incompatible ones. Keep the candidate unservable until
                    // every such intent has activated and validated.
                    var staged_open_options = candidate_open_options;
                    staged_open_options.open_mode = .writer_no_replay;
                    staged_open_options.staged_generation = &staged;
                    staged_open_options.start_index_workers = false;
                    staged_open_options.start_optional_runtimes = false;
                    staged_open_options.start_optional_runtime_workers = false;
                    var restored = try db_mod.DB.open(alloc, staged.path(), staged_open_options);
                    defer restored.close();
                    try repairNativeRestoreProjectionsUntilCompleteWithIo(
                        alloc,
                        &restored,
                        restore_io,
                        plan.cancellation,
                    );
                }
            },
        }
        try plan.cancellation.check();
        return try staged.publish();
    }

    pub fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        return try commitTransactionWithCancellation(ptr, alloc, tables, sync_level, .none);
    }

    pub fn commitTransactionWithCancellation(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        const txn_source: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        const txn_io: ?Io = txn_source.db.backend_runtime.io();
        const txn_id = nextTxnId(txn_io);
        return try commitBoundTransaction(ptr, alloc, txn_id, nextTxnTimestamp(txn_io), tables, sync_level, false, cancellation);
    }

    pub fn commitBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        return try commitBatchWithCancellation(ptr, alloc, tables, sync_level, .none);
    }

    pub fn commitBatchWithCancellation(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        if (tables.len == 1 and tables[0].predicates.len == 0 and tables[0].relational_index_maintenance == null) {
            const table = tables[0];
            const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, self.table_name, table.table_name)) return null;
            const db = try self.activeDb();
            const req: db_mod.types.BatchRequest = .{
                .writes = transactionWritesAsBatchWrites(table.writes),
                .deletes = table.deletes,
                .transforms = table.transforms,
                .sync_level = sync_level,
            };
            try validateTableBatchAgainstLocalSchema(alloc, db, req.writes, req.deletes, req.transforms);
            db.batchWithVisibilityCancellation(req, cancellation) catch |err| switch (err) {
                error.IntentConflict, error.VersionConflict => return .{ .conflict = boundConflict(table, err) },
                else => return err,
            };
            return .{ .committed = .{ .participant_count = 1 } };
        }
        const txn_source: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        const txn_io: ?Io = txn_source.db.backend_runtime.io();
        const txn_id = nextTxnId(txn_io);
        return try commitBoundTransaction(ptr, alloc, txn_id, nextTxnTimestamp(txn_io), tables, sync_level, false, cancellation);
    }

    pub fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        return try commitTransactionWithIdAndCancellation(ptr, alloc, txn_id, begin_timestamp, tables, sync_level, .none);
    }

    pub fn commitTransactionWithIdAndCancellation(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        return try commitBoundTransaction(ptr, alloc, txn_id, begin_timestamp, tables, sync_level, true, cancellation);
    }

    pub fn acknowledgeTransactionCommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        _: u64,
        coordinator_table_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, coordinator_table_name)) return null;
        const participant = try distributed_txn.participantIdForGroup(alloc, coordinator_table_name, 0);
        defer alloc.free(participant);
        try (try self.activeDb()).markTransactionParticipantResolved(txn_id, participant);
        return {};
    }

    pub fn commitBoundTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        retain_terminal: bool,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (tables.len != 1) return error.UnsupportedOperation;
        const table = tables[0];
        if (!std.mem.eql(u8, self.table_name, table.table_name)) return null;

        if (table.relational_index_maintenance) |command| if (command.owner_group_id != (self.owner_group_id orelse return error.UnsupportedOperation)) return error.PreparedGenerationChanged;
        const db = try self.activeDb();
        if (table.range_guards.len != 0) return error.SqlStatementSnapshotRequired;
        try validateTransactionAgainstLocalSchema(alloc, db, txn_id, table.writes, table.deletes, table.transforms);
        const commit_version = begin_timestamp + 1;
        const local_participant = try distributed_txn.participantIdForGroup(alloc, table.table_name, 0);
        defer alloc.free(local_participant);
        const participants = [_][]const u8{local_participant};

        _ = db.beginTransactionWithIdAndParticipantsCreatedAtRoleAndRetention(
            txn_id,
            begin_timestamp,
            nextTxnTimestamp(db.backend_runtime.io()),
            &participants,
            true,
            retain_terminal,
        ) catch |err| switch (err) {
            error.DecisionConflict => switch (try db.getTransactionStatus(txn_id)) {
                .committed => {
                    db.resolveTransactionIntentsWithSyncLevelAndCancellation(txn_id, .committed, commit_version, sync_level, cancellation) catch |barrier_err| {
                        const durable_status = db.getTransactionStatus(txn_id) catch return barrier_err;
                        if (durable_status != .committed) return barrier_err;
                        var propagation_pending = false;
                        if (!retain_terminal) {
                            db.markTransactionParticipantResolved(txn_id, local_participant) catch {
                                propagation_pending = true;
                            };
                        }
                        return .{ .committed = .{
                            .participant_count = 1,
                            .coordinator_group_id = if (retain_terminal) 0 else null,
                            .coordinator_table_name = if (retain_terminal) table.table_name else null,
                            .propagation_pending = propagation_pending,
                            .visibility_pending = true,
                            .visibility_retry_pending = barrier_err != error.EnrichmentWorkerFailed,
                            .visibility_repair_required = barrier_err == error.EnrichmentWorkerFailed,
                        } };
                    };
                    var propagation_pending = false;
                    if (!retain_terminal) {
                        db.markTransactionParticipantResolved(txn_id, local_participant) catch {
                            propagation_pending = true;
                        };
                    }
                    return .{ .committed = .{
                        .participant_count = 1,
                        .coordinator_group_id = if (retain_terminal) 0 else null,
                        .coordinator_table_name = if (retain_terminal) table.table_name else null,
                        .propagation_pending = propagation_pending,
                    } };
                },
                .aborted => {
                    db.markTransactionParticipantResolved(txn_id, local_participant) catch |ack_err| {
                        std.log.warn("bound transaction abort acknowledgement retry deferred txn_id={x} err={s}", .{
                            txn_id,
                            @errorName(ack_err),
                        });
                    };
                    return .{ .conflict = boundConflict(table, error.DecisionConflict) };
                },
                .pending => return error.TransactionBeginFailed,
            },
            else => return err,
        };
        db.writeTransaction(txn_id, .{
            .writes = table.writes,
            .deletes = table.deletes,
            .transforms = table.transforms,
            .predicates = table.predicates,
            .integrity = table.integrity,
            .integrity_commands = table.integrity_commands,
            .relational_activation = table.relational_activation,
            .relational_retirement = table.relational_retirement,
            .relational_index_maintenance = table.relational_index_maintenance,
            .schema_version = table.schema_version,
            .relational_schema_version = table.relational_schema_version,
            .relational_integrity_generation_set = table.relational_integrity_generation_set,
            .restore_staging_scope = table.restore_staging_scope,
            .restore_staging_plan_id = table.restore_staging_plan_id,
            .relational_repair = table.relational_repair,
        }) catch |err| {
            // A begun local transaction must reach a terminal state on every
            // rejected write. In particular, graph transform validation is
            // performed by DB.writeTransaction after begin, so returning the
            // validation error without aborting would strand its transaction.
            db.resolveTransactionIntents(txn_id, .aborted, commit_version) catch |abort_err| {
                std.log.err("failed to abort rejected bound transaction write_err={s} abort_err={s}", .{
                    @errorName(err),
                    @errorName(abort_err),
                });
                return abort_err;
            };
            db.markTransactionParticipantResolved(txn_id, local_participant) catch |ack_err| {
                // The abort is already durable; recovery can finish this
                // idempotent cleanup without changing the client result.
                std.log.warn("bound transaction abort acknowledgement deferred txn_id={x} err={s}", .{
                    txn_id,
                    @errorName(ack_err),
                });
            };
            switch (err) {
                error.VersionConflict, error.IntentConflict => return .{ .conflict = boundConflict(table, err) },
                error.InvalidBatchRequest,
                error.InvalidArgument,
                error.InvalidGraphEdges,
                error.UnsupportedTransformOperation,
                => return error.InvalidBatchRequest,
                else => return err,
            }
        };
        db.resolveTransactionIntentsWithSyncLevelAndCancellation(txn_id, .committed, commit_version, sync_level, cancellation) catch |err| {
            const durable_status = db.getTransactionStatus(txn_id) catch return err;
            if (durable_status == .committed) {
                std.log.warn("bound transaction acknowledged after durable commit barrier failure txn_id={x} err={s}", .{
                    txn_id,
                    @errorName(err),
                });
                var propagation_pending = false;
                if (!retain_terminal) {
                    db.markTransactionParticipantResolved(txn_id, local_participant) catch {
                        propagation_pending = true;
                    };
                }
                return .{ .committed = .{
                    .participant_count = 1,
                    .coordinator_group_id = if (retain_terminal) 0 else null,
                    .coordinator_table_name = if (retain_terminal) table.table_name else null,
                    .propagation_pending = propagation_pending,
                    .visibility_pending = true,
                    .visibility_retry_pending = err != error.EnrichmentWorkerFailed,
                    .visibility_repair_required = err == error.EnrichmentWorkerFailed,
                } };
            }
            return err;
        };
        var propagation_pending = false;
        if (!retain_terminal) {
            db.markTransactionParticipantResolved(txn_id, local_participant) catch {
                propagation_pending = true;
            };
        }
        return .{ .committed = .{
            .participant_count = 1,
            .coordinator_group_id = if (retain_terminal) 0 else null,
            .coordinator_table_name = if (retain_terminal) table.table_name else null,
            .propagation_pending = propagation_pending,
        } };
    }

    pub fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const db = try self.activeDb();
        const schema_json = try loadLocalTableSchemaJson(alloc, db);
        defer if (schema_json) |value| alloc.free(value);
        const expanded_index_json = try tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, tables_api.effectiveSchemaJson(schema_json));
        defer alloc.free(expanded_index_json);
        const cfg = try parseIndexConfig(alloc, index_name, expanded_index_json);
        defer {
            alloc.free(cfg.name);
            alloc.free(cfg.config_json);
        }
        try db.addIndex(cfg);
    }

    pub fn putArtifactEnrichment(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        enrichment_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        var parsed = try std.json.parseFromSlice(db_mod.types.EnrichmentConfig, alloc, enrichment_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        if (!std.mem.eql(u8, parsed.value.name, artifact_name)) return error.InvalidEnrichmentConfig;
        _ = try (try self.activeDb()).upsertEnrichment(parsed.value);
    }

    pub fn deleteArtifactEnrichment(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try deleteArtifactEnrichmentFromDbByName(alloc, try self.activeDb(), artifact_name);
    }

    pub fn dropIndex(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try (try self.activeDb()).deleteIndex(index_name);
    }

    pub fn graphMetricAction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
    ) !?db_mod.types.GraphMetricStatus {
        return try graphMetricActionWithCancellation(ptr, alloc, table_name, index_name, metric_name, action, .none);
    }

    pub fn graphMetricActionWithCancellation(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
        cancellation: db_mod.types.CancellationToken,
    ) !?db_mod.types.GraphMetricStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (cancellation.isCancelled()) return error.Canceled;
        return try applyGraphMetricActionToDb(alloc, try self.activeDb(), index_name, metric_name, action);
    }

    pub fn graphMetricMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try runGraphMetricMaintenanceOrActionJsonAlloc(alloc, try self.activeDb(), body);
    }

    pub fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try batch(ptr, alloc, table_name, req);
    }

    pub fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
    ) !?void {
        return try txnBeginGroupLocalWithPreDecisionContext(ptr, alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, retain_terminal, participants, .{});
    }

    pub fn txnBeginGroupLocalWithPreDecisionContext(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        _: u64,
        retain_terminal: bool,
        participants: []const []const u8,
        context: distributed_txn.PreDecisionContext,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try ensurePreDecisionContextActive(context);
        _ = try (try self.activeDb()).beginTransactionWithIdAndParticipantsCreatedAtRoleAndRetention(
            txn_id,
            begin_timestamp,
            nextTxnTimestamp(self.db.backend_runtime.io()),
            participants,
            true,
            retain_terminal,
        );
    }

    pub fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        return try txnPrepareGroupLocalWithPreDecisionContext(ptr, alloc, group_id, table_name, txn_id, topology_epoch, req, .{});
    }

    pub fn txnPrepareGroupLocalWithPreDecisionContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        _: u64,
        req: db_mod.types.TransactionIntentRequest,
        context: distributed_txn.PreDecisionContext,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try ensurePreDecisionContextActive(context);
        const db = try self.activeDb();
        if (context.route_fence != null) return error.CatalogRouteFenceUnsupported;
        try validateTransactionAgainstLocalSchema(alloc, db, txn_id, req.writes, req.deletes, req.transforms);
        if (req.relational_index_maintenance) |command| if (command.owner_group_id != (self.owner_group_id orelse return error.UnsupportedOperation)) return error.PreparedGenerationChanged;
        try ensurePreDecisionContextActive(context);
        try db.writeTransaction(txn_id, req);
    }

    pub fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
        topology_epoch: u64,
        sync_level: db_mod.types.SyncLevel,
    ) !?void {
        return try txnResolveGroupLocalWithCancellation(ptr, alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level, .none);
    }

    pub fn txnResolveGroupLocalWithCancellation(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const db = try self.activeDb();
        try db.resolveTransactionIntentsWithSyncLevelAndCancellation(txn_id, status, commit_version, sync_level, cancellation);
        const participant = try distributed_txn.participantIdForGroup(db.alloc, table_name, group_id);
        defer db.alloc.free(participant);
        db.markTransactionParticipantResolved(txn_id, participant) catch |err| switch (err) {
            transactions_mod.TxnError.TxnNotFound => if (status != .aborted) return err,
            else => return err,
        };
    }

    pub fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try (try self.activeDb()).getTransactionStatus(txn_id);
    }

    pub fn txnAcknowledgeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        participant: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = group_id;
        _ = alloc;
        try (try self.activeDb()).markTransactionParticipantResolved(txn_id, participant);
    }
};

pub const parseIndexKind = table_index_config.parseIndexKind;
pub const parseIndexConfig = table_index_config.parseIndexConfig;
pub const extractIndexConfigJson = table_index_config.extractIndexConfigJson;
pub fn nextTxnTimestamp(io: ?Io) u64 {
    // Transaction timestamps are stored in shard metadata and later compared
    // against transaction recovery cutoffs, so they must stay on realtime.
    if (io) |runtime_io| return @intCast(std.Io.Clock.real.now(runtime_io).toNanoseconds());
    return platform_time.realtimeNs();
}

pub fn nextTxnId(io: ?Io) db_mod.types.TxnId {
    var txn_id: db_mod.types.TxnId = undefined;
    if (io) |runtime_io| {
        runtime_io.random(&txn_id);
        return txn_id;
    }
    const nonce = txn_id_nonce.fetchAdd(1, .monotonic);
    std.mem.writeInt(u64, txn_id[0..8], nextTxnTimestamp(null), .big);
    std.mem.writeInt(u64, txn_id[8..16], nonce, .big);
    return txn_id;
}

pub fn boundConflict(table: distributed_txn.TableCommitRequest, err: anyerror) distributed_txn.CommitConflict {
    if (table.predicates.len > 0) {
        return .{
            .table_name = table.table_name,
            .key = table.predicates[0].key,
            .message = "version conflict",
            .phase = .prepare,
        };
    }
    const message = switch (err) {
        error.IntentConflict => "intent conflict",
        else => "transaction conflict",
    };
    if (table.writes.len > 0) {
        return .{ .table_name = table.table_name, .key = table.writes[0].key, .message = message, .phase = .prepare };
    }
    if (table.deletes.len > 0) {
        return .{ .table_name = table.table_name, .key = table.deletes[0], .message = message, .phase = .prepare };
    }
    return .{ .table_name = table.table_name, .key = "", .message = message, .phase = .prepare };
}

pub fn deleteArtifactEnrichmentFromDbByName(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    artifact_name: []const u8,
) !bool {
    const enrichments = try db.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);

    var deleted = false;
    for (enrichments) |cfg| {
        if (!std.mem.eql(u8, cfg.name, artifact_name)) continue;
        _ = try db.deleteEnrichment(cfg.kind, artifact_name);
        deleted = true;
    }
    return deleted;
}

pub const corruptEmbeddingArtifactInDb = physical_local_write.corruptEmbeddingArtifactInDb;

pub const loadLocalTableSchemaJson = physical_local_write.loadLocalTableSchemaJson;

pub const portableBackupShardRelPath = physical_local_write.portableBackupShardRelPath;

pub const deleteLocalNativeSnapshot = physical_local_write.deleteLocalNativeSnapshot;

pub const exportPortableBackupFile = physical_local_write.exportPortableBackupFile;

pub const exportPortableBackupFileWithSource = physical_local_write.exportPortableBackupFileWithSource;

pub fn importPortableBackupFileWithOptions(alloc: std.mem.Allocator, store: *db_mod.docstore.DocStore, path: []const u8, io: std.Io, options: portable_backup.ImportOptions) !void {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    try portable_backup.importPortableFileWithOptions(alloc, store, io, file, stat.size, options);
    try options.cancellation.check();
    portable_backup.validateCompleteDatabaseImageAlloc(alloc, store) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
    try options.cancellation.check();
}

pub fn transactionWritesToBatchWrites(
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

pub fn transactionWritesAsBatchWrites(
    writes: []const db_mod.types.TransactionWrite,
) []const db_mod.types.BatchWrite {
    comptime {
        std.debug.assert(@sizeOf(db_mod.types.TransactionWrite) == @sizeOf(db_mod.types.BatchWrite));
        std.debug.assert(@alignOf(db_mod.types.TransactionWrite) == @alignOf(db_mod.types.BatchWrite));
    }
    return @ptrCast(writes);
}

pub const validateTableBatchAgainstLocalSchema = physical_local_write.validateTableBatchAgainstLocalSchema;

// API preflight is advisory. Once a participant has a durable epoch/decision,
// validating a retry against the latest catalog can reject an already accepted
// write. DB preparation validates the pinned contract with its admission
// ledger; terminal retries resolve the existing decision instead of its input.
pub const transactionUsesDurableContract = physical_local_write.transactionUsesDurableContract;

pub fn validateTransactionAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    txn_id: db_mod.types.TxnId,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (try transactionUsesDurableContract(alloc, db, txn_id)) return;
    const batch_writes = try transactionWritesToBatchWrites(alloc, writes);
    defer alloc.free(batch_writes);
    try validateTableBatchAgainstLocalSchema(alloc, db, batch_writes, deletes, transforms);
}

pub const applyLocalTableSchemaJson = physical_local_write.applyLocalTableSchemaJson;

/// Applies the catalog-owned physical table contract when an opaque compiled
/// storage owner first opens a live group DB. This deliberately performs the
/// same schema and index reconciliation as the in-module managed writer path.
pub const executeBackupPinControl = @import("../storage/db/backup_pin_control.zig").execute;

pub fn exportPortableBackupShardWithSeal(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    backup_root: []const u8,
    backup_id: []const u8,
    group_id: u64,
    shared_io: ?std.Io,
    sealed: ?@import("backup_contract.zig").SealedHandle,
    cancellation: @import("operation.zig").CancellationToken,
) ![]backups_api.ShardSnapshot {
    const rel_path = try portableBackupShardRelPath(alloc, backup_id, group_id);
    errdefer alloc.free(rel_path);

    const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, rel_path });
    defer alloc.free(dest_path);
    var source_summary: ?[]@import("../storage/portable_backup.zig").SourceGenerationAdmissionSummaryEntry = null;
    errdefer if (source_summary) |entries| @import("../storage/portable_backup.zig").freeSourceGenerationAdmissionSummary(alloc, entries);
    if (sealed) |proof| {
        var fallback: ?std.Io.Threaded = if (shared_io == null) std.Io.Threaded.init(std.heap.page_allocator, .{}) else null;
        defer if (fallback) |*owned| owned.deinit();
        try exportPortableBackupFileWithSource(alloc, db.core.store, dest_path, shared_io orelse fallback.?.io(), .{ .db = db, .handle = proof.handle, .cancellation = cancellation, .source_generation_summary_output = &source_summary });
    } else try exportPortableBackupFile(alloc, db.core.store, dest_path, shared_io);

    const byte_range = db.getRange();
    const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
    shards[0] = .{
        .group_id = group_id,
        .start_key = try alloc.dupe(u8, byte_range.start),
        .end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null,
        .snapshot_path = rel_path,
    };
    errdefer shards[0].deinit(alloc);
    try backups_api.populateShardArtifactIntegrity(alloc, shared_io, .portable, dest_path, &shards[0]);
    if (sealed != null) {
        try physical_local_write.populateAcceptedGenerationSummary(db.core.identity_namespace, source_summary orelse return error.BackupIntegrityFailure, &shards[0]);
        source_summary = null;
    }
    return shards;
}
