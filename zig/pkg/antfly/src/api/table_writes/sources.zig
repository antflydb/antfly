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

const platform_time = @import("../../platform/time.zig");
const backend_types = @import("../../storage/backend_types.zig");
const backups_api = @import("../backups.zig");
const db_mod = @import("../../storage/db/mod.zig");
const distributed_txn = @import("../distributed_txn.zig");
const relational_rows_api = @import("../relational_rows.zig");
const runtime_status = @import("../runtime_status.zig");
const sql_adapter = @import("../../sql/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const tables_api = @import("../tables.zig");
const table_write_backup_restore = @import("backup_restore.zig");
const table_write_core = @import("core.zig");
const table_write_index_config = @import("index_config.zig");
const table_write_integrity = @import("integrity.zig");
const table_write_integrity_types = @import("integrity_types.zig");
const table_write_managed_db = @import("managed_db.zig");
const table_write_relational_mutation = @import("relational_mutation.zig");

const TableWriteSource = table_write_core.TableWriteSource;
const freeBackupShards = table_write_backup_restore.freeBackupShards;
const ForeignKeyIntegrityAction = table_write_integrity_types.ForeignKeyIntegrityAction;
const ForeignKeyIntegritySchemaControllerOptions = table_write_integrity_types.ForeignKeyIntegritySchemaControllerOptions;
const ForeignKeyIntegritySchemaControllerResult = table_write_integrity_types.ForeignKeyIntegritySchemaControllerResult;
const UniqueConstraintIntegrityAction = table_write_integrity_types.UniqueConstraintIntegrityAction;
const UniqueConstraintIntegritySchemaControllerOptions = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerOptions;
const UniqueConstraintIntegritySchemaControllerResult = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerResult;
const UniqueConstraintIntegrityResult = table_write_integrity_types.UniqueConstraintIntegrityResult;
const ForeignKeyIntegrityResult = table_write_integrity_types.ForeignKeyIntegrityResult;
const ForeignKeyIntegrityGroupReport = table_write_integrity_types.ForeignKeyIntegrityGroupReport;
const ForeignKeyIntegrityViolation = table_write_integrity_types.ForeignKeyIntegrityViolation;
const ForeignKeyActionJobStatus = table_write_integrity_types.ForeignKeyActionJobStatus;
const ForeignKeyActionJobResult = table_write_integrity_types.ForeignKeyActionJobResult;
const ForeignKeyActionJobProgressResult = table_write_integrity_types.ForeignKeyActionJobProgressResult;
const ForeignKeyActionScheduleStatus = table_write_integrity_types.ForeignKeyActionScheduleStatus;
const ForeignKeyActionScheduleProgressResult = table_write_integrity_types.ForeignKeyActionScheduleProgressResult;

const normalizeRelationalConstraintError = table_write_core.normalizeRelationalConstraintError;
const nextTxnTimestamp = table_write_core.nextTxnTimestamp;
const nextTxnId = table_write_core.nextTxnId;
const boundConflict = table_write_core.boundConflict;
const loadLocalTableSchemaJson = table_write_managed_db.loadLocalTableSchemaJson;
const applyLocalTableSchemaJson = table_write_managed_db.applyLocalTableSchemaJson;
const validateTableBatchAgainstLocalSchema = table_write_managed_db.validateTableBatchAgainstLocalSchema;
const validateTransactionAgainstLocalSchema = table_write_managed_db.validateTransactionAgainstLocalSchema;
const corruptEmbeddingArtifactInDb = table_write_managed_db.corruptEmbeddingArtifactInDb;
const isReservedTableIndexMetadataEntry = table_write_index_config.isReservedTableIndexMetadataEntry;
const parseIndexKind = table_write_index_config.parseIndexKind;
const extractIndexConfigJson = table_write_index_config.extractIndexConfigJson;
const parseIndexConfig = table_write_index_config.parseIndexConfig;
const mutateRowsFromSourceAutocommitOnDb = table_write_relational_mutation.mutateRowsFromSourceAutocommitOnDb;
const mutateRowsJoinedFromSourceRowsOnDb = table_write_relational_mutation.mutateRowsJoinedFromSourceRowsOnDb;
const mutateRowsJoinedFromSourceRowsAutocommitOnDb = table_write_relational_mutation.mutateRowsJoinedFromSourceRowsAutocommitOnDb;
const mergeRowsFromSourceRowsOnDb = table_write_relational_mutation.mergeRowsFromSourceRowsOnDb;
const cloneForeignKeyIntegrityResultViolation = table_write_integrity.cloneForeignKeyIntegrityResultViolation;
const foreignKeyActionJobStatusFromDbRecord = table_write_integrity.foreignKeyActionJobStatusFromDbRecord;
const foreignKeyActionScheduleStatusFromDbRecord = table_write_integrity.foreignKeyActionScheduleStatusFromDbRecord;
const foreignKeyActionJobProgressFromDbRecords = table_write_integrity.foreignKeyActionJobProgressFromDbRecords;
const foreignKeyActionScheduleProgressFromDbRecords = table_write_integrity.foreignKeyActionScheduleProgressFromDbRecords;
const emptyForeignKeyIntegrityControllerResult = table_write_integrity.emptyForeignKeyIntegrityControllerResult;
const foreignKeyIntegritySchemaControllerPassWithSchemaJson = table_write_integrity.foreignKeyIntegritySchemaControllerPassWithSchemaJson;
const foreignKeyIntegrityWorkerActionSupported = table_write_integrity.foreignKeyIntegrityWorkerActionSupported;
const startForeignKeyIntegrityJobOnDb = table_write_integrity.startForeignKeyIntegrityJobOnDb;
const finishForeignKeyIntegrityJobOnDb = table_write_integrity.finishForeignKeyIntegrityJobOnDb;
const attachForeignKeyIntegrityJobId = table_write_integrity.attachForeignKeyIntegrityJobId;
const hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb = table_write_integrity.hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb;
const buildForeignKeyIntegrityWorkStatuses = table_write_integrity.buildForeignKeyIntegrityWorkStatuses;
const foreignKeyIntegritySingleWorkUnit = table_write_integrity.foreignKeyIntegritySingleWorkUnit;
const foreignKeyIntegrityNowNs = table_write_integrity.foreignKeyIntegrityNowNs;
const foreignKeyIntegrityWorkStatusClaimable = table_write_integrity.foreignKeyIntegrityWorkStatusClaimable;
const foreignKeyIntegrityWorkStatusesHaveClaimable = table_write_integrity.foreignKeyIntegrityWorkStatusesHaveClaimable;
const foreignKeyIntegrityWorkStatusesValid = table_write_integrity.foreignKeyIntegrityWorkStatusesValid;
const cloneForeignKeyIntegrityWorkUnits = table_write_integrity.cloneForeignKeyIntegrityWorkUnits;
const cloneForeignKeyIntegrityProgressSlice = table_write_integrity.cloneForeignKeyIntegrityProgressSlice;
const cloneForeignKeyIntegrityWorkClaimSlice = table_write_integrity.cloneForeignKeyIntegrityWorkClaimSlice;
const mergeForeignKeyIntegrityReport = table_write_integrity.mergeForeignKeyIntegrityReport;
const runForeignKeyIntegrityOnDb = table_write_integrity.runForeignKeyIntegrityOnDb;
const runForeignKeyIntegrityClaimedWorkUnitOnDb = table_write_integrity.runForeignKeyIntegrityClaimedWorkUnitOnDb;
const runUniqueConstraintIntegrityOnDb = table_write_integrity.runUniqueConstraintIntegrityOnDb;

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

pub const BoundTableWriteSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,

    pub fn init(table_name: []const u8, db: *db_mod.DB) BoundTableWriteSource {
        return .{
            .table_name = table_name,
            .db = db,
        };
    }

    pub fn source(self: *BoundTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_table = createTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .graph_metric_action = graphMetricAction,
                .reprocess_document_artifact = reprocessDocumentArtifact,
                .reprocess_document_artifact_range = reprocessDocumentArtifactRange,
                .graph_metric_maintenance_group_local = graphMetricMaintenanceGroupLocal,
                .backup_table = backupTable,
                .restore_table = restoreTable,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .batch = batch,
                .mutate_rows_from_source = mutateRowsFromSource,
                .mutate_rows_from_source_autocommit = mutateRowsFromSourceAutocommit,
                .mutate_rows_joined_from_source_rows = mutateRowsJoinedFromSourceRows,
                .mutate_rows_joined_from_source_rows_autocommit = mutateRowsJoinedFromSourceRowsAutocommit,
                .merge_rows_from_source_rows = mergeRowsFromSourceRows,
                .begin_bulk_ingest = beginBulkIngest,
                .finish_bulk_ingest = finishBulkIngest,
                .abort_bulk_ingest = abortBulkIngest,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
                .foreign_key_integrity = foreignKeyIntegrity,
                .foreign_key_integrity_worker_pass = foreignKeyIntegrityWorkerPass,
                .foreign_key_integrity_schema_controller_pass = foreignKeyIntegritySchemaControllerPass,
                .foreign_key_integrity_schema_controller_maintenance_pass = foreignKeyIntegritySchemaControllerMaintenancePass,
                .foreign_key_action_job_page = foreignKeyActionJobPage,
                .foreign_key_action_job_schedule = foreignKeyActionJobSchedule,
                .foreign_key_action_job_requeue = foreignKeyActionJobRequeue,
                .foreign_key_action_job_group_local = foreignKeyActionJobGroupLocal,
                .foreign_key_action_job_group_local_schedule = foreignKeyActionJobGroupLocalSchedule,
                .foreign_key_action_job_group_local_requeue = foreignKeyActionJobGroupLocalRequeue,
                .foreign_key_action_job_progress = foreignKeyActionJobProgress,
                .foreign_key_action_job_group_local_progress = foreignKeyActionJobGroupLocalProgress,
                .foreign_key_action_schedule_progress = foreignKeyActionScheduleProgress,
                .foreign_key_action_schedule_group_local_progress = foreignKeyActionScheduleGroupLocalProgress,
                .foreign_key_action_schedule_mark_seeded = foreignKeyActionScheduleMarkSeeded,
                .foreign_key_action_schedule_requeue = foreignKeyActionScheduleRequeue,
                .foreign_key_action_schedule_group_local_mark_seeded = foreignKeyActionScheduleGroupLocalMarkSeeded,
                .foreign_key_action_schedule_group_local_requeue = foreignKeyActionScheduleGroupLocalRequeue,
                .unique_constraint_integrity = uniqueConstraintIntegrity,
                .unique_constraint_integrity_group_local = uniqueConstraintIntegrityGroupLocal,
                .unique_constraint_integrity_schema_controller_maintenance_pass = uniqueConstraintIntegritySchemaControllerMaintenancePass,
                .foreign_key_integrity_group_local = foreignKeyIntegrityGroupLocal,
                .foreign_key_integrity_work_unit_group_local = foreignKeyIntegrityWorkUnitGroupLocal,
                .foreign_key_ref_children_group_local = foreignKeyRefChildrenGroupLocal,
                .foreign_key_ref_children_page_group_local = foreignKeyRefChildrenPageGroupLocal,
            },
        };
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
        items[0] = .{
            .group_id = 0,
            .stats = try self.db.runtimeStatusStatsConsistent(alloc),
        };
        return .{ .items = items };
    }

    fn graphMetricMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        _ = group_id;
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body);
    }

    fn foreignKeyIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runForeignKeyIntegrityOnDb(alloc, self.db, 0, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    fn foreignKeyIntegrityWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!foreignKeyIntegrityWorkerActionSupported(action)) return error.InvalidForeignKeyIntegrityRequest;
        try startForeignKeyIntegrityJobOnDb(self.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);

        const planned_units = try foreignKeyIntegritySingleWorkUnit(alloc, 0, action, constraint_name, lower_doc_key, upper_doc_key);
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            alloc.free(planned_units);
        }

        var status_snapshot = try runForeignKeyIntegrityOnDb(alloc, self.db, 0, .progress, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        defer status_snapshot.deinit(alloc);
        const initial_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, status_snapshot.progress, status_snapshot.work_claims, .pending);
        defer {
            for (initial_statuses) |*status| status.deinit(alloc);
            if (initial_statuses.len > 0) alloc.free(initial_statuses);
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        var truncated = false;
        const now_ns = foreignKeyIntegrityNowNs();
        if (max_work_units > 0 and initial_statuses.len > 0 and foreignKeyIntegrityWorkStatusClaimable(initial_statuses[0], now_ns)) {
            var one = try runForeignKeyIntegrityClaimedWorkUnitOnDb(
                alloc,
                self.db,
                0,
                action,
                "child_range",
                initial_statuses[0].claim_key,
                worker_id,
                lease_ms,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                violation_limit,
            );
            defer one.deinit(alloc);
            mergeForeignKeyIntegrityReport(&aggregate, one.report);
            try group_reports.ensureUnusedCapacity(alloc, one.groups.len);
            for (one.groups) |group| group_reports.appendAssumeCapacity(group);
            const copy_count = @min(violation_limit, one.violations.len);
            try violations.ensureUnusedCapacity(alloc, copy_count);
            for (one.violations[0..copy_count]) |violation| {
                violations.appendAssumeCapacity(try cloneForeignKeyIntegrityResultViolation(alloc, violation));
            }
            truncated = one.violations_truncated or one.violations.len > violation_limit;
        }

        var final_snapshot = try runForeignKeyIntegrityOnDb(alloc, self.db, 0, .progress, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        defer final_snapshot.deinit(alloc);
        const work_units = try cloneForeignKeyIntegrityWorkUnits(alloc, planned_units);
        errdefer {
            for (work_units) |*unit| unit.deinit(alloc);
            if (work_units.len > 0) alloc.free(work_units);
        }
        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, final_snapshot.progress, final_snapshot.work_claims, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }

        const valid = foreignKeyIntegrityWorkStatusesValid(work_statuses);
        const complete = !foreignKeyIntegrityWorkStatusesHaveClaimable(work_statuses, foreignKeyIntegrityNowNs());
        var result: ForeignKeyIntegrityResult = .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try cloneForeignKeyIntegrityProgressSlice(alloc, final_snapshot.progress),
            .work_units = work_units,
            .work_claims = try cloneForeignKeyIntegrityWorkClaimSlice(alloc, final_snapshot.work_claims),
            .work_statuses = work_statuses,
            .violations = try violations.toOwnedSlice(alloc),
        };
        errdefer {
            var cleanup = result;
            cleanup.deinit(alloc);
        }
        try finishForeignKeyIntegrityJobOnDb(alloc, self.db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, self.db, 0, &result);
        return result;
    }

    fn foreignKeyIntegritySchemaControllerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const schema_json = (try loadLocalTableSchemaJson(alloc, self.db)) orelse {
            return try emptyForeignKeyIntegrityControllerResult(alloc, action, violation_limit);
        };
        defer alloc.free(schema_json);
        return try foreignKeyIntegritySchemaControllerPassWithSchemaJson(
            alloc,
            self.source(),
            table_name,
            schema_json,
            action,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: ForeignKeyIntegritySchemaControllerOptions,
    ) !?ForeignKeyIntegritySchemaControllerResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        return try table_write_integrity.runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
            alloc,
            self.source(),
            self.db,
            self.table_name,
            options,
        );
    }

    fn uniqueConstraintIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: UniqueConstraintIntegritySchemaControllerOptions,
    ) !?UniqueConstraintIntegritySchemaControllerResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        return try table_write_integrity.runLocalUniqueConstraintIntegritySchemaControllerMaintenancePass(
            alloc,
            self.source(),
            self.db,
            self.table_name,
            options,
        );
    }

    fn foreignKeyActionJobPage(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
    ) !?ForeignKeyActionJobResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionJobWithUpdatedParentKey(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        return try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionJobGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionJobWithUpdatedParentKey(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        return try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionJobGroupLocalSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        return try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionJobProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionJobRecords();
        defer self.db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, 0, records);
    }

    fn foreignKeyActionJobGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionJobRecords();
        defer self.db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, group_id, records);
    }

    fn foreignKeyActionScheduleProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionScheduleRecords();
        defer self.db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, 0, records);
    }

    fn foreignKeyActionScheduleGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionScheduleRecords();
        defer self.db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, group_id, records);
    }

    fn foreignKeyActionScheduleMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups);
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, 0, record);
    }

    fn foreignKeyActionScheduleRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            schedule_id,
            action_job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, 0, record);
    }

    fn foreignKeyActionScheduleGroupLocalMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups);
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionScheduleGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            schedule_id,
            action_job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
    }

    fn uniqueConstraintIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runUniqueConstraintIntegrityOnDb(alloc, self.db, 0, action, lower_doc_key, upper_doc_key);
    }

    fn foreignKeyIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runForeignKeyIntegrityOnDb(alloc, self.db, group_id, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    fn foreignKeyIntegrityWorkUnitGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        try startForeignKeyIntegrityJobOnDb(self.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
        var result = try runForeignKeyIntegrityClaimedWorkUnitOnDb(alloc, self.db, group_id, action, phase, claim_key, worker_id, lease_ms, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        errdefer result.deinit(alloc);
        try finishForeignKeyIntegrityJobOnDb(alloc, self.db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, self.db, group_id, &result);
        return result;
    }

    fn uniqueConstraintIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runUniqueConstraintIntegrityOnDb(alloc, self.db, group_id, action, lower_doc_key, upper_doc_key);
    }

    fn foreignKeyRefChildrenGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        limit: usize,
    ) !?[]db_mod.types.ForeignKeyRefChild {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.listForeignKeyRefChildrenForParent(alloc, constraint_name, parent_table, parent_key, limit);
    }

    fn foreignKeyRefChildrenPageGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        start_after_child_table: ?[]const u8,
        start_after_child_key: ?[]const u8,
        limit: usize,
    ) !?db_mod.types.ForeignKeyRefChildrenPage {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.listForeignKeyRefChildrenPageForParent(alloc, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!try corruptEmbeddingArtifactInDb(alloc, self.db, doc_key, index_name)) return error.NotFound;
    }

    fn createTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const raw_indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        const schema_json = tables_api.effectiveSchemaJson(req.schema_json);
        const expanded_indexes_json = try tables_api.prepareTableIndexesForSchemaAlloc(alloc, table_name, raw_indexes_json, schema_json);
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
            if (isReservedTableIndexMetadataEntry(entry.key_ptr.*)) continue;
            const kind = try parseIndexKind(entry.value_ptr.*);
            const config_json = try extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
            defer alloc.free(config_json);
            try self.db.addIndex(.{
                .name = entry.key_ptr.*,
                .kind = kind,
                .config_json = config_json,
            });
        }

        try applyLocalTableSchemaJson(alloc, self.db, schema_json);
    }

    fn updateSchema(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try applyLocalTableSchemaJson(alloc, self.db, schema_json);
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try validateTableBatchAgainstLocalSchema(alloc, self.db, req.writes, req.deletes, req.transforms);
        self.db.batch(req) catch |err| return normalizeRelationalConstraintError(err);
    }

    fn mutateRowsFromSource(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.mutateRelationalRowsFromSource(alloc, schema, req);
    }

    fn mutateRowsFromSourceAutocommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
        _: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try mutateRowsFromSourceAutocommitOnDb(alloc, self.db, schema, req);
    }

    fn mutateRowsJoinedFromSourceRows(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        return try mutateRowsJoinedFromSourceRowsOnDb(alloc, self.db, target_schema, source_schema, req, source_rows);
    }

    fn mutateRowsJoinedFromSourceRowsAutocommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
        _: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try mutateRowsJoinedFromSourceRowsAutocommitOnDb(alloc, self.db, target_schema, source_schema, req, source_rows);
    }

    fn mergeRowsFromSourceRows(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        plan: sql_adapter.LoweredMergeMutationPlan,
        source_rows: []const []const u8,
    ) !?relational_rows_api.OwnedRowsBatchRequest {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        return try mergeRowsFromSourceRowsOnDb(alloc, self.db, target_schema, source_schema, plan, source_rows, normalizeRelationalConstraintError);
    }

    fn beginBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.beginBulkIngestSession();
    }

    fn finishBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.finishBulkIngestSessionWithOptions(options);
    }

    fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return;
        self.db.abortBulkIngestSession();
    }

    fn backupTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try table_write_backup_restore.backupLocalTable(alloc, self.db, plan);
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try table_write_backup_restore.restoreLocalTable(alloc, self.db, plan);
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (tables.len != 1) return error.UnsupportedOperation;
        const table = tables[0];
        if (!std.mem.eql(u8, self.table_name, table.table_name)) return null;
        try validateTransactionAgainstLocalSchema(alloc, self.db, table.writes, table.deletes, table.transforms);

        const commit_version = begin_timestamp + 1;

        _ = try self.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, &.{});
        self.db.writeTransaction(txn_id, .{
            .writes = table.writes,
            .deletes = table.deletes,
            .transforms = table.transforms,
            .predicates = table.predicates,
        }) catch |err| switch (err) {
            error.VersionConflict, error.IntentConflict => {
                self.db.resolveTransactionIntents(txn_id, .aborted, commit_version) catch {};
                return .{ .conflict = boundConflict(table, err) };
            },
            else => return normalizeRelationalConstraintError(err),
        };
        self.db.resolveTransactionIntents(txn_id, .committed, commit_version) catch |err| {
            return normalizeRelationalConstraintError(err);
        };
        return .{ .committed = .{ .participant_count = 1 } };
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const schema_json = try loadLocalTableSchemaJson(alloc, self.db);
        defer if (schema_json) |value| alloc.free(value);
        const expanded_index_json = try tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, tables_api.effectiveSchemaJson(schema_json));
        defer alloc.free(expanded_index_json);
        const cfg = try parseIndexConfig(alloc, index_name, expanded_index_json);
        defer {
            alloc.free(cfg.name);
            alloc.free(cfg.config_json);
        }
        try self.db.addIndex(cfg);
    }

    fn dropIndex(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try self.db.deleteIndex(index_name);
    }

    fn graphMetricAction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
    ) !?db_mod.types.GraphMetricStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (std.mem.eql(u8, action, "refresh")) {
            return try self.db.refreshGraphMetric(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "rebuild")) {
            return try self.db.rebuildGraphMetric(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "delete")) {
            return try self.db.deleteGraphMetricMaterialization(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "pause")) {
            return try self.db.pauseGraphMetricMaintenance(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "resume")) {
            return try self.db.resumeGraphMetricMaintenance(alloc, index_name, metric_name);
        }
        return error.InvalidGraphMetricAction;
    }

    fn reprocessDocumentArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.reprocessDocumentArtifact(alloc, doc_key, artifact_name);
    }

    fn reprocessDocumentArtifactRange(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.reprocessDocumentArtifactRange(alloc, artifact_name, req);
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try batch(ptr, alloc, table_name, req);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        _: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try self.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        _: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try validateTransactionAgainstLocalSchema(alloc, self.db, req.writes, req.deletes, req.transforms);
        self.db.writeTransaction(txn_id, req) catch |err| return normalizeRelationalConstraintError(err);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        self.db.resolveTransactionIntents(txn_id, status, commit_version) catch |err| {
            return normalizeRelationalConstraintError(err);
        };
        const participant = try std.fmt.allocPrint(self.db.alloc, "group:{d}", .{group_id});
        defer self.db.alloc.free(participant);
        try self.db.markTransactionParticipantResolved(txn_id, participant);
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.getTransactionStatus(txn_id);
    }
};

test "bound table write source resolves internal group transactions into visible documents" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-txn-group-local";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");

    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    _ = try source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    _ = try source.source().txnResolveGroupLocal(alloc, 7, "docs", txn_id, .committed, 10_001);

    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, (try source.source().txnStatusGroupLocal(alloc, 7, "docs", txn_id)).?);

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "bound table write source provisions default full text index on create" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-create";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{};
    defer req.deinit(alloc);
    req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);

    _ = try source.source().createTable(alloc, "docs", req);
    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
}

test "bound table write source rejects invalid batch writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    try std.testing.expect(db.core.schema != null);
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, storage_schema.resolveFieldType(db.core.schema.?, "meta_status").?.field_type);
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "bound table write source enforces nested required fields and array items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nested-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"required\":[\"author\",\"tags\"],\"properties\":{\"author\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"active\":{\"type\":\"boolean\"}}},\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"author\":{\"name\":\"ann\",\"active\":true},\"tags\":[\"a\",\"b\"]}" }},
    });

    var written = (try db.lookup(alloc, "doc:good", .{})).?;
    defer written.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, written.json, "\"name\":\"ann\"") != null);

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing", .value = "{\"author\":{\"active\":true},\"tags\":[\"a\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-tag", .value = "{\"author\":{\"name\":\"ann\"},\"tags\":[1]}" }},
    }));
}

test "bound table write source enforces enums numeric bounds and anyOf" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-enum-bounds-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\",\"enum\":[\"draft\",\"published\"]},\"score\":{\"type\":\"numeric\",\"minimum\":0,\"maximum\":10},\"metric\":{\"anyOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"keyword\",\"enum\":[\"n/a\"]}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"n/a\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-status", .value = "{\"status\":\"archived\",\"score\":8,\"metric\":\"n/a\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-score", .value = "{\"status\":\"draft\",\"score\":11,\"metric\":\"n/a\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-metric", .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"bad\"}" }},
    }));
}

test "bound table write source enforces oneOf allOf pattern and item cardinality" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-pattern-compose-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"keyword\",\"pattern\":\"^[A-Z]{3}-[0-9]{2}$\"},\"tags\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":2,\"items\":{\"type\":\"keyword\"}},\"code\":{\"oneOf\":[{\"type\":\"keyword\",\"enum\":[\"A\"]},{\"type\":\"keyword\",\"enum\":[\"B\"]}]},\"score\":{\"allOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"numeric\",\"maximum\":5}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern", .value = "{\"sku\":\"bad\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-min-items", .value = "{\"sku\":\"ABC-12\",\"tags\":[],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-max-items", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\",\"y\",\"z\"],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-oneof", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"C\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-allof", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":8}" }},
    }));
}

test "bound table write source enforces string length and object cardinality" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-length-cardinality-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"minProperties\":2,\"maxProperties\":3,\"properties\":{\"title\":{\"type\":\"text\",\"minLength\":3,\"maxLength\":5},\"meta\":{\"type\":\"object\",\"minProperties\":1,\"maxProperties\":2,\"properties\":{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"},\"c\":{\"type\":\"keyword\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-min-length", .value = "{\"title\":\"hi\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-max-length", .value = "{\"title\":\"alphabet\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-cardinality", .value = "{\"title\":\"alpha\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-object-cardinality", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\",\"c\":\"z\"}}" }},
    }));
}

test "bound table write source backs up and restores a local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    var source = BoundTableWriteSource.init("docs", &db);
    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 1,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    db.close();
    db = try db_mod.DB.open(alloc, path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "bound table write source backs up and restores a portable local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-portable-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-portable-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    var source = BoundTableWriteSource.init("docs", &db);
    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "portable-snap",
        .format = .portable,
    })).?;
    defer freeBackupShards(alloc, shards);
    try std.testing.expectEqual(@as(usize, 1), shards.len);
    try std.testing.expectEqualStrings("portable-snap.afb", shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, shards[0].snapshot_path });
    defer alloc.free(afb_path);
    var afb_file = try std.Io.Dir.cwd().openFile(io_impl.io(), afb_path, .{});
    defer afb_file.close(io_impl.io());
    try std.testing.expect((try afb_file.stat(io_impl.io())).size > 0);

    var manifest = try backups_api.createManifest(alloc, "portable-snap", &.{
        .table_id = 1,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "bound table write source enforces root conditionals not and unique items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-conditional-unique-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"if\":{\"required\":[\"kind\"],\"properties\":{\"kind\":{\"enum\":[\"story\"]}}},\"then\":{\"required\":[\"headline\"]},\"else\":{\"required\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\",\"enum\":[\"story\",\"note\"]},\"headline\":{\"type\":\"text\"},\"slug\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"array\",\"uniqueItems\":true,\"items\":{\"type\":\"keyword\"}},\"status\":{\"type\":\"keyword\",\"not\":{\"enum\":[\"archived\"]}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:story", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:note", .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-headline", .value = "{\"kind\":\"story\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"note\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:duplicate-tags", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"a\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-status", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"archived\"}" }},
    }));
}

test "bound table write source enforces property names and dependent required" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-property-names-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"dependentRequired\":{\"kind\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"propertyNames\":{\"pattern\":\"^meta_[a-z]+$\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"attrs\":{\"meta_color\":\"red\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-dependent", .value = "{\"kind\":\"story\",\"attrs\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-property-name", .value = "{\"slug\":\"alpha\",\"attrs\":{\"bad\":\"red\"}}" }},
    }));
}

test "bound table write source enforces dependent schemas" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-dependent-schemas";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependentSchemas\":{\"kind\":{\"required\":[\"slug\"],\"properties\":{\"kind\":{\"const\":\"story\"},\"slug\":{\"type\":\"keyword\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"ok\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:no-trigger", .value = "{\"details\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"details\":\"ok\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-kind", .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"details\":\"ok\"}" }},
    }));
}

test "bound table write source enforces additional properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-additional-properties";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"title\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"keyword\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":1}}" }},
    }));
}

test "bound table write source enforces contains semantics" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-contains";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tags\":{\"type\":\"array\",\"contains\":{\"type\":\"keyword\",\"const\":\"hot\"},\"minContains\":1,\"maxContains\":2},\"scores\":{\"type\":\"array\",\"contains\":{\"type\":\"numeric\",\"minimum\":10}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"tags\":[\"hot\",\"warm\"],\"scores\":[1,10,20]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-contains", .value = "{\"tags\":[\"warm\"],\"scores\":[10]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:too-many-contains", .value = "{\"tags\":[\"hot\",\"hot\",\"hot\"],\"scores\":[10]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-score-match", .value = "{\"tags\":[\"hot\"],\"scores\":[1,2,3]}" }},
    }));
}

test "bound table write source enforces prefix items and pattern properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prefix-pattern";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"items\":{\"type\":\"numeric\"}},\"meta\":{\"type\":\"object\",\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"},\"^flag_[a-z]+$\":{\"type\":\"boolean\"}},\"additionalProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"coords\":[\"point\",1,2,3],\"meta\":{\"meta_color\":\"red\",\"flag_ready\":true}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-prefix-const", .value = "{\"coords\":[1,1,2],\"meta\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-prefix-type", .value = "{\"coords\":[\"point\",\"bad\"],\"meta\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern-type", .value = "{\"coords\":[\"point\",1],\"meta\":{\"meta_color\":1}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern-extra", .value = "{\"coords\":[\"point\",1],\"meta\":{\"other\":\"x\"}}" }},
    }));
}

test "bound table write source enforces exclusive numeric bounds and multipleOf" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-exclusive-multiple";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"score\":{\"type\":\"numeric\",\"exclusiveMinimum\":0,\"exclusiveMaximum\":10,\"multipleOf\":0.5}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"score\":5.5}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-exclusive-min", .value = "{\"score\":0}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-exclusive-max", .value = "{\"score\":10}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-multiple", .value = "{\"score\":5.25}" }},
    }));
}

test "bound table write source enforces nullable and type-array fields" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nullable-types";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"subtitle\":{\"type\":[\"text\",\"null\"]},\"score\":{\"type\":\"numeric\",\"nullable\":true},\"flag\":{\"type\":[\"boolean\"]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-nullable", .value = "{\"title\":\"alpha\",\"subtitle\":null,\"score\":null,\"flag\":true}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title-null", .value = "{\"title\":null,\"subtitle\":\"beta\",\"score\":1,\"flag\":true}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-flag-null", .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":null}" }},
    }));
}

test "bound table write source enforces local defs and refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-defs-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"metaField\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}}},\"scoreField\":{\"type\":\"numeric\",\"nullable\":true}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\"},\"meta\":{\"$ref\":\"#/$defs/metaField\"},\"score\":{\"$ref\":\"#/$defs/scoreField\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"status\":\"ready\"},\"score\":null}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title", .value = "{\"title\":1,\"meta\":{\"status\":\"ready\"},\"score\":null}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta", .value = "{\"title\":\"alpha\",\"meta\":{\"status\":1},\"score\":null}" }},
    }));
}

test "bound table write source enforces ref siblings and nested local defs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-siblings-local-defs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"sharedText\":{\"type\":\"text\",\"minLength\":8}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\",\"minLength\":3},\"meta\":{\"type\":\"object\",\"$defs\":{\"sharedText\":{\"type\":\"text\",\"minLength\":4}},\"properties\":{\"note\":{\"$ref\":\"#/$defs/sharedText\",\"maxLength\":6}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"short\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title", .value = "{\"title\":\"ab\",\"meta\":{\"note\":\"short\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-local-def", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"abc\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-sibling", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"toolong\"}}" }},
    }));
}

test "bound table write source enforces recursive root refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-recursive-root-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:good", .value = "{\"name\":\"root\",\"children\":[{\"name\":\"leaf\",\"children\":[]},{\"name\":\"branch\",\"children\":[{\"name\":\"twig\"}]}]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:bad-child-type", .value = "{\"name\":\"root\",\"children\":[{\"name\":1}]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:bad-null-child", .value = "{\"name\":\"root\",\"children\":[null]}" }},
    }));
}

test "bound table write source enforces format and additionalItems" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-format-additional-items";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\",\"format\":\"email\"},\"site\":{\"type\":\"keyword\",\"format\":\"uri\"},\"id\":{\"type\":\"keyword\",\"format\":\"uuid\"},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"additionalItems\":false},\"labels\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\"}],\"additionalItems\":{\"type\":\"keyword\",\"pattern\":\"^[a-z]+$\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\",\"beta\"]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-email", .value = "{\"email\":\"bad\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-uri", .value = "{\"email\":\"a@example.com\",\"site\":\"not a uri\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-uuid", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"bad-uuid\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-extra-items", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1,2],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-additional-schema", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",1]}" }},
    }));
}

test "bound table write source enforces broader string formats" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-broader-formats";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"published_at\":{\"type\":\"keyword\",\"format\":\"date-time\"},\"birthday\":{\"type\":\"keyword\",\"format\":\"date\"},\"v4\":{\"type\":\"keyword\",\"format\":\"ipv4\"},\"v6\":{\"type\":\"keyword\",\"format\":\"ipv6\"},\"host\":{\"type\":\"keyword\",\"format\":\"hostname\"},\"ref\":{\"type\":\"keyword\",\"format\":\"uri-reference\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-datetime", .value = "{\"published_at\":\"2024-01-02\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-date", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-13-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ipv4", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"999.1.1.1\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ipv6", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"invalid\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-host", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"-bad-host\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ref", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs bad\"}" }},
    }));
}

test "bound table write source enforces unevaluated properties and items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"meta\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"}],\"unevaluatedItems\":{\"type\":\"numeric\"}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",1,2]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":1},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-unevaluated-item", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",\"bad\"]}" }},
    }));
}

test "bound table write source enforces composed unevaluated coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-unevaluated-composed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"kind\":{\"type\":\"keyword\"}}},{\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"title\":{\"type\":\"text\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}],\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-point", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-line", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"line\",1,2]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"bad\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-array-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1,2]}" }},
    }));
}

test "bound table write source enforces root unevaluated properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-root-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad", .value = "{\"kind\":\"story\",\"slug\":1}" }},
    }));
}

test "bound table write source enforces conditional and dependency unevaluated coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-conditional-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"if\":{\"properties\":{\"kind\":{\"const\":\"story\"}}},\"then\":{\"required\":[\"slug\"],\"properties\":{\"slug\":{\"type\":\"keyword\"}}},\"else\":{\"required\":[\"rating\"],\"properties\":{\"rating\":{\"type\":\"numeric\"}}},\"dependentSchemas\":{\"kind\":{\"properties\":{\"details\":{\"type\":\"text\"}}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:story", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:score", .value = "{\"kind\":\"score\",\"rating\":5,\"details\":\"body\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"details\":\"body\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-rating", .value = "{\"kind\":\"score\",\"details\":\"body\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:extra", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\",\"extra\":\"bad\"}" }},
    }));
}

test "bound table write source enforces anyOf and oneOf branch evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-branch-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"allOf\":[{\"properties\":{\"meta\":{\"type\":\"object\",\"anyOf\":[{\"properties\":{\"mode\":{\"const\":\"alpha\"},\"a\":{\"type\":\"keyword\"}}},{\"properties\":{\"mode\":{\"const\":\"beta\"},\"b\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"choice\":{\"type\":\"object\",\"oneOf\":[{\"properties\":{\"mode\":{\"const\":\"left\"},\"left\":{\"type\":\"keyword\"}},\"required\":[\"mode\",\"left\"]},{\"properties\":{\"mode\":{\"const\":\"right\"},\"right\":{\"type\":\"numeric\"}},\"required\":[\"mode\",\"right\"]}],\"unevaluatedProperties\":false}}}],\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:alpha-left", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:beta-right", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"b\":3},\"choice\":{\"mode\":\"right\",\"right\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-anyof-alpha", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"b\":3},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-anyof-beta", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"a\":\"oops\"},\"choice\":{\"mode\":\"right\",\"right\":9}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-oneof", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"right\":9}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:extra", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\",\"extra\":\"bad\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    }));
}

test "bound table write source enforces anyOf and oneOf array evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-array-branch-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"minItems\":3,\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]},\"choice\":{\"type\":\"array\",\"oneOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"left\"},{\"type\":\"keyword\"}],\"unevaluatedItems\":false},{\"minItems\":2,\"prefixItems\":[{\"const\":\"right\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:point-left", .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",\"ok\"]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:line-right", .value = "{\"coords\":[\"line\",1,2],\"choice\":[\"right\",9]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:coords-extra", .value = "{\"coords\":[\"point\",1,2],\"choice\":[\"left\",\"ok\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:coords-short", .value = "{\"coords\":[\"line\",1],\"choice\":[\"right\",9]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-wrong-branch", .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",9]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-extra", .value = "{\"coords\":[\"point\",1],\"choice\":[\"right\",9,10]}" }},
    }));
}

test "bound table write source enforces composed contains-driven array evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-array-contains-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"series\":{\"type\":\"array\",\"allOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"set\"}]},{\"contains\":{\"type\":\"numeric\",\"minimum\":10},\"minContains\":1}],\"unevaluatedItems\":false},\"selector\":{\"type\":\"array\",\"anyOf\":[{\"contains\":{\"const\":\"hot\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"cold\"},\"minContains\":1,\"unevaluatedItems\":false}]},\"exclusive\":{\"type\":\"array\",\"oneOf\":[{\"contains\":{\"const\":\"left\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"right\"},\"minContains\":1,\"unevaluatedItems\":false}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:hot-left", .value = "{\"series\":[\"set\",10,11],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:cold-right", .value = "{\"series\":[\"set\",12],\"selector\":[\"cold\"],\"exclusive\":[\"right\"]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:series-leftover", .value = "{\"series\":[\"set\",10,1],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:selector-no-branch", .value = "{\"series\":[\"set\",12],\"selector\":[\"warm\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:selector-overlap", .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\",\"cold\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:exclusive-overlap", .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\"],\"exclusive\":[\"left\",\"right\"]}" }},
    }));
}

test "bound table write source enforces composed pattern and additional properties evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-pattern-additional-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},{\"additionalProperties\":{\"type\":\"numeric\"}}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}],\"unevaluatedProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:pattern", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:additional", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:meta-extra", .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-typed-wrong", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-overlap", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true,\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:exclusive-overlap", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\",\"score\":9}}" }},
    }));
}

test "bound table write source enforces composed ref closure evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-pattern-additional";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"meta_patterns\":{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},\"meta_count\":{\"properties\":{\"count\":{\"type\":\"numeric\"}}},\"choice_flags\":{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},\"choice_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"}},\"exclusive_names\":{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},\"exclusive_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}},\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"$ref\":\"#/$defs/meta_patterns\"},{\"$ref\":\"#/$defs/meta_count\"}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"$ref\":\"#/$defs/choice_flags\"},{\"$ref\":\"#/$defs/choice_numbers\"}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"$ref\":\"#/$defs/exclusive_names\"},{\"$ref\":\"#/$defs/exclusive_numbers\"}],\"unevaluatedProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:pattern", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:additional", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:meta-extra", .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-bad", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
}

test "bound table write source enforces nullable composed refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nullable-composed-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"nullable_keyword\":{\"type\":[\"keyword\",\"null\"]},\"null_or_x\":{\"anyOf\":[{\"const\":null},{\"type\":\"keyword\",\"enum\":[\"x\"]}]}},\"properties\":{\"maybe\":{\"allOf\":[{\"$ref\":\"#/$defs/nullable_keyword\"},{\"$ref\":\"#/$defs/null_or_x\"}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:null", .value = "{\"maybe\":null}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:x", .value = "{\"maybe\":\"x\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:y", .value = "{\"maybe\":\"y\"}" }},
    }));
}

test "bound table write source enforces recursive ref closure semantics" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-recursive-closure";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("nodes", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^tag_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "nodes", req);
    _ = try source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:root", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":\"leaf\"},\"children\":[]},{\"name\":\"branch\",\"meta\":{\"tag_kind\":\"branch\",\"count\":1},\"children\":[{\"name\":\"twig\",\"meta\":{\"tag_kind\":\"twig\"}}]}]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:extra", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"extra\":\"bad\"}]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:meta-extra", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"other\":\"bad\"},\"children\":[]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:meta-type", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":1}}]}" }},
    }));
}

test "bound table write source enforces escaped ref tokens and direct fragment refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-escaped-hash";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"slash/name\":{\"type\":\"text\"},\"tilde~name\":{\"type\":\"keyword\"}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/slash~1name\"},\"kind\":{\"$ref\":\"#/$defs/tilde~0name\"},\"meta\":{\"type\":\"object\",\"$defs\":{\"local/name\":{\"type\":\"text\"}},\"properties\":{\"note\":{\"$ref\":\"#/properties/meta/$defs/local~1name\"},\"shadow\":{\"$ref\":\"#/properties/title\"}},\"required\":[\"note\",\"shadow\"]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-slash-ref", .value = "{\"title\":1,\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-tilde-ref", .value = "{\"title\":\"alpha\",\"kind\":true,\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-direct-fragment", .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":1}}" }},
    }));
}

test "bound table write source enforces legacy dependencies keyword" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-legacy-dependencies";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"mode\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependencies\":{\"kind\":[\"slug\"],\"mode\":{\"required\":[\"details\"],\"properties\":{\"mode\":{\"const\":\"long\"},\"details\":{\"type\":\"text\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\",\"details\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"mode\":\"long\",\"details\":\"ok\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-details", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-mode-const", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"short\",\"details\":\"ok\"}" }},
    }));
}

test "bound table write source rejects invalid commit writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-commit-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    try std.testing.expectError(error.InvalidBatchRequest, source.source().commitTransaction(alloc, &.{.{
        .table_name = "docs",
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }}, .write));
}

test "bound table write source rejects invalid commit transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-commit-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().commitTransaction(alloc, &.{.{
        .table_name = "docs",
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }}, .write));
}

test "bound table write source rejects invalid txn prepare writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prepare-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    const txn_id = try distributed_txn.parseTxnIdHex("11112222333344445555666677778888");
    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    try std.testing.expectError(error.InvalidBatchRequest, source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "bound table write source rejects invalid txn prepare transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prepare-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const txn_id = try distributed_txn.parseTxnIdHex("11112222333344445555666677779999");
    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    try std.testing.expectError(error.InvalidBatchRequest, source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }));
}

test "bound table write source rejects invalid batch transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }));
}

test "bound table write source validates transforms against same-batch writes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-same-request-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"aliases\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"curated\"}" }},
        .transforms = &.{.{
            .key = "doc:a",
            .upsert = true,
            .operations = &.{
                .{ .op = .set_on_insert, .path = "body", .value_json = "\"would-be-invalid-on-insert\"" },
                .{ .op = .add_to_set, .path = "aliases", .value_json = "\"alpha\"" },
            },
        }},
    });

    const stored = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"title\":\"curated\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "would-be-invalid-on-insert") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"aliases\":[\"alpha\"]") != null);
}

test "bound table write source validates non-upsert transforms against same-batch deletes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-delete-no-upsert-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    _ = try source.source().batch(alloc, "docs", .{
        .deletes = &.{"doc:a"},
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"would-be-invalid\"" },
            },
        }},
    });

    const stored = try db.get(alloc, "doc:a");
    defer if (stored) |body| alloc.free(body);
    try std.testing.expect(stored == null);
}

test "bound table write source rejects upsert transforms against same-batch deletes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-delete-upsert-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .deletes = &.{"doc:a"},
        .transforms = &.{.{
            .key = "doc:a",
            .upsert = true,
            .operations = &.{
                .{ .op = .set_on_insert, .path = "body", .value_json = "\"invalid-insert\"" },
            },
        }},
    }));
}

test "bound table write source derives ttl timestamps from ttl_field values" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ttl-field-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"ttl_duration_ns\":1000000000,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"datetime\"},\"title\":{\"type\":\"text\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"expires_at\":5}" }},
        .timestamp_ns = 999,
    });
    try std.testing.expectEqual(@as(u64, 5), try db.getTimestamp(alloc, "doc:a"));

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"expires_at\":\"2024-01-02T03:04:05Z\"}" }},
        .timestamp_ns = 999,
    });
    try std.testing.expectEqual(@as(u64, 1_704_164_645_000_000_000), try db.getTimestamp(alloc, "doc:b"));

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"expires_at\":\"not-a-time\"}" }},
    }));
}

test "bound table write source applies batch writes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}
