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
