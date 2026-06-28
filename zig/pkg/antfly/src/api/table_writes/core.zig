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

const backups_api = @import("../backups.zig");
const catalog_resources = @import("../catalog_resources.zig");
const distributed_txn = @import("../distributed_txn.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const platform_time = @import("../../platform/time.zig");
const backend_types = @import("../../storage/backend_types.zig");
const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const runtime_status = @import("../runtime_status.zig");
const tables_api = @import("../tables.zig");
const sql_adapter = @import("../../sql/mod.zig");
const table_write_integrity_types = @import("integrity_types.zig");
const table_write_schema_jobs = @import("schema_jobs.zig");

const ForeignKeyIntegrityAction = table_write_integrity_types.ForeignKeyIntegrityAction;
const ForeignKeyIntegritySchemaControllerOptions = table_write_integrity_types.ForeignKeyIntegritySchemaControllerOptions;
const ForeignKeyIntegritySchemaControllerResult = table_write_integrity_types.ForeignKeyIntegritySchemaControllerResult;
const ForeignKeyIntegrityResult = table_write_integrity_types.ForeignKeyIntegrityResult;
const ForeignKeyActionJobResult = table_write_integrity_types.ForeignKeyActionJobResult;
const ForeignKeyActionJobStatus = table_write_integrity_types.ForeignKeyActionJobStatus;
const ForeignKeyActionJobProgressResult = table_write_integrity_types.ForeignKeyActionJobProgressResult;
const ForeignKeyActionScheduleStatus = table_write_integrity_types.ForeignKeyActionScheduleStatus;
const ForeignKeyActionScheduleProgressResult = table_write_integrity_types.ForeignKeyActionScheduleProgressResult;
const UniqueConstraintIntegrityAction = table_write_integrity_types.UniqueConstraintIntegrityAction;
const UniqueConstraintIntegrityResult = table_write_integrity_types.UniqueConstraintIntegrityResult;
const UniqueConstraintIntegritySchemaControllerOptions = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerOptions;
const UniqueConstraintIntegritySchemaControllerResult = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerResult;
const SecondaryIndexRebuildWorkerResult = table_write_schema_jobs.SecondaryIndexRebuildWorkerResult;
const SecondaryIndexRebuildWorkerPassResult = table_write_schema_jobs.SecondaryIndexRebuildWorkerPassResult;
const SchemaRewriteWorkerResult = table_write_schema_jobs.SchemaRewriteWorkerResult;
const SchemaRewriteWorkerPassResult = table_write_schema_jobs.SchemaRewriteWorkerPassResult;
const TableEmptyingWorkerResult = table_write_schema_jobs.TableEmptyingWorkerResult;
const TableEmptyingWorkerPassResult = table_write_schema_jobs.TableEmptyingWorkerPassResult;

pub const backend_current_root_generation: u64 = 0;

var txn_id_nonce: std.atomic.Value(u64) = .init(0);

pub fn normalizeRelationalConstraintError(err: anyerror) anyerror {
    return switch (err) {
        error.ForeignKeyViolation, error.UniqueConstraintViolation => error.InvalidBatchRequest,
        else => err,
    };
}

pub fn nextTxnTimestamp() u64 {
    // Transaction timestamps are stored in shard metadata and later compared
    // against transaction recovery cutoffs, so they must stay on realtime.
    return platform_time.realtimeNs();
}

pub fn nextTxnId() db_mod.types.TxnId {
    const nonce = txn_id_nonce.fetchAdd(1, .monotonic);
    var txn_id: db_mod.types.TxnId = undefined;
    std.mem.writeInt(u64, txn_id[0..8], nextTxnTimestamp(), .big);
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

pub const GroupBatch = struct {
    group_id: u64,
    writes: std.ArrayListUnmanaged(db_mod.types.BatchWrite) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    relational_identity_rewrites: std.ArrayListUnmanaged(db_mod.types.RelationalIdentityRewrite) = .empty,
    transforms: std.ArrayListUnmanaged(db_mod.types.DocumentTransform) = .empty,

    pub fn deinit(self: *GroupBatch, alloc: std.mem.Allocator) void {
        self.writes.deinit(alloc);
        self.deletes.deinit(alloc);
        self.relational_identity_rewrites.deinit(alloc);
        self.transforms.deinit(alloc);
        self.* = undefined;
    }
};

pub const TableEmptyingRequest = struct {
    primary_table_name: []const u8,
    additional_table_names: []const []const u8 = &.{},
    schema: storage_schema.TableSchema,
    mutation: db_mod.types.RelationalRowsMutationSourceRequest,
    sync_level: db_mod.types.SyncLevel = .write,
    restart_identity: bool = false,
    cascade: bool = false,
};

pub const RaftBatcher = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        batch_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!void,
        batch_group_local: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!void,
    };

    pub fn batchGroup(
        self: RaftBatcher,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !void {
        return try self.vtable.batch_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn batchGroupLocal(
        self: RaftBatcher,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !void {
        return try self.vtable.batch_group_local(self.ptr, alloc, group_id, table_name, req);
    }
};

pub const TableWriteSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: tables_api.CreateTableRequest,
        ) anyerror!?void = null,
        create_catalog_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            req: tables_api.CreateTableRequest,
        ) anyerror!?void = null,
        update_schema: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            schema_json: []const u8,
        ) anyerror!?void = null,
        create_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            index_json: []const u8,
        ) anyerror!?void = null,
        create_catalog_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            index_name: []const u8,
            index_json: []const u8,
        ) anyerror!?void = null,
        drop_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
        drop_catalog_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            index_name: []const u8,
        ) anyerror!?void = null,
        graph_metric_action: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            metric_name: []const u8,
            action: []const u8,
        ) anyerror!?db_mod.types.GraphMetricStatus = null,
        reprocess_document_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) anyerror!?bool = null,
        reprocess_document_artifact_range: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            req: db_mod.types.DocumentArtifactTableReprocessRequest,
        ) anyerror!?db_mod.types.DocumentArtifactTableReprocessResult = null,
        reprocess_document_artifact_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) anyerror!?bool = null,
        graph_metric_maintenance_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?[]u8 = null,
        drop_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            group_ids: []const u64,
        ) anyerror!?void = null,
        drop_catalog_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            group_ids: []const u64,
        ) anyerror!?void = null,
        backup_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backups_api.TableBackupPlan,
        ) anyerror!?[]backups_api.ShardSnapshot = null,
        backup_table_to_location: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            format: backups_api.BackupFormat,
            location_uri: []const u8,
            location: *backups_api.BackupLocation,
        ) anyerror!?[]backups_api.ShardSnapshot = null,
        backup_catalog_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            plan: backups_api.TableBackupPlan,
        ) anyerror!?[]backups_api.ShardSnapshot = null,
        restore_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backups_api.TableRestorePlan,
        ) anyerror!?void = null,
        restore_catalog_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            plan: backups_api.TableRestorePlan,
        ) anyerror!?void = null,
        commit_transaction: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        commit_transaction_with_id: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        batch: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void,
        batch_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void = null,
        mutate_rows_from_source: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsMutationSourceRequest,
        ) anyerror!?db_mod.types.RelationalRowsMutationSourceResult = null,
        mutate_rows_from_source_autocommit: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsMutationSourceRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?db_mod.types.RelationalRowsMutationSourceResult = null,
        table_emptying: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: TableEmptyingRequest,
        ) anyerror!?db_mod.types.RelationalRowsMutationSourceResult = null,
        mutate_rows_joined_from_source_rows: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            target_schema: storage_schema.TableSchema,
            source_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
            source_rows: []const []const u8,
        ) anyerror!?db_mod.types.RelationalRowsMutationSourceResult = null,
        mutate_rows_joined_from_source_rows_autocommit: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            target_schema: storage_schema.TableSchema,
            source_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
            source_rows: []const []const u8,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?db_mod.types.RelationalRowsMutationSourceResult = null,
        merge_rows_from_source_rows: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            target_schema: storage_schema.TableSchema,
            source_schema: storage_schema.TableSchema,
            plan: sql_adapter.LoweredMergeMutationPlan,
            source_rows: []const []const u8,
        ) anyerror!?relational_rows_api.OwnedRowsBatchRequest = null,
        begin_bulk_ingest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?void = null,
        finish_bulk_ingest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            options: backend_types.BulkIngestFinishOptions,
        ) anyerror!?void = null,
        abort_bulk_ingest: ?*const fn (
            ptr: *anyopaque,
            table_name: []const u8,
        ) void = null,
        batch_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void = null,
        txn_begin_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            topology_epoch: u64,
            participants: []const []const u8,
        ) anyerror!?void = null,
        txn_prepare_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            topology_epoch: u64,
            req: db_mod.types.TransactionIntentRequest,
        ) anyerror!?void = null,
        txn_resolve_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            status: db_mod.types.TxnStatus,
            commit_version: u64,
        ) anyerror!?void = null,
        txn_status_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!?db_mod.types.TxnStatus = null,
        corrupt_embedding_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
        local_runtime_statuses: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
        local_runtime_statuses_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
        foreign_key_integrity: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            action: ForeignKeyIntegrityAction,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            violation_limit: usize,
        ) anyerror!?ForeignKeyIntegrityResult = null,
        foreign_key_integrity_worker_pass: ?*const fn (
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
        ) anyerror!?ForeignKeyIntegrityResult = null,
        foreign_key_integrity_schema_controller_pass: ?*const fn (
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
        ) anyerror!?ForeignKeyIntegrityResult = null,
        foreign_key_integrity_schema_controller_maintenance_pass: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            options: ForeignKeyIntegritySchemaControllerOptions,
        ) anyerror!?ForeignKeyIntegritySchemaControllerResult = null,
        foreign_key_action_job_page: ?*const fn (
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
        ) anyerror!?ForeignKeyActionJobResult = null,
        foreign_key_action_job_schedule: ?*const fn (
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
        ) anyerror!?ForeignKeyActionJobResult = null,
        foreign_key_action_job_requeue: ?*const fn (
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
        ) anyerror!?ForeignKeyActionJobResult = null,
        foreign_key_action_job_group_local: ?*const fn (
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
        ) anyerror!?ForeignKeyActionJobStatus = null,
        foreign_key_action_job_group_local_schedule: ?*const fn (
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
        ) anyerror!?ForeignKeyActionJobStatus = null,
        foreign_key_action_job_group_local_requeue: ?*const fn (
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
        ) anyerror!?ForeignKeyActionJobStatus = null,
        foreign_key_action_job_progress: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?ForeignKeyActionJobProgressResult = null,
        foreign_key_action_job_group_local_progress: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?ForeignKeyActionJobProgressResult = null,
        foreign_key_action_schedule_progress: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?ForeignKeyActionScheduleProgressResult = null,
        foreign_key_action_schedule_group_local_progress: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?ForeignKeyActionScheduleProgressResult = null,
        foreign_key_action_schedule_mark_seeded: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            schedule_id: []const u8,
            scheduled_groups: u64,
        ) anyerror!?ForeignKeyActionScheduleStatus = null,
        foreign_key_action_schedule_requeue: ?*const fn (
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
        ) anyerror!?ForeignKeyActionScheduleStatus = null,
        foreign_key_action_schedule_group_local_mark_seeded: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            schedule_id: []const u8,
            scheduled_groups: u64,
        ) anyerror!?ForeignKeyActionScheduleStatus = null,
        foreign_key_action_schedule_group_local_requeue: ?*const fn (
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
        ) anyerror!?ForeignKeyActionScheduleStatus = null,
        unique_constraint_integrity: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            action: UniqueConstraintIntegrityAction,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) anyerror!?UniqueConstraintIntegrityResult = null,
        unique_constraint_integrity_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            action: UniqueConstraintIntegrityAction,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) anyerror!?UniqueConstraintIntegrityResult = null,
        unique_constraint_integrity_schema_controller_maintenance_pass: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            options: UniqueConstraintIntegritySchemaControllerOptions,
        ) anyerror!?UniqueConstraintIntegritySchemaControllerResult = null,
        secondary_index_rebuild_worker_pass: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            worker_id: []const u8,
            lease_ms: u64,
            max_work_units: usize,
        ) anyerror!?SecondaryIndexRebuildWorkerPassResult = null,
        secondary_index_rebuild_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
            worker_id: []const u8,
            lease_ms: u64,
        ) anyerror!?SecondaryIndexRebuildWorkerResult = null,
        schema_rewrite_worker_pass: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            worker_id: []const u8,
            lease_ms: u64,
            max_work_units: usize,
        ) anyerror!?SchemaRewriteWorkerPassResult = null,
        schema_rewrite_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            record: metadata_table_manager.SchemaRewriteJobRecord,
            worker_id: []const u8,
            lease_ms: u64,
        ) anyerror!?SchemaRewriteWorkerResult = null,
        table_emptying_worker_pass: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            worker_id: []const u8,
            lease_ms: u64,
            max_work_units: usize,
        ) anyerror!?TableEmptyingWorkerPassResult = null,
        table_emptying_worker_pass_for_table_id: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_id: u64,
            table_name: []const u8,
            worker_id: []const u8,
            lease_ms: u64,
            max_work_units: usize,
        ) anyerror!?TableEmptyingWorkerPassResult = null,
        table_emptying_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            record: metadata_table_manager.TableEmptyingJobRecord,
            worker_id: []const u8,
            lease_ms: u64,
        ) anyerror!?TableEmptyingWorkerResult = null,
        foreign_key_integrity_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            action: ForeignKeyIntegrityAction,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            violation_limit: usize,
        ) anyerror!?ForeignKeyIntegrityResult = null,
        foreign_key_integrity_work_unit_group_local: ?*const fn (
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
        ) anyerror!?ForeignKeyIntegrityResult = null,
        foreign_key_ref_children_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            limit: usize,
        ) anyerror!?[]db_mod.types.ForeignKeyRefChild = null,
        foreign_key_ref_children_page_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            start_after_child_table: ?[]const u8,
            start_after_child_key: ?[]const u8,
            limit: usize,
        ) anyerror!?db_mod.types.ForeignKeyRefChildrenPage = null,
    };

    pub fn batch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try self.vtable.batch(self.ptr, alloc, table_name, req);
    }

    pub fn batchCatalog(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: db_mod.types.BatchRequest,
    ) !?void {
        if (self.vtable.batch_catalog) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, req);
        return error.UnsupportedOperation;
    }

    pub fn mutateRowsFromSource(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const fn_ptr = self.vtable.mutate_rows_from_source orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, schema, req);
    }

    pub fn mutateRowsFromSourceAutocommit(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const fn_ptr = self.vtable.mutate_rows_from_source_autocommit orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, schema, req, sync_level);
    }

    pub fn tableEmptying(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        req: TableEmptyingRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const fn_ptr = self.vtable.table_emptying orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, req);
    }

    pub fn tableEmptyingWorkerPass(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?TableEmptyingWorkerPassResult {
        const fn_ptr = self.vtable.table_emptying_worker_pass orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, worker_id, lease_ms, max_work_units);
    }

    pub fn tableEmptyingWorkerPassForTableId(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_id: u64,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?TableEmptyingWorkerPassResult {
        const fn_ptr = self.vtable.table_emptying_worker_pass_for_table_id orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_id, table_name, worker_id, lease_ms, max_work_units);
    }

    pub fn tableEmptyingGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.TableEmptyingJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?TableEmptyingWorkerResult {
        const fn_ptr = self.vtable.table_emptying_group_local orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    pub fn mutateRowsJoinedFromSourceRows(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const fn_ptr = self.vtable.mutate_rows_joined_from_source_rows orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, target_schema, source_schema, req, source_rows);
    }

    pub fn mutateRowsJoinedFromSourceRowsAutocommit(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
        sync_level: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const fn_ptr = self.vtable.mutate_rows_joined_from_source_rows_autocommit orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, target_schema, source_schema, req, source_rows, sync_level);
    }

    pub fn mergeRowsFromSourceRows(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        plan: sql_adapter.LoweredMergeMutationPlan,
        source_rows: []const []const u8,
    ) !?relational_rows_api.OwnedRowsBatchRequest {
        const fn_ptr = self.vtable.merge_rows_from_source_rows orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, target_schema, source_schema, plan, source_rows);
    }

    pub fn beginBulkIngest(self: TableWriteSource, alloc: std.mem.Allocator, table_name: []const u8) !?void {
        const fn_ptr = self.vtable.begin_bulk_ingest orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn finishBulkIngest(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const fn_ptr = self.vtable.finish_bulk_ingest orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, options);
    }

    pub fn abortBulkIngest(self: TableWriteSource, table_name: []const u8) void {
        const fn_ptr = self.vtable.abort_bulk_ingest orelse return;
        fn_ptr(self.ptr, table_name);
    }

    pub fn createTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const fn_ptr = self.vtable.create_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn createCatalogTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: tables_api.CreateTableRequest,
    ) !?void {
        if (self.vtable.create_catalog_table) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, req);
        return error.UnsupportedOperation;
    }

    pub fn updateSchema(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.update_schema orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, schema_json);
    }

    pub fn createIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.create_index orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, index_name, index_json);
    }

    pub fn createCatalogIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        if (self.vtable.create_catalog_index) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, index_name, index_json);
        return error.UnsupportedOperation;
    }

    pub fn dropIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.drop_index orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, index_name);
    }

    pub fn dropCatalogIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        index_name: []const u8,
    ) !?void {
        if (self.vtable.drop_catalog_index) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, index_name);
        return error.UnsupportedOperation;
    }

    pub fn graphMetricAction(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
    ) !?db_mod.types.GraphMetricStatus {
        const fn_ptr = self.vtable.graph_metric_action orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, index_name, metric_name, action);
    }

    pub fn reprocessDocumentArtifact(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const fn_ptr = self.vtable.reprocess_document_artifact orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name);
    }

    pub fn reprocessDocumentArtifactRange(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const fn_ptr = self.vtable.reprocess_document_artifact_range orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, req);
    }

    pub fn reprocessDocumentArtifactGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const fn_ptr = self.vtable.reprocess_document_artifact_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, doc_key, artifact_name);
    }

    pub fn graphMetricMaintenanceGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const fn_ptr = self.vtable.graph_metric_maintenance_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn dropTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_ids: []const u64,
    ) !?void {
        const fn_ptr = self.vtable.drop_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, group_ids);
    }

    pub fn dropCatalogTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        group_ids: []const u64,
    ) !?void {
        if (self.vtable.drop_catalog_table) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, group_ids);
        return error.UnsupportedOperation;
    }

    pub fn backupTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, plan);
    }

    pub fn backupTableToLocation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        format: backups_api.BackupFormat,
        location_uri: []const u8,
        location: *backups_api.BackupLocation,
    ) !?[]backups_api.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table_to_location orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, backup_id, format, location_uri, location);
    }

    pub fn backupCatalogTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const fn_ptr = self.vtable.backup_catalog_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, plan);
    }

    pub fn restoreTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const fn_ptr = self.vtable.restore_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, plan);
    }

    pub fn restoreCatalogTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const fn_ptr = self.vtable.restore_catalog_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, plan);
    }

    pub fn commitTransaction(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_transaction orelse return null;
        return try fn_ptr(self.ptr, alloc, tables, sync_level);
    }

    pub fn commitTransactionWithId(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_transaction_with_id orelse return null;
        return try fn_ptr(self.ptr, alloc, txn_id, begin_timestamp, tables, sync_level);
    }

    pub fn batchGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const fn_ptr = self.vtable.batch_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn txnBeginGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        participants: []const []const u8,
    ) !?void {
        const fn_ptr = self.vtable.txn_begin_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, participants);
    }

    pub fn txnPrepareGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const fn_ptr = self.vtable.txn_prepare_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, topology_epoch, req);
    }

    pub fn txnResolveGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const fn_ptr = self.vtable.txn_resolve_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, status, commit_version);
    }

    pub fn txnStatusGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id);
    }

    pub fn corruptEmbeddingArtifact(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.corrupt_embedding_artifact orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, index_name);
    }

    pub fn localRuntimeStatuses(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn localRuntimeStatusesCatalog(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        if (self.vtable.local_runtime_statuses_catalog) |fn_ptr| return try fn_ptr(self.ptr, alloc, target);
        return error.UnsupportedOperation;
    }

    pub fn foreignKeyIntegrity(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const fn_ptr = self.vtable.foreign_key_integrity orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    pub fn foreignKeyIntegrityWorkerPass(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_integrity_worker_pass orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, action, job_id, worker_id, lease_ms, max_work_units, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    pub fn foreignKeyIntegritySchemaControllerPass(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_integrity_schema_controller_pass orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, action, worker_id, lease_ms, max_work_units, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    pub fn foreignKeyIntegritySchemaControllerMaintenancePass(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        options: ForeignKeyIntegritySchemaControllerOptions,
    ) !?ForeignKeyIntegritySchemaControllerResult {
        const fn_ptr = self.vtable.foreign_key_integrity_schema_controller_maintenance_pass orelse return null;
        return try fn_ptr(self.ptr, alloc, options);
    }

    pub fn foreignKeyActionJobPage(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_job_page orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms);
    }

    pub fn foreignKeyActionJobGroupLocal(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_job_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, cascade_depth, cascade_max_depth);
    }

    pub fn foreignKeyActionJobSchedule(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_job_schedule orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, cascade_depth, cascade_max_depth);
    }

    pub fn foreignKeyActionJobGroupLocalSchedule(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_job_group_local_schedule orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, cascade_depth, cascade_max_depth);
    }

    pub fn foreignKeyActionJobRequeue(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_job_requeue orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn foreignKeyActionJobGroupLocalRequeue(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_job_group_local_requeue orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn foreignKeyActionJobProgress(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const fn_ptr = self.vtable.foreign_key_action_job_progress orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn foreignKeyActionJobGroupLocalProgress(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const fn_ptr = self.vtable.foreign_key_action_job_group_local_progress orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name);
    }

    pub fn foreignKeyActionScheduleProgress(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const fn_ptr = self.vtable.foreign_key_action_schedule_progress orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn foreignKeyActionScheduleGroupLocalProgress(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const fn_ptr = self.vtable.foreign_key_action_schedule_group_local_progress orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name);
    }

    pub fn foreignKeyActionScheduleMarkSeeded(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const fn_ptr = self.vtable.foreign_key_action_schedule_mark_seeded orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, schedule_id, scheduled_groups);
    }

    pub fn foreignKeyActionScheduleRequeue(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_schedule_requeue orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn foreignKeyActionScheduleGroupLocalMarkSeeded(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const fn_ptr = self.vtable.foreign_key_action_schedule_group_local_mark_seeded orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, schedule_id, scheduled_groups);
    }

    pub fn foreignKeyActionScheduleGroupLocalRequeue(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_action_schedule_group_local_requeue orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn uniqueConstraintIntegrity(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const fn_ptr = self.vtable.unique_constraint_integrity orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, action, lower_doc_key, upper_doc_key);
    }

    pub fn uniqueConstraintIntegrityGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const fn_ptr = self.vtable.unique_constraint_integrity_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, action, lower_doc_key, upper_doc_key);
    }

    pub fn uniqueConstraintIntegritySchemaControllerMaintenancePass(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        options: UniqueConstraintIntegritySchemaControllerOptions,
    ) !?UniqueConstraintIntegritySchemaControllerResult {
        const fn_ptr = self.vtable.unique_constraint_integrity_schema_controller_maintenance_pass orelse return null;
        return try fn_ptr(self.ptr, alloc, options);
    }

    pub fn secondaryIndexRebuildWorkerPass(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?SecondaryIndexRebuildWorkerPassResult {
        const fn_ptr = self.vtable.secondary_index_rebuild_worker_pass orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, worker_id, lease_ms, max_work_units);
    }

    pub fn secondaryIndexRebuildGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SecondaryIndexRebuildWorkerResult {
        const fn_ptr = self.vtable.secondary_index_rebuild_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    pub fn schemaRewriteWorkerPass(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?SchemaRewriteWorkerPassResult {
        const fn_ptr = self.vtable.schema_rewrite_worker_pass orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, worker_id, lease_ms, max_work_units);
    }

    pub fn schemaRewriteGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SchemaRewriteJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SchemaRewriteWorkerResult {
        const fn_ptr = self.vtable.schema_rewrite_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    pub fn foreignKeyIntegrityGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const fn_ptr = self.vtable.foreign_key_integrity_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    pub fn foreignKeyIntegrityWorkUnitGroupLocal(
        self: TableWriteSource,
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
        const fn_ptr = self.vtable.foreign_key_integrity_work_unit_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, action, phase, job_id, claim_key, worker_id, lease_ms, max_work_units, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    pub fn foreignKeyRefChildrenGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        limit: usize,
    ) !?[]db_mod.types.ForeignKeyRefChild {
        if (self.vtable.foreign_key_ref_children_group_local) |fn_ptr| {
            return try fn_ptr(self.ptr, alloc, group_id, table_name, constraint_name, parent_table, parent_key, limit);
        }
        var page = (try self.foreignKeyRefChildrenPageGroupLocal(alloc, group_id, table_name, constraint_name, parent_table, parent_key, null, null, limit)) orelse return null;
        errdefer freeForeignKeyRefChildrenPage(alloc, &page);
        if (!page.complete) return error.ForeignKeyActionLimitExceeded;
        const children = page.children;
        page.children = &.{};
        freeForeignKeyRefChildrenPage(alloc, &page);
        return children;
    }

    pub fn foreignKeyRefChildrenPageGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        start_after_child_table: ?[]const u8,
        start_after_child_key: ?[]const u8,
        limit: usize,
    ) !?db_mod.types.ForeignKeyRefChildrenPage {
        const fn_ptr = self.vtable.foreign_key_ref_children_page_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
    }
};

pub fn freeForeignKeyRefChildrenPage(alloc: std.mem.Allocator, page: *db_mod.types.ForeignKeyRefChildrenPage) void {
    for (page.children) |child| {
        alloc.free(@constCast(child.child_table));
        alloc.free(@constCast(child.child_key));
    }
    if (page.children.len > 0) alloc.free(page.children);
    if (page.next_child_table) |value| alloc.free(@constCast(value));
    if (page.next_child_key) |value| alloc.free(@constCast(value));
    page.* = undefined;
}

test "table write source core forwards required batch and defaults optional capabilities" {
    const Probe = struct {
        batch_calls: usize = 0,

        fn batch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_calls += 1;
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(usize, 0), req.writes.len);
            return null;
        }
    };

    var probe = Probe{};
    const source = TableWriteSource{
        .ptr = &probe,
        .vtable = &.{ .batch = Probe.batch },
    };

    try std.testing.expect((try source.beginBulkIngest(std.testing.allocator, "docs")) == null);
    try std.testing.expect((try source.batch(std.testing.allocator, "docs", .{ .writes = &.{} })) == null);
    try std.testing.expectEqual(@as(usize, 1), probe.batch_calls);
    try std.testing.expectError(error.UnsupportedOperation, source.localRuntimeStatusesCatalog(std.testing.allocator, .{
        .database_name = "default",
        .namespace_name = "public",
        .table_name = "docs",
    }));
}
