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

//! Import-facing table write callback contract.
//! Implementations stay in table_writes.zig.

const std = @import("std");
const db_mod = struct {
    pub const types = @import("../storage/db/types.zig");
    pub const DocumentArtifactChildRangeApplyBatch = @import("../storage/db/document_artifact_child_range.zig").ApplyBatch;
};
const backend_types = @import("../storage/backend_types.zig");
const table_create_contract = @import("table_create_contract.zig");
const backup_contract = @import("backup_contract.zig");
const distributed_txn = @import("distributed_txn_contract.zig");
const runtime_status = @import("runtime_status.zig");

pub const LocalStructuralReconcileState = enum {
    complete,
    repair_pending,
    busy,
    degraded,
};

pub const LocalStructuralReconcileResult = struct {
    state: LocalStructuralReconcileState = .complete,
    indexes_added: u64 = 0,
    indexes_removed: u64 = 0,
    indexes_pending: u64 = 0,
    repair_discovered: u64 = 0,
    repair_attempted: u64 = 0,
    repair_repaired: u64 = 0,
    repair_remaining: u64 = 0,
    repair_terminal: u64 = 0,
    repair_busy: u64 = 0,
    repair_disk_waits: u64 = 0,
    next_retry_at_ms: u64 = 0,
};

pub const TableWriteSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: table_create_contract.CreateTableRequest,
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
        put_artifact_enrichment: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            enrichment_json: []const u8,
        ) anyerror!?void = null,
        delete_artifact_enrichment: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
        ) anyerror!?void = null,
        drop_index: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
        drop_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            group_ids: []const u64,
        ) anyerror!?void = null,
        backup_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backup_contract.TableBackupPlan,
        ) anyerror!?[]backup_contract.ShardSnapshot = null,
        backup_table_to_location: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            format: backup_contract.BackupFormat,
            location_uri: []const u8,
            connection: []const u8,
            location: *anyopaque,
        ) anyerror!?[]backup_contract.ShardSnapshot = null,
        restore_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backup_contract.TableRestorePlan,
        ) anyerror!?void = null,
        begin_restore_lifecycle: ?*const fn (
            ptr: *anyopaque,
            table_name: []const u8,
        ) anyerror!void = null,
        finish_restore_lifecycle: ?*const fn (
            ptr: *anyopaque,
            table_name: []const u8,
        ) void = null,
        commit_transaction: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        commit_batch: ?*const fn (
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
        acknowledge_transaction_commit: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            coordinator_group_id: u64,
            coordinator_table_name: []const u8,
        ) anyerror!?void = null,
        batch: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void,
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
            retain_terminal: bool,
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
            topology_epoch: u64,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?void = null,
        txn_status_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!?db_mod.types.TxnStatus = null,
        txn_acknowledge_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            participant: []const u8,
        ) anyerror!?void = null,
        corrupt_embedding_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
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
        list_artifact_repair_issues: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.ArtifactRepairListRequest,
        ) anyerror!?db_mod.types.ArtifactRepairListResult = null,
        repair_artifact_issues: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.ArtifactRepairRunRequest,
        ) anyerror!?db_mod.types.ArtifactRepairResult = null,
        repair_artifact_issues_controlled: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.ArtifactRepairRunRequest,
            options: db_mod.types.ArtifactRepairRunOptions,
        ) anyerror!?db_mod.types.ArtifactRepairResult = null,
        update_document_artifact_child_range_placement: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
        ) anyerror!?bool = null,
        apply_document_artifact_child_range_batch: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
        ) anyerror!?u64 = null,
        reprocess_document_artifact_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) anyerror!?bool = null,
        reprocess_document_artifact_range_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            artifact_name: []const u8,
            req: db_mod.types.DocumentArtifactTableReprocessRequest,
        ) anyerror!?db_mod.types.DocumentArtifactTableReprocessResult = null,
        list_artifact_repair_issues_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.ArtifactRepairListRequest,
        ) anyerror!?db_mod.types.ArtifactRepairListResult = null,
        repair_artifact_issues_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.ArtifactRepairRunRequest,
        ) anyerror!?db_mod.types.ArtifactRepairResult = null,
        repair_artifact_issues_group_local_controlled: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.ArtifactRepairRunRequest,
            options: db_mod.types.ArtifactRepairRunOptions,
        ) anyerror!?db_mod.types.ArtifactRepairResult = null,
        update_document_artifact_child_range_placement_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
        ) anyerror!?bool = null,
        apply_document_artifact_child_range_batch_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
        ) anyerror!?u64 = null,
        local_runtime_statuses: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
        request_table_structural_reconcile: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?void = null,
        request_table_index_structural_reconcile: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
        ) anyerror!?void = null,
        reconcile_table_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
            target_index_name: ?[]const u8,
            advance_index_repair: bool,
        ) anyerror!?LocalStructuralReconcileResult = null,
        retire_table_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?void = null,
    };

    pub fn batch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try self.vtable.batch(self.ptr, alloc, table_name, req);
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
        req: table_create_contract.CreateTableRequest,
    ) !?void {
        const fn_ptr = self.vtable.create_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req);
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

    pub fn putArtifactEnrichment(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        enrichment_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.put_artifact_enrichment orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, enrichment_json);
    }

    pub fn deleteArtifactEnrichment(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.delete_artifact_enrichment orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name);
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

    pub fn dropTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_ids: []const u64,
    ) !?void {
        const fn_ptr = self.vtable.drop_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, group_ids);
    }

    pub fn backupTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backup_contract.TableBackupPlan,
    ) !?[]backup_contract.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, plan);
    }

    pub fn backupTableToLocation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        format: backup_contract.BackupFormat,
        location_uri: []const u8,
        connection: []const u8,
        location: *anyopaque,
    ) !?[]backup_contract.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table_to_location orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, backup_id, format, location_uri, connection, location);
    }

    pub fn restoreTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backup_contract.TableRestorePlan,
    ) !?void {
        const lifecycle_active = try self.beginRestoreLifecycle(table_name);
        defer if (lifecycle_active) self.finishRestoreLifecycle(table_name);
        return try self.restoreTableReserved(alloc, table_name, plan);
    }

    /// Executes restore work under a lifecycle reservation already held by the
    /// caller. Coordinators use this when the reservation must span metadata
    /// replacement or remote snapshot staging; ordinary callers should use
    /// restoreTable so write fencing cannot be omitted.
    pub fn restoreTableReserved(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backup_contract.TableRestorePlan,
    ) !?void {
        const fn_ptr = self.vtable.restore_table orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, plan);
    }

    pub fn beginRestoreLifecycle(self: TableWriteSource, table_name: []const u8) !bool {
        const fn_ptr = self.vtable.begin_restore_lifecycle orelse return false;
        try fn_ptr(self.ptr, table_name);
        return true;
    }

    pub fn finishRestoreLifecycle(self: TableWriteSource, table_name: []const u8) void {
        const fn_ptr = self.vtable.finish_restore_lifecycle orelse return;
        fn_ptr(self.ptr, table_name);
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

    pub fn commitBatch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_batch orelse self.vtable.commit_transaction orelse return null;
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

    pub fn acknowledgeTransactionCommit(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        coordinator_group_id: u64,
        coordinator_table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.acknowledge_transaction_commit orelse return null;
        return try fn_ptr(self.ptr, alloc, txn_id, coordinator_group_id, coordinator_table_name);
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
        retain_terminal: bool,
        participants: []const []const u8,
    ) !?void {
        const fn_ptr = self.vtable.txn_begin_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, retain_terminal, participants);
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
        topology_epoch: u64,
        sync_level: db_mod.types.SyncLevel,
    ) !?void {
        const fn_ptr = self.vtable.txn_resolve_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level);
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

    pub fn txnAcknowledgeGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        participant: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.txn_acknowledge_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, txn_id, participant);
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

    pub fn listArtifactRepairIssues(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !?db_mod.types.ArtifactRepairListResult {
        const fn_ptr = self.vtable.list_artifact_repair_issues orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn repairArtifactIssues(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
    ) !?db_mod.types.ArtifactRepairResult {
        const fn_ptr = self.vtable.repair_artifact_issues orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn repairArtifactIssuesControlled(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
        options: db_mod.types.ArtifactRepairRunOptions,
    ) !?db_mod.types.ArtifactRepairResult {
        const fn_ptr = self.vtable.repair_artifact_issues_controlled orelse {
            if (options.cancelled()) return error.Canceled;
            return try self.repairArtifactIssues(alloc, table_name, req);
        };
        return try fn_ptr(self.ptr, alloc, table_name, req, options);
    }

    pub fn listArtifactRepairIssuesGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !?db_mod.types.ArtifactRepairListResult {
        const fn_ptr = self.vtable.list_artifact_repair_issues_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn repairArtifactIssuesGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
    ) !?db_mod.types.ArtifactRepairResult {
        const fn_ptr = self.vtable.repair_artifact_issues_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn repairArtifactIssuesGroupLocalControlled(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
        options: db_mod.types.ArtifactRepairRunOptions,
    ) !?db_mod.types.ArtifactRepairResult {
        const fn_ptr = self.vtable.repair_artifact_issues_group_local_controlled orelse {
            if (options.cancelled()) return error.Canceled;
            return try self.repairArtifactIssuesGroupLocal(alloc, group_id, table_name, req);
        };
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, options);
    }

    pub fn updateDocumentArtifactChildRangePlacement(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const fn_ptr = self.vtable.update_document_artifact_child_range_placement orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name, update);
    }

    pub fn applyDocumentArtifactChildRangeBatch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        const fn_ptr = self.vtable.apply_document_artifact_child_range_batch orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, doc_key, artifact_name, child_batch);
    }

    pub fn reprocessDocumentArtifactRangeGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const fn_ptr = self.vtable.reprocess_document_artifact_range_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, artifact_name, req);
    }

    pub fn updateDocumentArtifactChildRangePlacementGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const fn_ptr = self.vtable.update_document_artifact_child_range_placement_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, doc_key, artifact_name, update);
    }

    pub fn applyDocumentArtifactChildRangeBatchGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        const fn_ptr = self.vtable.apply_document_artifact_child_range_batch_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, doc_key, artifact_name, child_batch);
    }

    pub fn localRuntimeStatuses(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn requestTableStructuralReconcile(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.request_table_structural_reconcile orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn requestTableIndexStructuralReconcile(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.request_table_index_structural_reconcile orelse return try self.requestTableStructuralReconcile(alloc, table_name);
        return try fn_ptr(self.ptr, alloc, table_name, index_name);
    }

    pub fn reconcileTableGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?LocalStructuralReconcileResult {
        const fn_ptr = self.vtable.reconcile_table_group_local orelse return null;
        return try fn_ptr(self.ptr, group_id, table_name, target_index_name, advance_index_repair);
    }

    pub fn retireTableGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.retire_table_group_local orelse return null;
        return try fn_ptr(self.ptr, group_id, table_name);
    }
};
