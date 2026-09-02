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
    pub const TextMemoryAttributionStats = @import("../storage/db/text_memory_stats.zig").TextMemoryAttributionStats;
};
const backend_types = @import("../storage/backend_types.zig");
const table_create_contract = @import("table_create_contract.zig");
const backup_contract = @import("backup_contract.zig");
const distributed_txn = @import("distributed_txn_contract.zig");
const metadata_topology_protocol = @import("../metadata/topology_protocol.zig");
const runtime_status = @import("runtime_status.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const runtime_error_abi = @import("../runtime_error_abi.zig");
const runtime_native_abi = @import("../runtime_native_abi.zig");

pub const LocalStructuralReconcileState = enum {
    complete,
    repair_pending,
    busy,
    degraded,
    restore_repair_pending,
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
    restore_repair_attempted: u64 = 0,
    restore_repair_progressed: u64 = 0,
    restore_repair_pending: u64 = 0,
};

/// One exact structural observation captured before a transient compiled
/// owner is retired. The control plane publishes `runtime_status` under the
/// same table epoch that admitted the reconcile operation.
pub const LocalStructuralReconcileObservation = struct {
    result: LocalStructuralReconcileResult,
    runtime_status: ?runtime_status.LocalTableRuntimeStatus = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.runtime_status) |*status| status.deinit(alloc);
        self.* = undefined;
    }
};

pub const TableWriteSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

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
            contract: metadata_topology_protocol.DropCleanupContract,
        ) anyerror!?void = null,
        backup_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            plan: backup_contract.TableBackupPlan,
        ) anyerror!?[]backup_contract.ShardSnapshot = null,
        backup_table_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            plan: backup_contract.TableBackupPlan,
        ) anyerror!?[]backup_contract.ShardSnapshot = null,
        backup_table_to_location: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            format: backup_contract.BackupFormat,
            fence: backup_contract.TableBackupFence,
            location_uri: []const u8,
            connection: []const u8,
            location: *anyopaque,
            control: backup_contract.BackupOperationControl,
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
        begin_bulk_ingest_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?void = null,
        finish_bulk_ingest_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
            options: backend_types.BulkIngestFinishOptions,
        ) anyerror!?void = null,
        abort_bulk_ingest_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
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
        corrupt_embedding_artifact_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
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
        text_memory_attribution_stats_best_effort: ?*const fn (
            ptr: *anyopaque,
        ) db_mod.TextMemoryAttributionStats = null,
        preflight_write_admission_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?void = null,
        prepare_ha_seed_snapshot_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
            deadline_ns: u64,
        ) anyerror!?void = null,
        find_median_key_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?[]u8 = null,
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
        reconcile_table_group_local_transient: ?*const fn (
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
        /// Tail extensions retain the established vtable prefix. Cancellation
        /// tokens are transport-neutral borrowed callbacks and are valid over
        /// the private same-process native runtime boundary.
        commit_batch_with_cancellation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        txn_resolve_group_local_with_cancellation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            status: db_mod.types.TxnStatus,
            commit_version: u64,
            topology_epoch: u64,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?void = null,
        commit_transaction_with_id_with_cancellation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        commit_transaction_with_cancellation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        txn_begin_group_local_with_pre_decision_context: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            topology_epoch: u64,
            retain_terminal: bool,
            participants: []const []const u8,
            context: distributed_txn.PreDecisionContext,
        ) anyerror!?void = null,
        txn_prepare_group_local_with_pre_decision_context: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            topology_epoch: u64,
            req: db_mod.types.TransactionIntentRequest,
            context: distributed_txn.PreDecisionContext,
        ) anyerror!?void = null,
        reconcile_table_group_local_transient_observed: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            target_index_name: ?[]const u8,
            advance_index_repair: bool,
        ) anyerror!?LocalStructuralReconcileObservation = null,
        local_runtime_status_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatus = null,
        /// Routes a transaction decision observation to the current Raft
        /// leader and orders it behind a quorum read barrier.
        txn_status_group_linearizable: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!?db_mod.types.TxnStatus = null,
        txn_status_group_linearizable_until: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            deadline_ns: u64,
        ) anyerror!?db_mod.types.TxnStatus = null,
        /// Leader-local half of the linearizable status protocol. Internal
        /// RPC handlers use this callback so they fail closed on a follower
        /// instead of recursively forwarding.
        txn_status_group_authoritative_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!?db_mod.types.TxnStatus = null,
        txn_status_group_authoritative_local_until: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            deadline_ns: u64,
        ) anyerror!?db_mod.types.TxnStatus = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn batch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try BoundaryAbi.call("batch", self.boundary_dispatch, self.vtable.batch, .{ self.ptr, alloc, table_name, req });
    }

    pub fn beginBulkIngest(self: TableWriteSource, alloc: std.mem.Allocator, table_name: []const u8) !?void {
        const fn_ptr = self.vtable.begin_bulk_ingest orelse return null;
        return try BoundaryAbi.call("begin_bulk_ingest", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name });
    }

    pub fn finishBulkIngest(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const fn_ptr = self.vtable.finish_bulk_ingest orelse return null;
        return try BoundaryAbi.call("finish_bulk_ingest", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, options });
    }

    pub fn abortBulkIngest(self: TableWriteSource, table_name: []const u8) void {
        const fn_ptr = self.vtable.abort_bulk_ingest orelse return;
        fn_ptr(self.ptr, table_name);
    }

    pub fn beginBulkIngestGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.begin_bulk_ingest_group_local orelse return null;
        return try BoundaryAbi.call("begin_bulk_ingest_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name });
    }

    pub fn finishBulkIngestGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const fn_ptr = self.vtable.finish_bulk_ingest_group_local orelse return null;
        return try BoundaryAbi.call("finish_bulk_ingest_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, options });
    }

    pub fn abortBulkIngestGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
    ) void {
        const fn_ptr = self.vtable.abort_bulk_ingest_group_local orelse return;
        fn_ptr(self.ptr, group_id, table_name);
    }

    pub fn createTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: table_create_contract.CreateTableRequest,
    ) !?void {
        const fn_ptr = self.vtable.create_table orelse return null;
        return try BoundaryAbi.call("create_table", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, req });
    }

    pub fn updateSchema(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.update_schema orelse return null;
        return try BoundaryAbi.call("update_schema", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, schema_json });
    }

    pub fn createIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.create_index orelse return null;
        return try BoundaryAbi.call("create_index", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, index_name, index_json });
    }

    pub fn putArtifactEnrichment(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        enrichment_json: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.put_artifact_enrichment orelse return null;
        return try BoundaryAbi.call("put_artifact_enrichment", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, artifact_name, enrichment_json });
    }

    pub fn deleteArtifactEnrichment(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.delete_artifact_enrichment orelse return null;
        return try BoundaryAbi.call("delete_artifact_enrichment", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, artifact_name });
    }

    pub fn dropIndex(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.drop_index orelse return null;
        return try BoundaryAbi.call("drop_index", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, index_name });
    }

    pub fn dropTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        contract: metadata_topology_protocol.DropCleanupContract,
    ) !?void {
        const fn_ptr = self.vtable.drop_table orelse return null;
        return try BoundaryAbi.call("drop_table", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, contract });
    }

    pub fn backupTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backup_contract.TableBackupPlan,
    ) !?[]backup_contract.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table orelse return null;
        var owner_plan = plan;
        if (self.boundary_dispatch != BoundaryAbi.local_dispatch) owner_plan.io = null;
        return try BoundaryAbi.call("backup_table", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, owner_plan });
    }

    pub fn backupTableGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        plan: backup_contract.TableBackupPlan,
    ) !?[]backup_contract.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table_group_local orelse return null;
        var owner_plan = plan;
        if (self.boundary_dispatch != BoundaryAbi.local_dispatch) owner_plan.io = null;
        return try BoundaryAbi.call("backup_table_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, owner_plan });
    }

    pub fn backupTableToLocation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        format: backup_contract.BackupFormat,
        fence: backup_contract.TableBackupFence,
        location_uri: []const u8,
        connection: []const u8,
        location: *anyopaque,
        control: backup_contract.BackupOperationControl,
    ) !?[]backup_contract.ShardSnapshot {
        const fn_ptr = self.vtable.backup_table_to_location orelse return null;
        return try BoundaryAbi.call("backup_table_to_location", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, backup_id, format, fence, location_uri, connection, location, control });
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
        var owner_plan = plan;
        // Restore materialization performs all I/O in the storage runtime that
        // owns the callback. Do not pass the API archive's std.Io vtable across
        // this boundary.
        if (self.boundary_dispatch != BoundaryAbi.local_dispatch) owner_plan.io = null;
        return try BoundaryAbi.call("restore_table", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, owner_plan });
    }

    pub fn beginRestoreLifecycle(self: TableWriteSource, table_name: []const u8) !bool {
        const fn_ptr = self.vtable.begin_restore_lifecycle orelse return false;
        try BoundaryAbi.call("begin_restore_lifecycle", self.boundary_dispatch, fn_ptr, .{ self.ptr, table_name });
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
        return try BoundaryAbi.call("commit_transaction", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, tables, sync_level });
    }

    pub fn commitTransactionWithCancellation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_transaction_with_cancellation orelse
            return try self.commitTransaction(alloc, tables, sync_level);
        return try BoundaryAbi.call("commit_transaction_with_cancellation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, tables, sync_level, cancellation });
    }

    pub fn commitBatch(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_batch orelse self.vtable.commit_transaction orelse return null;
        return try BoundaryAbi.call("commit_batch", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, tables, sync_level });
    }

    pub fn commitBatchWithCancellation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_batch_with_cancellation orelse
            return try self.commitBatch(alloc, tables, sync_level);
        return try BoundaryAbi.call("commit_batch_with_cancellation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, tables, sync_level, cancellation });
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
        return try BoundaryAbi.call("commit_transaction_with_id", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, txn_id, begin_timestamp, tables, sync_level });
    }

    pub fn commitTransactionWithIdAndCancellation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?distributed_txn.CommitOutcome {
        const fn_ptr = self.vtable.commit_transaction_with_id_with_cancellation orelse
            return try self.commitTransactionWithId(alloc, txn_id, begin_timestamp, tables, sync_level);
        return try BoundaryAbi.call("commit_transaction_with_id_with_cancellation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, txn_id, begin_timestamp, tables, sync_level, cancellation });
    }

    pub fn acknowledgeTransactionCommit(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        coordinator_group_id: u64,
        coordinator_table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.acknowledge_transaction_commit orelse return null;
        return try BoundaryAbi.call("acknowledge_transaction_commit", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, txn_id, coordinator_group_id, coordinator_table_name });
    }

    pub fn batchGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const fn_ptr = self.vtable.batch_group_local orelse return null;
        return try BoundaryAbi.call("batch_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req });
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
        return try BoundaryAbi.call("txn_begin_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, retain_terminal, participants });
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
        return try BoundaryAbi.call("txn_prepare_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, topology_epoch, req });
    }

    pub fn txnBeginGroupLocalWithPreDecisionContext(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
        context: distributed_txn.PreDecisionContext,
    ) !?void {
        const fn_ptr = self.vtable.txn_begin_group_local_with_pre_decision_context orelse
            return try self.txnBeginGroupLocal(alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, retain_terminal, participants);
        return try BoundaryAbi.call("txn_begin_group_local_with_pre_decision_context", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, retain_terminal, participants, context });
    }

    pub fn txnPrepareGroupLocalWithPreDecisionContext(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
        context: distributed_txn.PreDecisionContext,
    ) !?void {
        const fn_ptr = self.vtable.txn_prepare_group_local_with_pre_decision_context orelse
            return try self.txnPrepareGroupLocal(alloc, group_id, table_name, txn_id, topology_epoch, req);
        return try BoundaryAbi.call("txn_prepare_group_local_with_pre_decision_context", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, topology_epoch, req, context });
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
        return try BoundaryAbi.call("txn_resolve_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level });
    }

    pub fn txnResolveGroupLocalWithCancellation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
        topology_epoch: u64,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
    ) !?void {
        const fn_ptr = self.vtable.txn_resolve_group_local_with_cancellation orelse
            return try self.txnResolveGroupLocal(alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level);
        return try BoundaryAbi.call("txn_resolve_group_local_with_cancellation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level, cancellation });
    }

    pub fn txnStatusGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_local orelse return null;
        return try BoundaryAbi.call("txn_status_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id });
    }

    pub fn txnStatusGroupLinearizable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_linearizable orelse
            return try self.txnStatusGroupLocal(alloc, group_id, table_name, txn_id);
        return try BoundaryAbi.call("txn_status_group_linearizable", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id });
    }

    pub fn txnStatusGroupLinearizableUntil(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        deadline_ns: u64,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_linearizable_until orelse
            return error.DeadlineAwareTxnStatusUnsupported;
        return try BoundaryAbi.call("txn_status_group_linearizable_until", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, deadline_ns });
    }

    pub fn txnStatusGroupAuthoritativeLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_authoritative_local orelse
            return try self.txnStatusGroupLocal(alloc, group_id, table_name, txn_id);
        return try BoundaryAbi.call("txn_status_group_authoritative_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id });
    }

    pub fn txnStatusGroupAuthoritativeLocalUntil(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        deadline_ns: u64,
    ) !?db_mod.types.TxnStatus {
        const fn_ptr = self.vtable.txn_status_group_authoritative_local_until orelse
            return error.DeadlineAwareTxnStatusUnsupported;
        return try BoundaryAbi.call("txn_status_group_authoritative_local_until", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, deadline_ns });
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
        return try BoundaryAbi.call("txn_acknowledge_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, participant });
    }

    pub fn corruptEmbeddingArtifact(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.corrupt_embedding_artifact orelse return null;
        return try BoundaryAbi.call("corrupt_embedding_artifact", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, doc_key, index_name });
    }

    pub fn corruptEmbeddingArtifactGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.corrupt_embedding_artifact_group_local orelse return null;
        return try BoundaryAbi.call("corrupt_embedding_artifact_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, index_name });
    }

    pub fn reprocessDocumentArtifact(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const fn_ptr = self.vtable.reprocess_document_artifact orelse return null;
        return try BoundaryAbi.call("reprocess_document_artifact", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, doc_key, artifact_name });
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
        return try BoundaryAbi.call("reprocess_document_artifact_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, artifact_name });
    }

    pub fn reprocessDocumentArtifactRange(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const fn_ptr = self.vtable.reprocess_document_artifact_range orelse return null;
        return try BoundaryAbi.call("reprocess_document_artifact_range", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, artifact_name, req });
    }

    pub fn listArtifactRepairIssues(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !?db_mod.types.ArtifactRepairListResult {
        const fn_ptr = self.vtable.list_artifact_repair_issues orelse return null;
        return try BoundaryAbi.call("list_artifact_repair_issues", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, req });
    }

    pub fn repairArtifactIssues(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
    ) !?db_mod.types.ArtifactRepairResult {
        const fn_ptr = self.vtable.repair_artifact_issues orelse return null;
        return try BoundaryAbi.call("repair_artifact_issues", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, req });
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
        return try BoundaryAbi.call("repair_artifact_issues_controlled", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, req, options });
    }

    pub fn listArtifactRepairIssuesGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairListRequest,
    ) !?db_mod.types.ArtifactRepairListResult {
        const fn_ptr = self.vtable.list_artifact_repair_issues_group_local orelse return null;
        return try BoundaryAbi.call("list_artifact_repair_issues_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req });
    }

    pub fn repairArtifactIssuesGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.ArtifactRepairRunRequest,
    ) !?db_mod.types.ArtifactRepairResult {
        const fn_ptr = self.vtable.repair_artifact_issues_group_local orelse return null;
        return try BoundaryAbi.call("repair_artifact_issues_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req });
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
        return try BoundaryAbi.call("repair_artifact_issues_group_local_controlled", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, options });
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
        return try BoundaryAbi.call("update_document_artifact_child_range_placement", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, doc_key, artifact_name, update });
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
        return try BoundaryAbi.call("apply_document_artifact_child_range_batch", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, artifact_name, child_batch });
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
        return try BoundaryAbi.call("reprocess_document_artifact_range_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, artifact_name, req });
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
        return try BoundaryAbi.call("update_document_artifact_child_range_placement_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, artifact_name, update });
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
        return try BoundaryAbi.call("apply_document_artifact_child_range_batch_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, artifact_name, child_batch });
    }

    pub fn localRuntimeStatuses(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try BoundaryAbi.call("local_runtime_statuses", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name });
    }

    pub fn localRuntimeStatusGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatus {
        const fn_ptr = self.vtable.local_runtime_status_group_local orelse return null;
        return try BoundaryAbi.call("local_runtime_status_group_local", self.boundary_dispatch, fn_ptr, .{
            self.ptr,
            alloc,
            group_id,
            table_name,
        });
    }

    pub fn textMemoryAttributionStatsBestEffort(
        self: TableWriteSource,
    ) db_mod.TextMemoryAttributionStats {
        const fn_ptr = self.vtable.text_memory_attribution_stats_best_effort orelse return .{};
        return fn_ptr(self.ptr);
    }

    pub fn requestTableStructuralReconcile(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.request_table_structural_reconcile orelse return null;
        return try BoundaryAbi.call("request_table_structural_reconcile", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name });
    }

    pub fn preflightWriteAdmissionGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.preflight_write_admission_group_local orelse return null;
        return try BoundaryAbi.call("preflight_write_admission_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name });
    }

    pub fn prepareHASeedSnapshotGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: u64,
    ) !?void {
        const fn_ptr = self.vtable.prepare_ha_seed_snapshot_group_local orelse return null;
        return try BoundaryAbi.call("prepare_ha_seed_snapshot_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, deadline_ns });
    }

    pub fn findMedianKeyGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?[]u8 {
        const fn_ptr = self.vtable.find_median_key_group_local orelse return null;
        return try BoundaryAbi.call("find_median_key_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name });
    }

    pub fn requestTableIndexStructuralReconcile(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.request_table_index_structural_reconcile orelse return try self.requestTableStructuralReconcile(alloc, table_name);
        return try BoundaryAbi.call("request_table_index_structural_reconcile", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, index_name });
    }

    pub fn reconcileTableGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?LocalStructuralReconcileResult {
        const fn_ptr = self.vtable.reconcile_table_group_local orelse return null;
        return try BoundaryAbi.call("reconcile_table_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, target_index_name, advance_index_repair });
    }

    /// Reconcile through a resident owner when one already exists, but do not
    /// turn a cold startup inspection into a long-lived writer owner.
    pub fn reconcileTableGroupLocalTransient(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?LocalStructuralReconcileResult {
        const fn_ptr = self.vtable.reconcile_table_group_local_transient orelse
            return try self.reconcileTableGroupLocal(
                group_id,
                table_name,
                target_index_name,
                advance_index_repair,
            );
        return try BoundaryAbi.call("reconcile_table_group_local_transient", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, target_index_name, advance_index_repair });
    }

    pub fn reconcileTableGroupLocalTransientObserved(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?LocalStructuralReconcileObservation {
        const fn_ptr = self.vtable.reconcile_table_group_local_transient_observed orelse {
            const result = (try self.reconcileTableGroupLocalTransient(
                group_id,
                table_name,
                target_index_name,
                advance_index_repair,
            )) orelse return null;
            return .{ .result = result };
        };
        return try BoundaryAbi.call("reconcile_table_group_local_transient_observed", self.boundary_dispatch, fn_ptr, .{
            self.ptr,
            alloc,
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
        });
    }

    pub fn retireTableGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.retire_table_group_local orelse return null;
        return try BoundaryAbi.call("retire_table_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name });
    }
};

test "compiled table write boundary transports cancellation and committed failure identity" {
    const Fake = struct {
        calls: usize = 0,
        transaction_calls: usize = 0,
        stateless_transaction_calls: usize = 0,
        pre_decision_calls: usize = 0,
        failure: ?anyerror = null,

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return null;
        }

        fn commitBatch(_: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel) anyerror!?distributed_txn.CommitOutcome {
            return error.TestUnexpectedResult;
        }

        fn commitBatchWithCancellation(ptr: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
            if (self.failure) |err| return err;
            return .{ .committed = .{ .participant_count = 1 } };
        }

        fn commitTransactionWithId(_: *anyopaque, _: std.mem.Allocator, _: db_mod.types.TxnId, _: u64, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel) anyerror!?distributed_txn.CommitOutcome {
            return error.TestUnexpectedResult;
        }

        fn commitTransactionWithIdAndCancellation(ptr: *anyopaque, _: std.mem.Allocator, _: db_mod.types.TxnId, _: u64, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.transaction_calls += 1;
            if (cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
            return .{ .committed = .{ .participant_count = 1 } };
        }

        fn commitTransactionWithCancellation(ptr: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.stateless_transaction_calls += 1;
            if (cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
            return .{ .committed = .{ .participant_count = 1 } };
        }

        fn txnBeginWithPreDecisionContext(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, context: distributed_txn.PreDecisionContext) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pre_decision_calls += 1;
            try std.testing.expectEqual(@as(?u64, 123), context.deadline_ns);
            try context.cancellation.check();
            if (self.failure) |err| return err;
            return {};
        }

        fn foreignDispatch(
            contract: *const runtime_native_abi.CallContract,
            callback: *const anyopaque,
            args: *const anyopaque,
            output: ?*anyopaque,
        ) callconv(.c) runtime_error_abi.Status {
            return TableWriteSource.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
    };

    var fake = Fake{};
    const source: TableWriteSource = .{
        .ptr = &fake,
        .vtable = &.{
            .batch = Fake.batch,
            .commit_batch = Fake.commitBatch,
            .commit_batch_with_cancellation = Fake.commitBatchWithCancellation,
            .commit_transaction_with_id = Fake.commitTransactionWithId,
            .commit_transaction_with_id_with_cancellation = Fake.commitTransactionWithIdAndCancellation,
            .commit_transaction_with_cancellation = Fake.commitTransactionWithCancellation,
            .txn_begin_group_local_with_pre_decision_context = Fake.txnBeginWithPreDecisionContext,
        },
        .boundary_dispatch = Fake.foreignDispatch,
    };

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.EnrichmentWaitCanceled,
        source.commitBatchWithCancellation(std.testing.allocator, &.{}, .enrichments, db_mod.types.CancellationToken.fromAtomic(&canceled)),
    );
    canceled.store(false, .release);
    fake.failure = error.EnrichmentWorkerFailed;
    try std.testing.expectError(
        error.EnrichmentWorkerFailed,
        source.commitBatchWithCancellation(std.testing.allocator, &.{}, .enrichments, db_mod.types.CancellationToken.fromAtomic(&canceled)),
    );
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    canceled.store(true, .release);
    try std.testing.expectError(
        error.EnrichmentWaitCanceled,
        source.commitTransactionWithIdAndCancellation(
            std.testing.allocator,
            std.mem.zeroes(db_mod.types.TxnId),
            1,
            &.{},
            .enrichments,
            db_mod.types.CancellationToken.fromAtomic(&canceled),
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), fake.transaction_calls);
    try std.testing.expectError(
        error.EnrichmentWaitCanceled,
        source.commitTransactionWithCancellation(
            std.testing.allocator,
            &.{},
            .enrichments,
            db_mod.types.CancellationToken.fromAtomic(&canceled),
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), fake.stateless_transaction_calls);
    canceled.store(false, .release);
    fake.failure = null;
    try std.testing.expect((try source.txnBeginGroupLocalWithPreDecisionContext(
        std.testing.allocator,
        7,
        "docs",
        std.mem.zeroes(db_mod.types.TxnId),
        1,
        2,
        false,
        &.{},
        .{
            .deadline_ns = 123,
            .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled),
        },
    )) != null);
    try std.testing.expectEqual(@as(usize, 1), fake.pre_decision_calls);
    fake.failure = error.PreDecisionDeadlineExceeded;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, source.txnBeginGroupLocalWithPreDecisionContext(
        std.testing.allocator,
        7,
        "docs",
        std.mem.zeroes(db_mod.types.TxnId),
        1,
        2,
        false,
        &.{},
        .{ .deadline_ns = 123 },
    ));
    try std.testing.expectEqual(@as(usize, 2), fake.pre_decision_calls);
}
