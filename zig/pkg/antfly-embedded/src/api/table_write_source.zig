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

test "controlled group batch context preserves aware dispatch and rejects legacy fence loss" {
    const State = struct {
        calls: usize = 0,
        received: distributed_txn.PreDecisionContext = .{},
        fn top(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return error.UnexpectedTopLevelBatch;
        }
        fn legacy(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.calls += 1;
            return {};
        }
        fn aware(raw: *anyopaque, _: std.mem.Allocator, group: u64, table: []const u8, _: db_mod.types.BatchRequest, context: distributed_txn.PreDecisionContext) !?void {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.calls += 1;
            scope.received = context;
            try std.testing.expectEqual(@as(u64, 7), group);
            try std.testing.expectEqualStrings("rows", table);
            return error.CatalogGenerationChanged;
        }
    };
    var state: State = .{};
    const aware_source: TableWriteSource = .{ .ptr = &state, .vtable = &.{ .batch = State.top, .batch_group_local = State.legacy, .batch_group_local_with_pre_decision_context = State.aware } };
    const context: distributed_txn.PreDecisionContext = .{ .deadline_ns = 123, .restore_staging_scope = @splat(4), .restore_staging_plan_id = @splat(5) };
    try std.testing.expectError(error.CatalogGenerationChanged, aware_source.batchGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "rows", .{}, context));
    try std.testing.expectEqualDeep(context, state.received);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    const legacy_source: TableWriteSource = .{ .ptr = &state, .vtable = &.{ .batch = State.top, .batch_group_local = State.legacy } };
    try std.testing.expectError(error.UnsupportedOperation, legacy_source.batchGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "rows", .{}, context));
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    _ = try legacy_source.batchGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "rows", .{}, .{});
    try std.testing.expectEqual(@as(usize, 2), state.calls);
}
const table_drop_contract = @import("table_drop_contract.zig");
const metadata_api = @import("../metadata/catalog_mutation_stamp.zig");
const runtime_status = @import("runtime_status.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const runtime_error_abi = @import("antfly_runtime_abi").error_abi;
const runtime_native_abi = @import("antfly_runtime_abi").native_abi;
const runnable_abi = @import("antfly_runtime_abi").runnable_abi;

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
    repair_paused: u64 = 0,
    repair_busy: u64 = 0,
    repair_disk_waits: u64 = 0,
    next_retry_at_ms: u64 = 0,
    restore_repair_attempted: u64 = 0,
    restore_repair_progressed: u64 = 0,
    restore_repair_pending: u64 = 0,
};

/// One exact structural observation captured while the compiled owner lease
/// still pins its generation. The control plane publishes `runtime_status` under the
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
    /// Transaction-ID commits enforce owner-routed range guards atomically
    /// with primary writes, including read-only guard participants.
    supports_sql_range_guards: bool = false,

    pub const VTable = struct {
        activate_range_tracking: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, @import("operation.zig").RequestContext) anyerror!void = null,
        txn_preparation_status_group_local: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, distributed_txn.TxnStatusRequest, @import("operation.zig").RequestContext, ?runnable_abi.Borrowed) anyerror!?@import("prepared_transaction_protocol.zig").Observation = null,
        txn_status_group_local_with_request: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_txn.TxnStatusRequest,
            context: @import("operation.zig").RequestContext,
        ) anyerror!?db_mod.types.TxnStatus = null,
        vector_migration_group_local: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request_json: []const u8) anyerror!?[]u8 = null,

        /// Committed replication has distinct transaction and entry-identity
        /// semantics from an ordinary request batch. Prepared application may
        /// only borrow an already configured owner, never consult the catalog.
        replicated_batch_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
            metadata_prepared: bool,
            entry: ?db_mod.types.OrderedApplyReceipt,
        ) anyerror!?void = null,

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
        graph_metric_action: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            metric_name: []const u8,
            action: []const u8,
        ) anyerror!?db_mod.types.GraphMetricStatus = null,
        graph_metric_action_with_cancellation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            metric_name: []const u8,
            action: []const u8,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?db_mod.types.GraphMetricStatus = null,
        graph_metric_maintenance_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?[]u8 = null,
        resolver_operation_group_local: ?*const fn (
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
            contract: table_drop_contract.DropCleanupContract,
        ) anyerror!?void = null,
        backup_pin_control: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: @import("../storage/db/native_backup_seal_contract.zig").Request, control: backup_contract.BackupOperationControl) anyerror!?[]u8 = null,
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
        batch_group_local_with_pre_decision_context: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, db_mod.types.BatchRequest, distributed_txn.PreDecisionContext) anyerror!?void = null,
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
        preflight_grouped_source_batch_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) anyerror!?void = null,
        preflight_write_admission_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
        ) anyerror!?void = null,
        capture_hot_standby_seed_snapshot_group_local: ?*const fn (ptr: *anyopaque, group_id: u64, table_name: []const u8, token: []const u8, destination: []const u8) anyerror!?void = null,
        prepare_hot_standby_seed_snapshot_group_local: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
            deadline_ns: u64,
        ) anyerror!?void = null,
        prepare_hot_standby_seed_snapshot_group_local_with_lifetime: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            table_name: []const u8,
            deadline_ns: u64,
            lifetime: *const @import("antfly_runtime_abi").runnable_abi.Reentry,
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
        reconcile_table_group_local_observed: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            target_index_name: ?[]const u8,
            advance_index_repair: bool,
            repair_options: db_mod.types.ArtifactRepairRunOptions,
            retain_cold_owner: bool,
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
        /// Offers one exact consensus-committed index mutation to the
        /// process-lifetime activation owner. `index_json == null` is an
        /// authoritative drop. Sources without a persistent owner leave this
        /// capability null and use their synchronous create/drop callbacks.
        accept_committed_index_mutation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            index_json: ?[]const u8,
            stamp: metadata_api.CatalogMutationStamp,
        ) anyerror!?void = null,
        /// Borrowed invocation state crosses only the private same-process
        /// boundary. The caller retains the team and its allocator until the
        /// callback has joined every child. Never serialize this capability.
        commit_batch_with_execution: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
            team: *runnable_abi.Team,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        commit_transaction_with_id_with_execution: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
            team: *runnable_abi.Team,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        /// Same-process authority retained by the caller through every child
        /// join. Sources must preserve the pre-decision deadline/clock and
        /// cancellation, independently of post-commit visibility cancellation.
        commit_batch_with_control: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
            control: distributed_txn.CommitControl,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        commit_transaction_with_id_with_control: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
            control: distributed_txn.CommitControl,
        ) anyerror!?distributed_txn.CommitOutcome = null,
        /// Original completion authority, owned through the synchronous call.
        /// This context has no newly minted pre-decision clock/deadline.
        txn_resolve_group_local_with_execution: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            status: db_mod.types.TxnStatus,
            commit_version: u64,
            topology_epoch: u64,
            sync_level: db_mod.types.SyncLevel,
            context: distributed_txn.PreDecisionContext,
        ) anyerror!?void = null,
    };

    pub fn activateRangeTracking(self: TableWriteSource, alloc: std.mem.Allocator, table: []const u8, context: @import("operation.zig").RequestContext) !void {
        const callback = self.vtable.activate_range_tracking orelse return error.SqlRangeTrackingRequired;
        return BoundaryAbi.call("activate_range_tracking", self.boundary_dispatch, callback, .{ self.ptr, alloc, table, context });
    }
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

    pub fn graphMetricAction(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
    ) !?db_mod.types.GraphMetricStatus {
        const fn_ptr = self.vtable.graph_metric_action orelse return null;
        return try BoundaryAbi.call("graph_metric_action", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, index_name, metric_name, action });
    }

    pub fn graphMetricActionWithCancellation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
        cancellation: db_mod.types.CancellationToken,
    ) !?db_mod.types.GraphMetricStatus {
        if (cancellation.isCancelled()) return error.Canceled;
        const fn_ptr = self.vtable.graph_metric_action_with_cancellation orelse
            return try self.graphMetricAction(alloc, table_name, index_name, metric_name, action);
        return try BoundaryAbi.call("graph_metric_action_with_cancellation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, index_name, metric_name, action, cancellation });
    }

    pub fn graphMetricMaintenanceGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const fn_ptr = self.vtable.graph_metric_maintenance_group_local orelse return null;
        return try BoundaryAbi.call("graph_metric_maintenance_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn resolverOperationGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const fn_ptr = self.vtable.resolver_operation_group_local orelse return null;
        return try BoundaryAbi.call("resolver_operation_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn dropTable(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        contract: table_drop_contract.DropCleanupContract,
    ) !?void {
        const fn_ptr = self.vtable.drop_table orelse return null;
        return try BoundaryAbi.call("drop_table", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, contract });
    }

    pub fn backupPinControl(self: TableWriteSource, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: @import("../storage/db/native_backup_seal_contract.zig").Request, control: backup_contract.BackupOperationControl) !?[]u8 {
        try control.ensureActive();
        const callback = self.vtable.backup_pin_control orelse return error.UnsupportedOperation;
        return BoundaryAbi.call("backup_pin_control", self.boundary_dispatch, callback, .{ self.ptr, alloc, table_name, group_id, request, control });
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

    pub fn commitBatchWithExecution(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
        team: ?*runnable_abi.Team,
    ) !?distributed_txn.CommitOutcome {
        const execution = team orelse return self.commitBatchWithCancellation(alloc, tables, sync_level, cancellation);
        const callback = self.vtable.commit_batch_with_execution orelse return error.UnsupportedOperation;
        return BoundaryAbi.call("commit_batch_with_execution", self.boundary_dispatch, callback, .{ self.ptr, alloc, tables, sync_level, cancellation, execution });
    }

    pub fn commitBatchWithControl(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
        control: distributed_txn.CommitControl,
    ) !?distributed_txn.CommitOutcome {
        if (self.vtable.commit_batch_with_control) |callback|
            return BoundaryAbi.call("commit_batch_with_control", self.boundary_dispatch, callback, .{ self.ptr, alloc, tables, sync_level, cancellation, control });
        if (control.requiresAwareCallback()) return error.UnsupportedOperation;
        return self.commitBatchWithCancellation(alloc, tables, sync_level, cancellation);
    }

    pub fn supportsTransactionControl(self: TableWriteSource, control: distributed_txn.CommitControl) bool {
        return self.vtable.commit_transaction_with_id_with_control != null or
            (!control.requiresAwareCallback() and (self.vtable.commit_transaction_with_id_with_cancellation != null or self.vtable.commit_transaction_with_id != null));
    }

    pub fn commitTransactionWithIdAndControl(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
        control: distributed_txn.CommitControl,
    ) !?distributed_txn.CommitOutcome {
        if (self.vtable.commit_transaction_with_id_with_control) |callback|
            return BoundaryAbi.call("commit_transaction_with_id_with_control", self.boundary_dispatch, callback, .{ self.ptr, alloc, txn_id, begin_timestamp, tables, sync_level, cancellation, control });
        if (control.requiresAwareCallback()) return error.UnsupportedOperation;
        return self.commitTransactionWithIdAndCancellation(alloc, txn_id, begin_timestamp, tables, sync_level, cancellation);
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

    pub fn supportsTransactionExecution(self: TableWriteSource) bool {
        return self.vtable.commit_transaction_with_id_with_execution != null;
    }

    pub fn commitTransactionWithIdAndExecution(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
        cancellation: db_mod.types.CancellationToken,
        team: ?*runnable_abi.Team,
    ) !?distributed_txn.CommitOutcome {
        const execution = team orelse return self.commitTransactionWithIdAndCancellation(alloc, txn_id, begin_timestamp, tables, sync_level, cancellation);
        const callback = self.vtable.commit_transaction_with_id_with_execution orelse return error.UnsupportedOperation;
        return BoundaryAbi.call("commit_transaction_with_id_with_execution", self.boundary_dispatch, callback, .{ self.ptr, alloc, txn_id, begin_timestamp, tables, sync_level, cancellation, execution });
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

    pub fn replicatedBatchGroupLocal(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
        metadata_prepared: bool,
        entry: ?db_mod.types.OrderedApplyReceipt,
    ) !?void {
        const callback = self.vtable.replicated_batch_group_local orelse return null;
        return try BoundaryAbi.call("replicated_batch_group_local", self.boundary_dispatch, callback, .{
            self.ptr, alloc, group_id, table_name, req, metadata_prepared, entry,
        });
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

    pub fn batchGroupLocalWithPreDecisionContext(self: TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.BatchRequest, context: distributed_txn.PreDecisionContext) !?void {
        const callback = self.vtable.batch_group_local_with_pre_decision_context orelse {
            if (context.requiresAwareCallback()) return error.UnsupportedOperation;
            return self.batchGroupLocal(alloc, group_id, table_name, req);
        };
        return BoundaryAbi.call("batch_group_local_with_pre_decision_context", self.boundary_dispatch, callback, .{ self.ptr, alloc, group_id, table_name, req, context });
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
        const fn_ptr = self.vtable.txn_begin_group_local_with_pre_decision_context orelse {
            if (context.requiresAwareCallback()) return error.UnsupportedOperation;
            return try self.txnBeginGroupLocal(alloc, group_id, table_name, txn_id, begin_timestamp, topology_epoch, retain_terminal, participants);
        };
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
        if (req.range_guards.len != 0 and context.route_fence == null) return error.CatalogRouteFenceRequired;
        if (context.route_fence != null and self.vtable.txn_prepare_group_local_with_pre_decision_context == null) return error.CatalogRouteFenceUnsupported;
        const fn_ptr = self.vtable.txn_prepare_group_local_with_pre_decision_context orelse {
            if (context.requiresAwareCallback()) return error.UnsupportedOperation;
            return try self.txnPrepareGroupLocal(alloc, group_id, table_name, txn_id, topology_epoch, req);
        };
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
        try cancellation.check();
        const fn_ptr = self.vtable.txn_resolve_group_local_with_cancellation orelse {
            // A completion deadline/lifetime may be carried in this token.
            // An incapable callback cannot discard it at a physical handoff.
            if (cancellation.ptr != null or cancellation.check_fn != null or cancellation.is_cancelled_fn != null)
                return error.UnsupportedOperation;
            return try self.txnResolveGroupLocal(alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level);
        };
        return try BoundaryAbi.call("txn_resolve_group_local_with_cancellation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level, cancellation });
    }

    pub fn txnResolveGroupLocalWithExecution(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
        topology_epoch: u64,
        sync_level: db_mod.types.SyncLevel,
        context: distributed_txn.PreDecisionContext,
    ) !?void {
        if (context.execution_runnable) |borrow| try borrow.checkLifetime();
        try context.cancellation.check();
        const callback = self.vtable.txn_resolve_group_local_with_execution orelse return error.UnsupportedOperation;
        return BoundaryAbi.call("txn_resolve_group_local_with_execution", self.boundary_dispatch, callback, .{ self.ptr, alloc, group_id, table_name, txn_id, status, commit_version, topology_epoch, sync_level, context });
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

    pub fn txnPreparationStatusGroupLocal(self: TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table: []const u8, req: distributed_txn.TxnStatusRequest, context: @import("operation.zig").RequestContext, runnable: ?runnable_abi.Borrowed) !?@import("prepared_transaction_protocol.zig").Observation {
        const callback = self.vtable.txn_preparation_status_group_local orelse return error.PreparedTransactionProtocolUnavailable;
        return BoundaryAbi.call("txn_preparation_status_group_local", self.boundary_dispatch, callback, .{ self.ptr, alloc, group_id, table, req, context, runnable });
    }

    pub fn txnStatusGroupLocalWithRequest(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_txn.TxnStatusRequest,
        context: @import("operation.zig").RequestContext,
    ) !?db_mod.types.TxnStatus {
        try context.ensureActive();
        if (req.restore_staging_scope == null or req.restore_staging_plan_id == null) return error.InvalidTxnRequest;
        const callback = self.vtable.txn_status_group_local_with_request orelse return error.DeadlineAwareTxnStatusUnsupported;
        return try BoundaryAbi.call("txn_status_group_local_with_request", self.boundary_dispatch, callback, .{ self.ptr, alloc, group_id, table_name, req, context });
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

    pub fn vectorMigrationGroupLocal(self: TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request_json: []const u8) !?[]u8 {
        const callback = self.vtable.vector_migration_group_local orelse return null;
        return try BoundaryAbi.call("vector_migration_group_local", self.boundary_dispatch, callback, .{ self.ptr, alloc, group_id, table_name, request_json });
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

    pub fn preflightGroupedSourceBatchGroupLocal(self: TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.BatchRequest) !?void {
        const fn_ptr = self.vtable.preflight_grouped_source_batch_group_local orelse return null;
        return try BoundaryAbi.call("preflight_grouped_source_batch_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req });
    }

    pub fn preflightWriteAdmissionGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const fn_ptr = self.vtable.preflight_write_admission_group_local orelse return null;
        return try BoundaryAbi.call("preflight_write_admission_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name });
    }

    pub fn captureHotStandbySeedSnapshotGroupLocal(self: TableWriteSource, group_id: u64, table_name: []const u8, token: []const u8, destination: []const u8) !?void {
        const fn_ptr = self.vtable.capture_hot_standby_seed_snapshot_group_local orelse return null;
        return try BoundaryAbi.call("capture_hot_standby_seed_snapshot_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, token, destination });
    }

    pub fn prepareHotStandbySeedSnapshotGroupLocal(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: u64,
    ) !?void {
        const fn_ptr = self.vtable.prepare_hot_standby_seed_snapshot_group_local orelse return null;
        return try BoundaryAbi.call("prepare_hot_standby_seed_snapshot_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, deadline_ns });
    }

    pub fn prepareHotStandbySeedSnapshotGroupLocalWithLifetime(
        self: TableWriteSource,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: u64,
        lifetime: *const @import("antfly_runtime_abi").runnable_abi.Reentry,
    ) !?void {
        try lifetime.checkpoint();
        const fn_ptr = self.vtable.prepare_hot_standby_seed_snapshot_group_local_with_lifetime orelse return error.ExecutionWaitUnsupported;
        const result = try BoundaryAbi.call("prepare_hot_standby_seed_snapshot_group_local_with_lifetime", self.boundary_dispatch, fn_ptr, .{ self.ptr, group_id, table_name, deadline_ns, lifetime });
        try lifetime.checkpoint();
        return result;
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

    pub fn reconcileTableGroupLocalObserved(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
        repair_options: db_mod.types.ArtifactRepairRunOptions,
        retain_cold_owner: bool,
    ) !?LocalStructuralReconcileObservation {
        const fn_ptr = self.vtable.reconcile_table_group_local_observed orelse return null;
        return try BoundaryAbi.call("reconcile_table_group_local_observed", self.boundary_dispatch, fn_ptr, .{
            self.ptr,
            alloc,
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
            repair_options,
            retain_cold_owner,
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

    pub fn acceptCommittedIndexMutation(
        self: TableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: ?[]const u8,
        stamp: metadata_api.CatalogMutationStamp,
    ) !?void {
        const fn_ptr = self.vtable.accept_committed_index_mutation orelse return null;
        return try BoundaryAbi.call("accept_committed_index_mutation", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, index_name, index_json, stamp });
    }
};

// Shared control tests belong to the consumer root, even when the physical
// implementation imports these contracts to implement its own operations.
pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const test_owner_root = @import("antfly_source_root");
    if (@hasDecl(test_owner_root, "implementation_tests_only") and test_owner_root.implementation_tests_only) return struct {};
    const Suite = struct {
        test "compiled table write boundary deadline clamps preserve clock epochs and caller fences" {
            const Clock = struct {
                current: i96,
                advance_other: ?*@This() = null,
                fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    if (self.advance_other) |other| other.current += 25;
                    return .{ .nanoseconds = self.current };
                }
                fn checkpoint(_: *anyopaque) callconv(.c) runtime_error_abi.Status {
                    return runtime_error_abi.statusFromError(error.InvalidRunnableTransition);
                }
                fn park(_: *anyopaque, _: c_int) callconv(.c) runtime_error_abi.Status {
                    return runtime_error_abi.statusFromError(error.InvalidRunnableTransition);
                }
                const vtable = blk: {
                    var value = std.Io.failing.vtable.*;
                    value.now = now;
                    break :blk value;
                };
                fn borrow(self: *@This()) @import("antfly_runtime_abi").io_abi.Borrow {
                    const io: std.Io = .{ .userdata = self, .vtable = &vtable };
                    return @import("antfly_runtime_abi").io_abi.Borrow.init(&io);
                }
            };
            var target: Clock = .{ .current = 10_000 };
            var origin: Clock = .{ .current = 100, .advance_other = &target };
            var canceled = std.atomic.Value(bool).init(false);
            const original: distributed_txn.PreDecisionContext = .{
                .route_fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat(1), .catalog_revision = 2, .table_id = 7, .topology_epoch = 3, .route = .{ .group_id = 9, .range_id = 10, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 10 } } },
                .execution_runnable = .{
                    .context = &origin,
                    .cpu_slots = 1,
                    .parent_class = @backingInt(runnable_abi.Class.write),
                    .checkpoint = Clock.checkpoint,
                    .park = Clock.park,
                    .enter = Clock.park,
                },
                .deadline_ns = 180,
                .deadline_io = origin.borrow(),
                .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled),
                .restore_staging_scope = @splat(3),
                .restore_staging_plan_id = @splat(4),
            };
            const translated = original.boundDeadline(20_000, target.borrow());
            // A source-clock read consumes 25 destination nanoseconds. That
            // latency cannot be added to the remaining 80-nanosecond budget.
            try std.testing.expectEqual(@as(?u64, 10_080), translated.deadline_ns);
            try std.testing.expectEqual(@as(i96, 55), @as(i96, @intCast(translated.deadline_ns.?)) - target.current);
            try std.testing.expectEqualDeep(original.execution_runnable, translated.execution_runnable);
            try std.testing.expectEqualDeep(original.route_fence, translated.route_fence);
            try std.testing.expectEqualDeep(original.cancellation, translated.cancellation);
            try std.testing.expectEqualDeep(original.restore_staging_scope, translated.restore_staging_scope);
            try std.testing.expectEqualDeep(original.restore_staging_plan_id, translated.restore_staging_plan_id);
            try std.testing.expect(translated.deadline_io.?.userdata == target.borrow().userdata);
            const attempt = translated.boundDeadline(10_050, target.borrow());
            const next = attempt.boundDeadline(30_000, target.borrow());
            try std.testing.expectEqual(attempt.deadline_ns, next.deadline_ns);
            canceled.store(true, .seq_cst);
            try std.testing.expectError(error.Canceled, next.cancellation.check());
            origin.current = 200;
            const expired = original.boundDeadline(20_000, target.borrow());
            try std.testing.expect(expired.deadline_ns.? <= @as(u64, @intCast(target.current)));
            const empty: distributed_txn.PreDecisionContext = .{};
            try std.testing.expectEqual(@as(?u64, 42), empty.boundDeadline(42, null).deadline_ns);
        }

        test "compiled table write boundary never discards pre-decision fences in legacy fallback" {
            const Fake = struct {
                legacy_calls: usize = 0,
                aware_calls: usize = 0,
                expected: distributed_txn.PreDecisionContext = .{},
                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }
                fn begin(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.legacy_calls += 1;
                    return {};
                }
                fn prepare(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.legacy_calls += 1;
                    return {};
                }
                fn beginAware(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, context: distributed_txn.PreDecisionContext) anyerror!?void {
                    return aware(raw, context);
                }
                fn prepareAware(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest, context: distributed_txn.PreDecisionContext) anyerror!?void {
                    return aware(raw, context);
                }
                fn aware(raw: *anyopaque, context: distributed_txn.PreDecisionContext) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.aware_calls += 1;
                    try std.testing.expectEqualDeep(self.expected, context);
                    try context.cancellation.check();
                    return {};
                }
                fn fence(_: *const anyopaque) anyerror!void {
                    return error.DeadlineExceeded;
                }
                fn checkpoint(_: *anyopaque) callconv(.c) runtime_error_abi.Status {
                    return runtime_error_abi.statusFromError(error.InvalidRunnableTransition);
                }
                fn park(_: *anyopaque, _: c_int) callconv(.c) runtime_error_abi.Status {
                    return runtime_error_abi.statusFromError(error.InvalidRunnableTransition);
                }
                fn foreignDispatch(contract: *const runtime_native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime_error_abi.Status {
                    return TableWriteSource.BoundaryAbi.local_dispatch(contract, callback, args, output);
                }
            };
            var fake: Fake = .{};
            var canceled = std.atomic.Value(bool).init(false);
            const io = std.testing.io;
            const clock = runtime_native_abi.IoBorrow.init(&io);
            const runnable: runnable_abi.Borrowed = .{
                .context = &fake,
                .cpu_slots = 1,
                .parent_class = @backingInt(runnable_abi.Class.write),
                .checkpoint = Fake.checkpoint,
                .park = Fake.park,
                .enter = Fake.park,
            };
            const contexts = [_]distributed_txn.PreDecisionContext{
                .{ .execution_runnable = runnable },
                .{ .deadline_ns = 123 },
                .{ .deadline_io = clock },
                .{ .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled) },
                .{ .cancellation = .{ .ptr = &canceled } },
                .{ .cancellation = .{ .check_fn = Fake.fence } },
                .{ .cancellation = .{ .ptr = &canceled, .check_fn = Fake.fence } },
                .{ .restore_staging_scope = @splat(1) },
                .{ .restore_staging_plan_id = @splat(2) },
            };
            const id = std.mem.zeroes(db_mod.types.TxnId);
            inline for (.{ TableWriteSource.BoundaryAbi.local_dispatch, Fake.foreignDispatch }) |dispatch| {
                var source: TableWriteSource = .{ .ptr = &fake, .boundary_dispatch = dispatch, .vtable = &.{ .batch = Fake.batch, .txn_begin_group_local = Fake.begin, .txn_prepare_group_local = Fake.prepare } };
                const before = fake.legacy_calls;
                for (contexts) |context| {
                    try std.testing.expectError(error.UnsupportedOperation, source.txnBeginGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 1, 2, false, &.{}, context));
                    try std.testing.expectError(error.UnsupportedOperation, source.txnPrepareGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 2, .{}, context));
                }
                try std.testing.expectEqual(before, fake.legacy_calls);
                _ = try source.txnBeginGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 1, 2, false, &.{}, .{});
                _ = try source.txnPrepareGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 2, .{}, .{});
                try std.testing.expectEqual(before + 2, fake.legacy_calls);
                source.vtable = &.{ .batch = Fake.batch, .txn_begin_group_local = Fake.begin, .txn_prepare_group_local = Fake.prepare, .txn_begin_group_local_with_pre_decision_context = Fake.beginAware, .txn_prepare_group_local_with_pre_decision_context = Fake.prepareAware };
                for (contexts) |context| {
                    fake.expected = context;
                    if (context.cancellation.ptr != null and context.cancellation.check_fn != null) {
                        try std.testing.expectError(error.DeadlineExceeded, source.txnBeginGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 1, 2, false, &.{}, context));
                        try std.testing.expectError(error.DeadlineExceeded, source.txnPrepareGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 2, .{}, context));
                    } else {
                        _ = try source.txnBeginGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 1, 2, false, &.{}, context);
                        _ = try source.txnPrepareGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 2, .{}, context);
                    }
                }
                canceled.store(true, .release);
                fake.expected = .{ .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled) };
                try std.testing.expectError(error.Canceled, source.txnBeginGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 1, 2, false, &.{}, fake.expected));
                try std.testing.expectError(error.Canceled, source.txnPrepareGroupLocalWithPreDecisionContext(std.testing.allocator, 7, "docs", id, 2, .{}, fake.expected));
                canceled.store(false, .release);
            }
            try std.testing.expectEqual(@as(usize, 40), fake.aware_calls);
        }

        test "compiled table write boundary preserves local resolve completion cancellation" {
            const Fake = struct {
                expected: db_mod.types.CancellationToken = .none,
                legacy_calls: usize = 0,
                aware_calls: usize = 0,
                stopped: bool = false,
                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }
                fn legacy(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64, _: u64, _: db_mod.types.SyncLevel) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.legacy_calls += 1;
                    return {};
                }
                fn aware(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64, _: u64, _: db_mod.types.SyncLevel, token: db_mod.types.CancellationToken) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.aware_calls += 1;
                    try std.testing.expectEqual(self.expected.ptr, token.ptr);
                    try std.testing.expectEqual(self.expected.check_fn, token.check_fn);
                    try std.testing.expectEqual(self.expected.is_cancelled_fn, token.is_cancelled_fn);
                    self.stopped = true;
                    try token.check();
                    return {};
                }
                fn lifetime(raw: *anyopaque) callconv(.c) runtime_error_abi.Status {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    return if (self.stopped) runtime_error_abi.statusFromError(error.DeadlineExceeded) else .ok;
                }
                fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime_error_abi.Status {
                    @panic("callback handoff must not create another CPU grant");
                }
                fn controlled(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64, _: u64, _: db_mod.types.SyncLevel, context: distributed_txn.PreDecisionContext) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expectEqual(raw, context.execution_runnable.?.context);
                    try std.testing.expect(context.deadline_ns == null and context.deadline_io == null);
                    try std.testing.expectEqualDeep(self.expected, context.cancellation);
                    try context.execution_runnable.?.checkLifetime();
                    try context.cancellation.check();
                    self.aware_calls += 1;
                    return {};
                }
                fn foreignDispatch(contract: *const runtime_native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime_error_abi.Status {
                    return TableWriteSource.BoundaryAbi.local_dispatch(contract, callback, args, output);
                }
                fn check(raw: *const anyopaque) !void {
                    const self: *const @This() = @ptrCast(@alignCast(raw));
                    if (self.stopped) return error.DeadlineExceeded;
                }
            };
            inline for (.{ TableWriteSource.BoundaryAbi.local_dispatch, Fake.foreignDispatch }) |dispatch| {
                var fake: Fake = .{};
                const id: db_mod.types.TxnId = @splat(1);
                var source: TableWriteSource = .{ .boundary_dispatch = dispatch, .ptr = &fake, .vtable = &.{ .batch = Fake.batch, .txn_resolve_group_local = Fake.legacy } };
                fake.expected = .{ .ptr = &fake, .check_fn = Fake.check };
                try std.testing.expectError(error.UnsupportedOperation, source.txnResolveGroupLocalWithCancellation(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, fake.expected));
                try std.testing.expectEqual(@as(usize, 0), fake.legacy_calls);
                _ = try source.txnResolveGroupLocalWithCancellation(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, .none);
                try std.testing.expectEqual(@as(usize, 1), fake.legacy_calls);
                source.vtable = &.{ .batch = Fake.batch, .txn_resolve_group_local = Fake.legacy, .txn_resolve_group_local_with_cancellation = Fake.aware };
                try std.testing.expectError(error.DeadlineExceeded, source.txnResolveGroupLocalWithCancellation(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, fake.expected));
                try std.testing.expectEqual(@as(usize, 1), fake.aware_calls);
                // Expired original completion authority cannot invoke a second RPC.
                try std.testing.expectError(error.DeadlineExceeded, source.txnResolveGroupLocalWithCancellation(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, fake.expected));
                try std.testing.expectEqual(@as(usize, 1), fake.aware_calls);
                var canceled = std.atomic.Value(bool).init(true);
                try std.testing.expectError(error.Canceled, source.txnResolveGroupLocalWithCancellation(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, db_mod.types.CancellationToken.fromAtomic(&canceled)));
                try std.testing.expectEqual(@as(usize, 1), fake.aware_calls);
                try std.testing.expectEqual(@as(usize, 1), fake.legacy_calls);
                fake.stopped = false;
                const context: distributed_txn.PreDecisionContext = .{
                    .execution_runnable = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .checkpoint = Fake.lifetime, .check_lifetime = Fake.lifetime, .park = Fake.transition, .enter = Fake.transition },
                    .cancellation = fake.expected,
                };
                try std.testing.expectError(error.UnsupportedOperation, source.txnResolveGroupLocalWithExecution(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, context));
                try std.testing.expectEqual(@as(usize, 1), fake.aware_calls);
                source.vtable = &.{ .batch = Fake.batch, .txn_resolve_group_local_with_execution = Fake.controlled };
                _ = try source.txnResolveGroupLocalWithExecution(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, context);
                fake.stopped = true;
                try std.testing.expectError(error.DeadlineExceeded, source.txnResolveGroupLocalWithExecution(std.testing.allocator, 7, "docs", id, .committed, 3, 4, .write, context));
                try std.testing.expectEqual(@as(usize, 2), fake.aware_calls);
            }
        }

        test "compiled table write boundary forwards original commit controls and rejects incapable fallback" {
            const Fake = struct {
                expected: distributed_txn.CommitControl = .{},
                post_commit: db_mod.types.CancellationToken = .none,
                legacy_calls: usize = 0,
                controlled_calls: usize = 0,
                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }
                fn legacy(raw: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, _: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.legacy_calls += 1;
                    return .{ .committed = .{ .participant_count = 0 } };
                }
                fn legacyId(raw: *anyopaque, alloc: std.mem.Allocator, _: db_mod.types.TxnId, _: u64, tables: []const distributed_txn.TableCommitRequest, sync: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
                    return legacy(raw, alloc, tables, sync, cancellation);
                }
                fn controlled(raw: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, post_commit: db_mod.types.CancellationToken, control: distributed_txn.CommitControl) anyerror!?distributed_txn.CommitOutcome {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.controlled_calls += 1;
                    try std.testing.expectEqualDeep(self.expected, control);
                    try std.testing.expectEqualDeep(self.post_commit, post_commit);
                    try control.pre_decision.cancellation.check();
                    return .{ .committed = .{ .participant_count = 1 } };
                }
                fn controlledId(raw: *anyopaque, alloc: std.mem.Allocator, id: db_mod.types.TxnId, timestamp: u64, tables: []const distributed_txn.TableCommitRequest, sync: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken, control: distributed_txn.CommitControl) anyerror!?distributed_txn.CommitOutcome {
                    try std.testing.expectEqual(@as(u64, 19), timestamp);
                    try std.testing.expectEqual(std.mem.zeroes(db_mod.types.TxnId), id);
                    return controlled(raw, alloc, tables, sync, cancellation, control);
                }
                fn check(_: *anyopaque) callconv(.c) runtime_error_abi.Status {
                    return .ok;
                }
                fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime_error_abi.Status {
                    return .ok;
                }
                fn rejected(_: *const anyopaque) anyerror!void {
                    return error.DeadlineExceeded;
                }
                fn dispatch(contract: *const runtime_native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime_error_abi.Status {
                    return TableWriteSource.BoundaryAbi.local_dispatch(contract, callback, args, output);
                }
            };
            var fake: Fake = .{};
            var canceled = std.atomic.Value(bool).init(false);
            var post_canceled = std.atomic.Value(bool).init(true);
            const token = db_mod.types.CancellationToken.fromAtomic(&canceled);
            fake.post_commit = db_mod.types.CancellationToken.fromAtomic(&post_canceled);
            var team: runnable_abi.Team = .{ .borrow = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .checkpoint = Fake.check, .park = Fake.phase, .enter = Fake.phase }, .maximum_slots = 1, .allocator = std.testing.allocator };
            const io = std.testing.io;
            const clock = @import("antfly_runtime_abi").io_abi.Borrow.init(&io);
            const controls = [_]distributed_txn.CommitControl{
                .{ .team = &team },                                                                                                                       .{ .pre_decision = .{ .deadline_ns = 123 } },
                .{ .pre_decision = .{ .deadline_io = clock } },                                                                                           .{ .pre_decision = .{ .cancellation = token } },
                .{ .pre_decision = .{ .cancellation = .{ .ptr = &canceled } } },                                                                          .{ .pre_decision = .{ .restore_staging_scope = @splat(1), .restore_staging_plan_id = @splat(2) } },
                distributed_txn.CommitControl.fromRequest(.{ .execution_team = &team, .deadline_ns = 456, .deadline_io = clock, .cancellation = token }),
            };
            try std.testing.expectEqual(@as(?u64, 456), controls[6].pre_decision.deadline_ns);
            try std.testing.expectEqualDeep(clock, controls[6].pre_decision.deadline_io.?);
            try std.testing.expect(controls[6].team.? == &team);
            const id = std.mem.zeroes(db_mod.types.TxnId);
            inline for (.{ TableWriteSource.BoundaryAbi.local_dispatch, Fake.dispatch }) |dispatch| {
                var source: TableWriteSource = .{ .ptr = &fake, .boundary_dispatch = dispatch, .vtable = &.{ .batch = Fake.batch, .commit_batch_with_cancellation = Fake.legacy, .commit_transaction_with_id_with_cancellation = Fake.legacyId } };
                const before = fake.legacy_calls;
                for (controls) |control| {
                    try std.testing.expect(!source.supportsTransactionControl(control));
                    try std.testing.expectError(error.UnsupportedOperation, source.commitBatchWithControl(std.testing.allocator, &.{}, .write, fake.post_commit, control));
                    try std.testing.expectError(error.UnsupportedOperation, source.commitTransactionWithIdAndControl(std.testing.allocator, id, 19, &.{}, .write, fake.post_commit, control));
                }
                try std.testing.expectEqual(before, fake.legacy_calls);
                try std.testing.expect(source.supportsTransactionControl(.{}));
                _ = try source.commitBatchWithControl(std.testing.allocator, &.{}, .write, fake.post_commit, .{});
                _ = try source.commitTransactionWithIdAndControl(std.testing.allocator, id, 19, &.{}, .write, fake.post_commit, .{});
                source.vtable = &.{ .batch = Fake.batch, .commit_batch_with_control = Fake.controlled, .commit_transaction_with_id_with_control = Fake.controlledId };
                for (controls) |control| {
                    fake.expected = control;
                    try std.testing.expect(source.supportsTransactionControl(control));
                    _ = try source.commitBatchWithControl(std.testing.allocator, &.{}, .write, fake.post_commit, control);
                    _ = try source.commitTransactionWithIdAndControl(std.testing.allocator, id, 19, &.{}, .write, fake.post_commit, control);
                }
                fake.expected = controls[6];
                canceled.store(true, .seq_cst);
                try std.testing.expectError(error.Canceled, source.commitBatchWithControl(std.testing.allocator, &.{}, .write, fake.post_commit, fake.expected));
                try std.testing.expectError(error.Canceled, source.commitTransactionWithIdAndControl(std.testing.allocator, id, 19, &.{}, .write, fake.post_commit, fake.expected));
                canceled.store(false, .seq_cst);
                fake.expected.pre_decision.cancellation = .{ .ptr = &canceled, .check_fn = Fake.rejected };
                try std.testing.expectError(error.DeadlineExceeded, source.commitBatchWithControl(std.testing.allocator, &.{}, .write, fake.post_commit, fake.expected));
                try std.testing.expectError(error.DeadlineExceeded, source.commitTransactionWithIdAndControl(std.testing.allocator, id, 19, &.{}, .write, fake.post_commit, fake.expected));
            }
            try std.testing.expectEqual(@as(usize, 4), fake.legacy_calls);
            try std.testing.expectEqual(@as(usize, 36), fake.controlled_calls);
        }

        test "compiled table write boundary forwards retained execution teams without legacy fallback" {
            const Fake = struct {
                expected: ?*runnable_abi.Team = null,
                calls: usize = 0,
                legacy_calls: usize = 0,
                resizes: usize = 0,
                failure: ?anyerror = null,
                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }
                fn legacy(raw: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.legacy_calls += 1;
                    try cancellation.check();
                    return .{ .committed = .{ .participant_count = 0 } };
                }
                fn legacyId(raw: *anyopaque, alloc: std.mem.Allocator, _: db_mod.types.TxnId, _: u64, tables: []const distributed_txn.TableCommitRequest, sync_level: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken) anyerror!?distributed_txn.CommitOutcome {
                    return legacy(raw, alloc, tables, sync_level, cancellation);
                }
                fn check(_: *anyopaque) callconv(.c) runtime_error_abi.Status {
                    return runtime_error_abi.statusFromError(error.UnsupportedOperation);
                }
                fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime_error_abi.Status {
                    return runtime_error_abi.statusFromError(error.UnsupportedOperation);
                }
                fn resize(raw: *anyopaque, slots: usize) callconv(.c) runtime_error_abi.Status {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (slots != 3) return runtime_error_abi.statusFromError(error.InvalidRunnableDemand);
                    self.resizes += 1;
                    return .{};
                }
                fn commit(raw: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken, team: *runnable_abi.Team) anyerror!?distributed_txn.CommitOutcome {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expect(self.expected.? == team);
                    self.calls += 1;
                    try cancellation.check();
                    try team.reserve(8);
                    defer team.finish();
                    try std.testing.expectEqual(@as(usize, 2), team.childCapacity());
                    // The boundary forwards the bounded allocator itself,
                    // rather than replacing it with an unbounded fallback.
                    try std.testing.expectError(error.OutOfMemory, team.allocator.alloc(u8, 1));
                    if (self.failure) |err| return err;
                    return .{ .committed = .{ .participant_count = 1 } };
                }
                fn commitId(raw: *anyopaque, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, timestamp: u64, tables: []const distributed_txn.TableCommitRequest, sync_level: db_mod.types.SyncLevel, cancellation: db_mod.types.CancellationToken, team: *runnable_abi.Team) anyerror!?distributed_txn.CommitOutcome {
                    try std.testing.expectEqual(std.mem.zeroes(db_mod.types.TxnId), txn_id);
                    try std.testing.expectEqual(@as(u64, 19), timestamp);
                    return commit(raw, alloc, tables, sync_level, cancellation, team);
                }
                fn foreignDispatch(contract: *const runtime_native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime_error_abi.Status {
                    return TableWriteSource.BoundaryAbi.local_dispatch(contract, callback, args, output);
                }
            };
            var fake = Fake{};
            var storage: [0]u8 = .{};
            var bounded = std.heap.FixedBufferAllocator.init(&storage);
            var team: runnable_abi.Team = .{
                .borrow = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .checkpoint = Fake.check, .park = Fake.phase, .enter = Fake.phase, .resize_team = Fake.resize },
                .maximum_slots = 3,
                .allocator = bounded.allocator(),
            };
            fake.expected = &team;
            const txn_id = std.mem.zeroes(db_mod.types.TxnId);
            var canceled = std.atomic.Value(bool).init(false);
            const cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled);
            inline for (.{ TableWriteSource.BoundaryAbi.local_dispatch, Fake.foreignDispatch }) |dispatch| {
                const source: TableWriteSource = .{
                    .ptr = &fake,
                    .boundary_dispatch = dispatch,
                    .vtable = &.{ .batch = Fake.batch, .commit_batch_with_cancellation = Fake.legacy, .commit_transaction_with_id_with_cancellation = Fake.legacyId, .commit_batch_with_execution = Fake.commit, .commit_transaction_with_id_with_execution = Fake.commitId },
                };
                try std.testing.expect(source.supportsTransactionExecution());
                const outcome = (try source.commitBatchWithExecution(std.testing.allocator, &.{}, .write, cancellation, &team)).?;
                try std.testing.expect(outcome == .committed);
                try std.testing.expect(team.state == .joined);
                _ = try source.commitTransactionWithIdAndExecution(std.testing.allocator, txn_id, 19, &.{}, .write, cancellation, &team);
                inline for (.{ error.SchedulerFull, error.RunnableWaitTimeout, error.CommitDecisionUnknown, error.RaftBatchWriteOutcomeUnknown }) |failure| {
                    fake.failure = failure;
                    try std.testing.expectError(failure, source.commitBatchWithExecution(std.testing.allocator, &.{}, .write, cancellation, &team));
                    try std.testing.expectError(failure, source.commitTransactionWithIdAndExecution(std.testing.allocator, txn_id, 19, &.{}, .write, cancellation, &team));
                    try std.testing.expect(team.state == .joined);
                }
                fake.failure = null;
                canceled.store(true, .release);
                const resizes = fake.resizes;
                try std.testing.expectError(error.Canceled, source.commitBatchWithExecution(std.testing.allocator, &.{}, .write, cancellation, &team));
                try std.testing.expectEqual(resizes, fake.resizes);
                canceled.store(false, .release);
                try std.testing.expectEqual(@as(usize, 0), fake.legacy_calls);
                _ = try source.commitBatchWithExecution(std.testing.allocator, &.{}, .write, cancellation, null);
                _ = try source.commitTransactionWithIdAndExecution(std.testing.allocator, txn_id, 19, &.{}, .write, cancellation, null);
                try std.testing.expectEqual(@as(usize, 2), fake.legacy_calls);
                fake.legacy_calls = 0;
                const unsupported: TableWriteSource = .{
                    .ptr = &fake,
                    .boundary_dispatch = dispatch,
                    .vtable = &.{ .batch = Fake.batch, .commit_batch_with_cancellation = Fake.legacy, .commit_transaction_with_id_with_cancellation = Fake.legacyId },
                };
                try std.testing.expect(!unsupported.supportsTransactionExecution());
                try std.testing.expectError(error.UnsupportedOperation, unsupported.commitBatchWithExecution(std.testing.allocator, &.{}, .write, cancellation, &team));
                try std.testing.expectError(error.UnsupportedOperation, unsupported.commitTransactionWithIdAndExecution(std.testing.allocator, txn_id, 19, &.{}, .write, cancellation, &team));
                try std.testing.expectEqual(@as(usize, 0), fake.legacy_calls);
            }
        }

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
            // Exercise the foreign dispatcher used by production archives:
            // admission failures retain their exact public classification,
            // while an ambiguous proposal must never become retryable.
            inline for (.{
                error.StorageBusy,
                error.CatalogRoutingSnapshotTimeout,
                error.CatalogRoutingUnavailable,
                error.CatalogProjectionRefreshRequired,
                error.RaftBatchWriteOutcomeUnknown,
                error.CommitDecisionUnknown,
            }) |failure| {
                fake.failure = failure;
                try std.testing.expectError(failure, source.commitBatchWithCancellation(
                    std.testing.allocator,
                    &.{},
                    .write,
                    .none,
                ));
            }
            fake.failure = error.EnrichmentWorkerFailed;
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
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
