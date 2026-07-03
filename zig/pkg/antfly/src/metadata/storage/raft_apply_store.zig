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
const raft_engine = @import("raft_engine");
const fs_paths = @import("../../common/fs_paths.zig");
const group_ids = @import("../../common/group_ids.zig");
const docstore = @import("../../storage/docstore.zig");
const lsm_backend = @import("../../storage/lsm_backend.zig");
const metadata = @import("../mod.zig");
const extension_domain = @import("../../extensions/mod.zig");
const metadata_table_manager = @import("../table_manager.zig");
const runtime_schema = @import("../../storage/schema.zig");
const raft_catalog = @import("../../raft/catalog.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const raft_storage_mod = @import("../../raft/storage/mod.zig");
const wal_replica_state_mod = @import("../../raft/storage/wal_replica_state.zig");
const raft_state_machine = @import("../../raft/state_machine/mod.zig");

pub const AppliedMetadataBatch = struct {
    commit_index: u64,
    entries_bytes: []const u8,
};

pub const ExtensionMemberKey = struct {
    extension_name: []const u8,
    object_kind: extension_domain.ExtensionObjectKind,
    object_name: []const u8,
};

pub const ExtensionDependencyKey = struct {
    extension_name: []const u8,
    required_extension_name: []const u8,
    package_name: []const u8,
};

pub const ExtensionLifecycleDelta = struct {
    upsert_tables: []const metadata.TableRecord = &.{},
    upsert_installed_extensions: []const extension_domain.InstalledExtension = &.{},
    remove_installed_extensions: []const []const u8 = &.{},
    upsert_extension_members: []const extension_domain.ExtensionMember = &.{},
    remove_extension_members: []const ExtensionMemberKey = &.{},
    upsert_extension_dependencies: []const extension_domain.ExtensionDependency = &.{},
    remove_extension_dependencies: []const ExtensionDependencyKey = &.{},
};

pub const TransitionCommand = union(enum) {
    upsert_node: metadata.NodeRecord,
    register_node: metadata.NodeRecord,
    remove_node: struct {
        node_id: u64,
    },
    request_node_shutdown: struct {
        node_id: u64,
    },
    cancel_node_shutdown: struct {
        node_id: u64,
    },
    finalize_node_shutdown: struct {
        node_id: u64,
    },
    upsert_store: metadata.StoreRecord,
    register_store: metadata.StoreRecord,
    remove_store: struct {
        store_id: u64,
    },
    upsert_replica_intent: raft_reconciler.PlacementIntent,
    remove_replica_intent: struct {
        group_id: u64,
        local_node_id: u64,
    },
    upsert_database: metadata.DatabaseRecord,
    remove_database: struct {
        database_id: u64,
    },
    upsert_namespace: metadata.NamespaceRecord,
    remove_namespace: struct {
        namespace_id: u64,
    },
    upsert_tablespace: metadata.TablespaceRecord,
    remove_tablespace: struct {
        tablespace_id: u64,
    },
    upsert_sequence: metadata.SequenceRecord,
    remove_sequence: struct {
        sequence_id: u64,
    },
    compare_and_swap_sequence: metadata_table_manager.SequenceCompareAndSwapRequest,
    upsert_table: metadata.TableRecord,
    remove_table: struct {
        table_id: u64,
    },
    upsert_schema_progress: metadata.SchemaProgressRecord,
    remove_schema_progress: struct {
        table_id: u64,
        node_id: u64,
    },
    upsert_restore_progress: metadata.RestoreProgressRecord,
    remove_restore_progress: struct {
        table_id: u64,
        node_id: u64,
        group_id: u64,
    },
    upsert_replication_source_status: metadata.ReplicationSourceStatusRecord,
    remove_replication_source_status: struct {
        table_id: u64,
        source_ordinal: u32,
    },
    upsert_range: metadata.RangeRecord,
    remove_range: struct {
        group_id: u64,
    },
    upsert_foreign_key_ref_range: metadata.ForeignKeyReferenceRangeRecord,
    remove_foreign_key_ref_range: struct {
        child_table_id: u64,
        constraint_name: []const u8,
        parent_table_id: u64,
        start_parent_key: []const u8,
    },
    begin_foreign_key_ref_range_split: metadata_table_manager.ForeignKeyReferenceRangeSplitRequest,
    finish_foreign_key_ref_range_split: metadata_table_manager.ForeignKeyReferenceRangeSplitRequest,
    begin_foreign_key_ref_range_merge: metadata_table_manager.ForeignKeyReferenceRangeMergeRequest,
    finish_foreign_key_ref_range_merge: metadata_table_manager.ForeignKeyReferenceRangeMergeRequest,
    begin_foreign_key_ref_range_rebuild: metadata_table_manager.ForeignKeyReferenceRangeSelector,
    finish_foreign_key_ref_range_rebuild: metadata_table_manager.ForeignKeyReferenceRangeSelector,
    upsert_unique_constraint_range: metadata.UniqueConstraintRangeRecord,
    remove_unique_constraint_range: struct {
        table_id: u64,
        constraint_name: []const u8,
        start_encoded_value: []const u8,
    },
    begin_unique_constraint_range_split: metadata_table_manager.UniqueConstraintRangeSplitRequest,
    finish_unique_constraint_range_split: metadata_table_manager.UniqueConstraintRangeSplitRequest,
    begin_unique_constraint_range_merge: metadata_table_manager.UniqueConstraintRangeMergeRequest,
    finish_unique_constraint_range_merge: metadata_table_manager.UniqueConstraintRangeMergeRequest,
    begin_unique_constraint_range_rebuild: metadata_table_manager.UniqueConstraintRangeSelector,
    finish_unique_constraint_range_rebuild: metadata_table_manager.UniqueConstraintRangeSelector,
    upsert_secondary_index_rebuild_range: metadata.SecondaryIndexRebuildRangeRecord,
    remove_secondary_index_rebuild_range: metadata_table_manager.SecondaryIndexRebuildRangeSelector,
    begin_secondary_index_rebuild_range: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest,
    finish_secondary_index_rebuild_range: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest,
    invalidate_secondary_index_rebuild_range: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest,
    upsert_schema_rewrite_job: metadata.SchemaRewriteJobRecord,
    apply_table_catalog_update_with_schema_rewrite_jobs: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
    apply_table_catalog_batch_update_with_schema_rewrite_jobs: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
    apply_table_catalog_drop_with_schema_rewrite_jobs: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest,
    remove_schema_rewrite_job: struct {
        job_id: u64,
    },
    begin_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobBeginRequest,
    finish_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobFinishRequest,
    invalidate_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobInvalidateRequest,
    pause_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobControlRequest,
    resume_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobControlRequest,
    retry_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobControlRequest,
    cancel_schema_rewrite_job: metadata_table_manager.SchemaRewriteJobControlRequest,
    upsert_table_emptying_job: metadata.TableEmptyingJobRecord,
    remove_table_emptying_job: struct {
        job_id: u64,
    },
    begin_table_emptying_job: metadata_table_manager.TableEmptyingJobBeginRequest,
    finish_table_emptying_job: metadata_table_manager.TableEmptyingJobFinishRequest,
    invalidate_table_emptying_job: metadata_table_manager.TableEmptyingJobInvalidateRequest,
    pause_table_emptying_job: metadata_table_manager.TableEmptyingJobControlRequest,
    resume_table_emptying_job: metadata_table_manager.TableEmptyingJobControlRequest,
    retry_table_emptying_job: metadata_table_manager.TableEmptyingJobControlRequest,
    cancel_table_emptying_job: metadata_table_manager.TableEmptyingJobControlRequest,
    promote_table_emptying_barrier: metadata_table_manager.TableEmptyingBarrierPromotionRequest,
    promote_secondary_index_ready: metadata_table_manager.SecondaryIndexReadyPromotionRequest,
    compare_and_swap_table_schema: metadata_table_manager.TableSchemaCompareAndSwapRequest,
    upsert_split_transition: metadata.SplitTransitionRecord,
    remove_split_transition: struct {
        transition_id: u64,
    },
    upsert_merge_transition: metadata.MergeTransitionRecord,
    remove_merge_transition: struct {
        transition_id: u64,
    },
    upsert_reconcile_lease: metadata.ReconcileLeaseRecord,
    remove_reconcile_lease: struct {},
    upsert_shuffle_join_lease: metadata.ShuffleJoinLeaseRecord,
    remove_shuffle_join_lease: struct {
        job_id: u64,
    },
    upsert_reallocation_request: metadata.ReallocationRequestRecord,
    remove_reallocation_request: struct {},
    upsert_extension_package: extension_domain.PackageManifest,
    remove_extension_package: struct {
        name: []const u8,
        version: []const u8,
    },
    upsert_installed_extension: extension_domain.InstalledExtension,
    remove_installed_extension: struct {
        name: []const u8,
    },
    upsert_extension_member: extension_domain.ExtensionMember,
    remove_extension_member: struct {
        extension_name: []const u8,
        object_kind: extension_domain.ExtensionObjectKind,
        object_name: []const u8,
    },
    upsert_extension_dependency: extension_domain.ExtensionDependency,
    remove_extension_dependency: struct {
        extension_name: []const u8,
        required_extension_name: []const u8,
        package_name: []const u8,
    },
    apply_extension_lifecycle: ExtensionLifecycleDelta,

    pub fn deinit(self: *TransitionCommand, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .upsert_node, .register_node => |*record| {
                metadata_table_manager.freeNode(alloc, record.*);
            },
            .upsert_store, .register_store => |*record| {
                metadata_table_manager.freeStore(alloc, record.*);
            },
            .upsert_replica_intent => |*intent| {
                var record = intent.record;
                record.deinit(alloc);
                if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            },
            .upsert_database => |*record| {
                metadata_table_manager.freeDatabase(alloc, record.*);
            },
            .upsert_namespace => |*record| {
                metadata_table_manager.freeNamespace(alloc, record.*);
            },
            .upsert_tablespace => |*record| {
                metadata_table_manager.freeTablespace(alloc, record.*);
            },
            .upsert_sequence => |*record| {
                metadata_table_manager.freeSequence(alloc, record.*);
            },
            .upsert_table => |*record| {
                metadata_table_manager.freeTable(alloc, record.*);
            },
            .upsert_restore_progress => |*record| {
                metadata_table_manager.freeRestoreProgress(alloc, record.*);
            },
            .upsert_replication_source_status => |*record| {
                metadata_table_manager.freeReplicationSourceStatus(alloc, record.*);
            },
            .upsert_range => |*record| {
                metadata_table_manager.freeRange(alloc, record.*);
            },
            .upsert_foreign_key_ref_range => |*record| {
                metadata_table_manager.freeForeignKeyReferenceRange(alloc, record.*);
            },
            .remove_foreign_key_ref_range => |*record| {
                alloc.free(record.constraint_name);
                alloc.free(record.start_parent_key);
            },
            .begin_foreign_key_ref_range_split, .finish_foreign_key_ref_range_split => |*request| {
                freeForeignKeyReferenceRangeSplitRequest(alloc, request.*);
            },
            .begin_foreign_key_ref_range_merge, .finish_foreign_key_ref_range_merge => |*request| {
                freeForeignKeyReferenceRangeMergeRequest(alloc, request.*);
            },
            .begin_foreign_key_ref_range_rebuild, .finish_foreign_key_ref_range_rebuild => |*selector| {
                freeForeignKeyReferenceRangeSelector(alloc, selector.*);
            },
            .upsert_unique_constraint_range => |*record| {
                metadata_table_manager.freeUniqueConstraintRange(alloc, record.*);
            },
            .remove_unique_constraint_range => |*record| {
                alloc.free(record.constraint_name);
                alloc.free(record.start_encoded_value);
            },
            .begin_unique_constraint_range_split, .finish_unique_constraint_range_split => |*request| {
                freeUniqueConstraintRangeSplitRequest(alloc, request.*);
            },
            .begin_unique_constraint_range_merge, .finish_unique_constraint_range_merge => |*request| {
                freeUniqueConstraintRangeMergeRequest(alloc, request.*);
            },
            .begin_unique_constraint_range_rebuild, .finish_unique_constraint_range_rebuild => |*selector| {
                freeUniqueConstraintRangeSelector(alloc, selector.*);
            },
            .upsert_secondary_index_rebuild_range => |*record| {
                metadata_table_manager.freeSecondaryIndexRebuildRange(alloc, record.*);
            },
            .upsert_schema_rewrite_job => |*record| {
                metadata_table_manager.freeSchemaRewriteJob(alloc, record.*);
            },
            .apply_table_catalog_update_with_schema_rewrite_jobs => |*request| {
                freeTableCatalogUpdateWithSchemaRewriteJobsRequest(alloc, request.*);
            },
            .apply_table_catalog_batch_update_with_schema_rewrite_jobs => |*request| {
                freeTableCatalogBatchUpdateWithSchemaRewriteJobsRequest(alloc, request.*);
            },
            .apply_table_catalog_drop_with_schema_rewrite_jobs => |*request| {
                freeTableCatalogDropWithSchemaRewriteJobsRequest(alloc, request.*);
            },
            .remove_secondary_index_rebuild_range => |*selector| {
                freeSecondaryIndexRebuildRangeSelector(alloc, selector.*);
            },
            .begin_secondary_index_rebuild_range => |*request| {
                freeSecondaryIndexRebuildRangeBeginRequest(alloc, request.*);
            },
            .finish_secondary_index_rebuild_range => |*request| {
                freeSecondaryIndexRebuildRangeFinishRequest(alloc, request.*);
            },
            .invalidate_secondary_index_rebuild_range => |*request| {
                freeSecondaryIndexRebuildRangeInvalidateRequest(alloc, request.*);
            },
            .begin_schema_rewrite_job => |*request| {
                freeSchemaRewriteJobBeginRequest(alloc, request.*);
            },
            .finish_schema_rewrite_job => |*request| {
                freeSchemaRewriteJobFinishRequest(alloc, request.*);
            },
            .invalidate_schema_rewrite_job => |*request| {
                freeSchemaRewriteJobInvalidateRequest(alloc, request.*);
            },
            .pause_schema_rewrite_job, .resume_schema_rewrite_job, .retry_schema_rewrite_job, .cancel_schema_rewrite_job => |*request| {
                freeSchemaRewriteJobControlRequest(alloc, request.*);
            },
            .upsert_table_emptying_job => |*record| {
                metadata_table_manager.freeTableEmptyingJob(alloc, record.*);
            },
            .begin_table_emptying_job => |*request| {
                freeTableEmptyingJobBeginRequest(alloc, request.*);
            },
            .finish_table_emptying_job => |*request| {
                freeTableEmptyingJobFinishRequest(alloc, request.*);
            },
            .invalidate_table_emptying_job => |*request| {
                freeTableEmptyingJobInvalidateRequest(alloc, request.*);
            },
            .pause_table_emptying_job, .resume_table_emptying_job, .retry_table_emptying_job, .cancel_table_emptying_job => |*request| {
                freeTableEmptyingJobControlRequest(alloc, request.*);
            },
            .promote_table_emptying_barrier => |*request| {
                freeTableEmptyingBarrierPromotionRequest(alloc, request.*);
            },
            .promote_secondary_index_ready => |*request| {
                freeSecondaryIndexReadyPromotionRequest(alloc, request.*);
            },
            .compare_and_swap_table_schema => |*request| {
                freeTableSchemaCompareAndSwapRequest(alloc, request.*);
            },
            .upsert_split_transition => |*record| {
                if (record.split_key) |split_key| alloc.free(split_key);
                if (record.source_range_end) |end| alloc.free(end);
                if (record.rollback_reason) |reason| alloc.free(reason);
            },
            .upsert_merge_transition => |*record| {
                if (record.rollback_reason) |reason| alloc.free(reason);
            },
            .upsert_extension_package => |*record| record.deinitOwned(alloc),
            .remove_extension_package => |record| {
                alloc.free(record.name);
                alloc.free(record.version);
            },
            .upsert_installed_extension => |*record| record.deinitOwned(alloc),
            .remove_installed_extension => |record| {
                alloc.free(record.name);
            },
            .upsert_extension_member => |*record| record.deinitOwned(alloc),
            .remove_extension_member => |record| {
                alloc.free(record.extension_name);
                alloc.free(record.object_name);
            },
            .upsert_extension_dependency => |*record| record.deinitOwned(alloc),
            .remove_extension_dependency => |record| {
                alloc.free(record.extension_name);
                alloc.free(record.required_extension_name);
                alloc.free(record.package_name);
            },
            .apply_extension_lifecycle => |*delta| freeExtensionLifecycleDelta(alloc, delta.*),
            else => {},
        }
        self.* = undefined;
    }
};

pub fn validateTransitionCommandDataGroupIds(command: TransitionCommand) !void {
    switch (command) {
        .upsert_replica_intent => |intent| try group_ids.requireDataGroupId(intent.record.group_id),
        .remove_replica_intent => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_restore_progress => |record| try group_ids.requireDataGroupId(record.group_id),
        .remove_restore_progress => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .remove_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_foreign_key_ref_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .begin_foreign_key_ref_range_split, .finish_foreign_key_ref_range_split => |request| {
            try group_ids.requireDataGroupId(request.left_group_id);
            try group_ids.requireDataGroupId(request.right_group_id);
        },
        .begin_foreign_key_ref_range_merge, .finish_foreign_key_ref_range_merge => |request| {
            try group_ids.requireDataGroupId(request.merged_group_id);
        },
        .upsert_unique_constraint_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .begin_unique_constraint_range_split, .finish_unique_constraint_range_split => |request| {
            try group_ids.requireDataGroupId(request.left_group_id);
            try group_ids.requireDataGroupId(request.right_group_id);
        },
        .begin_unique_constraint_range_merge, .finish_unique_constraint_range_merge => |request| {
            try group_ids.requireDataGroupId(request.merged_group_id);
        },
        .upsert_secondary_index_rebuild_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .apply_table_catalog_update_with_schema_rewrite_jobs => |request| {
            for (request.schema_rewrite_jobs) |record| try group_ids.requireDataGroupId(record.group_id);
        },
        .apply_table_catalog_batch_update_with_schema_rewrite_jobs => |request| {
            for (request.schema_rewrite_jobs) |record| try group_ids.requireDataGroupId(record.group_id);
        },
        .apply_table_catalog_drop_with_schema_rewrite_jobs => |request| {
            for (request.range_group_ids) |range_group_id| try group_ids.requireDataGroupId(range_group_id);
            for (request.schema_rewrite_jobs) |record| try group_ids.requireDataGroupId(record.group_id);
        },
        .upsert_table_emptying_job => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_split_transition => |record| {
            try group_ids.requireDataGroupId(record.source_group_id);
            try group_ids.requireDataGroupId(record.destination_group_id);
        },
        .upsert_merge_transition => |record| {
            try group_ids.requireDataGroupId(record.donor_group_id);
            try group_ids.requireDataGroupId(record.receiver_group_id);
        },
        .upsert_shuffle_join_lease => |record| try group_ids.requireDataGroupId(record.owner_group_id),
        else => {},
    }
}

test "transition command validation rejects metadata group ids in data group fields" {
    const metadata_group_id = group_ids.main_metadata_group_id;
    const commands = [_]TransitionCommand{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = metadata_group_id, .replica_id = 1, .local_node_id = 1 } } },
        .{ .remove_replica_intent = .{ .group_id = metadata_group_id, .local_node_id = 1 } },
        .{ .upsert_restore_progress = .{ .table_id = 1, .node_id = 1, .group_id = metadata_group_id, .backup_id = "backup" } },
        .{ .remove_restore_progress = .{ .table_id = 1, .node_id = 1, .group_id = metadata_group_id } },
        .{ .upsert_range = .{ .group_id = metadata_group_id, .table_id = 1, .start_key = "" } },
        .{ .remove_range = .{ .group_id = metadata_group_id } },
        .{ .upsert_foreign_key_ref_range = .{ .child_table_id = 1, .constraint_name = "orders_customer_id_fkey", .parent_table_id = 2, .start_parent_key = "", .group_id = metadata_group_id } },
        .{ .upsert_unique_constraint_range = .{ .table_id = 1, .constraint_name = "users_email_key", .start_encoded_value = "", .group_id = metadata_group_id } },
        .{ .upsert_secondary_index_rebuild_range = .{ .table_id = 1, .index_name = "users_email_idx", .index_generation = 1, .start_row_key = "", .group_id = metadata_group_id } },
        .{ .upsert_table_emptying_job = .{ .job_id = 1, .table_id = 1, .group_id = metadata_group_id, .schema_generation = 1, .affected_table_ids = &.{1} } },
        .{ .upsert_split_transition = .{ .transition_id = 1, .source_group_id = metadata_group_id, .destination_group_id = 2 } },
        .{ .upsert_split_transition = .{ .transition_id = 1, .source_group_id = 2, .destination_group_id = metadata_group_id } },
        .{ .upsert_merge_transition = .{ .transition_id = 1, .donor_group_id = metadata_group_id, .receiver_group_id = 2 } },
        .{ .upsert_merge_transition = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = metadata_group_id } },
        .{ .upsert_shuffle_join_lease = .{ .job_id = 1, .owner_group_id = metadata_group_id, .expires_at_ms = 1 } },
    };
    for (commands) |command| {
        try std.testing.expectError(error.ReservedGroupId, validateTransitionCommandDataGroupIds(command));
    }
}

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    map_size: usize = 16 * 1024 * 1024,
    no_sync: bool = false,
    read_only: bool = false,
    // Metadata apply traffic is many tiny durable WAL-backed writes. Flushing
    // every commit amplifies manifest churn and makes simulation/runtime costs
    // pathological without improving durability.
    flush_threshold: usize = 64,
};

pub const ProjectionSignalKind = enum {
    table,
    range,
    store,
    placement_intent,
    reconcile_lease,
    shuffle_join_lease,
    split_transition,
    merge_transition,
    schema_progress,
    restore_progress,
    replication_source_status,
    foreign_key_ref_range,
    unique_constraint_range,
    secondary_index_rebuild_range,
    schema_rewrite_job,
    table_emptying_job,
};

pub const ProjectionSignal = struct {
    kind: ProjectionSignalKind,
    metadata_group_id: u64,
    table_name: ?[]const u8 = null,
    table_id: u64 = 0,
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
};

pub const ProjectionListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        on_projection_signal: *const fn (ptr: *anyopaque, signal: ProjectionSignal) void,
    };

    pub fn onProjectionSignal(self: ProjectionListener, signal: ProjectionSignal) void {
        self.vtable.on_projection_signal(self.ptr, signal);
    }
};

pub const CommittedKeySignal = struct {
    metadata_group_id: u64,
    key: []const u8,
};

pub const CommittedKeyListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        matches_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) bool,
        on_committed_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) void,
    };

    pub fn onCommittedKey(self: CommittedKeyListener, signal: CommittedKeySignal) void {
        if (!self.vtable.matches_key(self.ptr, signal)) return;
        self.vtable.on_committed_key(self.ptr, signal);
    }
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    path: []u8,
    backend: lsm_backend.BackendHandle,
    store: docstore.DocStore,
    batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,
    projected_placement_intents: std.ArrayListUnmanaged(ProjectedPlacementIntent) = .empty,
    loaded_placement_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,
    projection_listeners: std.ArrayListUnmanaged(ProjectionListener) = .empty,
    committed_key_listeners: std.ArrayListUnmanaged(CommittedKeyListener) = .empty,

    const OwnedBatch = struct {
        commit_index: u64,
        entries_bytes: []u8,
    };

    const ProjectedPlacementIntent = struct {
        metadata_group_id: u64,
        intent: raft_reconciler.PlacementIntent,
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), root_dir);

        const path = try std.fmt.allocPrint(alloc, "{s}/metadata-apply-store", .{root_dir});
        errdefer alloc.free(path);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), path);

        var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
            .backend = .{
                .durability = if (cfg.no_sync) .none else .full,
                .read_only = cfg.read_only,
                .create_if_missing = !cfg.read_only,
            },
            .flush_threshold = cfg.flush_threshold,
        });
        errdefer backend.close();

        var runtime_store = try backend.backend.runtimeStore(alloc, .{ .name = "metadata-apply" });
        errdefer runtime_store.deinit();

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = root_dir,
            .path = path,
            .backend = backend,
            .store = try docstore.DocStore.openRuntime(alloc, runtime_store),
        };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        var it = self.batches.valueIterator();
        while (it.next()) |batch| self.alloc.free(batch.entries_bytes);
        self.batches.deinit(self.alloc);
        for (self.projected_placement_intents.items) |*entry| freePlacementIntent(self.alloc, entry.intent);
        self.projected_placement_intents.deinit(self.alloc);
        self.loaded_placement_groups.deinit(self.alloc);
        self.projection_listeners.deinit(self.alloc);
        self.committed_key_listeners.deinit(self.alloc);
        self.store.close();
        self.backend.close();
        self.alloc.free(self.path);
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_snapshot = buildSnapshot,
                .apply_batch = applyBatch,
            },
        };
    }

    pub fn snapshotWriteStats(self: *const RaftApplyStore) lsm_backend.Backend.WriteStats {
        return self.backend.snapshotWriteStats();
    }

    pub fn snapshotMaintenanceStats(self: *const RaftApplyStore) lsm_backend.Backend.MaintenanceStats {
        return self.backend.snapshotMaintenanceStats();
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedMetadataBatch {
        const batch = (try self.ensureLoaded(group_id)) orelse return null;
        return .{
            .commit_index = batch.commit_index,
            .entries_bytes = batch.entries_bytes,
        };
    }

    pub fn addProjectionListener(self: *RaftApplyStore, listener: ProjectionListener) !void {
        try self.projection_listeners.append(self.alloc, listener);
    }

    pub fn addCommittedKeyListener(self: *RaftApplyStore, listener: CommittedKeyListener) !void {
        try self.committed_key_listeners.append(self.alloc, listener);
    }

    pub fn listSplitTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SplitTransitionRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try splitTransitionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.SplitTransitionRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |*record| {
                if (record.split_key) |split_key| alloc.free(split_key);
                if (record.source_range_end) |end| alloc.free(end);
                if (record.rollback_reason) |reason| alloc.free(reason);
            }
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeSplitTransitionRecord(alloc, kv.value);
        return out;
    }

    pub fn listPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]raft_reconciler.PlacementIntent {
        try self.ensurePlacementIntentsLoaded(alloc, group_id);

        var count: usize = 0;
        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id == group_id) count += 1;
        }

        const out = try alloc.alloc(raft_reconciler.PlacementIntent, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |intent| {
                var record = intent.record;
                record.deinit(alloc);
                if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            }
            alloc.free(out);
        }

        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != group_id) continue;
            out[initialized] = try clonePlacementIntent(alloc, entry.intent);
            initialized += 1;
        }
        return out;
    }

    pub fn listLocalPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, metadata_group_id: u64, local_node_id: u64) ![]raft_reconciler.PlacementIntent {
        try self.ensurePlacementIntentsLoaded(alloc, metadata_group_id);

        var count: usize = 0;
        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            count += 1;
        }

        const out = try alloc.alloc(raft_reconciler.PlacementIntent, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |intent| freePlacementIntent(alloc, intent);
            alloc.free(out);
        }

        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            out[initialized] = try clonePlacementIntent(alloc, entry.intent);
            initialized += 1;
        }
        return out;
    }

    pub fn freePlacementIntents(_: *RaftApplyStore, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        for (intents) |intent| {
            freePlacementIntent(alloc, intent);
        }
        alloc.free(intents);
    }

    fn ensurePlacementIntentsLoaded(self: *RaftApplyStore, alloc: std.mem.Allocator, metadata_group_id: u64) !void {
        if (self.loaded_placement_groups.contains(metadata_group_id)) return;

        var prefix_buf: [128]u8 = undefined;
        const prefix = try placementPrefixForGroup(&prefix_buf, metadata_group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        for (kvs) |kv| {
            const intent = try decodePlacementIntent(alloc, kv.value);
            defer freePlacementIntent(alloc, intent);
            try self.upsertProjectedPlacementIntent(metadata_group_id, intent);
        }
        try self.loaded_placement_groups.put(self.alloc, metadata_group_id, {});
    }

    fn upsertProjectedPlacementIntent(self: *RaftApplyStore, metadata_group_id: u64, intent: raft_reconciler.PlacementIntent) !void {
        for (self.projected_placement_intents.items) |*entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.group_id != intent.record.group_id) continue;
            if (entry.intent.record.local_node_id != intent.record.local_node_id) continue;
            freePlacementIntent(self.alloc, entry.intent);
            entry.* = .{
                .metadata_group_id = metadata_group_id,
                .intent = try clonePlacementIntent(self.alloc, intent),
            };
            return;
        }
        try self.projected_placement_intents.append(self.alloc, .{
            .metadata_group_id = metadata_group_id,
            .intent = try clonePlacementIntent(self.alloc, intent),
        });
    }

    fn removeProjectedPlacementIntent(self: *RaftApplyStore, metadata_group_id: u64, range_group_id: u64, local_node_id: u64) void {
        for (self.projected_placement_intents.items, 0..) |entry, i| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.group_id != range_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            freePlacementIntent(self.alloc, entry.intent);
            _ = self.projected_placement_intents.swapRemove(i);
            return;
        }
    }

    pub fn listNodes(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.NodeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try nodePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.NodeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeNode(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeNodeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeNodes(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.NodeRecord) void {
        for (records) |record| metadata_table_manager.freeNode(alloc, record);
        alloc.free(records);
    }

    pub fn listStores(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.StoreRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.StoreRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeStore(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeStoreRecord(alloc, kv.value);
        return out;
    }

    pub fn freeStores(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.StoreRecord) void {
        for (records) |record| metadata_table_manager.freeStore(alloc, record);
        alloc.free(records);
    }

    pub fn listMergeTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.MergeTransitionRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try mergeTransitionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.MergeTransitionRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |*record| {
                if (record.rollback_reason) |reason| alloc.free(reason);
            }
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeMergeTransitionRecord(alloc, kv.value);
        return out;
    }

    pub fn listTables(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TableRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try tablePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.TableRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| {
                metadata_table_manager.freeTable(alloc, record);
            }
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeTableRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeTables(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.TableRecord) void {
        for (records) |record| {
            metadata_table_manager.freeTable(alloc, record);
        }
        alloc.free(records);
    }

    pub fn listDatabases(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.DatabaseRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try databasePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.DatabaseRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeDatabase(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeDatabaseRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeDatabases(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.DatabaseRecord) void {
        for (records) |record| metadata_table_manager.freeDatabase(alloc, record);
        alloc.free(records);
    }

    pub fn listNamespaces(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.NamespaceRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try namespacePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.NamespaceRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeNamespace(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeNamespaceRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeNamespaces(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.NamespaceRecord) void {
        for (records) |record| metadata_table_manager.freeNamespace(alloc, record);
        alloc.free(records);
    }

    pub fn listTablespaces(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TablespaceRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try tablespacePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.TablespaceRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeTablespace(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeTablespaceRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeTablespaces(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.TablespaceRecord) void {
        for (records) |record| metadata_table_manager.freeTablespace(alloc, record);
        alloc.free(records);
    }

    pub fn listSequences(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SequenceRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try sequencePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.SequenceRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeSequence(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeSequenceRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeSequences(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SequenceRecord) void {
        for (records) |record| metadata_table_manager.freeSequence(alloc, record);
        alloc.free(records);
    }

    pub fn listSchemaProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SchemaProgressRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try schemaProgressPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.SchemaProgressRecord, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| out[i] = try decodeSchemaProgressRecord(kv.value);
        return out;
    }

    pub fn freeSchemaProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SchemaProgressRecord) void {
        alloc.free(records);
    }

    pub fn listRestoreProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RestoreProgressRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try restoreProgressPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.RestoreProgressRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeRestoreProgressRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeRestoreProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.RestoreProgressRecord) void {
        for (records) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
        alloc.free(records);
    }

    pub fn listReplicationSourceStatuses(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ReplicationSourceStatusRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try replicationSourceStatusPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.ReplicationSourceStatusRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeReplicationSourceStatusRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeReplicationSourceStatuses(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ReplicationSourceStatusRecord) void {
        for (records) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
        alloc.free(records);
    }

    pub fn listExtensionPackages(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.PackageManifest {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try extensionPackagePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.PackageManifest, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeExtensionPackageRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeExtensionPackages(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.PackageManifest) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listInstalledExtensions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.InstalledExtension {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try installedExtensionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.InstalledExtension, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeInstalledExtensionRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeInstalledExtensions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.InstalledExtension) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listExtensionMembers(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.ExtensionMember {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try extensionMemberPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.ExtensionMember, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeExtensionMemberRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeExtensionMembers(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.ExtensionMember) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listExtensionDependencies(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.ExtensionDependency {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try extensionDependencyPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.ExtensionDependency, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeExtensionDependencyRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeExtensionDependencies(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.ExtensionDependency) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listShuffleJoinLeases(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ShuffleJoinLeaseRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try shuffleJoinLeasePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.ShuffleJoinLeaseRecord, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| {
            var pos: usize = 0;
            out[i] = try readShuffleJoinLeaseRecord(kv.value, &pos);
        }
        return out;
    }

    pub fn freeShuffleJoinLeases(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ShuffleJoinLeaseRecord) void {
        alloc.free(records);
    }

    pub fn listRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RangeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try rangePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.RangeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| out[i] = try decodeRangeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.RangeRecord) void {
        for (records) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(records);
    }

    pub fn listForeignKeyReferenceRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ForeignKeyReferenceRangeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try foreignKeyReferenceRangePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.ForeignKeyReferenceRangeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeForeignKeyReferenceRange(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| out[i] = try decodeForeignKeyReferenceRangeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeForeignKeyReferenceRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ForeignKeyReferenceRangeRecord) void {
        for (records) |record| metadata_table_manager.freeForeignKeyReferenceRange(alloc, record);
        alloc.free(records);
    }

    pub fn listUniqueConstraintRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.UniqueConstraintRangeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try uniqueConstraintRangePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.UniqueConstraintRangeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeUniqueConstraintRange(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| out[i] = try decodeUniqueConstraintRangeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeUniqueConstraintRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.UniqueConstraintRangeRecord) void {
        for (records) |record| metadata_table_manager.freeUniqueConstraintRange(alloc, record);
        alloc.free(records);
    }

    pub fn listSecondaryIndexRebuildRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SecondaryIndexRebuildRangeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try secondaryIndexRebuildRangePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.SecondaryIndexRebuildRangeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeSecondaryIndexRebuildRange(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| out[i] = try decodeSecondaryIndexRebuildRangeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeSecondaryIndexRebuildRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SecondaryIndexRebuildRangeRecord) void {
        for (records) |record| metadata_table_manager.freeSecondaryIndexRebuildRange(alloc, record);
        alloc.free(records);
    }

    pub fn listSchemaRewriteJobs(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SchemaRewriteJobRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try schemaRewriteJobPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.SchemaRewriteJobRecord, kvs.len);
        var decoded: usize = 0;
        errdefer {
            for (out[0..decoded]) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeSchemaRewriteJobRecord(alloc, kv.value);
            decoded = i + 1;
        }
        return out;
    }

    pub fn freeSchemaRewriteJobs(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SchemaRewriteJobRecord) void {
        for (records) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(records);
    }

    pub fn listTableEmptyingJobs(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TableEmptyingJobRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try tableEmptyingJobPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.TableEmptyingJobRecord, kvs.len);
        var decoded: usize = 0;
        errdefer {
            for (out[0..decoded]) |record| metadata_table_manager.freeTableEmptyingJob(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeTableEmptyingJobRecord(alloc, kv.value);
            decoded = i + 1;
        }
        return out;
    }

    pub fn freeTableEmptyingJobs(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.TableEmptyingJobRecord) void {
        for (records) |record| metadata_table_manager.freeTableEmptyingJob(alloc, record);
        alloc.free(records);
    }

    fn freeKvs(alloc: std.mem.Allocator, kvs: anytype) void {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }

    pub fn freeSplitTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SplitTransitionRecord) void {
        for (records) |record| {
            if (record.split_key) |split_key| alloc.free(split_key);
            if (record.source_range_end) |end| alloc.free(end);
            if (record.rollback_reason) |reason| alloc.free(reason);
        }
        alloc.free(records);
    }

    pub fn freeMergeTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.MergeTransitionRecord) void {
        for (records) |record| {
            if (record.rollback_reason) |reason| alloc.free(reason);
        }
        alloc.free(records);
    }

    pub fn getReconcileLease(self: *RaftApplyStore, group_id: u64) !?metadata.ReconcileLeaseRecord {
        var key_buf: [160]u8 = undefined;
        const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readReconcileLeaseRecord(encoded, &pos);
    }

    pub fn getReallocationRequest(self: *RaftApplyStore, group_id: u64) !?metadata.ReallocationRequestRecord {
        var key_buf: [160]u8 = undefined;
        const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readReallocationRequestRecord(encoded, &pos);
    }

    pub fn getShuffleJoinLease(self: *RaftApplyStore, group_id: u64, job_id: u64) !?metadata.ShuffleJoinLeaseRecord {
        var key_buf: [192]u8 = undefined;
        const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, job_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readShuffleJoinLeaseRecord(encoded, &pos);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        const batch = try self.ensureLoaded(group_id) orelse return error.MissingAppliedBatch;
        return try alloc.dupe(u8, batch.entries_bytes);
    }

    fn applyBatch(ptr: *anyopaque, batch: raft_state_machine.ApplyBatch) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try self.writeBatch(batch.group_id, batch.commit_index, batch.entries_bytes);
    }

    fn writeBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
        var value = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
        defer self.alloc.free(value);
        std.mem.writeInt(u64, value[0..8], commit_index, .little);
        @memcpy(value[8..], entries_bytes);

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        var txn = try self.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(key, value);
        try self.projectEntriesTxn(&txn, group_id, entries_bytes);
        try txn.commit();

        const owned_entries = try self.alloc.dupe(u8, entries_bytes);
        errdefer self.alloc.free(owned_entries);
        if (self.batches.getPtr(group_id)) |existing| {
            self.alloc.free(existing.entries_bytes);
            existing.* = .{
                .commit_index = commit_index,
                .entries_bytes = owned_entries,
            };
            return;
        }
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entries_bytes = owned_entries,
        });
    }

    fn ensureLoaded(self: *RaftApplyStore, group_id: u64) !?*OwnedBatch {
        if (self.batches.getPtr(group_id)) |batch| return batch;

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidMetadataApplyBatch;

        const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        const owned_entries = try self.alloc.dupe(u8, encoded[8..]);
        errdefer self.alloc.free(owned_entries);
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entries_bytes = owned_entries,
        });
        return self.batches.getPtr(group_id);
    }

    fn keyForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_raft_apply:{d}", .{group_id});
    }

    fn projectEntriesTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, entries_bytes: []const u8) !void {
        const decoded = raft_state_machine.decodeCommittedEntries(self.alloc, entries_bytes) catch |err| switch (err) {
            error.InvalidCommittedEntriesEncoding => return,
            else => return err,
        };
        defer self.alloc.free(decoded);

        for (decoded) |entry| {
            if (entry.entry_type != .normal) continue;
            var command = (try decodeTransitionCommand(self.alloc, entry.data)) orelse continue;
            defer command.deinit(self.alloc);
            try self.applyTransitionCommandTxn(txn, group_id, command);
        }
    }

    fn applyTransitionCommandTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, command: TransitionCommand) !void {
        try validateTransitionCommandDataGroupIds(command);
        switch (command) {
            .upsert_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                const value = try encodeNodeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .register_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                const value = try self.encodeRegistrationNodeRecordTxn(txn, group_id, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .request_node_shutdown => |record| {
                try self.applyNodeShutdownRequestTxn(txn, group_id, record.node_id);
            },
            .cancel_node_shutdown => |record| {
                try self.applyNodeShutdownCancelTxn(txn, group_id, record.node_id);
            },
            .finalize_node_shutdown => |record| {
                try self.applyNodeShutdownFinalizeTxn(txn, group_id, record.node_id);
            },
            .upsert_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const value = try encodeStoreRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                    .node_id = record.node_id,
                });
            },
            .register_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const applied = try self.normalizeStoreDrainIntentTxn(txn, group_id, record);
                const value = try encodeStoreRecord(self.alloc, applied);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                    .node_id = record.node_id,
                });
            },
            .remove_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                });
            },
            .upsert_replica_intent => |intent| {
                var key_buf: [192]u8 = undefined;
                const key = try placementKeyForGroup(&key_buf, group_id, intent.record.group_id, intent.record.local_node_id);
                const value = try encodePlacementIntent(self.alloc, intent);
                defer self.alloc.free(value);
                try txn.put(key, value);
                try self.upsertProjectedPlacementIntent(group_id, intent);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .placement_intent,
                    .metadata_group_id = group_id,
                    .group_id = intent.record.group_id,
                    .node_id = intent.record.local_node_id,
                });
            },
            .remove_replica_intent => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try placementKeyForGroup(&key_buf, group_id, record.group_id, record.local_node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.removeProjectedPlacementIntent(group_id, record.group_id, record.local_node_id);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .placement_intent,
                    .metadata_group_id = group_id,
                    .group_id = record.group_id,
                    .node_id = record.local_node_id,
                });
            },
            .upsert_database => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try databaseKeyForGroup(&key_buf, group_id, record.database_id);
                const value = try encodeDatabaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_database => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try databaseKeyForGroup(&key_buf, group_id, record.database_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_namespace => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try namespaceKeyForGroup(&key_buf, group_id, record.namespace_id);
                const value = try encodeNamespaceRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_namespace => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try namespaceKeyForGroup(&key_buf, group_id, record.namespace_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_tablespace => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try tablespaceKeyForGroup(&key_buf, group_id, record.tablespace_id);
                const value = try encodeTablespaceRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_tablespace => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try tablespaceKeyForGroup(&key_buf, group_id, record.tablespace_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_sequence => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try sequenceKeyForGroup(&key_buf, group_id, record.sequence_id);
                const value = try encodeSequenceRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_sequence => |record| {
                try self.deleteSequenceRecordTxn(txn, group_id, record.sequence_id);
            },
            .compare_and_swap_sequence => |request| {
                try self.compareAndSwapSequenceTxn(txn, group_id, request);
            },
            .upsert_table => |record| {
                try self.putTableRecordTxn(txn, group_id, record);
            },
            .remove_table => |record| {
                const existing_table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (existing_table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .table,
                    .metadata_group_id = group_id,
                    .table_name = existing_table_name,
                    .table_id = record.table_id,
                });
            },
            .upsert_schema_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [192]u8 = undefined;
                const key = try schemaProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id);
                const value = try encodeSchemaProgressRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .schema_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                });
            },
            .remove_schema_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [192]u8 = undefined;
                const key = try schemaProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .schema_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                });
            },
            .upsert_restore_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try restoreProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id, record.group_id);
                const value = try encodeRestoreProgressRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                    .group_id = record.group_id,
                });
            },
            .remove_restore_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try restoreProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id, record.group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                    .group_id = record.group_id,
                });
            },
            .upsert_replication_source_status => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try replicationSourceStatusKeyForGroup(&key_buf, group_id, record.table_id, record.source_ordinal);
                const value = try encodeReplicationSourceStatusRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .replication_source_status,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                });
            },
            .remove_replication_source_status => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try replicationSourceStatusKeyForGroup(&key_buf, group_id, record.table_id, record.source_ordinal);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .replication_source_status,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                });
            },
            .upsert_range => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                const value = try encodeRangeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .group_id = record.group_id,
                });
            },
            .remove_range => |record| {
                const existing = blk: {
                    var key_buf: [160]u8 = undefined;
                    const existing_key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                    const encoded = txn.get(existing_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const decoded = try decodeRangeRecord(self.alloc, encoded);
                    break :blk decoded;
                };
                defer if (existing) |record_existing| metadata_table_manager.freeRange(self.alloc, record_existing);
                const existing_table_id = if (existing) |record_existing| record_existing.table_id else 0;
                const table_name = if (existing) |record_existing|
                    try self.lookupTableNameTxn(txn, group_id, record_existing.table_id)
                else
                    null;
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = existing_table_id,
                    .group_id = record.group_id,
                });
            },
            .upsert_foreign_key_ref_range => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.child_table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [512]u8 = undefined;
                const key = try foreignKeyReferenceRangeKeyForGroup(&key_buf, group_id, record.child_table_id, record.constraint_name, record.parent_table_id, record.start_parent_key);
                const value = try encodeForeignKeyReferenceRangeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .foreign_key_ref_range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.child_table_id,
                    .group_id = record.group_id,
                });
            },
            .remove_foreign_key_ref_range => |record| {
                const existing = blk: {
                    var key_buf: [512]u8 = undefined;
                    const existing_key = try foreignKeyReferenceRangeKeyForGroup(&key_buf, group_id, record.child_table_id, record.constraint_name, record.parent_table_id, record.start_parent_key);
                    const encoded = txn.get(existing_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const decoded = try decodeForeignKeyReferenceRangeRecord(self.alloc, encoded);
                    break :blk decoded;
                };
                defer if (existing) |record_existing| metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, record_existing);
                const owner_group_id = if (existing) |record_existing| record_existing.group_id else 0;
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.child_table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [512]u8 = undefined;
                const key = try foreignKeyReferenceRangeKeyForGroup(&key_buf, group_id, record.child_table_id, record.constraint_name, record.parent_table_id, record.start_parent_key);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .foreign_key_ref_range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.child_table_id,
                    .group_id = owner_group_id,
                });
            },
            .begin_foreign_key_ref_range_split => |request| {
                try self.applyForeignKeyReferenceRangeBeginSplitTxn(txn, group_id, request);
            },
            .finish_foreign_key_ref_range_split => |request| {
                try self.applyForeignKeyReferenceRangeFinishSplitTxn(txn, group_id, request);
            },
            .begin_foreign_key_ref_range_merge => |request| {
                try self.applyForeignKeyReferenceRangeBeginMergeTxn(txn, group_id, request);
            },
            .finish_foreign_key_ref_range_merge => |request| {
                try self.applyForeignKeyReferenceRangeFinishMergeTxn(txn, group_id, request);
            },
            .begin_foreign_key_ref_range_rebuild => |selector| {
                try self.applyForeignKeyReferenceRangeSetStateTxn(txn, group_id, selector, metadata_table_manager.foreign_key_ref_range_active, metadata_table_manager.foreign_key_ref_range_rebuilding);
            },
            .finish_foreign_key_ref_range_rebuild => |selector| {
                try self.applyForeignKeyReferenceRangeSetStateTxn(txn, group_id, selector, metadata_table_manager.foreign_key_ref_range_rebuilding, metadata_table_manager.foreign_key_ref_range_active);
            },
            .upsert_unique_constraint_range => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [512]u8 = undefined;
                const key = try uniqueConstraintRangeKeyForGroup(&key_buf, group_id, record.table_id, record.constraint_name, record.start_encoded_value);
                const value = try encodeUniqueConstraintRangeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .unique_constraint_range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .group_id = record.group_id,
                });
            },
            .remove_unique_constraint_range => |record| {
                const existing = blk: {
                    var key_buf: [512]u8 = undefined;
                    const existing_key = try uniqueConstraintRangeKeyForGroup(&key_buf, group_id, record.table_id, record.constraint_name, record.start_encoded_value);
                    const encoded = txn.get(existing_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const decoded = try decodeUniqueConstraintRangeRecord(self.alloc, encoded);
                    break :blk decoded;
                };
                defer if (existing) |record_existing| metadata_table_manager.freeUniqueConstraintRange(self.alloc, record_existing);
                const owner_group_id = if (existing) |record_existing| record_existing.group_id else 0;
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [512]u8 = undefined;
                const key = try uniqueConstraintRangeKeyForGroup(&key_buf, group_id, record.table_id, record.constraint_name, record.start_encoded_value);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .unique_constraint_range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .group_id = owner_group_id,
                });
            },
            .begin_unique_constraint_range_split => |request| {
                try self.applyUniqueConstraintRangeBeginSplitTxn(txn, group_id, request);
            },
            .finish_unique_constraint_range_split => |request| {
                try self.applyUniqueConstraintRangeFinishSplitTxn(txn, group_id, request);
            },
            .begin_unique_constraint_range_merge => |request| {
                try self.applyUniqueConstraintRangeBeginMergeTxn(txn, group_id, request);
            },
            .finish_unique_constraint_range_merge => |request| {
                try self.applyUniqueConstraintRangeFinishMergeTxn(txn, group_id, request);
            },
            .begin_unique_constraint_range_rebuild => |selector| {
                try self.applyUniqueConstraintRangeSetStateTxn(txn, group_id, selector, metadata_table_manager.unique_constraint_range_active, metadata_table_manager.unique_constraint_range_rebuilding);
            },
            .finish_unique_constraint_range_rebuild => |selector| {
                try self.applyUniqueConstraintRangeSetStateTxn(txn, group_id, selector, metadata_table_manager.unique_constraint_range_rebuilding, metadata_table_manager.unique_constraint_range_active);
            },
            .upsert_secondary_index_rebuild_range => |record| {
                try self.putSecondaryIndexRebuildRangeTxn(txn, group_id, record);
            },
            .remove_secondary_index_rebuild_range => |selector| {
                const existing = (try self.loadSecondaryIndexRebuildRangeTxn(txn, group_id, selector)) orelse null;
                defer if (existing) |record_existing| metadata_table_manager.freeSecondaryIndexRebuildRange(self.alloc, record_existing);
                try self.deleteSecondaryIndexRebuildRangeTxn(txn, group_id, selector, if (existing) |record_existing| record_existing.group_id else 0);
            },
            .begin_secondary_index_rebuild_range => |request| {
                try self.applySecondaryIndexRebuildRangeBeginTxn(txn, group_id, request);
            },
            .finish_secondary_index_rebuild_range => |request| {
                try self.applySecondaryIndexRebuildRangeFinishTxn(txn, group_id, request);
            },
            .invalidate_secondary_index_rebuild_range => |request| {
                try self.applySecondaryIndexRebuildRangeInvalidateTxn(txn, group_id, request);
            },
            .upsert_schema_rewrite_job => |record| {
                try self.putSchemaRewriteJobTxn(txn, group_id, record);
            },
            .apply_table_catalog_update_with_schema_rewrite_jobs => |request| {
                try self.applyTableCatalogUpdateWithSchemaRewriteJobsTxn(txn, group_id, request);
            },
            .apply_table_catalog_batch_update_with_schema_rewrite_jobs => |request| {
                try self.applyTableCatalogBatchUpdateWithSchemaRewriteJobsTxn(txn, group_id, request);
            },
            .apply_table_catalog_drop_with_schema_rewrite_jobs => |request| {
                try self.applyTableCatalogDropWithSchemaRewriteJobsTxn(txn, group_id, request);
            },
            .remove_schema_rewrite_job => |record| {
                try self.deleteSchemaRewriteJobTxn(txn, group_id, record.job_id);
            },
            .begin_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobBeginTxn(txn, group_id, request);
            },
            .finish_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobFinishTxn(txn, group_id, request);
            },
            .invalidate_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobInvalidateTxn(txn, group_id, request);
            },
            .pause_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobPauseTxn(txn, group_id, request);
            },
            .resume_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobResumeTxn(txn, group_id, request);
            },
            .retry_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobRetryTxn(txn, group_id, request);
            },
            .cancel_schema_rewrite_job => |request| {
                try self.applySchemaRewriteJobCancelTxn(txn, group_id, request);
            },
            .upsert_table_emptying_job => |record| {
                try self.putTableEmptyingJobTxn(txn, group_id, record);
            },
            .remove_table_emptying_job => |record| {
                try self.deleteTableEmptyingJobTxn(txn, group_id, record.job_id);
            },
            .begin_table_emptying_job => |request| {
                try self.applyTableEmptyingJobBeginTxn(txn, group_id, request);
            },
            .finish_table_emptying_job => |request| {
                try self.applyTableEmptyingJobFinishTxn(txn, group_id, request);
            },
            .invalidate_table_emptying_job => |request| {
                try self.applyTableEmptyingJobInvalidateTxn(txn, group_id, request);
            },
            .pause_table_emptying_job => |request| {
                try self.applyTableEmptyingJobPauseTxn(txn, group_id, request);
            },
            .resume_table_emptying_job => |request| {
                try self.applyTableEmptyingJobResumeTxn(txn, group_id, request);
            },
            .retry_table_emptying_job => |request| {
                try self.applyTableEmptyingJobRetryTxn(txn, group_id, request);
            },
            .cancel_table_emptying_job => |request| {
                try self.applyTableEmptyingJobCancelTxn(txn, group_id, request);
            },
            .promote_table_emptying_barrier => |request| {
                try self.applyTableEmptyingBarrierPromotionTxn(txn, group_id, request);
            },
            .promote_secondary_index_ready => |request| {
                try self.applySecondaryIndexReadyPromotionTxn(txn, group_id, request);
            },
            .compare_and_swap_table_schema => |request| {
                try self.applyTableSchemaCompareAndSwapTxn(txn, group_id, request);
            },
            .upsert_split_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try splitTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                const value = try encodeSplitTransitionRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .split_transition,
                    .metadata_group_id = group_id,
                    .group_id = record.source_group_id,
                });
            },
            .remove_split_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try splitTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .split_transition,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_merge_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try mergeTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                const value = try encodeMergeTransitionRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .merge_transition,
                    .metadata_group_id = group_id,
                    .group_id = record.receiver_group_id,
                });
            },
            .remove_merge_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try mergeTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .merge_transition,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_reconcile_lease => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
                const value = try encodeReconcileLeaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .reconcile_lease,
                    .metadata_group_id = group_id,
                });
            },
            .remove_reconcile_lease => {
                var key_buf: [160]u8 = undefined;
                const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .reconcile_lease,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_shuffle_join_lease => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, record.job_id);
                const value = try encodeShuffleJoinLeaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .shuffle_join_lease,
                    .metadata_group_id = group_id,
                });
            },
            .remove_shuffle_join_lease => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, record.job_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .shuffle_join_lease,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_reallocation_request => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
                const value = try encodeReallocationRequestRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_reallocation_request => {
                var key_buf: [160]u8 = undefined;
                const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_extension_package => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionPackageKeyForGroup(&key_buf, group_id, record.name, record.version);
                const value = try encodeExtensionPackageRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_extension_package => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionPackageKeyForGroup(&key_buf, group_id, record.name, record.version);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_installed_extension => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try installedExtensionKeyForGroup(&key_buf, group_id, record.name);
                const value = try encodeInstalledExtensionRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_installed_extension => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try installedExtensionKeyForGroup(&key_buf, group_id, record.name);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_extension_member => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
                const value = try encodeExtensionMemberRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_extension_member => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_extension_dependency => |record| {
                var key_buf: [320]u8 = undefined;
                const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
                const value = try encodeExtensionDependencyRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_extension_dependency => |record| {
                var key_buf: [320]u8 = undefined;
                const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .apply_extension_lifecycle => |delta| {
                try self.applyExtensionLifecycleDeltaTxn(txn, group_id, delta);
            },
        }
    }

    fn putTableRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.TableRecord,
    ) !void {
        try self.ensureTableCatalogIdentityTxn(txn, group_id, record);
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
        const value = try encodeTableRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table,
            .metadata_group_id = group_id,
            .table_name = record.name,
            .table_id = record.table_id,
        });
    }

    fn loadTableRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        table_id: u64,
    ) !?metadata.TableRecord {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeTableRecord(self.alloc, encoded);
    }

    fn deleteTableRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        table_id: u64,
        table_name: ?[]const u8,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = table_id,
        });
    }

    fn loadRangeRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        range_group_id: u64,
    ) !?metadata.RangeRecord {
        var key_buf: [160]u8 = undefined;
        const key = try rangeKeyForGroup(&key_buf, group_id, range_group_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeRangeRecord(self.alloc, encoded);
    }

    fn deleteRangeRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        range_group_id: u64,
    ) !void {
        const existing = try self.loadRangeRecordTxn(txn, group_id, range_group_id);
        defer if (existing) |record_existing| metadata_table_manager.freeRange(self.alloc, record_existing);
        const existing_table_id = if (existing) |record_existing| record_existing.table_id else 0;
        const table_name = if (existing) |record_existing|
            try self.lookupTableNameTxn(txn, group_id, record_existing.table_id)
        else
            null;
        defer if (table_name) |name| self.alloc.free(name);
        var key_buf: [160]u8 = undefined;
        const key = try rangeKeyForGroup(&key_buf, group_id, range_group_id);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = existing_table_id,
            .group_id = range_group_id,
        });
    }

    fn sequenceExistsTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        sequence_id: u64,
    ) !bool {
        _ = self;
        var key_buf: [160]u8 = undefined;
        const key = try sequenceKeyForGroup(&key_buf, group_id, sequence_id);
        _ = txn.get(key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        return true;
    }

    fn deleteSequenceRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        sequence_id: u64,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try sequenceKeyForGroup(&key_buf, group_id, sequence_id);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
    }

    fn applyTableCatalogUpdateWithSchemaRewriteJobsTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
    ) !void {
        return try self.applyTableCatalogBatchUpdateWithSchemaRewriteJobsTxn(txn, group_id, .{
            .tables = &[_]metadata.TableRecord{request.table},
            .schema_rewrite_jobs = request.schema_rewrite_jobs,
        });
    }

    fn applyTableCatalogBatchUpdateWithSchemaRewriteJobsTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
    ) !void {
        if (request.tables.len == 0) return error.UnknownTable;
        for (request.tables, 0..) |table, i| {
            if (table.table_id == 0) return error.UnknownTable;
            if (metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json) == 0) return error.InvalidSchemaRewriteGeneration;
            for (request.tables[0..i]) |previous| {
                if (previous.table_id == table.table_id) return error.UnknownTable;
            }
        }
        for (request.schema_rewrite_jobs, 0..) |job, i| {
            try validateSchemaRewriteJobForBatchUpdate(request.tables, job);
            try group_ids.requireDataGroupId(job.group_id);
            for (request.schema_rewrite_jobs[0..i]) |previous| {
                if (previous.job_id == job.job_id) return error.InvalidSchemaRewriteJob;
            }
        }

        for (request.tables) |table| try self.putTableRecordTxn(txn, group_id, table);
        for (request.schema_rewrite_jobs) |job| try self.putSchemaRewriteJobTxn(txn, group_id, job);
    }

    fn applyTableCatalogDropWithSchemaRewriteJobsTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest,
    ) !void {
        if (request.table_id == 0) return error.UnknownTable;
        const existing_table_name = try self.lookupTableNameTxn(txn, group_id, request.table_id);
        if (existing_table_name == null) return error.UnknownTable;
        defer self.alloc.free(existing_table_name.?);

        if (request.table_updates.len > 0 or request.schema_rewrite_jobs.len > 0) {
            try self.validateTableCatalogBatchUpdateWithSchemaRewriteJobsTxn(.{
                .tables = request.table_updates,
                .schema_rewrite_jobs = request.schema_rewrite_jobs,
            });
        }
        for (request.table_updates) |table| {
            if (table.table_id == request.table_id) return error.UnknownTable;
        }
        for (request.sequence_ids, 0..) |sequence_id, i| {
            if (!try self.sequenceExistsTxn(txn, group_id, sequence_id)) return error.SequenceNotFound;
            for (request.sequence_ids[0..i]) |previous| {
                if (previous == sequence_id) return error.SequenceNotFound;
            }
        }
        for (request.range_group_ids, 0..) |range_group_id, i| {
            const range = try self.loadRangeRecordTxn(txn, group_id, range_group_id);
            defer if (range) |record| metadata_table_manager.freeRange(self.alloc, record);
            if (range == null or range.?.table_id != request.table_id) return error.UnknownRange;
            for (request.range_group_ids[0..i]) |previous| {
                if (previous == range_group_id) return error.UnknownRange;
            }
        }
        try self.validateTableCatalogDropRangeSetTxn(txn, group_id, request.table_id, request.range_group_ids);

        for (request.table_updates) |table| try self.putTableRecordTxn(txn, group_id, table);
        for (request.schema_rewrite_jobs) |job| try self.putSchemaRewriteJobTxn(txn, group_id, job);
        for (request.range_group_ids) |range_group_id| try self.deleteRangeRecordTxn(txn, group_id, range_group_id);
        for (request.sequence_ids) |sequence_id| try self.deleteSequenceRecordTxn(txn, group_id, sequence_id);
        try self.deleteTableRecordTxn(txn, group_id, request.table_id, existing_table_name.?);
    }

    fn validateTableCatalogDropRangeSetTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        table_id: u64,
        range_group_ids: []const u64,
    ) !void {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try rangePrefixForGroup(&prefix_buf, group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const range = try decodeRangeRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeRange(self.alloc, range);
            if (range.table_id != table_id) continue;
            if (std.mem.indexOfScalar(u64, range_group_ids, range.group_id) == null) return error.UnknownRange;
        }
    }

    fn validateTableCatalogBatchUpdateWithSchemaRewriteJobsTxn(
        self: *RaftApplyStore,
        request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
    ) !void {
        _ = self;
        if (request.tables.len == 0) return error.UnknownTable;
        for (request.tables, 0..) |table, i| {
            if (table.table_id == 0) return error.UnknownTable;
            if (metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json) == 0) return error.InvalidSchemaRewriteGeneration;
            for (request.tables[0..i]) |previous| {
                if (previous.table_id == table.table_id) return error.UnknownTable;
            }
        }
        for (request.schema_rewrite_jobs, 0..) |job, i| {
            try validateSchemaRewriteJobForBatchUpdate(request.tables, job);
            try group_ids.requireDataGroupId(job.group_id);
            for (request.schema_rewrite_jobs[0..i]) |previous| {
                if (previous.job_id == job.job_id) return error.InvalidSchemaRewriteJob;
            }
        }
    }

    fn validateSchemaRewriteJobForBatchUpdate(tables: []const metadata.TableRecord, job: metadata.SchemaRewriteJobRecord) !void {
        if (job.job_id == 0) return error.InvalidSchemaRewriteJob;
        for (tables) |table| {
            if (job.table_id != table.table_id) continue;
            const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
            if (job.schema_generation != schema_generation) return error.InvalidSchemaRewriteGeneration;
            return;
        }
        return error.InvalidSchemaRewriteJob;
    }

    fn applyExtensionLifecycleDeltaTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, delta: ExtensionLifecycleDelta) !void {
        for (delta.upsert_tables) |record| {
            var key_buf: [160]u8 = undefined;
            const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
            const value = try encodeTableRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.remove_extension_dependencies) |record| {
            var key_buf: [320]u8 = undefined;
            const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.remove_extension_members) |record| {
            var key_buf: [256]u8 = undefined;
            const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.remove_installed_extensions) |name| {
            var key_buf: [192]u8 = undefined;
            const key = try installedExtensionKeyForGroup(&key_buf, group_id, name);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.upsert_installed_extensions) |record| {
            var key_buf: [192]u8 = undefined;
            const key = try installedExtensionKeyForGroup(&key_buf, group_id, record.name);
            const value = try encodeInstalledExtensionRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.upsert_extension_dependencies) |record| {
            var key_buf: [320]u8 = undefined;
            const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
            const value = try encodeExtensionDependencyRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.upsert_extension_members) |record| {
            var key_buf: [256]u8 = undefined;
            const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
            const value = try encodeExtensionMemberRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
    }

    fn encodeRegistrationNodeRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.NodeRecord,
    ) ![]u8 {
        var applied = record;
        const existing = try self.loadNodeRecordTxn(txn, group_id, record.node_id);
        defer if (existing) |existing_record| metadata_table_manager.freeNode(self.alloc, existing_record);
        if (existing) |existing_record| {
            applied.lifecycle = existing_record.lifecycle;
        }
        return try encodeNodeRecord(self.alloc, applied);
    }

    fn applyNodeShutdownRequestTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        try self.setNodeLifecycleTxn(txn, group_id, node_id, metadata_table_manager.node_lifecycle_draining, true);
        try self.setNodeStoresDrainRequestedTxn(txn, group_id, node_id, true);
    }

    fn applyNodeShutdownCancelTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        try self.setNodeLifecycleTxn(txn, group_id, node_id, metadata_table_manager.node_lifecycle_active, false);
        try self.setNodeStoresDrainRequestedTxn(txn, group_id, node_id, false);
    }

    fn applyNodeShutdownFinalizeTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        var node_key_buf: [160]u8 = undefined;
        const node_key = try nodeKeyForGroup(&node_key_buf, group_id, node_id);

        const existing_node = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing_node) |record| metadata_table_manager.freeNode(self.alloc, record);
        var draining_node = false;
        if (existing_node) |record| {
            if (metadata_table_manager.nodeLifecycleActive(record.lifecycle)) return error.ActiveNodeFinalizeRejected;
            draining_node = true;
        }

        const StoreRef = struct {
            store_id: u64,
            node_id: u64,
        };
        var stores_to_delete = std.ArrayListUnmanaged(StoreRef).empty;
        defer stores_to_delete.deinit(self.alloc);

        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var entry = try cur.seekAtOrAfter(prefix);
            while (entry) |kv| : (entry = try cur.next()) {
                if (!std.mem.startsWith(u8, kv.key, prefix)) break;
                const store = try decodeStoreRecord(self.alloc, kv.value);
                defer metadata_table_manager.freeStore(self.alloc, store);
                if (store.node_id == node_id) {
                    if (!draining_node and !store.drain_requested) return error.ActiveNodeFinalizeRejected;
                    try stores_to_delete.append(self.alloc, .{ .store_id = store.store_id, .node_id = store.node_id });
                }
            }
        }

        txn.delete(node_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = node_key });

        for (stores_to_delete.items) |store| {
            var key_buf: [160]u8 = undefined;
            const key = try storeKeyForGroup(&key_buf, group_id, store.store_id);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .store,
                .metadata_group_id = group_id,
                .store_id = store.store_id,
                .node_id = store.node_id,
            });
        }
    }

    fn setNodeLifecycleTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
        lifecycle: []const u8,
        create_if_missing: bool,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try nodeKeyForGroup(&key_buf, group_id, node_id);

        const existing = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing) |existing_record| metadata_table_manager.freeNode(self.alloc, existing_record);

        if (existing) |existing_record| {
            if (std.mem.eql(u8, existing_record.lifecycle, lifecycle)) return;
            var applied = existing_record;
            applied.lifecycle = lifecycle;
            const value = try encodeNodeRecord(self.alloc, applied);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            return;
        }

        if (!create_if_missing) return;
        const record: metadata.NodeRecord = .{
            .node_id = node_id,
            .role = "data",
            .lifecycle = lifecycle,
        };
        const value = try encodeNodeRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
    }

    fn setNodeStoresDrainRequestedTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
        drain_requested: bool,
    ) !void {
        var updates = std.ArrayListUnmanaged(metadata.StoreRecord).empty;
        defer {
            for (updates.items) |record| metadata_table_manager.freeStore(self.alloc, record);
            updates.deinit(self.alloc);
        }

        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var entry = try cur.seekAtOrAfter(prefix);
            while (entry) |kv| : (entry = try cur.next()) {
                if (!std.mem.startsWith(u8, kv.key, prefix)) break;
                var store = try decodeStoreRecord(self.alloc, kv.value);
                var store_owned = true;
                errdefer if (store_owned) metadata_table_manager.freeStore(self.alloc, store);
                if (store.node_id == node_id and store.drain_requested != drain_requested) {
                    store.drain_requested = drain_requested;
                    try updates.append(self.alloc, store);
                    store_owned = false;
                } else {
                    store_owned = false;
                    metadata_table_manager.freeStore(self.alloc, store);
                }
            }
        }

        for (updates.items) |record| {
            var key_buf: [160]u8 = undefined;
            const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
            const value = try encodeStoreRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .store,
                .metadata_group_id = group_id,
                .store_id = record.store_id,
                .node_id = record.node_id,
            });
        }
    }

    fn normalizeStoreDrainIntentTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.StoreRecord,
    ) !metadata.StoreRecord {
        var applied = record;
        if (try self.nodeDrainRequestedTxn(txn, group_id, record.node_id)) {
            applied.drain_requested = true;
            return applied;
        }
        const existing = try self.loadStoreRecordTxn(txn, group_id, record.store_id);
        defer if (existing) |existing_record| metadata_table_manager.freeStore(self.alloc, existing_record);
        if (existing) |existing_record| {
            applied.drain_requested = existing_record.drain_requested;
        } else {
            applied.drain_requested = false;
        }
        return applied;
    }

    fn loadNodeRecordTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !?metadata.NodeRecord {
        var key_buf: [160]u8 = undefined;
        const key = try nodeKeyForGroup(&key_buf, group_id, node_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeNodeRecord(self.alloc, encoded);
    }

    fn nodeDrainRequestedTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !bool {
        const node = (try self.loadNodeRecordTxn(txn, group_id, node_id)) orelse return false;
        defer metadata_table_manager.freeNode(self.alloc, node);
        return !metadata_table_manager.nodeLifecycleActive(node.lifecycle);
    }

    fn loadStoreRecordTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, store_id: u64) !?metadata.StoreRecord {
        var key_buf: [160]u8 = undefined;
        const key = try storeKeyForGroup(&key_buf, group_id, store_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeStoreRecord(self.alloc, encoded);
    }

    fn applyForeignKeyReferenceRangeBeginSplitTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.ForeignKeyReferenceRangeSplitRequest,
    ) !void {
        if (request.left_group_id == request.right_group_id) return error.InvalidForeignKeyReferenceRangeSplit;
        var source = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, source);
        if (!std.mem.eql(u8, source.state, metadata_table_manager.foreign_key_ref_range_active)) return error.ForeignKeyReferenceRangeNotActive;
        if (!foreignKeyReferenceSplitKeyInsideRange(request.split_parent_key, source.start_parent_key, source.end_parent_key)) return error.InvalidForeignKeyReferenceRangeSplit;
        if (request.left_group_id != source.group_id and try self.foreignKeyReferenceRangeGroupExistsTxn(txn, group_id, request.left_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (try self.foreignKeyReferenceRangeGroupExistsTxn(txn, group_id, request.right_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        try replaceOwnedString(self.alloc, &source.state, metadata_table_manager.foreign_key_ref_range_splitting);
        source.topology_epoch +%= 1;
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, source);
    }

    fn applyForeignKeyReferenceRangeFinishSplitTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.ForeignKeyReferenceRangeSplitRequest,
    ) !void {
        if (request.left_group_id == request.right_group_id) return error.InvalidForeignKeyReferenceRangeSplit;
        const source = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, source);
        if (!std.mem.eql(u8, source.state, metadata_table_manager.foreign_key_ref_range_splitting)) return error.ForeignKeyReferenceRangeNotSplitting;
        if (!foreignKeyReferenceSplitKeyInsideRange(request.split_parent_key, source.start_parent_key, source.end_parent_key)) return error.InvalidForeignKeyReferenceRangeSplit;
        if (request.left_group_id != source.group_id and try self.foreignKeyReferenceRangeGroupExistsTxn(txn, group_id, request.left_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;
        if (try self.foreignKeyReferenceRangeGroupExistsTxn(txn, group_id, request.right_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        try self.deleteForeignKeyReferenceRangeTxn(txn, group_id, source);
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, .{
            .child_table_id = source.child_table_id,
            .constraint_name = source.constraint_name,
            .parent_table_id = source.parent_table_id,
            .start_parent_key = source.start_parent_key,
            .end_parent_key = request.split_parent_key,
            .group_id = request.left_group_id,
            .topology_epoch = source.topology_epoch +% 1,
            .state = metadata_table_manager.foreign_key_ref_range_active,
        });
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, .{
            .child_table_id = source.child_table_id,
            .constraint_name = source.constraint_name,
            .parent_table_id = source.parent_table_id,
            .start_parent_key = request.split_parent_key,
            .end_parent_key = source.end_parent_key,
            .group_id = request.right_group_id,
            .topology_epoch = source.topology_epoch +% 1,
            .state = metadata_table_manager.foreign_key_ref_range_active,
        });
    }

    fn applyForeignKeyReferenceRangeBeginMergeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.ForeignKeyReferenceRangeMergeRequest,
    ) !void {
        var left = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, request.left_selector)) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, left);
        var right = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, .{
            .child_table_id = request.left_selector.child_table_id,
            .constraint_name = request.left_selector.constraint_name,
            .parent_table_id = request.left_selector.parent_table_id,
            .start_parent_key = request.right_start_parent_key,
        })) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, right);
        if (!foreignKeyReferenceRangesAdjacent(left, right)) return error.ForeignKeyReferenceRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, metadata_table_manager.foreign_key_ref_range_active) or !std.mem.eql(u8, right.state, metadata_table_manager.foreign_key_ref_range_active)) return error.ForeignKeyReferenceRangeNotActive;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and try self.foreignKeyReferenceRangeGroupExistsTxn(txn, group_id, request.merged_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        try replaceOwnedString(self.alloc, &left.state, metadata_table_manager.foreign_key_ref_range_merging);
        left.topology_epoch +%= 1;
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, left);
        try replaceOwnedString(self.alloc, &right.state, metadata_table_manager.foreign_key_ref_range_merging);
        right.topology_epoch +%= 1;
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, right);
    }

    fn applyForeignKeyReferenceRangeFinishMergeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.ForeignKeyReferenceRangeMergeRequest,
    ) !void {
        const left = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, request.left_selector)) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, left);
        const right = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, .{
            .child_table_id = request.left_selector.child_table_id,
            .constraint_name = request.left_selector.constraint_name,
            .parent_table_id = request.left_selector.parent_table_id,
            .start_parent_key = request.right_start_parent_key,
        })) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, right);
        if (!foreignKeyReferenceRangesAdjacent(left, right)) return error.ForeignKeyReferenceRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, metadata_table_manager.foreign_key_ref_range_merging) or !std.mem.eql(u8, right.state, metadata_table_manager.foreign_key_ref_range_merging)) return error.ForeignKeyReferenceRangeNotMerging;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and try self.foreignKeyReferenceRangeGroupExistsTxn(txn, group_id, request.merged_group_id)) return error.ForeignKeyReferenceRangeGroupCollision;

        try self.deleteForeignKeyReferenceRangeTxn(txn, group_id, left);
        try self.deleteForeignKeyReferenceRangeTxn(txn, group_id, right);
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, .{
            .child_table_id = left.child_table_id,
            .constraint_name = left.constraint_name,
            .parent_table_id = left.parent_table_id,
            .start_parent_key = left.start_parent_key,
            .end_parent_key = right.end_parent_key,
            .group_id = request.merged_group_id,
            .topology_epoch = @max(left.topology_epoch, right.topology_epoch) +% 1,
            .state = metadata_table_manager.foreign_key_ref_range_active,
        });
    }

    fn applyForeignKeyReferenceRangeSetStateTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        selector: metadata_table_manager.ForeignKeyReferenceRangeSelector,
        expected_state: []const u8,
        next_state: []const u8,
    ) !void {
        var record = (try self.loadForeignKeyReferenceRangeTxn(txn, group_id, selector)) orelse return error.UnknownForeignKeyReferenceRange;
        defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, record);
        if (!std.mem.eql(u8, record.state, expected_state)) {
            if (std.mem.eql(u8, expected_state, metadata_table_manager.foreign_key_ref_range_active)) return error.ForeignKeyReferenceRangeNotActive;
            if (std.mem.eql(u8, expected_state, metadata_table_manager.foreign_key_ref_range_rebuilding)) return error.ForeignKeyReferenceRangeNotRebuilding;
            return error.InvalidForeignKeyReferenceRangeState;
        }
        try replaceOwnedString(self.alloc, &record.state, next_state);
        record.topology_epoch +%= 1;
        try self.putForeignKeyReferenceRangeTxn(txn, group_id, record);
    }

    fn loadForeignKeyReferenceRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        selector: metadata_table_manager.ForeignKeyReferenceRangeSelector,
    ) !?metadata.ForeignKeyReferenceRangeRecord {
        var key_buf: [512]u8 = undefined;
        const key = try foreignKeyReferenceRangeKeyForGroup(&key_buf, group_id, selector.child_table_id, selector.constraint_name, selector.parent_table_id, selector.start_parent_key);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeForeignKeyReferenceRangeRecord(self.alloc, encoded);
    }

    fn putForeignKeyReferenceRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.ForeignKeyReferenceRangeRecord,
    ) !void {
        var key_buf: [512]u8 = undefined;
        const key = try foreignKeyReferenceRangeKeyForGroup(&key_buf, group_id, record.child_table_id, record.constraint_name, record.parent_table_id, record.start_parent_key);
        const value = try encodeForeignKeyReferenceRangeRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        const table_name = try self.lookupTableNameTxn(txn, group_id, record.child_table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyProjectionListeners(.{
            .kind = .foreign_key_ref_range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = record.child_table_id,
            .group_id = record.group_id,
        });
    }

    fn deleteForeignKeyReferenceRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.ForeignKeyReferenceRangeRecord,
    ) !void {
        var key_buf: [512]u8 = undefined;
        const key = try foreignKeyReferenceRangeKeyForGroup(&key_buf, group_id, record.child_table_id, record.constraint_name, record.parent_table_id, record.start_parent_key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        const table_name = try self.lookupTableNameTxn(txn, group_id, record.child_table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyProjectionListeners(.{
            .kind = .foreign_key_ref_range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = record.child_table_id,
            .group_id = record.group_id,
        });
    }

    fn foreignKeyReferenceRangeGroupExistsTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        owner_group_id: u64,
    ) !bool {
        var range_key_buf: [160]u8 = undefined;
        const range_key = try rangeKeyForGroup(&range_key_buf, group_id, owner_group_id);
        if (txn.get(range_key)) |_| return true else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }

        var prefix_buf: [128]u8 = undefined;
        const prefix = try foreignKeyReferenceRangePrefixForGroup(&prefix_buf, group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const record = try decodeForeignKeyReferenceRangeRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeForeignKeyReferenceRange(self.alloc, record);
            if (record.group_id == owner_group_id) return true;
        }
        return false;
    }

    fn applyUniqueConstraintRangeBeginSplitTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.UniqueConstraintRangeSplitRequest,
    ) !void {
        if (request.left_group_id == request.right_group_id) return error.InvalidUniqueConstraintRangeSplit;
        var source = (try self.loadUniqueConstraintRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, source);
        if (!std.mem.eql(u8, source.state, metadata_table_manager.unique_constraint_range_active)) return error.UniqueConstraintRangeNotActive;
        if (!uniqueConstraintSplitKeyInsideRange(request.split_encoded_value, source.start_encoded_value, source.end_encoded_value)) return error.InvalidUniqueConstraintRangeSplit;
        if (request.left_group_id != source.group_id and try self.uniqueConstraintRangeGroupExistsTxn(txn, group_id, request.left_group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (try self.uniqueConstraintRangeGroupExistsTxn(txn, group_id, request.right_group_id)) return error.UniqueConstraintRangeGroupCollision;

        try replaceOwnedString(self.alloc, &source.state, metadata_table_manager.unique_constraint_range_splitting);
        source.topology_epoch +%= 1;
        try self.putUniqueConstraintRangeTxn(txn, group_id, source);
    }

    fn applyUniqueConstraintRangeFinishSplitTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.UniqueConstraintRangeSplitRequest,
    ) !void {
        if (request.left_group_id == request.right_group_id) return error.InvalidUniqueConstraintRangeSplit;
        const source = (try self.loadUniqueConstraintRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, source);
        if (!std.mem.eql(u8, source.state, metadata_table_manager.unique_constraint_range_splitting)) return error.UniqueConstraintRangeNotSplitting;
        if (!uniqueConstraintSplitKeyInsideRange(request.split_encoded_value, source.start_encoded_value, source.end_encoded_value)) return error.InvalidUniqueConstraintRangeSplit;
        if (request.left_group_id != source.group_id and try self.uniqueConstraintRangeGroupExistsTxn(txn, group_id, request.left_group_id)) return error.UniqueConstraintRangeGroupCollision;
        if (try self.uniqueConstraintRangeGroupExistsTxn(txn, group_id, request.right_group_id)) return error.UniqueConstraintRangeGroupCollision;

        try self.deleteUniqueConstraintRangeTxn(txn, group_id, source);
        try self.putUniqueConstraintRangeTxn(txn, group_id, .{
            .table_id = source.table_id,
            .constraint_name = source.constraint_name,
            .start_encoded_value = source.start_encoded_value,
            .end_encoded_value = request.split_encoded_value,
            .group_id = request.left_group_id,
            .topology_epoch = source.topology_epoch +% 1,
            .state = metadata_table_manager.unique_constraint_range_active,
        });
        try self.putUniqueConstraintRangeTxn(txn, group_id, .{
            .table_id = source.table_id,
            .constraint_name = source.constraint_name,
            .start_encoded_value = request.split_encoded_value,
            .end_encoded_value = source.end_encoded_value,
            .group_id = request.right_group_id,
            .topology_epoch = source.topology_epoch +% 1,
            .state = metadata_table_manager.unique_constraint_range_active,
        });
    }

    fn applyUniqueConstraintRangeBeginMergeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.UniqueConstraintRangeMergeRequest,
    ) !void {
        var left = (try self.loadUniqueConstraintRangeTxn(txn, group_id, request.left_selector)) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, left);
        var right = (try self.loadUniqueConstraintRangeTxn(txn, group_id, .{
            .table_id = request.left_selector.table_id,
            .constraint_name = request.left_selector.constraint_name,
            .start_encoded_value = request.right_start_encoded_value,
        })) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, right);
        if (!uniqueConstraintRangesAdjacent(left, right)) return error.UniqueConstraintRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, metadata_table_manager.unique_constraint_range_active) or !std.mem.eql(u8, right.state, metadata_table_manager.unique_constraint_range_active)) return error.UniqueConstraintRangeNotActive;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and try self.uniqueConstraintRangeGroupExistsTxn(txn, group_id, request.merged_group_id)) return error.UniqueConstraintRangeGroupCollision;

        try replaceOwnedString(self.alloc, &left.state, metadata_table_manager.unique_constraint_range_merging);
        left.topology_epoch +%= 1;
        try self.putUniqueConstraintRangeTxn(txn, group_id, left);
        try replaceOwnedString(self.alloc, &right.state, metadata_table_manager.unique_constraint_range_merging);
        right.topology_epoch +%= 1;
        try self.putUniqueConstraintRangeTxn(txn, group_id, right);
    }

    fn applyUniqueConstraintRangeFinishMergeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.UniqueConstraintRangeMergeRequest,
    ) !void {
        const left = (try self.loadUniqueConstraintRangeTxn(txn, group_id, request.left_selector)) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, left);
        const right = (try self.loadUniqueConstraintRangeTxn(txn, group_id, .{
            .table_id = request.left_selector.table_id,
            .constraint_name = request.left_selector.constraint_name,
            .start_encoded_value = request.right_start_encoded_value,
        })) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, right);
        if (!uniqueConstraintRangesAdjacent(left, right)) return error.UniqueConstraintRangesNotAdjacent;
        if (!std.mem.eql(u8, left.state, metadata_table_manager.unique_constraint_range_merging) or !std.mem.eql(u8, right.state, metadata_table_manager.unique_constraint_range_merging)) return error.UniqueConstraintRangeNotMerging;
        if (request.merged_group_id != left.group_id and request.merged_group_id != right.group_id and try self.uniqueConstraintRangeGroupExistsTxn(txn, group_id, request.merged_group_id)) return error.UniqueConstraintRangeGroupCollision;

        try self.deleteUniqueConstraintRangeTxn(txn, group_id, left);
        try self.deleteUniqueConstraintRangeTxn(txn, group_id, right);
        try self.putUniqueConstraintRangeTxn(txn, group_id, .{
            .table_id = left.table_id,
            .constraint_name = left.constraint_name,
            .start_encoded_value = left.start_encoded_value,
            .end_encoded_value = right.end_encoded_value,
            .group_id = request.merged_group_id,
            .topology_epoch = @max(left.topology_epoch, right.topology_epoch) +% 1,
            .state = metadata_table_manager.unique_constraint_range_active,
        });
    }

    fn applyUniqueConstraintRangeSetStateTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        selector: metadata_table_manager.UniqueConstraintRangeSelector,
        expected_state: []const u8,
        next_state: []const u8,
    ) !void {
        var record = (try self.loadUniqueConstraintRangeTxn(txn, group_id, selector)) orelse return error.UnknownUniqueConstraintRange;
        defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, record);
        if (!std.mem.eql(u8, record.state, expected_state)) {
            if (std.mem.eql(u8, expected_state, metadata_table_manager.unique_constraint_range_active)) return error.UniqueConstraintRangeNotActive;
            if (std.mem.eql(u8, expected_state, metadata_table_manager.unique_constraint_range_rebuilding)) return error.UniqueConstraintRangeNotRebuilding;
            return error.InvalidUniqueConstraintRangeState;
        }
        try replaceOwnedString(self.alloc, &record.state, next_state);
        record.topology_epoch +%= 1;
        try self.putUniqueConstraintRangeTxn(txn, group_id, record);
    }

    fn loadUniqueConstraintRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        selector: metadata_table_manager.UniqueConstraintRangeSelector,
    ) !?metadata.UniqueConstraintRangeRecord {
        var key_buf: [512]u8 = undefined;
        const key = try uniqueConstraintRangeKeyForGroup(&key_buf, group_id, selector.table_id, selector.constraint_name, selector.start_encoded_value);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeUniqueConstraintRangeRecord(self.alloc, encoded);
    }

    fn putUniqueConstraintRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.UniqueConstraintRangeRecord,
    ) !void {
        var key_buf: [512]u8 = undefined;
        const key = try uniqueConstraintRangeKeyForGroup(&key_buf, group_id, record.table_id, record.constraint_name, record.start_encoded_value);
        const value = try encodeUniqueConstraintRangeRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyProjectionListeners(.{
            .kind = .unique_constraint_range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = record.table_id,
            .group_id = record.group_id,
        });
    }

    fn deleteUniqueConstraintRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.UniqueConstraintRangeRecord,
    ) !void {
        var key_buf: [512]u8 = undefined;
        const key = try uniqueConstraintRangeKeyForGroup(&key_buf, group_id, record.table_id, record.constraint_name, record.start_encoded_value);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyProjectionListeners(.{
            .kind = .unique_constraint_range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = record.table_id,
            .group_id = record.group_id,
        });
    }

    fn uniqueConstraintRangeGroupExistsTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        owner_group_id: u64,
    ) !bool {
        var range_key_buf: [160]u8 = undefined;
        const range_key = try rangeKeyForGroup(&range_key_buf, group_id, owner_group_id);
        if (txn.get(range_key)) |_| return true else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }

        var prefix_buf: [128]u8 = undefined;
        const prefix = try uniqueConstraintRangePrefixForGroup(&prefix_buf, group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const record = try decodeUniqueConstraintRangeRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeUniqueConstraintRange(self.alloc, record);
            if (record.group_id == owner_group_id) return true;
        }
        return false;
    }

    fn applySecondaryIndexRebuildRangeBeginTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest,
    ) !void {
        var record = (try self.loadSecondaryIndexRebuildRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownSecondaryIndexRebuildRange;
        defer metadata_table_manager.freeSecondaryIndexRebuildRange(self.alloc, record);
        const claimable = std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_declared) or
            (std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_building) and record.lease_expires_at_ms != 0 and record.lease_expires_at_ms <= request.now_ms);
        if (!claimable) {
            if (std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_building)) return error.SecondaryIndexRebuildRangeClaimBusy;
            return error.SecondaryIndexRebuildRangeNotDeclared;
        }
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.secondary_index_rebuild_building);
        try replaceOwnedString(self.alloc, &record.lease_owner, request.lease_owner);
        record.lease_expires_at_ms = request.lease_expires_at_ms;
        record.attempts +%= 1;
        record.topology_epoch +%= 1;
        try self.putSecondaryIndexRebuildRangeTxn(txn, group_id, record);
    }

    fn applySecondaryIndexRebuildRangeFinishTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest,
    ) !void {
        var record = (try self.loadSecondaryIndexRebuildRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownSecondaryIndexRebuildRange;
        defer metadata_table_manager.freeSecondaryIndexRebuildRange(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_building)) return error.SecondaryIndexRebuildRangeNotBuilding;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.secondary_index_rebuild_ready);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        record.completed_row_count = request.completed_row_count;
        try replaceOwnedString(self.alloc, &record.progress_row_key, request.progress_row_key);
        try replaceOwnedString(self.alloc, &record.last_error, "");
        record.topology_epoch +%= 1;
        try self.putSecondaryIndexRebuildRangeTxn(txn, group_id, record);
    }

    fn applySecondaryIndexRebuildRangeInvalidateTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest,
    ) !void {
        var record = (try self.loadSecondaryIndexRebuildRangeTxn(txn, group_id, request.selector)) orelse return error.UnknownSecondaryIndexRebuildRange;
        defer metadata_table_manager.freeSecondaryIndexRebuildRange(self.alloc, record);
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.secondary_index_rebuild_invalid);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.last_error);
        record.topology_epoch +%= 1;
        try self.putSecondaryIndexRebuildRangeTxn(txn, group_id, record);
    }

    fn loadSecondaryIndexRebuildRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector,
    ) !?metadata.SecondaryIndexRebuildRangeRecord {
        var key_buf: [512]u8 = undefined;
        const key = try secondaryIndexRebuildRangeKeyForGroup(&key_buf, group_id, selector.table_id, selector.index_name, selector.index_generation, selector.start_row_key);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeSecondaryIndexRebuildRangeRecord(self.alloc, encoded);
    }

    fn putSecondaryIndexRebuildRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.SecondaryIndexRebuildRangeRecord,
    ) !void {
        var key_buf: [512]u8 = undefined;
        const key = try secondaryIndexRebuildRangeKeyForGroup(&key_buf, group_id, record.table_id, record.index_name, record.index_generation, record.start_row_key);
        const value = try encodeSecondaryIndexRebuildRangeRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyProjectionListeners(.{
            .kind = .secondary_index_rebuild_range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = record.table_id,
            .group_id = record.group_id,
        });
    }

    fn deleteSecondaryIndexRebuildRangeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector,
        owner_group_id: u64,
    ) !void {
        var key_buf: [512]u8 = undefined;
        const key = try secondaryIndexRebuildRangeKeyForGroup(&key_buf, group_id, selector.table_id, selector.index_name, selector.index_generation, selector.start_row_key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        const table_name = try self.lookupTableNameTxn(txn, group_id, selector.table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyProjectionListeners(.{
            .kind = .secondary_index_rebuild_range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = selector.table_id,
            .group_id = owner_group_id,
        });
    }

    fn applySchemaRewriteJobBeginTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobBeginRequest,
    ) !void {
        if (request.lease_owner.len == 0 or request.lease_expires_at_ms <= request.now_ms) return error.InvalidSchemaRewriteJobLease;
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        const claimable = std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_declared) or
            (std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running) and record.lease_expires_at_ms != 0 and record.lease_expires_at_ms <= request.now_ms);
        if (!claimable) {
            if (std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running)) return error.SchemaRewriteJobClaimBusy;
            return error.SchemaRewriteJobNotDeclared;
        }
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_running);
        try replaceOwnedString(self.alloc, &record.lease_owner, request.lease_owner);
        record.lease_expires_at_ms = request.lease_expires_at_ms;
        record.attempts +%= 1;
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn applySchemaRewriteJobFinishTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobFinishRequest,
    ) !void {
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running)) return error.SchemaRewriteJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.SchemaRewriteJobLeaseMismatch;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_ready);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        record.completed_row_count = request.completed_row_count;
        try replaceOwnedString(self.alloc, &record.progress_row_key, request.progress_row_key);
        try replaceOwnedString(self.alloc, &record.last_error, "");
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn applySchemaRewriteJobInvalidateTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobInvalidateRequest,
    ) !void {
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running)) return error.SchemaRewriteJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.SchemaRewriteJobLeaseMismatch;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_invalid);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.last_error);
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn applySchemaRewriteJobPauseTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobControlRequest,
    ) !void {
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_declared) and
            !std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running))
        {
            return error.SchemaRewriteJobNotDeclared;
        }
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_paused);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn applySchemaRewriteJobResumeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobControlRequest,
    ) !void {
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_paused)) return error.SchemaRewriteJobNotPaused;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_declared);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn applySchemaRewriteJobRetryTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobControlRequest,
    ) !void {
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_invalid)) return error.SchemaRewriteJobNotInvalid;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_declared);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn applySchemaRewriteJobCancelTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SchemaRewriteJobControlRequest,
    ) !void {
        var record = (try self.loadSchemaRewriteJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownSchemaRewriteJob;
        defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
        if (std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_ready)) return error.SchemaRewriteJobComplete;
        if (std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_canceled)) return error.SchemaRewriteJobCanceled;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.schema_rewrite_canceled);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putSchemaRewriteJobTxn(txn, group_id, record);
    }

    fn loadSchemaRewriteJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        job_id: u64,
    ) !?metadata.SchemaRewriteJobRecord {
        var key_buf: [160]u8 = undefined;
        const key = try schemaRewriteJobKeyForGroup(&key_buf, group_id, job_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeSchemaRewriteJobRecord(self.alloc, encoded);
    }

    fn putSchemaRewriteJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.SchemaRewriteJobRecord,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try schemaRewriteJobKeyForGroup(&key_buf, group_id, record.job_id);
        const value = try encodeSchemaRewriteJobRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .schema_rewrite_job,
            .metadata_group_id = group_id,
            .table_id = record.table_id,
        });
    }

    fn deleteSchemaRewriteJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        job_id: u64,
    ) !void {
        const existing = (try self.loadSchemaRewriteJobTxn(txn, group_id, job_id)) orelse null;
        defer if (existing) |record_existing| metadata_table_manager.freeSchemaRewriteJob(self.alloc, record_existing);
        var key_buf: [160]u8 = undefined;
        const key = try schemaRewriteJobKeyForGroup(&key_buf, group_id, job_id);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .schema_rewrite_job,
            .metadata_group_id = group_id,
            .table_id = if (existing) |record_existing| record_existing.table_id else 0,
        });
    }

    fn applyTableEmptyingJobBeginTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobBeginRequest,
    ) !void {
        if (request.lease_owner.len == 0 or request.lease_expires_at_ms <= request.now_ms) return error.InvalidTableEmptyingJobLease;
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        const claimable = std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_declared) or
            (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running) and record.lease_expires_at_ms != 0 and record.lease_expires_at_ms <= request.now_ms);
        if (!claimable) {
            if (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running)) return error.TableEmptyingJobClaimBusy;
            return error.TableEmptyingJobNotDeclared;
        }
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_running);
        try replaceOwnedString(self.alloc, &record.lease_owner, request.lease_owner);
        record.lease_expires_at_ms = request.lease_expires_at_ms;
        record.attempts +%= 1;
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn applyTableEmptyingJobFinishTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobFinishRequest,
    ) !void {
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running)) return error.TableEmptyingJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.TableEmptyingJobLeaseMismatch;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_ready);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        record.completed_row_count = request.completed_row_count;
        try replaceOwnedString(self.alloc, &record.progress_row_key, request.progress_row_key);
        try replaceOwnedString(self.alloc, &record.last_error, "");
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn applyTableEmptyingJobInvalidateTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobInvalidateRequest,
    ) !void {
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running)) return error.TableEmptyingJobNotRunning;
        if (request.lease_owner.len == 0 or !std.mem.eql(u8, record.lease_owner, request.lease_owner)) return error.TableEmptyingJobLeaseMismatch;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_invalid);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.last_error);
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn applyTableEmptyingJobPauseTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobControlRequest,
    ) !void {
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_declared) and
            !std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running))
        {
            return error.TableEmptyingJobNotDeclared;
        }
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_paused);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn applyTableEmptyingJobResumeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobControlRequest,
    ) !void {
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_paused)) return error.TableEmptyingJobNotPaused;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_declared);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn applyTableEmptyingJobRetryTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobControlRequest,
    ) !void {
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        if (!std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_invalid)) return error.TableEmptyingJobNotInvalid;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_declared);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn applyTableEmptyingJobCancelTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingJobControlRequest,
    ) !void {
        var record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_id)) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
        if (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_ready)) return error.TableEmptyingJobComplete;
        if (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_canceled)) return error.TableEmptyingJobCanceled;
        try replaceOwnedString(self.alloc, &record.state, metadata_table_manager.table_emptying_canceled);
        try replaceOwnedString(self.alloc, &record.lease_owner, "");
        record.lease_expires_at_ms = 0;
        try replaceOwnedString(self.alloc, &record.last_error, request.reason);
        try self.putTableEmptyingJobTxn(txn, group_id, record);
    }

    fn loadTableEmptyingJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        job_id: u64,
    ) !?metadata.TableEmptyingJobRecord {
        var key_buf: [160]u8 = undefined;
        const key = try tableEmptyingJobKeyForGroup(&key_buf, group_id, job_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeTableEmptyingJobRecord(self.alloc, encoded);
    }

    fn putTableEmptyingJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.TableEmptyingJobRecord,
    ) !void {
        if (record.job_id == 0) return error.InvalidTableEmptyingJob;
        if (record.schema_generation == 0) return error.InvalidTableEmptyingGeneration;
        if (!metadata_table_manager.tableEmptyingJobStateValid(record.state)) return error.InvalidTableEmptyingJobState;
        if (record.end_row_key) |end_row_key| {
            if (std.mem.order(u8, record.start_row_key, end_row_key) != .lt) return error.InvalidTableEmptyingJob;
        }
        if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalValid(record.table_id, record.affected_table_ids)) {
            return error.InvalidTableEmptyingJob;
        }
        const table = (try self.loadTableRecordTxn(txn, group_id, record.table_id)) orelse return error.UnknownTable;
        metadata_table_manager.freeTable(self.alloc, table);
        for (record.affected_table_ids) |table_id| {
            const affected = (try self.loadTableRecordTxn(txn, group_id, table_id)) orelse return error.UnknownTable;
            metadata_table_manager.freeTable(self.alloc, affected);
        }
        var key_buf: [160]u8 = undefined;
        const key = try tableEmptyingJobKeyForGroup(&key_buf, group_id, record.job_id);
        const value = try encodeTableEmptyingJobRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table_emptying_job,
            .metadata_group_id = group_id,
            .table_id = record.table_id,
            .group_id = record.group_id,
        });
    }

    fn deleteTableEmptyingJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        job_id: u64,
    ) !void {
        const existing = (try self.loadTableEmptyingJobTxn(txn, group_id, job_id)) orelse null;
        defer if (existing) |record_existing| metadata_table_manager.freeTableEmptyingJob(self.alloc, record_existing);
        var key_buf: [160]u8 = undefined;
        const key = try tableEmptyingJobKeyForGroup(&key_buf, group_id, job_id);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table_emptying_job,
            .metadata_group_id = group_id,
            .table_id = if (existing) |record_existing| record_existing.table_id else 0,
            .group_id = if (existing) |record_existing| record_existing.group_id else 0,
        });
    }

    fn tableEmptyingPromotionJobIdsContain(job_ids: []const u64, needle: u64) bool {
        for (job_ids) |job_id| {
            if (job_id == needle) return true;
        }
        return false;
    }

    fn tableEmptyingPromotionAffectedTablesContain(table_ids: []const u64, needle: u64) bool {
        for (table_ids) |table_id| {
            if (table_id == needle) return true;
        }
        return false;
    }

    fn tableEmptyingPromotionU64SlicesEqual(a: []const u64, b: []const u64) bool {
        if (a.len != b.len) return false;
        for (a, b) |left, right| {
            if (left != right) return false;
        }
        return true;
    }

    fn tableEmptyingPromotionOptionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
        if (a == null and b == null) return true;
        if (a == null or b == null) return false;
        return std.mem.eql(u8, a.?, b.?);
    }

    fn tableEmptyingPromotionHasReadyRangeJobTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        barrier: metadata.TableEmptyingJobRecord,
        job_ids: []const u64,
        table: metadata.TableRecord,
        range: metadata.RangeRecord,
    ) !bool {
        const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
        for (job_ids) |job_id| {
            const maybe_record = try self.loadTableEmptyingJobTxn(txn, group_id, job_id);
            const record = maybe_record orelse continue;
            defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
            if (!std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_ready)) continue;
            if (record.barrier_id != barrier.barrier_id) continue;
            if (record.table_id != table.table_id) continue;
            if (record.group_id != range.group_id) continue;
            if (record.range_id != 0 and record.range_id != range.range_id) continue;
            if (record.schema_generation != schema_generation) continue;
            if (record.data_generation != table.data_generation) continue;
            if (!std.mem.eql(u8, record.start_row_key, range.start_key)) continue;
            if (!tableEmptyingPromotionOptionalStringsEqual(record.end_row_key, range.end_key)) continue;
            if (!tableEmptyingPromotionU64SlicesEqual(record.affected_table_ids, barrier.affected_table_ids)) continue;
            if (record.restart_identity != barrier.restart_identity) continue;
            if (record.cascade != barrier.cascade) continue;
            return true;
        }
        return false;
    }

    fn validateCompleteTableEmptyingBarrierTableTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        barrier: metadata.TableEmptyingJobRecord,
        job_ids: []const u64,
        table_id: u64,
    ) !void {
        const table = (try self.loadTableRecordTxn(txn, group_id, table_id)) orelse return error.UnknownTable;
        defer metadata_table_manager.freeTable(self.alloc, table);

        var range_count: usize = 0;
        var prefix_buf: [128]u8 = undefined;
        const prefix = try rangePrefixForGroup(&prefix_buf, group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const range = try decodeRangeRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeRange(self.alloc, range);
            if (range.table_id != table_id) continue;
            range_count += 1;
            if (!try self.tableEmptyingPromotionHasReadyRangeJobTxn(txn, group_id, barrier, job_ids, table, range)) {
                return error.InvalidTableEmptyingBarrierPromotion;
            }
        }
        if (range_count == 0) return error.InvalidTableEmptyingBarrierPromotion;
    }

    fn tableEmptyingIdentityAllocatorResetTargetsTxnAlloc(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingIdentityAllocatorResetRequest,
    ) ![]metadata_table_manager.SequenceIdentityAllocatorReset {
        var manager = metadata_table_manager.TableManager.init(alloc);
        defer manager.deinit();

        for (request.affected_table_ids) |table_id| {
            const table = (try self.loadTableRecordTxn(txn, group_id, table_id)) orelse return error.UnknownTable;
            defer metadata_table_manager.freeTable(self.alloc, table);
            try manager.upsertTable(table);
        }

        var range_prefix_buf: [128]u8 = undefined;
        const range_prefix = try rangePrefixForGroup(&range_prefix_buf, group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var range_entry = try cur.seekAtOrAfter(range_prefix);
        while (range_entry) |kv| : (range_entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, range_prefix)) break;
            const range = try decodeRangeRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeRange(self.alloc, range);
            if (!tableEmptyingPromotionAffectedTablesContain(request.affected_table_ids, range.table_id)) continue;
            try manager.upsertRange(range);
        }

        for (request.job_ids) |job_id| {
            const record = (try self.loadTableEmptyingJobTxn(txn, group_id, job_id)) orelse return error.UnknownTableEmptyingJob;
            defer metadata_table_manager.freeTableEmptyingJob(self.alloc, record);
            try manager.upsertTableEmptyingJob(record);
        }

        var sequence_prefix_buf: [128]u8 = undefined;
        const sequence_prefix = try sequencePrefixForGroup(&sequence_prefix_buf, group_id);
        var sequence_entry = try cur.seekAtOrAfter(sequence_prefix);
        while (sequence_entry) |kv| : (sequence_entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, sequence_prefix)) break;
            const sequence = try decodeSequenceRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeSequence(self.alloc, sequence);
            try manager.upsertSequence(sequence);
        }

        return try manager.tableEmptyingIdentityAllocatorResetTargetsAlloc(alloc, request);
    }

    fn resetIdentityAllocatorsForTableEmptyingBarrierTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingIdentityAllocatorResetRequest,
    ) !void {
        const targets = try self.tableEmptyingIdentityAllocatorResetTargetsTxnAlloc(self.alloc, txn, group_id, request);
        defer self.alloc.free(targets);

        for (targets) |target| {
            var key_buf: [160]u8 = undefined;
            const key = try sequenceKeyForGroup(&key_buf, group_id, target.sequence_id);
            const encoded = txn.get(key) catch |err| switch (err) {
                error.NotFound => return error.SequenceNotFound,
                else => return err,
            };
            var current = try decodeSequenceRecord(self.alloc, encoded);
            defer metadata_table_manager.freeSequence(self.alloc, current);
            if (current.last_value == target.reset_last_value and current.last_allocation_id == 0) continue;
            current.last_value = target.reset_last_value;
            current.last_allocation_id = 0;
            const value = try encodeSequenceRecord(self.alloc, current);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
    }

    fn applyTableEmptyingBarrierPromotionTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableEmptyingBarrierPromotionRequest,
    ) !void {
        if (request.job_ids.len == 0 or request.promotions.len == 0) return error.InvalidTableEmptyingBarrierPromotion;

        const first_record = (try self.loadTableEmptyingJobTxn(txn, group_id, request.job_ids[0])) orelse return error.UnknownTableEmptyingJob;
        defer metadata_table_manager.freeTableEmptyingJob(self.alloc, first_record);
        if (first_record.barrier_id == 0 or first_record.affected_table_ids.len == 0) return error.InvalidTableEmptyingBarrierPromotion;
        if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalValid(first_record.table_id, first_record.affected_table_ids)) return error.InvalidTableEmptyingBarrierPromotion;
        if (request.promotions.len != first_record.affected_table_ids.len) return error.InvalidTableEmptyingBarrierPromotion;

        for (request.job_ids, 0..) |job_id, i| {
            if (job_id == 0) return error.InvalidTableEmptyingBarrierPromotion;
            for (request.job_ids[0..i]) |previous| {
                if (previous == job_id) return error.InvalidTableEmptyingBarrierPromotion;
            }
            const job = (try self.loadTableEmptyingJobTxn(txn, group_id, job_id)) orelse return error.UnknownTableEmptyingJob;
            defer metadata_table_manager.freeTableEmptyingJob(self.alloc, job);
            if (!std.mem.eql(u8, job.state, metadata_table_manager.table_emptying_ready)) return error.TableEmptyingJobNotReady;
            if (job.barrier_id != first_record.barrier_id or
                job.restart_identity != first_record.restart_identity or
                job.cascade != first_record.cascade or
                !tableEmptyingPromotionU64SlicesEqual(job.affected_table_ids, first_record.affected_table_ids) or
                !tableEmptyingPromotionAffectedTablesContain(first_record.affected_table_ids, job.table_id))
            {
                return error.InvalidTableEmptyingBarrierPromotion;
            }
        }

        for (request.promotions, 0..) |promotion, i| {
            if (promotion.table_id == 0 or promotion.target_generation == 0) return error.InvalidTableEmptyingBarrierPromotion;
            if (!tableEmptyingPromotionAffectedTablesContain(first_record.affected_table_ids, promotion.table_id)) return error.InvalidTableEmptyingBarrierPromotion;
            for (request.promotions[0..i]) |previous| {
                if (previous.table_id == promotion.table_id) return error.InvalidTableEmptyingBarrierPromotion;
            }
            const table = (try self.loadTableRecordTxn(txn, group_id, promotion.table_id)) orelse return error.UnknownTable;
            defer metadata_table_manager.freeTable(self.alloc, table);
            if (promotion.target_generation != table.data_generation +| 1) return error.InvalidTableEmptyingBarrierPromotion;
        }
        for (first_record.affected_table_ids) |table_id| {
            var has_promotion = false;
            for (request.promotions) |promotion| {
                if (promotion.table_id == table_id) {
                    has_promotion = true;
                    break;
                }
            }
            if (!has_promotion) return error.InvalidTableEmptyingBarrierPromotion;
            try self.validateCompleteTableEmptyingBarrierTableTxn(txn, group_id, first_record, request.job_ids, table_id);
        }

        if (first_record.restart_identity) {
            try self.resetIdentityAllocatorsForTableEmptyingBarrierTxn(txn, group_id, .{
                .barrier_id = first_record.barrier_id,
                .affected_table_ids = first_record.affected_table_ids,
                .job_ids = request.job_ids,
                .cascade = first_record.cascade,
            });
        }

        for (request.promotions) |promotion| {
            var key_buf: [160]u8 = undefined;
            const key = try tableKeyForGroup(&key_buf, group_id, promotion.table_id);
            const encoded = txn.get(key) catch |err| switch (err) {
                error.NotFound => return error.UnknownTable,
                else => return err,
            };
            var table = try decodeTableRecord(self.alloc, encoded);
            defer metadata_table_manager.freeTable(self.alloc, table);
            if (table.data_generation >= promotion.target_generation) continue;
            table.data_generation = promotion.target_generation;
            const updated = try encodeTableRecord(self.alloc, table);
            defer self.alloc.free(updated);
            try txn.put(key, updated);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .table,
                .metadata_group_id = group_id,
                .table_id = table.table_id,
                .group_id = 0,
            });
        }

        for (request.job_ids) |job_id| {
            try self.deleteTableEmptyingJobTxn(txn, group_id, job_id);
        }
    }

    fn collectCompletedSchemaRewriteJobIdsForTableGenerationTxn(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        table_id: u64,
        schema_generation: u64,
    ) ![]u64 {
        var job_ids = std.ArrayListUnmanaged(u64).empty;
        errdefer job_ids.deinit(alloc);
        var prefix_buf: [128]u8 = undefined;
        const prefix = try schemaRewriteJobPrefixForGroup(&prefix_buf, group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const record = try decodeSchemaRewriteJobRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeSchemaRewriteJob(self.alloc, record);
            if (record.table_id != table_id) continue;
            if (record.schema_generation != schema_generation) continue;
            if (!metadata_table_manager.schemaRewriteJobComplete(record)) return error.SchemaRewriteJobsIncomplete;
            try job_ids.append(alloc, record.job_id);
        }
        return try job_ids.toOwnedSlice(alloc);
    }

    fn applySecondaryIndexReadyPromotionTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SecondaryIndexReadyPromotionRequest,
    ) !void {
        if (request.table_id == 0 or request.promoted_table.table_id != request.table_id) return error.InvalidSecondaryIndexPromotionRequest;
        if (request.index_name.len == 0 or request.expected_index_generation == 0) return error.InvalidSecondaryIndexPromotionRequest;

        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, request.table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return error.UnknownTable,
            else => return err,
        };
        const current = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, current);

        if (!std.mem.eql(u8, current.name, request.promoted_table.name)) return error.InvalidSecondaryIndexPromotionRequest;
        if (!std.mem.eql(u8, current.schema_json, request.expected_schema_json)) return;

        const value = try encodeTableRecord(self.alloc, request.promoted_table);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table,
            .metadata_group_id = group_id,
            .table_name = request.promoted_table.name,
            .table_id = request.table_id,
        });
    }

    fn applyTableSchemaCompareAndSwapTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.TableSchemaCompareAndSwapRequest,
    ) !void {
        if (request.table_id == 0 or request.promoted_table.table_id != request.table_id) return error.InvalidTableSchemaCompareAndSwapRequest;

        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, request.table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return error.UnknownTable,
            else => return err,
        };
        const current = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, current);

        if (!std.mem.eql(u8, current.name, request.promoted_table.name)) return error.InvalidTableSchemaCompareAndSwapRequest;
        if (!std.mem.eql(u8, current.schema_json, request.expected_schema_json)) return;
        const completed_rewrite_job_ids = try self.collectCompletedSchemaRewriteJobIdsForTableGenerationTxn(
            self.alloc,
            txn,
            group_id,
            request.table_id,
            metadata_table_manager.schemaRewriteGenerationForSchemaJson(request.expected_schema_json),
        );
        defer if (completed_rewrite_job_ids.len > 0) self.alloc.free(completed_rewrite_job_ids);

        const value = try encodeTableRecord(self.alloc, request.promoted_table);
        defer self.alloc.free(value);
        try txn.put(key, value);
        for (completed_rewrite_job_ids) |job_id| try self.deleteSchemaRewriteJobTxn(txn, group_id, job_id);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table,
            .metadata_group_id = group_id,
            .table_name = request.promoted_table.name,
            .table_id = request.table_id,
        });
    }

    fn compareAndSwapSequenceTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        request: metadata_table_manager.SequenceCompareAndSwapRequest,
    ) !void {
        if (request.sequence_id == 0) return error.InvalidSequenceCatalog;
        var key_buf: [160]u8 = undefined;
        const key = try sequenceKeyForGroup(&key_buf, group_id, request.sequence_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return error.SequenceNotFound,
            else => return err,
        };
        var current = try decodeSequenceRecord(self.alloc, encoded);
        defer metadata_table_manager.freeSequence(self.alloc, current);
        if (current.last_value != request.expected_last_value) return;
        current.last_value = request.next_last_value;
        current.last_allocation_id = request.allocation_id;
        const value = try encodeSequenceRecord(self.alloc, current);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
    }

    fn notifyProjectionListeners(self: *RaftApplyStore, signal: ProjectionSignal) void {
        for (self.projection_listeners.items) |listener| listener.onProjectionSignal(signal);
    }

    fn notifyCommittedKeyListeners(self: *RaftApplyStore, signal: CommittedKeySignal) void {
        for (self.committed_key_listeners.items) |listener| listener.onCommittedKey(signal);
    }

    fn lookupTableName(self: *RaftApplyStore, group_id: u64, table_id: u64) !?[]u8 {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return try self.alloc.dupe(u8, table.name);
    }

    fn lookupTableNameTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64) !?[]u8 {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return try self.alloc.dupe(u8, table.name);
    }

    fn ensureTableCatalogIdentityTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.TableRecord,
    ) !void {
        const database_id = metadata_table_manager.deriveDatabaseId(record.database_name);
        var database_key_buf: [160]u8 = undefined;
        const database_key = try databaseKeyForGroup(&database_key_buf, group_id, database_id);
        const database_missing = if (txn.get(database_key)) |_| false else |err| switch (err) {
            error.NotFound => true,
            else => return err,
        };
        if (database_missing) {
            const value = try encodeDatabaseRecord(self.alloc, .{
                .database_id = database_id,
                .name = record.database_name,
            });
            defer self.alloc.free(value);
            try txn.put(database_key, value);
        }

        const namespace_id = metadata_table_manager.deriveNamespaceId(database_id, record.namespace_name);
        var namespace_key_buf: [160]u8 = undefined;
        const namespace_key = try namespaceKeyForGroup(&namespace_key_buf, group_id, namespace_id);
        const namespace_missing = if (txn.get(namespace_key)) |_| false else |err| switch (err) {
            error.NotFound => true,
            else => return err,
        };
        if (namespace_missing) {
            const value = try encodeNamespaceRecord(self.alloc, .{
                .namespace_id = namespace_id,
                .database_id = database_id,
                .name = record.namespace_name,
            });
            defer self.alloc.free(value);
            try txn.put(namespace_key, value);
        }
    }
};

const transition_magic = "afmd1";
const runtime_status_record_version: u16 = 4;

fn freeForeignKeyReferenceRangeSelector(alloc: std.mem.Allocator, selector: metadata_table_manager.ForeignKeyReferenceRangeSelector) void {
    alloc.free(selector.constraint_name);
    alloc.free(selector.start_parent_key);
}

fn freeForeignKeyReferenceRangeSplitRequest(alloc: std.mem.Allocator, request: metadata_table_manager.ForeignKeyReferenceRangeSplitRequest) void {
    freeForeignKeyReferenceRangeSelector(alloc, request.selector);
    alloc.free(request.split_parent_key);
}

fn freeForeignKeyReferenceRangeMergeRequest(alloc: std.mem.Allocator, request: metadata_table_manager.ForeignKeyReferenceRangeMergeRequest) void {
    freeForeignKeyReferenceRangeSelector(alloc, request.left_selector);
    alloc.free(request.right_start_parent_key);
}

fn freeUniqueConstraintRangeSelector(alloc: std.mem.Allocator, selector: metadata_table_manager.UniqueConstraintRangeSelector) void {
    alloc.free(selector.constraint_name);
    alloc.free(selector.start_encoded_value);
}

fn freeUniqueConstraintRangeSplitRequest(alloc: std.mem.Allocator, request: metadata_table_manager.UniqueConstraintRangeSplitRequest) void {
    freeUniqueConstraintRangeSelector(alloc, request.selector);
    alloc.free(request.split_encoded_value);
}

fn freeUniqueConstraintRangeMergeRequest(alloc: std.mem.Allocator, request: metadata_table_manager.UniqueConstraintRangeMergeRequest) void {
    freeUniqueConstraintRangeSelector(alloc, request.left_selector);
    alloc.free(request.right_start_encoded_value);
}

fn freeSecondaryIndexRebuildRangeSelector(alloc: std.mem.Allocator, selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector) void {
    alloc.free(selector.index_name);
    alloc.free(selector.start_row_key);
}

fn freeSecondaryIndexRebuildRangeBeginRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest) void {
    freeSecondaryIndexRebuildRangeSelector(alloc, request.selector);
    alloc.free(request.lease_owner);
}

fn freeSecondaryIndexRebuildRangeFinishRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest) void {
    freeSecondaryIndexRebuildRangeSelector(alloc, request.selector);
    alloc.free(request.progress_row_key);
}

fn freeSecondaryIndexRebuildRangeInvalidateRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest) void {
    freeSecondaryIndexRebuildRangeSelector(alloc, request.selector);
    alloc.free(request.last_error);
}

fn freeSchemaRewriteJobBeginRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SchemaRewriteJobBeginRequest) void {
    alloc.free(request.lease_owner);
}

fn freeSchemaRewriteJobFinishRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SchemaRewriteJobFinishRequest) void {
    alloc.free(request.lease_owner);
    alloc.free(request.progress_row_key);
}

fn freeSchemaRewriteJobInvalidateRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) void {
    alloc.free(request.lease_owner);
    alloc.free(request.last_error);
}

fn freeSchemaRewriteJobControlRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SchemaRewriteJobControlRequest) void {
    alloc.free(request.reason);
}

fn freeTableCatalogUpdateWithSchemaRewriteJobsRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest) void {
    metadata_table_manager.freeTable(alloc, request.table);
    for (request.schema_rewrite_jobs) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
    if (request.schema_rewrite_jobs.len > 0) alloc.free(request.schema_rewrite_jobs);
}

fn freeTableCatalogBatchUpdateWithSchemaRewriteJobsRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest) void {
    for (request.tables) |record| metadata_table_manager.freeTable(alloc, record);
    if (request.tables.len > 0) alloc.free(request.tables);
    for (request.schema_rewrite_jobs) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
    if (request.schema_rewrite_jobs.len > 0) alloc.free(request.schema_rewrite_jobs);
}

fn freeTableCatalogDropWithSchemaRewriteJobsRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest) void {
    if (request.sequence_ids.len > 0) alloc.free(request.sequence_ids);
    if (request.range_group_ids.len > 0) alloc.free(request.range_group_ids);
    for (request.table_updates) |record| metadata_table_manager.freeTable(alloc, record);
    if (request.table_updates.len > 0) alloc.free(request.table_updates);
    for (request.schema_rewrite_jobs) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
    if (request.schema_rewrite_jobs.len > 0) alloc.free(request.schema_rewrite_jobs);
}

fn freeTableEmptyingJobBeginRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableEmptyingJobBeginRequest) void {
    alloc.free(request.lease_owner);
}

fn freeTableEmptyingJobFinishRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableEmptyingJobFinishRequest) void {
    alloc.free(request.lease_owner);
    alloc.free(request.progress_row_key);
}

fn freeTableEmptyingJobInvalidateRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) void {
    alloc.free(request.lease_owner);
    alloc.free(request.last_error);
}

fn freeTableEmptyingJobControlRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableEmptyingJobControlRequest) void {
    alloc.free(request.reason);
}

fn freeTableEmptyingBarrierPromotionRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableEmptyingBarrierPromotionRequest) void {
    if (request.job_ids.len > 0) alloc.free(request.job_ids);
    if (request.promotions.len > 0) alloc.free(request.promotions);
}

fn freeSecondaryIndexReadyPromotionRequest(alloc: std.mem.Allocator, request: metadata_table_manager.SecondaryIndexReadyPromotionRequest) void {
    alloc.free(request.index_name);
    alloc.free(request.expected_schema_json);
    metadata_table_manager.freeTable(alloc, request.promoted_table);
}

fn freeTableSchemaCompareAndSwapRequest(alloc: std.mem.Allocator, request: metadata_table_manager.TableSchemaCompareAndSwapRequest) void {
    alloc.free(request.expected_schema_json);
    metadata_table_manager.freeTable(alloc, request.promoted_table);
}

fn replaceOwnedString(alloc: std.mem.Allocator, target: *[]const u8, replacement: []const u8) !void {
    const owned = try alloc.dupe(u8, replacement);
    alloc.free(target.*);
    target.* = owned;
}

fn foreignKeyReferenceSplitKeyInsideRange(split_key: []const u8, start_parent_key: []const u8, end_parent_key: ?[]const u8) bool {
    if (std.mem.order(u8, start_parent_key, split_key) != .lt) return false;
    if (end_parent_key) |end| return std.mem.order(u8, split_key, end) == .lt;
    return true;
}

fn foreignKeyReferenceRangesAdjacent(left: metadata.ForeignKeyReferenceRangeRecord, right: metadata.ForeignKeyReferenceRangeRecord) bool {
    if (left.child_table_id != right.child_table_id) return false;
    if (left.parent_table_id != right.parent_table_id) return false;
    if (!std.mem.eql(u8, left.constraint_name, right.constraint_name)) return false;
    const left_end = left.end_parent_key orelse return false;
    return std.mem.eql(u8, left_end, right.start_parent_key);
}

fn uniqueConstraintSplitKeyInsideRange(split_key: []const u8, start_encoded_value: []const u8, end_encoded_value: ?[]const u8) bool {
    if (std.mem.order(u8, start_encoded_value, split_key) != .lt) return false;
    if (end_encoded_value) |end| return std.mem.order(u8, split_key, end) == .lt;
    return true;
}

fn uniqueConstraintRangesAdjacent(left: metadata.UniqueConstraintRangeRecord, right: metadata.UniqueConstraintRangeRecord) bool {
    if (left.table_id != right.table_id) return false;
    if (!std.mem.eql(u8, left.constraint_name, right.constraint_name)) return false;
    const left_end = left.end_encoded_value orelse return false;
    return std.mem.eql(u8, left_end, right.start_encoded_value);
}

const TransitionTag = enum(u8) {
    upsert_node = 1,
    remove_node = 2,
    upsert_store = 3,
    remove_store = 4,
    upsert_replica_intent = 5,
    remove_replica_intent = 6,
    upsert_table = 7,
    remove_table = 8,
    upsert_schema_progress = 9,
    remove_schema_progress = 10,
    upsert_restore_progress = 11,
    remove_restore_progress = 12,
    upsert_replication_source_status = 13,
    remove_replication_source_status = 14,
    upsert_range = 15,
    remove_range = 16,
    upsert_split_transition = 17,
    remove_split_transition = 18,
    upsert_merge_transition = 19,
    remove_merge_transition = 20,
    upsert_reconcile_lease = 21,
    remove_reconcile_lease = 22,
    upsert_shuffle_join_lease = 23,
    remove_shuffle_join_lease = 24,
    upsert_reallocation_request = 25,
    remove_reallocation_request = 26,
    request_node_shutdown = 27,
    cancel_node_shutdown = 28,
    register_node = 29,
    register_store = 30,
    finalize_node_shutdown = 31,
    upsert_extension_package = 32,
    remove_extension_package = 33,
    upsert_installed_extension = 34,
    remove_installed_extension = 35,
    upsert_extension_member = 36,
    remove_extension_member = 37,
    upsert_extension_dependency = 38,
    remove_extension_dependency = 39,
    apply_extension_lifecycle = 40,
    upsert_foreign_key_ref_range = 41,
    remove_foreign_key_ref_range = 42,
    begin_foreign_key_ref_range_split = 43,
    finish_foreign_key_ref_range_split = 44,
    begin_foreign_key_ref_range_merge = 45,
    finish_foreign_key_ref_range_merge = 46,
    begin_foreign_key_ref_range_rebuild = 47,
    finish_foreign_key_ref_range_rebuild = 48,
    upsert_unique_constraint_range = 49,
    remove_unique_constraint_range = 50,
    begin_unique_constraint_range_split = 51,
    finish_unique_constraint_range_split = 52,
    begin_unique_constraint_range_merge = 53,
    finish_unique_constraint_range_merge = 54,
    begin_unique_constraint_range_rebuild = 55,
    finish_unique_constraint_range_rebuild = 56,
    upsert_secondary_index_rebuild_range = 57,
    remove_secondary_index_rebuild_range = 58,
    begin_secondary_index_rebuild_range = 59,
    finish_secondary_index_rebuild_range = 60,
    invalidate_secondary_index_rebuild_range = 61,
    promote_secondary_index_ready = 62,
    compare_and_swap_table_schema = 63,
    upsert_database = 64,
    remove_database = 65,
    upsert_namespace = 66,
    remove_namespace = 67,
    upsert_tablespace = 68,
    remove_tablespace = 69,
    upsert_schema_rewrite_job = 70,
    remove_schema_rewrite_job = 71,
    begin_schema_rewrite_job = 72,
    finish_schema_rewrite_job = 73,
    invalidate_schema_rewrite_job = 74,
    upsert_table_emptying_job = 75,
    remove_table_emptying_job = 76,
    begin_table_emptying_job = 77,
    finish_table_emptying_job = 78,
    invalidate_table_emptying_job = 79,
    promote_table_emptying_barrier = 80,
    apply_table_catalog_update_with_schema_rewrite_jobs = 81,
    upsert_sequence = 82,
    remove_sequence = 83,
    compare_and_swap_sequence = 84,
    apply_table_catalog_batch_update_with_schema_rewrite_jobs = 85,
    apply_table_catalog_drop_with_schema_rewrite_jobs = 86,
    pause_schema_rewrite_job = 87,
    resume_schema_rewrite_job = 88,
    retry_schema_rewrite_job = 89,
    cancel_schema_rewrite_job = 90,
    pause_table_emptying_job = 91,
    resume_table_emptying_job = 92,
    retry_table_emptying_job = 93,
    cancel_table_emptying_job = 94,
};

pub fn encodeTransitionCommand(alloc: std.mem.Allocator, command: TransitionCommand) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, transition_magic);
    switch (command) {
        .upsert_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_node));
            try appendNodeRecord(alloc, &out, record);
        },
        .register_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.register_node));
            try appendNodeRecord(alloc, &out, record);
        },
        .remove_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_node));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .request_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.request_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .cancel_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.cancel_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .finalize_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.finalize_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .upsert_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_store));
            try appendStoreRecord(alloc, &out, record);
        },
        .register_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.register_store));
            try appendStoreRecord(alloc, &out, record);
        },
        .remove_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_store));
            try appendInt(alloc, &out, u64, record.store_id);
        },
        .upsert_replica_intent => |intent| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_replica_intent));
            try appendPlacementIntent(alloc, &out, intent);
        },
        .remove_replica_intent => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_replica_intent));
            try appendInt(alloc, &out, u64, record.group_id);
            try appendInt(alloc, &out, u64, record.local_node_id);
        },
        .upsert_database => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_database));
            try appendDatabaseRecord(alloc, &out, record);
        },
        .remove_database => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_database));
            try appendInt(alloc, &out, u64, record.database_id);
        },
        .upsert_namespace => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_namespace));
            try appendNamespaceRecord(alloc, &out, record);
        },
        .remove_namespace => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_namespace));
            try appendInt(alloc, &out, u64, record.namespace_id);
        },
        .upsert_tablespace => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_tablespace));
            try appendTablespaceRecord(alloc, &out, record);
        },
        .remove_tablespace => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_tablespace));
            try appendInt(alloc, &out, u64, record.tablespace_id);
        },
        .upsert_sequence => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_sequence));
            try appendSequenceRecord(alloc, &out, record);
        },
        .remove_sequence => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_sequence));
            try appendInt(alloc, &out, u64, record.sequence_id);
        },
        .compare_and_swap_sequence => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.compare_and_swap_sequence));
            try appendInt(alloc, &out, u64, request.sequence_id);
            try appendInt(alloc, &out, i64, request.expected_last_value);
            try appendInt(alloc, &out, i64, request.next_last_value);
            try appendInt(alloc, &out, u128, request.allocation_id);
        },
        .upsert_table => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_table));
            try appendTableRecord(alloc, &out, record);
        },
        .remove_table => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_table));
            try appendInt(alloc, &out, u64, record.table_id);
        },
        .upsert_schema_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_schema_progress));
            try appendSchemaProgressRecord(alloc, &out, record);
        },
        .remove_schema_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_schema_progress));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .upsert_restore_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_restore_progress));
            try appendRestoreProgressRecord(alloc, &out, record);
        },
        .remove_restore_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_restore_progress));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u64, record.node_id);
            try appendInt(alloc, &out, u64, record.group_id);
        },
        .upsert_replication_source_status => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_replication_source_status));
            try appendReplicationSourceStatusRecord(alloc, &out, record);
        },
        .remove_replication_source_status => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_replication_source_status));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u32, record.source_ordinal);
        },
        .upsert_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_range));
            try appendRangeRecord(alloc, &out, record);
        },
        .remove_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_range));
            try appendInt(alloc, &out, u64, record.group_id);
        },
        .upsert_foreign_key_ref_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_foreign_key_ref_range));
            try appendForeignKeyReferenceRangeRecord(alloc, &out, record);
        },
        .remove_foreign_key_ref_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_foreign_key_ref_range));
            try appendInt(alloc, &out, u64, record.child_table_id);
            try appendInt(alloc, &out, u32, @intCast(record.constraint_name.len));
            try out.appendSlice(alloc, record.constraint_name);
            try appendInt(alloc, &out, u64, record.parent_table_id);
            try appendInt(alloc, &out, u32, @intCast(record.start_parent_key.len));
            try out.appendSlice(alloc, record.start_parent_key);
        },
        .begin_foreign_key_ref_range_split => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_foreign_key_ref_range_split));
            try appendForeignKeyReferenceRangeSplitRequest(alloc, &out, request);
        },
        .finish_foreign_key_ref_range_split => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_foreign_key_ref_range_split));
            try appendForeignKeyReferenceRangeSplitRequest(alloc, &out, request);
        },
        .begin_foreign_key_ref_range_merge => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_foreign_key_ref_range_merge));
            try appendForeignKeyReferenceRangeMergeRequest(alloc, &out, request);
        },
        .finish_foreign_key_ref_range_merge => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_foreign_key_ref_range_merge));
            try appendForeignKeyReferenceRangeMergeRequest(alloc, &out, request);
        },
        .begin_foreign_key_ref_range_rebuild => |selector| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_foreign_key_ref_range_rebuild));
            try appendForeignKeyReferenceRangeSelector(alloc, &out, selector);
        },
        .finish_foreign_key_ref_range_rebuild => |selector| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_foreign_key_ref_range_rebuild));
            try appendForeignKeyReferenceRangeSelector(alloc, &out, selector);
        },
        .upsert_unique_constraint_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_unique_constraint_range));
            try appendUniqueConstraintRangeRecord(alloc, &out, record);
        },
        .remove_unique_constraint_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_unique_constraint_range));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u32, @intCast(record.constraint_name.len));
            try out.appendSlice(alloc, record.constraint_name);
            try appendInt(alloc, &out, u32, @intCast(record.start_encoded_value.len));
            try out.appendSlice(alloc, record.start_encoded_value);
        },
        .begin_unique_constraint_range_split => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_unique_constraint_range_split));
            try appendUniqueConstraintRangeSplitRequest(alloc, &out, request);
        },
        .finish_unique_constraint_range_split => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_unique_constraint_range_split));
            try appendUniqueConstraintRangeSplitRequest(alloc, &out, request);
        },
        .begin_unique_constraint_range_merge => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_unique_constraint_range_merge));
            try appendUniqueConstraintRangeMergeRequest(alloc, &out, request);
        },
        .finish_unique_constraint_range_merge => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_unique_constraint_range_merge));
            try appendUniqueConstraintRangeMergeRequest(alloc, &out, request);
        },
        .begin_unique_constraint_range_rebuild => |selector| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_unique_constraint_range_rebuild));
            try appendUniqueConstraintRangeSelector(alloc, &out, selector);
        },
        .finish_unique_constraint_range_rebuild => |selector| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_unique_constraint_range_rebuild));
            try appendUniqueConstraintRangeSelector(alloc, &out, selector);
        },
        .upsert_secondary_index_rebuild_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_secondary_index_rebuild_range));
            try appendSecondaryIndexRebuildRangeRecord(alloc, &out, record);
        },
        .remove_secondary_index_rebuild_range => |selector| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_secondary_index_rebuild_range));
            try appendSecondaryIndexRebuildRangeSelector(alloc, &out, selector);
        },
        .begin_secondary_index_rebuild_range => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_secondary_index_rebuild_range));
            try appendSecondaryIndexRebuildRangeBeginRequest(alloc, &out, request);
        },
        .finish_secondary_index_rebuild_range => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_secondary_index_rebuild_range));
            try appendSecondaryIndexRebuildRangeFinishRequest(alloc, &out, request);
        },
        .invalidate_secondary_index_rebuild_range => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.invalidate_secondary_index_rebuild_range));
            try appendSecondaryIndexRebuildRangeInvalidateRequest(alloc, &out, request);
        },
        .upsert_schema_rewrite_job => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_schema_rewrite_job));
            try appendSchemaRewriteJobRecord(alloc, &out, record);
        },
        .apply_table_catalog_update_with_schema_rewrite_jobs => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.apply_table_catalog_update_with_schema_rewrite_jobs));
            try appendTableCatalogUpdateWithSchemaRewriteJobsRequest(alloc, &out, request);
        },
        .apply_table_catalog_batch_update_with_schema_rewrite_jobs => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.apply_table_catalog_batch_update_with_schema_rewrite_jobs));
            try appendTableCatalogBatchUpdateWithSchemaRewriteJobsRequest(alloc, &out, request);
        },
        .apply_table_catalog_drop_with_schema_rewrite_jobs => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.apply_table_catalog_drop_with_schema_rewrite_jobs));
            try appendTableCatalogDropWithSchemaRewriteJobsRequest(alloc, &out, request);
        },
        .remove_schema_rewrite_job => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_schema_rewrite_job));
            try appendInt(alloc, &out, u64, record.job_id);
        },
        .begin_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_schema_rewrite_job));
            try appendSchemaRewriteJobBeginRequest(alloc, &out, request);
        },
        .finish_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_schema_rewrite_job));
            try appendSchemaRewriteJobFinishRequest(alloc, &out, request);
        },
        .invalidate_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.invalidate_schema_rewrite_job));
            try appendSchemaRewriteJobInvalidateRequest(alloc, &out, request);
        },
        .pause_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.pause_schema_rewrite_job));
            try appendSchemaRewriteJobControlRequest(alloc, &out, request);
        },
        .resume_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.resume_schema_rewrite_job));
            try appendSchemaRewriteJobControlRequest(alloc, &out, request);
        },
        .retry_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.retry_schema_rewrite_job));
            try appendSchemaRewriteJobControlRequest(alloc, &out, request);
        },
        .cancel_schema_rewrite_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.cancel_schema_rewrite_job));
            try appendSchemaRewriteJobControlRequest(alloc, &out, request);
        },
        .upsert_table_emptying_job => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_table_emptying_job));
            try appendTableEmptyingJobRecord(alloc, &out, record);
        },
        .remove_table_emptying_job => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_table_emptying_job));
            try appendInt(alloc, &out, u64, record.job_id);
        },
        .begin_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.begin_table_emptying_job));
            try appendTableEmptyingJobBeginRequest(alloc, &out, request);
        },
        .finish_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.finish_table_emptying_job));
            try appendTableEmptyingJobFinishRequest(alloc, &out, request);
        },
        .invalidate_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.invalidate_table_emptying_job));
            try appendTableEmptyingJobInvalidateRequest(alloc, &out, request);
        },
        .pause_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.pause_table_emptying_job));
            try appendTableEmptyingJobControlRequest(alloc, &out, request);
        },
        .resume_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.resume_table_emptying_job));
            try appendTableEmptyingJobControlRequest(alloc, &out, request);
        },
        .retry_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.retry_table_emptying_job));
            try appendTableEmptyingJobControlRequest(alloc, &out, request);
        },
        .cancel_table_emptying_job => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.cancel_table_emptying_job));
            try appendTableEmptyingJobControlRequest(alloc, &out, request);
        },
        .promote_table_emptying_barrier => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.promote_table_emptying_barrier));
            try appendTableEmptyingBarrierPromotionRequest(alloc, &out, request);
        },
        .promote_secondary_index_ready => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.promote_secondary_index_ready));
            try appendSecondaryIndexReadyPromotionRequest(alloc, &out, request);
        },
        .compare_and_swap_table_schema => |request| {
            try out.append(alloc, @intFromEnum(TransitionTag.compare_and_swap_table_schema));
            try appendTableSchemaCompareAndSwapRequest(alloc, &out, request);
        },
        .upsert_split_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_split_transition));
            try appendSplitTransitionRecord(alloc, &out, record);
        },
        .remove_split_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_split_transition));
            try appendInt(alloc, &out, u64, record.transition_id);
        },
        .upsert_merge_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_merge_transition));
            try appendMergeTransitionRecord(alloc, &out, record);
        },
        .remove_merge_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_merge_transition));
            try appendInt(alloc, &out, u64, record.transition_id);
        },
        .upsert_reconcile_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_reconcile_lease));
            try appendReconcileLeaseRecord(alloc, &out, record);
        },
        .remove_reconcile_lease => {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_reconcile_lease));
        },
        .upsert_shuffle_join_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_shuffle_join_lease));
            try appendShuffleJoinLeaseRecord(alloc, &out, record);
        },
        .remove_shuffle_join_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_shuffle_join_lease));
            try appendInt(alloc, &out, u64, record.job_id);
        },
        .upsert_reallocation_request => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_reallocation_request));
            try appendReallocationRequestRecord(alloc, &out, record);
        },
        .remove_reallocation_request => {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_reallocation_request));
        },
        .upsert_extension_package => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_extension_package));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_extension_package => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_extension_package));
            try appendRequiredString(alloc, &out, record.name);
            try appendRequiredString(alloc, &out, record.version);
        },
        .upsert_installed_extension => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_installed_extension));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_installed_extension => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_installed_extension));
            try appendRequiredString(alloc, &out, record.name);
        },
        .upsert_extension_member => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_extension_member));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_extension_member => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_extension_member));
            try appendRequiredString(alloc, &out, record.extension_name);
            try appendRequiredString(alloc, &out, @tagName(record.object_kind));
            try appendRequiredString(alloc, &out, record.object_name);
        },
        .upsert_extension_dependency => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_extension_dependency));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_extension_dependency => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_extension_dependency));
            try appendRequiredString(alloc, &out, record.extension_name);
            try appendRequiredString(alloc, &out, record.required_extension_name);
            try appendRequiredString(alloc, &out, record.package_name);
        },
        .apply_extension_lifecycle => |delta| {
            try out.append(alloc, @intFromEnum(TransitionTag.apply_extension_lifecycle));
            try appendJsonRecord(alloc, &out, delta);
        },
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeTransitionCommand(alloc: std.mem.Allocator, encoded: []const u8) !?TransitionCommand {
    if (encoded.len < transition_magic.len + 1) return null;
    if (!std.mem.eql(u8, encoded[0..transition_magic.len], transition_magic)) return null;

    var pos: usize = transition_magic.len;
    const tag: TransitionTag = @enumFromInt(encoded[pos]);
    pos += 1;

    return switch (tag) {
        .upsert_node => .{
            .upsert_node = try readNodeRecord(alloc, encoded, &pos),
        },
        .register_node => .{
            .register_node = try readNodeRecord(alloc, encoded, &pos),
        },
        .remove_node => .{
            .remove_node = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .request_node_shutdown => .{
            .request_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .cancel_node_shutdown => .{
            .cancel_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .finalize_node_shutdown => .{
            .finalize_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_store => .{
            .upsert_store = try readStoreRecord(alloc, encoded, &pos),
        },
        .register_store => .{
            .register_store = try readStoreRecord(alloc, encoded, &pos),
        },
        .remove_store => .{
            .remove_store = .{ .store_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_replica_intent => .{
            .upsert_replica_intent = try readPlacementIntent(alloc, encoded, &pos),
        },
        .remove_replica_intent => .{
            .remove_replica_intent = .{
                .group_id = try readInt(encoded, &pos, u64),
                .local_node_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_database => .{
            .upsert_database = try readDatabaseRecord(alloc, encoded, &pos),
        },
        .remove_database => .{
            .remove_database = .{ .database_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_namespace => .{
            .upsert_namespace = try readNamespaceRecord(alloc, encoded, &pos),
        },
        .remove_namespace => .{
            .remove_namespace = .{ .namespace_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_tablespace => .{
            .upsert_tablespace = try readTablespaceRecord(alloc, encoded, &pos),
        },
        .remove_tablespace => .{
            .remove_tablespace = .{ .tablespace_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_sequence => .{
            .upsert_sequence = try readSequenceRecord(alloc, encoded, &pos),
        },
        .remove_sequence => .{
            .remove_sequence = .{ .sequence_id = try readInt(encoded, &pos, u64) },
        },
        .compare_and_swap_sequence => .{
            .compare_and_swap_sequence = .{
                .sequence_id = try readInt(encoded, &pos, u64),
                .expected_last_value = try readInt(encoded, &pos, i64),
                .next_last_value = try readInt(encoded, &pos, i64),
                .allocation_id = try readInt(encoded, &pos, u128),
            },
        },
        .upsert_table => .{
            .upsert_table = try readTableRecord(alloc, encoded, &pos),
        },
        .remove_table => .{
            .remove_table = .{ .table_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_schema_progress => .{
            .upsert_schema_progress = try readSchemaProgressRecord(encoded, &pos),
        },
        .remove_schema_progress => .{
            .remove_schema_progress = .{
                .table_id = try readInt(encoded, &pos, u64),
                .node_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_restore_progress => .{
            .upsert_restore_progress = try readRestoreProgressRecord(alloc, encoded, &pos),
        },
        .remove_restore_progress => .{
            .remove_restore_progress = .{
                .table_id = try readInt(encoded, &pos, u64),
                .node_id = try readInt(encoded, &pos, u64),
                .group_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_replication_source_status => .{
            .upsert_replication_source_status = try readReplicationSourceStatusRecord(alloc, encoded, &pos),
        },
        .remove_replication_source_status => .{
            .remove_replication_source_status = .{
                .table_id = try readInt(encoded, &pos, u64),
                .source_ordinal = try readInt(encoded, &pos, u32),
            },
        },
        .upsert_range => .{
            .upsert_range = try readRangeRecord(alloc, encoded, &pos),
        },
        .remove_range => .{
            .remove_range = .{ .group_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_foreign_key_ref_range => .{
            .upsert_foreign_key_ref_range = try readForeignKeyReferenceRangeRecord(alloc, encoded, &pos),
        },
        .remove_foreign_key_ref_range => .{
            .remove_foreign_key_ref_range = .{
                .child_table_id = try readInt(encoded, &pos, u64),
                .constraint_name = try readRequiredString(alloc, encoded, &pos),
                .parent_table_id = try readInt(encoded, &pos, u64),
                .start_parent_key = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .begin_foreign_key_ref_range_split => .{
            .begin_foreign_key_ref_range_split = try readForeignKeyReferenceRangeSplitRequest(alloc, encoded, &pos),
        },
        .finish_foreign_key_ref_range_split => .{
            .finish_foreign_key_ref_range_split = try readForeignKeyReferenceRangeSplitRequest(alloc, encoded, &pos),
        },
        .begin_foreign_key_ref_range_merge => .{
            .begin_foreign_key_ref_range_merge = try readForeignKeyReferenceRangeMergeRequest(alloc, encoded, &pos),
        },
        .finish_foreign_key_ref_range_merge => .{
            .finish_foreign_key_ref_range_merge = try readForeignKeyReferenceRangeMergeRequest(alloc, encoded, &pos),
        },
        .begin_foreign_key_ref_range_rebuild => .{
            .begin_foreign_key_ref_range_rebuild = try readForeignKeyReferenceRangeSelector(alloc, encoded, &pos),
        },
        .finish_foreign_key_ref_range_rebuild => .{
            .finish_foreign_key_ref_range_rebuild = try readForeignKeyReferenceRangeSelector(alloc, encoded, &pos),
        },
        .upsert_unique_constraint_range => .{
            .upsert_unique_constraint_range = try readUniqueConstraintRangeRecord(alloc, encoded, &pos),
        },
        .remove_unique_constraint_range => .{
            .remove_unique_constraint_range = .{
                .table_id = try readInt(encoded, &pos, u64),
                .constraint_name = try readRequiredString(alloc, encoded, &pos),
                .start_encoded_value = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .begin_unique_constraint_range_split => .{
            .begin_unique_constraint_range_split = try readUniqueConstraintRangeSplitRequest(alloc, encoded, &pos),
        },
        .finish_unique_constraint_range_split => .{
            .finish_unique_constraint_range_split = try readUniqueConstraintRangeSplitRequest(alloc, encoded, &pos),
        },
        .begin_unique_constraint_range_merge => .{
            .begin_unique_constraint_range_merge = try readUniqueConstraintRangeMergeRequest(alloc, encoded, &pos),
        },
        .finish_unique_constraint_range_merge => .{
            .finish_unique_constraint_range_merge = try readUniqueConstraintRangeMergeRequest(alloc, encoded, &pos),
        },
        .begin_unique_constraint_range_rebuild => .{
            .begin_unique_constraint_range_rebuild = try readUniqueConstraintRangeSelector(alloc, encoded, &pos),
        },
        .finish_unique_constraint_range_rebuild => .{
            .finish_unique_constraint_range_rebuild = try readUniqueConstraintRangeSelector(alloc, encoded, &pos),
        },
        .upsert_secondary_index_rebuild_range => .{
            .upsert_secondary_index_rebuild_range = try readSecondaryIndexRebuildRangeRecord(alloc, encoded, &pos),
        },
        .remove_secondary_index_rebuild_range => .{
            .remove_secondary_index_rebuild_range = try readSecondaryIndexRebuildRangeSelector(alloc, encoded, &pos),
        },
        .begin_secondary_index_rebuild_range => .{
            .begin_secondary_index_rebuild_range = try readSecondaryIndexRebuildRangeBeginRequest(alloc, encoded, &pos),
        },
        .finish_secondary_index_rebuild_range => .{
            .finish_secondary_index_rebuild_range = try readSecondaryIndexRebuildRangeFinishRequest(alloc, encoded, &pos),
        },
        .invalidate_secondary_index_rebuild_range => .{
            .invalidate_secondary_index_rebuild_range = try readSecondaryIndexRebuildRangeInvalidateRequest(alloc, encoded, &pos),
        },
        .upsert_schema_rewrite_job => .{
            .upsert_schema_rewrite_job = try readSchemaRewriteJobRecord(alloc, encoded, &pos),
        },
        .apply_table_catalog_update_with_schema_rewrite_jobs => .{
            .apply_table_catalog_update_with_schema_rewrite_jobs = try readTableCatalogUpdateWithSchemaRewriteJobsRequest(alloc, encoded, &pos),
        },
        .apply_table_catalog_batch_update_with_schema_rewrite_jobs => .{
            .apply_table_catalog_batch_update_with_schema_rewrite_jobs = try readTableCatalogBatchUpdateWithSchemaRewriteJobsRequest(alloc, encoded, &pos),
        },
        .apply_table_catalog_drop_with_schema_rewrite_jobs => .{
            .apply_table_catalog_drop_with_schema_rewrite_jobs = try readTableCatalogDropWithSchemaRewriteJobsRequest(alloc, encoded, &pos),
        },
        .remove_schema_rewrite_job => .{
            .remove_schema_rewrite_job = .{ .job_id = try readInt(encoded, &pos, u64) },
        },
        .begin_schema_rewrite_job => .{
            .begin_schema_rewrite_job = try readSchemaRewriteJobBeginRequest(alloc, encoded, &pos),
        },
        .finish_schema_rewrite_job => .{
            .finish_schema_rewrite_job = try readSchemaRewriteJobFinishRequest(alloc, encoded, &pos),
        },
        .invalidate_schema_rewrite_job => .{
            .invalidate_schema_rewrite_job = try readSchemaRewriteJobInvalidateRequest(alloc, encoded, &pos),
        },
        .pause_schema_rewrite_job => .{
            .pause_schema_rewrite_job = try readSchemaRewriteJobControlRequest(alloc, encoded, &pos),
        },
        .resume_schema_rewrite_job => .{
            .resume_schema_rewrite_job = try readSchemaRewriteJobControlRequest(alloc, encoded, &pos),
        },
        .retry_schema_rewrite_job => .{
            .retry_schema_rewrite_job = try readSchemaRewriteJobControlRequest(alloc, encoded, &pos),
        },
        .cancel_schema_rewrite_job => .{
            .cancel_schema_rewrite_job = try readSchemaRewriteJobControlRequest(alloc, encoded, &pos),
        },
        .upsert_table_emptying_job => .{
            .upsert_table_emptying_job = try readTableEmptyingJobRecord(alloc, encoded, &pos),
        },
        .remove_table_emptying_job => .{
            .remove_table_emptying_job = .{ .job_id = try readInt(encoded, &pos, u64) },
        },
        .begin_table_emptying_job => .{
            .begin_table_emptying_job = try readTableEmptyingJobBeginRequest(alloc, encoded, &pos),
        },
        .finish_table_emptying_job => .{
            .finish_table_emptying_job = try readTableEmptyingJobFinishRequest(alloc, encoded, &pos),
        },
        .invalidate_table_emptying_job => .{
            .invalidate_table_emptying_job = try readTableEmptyingJobInvalidateRequest(alloc, encoded, &pos),
        },
        .pause_table_emptying_job => .{
            .pause_table_emptying_job = try readTableEmptyingJobControlRequest(alloc, encoded, &pos),
        },
        .resume_table_emptying_job => .{
            .resume_table_emptying_job = try readTableEmptyingJobControlRequest(alloc, encoded, &pos),
        },
        .retry_table_emptying_job => .{
            .retry_table_emptying_job = try readTableEmptyingJobControlRequest(alloc, encoded, &pos),
        },
        .cancel_table_emptying_job => .{
            .cancel_table_emptying_job = try readTableEmptyingJobControlRequest(alloc, encoded, &pos),
        },
        .promote_table_emptying_barrier => .{
            .promote_table_emptying_barrier = try readTableEmptyingBarrierPromotionRequest(alloc, encoded, &pos),
        },
        .promote_secondary_index_ready => .{
            .promote_secondary_index_ready = try readSecondaryIndexReadyPromotionRequest(alloc, encoded, &pos),
        },
        .compare_and_swap_table_schema => .{
            .compare_and_swap_table_schema = try readTableSchemaCompareAndSwapRequest(alloc, encoded, &pos),
        },
        .upsert_split_transition => .{
            .upsert_split_transition = try readSplitTransitionRecord(alloc, encoded, &pos),
        },
        .remove_split_transition => .{
            .remove_split_transition = .{ .transition_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_merge_transition => .{
            .upsert_merge_transition = try readMergeTransitionRecord(alloc, encoded, &pos),
        },
        .remove_merge_transition => .{
            .remove_merge_transition = .{ .transition_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_reconcile_lease => .{
            .upsert_reconcile_lease = try readReconcileLeaseRecord(encoded, &pos),
        },
        .remove_reconcile_lease => .{
            .remove_reconcile_lease = .{},
        },
        .upsert_shuffle_join_lease => .{
            .upsert_shuffle_join_lease = try readShuffleJoinLeaseRecord(encoded, &pos),
        },
        .remove_shuffle_join_lease => .{
            .remove_shuffle_join_lease = .{
                .job_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_reallocation_request => .{
            .upsert_reallocation_request = try readReallocationRequestRecord(encoded, &pos),
        },
        .remove_reallocation_request => .{
            .remove_reallocation_request = .{},
        },
        .upsert_extension_package => .{
            .upsert_extension_package = try readJsonRecord(extension_domain.PackageManifest, alloc, encoded, &pos),
        },
        .remove_extension_package => .{
            .remove_extension_package = .{
                .name = try readRequiredString(alloc, encoded, &pos),
                .version = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .upsert_installed_extension => .{
            .upsert_installed_extension = try readJsonRecord(extension_domain.InstalledExtension, alloc, encoded, &pos),
        },
        .remove_installed_extension => .{
            .remove_installed_extension = .{
                .name = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .upsert_extension_member => .{
            .upsert_extension_member = try readJsonRecord(extension_domain.ExtensionMember, alloc, encoded, &pos),
        },
        .remove_extension_member => blk: {
            const extension_name = try readRequiredString(alloc, encoded, &pos);
            errdefer alloc.free(extension_name);
            const kind_name = try readRequiredString(alloc, encoded, &pos);
            defer alloc.free(kind_name);
            const object_kind = std.meta.stringToEnum(extension_domain.ExtensionObjectKind, kind_name) orelse return error.InvalidMetadataTransitionEncoding;
            const object_name = try readRequiredString(alloc, encoded, &pos);
            break :blk .{
                .remove_extension_member = .{
                    .extension_name = extension_name,
                    .object_kind = object_kind,
                    .object_name = object_name,
                },
            };
        },
        .upsert_extension_dependency => .{
            .upsert_extension_dependency = try readJsonRecord(extension_domain.ExtensionDependency, alloc, encoded, &pos),
        },
        .remove_extension_dependency => .{
            .remove_extension_dependency = .{
                .extension_name = try readRequiredString(alloc, encoded, &pos),
                .required_extension_name = try readRequiredString(alloc, encoded, &pos),
                .package_name = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .apply_extension_lifecycle => .{
            .apply_extension_lifecycle = try readJsonRecord(ExtensionLifecycleDelta, alloc, encoded, &pos),
        },
    };
}

fn encodePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendPlacementIntent(alloc, &out, intent);
    return try out.toOwnedSlice(alloc);
}

fn encodeNodeRecord(alloc: std.mem.Allocator, record: metadata.NodeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendNodeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeStoreRecord(alloc: std.mem.Allocator, record: metadata.StoreRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendStoreRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeDatabaseRecord(alloc: std.mem.Allocator, record: metadata.DatabaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendDatabaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeDatabaseRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.DatabaseRecord {
    var pos: usize = 0;
    const record = try readDatabaseRecord(alloc, encoded, &pos);
    if (pos != encoded.len) {
        metadata_table_manager.freeDatabase(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

fn encodeNamespaceRecord(alloc: std.mem.Allocator, record: metadata.NamespaceRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendNamespaceRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeNamespaceRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.NamespaceRecord {
    var pos: usize = 0;
    const record = try readNamespaceRecord(alloc, encoded, &pos);
    if (pos != encoded.len) {
        metadata_table_manager.freeNamespace(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

fn encodeTablespaceRecord(alloc: std.mem.Allocator, record: metadata.TablespaceRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendTablespaceRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeTablespaceRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.TablespaceRecord {
    var pos: usize = 0;
    const record = try readTablespaceRecord(alloc, encoded, &pos);
    if (pos != encoded.len) {
        metadata_table_manager.freeTablespace(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

fn encodeSequenceRecord(alloc: std.mem.Allocator, record: metadata.SequenceRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSequenceRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeSequenceRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.SequenceRecord {
    var pos: usize = 0;
    const record = try readSequenceRecord(alloc, encoded, &pos);
    if (pos != encoded.len) {
        metadata_table_manager.freeSequence(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

fn encodeTableRecord(alloc: std.mem.Allocator, record: metadata.TableRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendTableRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeRangeRecord(alloc: std.mem.Allocator, record: metadata.RangeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendRangeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeForeignKeyReferenceRangeRecord(alloc: std.mem.Allocator, record: metadata.ForeignKeyReferenceRangeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendForeignKeyReferenceRangeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeUniqueConstraintRangeRecord(alloc: std.mem.Allocator, record: metadata.UniqueConstraintRangeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendUniqueConstraintRangeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeSecondaryIndexRebuildRangeRecord(alloc: std.mem.Allocator, record: metadata.SecondaryIndexRebuildRangeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSecondaryIndexRebuildRangeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeSchemaRewriteJobRecord(alloc: std.mem.Allocator, record: metadata.SchemaRewriteJobRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSchemaRewriteJobRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeTableEmptyingJobRecord(alloc: std.mem.Allocator, record: metadata.TableEmptyingJobRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendTableEmptyingJobRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeSchemaProgressRecord(alloc: std.mem.Allocator, record: metadata.SchemaProgressRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSchemaProgressRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeRestoreProgressRecord(alloc: std.mem.Allocator, record: metadata.RestoreProgressRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendRestoreProgressRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReplicationSourceStatusRecord(alloc: std.mem.Allocator, record: metadata.ReplicationSourceStatusRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReplicationSourceStatusRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReconcileLeaseRecord(alloc: std.mem.Allocator, record: metadata.ReconcileLeaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReconcileLeaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeShuffleJoinLeaseRecord(alloc: std.mem.Allocator, record: metadata.ShuffleJoinLeaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendShuffleJoinLeaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReallocationRequestRecord(alloc: std.mem.Allocator, record: metadata.ReallocationRequestRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReallocationRequestRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeExtensionPackageRecord(alloc: std.mem.Allocator, record: extension_domain.PackageManifest) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeInstalledExtensionRecord(alloc: std.mem.Allocator, record: extension_domain.InstalledExtension) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeExtensionMemberRecord(alloc: std.mem.Allocator, record: extension_domain.ExtensionMember) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeExtensionDependencyRecord(alloc: std.mem.Allocator, record: extension_domain.ExtensionDependency) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeSplitTransitionRecord(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSplitTransitionRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeMergeTransitionRecord(alloc: std.mem.Allocator, record: metadata.MergeTransitionRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendMergeTransitionRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeSplitTransitionRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.SplitTransitionRecord {
    var pos: usize = 0;
    return try readSplitTransitionRecord(alloc, encoded, &pos);
}

fn decodeMergeTransitionRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.MergeTransitionRecord {
    var pos: usize = 0;
    return try readMergeTransitionRecord(alloc, encoded, &pos);
}

fn decodeTableRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.TableRecord {
    var pos: usize = 0;
    return try readTableRecord(alloc, encoded, &pos);
}

fn decodeRangeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.RangeRecord {
    var pos: usize = 0;
    return try readRangeRecord(alloc, encoded, &pos);
}

fn decodeForeignKeyReferenceRangeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.ForeignKeyReferenceRangeRecord {
    var pos: usize = 0;
    return try readForeignKeyReferenceRangeRecord(alloc, encoded, &pos);
}

fn decodeUniqueConstraintRangeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.UniqueConstraintRangeRecord {
    var pos: usize = 0;
    return try readUniqueConstraintRangeRecord(alloc, encoded, &pos);
}

fn decodeSecondaryIndexRebuildRangeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.SecondaryIndexRebuildRangeRecord {
    var pos: usize = 0;
    return try readSecondaryIndexRebuildRangeRecord(alloc, encoded, &pos);
}

fn decodeSchemaRewriteJobRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.SchemaRewriteJobRecord {
    var pos: usize = 0;
    return try readSchemaRewriteJobRecord(alloc, encoded, &pos);
}

fn decodeTableEmptyingJobRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.TableEmptyingJobRecord {
    var pos: usize = 0;
    return try readTableEmptyingJobRecord(alloc, encoded, &pos);
}

fn decodeSchemaProgressRecord(encoded: []const u8) !metadata.SchemaProgressRecord {
    var pos: usize = 0;
    return try readSchemaProgressRecord(encoded, &pos);
}

fn decodeRestoreProgressRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.RestoreProgressRecord {
    var pos: usize = 0;
    return try readRestoreProgressRecord(alloc, encoded, &pos);
}

fn decodeReplicationSourceStatusRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.ReplicationSourceStatusRecord {
    var pos: usize = 0;
    return try readReplicationSourceStatusRecord(alloc, encoded, &pos);
}

fn decodeExtensionPackageRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.PackageManifest {
    var value = try std.json.parseFromSliceLeaky(extension_domain.PackageManifest, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodeInstalledExtensionRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.InstalledExtension {
    var value = try std.json.parseFromSliceLeaky(extension_domain.InstalledExtension, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodeExtensionMemberRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.ExtensionMember {
    var value = try std.json.parseFromSliceLeaky(extension_domain.ExtensionMember, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodeExtensionDependencyRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.ExtensionDependency {
    var value = try std.json.parseFromSliceLeaky(extension_domain.ExtensionDependency, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodePlacementIntent(alloc: std.mem.Allocator, encoded: []const u8) !raft_reconciler.PlacementIntent {
    var pos: usize = 0;
    return try readPlacementIntent(alloc, encoded, &pos);
}

fn clonePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) !raft_reconciler.PlacementIntent {
    return .{
        .record = try intent.record.clone(alloc),
        .store_id = intent.store_id,
        .peer_node_ids = try alloc.dupe(u64, intent.peer_node_ids),
    };
}

fn freePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) void {
    var record = intent.record;
    record.deinit(alloc);
    if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
}

fn decodeNodeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.NodeRecord {
    var pos: usize = 0;
    return try readNodeRecord(alloc, encoded, &pos);
}

fn decodeStoreRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.StoreRecord {
    var pos: usize = 0;
    return try readStoreRecord(alloc, encoded, &pos);
}

fn appendNodeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.NodeRecord,
) !void {
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, @intCast(record.role.len));
    try out.appendSlice(alloc, record.role);
    try appendInt(alloc, out, u32, @intCast(record.lifecycle.len));
    try out.appendSlice(alloc, record.lifecycle);
}

fn readNodeRecord(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !metadata.NodeRecord {
    const node_id = try readInt(encoded, pos, u64);
    const role_len = try readInt(encoded, pos, u32);
    if (pos.* + role_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const role = try alloc.dupe(u8, encoded[pos.* .. pos.* + role_len]);
    errdefer alloc.free(role);
    pos.* += role_len;
    const lifecycle = if (pos.* < encoded.len) blk: {
        const lifecycle_len = try readInt(encoded, pos, u32);
        if (pos.* + lifecycle_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
        const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + lifecycle_len]);
        pos.* += lifecycle_len;
        break :blk value;
    } else try alloc.dupe(u8, metadata_table_manager.node_lifecycle_active);
    return .{
        .node_id = node_id,
        .role = role,
        .lifecycle = lifecycle,
    };
}

fn appendStoreRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.StoreRecord,
) !void {
    try appendInt(alloc, out, u64, record.store_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, @intCast(record.api_url.len));
    try out.appendSlice(alloc, record.api_url);
    try appendInt(alloc, out, u32, @intCast(record.raft_url.len));
    try out.appendSlice(alloc, record.raft_url);
    try appendInt(alloc, out, u32, @intCast(record.role.len));
    try out.appendSlice(alloc, record.role);
    try appendInt(alloc, out, u32, @intCast(record.health_class.len));
    try out.appendSlice(alloc, record.health_class);
    try appendInt(alloc, out, u32, @intCast(record.failure_domain.len));
    try out.appendSlice(alloc, record.failure_domain);
    try out.append(alloc, if (record.live) 1 else 0);
    try appendInt(alloc, out, u64, record.capacity_bytes);
    try appendInt(alloc, out, u64, record.available_bytes);
    try appendInt(alloc, out, u32, record.lease_pressure);
    try appendInt(alloc, out, u32, record.read_load);
    try appendInt(alloc, out, u32, record.write_load);
    try appendInt(alloc, out, u32, record.active_backfills);
    try appendInt(alloc, out, u16, record.backfill_progress_millis);
    try appendInt(alloc, out, u32, @intCast(record.group_statuses.len));
    for (record.group_statuses) |group_status| try appendGroupStatusRecord(alloc, out, group_status);
    try appendInt(alloc, out, u32, @intCast(record.runtime_statuses.len));
    for (record.runtime_statuses) |runtime_status| try appendRuntimeGroupStatusRecord(alloc, out, runtime_status);
    try out.append(alloc, if (record.drain_requested) 1 else 0);
}

fn readStoreRecord(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !metadata.StoreRecord {
    const store_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const api_url_len = try readInt(encoded, pos, u32);
    if (pos.* + api_url_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const api_url = try alloc.dupe(u8, encoded[pos.* .. pos.* + api_url_len]);
    errdefer alloc.free(api_url);
    pos.* += api_url_len;
    const raft_url_len = try readInt(encoded, pos, u32);
    if (pos.* + raft_url_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const raft_url = try alloc.dupe(u8, encoded[pos.* .. pos.* + raft_url_len]);
    errdefer alloc.free(raft_url);
    pos.* += raft_url_len;
    const role_len = try readInt(encoded, pos, u32);
    if (pos.* + role_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const role = try alloc.dupe(u8, encoded[pos.* .. pos.* + role_len]);
    errdefer alloc.free(role);
    pos.* += role_len;
    const health_class_len = try readInt(encoded, pos, u32);
    if (pos.* + health_class_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const health_class = try alloc.dupe(u8, encoded[pos.* .. pos.* + health_class_len]);
    errdefer alloc.free(health_class);
    pos.* += health_class_len;
    const failure_domain_len = try readInt(encoded, pos, u32);
    if (pos.* + failure_domain_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const failure_domain = try alloc.dupe(u8, encoded[pos.* .. pos.* + failure_domain_len]);
    errdefer alloc.free(failure_domain);
    pos.* += failure_domain_len;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const live = encoded[pos.*] != 0;
    pos.* += 1;
    const capacity_bytes = try readInt(encoded, pos, u64);
    const available_bytes = try readInt(encoded, pos, u64);
    const lease_pressure = try readInt(encoded, pos, u32);
    const read_load = try readInt(encoded, pos, u32);
    const write_load = try readInt(encoded, pos, u32);
    const active_backfills = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const backfill_progress_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u16) else 1000;
    const group_status_count = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const group_statuses = try alloc.alloc(metadata.GroupStatusReport, group_status_count);
    var initialized: usize = 0;
    errdefer {
        for (group_statuses[0..initialized]) |record| metadata_table_manager.freeGroupStatus(alloc, record);
        if (group_statuses.len > 0) alloc.free(group_statuses);
    }
    while (initialized < group_status_count) : (initialized += 1) {
        group_statuses[initialized] = try readGroupStatusRecord(alloc, encoded, pos);
    }
    const runtime_status_count = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const runtime_statuses = try alloc.alloc(metadata.RuntimeGroupStatusReport, runtime_status_count);
    var initialized_runtime_statuses: usize = 0;
    errdefer {
        for (runtime_statuses[0..initialized_runtime_statuses]) |record| metadata_table_manager.freeRuntimeGroupStatusReport(alloc, record);
        if (runtime_statuses.len > 0) alloc.free(runtime_statuses);
    }
    while (initialized_runtime_statuses < runtime_status_count) : (initialized_runtime_statuses += 1) {
        runtime_statuses[initialized_runtime_statuses] = try readRuntimeGroupStatusRecord(alloc, encoded, pos);
    }
    const drain_requested = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    return .{
        .store_id = store_id,
        .node_id = node_id,
        .api_url = api_url,
        .raft_url = raft_url,
        .role = role,
        .health_class = health_class,
        .failure_domain = failure_domain,
        .live = live,
        .drain_requested = drain_requested,
        .capacity_bytes = capacity_bytes,
        .available_bytes = available_bytes,
        .lease_pressure = lease_pressure,
        .read_load = read_load,
        .write_load = write_load,
        .active_backfills = active_backfills,
        .backfill_progress_millis = backfill_progress_millis,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

fn appendRuntimeGroupStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeGroupStatusReport,
) !void {
    try appendInt(alloc, out, u16, runtime_status_record_version);
    try appendInt(alloc, out, u64, record.table_id);
    try appendRequiredString(alloc, out, record.table_name);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.store_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u64, record.updated_at_ns);
    try appendRequiredString(alloc, out, record.source);
    try appendRequiredString(alloc, out, record.freshness);
    try appendInt(alloc, out, u64, record.topology_generation);
    try appendInt(alloc, out, u64, record.lsm_root_generation);
    try appendInt(alloc, out, u64, record.status_generation);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.disk_bytes);
    try appendInt(alloc, out, u64, record.created_at_millis);
    try appendInt(alloc, out, u32, record.index_count);
    try out.append(alloc, if (record.enrichment_enabled) 1 else 0);
    try appendInt(alloc, out, u64, record.enrichment_target_sequence);
    try appendInt(alloc, out, u64, record.enrichment_applied_sequence);
    try out.append(alloc, if (record.enrichment_retrying) 1 else 0);
    try out.append(alloc, if (record.enrichment_worker_failed) 1 else 0);
    try out.append(alloc, if (record.async_indexing_active) 1 else 0);
    try out.append(alloc, if (record.async_startup_active) 1 else 0);
    try out.append(alloc, if (record.async_dense_catch_up_active) 1 else 0);
    try out.append(alloc, if (record.async_bulk_coalescing_active) 1 else 0);
    try appendRuntimeDocIdentityStatusRecord(alloc, out, record.doc_identity);
    try appendRuntimeDocSetPlanningStatusRecord(alloc, out, record.doc_set_planning);
    try appendInt(alloc, out, u32, @intCast(record.indexes.len));
    for (record.indexes) |index| try appendRuntimeIndexStatusRecord(alloc, out, index);
}

fn readRuntimeGroupStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeGroupStatusReport {
    const version = try readInt(encoded, pos, u16);
    if (version != 1 and version != 2 and version != 3 and version != runtime_status_record_version) return error.InvalidMetadataTransitionEncoding;
    const table_id = try readInt(encoded, pos, u64);
    const table_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(table_name);
    const group_id = try readInt(encoded, pos, u64);
    const store_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const updated_at_ns = try readInt(encoded, pos, u64);
    const source = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(source);
    const freshness = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(freshness);
    const topology_generation = try readInt(encoded, pos, u64);
    const lsm_root_generation = try readInt(encoded, pos, u64);
    const status_generation = try readInt(encoded, pos, u64);
    const doc_count = try readInt(encoded, pos, u64);
    const disk_bytes = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const created_at_millis = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const index_count = try readInt(encoded, pos, u32);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const enrichment_enabled = encoded[pos.*] != 0;
    pos.* += 1;
    const enrichment_target_sequence = try readInt(encoded, pos, u64);
    const enrichment_applied_sequence = try readInt(encoded, pos, u64);
    if (pos.* + 3 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const enrichment_retrying = encoded[pos.*] != 0;
    pos.* += 1;
    const enrichment_worker_failed = encoded[pos.*] != 0;
    pos.* += 1;
    const async_indexing_active = encoded[pos.*] != 0;
    pos.* += 1;
    if (pos.* + 3 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const async_startup_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_dense_catch_up_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_bulk_coalescing_active = encoded[pos.*] != 0;
    pos.* += 1;
    const doc_identity = if (version >= 3) try readRuntimeDocIdentityStatusRecord(encoded, pos) else metadata.RuntimeDocIdentityStatusReport{};
    const doc_set_planning = if (version >= 3) try readRuntimeDocSetPlanningStatusRecord(encoded, pos, version) else metadata.RuntimeDocSetPlanningStatusReport{};
    const runtime_index_count = try readInt(encoded, pos, u32);
    const indexes = try alloc.alloc(metadata.RuntimeIndexStatusReport, runtime_index_count);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |record| metadata_table_manager.freeRuntimeIndexStatusReport(alloc, record);
        if (indexes.len > 0) alloc.free(indexes);
    }
    while (initialized < runtime_index_count) : (initialized += 1) {
        indexes[initialized] = try readRuntimeIndexStatusRecord(alloc, encoded, pos);
    }
    return .{
        .table_id = table_id,
        .table_name = table_name,
        .group_id = group_id,
        .store_id = store_id,
        .node_id = node_id,
        .updated_at_ns = updated_at_ns,
        .source = source,
        .freshness = freshness,
        .topology_generation = topology_generation,
        .lsm_root_generation = lsm_root_generation,
        .status_generation = status_generation,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .created_at_millis = created_at_millis,
        .index_count = index_count,
        .enrichment_enabled = enrichment_enabled,
        .enrichment_target_sequence = enrichment_target_sequence,
        .enrichment_applied_sequence = enrichment_applied_sequence,
        .enrichment_retrying = enrichment_retrying,
        .enrichment_worker_failed = enrichment_worker_failed,
        .async_indexing_active = async_indexing_active,
        .async_startup_active = async_startup_active,
        .async_dense_catch_up_active = async_dense_catch_up_active,
        .async_bulk_coalescing_active = async_bulk_coalescing_active,
        .doc_identity = doc_identity,
        .doc_set_planning = doc_set_planning,
        .indexes = indexes,
    };
}

fn appendRuntimeDocIdentityStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeDocIdentityStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.namespace_table_id);
    try appendInt(alloc, out, u64, record.namespace_shard_id);
    try appendInt(alloc, out, u64, record.namespace_range_id);
    try appendInt(alloc, out, u32, record.next_ordinal);
    try appendInt(alloc, out, u64, record.allocated_ordinals);
    try appendInt(alloc, out, u64, record.ordinal_capacity_remaining);
    try out.append(alloc, if (record.ordinal_capacity_exhausted) 1 else 0);
    try out.append(alloc, if (record.rebuild_required) 1 else 0);
    try appendInt(alloc, out, u64, record.state_rows);
    try appendInt(alloc, out, u64, record.live_ordinals);
    try appendInt(alloc, out, u64, record.tombstone_ordinals);
    try appendInt(alloc, out, u64, record.min_created_generation);
    try appendInt(alloc, out, u64, record.max_created_generation);
    try appendInt(alloc, out, u64, record.min_deleted_generation);
    try appendInt(alloc, out, u64, record.max_deleted_generation);
    try appendInt(alloc, out, u64, record.scanned_primary_docs);
    try appendInt(alloc, out, u64, record.primary_docs_missing_ordinals);
    try appendInt(alloc, out, u64, record.primary_docs_missing_identity_state);
    try appendInt(alloc, out, u64, record.primary_docs_with_tombstone_ordinals);
    try out.append(alloc, if (record.complete) 1 else 0);
}

fn readRuntimeDocIdentityStatusRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeDocIdentityStatusReport {
    const namespace_table_id = try readInt(encoded, pos, u64);
    const namespace_shard_id = try readInt(encoded, pos, u64);
    const namespace_range_id = try readInt(encoded, pos, u64);
    const next_ordinal = try readInt(encoded, pos, u32);
    const allocated_ordinals = try readInt(encoded, pos, u64);
    const ordinal_capacity_remaining = try readInt(encoded, pos, u64);
    if (pos.* + 2 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const ordinal_capacity_exhausted = encoded[pos.*] != 0;
    pos.* += 1;
    const rebuild_required = encoded[pos.*] != 0;
    pos.* += 1;
    const state_rows = try readInt(encoded, pos, u64);
    const live_ordinals = try readInt(encoded, pos, u64);
    const tombstone_ordinals = try readInt(encoded, pos, u64);
    const min_created_generation = try readInt(encoded, pos, u64);
    const max_created_generation = try readInt(encoded, pos, u64);
    const min_deleted_generation = try readInt(encoded, pos, u64);
    const max_deleted_generation = try readInt(encoded, pos, u64);
    const scanned_primary_docs = try readInt(encoded, pos, u64);
    const primary_docs_missing_ordinals = try readInt(encoded, pos, u64);
    const primary_docs_missing_identity_state = try readInt(encoded, pos, u64);
    const primary_docs_with_tombstone_ordinals = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const complete = encoded[pos.*] != 0;
    pos.* += 1;
    return .{
        .namespace_table_id = namespace_table_id,
        .namespace_shard_id = namespace_shard_id,
        .namespace_range_id = namespace_range_id,
        .next_ordinal = next_ordinal,
        .allocated_ordinals = allocated_ordinals,
        .ordinal_capacity_remaining = ordinal_capacity_remaining,
        .ordinal_capacity_exhausted = ordinal_capacity_exhausted,
        .rebuild_required = rebuild_required,
        .state_rows = state_rows,
        .live_ordinals = live_ordinals,
        .tombstone_ordinals = tombstone_ordinals,
        .min_created_generation = min_created_generation,
        .max_created_generation = max_created_generation,
        .min_deleted_generation = min_deleted_generation,
        .max_deleted_generation = max_deleted_generation,
        .scanned_primary_docs = scanned_primary_docs,
        .primary_docs_missing_ordinals = primary_docs_missing_ordinals,
        .primary_docs_missing_identity_state = primary_docs_missing_identity_state,
        .primary_docs_with_tombstone_ordinals = primary_docs_with_tombstone_ordinals,
        .complete = complete,
    };
}

fn appendRuntimeDocSetPlanningStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeDocSetPlanningStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.resolved_set_count);
    try appendInt(alloc, out, u64, record.all_set_count);
    try appendInt(alloc, out, u64, record.none_set_count);
    try appendInt(alloc, out, u64, record.doc_key_list_count);
    try appendInt(alloc, out, u64, record.ordinal_list_count);
    try appendInt(alloc, out, u64, record.ordinal_bitmap_count);
    try appendInt(alloc, out, u64, record.doc_key_list_docs);
    try appendInt(alloc, out, u64, record.ordinal_list_docs);
    try appendInt(alloc, out, u64, record.ordinal_bitmap_docs);
    try appendInt(alloc, out, u64, record.missing_ordinal_coverage_count);
    try appendInt(alloc, out, u64, record.bitmap_promotion_count);
    try appendInt(alloc, out, u64, record.unsupported_filter_shape_count);
    try appendInt(alloc, out, u64, record.stale_identity_generation_rejection_count);
}

fn readRuntimeDocSetPlanningStatusRecord(
    encoded: []const u8,
    pos: *usize,
    version: u16,
) !metadata.RuntimeDocSetPlanningStatusReport {
    return .{
        .resolved_set_count = try readInt(encoded, pos, u64),
        .all_set_count = try readInt(encoded, pos, u64),
        .none_set_count = try readInt(encoded, pos, u64),
        .doc_key_list_count = try readInt(encoded, pos, u64),
        .ordinal_list_count = try readInt(encoded, pos, u64),
        .ordinal_bitmap_count = try readInt(encoded, pos, u64),
        .doc_key_list_docs = try readInt(encoded, pos, u64),
        .ordinal_list_docs = try readInt(encoded, pos, u64),
        .ordinal_bitmap_docs = try readInt(encoded, pos, u64),
        .missing_ordinal_coverage_count = try readInt(encoded, pos, u64),
        .bitmap_promotion_count = try readInt(encoded, pos, u64),
        .unsupported_filter_shape_count = try readInt(encoded, pos, u64),
        .stale_identity_generation_rejection_count = if (version >= 4) try readInt(encoded, pos, u64) else 0,
    };
}

fn appendRuntimeIndexStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeIndexStatusReport,
) !void {
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.kind);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.term_count);
    try appendInt(alloc, out, u64, record.edge_count);
    try appendInt(alloc, out, u64, record.node_count);
    try appendInt(alloc, out, u64, record.root_node);
    try out.append(alloc, if (record.backfill_active) 1 else 0);
    try appendInt(alloc, out, u16, record.backfill_progress_millis);
    try appendInt(alloc, out, u64, record.replay_applied_sequence);
    try appendInt(alloc, out, u64, record.replay_target_sequence);
    try out.append(alloc, if (record.replay_catch_up_required) 1 else 0);
}

fn readRuntimeIndexStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeIndexStatusReport {
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const kind = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(kind);
    const doc_count = try readInt(encoded, pos, u64);
    const term_count = try readInt(encoded, pos, u64);
    const edge_count = try readInt(encoded, pos, u64);
    const node_count = try readInt(encoded, pos, u64);
    const root_node = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const backfill_active = encoded[pos.*] != 0;
    pos.* += 1;
    const backfill_progress_millis = try readInt(encoded, pos, u16);
    const replay_applied_sequence = try readInt(encoded, pos, u64);
    const replay_target_sequence = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const replay_catch_up_required = encoded[pos.*] != 0;
    pos.* += 1;
    return .{
        .name = name,
        .kind = kind,
        .doc_count = doc_count,
        .term_count = term_count,
        .edge_count = edge_count,
        .node_count = node_count,
        .root_node = root_node,
        .backfill_active = backfill_active,
        .backfill_progress_millis = backfill_progress_millis,
        .replay_applied_sequence = replay_applied_sequence,
        .replay_target_sequence = replay_target_sequence,
        .replay_catch_up_required = replay_catch_up_required,
    };
}

fn appendGroupStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.GroupStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.disk_bytes);
    try out.append(alloc, if (record.empty) 1 else 0);
    try appendInt(alloc, out, u32, 0);
    try appendInt(alloc, out, u64, record.updated_at_millis);
    try out.append(alloc, if (record.local_leader) 1 else 0);
    try appendInt(alloc, out, u64, record.created_at_millis);
    try out.append(alloc, if (record.transition_pending) 1 else 0);
    try out.append(alloc, if (record.replay_required) 1 else 0);
    try out.append(alloc, if (record.replay_caught_up) 1 else 0);
    try out.append(alloc, if (record.cutover_ready) 1 else 0);
    try out.append(alloc, if (record.reads_ready_after_cutover) 1 else 0);
    try out.append(alloc, if (record.local_voter) 1 else 0);
    try appendInt(alloc, out, u16, record.voter_count);
    try out.append(alloc, if (record.joint_consensus) 1 else 0);
}

fn readGroupStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.GroupStatusReport {
    _ = alloc;
    const group_id = try readInt(encoded, pos, u64);
    const doc_count = try readInt(encoded, pos, u64);
    const disk_bytes = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const empty = encoded[pos.*] != 0;
    pos.* += 1;
    const median_key_len = try readInt(encoded, pos, u32);
    if (pos.* + median_key_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    pos.* += median_key_len;
    const updated_at_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const local_leader = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const created_at_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const transition_pending = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const replay_required = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const replay_caught_up = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const cutover_ready = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const reads_ready_after_cutover = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const local_voter = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const voter_count = if (pos.* + @sizeOf(u16) <= encoded.len)
        try readInt(encoded, pos, u16)
    else
        0;
    const joint_consensus = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    return .{
        .group_id = group_id,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .empty = empty,
        .created_at_millis = created_at_millis,
        .updated_at_millis = updated_at_millis,
        .local_leader = local_leader,
        .local_voter = local_voter,
        .voter_count = voter_count,
        .joint_consensus = joint_consensus,
        .transition_pending = transition_pending,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = cutover_ready,
        .reads_ready_after_cutover = reads_ready_after_cutover,
    };
}

fn appendPlacementIntent(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    intent: raft_reconciler.PlacementIntent,
) !void {
    try appendInt(alloc, out, u64, intent.record.group_id);
    try appendInt(alloc, out, u64, intent.record.replica_id);
    try appendInt(alloc, out, u64, intent.record.local_node_id);
    try appendInt(alloc, out, u64, intent.record.metadata_version);
    try out.append(alloc, @intFromEnum(intent.record.bootstrap_mode));
    try appendInt(alloc, out, u32, @intCast(intent.peer_node_ids.len));
    for (intent.peer_node_ids) |node_id| try appendInt(alloc, out, u64, node_id);
    try appendInt(alloc, out, u64, intent.store_id);
    const source_tag: u8 = if (intent.record.snapshot_bootstrap != null)
        1
    else if (intent.record.backup_restore_bootstrap != null)
        2
    else
        0;
    try out.append(alloc, source_tag);
    switch (source_tag) {
        1 => {
            const snapshot = intent.record.snapshot_bootstrap.?;
            try appendInt(alloc, out, u64, snapshot.from_node_id);
            try appendInt(alloc, out, u64, snapshot.term);
            try appendInt(alloc, out, u32, @intCast(snapshot.snapshot_id.len));
            try out.appendSlice(alloc, snapshot.snapshot_id);
            try appendInt(alloc, out, u32, @intCast(snapshot.uri.len));
            try out.appendSlice(alloc, snapshot.uri);
        },
        2 => {
            const backup = intent.record.backup_restore_bootstrap.?;
            try appendInt(alloc, out, u32, @intCast(backup.backup_id.len));
            try out.appendSlice(alloc, backup.backup_id);
            try appendInt(alloc, out, u32, @intCast(backup.location.len));
            try out.appendSlice(alloc, backup.location);
            try appendInt(alloc, out, u32, @intCast(backup.snapshot_path.len));
            try out.appendSlice(alloc, backup.snapshot_path);
        },
        else => {},
    }
}

fn appendDatabaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.DatabaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.database_id);
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.settings_json);
    try appendRequiredString(alloc, out, record.tablespace_name);
}

fn readDatabaseRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.DatabaseRecord {
    const database_id = try readInt(encoded, pos, u64);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const settings_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(settings_json);
    const tablespace_name = try readOptionalRequiredString(alloc, encoded, pos, "");
    errdefer alloc.free(tablespace_name);
    return .{
        .database_id = database_id,
        .name = name,
        .settings_json = settings_json,
        .tablespace_name = tablespace_name,
    };
}

fn appendNamespaceRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.NamespaceRecord,
) !void {
    try appendInt(alloc, out, u64, record.namespace_id);
    try appendInt(alloc, out, u64, record.database_id);
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.tablespace_name);
}

fn readNamespaceRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.NamespaceRecord {
    const namespace_id = try readInt(encoded, pos, u64);
    const database_id = try readInt(encoded, pos, u64);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const tablespace_name = try readOptionalRequiredString(alloc, encoded, pos, "");
    errdefer alloc.free(tablespace_name);
    return .{
        .namespace_id = namespace_id,
        .database_id = database_id,
        .name = name,
        .tablespace_name = tablespace_name,
    };
}

fn appendTablespaceRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.TablespaceRecord,
) !void {
    try appendInt(alloc, out, u64, record.tablespace_id);
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.location_json);
    try appendRequiredString(alloc, out, record.placement_policy_json);
}

fn readTablespaceRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TablespaceRecord {
    const tablespace_id = try readInt(encoded, pos, u64);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const location_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(location_json);
    const placement_policy_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_policy_json);
    return .{
        .tablespace_id = tablespace_id,
        .name = name,
        .location_json = location_json,
        .placement_policy_json = placement_policy_json,
    };
}

fn appendSequenceRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SequenceRecord,
) !void {
    try appendInt(alloc, out, u64, record.sequence_id);
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.database_name);
    try appendRequiredString(alloc, out, record.namespace_name);
    try appendRequiredString(alloc, out, record.options_json);
    try appendInt(alloc, out, i64, record.last_value);
    try appendInt(alloc, out, u128, record.last_allocation_id);
}

fn readSequenceRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.SequenceRecord {
    const sequence_id = try readInt(encoded, pos, u64);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const database_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(database_name);
    const namespace_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(namespace_name);
    const options_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(options_json);
    const last_value = if (pos.* < encoded.len)
        try readInt(encoded, pos, i64)
    else
        try metadata.sequenceInitialLastValueFromOptionsJson(alloc, options_json);
    const last_allocation_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u128)
    else
        0;
    return .{
        .sequence_id = sequence_id,
        .name = name,
        .database_name = database_name,
        .namespace_name = namespace_name,
        .options_json = options_json,
        .last_value = last_value,
        .last_allocation_id = last_allocation_id,
    };
}

fn appendTableRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.TableRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u16, record.desired_replica_count);
    try appendInt(alloc, out, u32, record.min_ranges);
    try appendInt(alloc, out, u32, @intCast(record.database_name.len));
    try out.appendSlice(alloc, record.database_name);
    try appendInt(alloc, out, u32, @intCast(record.namespace_name.len));
    try out.appendSlice(alloc, record.namespace_name);
    try appendInt(alloc, out, u32, @intCast(record.name.len));
    try out.appendSlice(alloc, record.name);
    try appendInt(alloc, out, u32, @intCast(record.description.len));
    try out.appendSlice(alloc, record.description);
    try appendInt(alloc, out, u32, @intCast(record.schema_json.len));
    try out.appendSlice(alloc, record.schema_json);
    try appendInt(alloc, out, u32, @intCast(record.read_schema_json.len));
    try out.appendSlice(alloc, record.read_schema_json);
    try appendInt(alloc, out, u32, @intCast(record.foreign_key_validation_json.len));
    try out.appendSlice(alloc, record.foreign_key_validation_json);
    try appendInt(alloc, out, u32, @intCast(record.indexes_json.len));
    try out.appendSlice(alloc, record.indexes_json);
    try appendInt(alloc, out, u32, @intCast(record.replication_sources_json.len));
    try out.appendSlice(alloc, record.replication_sources_json);
    try appendInt(alloc, out, u32, @intCast(record.placement_role.len));
    try out.appendSlice(alloc, record.placement_role);
    try appendInt(alloc, out, u32, @intCast(record.restore_backup_id.len));
    try out.appendSlice(alloc, record.restore_backup_id);
    try appendInt(alloc, out, u32, @intCast(record.restore_location.len));
    try out.appendSlice(alloc, record.restore_location);
    try appendRequiredString(alloc, out, record.tablespace_name);
    try appendInt(alloc, out, u64, record.data_generation);
}

fn appendSchemaProgressRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SchemaProgressRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, record.schema_version);
}

fn appendRestoreProgressRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RestoreProgressRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u32, @intCast(record.backup_id.len));
    try out.appendSlice(alloc, record.backup_id);
    try appendInt(alloc, out, u32, @intCast(record.snapshot_path.len));
    try out.appendSlice(alloc, record.snapshot_path);
    try out.append(alloc, if (record.primary_restored) 1 else 0);
    try out.append(alloc, if (record.runtime_repair_complete) 1 else 0);
    try appendInt(alloc, out, u32, @intCast(record.phase.len));
    try out.appendSlice(alloc, record.phase);
    try appendInt(alloc, out, u32, @intCast(record.last_error.len));
    try out.appendSlice(alloc, record.last_error);
    try appendInt(alloc, out, u64, record.updated_at_ms);
}

fn appendReplicationSourceStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReplicationSourceStatusRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, record.source_ordinal);
    try appendInt(alloc, out, u32, @intCast(record.source_kind.len));
    try out.appendSlice(alloc, record.source_kind);
    try appendInt(alloc, out, u32, @intCast(record.external_table.len));
    try out.appendSlice(alloc, record.external_table);
    try appendInt(alloc, out, u32, @intCast(record.slot_name.len));
    try out.appendSlice(alloc, record.slot_name);
    try appendInt(alloc, out, u32, @intCast(record.publication_name.len));
    try out.appendSlice(alloc, record.publication_name);
    try appendInt(alloc, out, u32, @intCast(record.phase.len));
    try out.appendSlice(alloc, record.phase);
    try appendInt(alloc, out, u32, @intCast(record.checkpoint.len));
    try out.appendSlice(alloc, record.checkpoint);
    try appendInt(alloc, out, u64, record.snapshot_offset);
    try appendInt(alloc, out, u32, @intCast(record.stream_checkpoint.len));
    try out.appendSlice(alloc, record.stream_checkpoint);
    try appendInt(alloc, out, u32, @intCast(record.last_error.len));
    try out.appendSlice(alloc, record.last_error);
    try appendInt(alloc, out, u64, record.lag_records);
    try appendInt(alloc, out, u64, record.updated_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.prepared_checkpoint.len));
    try out.appendSlice(alloc, record.prepared_checkpoint);
    try appendInt(alloc, out, u32, @intCast(record.cutover_mode.len));
    try out.appendSlice(alloc, record.cutover_mode);
    try appendInt(alloc, out, u64, record.consecutive_failures);
    try appendInt(alloc, out, u64, record.last_success_at_ms);
    try appendInt(alloc, out, u64, record.last_change_applied_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.failure_class.len));
    try out.appendSlice(alloc, record.failure_class);
    try appendInt(alloc, out, u64, record.lag_millis);
    try appendInt(alloc, out, u64, record.last_source_commit_at_ms);
}

fn appendRangeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RangeRecord,
) !void {
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, @intCast(record.start_key.len));
    try out.appendSlice(alloc, record.start_key);
    if (record.end_key) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    try appendInt(alloc, out, u32, @intCast(record.restore_backup_id.len));
    try out.appendSlice(alloc, record.restore_backup_id);
    try appendInt(alloc, out, u32, @intCast(record.restore_location.len));
    try out.appendSlice(alloc, record.restore_location);
    try appendInt(alloc, out, u32, @intCast(record.restore_snapshot_path.len));
    try out.appendSlice(alloc, record.restore_snapshot_path);
    const range_id = if (record.range_id == 0) record.group_id else record.range_id;
    try appendInt(alloc, out, u64, range_id);
    try appendInt(alloc, out, u64, record.doc_identity_shard_id);
    try appendInt(alloc, out, u64, record.doc_identity_range_id);
}

fn appendForeignKeyReferenceRangeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ForeignKeyReferenceRangeRecord,
) !void {
    try appendInt(alloc, out, u64, record.child_table_id);
    try appendInt(alloc, out, u32, @intCast(record.constraint_name.len));
    try out.appendSlice(alloc, record.constraint_name);
    try appendInt(alloc, out, u64, record.parent_table_id);
    try appendInt(alloc, out, u32, @intCast(record.start_parent_key.len));
    try out.appendSlice(alloc, record.start_parent_key);
    if (record.end_parent_key) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.range_id);
    try appendInt(alloc, out, u64, record.topology_epoch);
    try appendInt(alloc, out, u32, @intCast(record.state.len));
    try out.appendSlice(alloc, record.state);
}

fn appendForeignKeyReferenceRangeSelector(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    selector: metadata_table_manager.ForeignKeyReferenceRangeSelector,
) !void {
    try appendInt(alloc, out, u64, selector.child_table_id);
    try appendInt(alloc, out, u32, @intCast(selector.constraint_name.len));
    try out.appendSlice(alloc, selector.constraint_name);
    try appendInt(alloc, out, u64, selector.parent_table_id);
    try appendInt(alloc, out, u32, @intCast(selector.start_parent_key.len));
    try out.appendSlice(alloc, selector.start_parent_key);
    try appendInt(alloc, out, u64, selector.range_id);
}

fn appendForeignKeyReferenceRangeSplitRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.ForeignKeyReferenceRangeSplitRequest,
) !void {
    try appendForeignKeyReferenceRangeSelector(alloc, out, request.selector);
    try appendInt(alloc, out, u32, @intCast(request.split_parent_key.len));
    try out.appendSlice(alloc, request.split_parent_key);
    try appendInt(alloc, out, u64, request.left_group_id);
    try appendInt(alloc, out, u64, request.right_group_id);
}

fn appendForeignKeyReferenceRangeMergeRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.ForeignKeyReferenceRangeMergeRequest,
) !void {
    try appendForeignKeyReferenceRangeSelector(alloc, out, request.left_selector);
    try appendInt(alloc, out, u32, @intCast(request.right_start_parent_key.len));
    try out.appendSlice(alloc, request.right_start_parent_key);
    try appendInt(alloc, out, u64, request.merged_group_id);
}

fn appendUniqueConstraintRangeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.UniqueConstraintRangeRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, @intCast(record.constraint_name.len));
    try out.appendSlice(alloc, record.constraint_name);
    try appendInt(alloc, out, u32, @intCast(record.start_encoded_value.len));
    try out.appendSlice(alloc, record.start_encoded_value);
    if (record.end_encoded_value) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.range_id);
    try appendInt(alloc, out, u64, record.topology_epoch);
    try appendInt(alloc, out, u32, @intCast(record.state.len));
    try out.appendSlice(alloc, record.state);
}

fn appendUniqueConstraintRangeSelector(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    selector: metadata_table_manager.UniqueConstraintRangeSelector,
) !void {
    try appendInt(alloc, out, u64, selector.table_id);
    try appendInt(alloc, out, u32, @intCast(selector.constraint_name.len));
    try out.appendSlice(alloc, selector.constraint_name);
    try appendInt(alloc, out, u32, @intCast(selector.start_encoded_value.len));
    try out.appendSlice(alloc, selector.start_encoded_value);
    try appendInt(alloc, out, u64, selector.range_id);
}

fn appendUniqueConstraintRangeSplitRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.UniqueConstraintRangeSplitRequest,
) !void {
    try appendUniqueConstraintRangeSelector(alloc, out, request.selector);
    try appendInt(alloc, out, u32, @intCast(request.split_encoded_value.len));
    try out.appendSlice(alloc, request.split_encoded_value);
    try appendInt(alloc, out, u64, request.left_group_id);
    try appendInt(alloc, out, u64, request.right_group_id);
}

fn appendUniqueConstraintRangeMergeRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.UniqueConstraintRangeMergeRequest,
) !void {
    try appendUniqueConstraintRangeSelector(alloc, out, request.left_selector);
    try appendInt(alloc, out, u32, @intCast(request.right_start_encoded_value.len));
    try out.appendSlice(alloc, request.right_start_encoded_value);
    try appendInt(alloc, out, u64, request.merged_group_id);
}

fn appendSecondaryIndexRebuildRangeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SecondaryIndexRebuildRangeRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendRequiredString(alloc, out, record.index_name);
    try appendInt(alloc, out, u64, record.index_generation);
    try appendRequiredString(alloc, out, record.start_row_key);
    if (record.end_row_key) |end| {
        try out.append(alloc, 1);
        try appendRequiredString(alloc, out, end);
    } else {
        try out.append(alloc, 0);
    }
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.topology_epoch);
    try appendRequiredString(alloc, out, record.state);
    try appendRequiredString(alloc, out, record.lease_owner);
    try appendInt(alloc, out, u64, record.lease_expires_at_ms);
    try appendInt(alloc, out, u32, record.attempts);
    try appendInt(alloc, out, u64, record.completed_row_count);
    try appendRequiredString(alloc, out, record.progress_row_key);
    try appendRequiredString(alloc, out, record.last_error);
    try appendInt(alloc, out, u64, record.range_id);
}

fn appendSecondaryIndexRebuildRangeSelector(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector,
) !void {
    try appendInt(alloc, out, u64, selector.table_id);
    try appendRequiredString(alloc, out, selector.index_name);
    try appendInt(alloc, out, u64, selector.index_generation);
    try appendRequiredString(alloc, out, selector.start_row_key);
}

fn appendSecondaryIndexRebuildRangeBeginRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest,
) !void {
    try appendSecondaryIndexRebuildRangeSelector(alloc, out, request.selector);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendInt(alloc, out, u64, request.now_ms);
    try appendInt(alloc, out, u64, request.lease_expires_at_ms);
}

fn appendSecondaryIndexRebuildRangeFinishRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest,
) !void {
    try appendSecondaryIndexRebuildRangeSelector(alloc, out, request.selector);
    try appendInt(alloc, out, u64, request.completed_row_count);
    try appendRequiredString(alloc, out, request.progress_row_key);
}

fn appendSecondaryIndexRebuildRangeInvalidateRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest,
) !void {
    try appendSecondaryIndexRebuildRangeSelector(alloc, out, request.selector);
    try appendRequiredString(alloc, out, request.last_error);
}

fn schemaRewriteExpressionJsonAlloc(
    alloc: std.mem.Allocator,
    expression: runtime_schema.RelationalRowsExpression,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, expression, .{});
}

fn parseSchemaRewriteExpressionJsonAlloc(
    alloc: std.mem.Allocator,
    expression_json: []const u8,
) !runtime_schema.RelationalRowsExpression {
    var parsed = try std.json.parseFromSlice(runtime_schema.RelationalRowsExpression, alloc, expression_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, parsed.value);
}

fn appendSchemaRewriteJobRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SchemaRewriteJobRecord,
) !void {
    try appendInt(alloc, out, u64, record.job_id);
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.schema_generation);
    try appendRequiredString(alloc, out, record.action);
    try appendRequiredString(alloc, out, record.reason);
    try appendRequiredString(alloc, out, record.start_row_key);
    try appendOptionalString(alloc, out, record.end_row_key);
    try appendRequiredString(alloc, out, record.state);
    try appendRequiredString(alloc, out, record.target_column);
    if (record.expression) |expression| {
        try out.append(alloc, 1);
        const expression_json = try schemaRewriteExpressionJsonAlloc(alloc, expression);
        defer alloc.free(expression_json);
        try appendRequiredString(alloc, out, expression_json);
    } else {
        try out.append(alloc, 0);
    }
    try out.append(alloc, if (record.full_row_rewrite) 1 else 0);
    try appendInt(alloc, out, u32, @intCast(record.rewrite_renames.len));
    for (record.rewrite_renames) |rename| {
        try appendRequiredString(alloc, out, rename.old_path);
        try appendRequiredString(alloc, out, rename.new_path);
    }
    try appendInt(alloc, out, u32, @intCast(record.rewrite_drops.len));
    for (record.rewrite_drops) |drop| try appendRequiredString(alloc, out, drop);
    try appendRequiredString(alloc, out, record.lease_owner);
    try appendInt(alloc, out, u64, record.lease_expires_at_ms);
    try appendInt(alloc, out, u32, record.attempts);
    try appendInt(alloc, out, u64, record.completed_row_count);
    try appendRequiredString(alloc, out, record.progress_row_key);
    try appendRequiredString(alloc, out, record.last_error);
    try appendInt(alloc, out, u64, record.range_id);
}

fn appendTableCatalogUpdateWithSchemaRewriteJobsRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
) !void {
    try appendTableRecord(alloc, out, request.table);
    try appendInt(alloc, out, u64, @intCast(request.schema_rewrite_jobs.len));
    for (request.schema_rewrite_jobs) |record| try appendSchemaRewriteJobRecord(alloc, out, record);
}

fn appendTableCatalogBatchUpdateWithSchemaRewriteJobsRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
) !void {
    try appendInt(alloc, out, u64, @intCast(request.tables.len));
    for (request.tables) |record| try appendTableRecord(alloc, out, record);
    try appendInt(alloc, out, u64, @intCast(request.schema_rewrite_jobs.len));
    for (request.schema_rewrite_jobs) |record| try appendSchemaRewriteJobRecord(alloc, out, record);
}

fn appendTableCatalogDropWithSchemaRewriteJobsRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest,
) !void {
    try appendInt(alloc, out, u64, request.table_id);
    try appendInt(alloc, out, u64, @intCast(request.sequence_ids.len));
    for (request.sequence_ids) |sequence_id| try appendInt(alloc, out, u64, sequence_id);
    try appendInt(alloc, out, u64, @intCast(request.range_group_ids.len));
    for (request.range_group_ids) |range_group_id| try appendInt(alloc, out, u64, range_group_id);
    try appendInt(alloc, out, u64, @intCast(request.table_updates.len));
    for (request.table_updates) |record| try appendTableRecord(alloc, out, record);
    try appendInt(alloc, out, u64, @intCast(request.schema_rewrite_jobs.len));
    for (request.schema_rewrite_jobs) |record| try appendSchemaRewriteJobRecord(alloc, out, record);
}

fn appendSchemaRewriteJobBeginRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SchemaRewriteJobBeginRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendInt(alloc, out, u64, request.now_ms);
    try appendInt(alloc, out, u64, request.lease_expires_at_ms);
}

fn appendSchemaRewriteJobFinishRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SchemaRewriteJobFinishRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendInt(alloc, out, u64, request.completed_row_count);
    try appendRequiredString(alloc, out, request.progress_row_key);
}

fn appendSchemaRewriteJobInvalidateRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SchemaRewriteJobInvalidateRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendRequiredString(alloc, out, request.last_error);
}

fn appendSchemaRewriteJobControlRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SchemaRewriteJobControlRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.reason);
}

fn appendTableEmptyingJobRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.TableEmptyingJobRecord,
) !void {
    try appendInt(alloc, out, u64, record.job_id);
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.schema_generation);
    try appendRequiredString(alloc, out, record.start_row_key);
    try appendOptionalString(alloc, out, record.end_row_key);
    try appendInt(alloc, out, u32, @intCast(record.affected_table_ids.len));
    for (record.affected_table_ids) |table_id| try appendInt(alloc, out, u64, table_id);
    try out.append(alloc, if (record.restart_identity) 1 else 0);
    try out.append(alloc, if (record.cascade) 1 else 0);
    try appendRequiredString(alloc, out, record.state);
    try appendRequiredString(alloc, out, record.lease_owner);
    try appendInt(alloc, out, u64, record.lease_expires_at_ms);
    try appendInt(alloc, out, u32, record.attempts);
    try appendInt(alloc, out, u64, record.completed_row_count);
    try appendRequiredString(alloc, out, record.progress_row_key);
    try appendRequiredString(alloc, out, record.last_error);
    try appendInt(alloc, out, u64, record.data_generation);
    try appendInt(alloc, out, u64, record.barrier_id);
    try appendInt(alloc, out, u64, record.range_id);
}

fn appendTableEmptyingJobBeginRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableEmptyingJobBeginRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendInt(alloc, out, u64, request.now_ms);
    try appendInt(alloc, out, u64, request.lease_expires_at_ms);
}

fn appendTableEmptyingJobFinishRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableEmptyingJobFinishRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendInt(alloc, out, u64, request.completed_row_count);
    try appendRequiredString(alloc, out, request.progress_row_key);
}

fn appendTableEmptyingJobInvalidateRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableEmptyingJobInvalidateRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.lease_owner);
    try appendRequiredString(alloc, out, request.last_error);
}

fn appendTableEmptyingJobControlRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableEmptyingJobControlRequest,
) !void {
    try appendInt(alloc, out, u64, request.job_id);
    try appendRequiredString(alloc, out, request.reason);
}

fn appendTableEmptyingBarrierPromotionRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableEmptyingBarrierPromotionRequest,
) !void {
    try appendInt(alloc, out, u64, @intCast(request.job_ids.len));
    for (request.job_ids) |job_id| try appendInt(alloc, out, u64, job_id);
    try appendInt(alloc, out, u64, @intCast(request.promotions.len));
    for (request.promotions) |promotion| {
        try appendInt(alloc, out, u64, promotion.table_id);
        try appendInt(alloc, out, u64, promotion.target_generation);
    }
}

fn appendSecondaryIndexReadyPromotionRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.SecondaryIndexReadyPromotionRequest,
) !void {
    try appendInt(alloc, out, u64, request.table_id);
    try appendRequiredString(alloc, out, request.index_name);
    try appendInt(alloc, out, u64, request.expected_index_generation);
    try appendRequiredString(alloc, out, request.expected_schema_json);
    try appendTableRecord(alloc, out, request.promoted_table);
}

fn appendTableSchemaCompareAndSwapRequest(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    request: metadata_table_manager.TableSchemaCompareAndSwapRequest,
) !void {
    try appendInt(alloc, out, u64, request.table_id);
    try appendRequiredString(alloc, out, request.expected_schema_json);
    try appendTableRecord(alloc, out, request.promoted_table);
}

fn appendSplitTransitionRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SplitTransitionRecord,
) !void {
    try appendInt(alloc, out, u64, record.transition_id);
    try appendInt(alloc, out, u64, record.source_group_id);
    try appendInt(alloc, out, u64, record.destination_group_id);
    try out.append(alloc, @intFromEnum(record.phase));
    if (record.split_key) |split_key| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(split_key.len));
        try out.appendSlice(alloc, split_key);
    } else {
        try out.append(alloc, 0);
    }
    if (record.source_range_end) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    if (record.rollback_reason) |reason| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(reason.len));
        try out.appendSlice(alloc, reason);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendMergeTransitionRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.MergeTransitionRecord,
) !void {
    try appendInt(alloc, out, u64, record.transition_id);
    try appendInt(alloc, out, u64, record.donor_group_id);
    try appendInt(alloc, out, u64, record.receiver_group_id);
    try out.append(alloc, @intFromEnum(record.phase));
    if (record.rollback_reason) |reason| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(reason.len));
        try out.appendSlice(alloc, reason);
    } else {
        try out.append(alloc, 0);
    }
    try out.append(alloc, if (record.allow_doc_identity_reassignment) 1 else 0);
}

fn appendReconcileLeaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReconcileLeaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.owner_node_id);
    try appendInt(alloc, out, u64, record.expires_at_ms);
}

fn appendShuffleJoinLeaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ShuffleJoinLeaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.job_id);
    try appendInt(alloc, out, u64, record.owner_group_id);
    try appendInt(alloc, out, u64, record.expires_at_ms);
}

fn appendReallocationRequestRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReallocationRequestRecord,
) !void {
    try appendInt(alloc, out, u64, record.requested_at_ms);
}

fn readTableRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const start = pos.*;
    const data_generation_record = readTableRecordWithDataGeneration(alloc, encoded, pos) catch null;
    if (data_generation_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const catalog_identity_record = readTableRecordWithCatalogIdentity(alloc, encoded, pos) catch null;
    if (catalog_identity_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const newest_record = readTableRecordWithForeignKeyValidation(alloc, encoded, pos) catch null;
    if (newest_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const restore_intent_record = readTableRecordWithRestoreIntent(alloc, encoded, pos) catch null;
    if (restore_intent_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const old_record = readTableRecordLegacy(alloc, encoded, pos) catch null;
    if (old_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const record = try readTableRecordWithReadSchema(alloc, encoded, pos);
    if (pos.* != encoded.len) {
        metadata_table_manager.freeTable(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

const OwnedTableCatalogIdentity = struct {
    database_name: []u8,
    namespace_name: []u8,

    fn deinit(self: OwnedTableCatalogIdentity, alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        alloc.free(self.namespace_name);
    }
};

fn defaultTableCatalogIdentity(alloc: std.mem.Allocator) !OwnedTableCatalogIdentity {
    const database_name = try alloc.dupe(u8, metadata_table_manager.default_database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, metadata_table_manager.default_namespace_name);
    return .{
        .database_name = database_name,
        .namespace_name = namespace_name,
    };
}

fn readTableRecordLegacy(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const catalog_identity = try defaultTableCatalogIdentity(alloc);
    errdefer catalog_identity.deinit(alloc);
    return .{
        .table_id = table_id,
        .name = name,
        .database_name = catalog_identity.database_name,
        .namespace_name = catalog_identity.namespace_name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = try alloc.dupe(u8, ""),
        .foreign_key_validation_json = try alloc.dupe(u8, "{}"),
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = try alloc.dupe(u8, ""),
        .restore_location = try alloc.dupe(u8, ""),
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithReadSchema(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const catalog_identity = try defaultTableCatalogIdentity(alloc);
    errdefer catalog_identity.deinit(alloc);
    return .{
        .table_id = table_id,
        .name = name,
        .database_name = catalog_identity.database_name,
        .namespace_name = catalog_identity.namespace_name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .foreign_key_validation_json = try alloc.dupe(u8, "{}"),
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = try alloc.dupe(u8, ""),
        .restore_location = try alloc.dupe(u8, ""),
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithRestoreIntent(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_location);
    const tablespace_name = try readOptionalRequiredString(alloc, encoded, pos, "");
    errdefer alloc.free(tablespace_name);
    const catalog_identity = try defaultTableCatalogIdentity(alloc);
    errdefer catalog_identity.deinit(alloc);
    return .{
        .table_id = table_id,
        .name = name,
        .database_name = catalog_identity.database_name,
        .namespace_name = catalog_identity.namespace_name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .foreign_key_validation_json = try alloc.dupe(u8, "{}"),
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .tablespace_name = tablespace_name,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithForeignKeyValidation(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const foreign_key_validation_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(foreign_key_validation_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_location);
    const catalog_identity = try defaultTableCatalogIdentity(alloc);
    errdefer catalog_identity.deinit(alloc);
    return .{
        .table_id = table_id,
        .name = name,
        .database_name = catalog_identity.database_name,
        .namespace_name = catalog_identity.namespace_name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .foreign_key_validation_json = foreign_key_validation_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithDataGeneration(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    var record = try readTableRecordWithCatalogIdentity(alloc, encoded, pos);
    errdefer metadata_table_manager.freeTable(alloc, record);
    record.data_generation = try readInt(encoded, pos, u64);
    return record;
}

fn readTableRecordWithCatalogIdentity(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const database_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(database_name);
    const namespace_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(namespace_name);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const foreign_key_validation_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(foreign_key_validation_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_location);
    const tablespace_name = try readOptionalRequiredString(alloc, encoded, pos, "");
    errdefer alloc.free(tablespace_name);
    return .{
        .table_id = table_id,
        .name = name,
        .database_name = database_name,
        .namespace_name = namespace_name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .foreign_key_validation_json = foreign_key_validation_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .tablespace_name = tablespace_name,
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readRequiredString(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) ![]u8 {
    const value_len = try readInt(encoded, pos, u32);
    if (pos.* + value_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + value_len]);
    pos.* += value_len;
    return value;
}

fn readOptionalRequiredString(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
    default_value: []const u8,
) ![]u8 {
    if (pos.* >= encoded.len) return try alloc.dupe(u8, default_value);
    return try readRequiredString(alloc, encoded, pos);
}

fn readJsonRecord(comptime T: type, alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !T {
    const json = try readRequiredString(alloc, encoded, pos);
    defer alloc.free(json);
    var value = try std.json.parseFromSliceLeaky(T, alloc, json, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer deinitExtensionJsonRecord(T, alloc, &value);
    if (@hasDecl(T, "validate")) try value.validate();
    return value;
}

fn appendRequiredString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    try appendInt(alloc, out, u32, @intCast(value.len));
    try out.appendSlice(alloc, value);
}

fn appendOptionalString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: ?[]const u8,
) !void {
    if (value) |text| {
        try out.append(alloc, 1);
        try appendRequiredString(alloc, out, text);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendJsonRecord(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), record: anytype) !void {
    const json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
    defer alloc.free(json);
    try appendRequiredString(alloc, out, json);
}

fn deinitExtensionJsonRecord(comptime T: type, alloc: std.mem.Allocator, value: *T) void {
    if (@hasDecl(T, "deinitOwned")) {
        value.deinitOwned(alloc);
    }
}

fn freeExtensionLifecycleDelta(alloc: std.mem.Allocator, delta: ExtensionLifecycleDelta) void {
    for (delta.upsert_tables) |record| metadata_table_manager.freeTable(alloc, record);
    if (delta.upsert_tables.len > 0) alloc.free(@constCast(delta.upsert_tables));
    for (delta.upsert_installed_extensions) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (delta.upsert_installed_extensions.len > 0) alloc.free(@constCast(delta.upsert_installed_extensions));
    for (delta.remove_installed_extensions) |name| alloc.free(@constCast(name));
    if (delta.remove_installed_extensions.len > 0) alloc.free(@constCast(delta.remove_installed_extensions));
    for (delta.upsert_extension_members) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (delta.upsert_extension_members.len > 0) alloc.free(@constCast(delta.upsert_extension_members));
    for (delta.remove_extension_members) |record| {
        alloc.free(@constCast(record.extension_name));
        alloc.free(@constCast(record.object_name));
    }
    if (delta.remove_extension_members.len > 0) alloc.free(@constCast(delta.remove_extension_members));
    for (delta.upsert_extension_dependencies) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (delta.upsert_extension_dependencies.len > 0) alloc.free(@constCast(delta.upsert_extension_dependencies));
    for (delta.remove_extension_dependencies) |record| {
        alloc.free(@constCast(record.extension_name));
        alloc.free(@constCast(record.required_extension_name));
        alloc.free(@constCast(record.package_name));
    }
    if (delta.remove_extension_dependencies.len > 0) alloc.free(@constCast(delta.remove_extension_dependencies));
}

fn readRangeRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RangeRecord {
    const group_id = try readInt(encoded, pos, u64);
    const table_id = try readInt(encoded, pos, u64);
    const start_len = try readInt(encoded, pos, u32);
    if (pos.* + start_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const start_key = try alloc.dupe(u8, encoded[pos.* .. pos.* + start_len]);
    pos.* += start_len;
    const end_key = try readOptionalString(alloc, encoded, pos);
    const restore_backup_id = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_backup_id);
    const restore_location = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_location);
    const restore_snapshot_path = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_snapshot_path);
    const range_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        group_id;
    const doc_identity_shard_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const doc_identity_range_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    return .{
        .group_id = group_id,
        .range_id = if (range_id == 0) group_id else range_id,
        .table_id = table_id,
        .start_key = start_key,
        .end_key = end_key,
        .doc_identity_shard_id = doc_identity_shard_id,
        .doc_identity_range_id = doc_identity_range_id,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
    };
}

fn readForeignKeyReferenceRangeRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.ForeignKeyReferenceRangeRecord {
    const child_table_id = try readInt(encoded, pos, u64);
    const constraint_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(constraint_name);
    const parent_table_id = try readInt(encoded, pos, u64);
    const start_parent_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_parent_key);
    const end_parent_key = try readOptionalString(alloc, encoded, pos);
    errdefer if (end_parent_key) |end| alloc.free(end);
    const group_id = try readInt(encoded, pos, u64);
    const range_id = try readInt(encoded, pos, u64);
    const topology_epoch = try readInt(encoded, pos, u64);
    const state = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(state);
    return .{
        .child_table_id = child_table_id,
        .constraint_name = constraint_name,
        .parent_table_id = parent_table_id,
        .start_parent_key = start_parent_key,
        .end_parent_key = end_parent_key,
        .group_id = group_id,
        .range_id = range_id,
        .topology_epoch = topology_epoch,
        .state = state,
    };
}

fn readForeignKeyReferenceRangeSelector(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.ForeignKeyReferenceRangeSelector {
    const child_table_id = try readInt(encoded, pos, u64);
    const constraint_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(constraint_name);
    const parent_table_id = try readInt(encoded, pos, u64);
    const start_parent_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_parent_key);
    const range_id = try readInt(encoded, pos, u64);
    return .{
        .child_table_id = child_table_id,
        .constraint_name = constraint_name,
        .parent_table_id = parent_table_id,
        .start_parent_key = start_parent_key,
        .range_id = range_id,
    };
}

fn readForeignKeyReferenceRangeSplitRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.ForeignKeyReferenceRangeSplitRequest {
    const selector = try readForeignKeyReferenceRangeSelector(alloc, encoded, pos);
    errdefer freeForeignKeyReferenceRangeSelector(alloc, selector);
    const split_parent_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(split_parent_key);
    return .{
        .selector = selector,
        .split_parent_key = split_parent_key,
        .left_group_id = try readInt(encoded, pos, u64),
        .right_group_id = try readInt(encoded, pos, u64),
    };
}

fn readForeignKeyReferenceRangeMergeRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.ForeignKeyReferenceRangeMergeRequest {
    const left_selector = try readForeignKeyReferenceRangeSelector(alloc, encoded, pos);
    errdefer freeForeignKeyReferenceRangeSelector(alloc, left_selector);
    const right_start_parent_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(right_start_parent_key);
    return .{
        .left_selector = left_selector,
        .right_start_parent_key = right_start_parent_key,
        .merged_group_id = try readInt(encoded, pos, u64),
    };
}

fn readUniqueConstraintRangeRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.UniqueConstraintRangeRecord {
    const table_id = try readInt(encoded, pos, u64);
    const constraint_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(constraint_name);
    const start_encoded_value = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_encoded_value);
    const end_encoded_value = try readOptionalString(alloc, encoded, pos);
    errdefer if (end_encoded_value) |end| alloc.free(end);
    const group_id = try readInt(encoded, pos, u64);
    const range_id = try readInt(encoded, pos, u64);
    const topology_epoch = try readInt(encoded, pos, u64);
    const state = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(state);
    return .{
        .table_id = table_id,
        .constraint_name = constraint_name,
        .start_encoded_value = start_encoded_value,
        .end_encoded_value = end_encoded_value,
        .group_id = group_id,
        .range_id = range_id,
        .topology_epoch = topology_epoch,
        .state = state,
    };
}

fn readUniqueConstraintRangeSelector(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.UniqueConstraintRangeSelector {
    const table_id = try readInt(encoded, pos, u64);
    const constraint_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(constraint_name);
    const start_encoded_value = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_encoded_value);
    const range_id = try readInt(encoded, pos, u64);
    return .{
        .table_id = table_id,
        .constraint_name = constraint_name,
        .start_encoded_value = start_encoded_value,
        .range_id = range_id,
    };
}

fn readUniqueConstraintRangeSplitRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.UniqueConstraintRangeSplitRequest {
    const selector = try readUniqueConstraintRangeSelector(alloc, encoded, pos);
    errdefer freeUniqueConstraintRangeSelector(alloc, selector);
    const split_encoded_value = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(split_encoded_value);
    return .{
        .selector = selector,
        .split_encoded_value = split_encoded_value,
        .left_group_id = try readInt(encoded, pos, u64),
        .right_group_id = try readInt(encoded, pos, u64),
    };
}

fn readUniqueConstraintRangeMergeRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.UniqueConstraintRangeMergeRequest {
    const left_selector = try readUniqueConstraintRangeSelector(alloc, encoded, pos);
    errdefer freeUniqueConstraintRangeSelector(alloc, left_selector);
    const right_start_encoded_value = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(right_start_encoded_value);
    return .{
        .left_selector = left_selector,
        .right_start_encoded_value = right_start_encoded_value,
        .merged_group_id = try readInt(encoded, pos, u64),
    };
}

fn readSecondaryIndexRebuildRangeRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.SecondaryIndexRebuildRangeRecord {
    const table_id = try readInt(encoded, pos, u64);
    const index_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(index_name);
    const index_generation = try readInt(encoded, pos, u64);
    const start_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_row_key);
    const end_row_key = try readOptionalString(alloc, encoded, pos);
    errdefer if (end_row_key) |end| alloc.free(end);
    const group_id = try readInt(encoded, pos, u64);
    const topology_epoch = try readInt(encoded, pos, u64);
    const state = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(state);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const lease_expires_at_ms = try readInt(encoded, pos, u64);
    const attempts = try readInt(encoded, pos, u32);
    const completed_row_count = try readInt(encoded, pos, u64);
    const progress_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(progress_row_key);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const range_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    return .{
        .table_id = table_id,
        .index_name = index_name,
        .index_generation = index_generation,
        .start_row_key = start_row_key,
        .end_row_key = end_row_key,
        .group_id = group_id,
        .range_id = range_id,
        .topology_epoch = topology_epoch,
        .state = state,
        .lease_owner = lease_owner,
        .lease_expires_at_ms = lease_expires_at_ms,
        .attempts = attempts,
        .completed_row_count = completed_row_count,
        .progress_row_key = progress_row_key,
        .last_error = last_error,
    };
}

fn readSecondaryIndexRebuildRangeSelector(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SecondaryIndexRebuildRangeSelector {
    const table_id = try readInt(encoded, pos, u64);
    const index_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(index_name);
    const index_generation = try readInt(encoded, pos, u64);
    const start_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_row_key);
    return .{
        .table_id = table_id,
        .index_name = index_name,
        .index_generation = index_generation,
        .start_row_key = start_row_key,
    };
}

fn readSecondaryIndexRebuildRangeBeginRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest {
    const selector = try readSecondaryIndexRebuildRangeSelector(alloc, encoded, pos);
    errdefer freeSecondaryIndexRebuildRangeSelector(alloc, selector);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    return .{
        .selector = selector,
        .lease_owner = lease_owner,
        .now_ms = try readInt(encoded, pos, u64),
        .lease_expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readSecondaryIndexRebuildRangeFinishRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest {
    const selector = try readSecondaryIndexRebuildRangeSelector(alloc, encoded, pos);
    errdefer freeSecondaryIndexRebuildRangeSelector(alloc, selector);
    const completed_row_count = try readInt(encoded, pos, u64);
    const progress_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(progress_row_key);
    return .{
        .selector = selector,
        .completed_row_count = completed_row_count,
        .progress_row_key = progress_row_key,
    };
}

fn readSecondaryIndexRebuildRangeInvalidateRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest {
    const selector = try readSecondaryIndexRebuildRangeSelector(alloc, encoded, pos);
    errdefer freeSecondaryIndexRebuildRangeSelector(alloc, selector);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    return .{
        .selector = selector,
        .last_error = last_error,
    };
}

fn readSchemaRewriteJobRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.SchemaRewriteJobRecord {
    const job_id = try readInt(encoded, pos, u64);
    const table_id = try readInt(encoded, pos, u64);
    const group_id = try readInt(encoded, pos, u64);
    const schema_generation = try readInt(encoded, pos, u64);
    const action = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(action);
    const reason = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(reason);
    const start_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_row_key);
    const end_row_key = try readOptionalString(alloc, encoded, pos);
    errdefer if (end_row_key) |value| alloc.free(value);
    const state = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(state);
    const target_column = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(target_column);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const has_expression = encoded[pos.*] != 0;
    pos.* += 1;
    const expression = if (has_expression) blk: {
        const expression_json = try readRequiredString(alloc, encoded, pos);
        defer alloc.free(expression_json);
        break :blk try parseSchemaRewriteExpressionJsonAlloc(alloc, expression_json);
    } else null;
    errdefer if (expression) |expr| runtime_schema.freeRelationalRowsExpression(alloc, expr);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const full_row_rewrite = encoded[pos.*] != 0;
    pos.* += 1;
    const rewrite_renames_len = try readInt(encoded, pos, u32);
    const rewrite_renames = try alloc.alloc(metadata.SchemaRewriteRename, rewrite_renames_len);
    var rewrite_renames_initialized: usize = 0;
    errdefer {
        for (rewrite_renames[0..rewrite_renames_initialized]) |rename| {
            alloc.free(rename.old_path);
            alloc.free(rename.new_path);
        }
        alloc.free(rewrite_renames);
    }
    for (rewrite_renames, 0..) |*rename, i| {
        const old_path = try readRequiredString(alloc, encoded, pos);
        var old_path_transferred = false;
        errdefer if (!old_path_transferred) alloc.free(old_path);
        const new_path = try readRequiredString(alloc, encoded, pos);
        rename.* = .{ .old_path = old_path, .new_path = new_path };
        old_path_transferred = true;
        rewrite_renames_initialized = i + 1;
    }
    const rewrite_drops_len = try readInt(encoded, pos, u32);
    const rewrite_drops = try alloc.alloc([]const u8, rewrite_drops_len);
    var rewrite_drops_initialized: usize = 0;
    errdefer {
        for (rewrite_drops[0..rewrite_drops_initialized]) |drop| alloc.free(drop);
        alloc.free(rewrite_drops);
    }
    for (rewrite_drops, 0..) |*drop, i| {
        drop.* = try readRequiredString(alloc, encoded, pos);
        rewrite_drops_initialized = i + 1;
    }
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const lease_expires_at_ms = try readInt(encoded, pos, u64);
    const attempts = try readInt(encoded, pos, u32);
    const completed_row_count = try readInt(encoded, pos, u64);
    const progress_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(progress_row_key);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const range_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    return .{
        .job_id = job_id,
        .table_id = table_id,
        .group_id = group_id,
        .range_id = range_id,
        .schema_generation = schema_generation,
        .action = action,
        .reason = reason,
        .start_row_key = start_row_key,
        .end_row_key = end_row_key,
        .state = state,
        .target_column = target_column,
        .expression = expression,
        .full_row_rewrite = full_row_rewrite,
        .rewrite_renames = rewrite_renames,
        .rewrite_drops = rewrite_drops,
        .lease_owner = lease_owner,
        .lease_expires_at_ms = lease_expires_at_ms,
        .attempts = attempts,
        .completed_row_count = completed_row_count,
        .progress_row_key = progress_row_key,
        .last_error = last_error,
    };
}

fn readTableCatalogUpdateWithSchemaRewriteJobsRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest {
    const table = try readTableRecordWithDataGeneration(alloc, encoded, pos);
    errdefer metadata_table_manager.freeTable(alloc, table);
    const jobs_len = try readInt(encoded, pos, u64);
    if (jobs_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const jobs = try alloc.alloc(metadata_table_manager.SchemaRewriteJobRecord, @intCast(jobs_len));
    var initialized: usize = 0;
    errdefer {
        for (jobs[0..initialized]) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(jobs);
    }
    for (jobs, 0..) |*job, i| {
        job.* = try readSchemaRewriteJobRecord(alloc, encoded, pos);
        initialized = i + 1;
    }
    return .{
        .table = table,
        .schema_rewrite_jobs = jobs,
    };
}

fn readTableCatalogBatchUpdateWithSchemaRewriteJobsRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest {
    const tables_len = try readInt(encoded, pos, u64);
    if (tables_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const tables = try alloc.alloc(metadata.TableRecord, @intCast(tables_len));
    var initialized_tables: usize = 0;
    errdefer {
        for (tables[0..initialized_tables]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(tables);
    }
    for (tables, 0..) |*table, i| {
        table.* = try readTableRecordWithDataGeneration(alloc, encoded, pos);
        initialized_tables = i + 1;
    }

    const jobs_len = try readInt(encoded, pos, u64);
    if (jobs_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const jobs = try alloc.alloc(metadata_table_manager.SchemaRewriteJobRecord, @intCast(jobs_len));
    var initialized_jobs: usize = 0;
    errdefer {
        for (jobs[0..initialized_jobs]) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(jobs);
    }
    for (jobs, 0..) |*job, i| {
        job.* = try readSchemaRewriteJobRecord(alloc, encoded, pos);
        initialized_jobs = i + 1;
    }
    return .{
        .tables = tables,
        .schema_rewrite_jobs = jobs,
    };
}

fn readTableCatalogDropWithSchemaRewriteJobsRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest {
    const table_id = try readInt(encoded, pos, u64);
    const sequence_ids_len = try readInt(encoded, pos, u64);
    if (sequence_ids_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const sequence_ids = try alloc.alloc(u64, @intCast(sequence_ids_len));
    errdefer alloc.free(sequence_ids);
    for (sequence_ids) |*sequence_id| sequence_id.* = try readInt(encoded, pos, u64);

    const range_group_ids_len = try readInt(encoded, pos, u64);
    if (range_group_ids_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const range_group_ids = try alloc.alloc(u64, @intCast(range_group_ids_len));
    errdefer alloc.free(range_group_ids);
    for (range_group_ids) |*range_group_id| range_group_id.* = try readInt(encoded, pos, u64);

    const table_updates_len = try readInt(encoded, pos, u64);
    if (table_updates_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const table_updates = try alloc.alloc(metadata.TableRecord, @intCast(table_updates_len));
    var initialized_tables: usize = 0;
    errdefer {
        for (table_updates[0..initialized_tables]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(table_updates);
    }
    for (table_updates, 0..) |*table, i| {
        table.* = try readTableRecordWithDataGeneration(alloc, encoded, pos);
        initialized_tables = i + 1;
    }

    const jobs_len = try readInt(encoded, pos, u64);
    if (jobs_len > std.math.maxInt(usize)) return error.InvalidMetadataTransitionEncoding;
    const jobs = try alloc.alloc(metadata_table_manager.SchemaRewriteJobRecord, @intCast(jobs_len));
    var initialized_jobs: usize = 0;
    errdefer {
        for (jobs[0..initialized_jobs]) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(jobs);
    }
    for (jobs, 0..) |*job, i| {
        job.* = try readSchemaRewriteJobRecord(alloc, encoded, pos);
        initialized_jobs = i + 1;
    }
    return .{
        .table_id = table_id,
        .sequence_ids = sequence_ids,
        .range_group_ids = range_group_ids,
        .table_updates = table_updates,
        .schema_rewrite_jobs = jobs,
    };
}

fn readSchemaRewriteJobBeginRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SchemaRewriteJobBeginRequest {
    const job_id = try readInt(encoded, pos, u64);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    return .{
        .job_id = job_id,
        .lease_owner = lease_owner,
        .now_ms = try readInt(encoded, pos, u64),
        .lease_expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readSchemaRewriteJobFinishRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SchemaRewriteJobFinishRequest {
    const job_id = try readInt(encoded, pos, u64);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const completed_row_count = try readInt(encoded, pos, u64);
    const progress_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(progress_row_key);
    return .{
        .job_id = job_id,
        .lease_owner = lease_owner,
        .completed_row_count = completed_row_count,
        .progress_row_key = progress_row_key,
    };
}

fn readSchemaRewriteJobInvalidateRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SchemaRewriteJobInvalidateRequest {
    const job_id = try readInt(encoded, pos, u64);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    return .{
        .job_id = job_id,
        .lease_owner = lease_owner,
        .last_error = last_error,
    };
}

fn readSchemaRewriteJobControlRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SchemaRewriteJobControlRequest {
    const job_id = try readInt(encoded, pos, u64);
    const reason = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(reason);
    return .{
        .job_id = job_id,
        .reason = reason,
    };
}

fn readTableEmptyingJobRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableEmptyingJobRecord {
    const job_id = try readInt(encoded, pos, u64);
    const table_id = try readInt(encoded, pos, u64);
    const group_id = try readInt(encoded, pos, u64);
    const schema_generation = try readInt(encoded, pos, u64);
    const start_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(start_row_key);
    const end_row_key = try readOptionalString(alloc, encoded, pos);
    errdefer if (end_row_key) |value| alloc.free(value);
    const affected_table_ids_len = try readInt(encoded, pos, u32);
    const affected_table_ids = try alloc.alloc(u64, affected_table_ids_len);
    errdefer alloc.free(affected_table_ids);
    for (affected_table_ids) |*affected_table_id| {
        affected_table_id.* = try readInt(encoded, pos, u64);
    }
    if (pos.* + 2 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const restart_identity = encoded[pos.*] != 0;
    pos.* += 1;
    const cascade = encoded[pos.*] != 0;
    pos.* += 1;
    const state = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(state);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const lease_expires_at_ms = try readInt(encoded, pos, u64);
    const attempts = try readInt(encoded, pos, u32);
    const completed_row_count = try readInt(encoded, pos, u64);
    const progress_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(progress_row_key);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const data_generation = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const barrier_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const range_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    return .{
        .job_id = job_id,
        .table_id = table_id,
        .group_id = group_id,
        .range_id = range_id,
        .schema_generation = schema_generation,
        .data_generation = data_generation,
        .barrier_id = barrier_id,
        .start_row_key = start_row_key,
        .end_row_key = end_row_key,
        .affected_table_ids = affected_table_ids,
        .restart_identity = restart_identity,
        .cascade = cascade,
        .state = state,
        .lease_owner = lease_owner,
        .lease_expires_at_ms = lease_expires_at_ms,
        .attempts = attempts,
        .completed_row_count = completed_row_count,
        .progress_row_key = progress_row_key,
        .last_error = last_error,
    };
}

fn readTableEmptyingJobBeginRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableEmptyingJobBeginRequest {
    const job_id = try readInt(encoded, pos, u64);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    return .{
        .job_id = job_id,
        .lease_owner = lease_owner,
        .now_ms = try readInt(encoded, pos, u64),
        .lease_expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readTableEmptyingJobFinishRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableEmptyingJobFinishRequest {
    const job_id = try readInt(encoded, pos, u64);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const completed_row_count = try readInt(encoded, pos, u64);
    const progress_row_key = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(progress_row_key);
    return .{
        .job_id = job_id,
        .lease_owner = lease_owner,
        .completed_row_count = completed_row_count,
        .progress_row_key = progress_row_key,
    };
}

fn readTableEmptyingJobInvalidateRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableEmptyingJobInvalidateRequest {
    const job_id = try readInt(encoded, pos, u64);
    const lease_owner = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(lease_owner);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    return .{
        .job_id = job_id,
        .lease_owner = lease_owner,
        .last_error = last_error,
    };
}

fn readTableEmptyingJobControlRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableEmptyingJobControlRequest {
    const job_id = try readInt(encoded, pos, u64);
    const reason = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(reason);
    return .{
        .job_id = job_id,
        .reason = reason,
    };
}

fn readSecondaryIndexReadyPromotionRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.SecondaryIndexReadyPromotionRequest {
    const table_id = try readInt(encoded, pos, u64);
    const index_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(index_name);
    const expected_index_generation = try readInt(encoded, pos, u64);
    const expected_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(expected_schema_json);
    const promoted_table = try readTableRecord(alloc, encoded, pos);
    errdefer metadata_table_manager.freeTable(alloc, promoted_table);
    return .{
        .table_id = table_id,
        .index_name = index_name,
        .expected_index_generation = expected_index_generation,
        .expected_schema_json = expected_schema_json,
        .promoted_table = promoted_table,
    };
}

fn readTableEmptyingBarrierPromotionRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableEmptyingBarrierPromotionRequest {
    const job_count = try readInt(encoded, pos, u64);
    if (job_count > std.math.maxInt(usize)) return error.InvalidTransitionEncoding;
    const job_ids = try alloc.alloc(u64, @intCast(job_count));
    var initialized_jobs: usize = 0;
    errdefer alloc.free(job_ids);
    while (initialized_jobs < job_ids.len) : (initialized_jobs += 1) {
        job_ids[initialized_jobs] = try readInt(encoded, pos, u64);
    }

    const promotion_count = try readInt(encoded, pos, u64);
    if (promotion_count > std.math.maxInt(usize)) return error.InvalidTransitionEncoding;
    const promotions = try alloc.alloc(metadata_table_manager.TableEmptyingBarrierPromotion, @intCast(promotion_count));
    var initialized_promotions: usize = 0;
    errdefer alloc.free(promotions);
    while (initialized_promotions < promotions.len) : (initialized_promotions += 1) {
        promotions[initialized_promotions] = .{
            .table_id = try readInt(encoded, pos, u64),
            .target_generation = try readInt(encoded, pos, u64),
        };
    }

    return .{
        .job_ids = job_ids,
        .promotions = promotions,
    };
}

fn readTableSchemaCompareAndSwapRequest(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata_table_manager.TableSchemaCompareAndSwapRequest {
    const table_id = try readInt(encoded, pos, u64);
    const expected_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(expected_schema_json);
    const promoted_table = try readTableRecord(alloc, encoded, pos);
    errdefer metadata_table_manager.freeTable(alloc, promoted_table);
    return .{
        .table_id = table_id,
        .expected_schema_json = expected_schema_json,
        .promoted_table = promoted_table,
    };
}

fn readSchemaProgressRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.SchemaProgressRecord {
    return .{
        .table_id = try readInt(encoded, pos, u64),
        .node_id = try readInt(encoded, pos, u64),
        .schema_version = try readInt(encoded, pos, u32),
    };
}

fn readRestoreProgressRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RestoreProgressRecord {
    const table_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const group_id = try readInt(encoded, pos, u64);
    const backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(backup_id);
    const snapshot_path = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(snapshot_path);
    if (pos.* >= encoded.len) return error.InvalidRestoreProgressRecord;
    const primary_restored = switch (encoded[pos.*]) {
        0 => false,
        1 => true,
        else => return error.InvalidRestoreProgressRecord,
    };
    pos.* += 1;
    if (pos.* >= encoded.len) return error.InvalidRestoreProgressRecord;
    const runtime_repair_complete = switch (encoded[pos.*]) {
        0 => false,
        1 => true,
        else => return error.InvalidRestoreProgressRecord,
    };
    pos.* += 1;
    const phase = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(phase);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const updated_at_ms = try readInt(encoded, pos, u64);
    return .{
        .table_id = table_id,
        .node_id = node_id,
        .group_id = group_id,
        .backup_id = backup_id,
        .snapshot_path = snapshot_path,
        .primary_restored = primary_restored,
        .runtime_repair_complete = runtime_repair_complete,
        .phase = phase,
        .last_error = last_error,
        .updated_at_ms = updated_at_ms,
    };
}

fn readReplicationSourceStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.ReplicationSourceStatusRecord {
    const table_id = try readInt(encoded, pos, u64);
    const source_ordinal = try readInt(encoded, pos, u32);
    const source_kind = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(source_kind);
    const external_table = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(external_table);
    const slot_name = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(slot_name);
    const publication_name = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(publication_name);
    const phase = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(phase);
    const checkpoint = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(checkpoint);
    const snapshot_offset = if (pos.* + @sizeOf(u64) <= encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const stream_checkpoint = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(stream_checkpoint);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const lag_records = try readInt(encoded, pos, u64);
    const updated_at_ms = try readInt(encoded, pos, u64);
    const prepared_checkpoint = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(prepared_checkpoint);
    const cutover_mode = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(cutover_mode);
    const consecutive_failures = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_success_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_change_applied_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const failure_class = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(failure_class);
    const lag_millis = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_source_commit_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    return .{
        .table_id = table_id,
        .source_ordinal = source_ordinal,
        .source_kind = source_kind,
        .external_table = external_table,
        .cutover_mode = cutover_mode,
        .slot_name = slot_name,
        .publication_name = publication_name,
        .phase = phase,
        .checkpoint = checkpoint,
        .snapshot_offset = snapshot_offset,
        .prepared_checkpoint = prepared_checkpoint,
        .stream_checkpoint = stream_checkpoint,
        .last_error = last_error,
        .failure_class = failure_class,
        .lag_records = lag_records,
        .lag_millis = lag_millis,
        .consecutive_failures = consecutive_failures,
        .last_source_commit_at_ms = last_source_commit_at_ms,
        .last_success_at_ms = last_success_at_ms,
        .last_change_applied_at_ms = last_change_applied_at_ms,
        .updated_at_ms = updated_at_ms,
    };
}

fn readPlacementIntent(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !raft_reconciler.PlacementIntent {
    const group_id = try readInt(encoded, pos, u64);
    const replica_id = try readInt(encoded, pos, u64);
    const local_node_id = try readInt(encoded, pos, u64);
    const metadata_version = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const bootstrap_mode: raft_catalog.ReplicaBootstrapMode = @enumFromInt(encoded[pos.*]);
    pos.* += 1;
    const peer_count = try readInt(encoded, pos, u32);
    const peer_node_ids = try alloc.alloc(u64, peer_count);
    errdefer alloc.free(peer_node_ids);
    for (peer_node_ids) |*node_id| node_id.* = try readInt(encoded, pos, u64);
    const store_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    var snapshot_bootstrap: ?raft_catalog.SnapshotBootstrapRecord = null;
    var backup_restore_bootstrap: ?raft_catalog.BackupRestoreBootstrapRecord = null;
    if (pos.* < encoded.len) {
        const source_tag = encoded[pos.*];
        pos.* += 1;
        switch (source_tag) {
            0 => {},
            1 => {
                const from_node_id = try readInt(encoded, pos, u64);
                const term = try readInt(encoded, pos, u64);
                const snapshot_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(snapshot_id);
                const uri = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(uri);
                snapshot_bootstrap = .{
                    .from_node_id = from_node_id,
                    .term = term,
                    .snapshot_id = snapshot_id,
                    .uri = uri,
                };
            },
            2 => {
                const backup_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(backup_id);
                const location = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(location);
                const snapshot_path = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(snapshot_path);
                backup_restore_bootstrap = .{
                    .backup_id = backup_id,
                    .location = location,
                    .snapshot_path = snapshot_path,
                };
            },
            else => return error.InvalidMetadataTransitionEncoding,
        }
    }
    return .{
        .record = .{
            .group_id = group_id,
            .replica_id = replica_id,
            .local_node_id = local_node_id,
            .metadata_version = metadata_version,
            .bootstrap_mode = bootstrap_mode,
            .snapshot_bootstrap = snapshot_bootstrap,
            .backup_restore_bootstrap = backup_restore_bootstrap,
        },
        .store_id = store_id,
        .peer_node_ids = peer_node_ids,
    };
}

fn readSplitTransitionRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.SplitTransitionRecord {
    const transition_id = try readInt(encoded, pos, u64);
    const source_group_id = try readInt(encoded, pos, u64);
    const destination_group_id = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const phase: metadata.TransitionPhase = @enumFromInt(encoded[pos.*]);
    pos.* += 1;
    const split_key = try readOptionalString(alloc, encoded, pos);
    const source_range_end = try readOptionalString(alloc, encoded, pos);
    const rollback_reason = try readOptionalString(alloc, encoded, pos);
    return .{
        .transition_id = transition_id,
        .source_group_id = source_group_id,
        .destination_group_id = destination_group_id,
        .phase = phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
    };
}

fn readMergeTransitionRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.MergeTransitionRecord {
    const transition_id = try readInt(encoded, pos, u64);
    const donor_group_id = try readInt(encoded, pos, u64);
    const receiver_group_id = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const phase: metadata.TransitionPhase = @enumFromInt(encoded[pos.*]);
    pos.* += 1;
    const rollback_reason = try readOptionalString(alloc, encoded, pos);
    const allow_doc_identity_reassignment = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    return .{
        .transition_id = transition_id,
        .donor_group_id = donor_group_id,
        .receiver_group_id = receiver_group_id,
        .phase = phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = allow_doc_identity_reassignment,
    };
}

fn readReconcileLeaseRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ReconcileLeaseRecord {
    return .{
        .owner_node_id = try readInt(encoded, pos, u64),
        .expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readShuffleJoinLeaseRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ShuffleJoinLeaseRecord {
    return .{
        .job_id = try readInt(encoded, pos, u64),
        .owner_group_id = try readInt(encoded, pos, u64),
        .expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readReallocationRequestRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ReallocationRequestRecord {
    return .{
        .requested_at_ms = try readInt(encoded, pos, u64),
    };
}

fn appendInt(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn readInt(encoded: []const u8, pos: *usize, comptime T: type) !T {
    if (pos.* + @sizeOf(T) > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const bytes: *const [@sizeOf(T)]u8 = @ptrCast(encoded[pos.* .. pos.* + @sizeOf(T)]);
    const value = std.mem.readInt(T, bytes, .little);
    pos.* += @sizeOf(T);
    return value;
}

fn readOptionalString(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !?[]u8 {
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const present = encoded[pos.*];
    pos.* += 1;
    if (present == 0) return null;
    const len = try readInt(encoded, pos, u32);
    if (pos.* + len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const reason = try alloc.dupe(u8, encoded[pos.* .. pos.* + len]);
    pos.* += len;
    return reason;
}

pub fn splitTransitionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:split:{d}:", .{group_id});
}

pub fn placementPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement:{d}:", .{group_id});
}

pub fn nodePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_node:{d}:", .{group_id});
}

pub fn storePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_store:{d}:", .{group_id});
}

pub fn tablePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table:{d}:", .{group_id});
}

pub fn databasePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_database:{d}:", .{group_id});
}

pub fn namespacePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_namespace:{d}:", .{group_id});
}

pub fn tablespacePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_tablespace:{d}:", .{group_id});
}

pub fn sequencePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_sequence:{d}:", .{group_id});
}

pub fn schemaProgressPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_progress:{d}:", .{group_id});
}

pub fn restoreProgressPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_progress:{d}:", .{group_id});
}

pub fn replicationSourceStatusPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_replication_source_status:{d}:", .{group_id});
}

pub fn extensionPackagePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_package:{d}:", .{group_id});
}

pub fn installedExtensionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_installed_extension:{d}:", .{group_id});
}

pub fn extensionMemberPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_member:{d}:", .{group_id});
}

pub fn extensionDependencyPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_dependency:{d}:", .{group_id});
}

pub fn shuffleJoinLeasePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_shuffle_join_lease:{d}:", .{group_id});
}

pub fn rangePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_range:{d}:", .{group_id});
}

pub fn foreignKeyReferenceRangePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_foreign_key_ref_range:{d}:", .{group_id});
}

pub fn uniqueConstraintRangePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_unique_constraint_range:{d}:", .{group_id});
}

pub fn secondaryIndexRebuildRangePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_secondary_index_rebuild_range:{d}:", .{group_id});
}

pub fn schemaRewriteJobPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_rewrite_job:{d}:", .{group_id});
}

pub fn tableEmptyingJobPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table_emptying_job:{d}:", .{group_id});
}

pub fn mergeTransitionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:merge:{d}:", .{group_id});
}

pub fn reconcileLeaseKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reconcile_lease:{d}", .{group_id});
}

pub fn reallocationRequestKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reallocation_request:{d}", .{group_id});
}

pub fn shuffleJoinLeaseKeyForGroup(buf: []u8, group_id: u64, job_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_shuffle_join_lease:{d}:{d}", .{ group_id, job_id });
}

fn splitTransitionKeyForGroup(buf: []u8, group_id: u64, transition_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:split:{d}:{d}", .{ group_id, transition_id });
}

fn mergeTransitionKeyForGroup(buf: []u8, group_id: u64, transition_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:merge:{d}:{d}", .{ group_id, transition_id });
}

fn tableKeyForGroup(buf: []u8, group_id: u64, table_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table:{d}:{d}", .{ group_id, table_id });
}

fn databaseKeyForGroup(buf: []u8, group_id: u64, database_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_database:{d}:{d}", .{ group_id, database_id });
}

fn namespaceKeyForGroup(buf: []u8, group_id: u64, namespace_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_namespace:{d}:{d}", .{ group_id, namespace_id });
}

fn tablespaceKeyForGroup(buf: []u8, group_id: u64, tablespace_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_tablespace:{d}:{d}", .{ group_id, tablespace_id });
}

fn sequenceKeyForGroup(buf: []u8, group_id: u64, sequence_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_sequence:{d}:{d}", .{ group_id, sequence_id });
}

fn schemaProgressKeyForGroup(buf: []u8, group_id: u64, table_id: u64, node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_progress:{d}:{d}:{d}", .{ group_id, table_id, node_id });
}

fn restoreProgressKeyForGroup(buf: []u8, group_id: u64, table_id: u64, node_id: u64, range_group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_progress:{d}:{d}:{d}:{d}", .{ group_id, table_id, node_id, range_group_id });
}

fn replicationSourceStatusKeyForGroup(buf: []u8, group_id: u64, table_id: u64, source_ordinal: u32) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_replication_source_status:{d}:{d}:{d}", .{ group_id, table_id, source_ordinal });
}

fn extensionPackageKeyForGroup(buf: []u8, group_id: u64, name: []const u8, version: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_package:{d}:{s}:{s}", .{ group_id, name, version });
}

fn installedExtensionKeyForGroup(buf: []u8, group_id: u64, name: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_installed_extension:{d}:{s}", .{ group_id, name });
}

fn extensionMemberKeyForGroup(buf: []u8, group_id: u64, extension_name: []const u8, object_kind: extension_domain.ExtensionObjectKind, object_name: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_member:{d}:{s}:{s}:{s}", .{ group_id, extension_name, @tagName(object_kind), object_name });
}

fn extensionDependencyKeyForGroup(buf: []u8, group_id: u64, extension_name: []const u8, required_extension_name: []const u8, package_name: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_dependency:{d}:{s}:{s}:{s}", .{ group_id, extension_name, required_extension_name, package_name });
}

fn rangeKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_range:{d}:{d}", .{ group_id, range_group_id });
}

fn foreignKeyReferenceRangeKeyForGroup(
    buf: []u8,
    group_id: u64,
    child_table_id: u64,
    constraint_name: []const u8,
    parent_table_id: u64,
    start_parent_key: []const u8,
) ![]const u8 {
    return try std.fmt.bufPrint(
        buf,
        "\x00\x00__metadata__:metadata_foreign_key_ref_range:{d}:{d}:{d}:{s}:{d}:{d}:{s}",
        .{
            group_id,
            child_table_id,
            constraint_name.len,
            constraint_name,
            parent_table_id,
            start_parent_key.len,
            start_parent_key,
        },
    );
}

fn uniqueConstraintRangeKeyForGroup(
    buf: []u8,
    group_id: u64,
    table_id: u64,
    constraint_name: []const u8,
    start_encoded_value: []const u8,
) ![]const u8 {
    return try std.fmt.bufPrint(
        buf,
        "\x00\x00__metadata__:metadata_unique_constraint_range:{d}:{d}:{d}:{s}:{d}:{s}",
        .{
            group_id,
            table_id,
            constraint_name.len,
            constraint_name,
            start_encoded_value.len,
            start_encoded_value,
        },
    );
}

fn secondaryIndexRebuildRangeKeyForGroup(
    buf: []u8,
    group_id: u64,
    table_id: u64,
    index_name: []const u8,
    index_generation: u64,
    start_row_key: []const u8,
) ![]const u8 {
    return try std.fmt.bufPrint(
        buf,
        "\x00\x00__metadata__:metadata_secondary_index_rebuild_range:{d}:{d}:{d}:{s}:{d}:{d}:{s}",
        .{
            group_id,
            table_id,
            index_name.len,
            index_name,
            index_generation,
            start_row_key.len,
            start_row_key,
        },
    );
}

fn schemaRewriteJobKeyForGroup(buf: []u8, group_id: u64, job_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_rewrite_job:{d}:{d}", .{ group_id, job_id });
}

fn tableEmptyingJobKeyForGroup(buf: []u8, group_id: u64, job_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table_emptying_job:{d}:{d}", .{ group_id, job_id });
}

fn placementKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64, local_node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement:{d}:{d}:{d}", .{ group_id, range_group_id, local_node_id });
}

fn nodeKeyForGroup(buf: []u8, group_id: u64, node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_node:{d}:{d}", .{ group_id, node_id });
}

fn storeKeyForGroup(buf: []u8, group_id: u64, store_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_store:{d}:{d}", .{ group_id, store_id });
}

test "metadata raft apply store persists batches across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-apply-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 13,
            .entries_bytes = "metadata-batch",
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const batch = (try store.latestBatch(21)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 13), batch.commit_index);
        try std.testing.expectEqualStrings("metadata-batch", batch.entries_bytes);
    }
}

test "metadata raft apply store projects transition records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-transition-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .source_group_id = 21,
            .destination_group_id = 22,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .rollback_reason = "slow-peer",
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = 31,
            .receiver_group_id = 30,
            .phase = .replay_deltas,
            .allow_doc_identity_reassignment = true,
        },
    });
    defer std.testing.allocator.free(merge_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = split_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = merge_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 8,
            .entries_bytes = encoded_entries,
        });

        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);

        try std.testing.expectEqual(@as(usize, 1), splits.len);
        try std.testing.expectEqual(@as(u64, 501), splits[0].transition_id);
        try std.testing.expectEqualStrings("doc:m", splits[0].split_key.?);
        try std.testing.expectEqualStrings("doc:z", splits[0].source_range_end.?);
        try std.testing.expectEqualStrings("slow-peer", splits[0].rollback_reason.?);
        try std.testing.expectEqual(@as(usize, 1), merges.len);
        try std.testing.expectEqual(@as(u64, 601), merges[0].transition_id);
        try std.testing.expect(merges[0].allow_doc_identity_reassignment);
    }

    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_split_transition = .{ .transition_id = 501 },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_remove = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 9, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_remove);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 9,
            .entries_bytes = encoded_remove,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);

        try std.testing.expectEqual(@as(usize, 0), splits.len);
        try std.testing.expectEqual(@as(usize, 1), merges.len);
        try std.testing.expectEqual(@as(u64, 601), merges[0].transition_id);
        try std.testing.expect(merges[0].allow_doc_identity_reassignment);
    }
}

test "metadata raft apply store resolves stale store drain intent at apply time" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 9 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const draining_registration_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(draining_registration_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 9 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true, .drain_requested = true },
    });
    defer std.testing.allocator.free(stale_draining_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = draining_registration_cmd },
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = stale_draining_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 4,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 21);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 21);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(!stores[0].drain_requested);
}

test "metadata raft apply store ignores stale drained first store registration after cancellation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-first-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 10 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 10 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 10, .node_id = 10, .role = "data", .health_class = "healthy", .live = true, .drain_requested = true },
    });
    defer std.testing.allocator.free(stale_draining_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_draining_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 22,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 22);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 22);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(!stores[0].drain_requested);
}

test "metadata raft apply store ignores stale draining node registration after cancellation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-stale-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 11 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 11 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_node = .{ .node_id = 11, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    });
    defer std.testing.allocator.free(stale_draining_node_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_draining_node_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 23,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 23);
    defer store.freeNodes(std.testing.allocator, nodes);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
}

test "metadata raft apply store finalizes node shutdown by deleting node and stores" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-finalize-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const target_statuses = [_]metadata.GroupStatusReport{.{
        .group_id = 41,
        .updated_at_millis = 10,
        .local_voter = true,
        .voter_count = 1,
    }};
    const target_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 12, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    });
    defer std.testing.allocator.free(target_node_cmd);
    const other_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 13, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
    });
    defer std.testing.allocator.free(other_node_cmd);
    const target_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{
            .store_id = 12,
            .node_id = 12,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .drain_requested = true,
            .group_statuses = @constCast(target_statuses[0..]),
        },
    });
    defer std.testing.allocator.free(target_store_cmd);
    const other_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 13, .node_id = 13, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(other_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(finalize_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = target_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = other_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = target_store_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = other_store_cmd },
        .{ .term = 2, .index = 5, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 24,
        .commit_index = 5,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 24);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 24);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqual(@as(u64, 13), nodes[0].node_id);
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 13), stores[0].store_id);
}

test "metadata raft apply store rejects finalizing active node shutdown" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-finalize-active-reject", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 14, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
    });
    defer std.testing.allocator.free(active_node_cmd);
    const active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 14, .node_id = 14, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(active_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 14 } });
    defer std.testing.allocator.free(finalize_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = active_store_cmd },
    });
    defer std.testing.allocator.free(initial_entries);
    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(finalize_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 25,
        .commit_index = 2,
        .entries_bytes = initial_entries,
    });

    try std.testing.expectError(error.ActiveNodeFinalizeRejected, store.snapshotBuilder().applyBatch(.{
        .group_id = 25,
        .commit_index = 3,
        .entries_bytes = finalize_entries,
    }));

    const nodes = try store.listNodes(std.testing.allocator, 25);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 25);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqual(@as(u64, 14), nodes[0].node_id);
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 14), stores[0].store_id);
}

test "metadata raft apply store rejects finalizing active store-only node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-only-finalize-active-reject", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 15, .node_id = 15, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(active_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 15 } });
    defer std.testing.allocator.free(finalize_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_store_cmd },
    });
    defer std.testing.allocator.free(initial_entries);
    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(finalize_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 26,
        .commit_index = 1,
        .entries_bytes = initial_entries,
    });

    try std.testing.expectError(error.ActiveNodeFinalizeRejected, store.snapshotBuilder().applyBatch(.{
        .group_id = 26,
        .commit_index = 2,
        .entries_bytes = finalize_entries,
    }));

    const stores = try store.listStores(std.testing.allocator, 26);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 15), stores[0].store_id);
}

test "metadata raft apply store projects table and range records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"kind\":\"demo\"}",
            .indexes_json = "{\"default\":{}}",
            .replication_sources_json = "[\"seed\"]",
            .desired_replica_count = 5,
            .min_ranges = 2,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 4101,
            .range_id = 4101,
            .table_id = 41,
            .start_key = "doc:a",
            .end_key = "doc:z",
            .doc_identity_shard_id = 4001,
            .doc_identity_range_id = 9001,
        },
    });
    defer std.testing.allocator.free(range_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    const tables = try store.listTables(std.testing.allocator, 41);
    defer store.freeTables(std.testing.allocator, tables);
    const ranges = try store.listRanges(std.testing.allocator, 41);
    defer store.freeRanges(std.testing.allocator, ranges);

    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 41), tables[0].table_id);
    try std.testing.expectEqualStrings("docs", tables[0].name);
    try std.testing.expectEqualStrings("docs table", tables[0].description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", tables[0].schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", tables[0].indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", tables[0].replication_sources_json);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 4101), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 4001), ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 9001), ranges[0].doc_identity_range_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].start_key);
}

test "metadata raft apply store projects sequence records across reopen and removal" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sequence-catalog", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const sequence_id = metadata_table_manager.deriveSequenceId("tenant", "billing", "usage_id_seq");
    const upsert_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_sequence = .{
            .sequence_id = sequence_id,
            .name = "usage_id_seq",
            .database_name = "tenant",
            .namespace_name = "billing",
            .options_json = "{\"as_type\":\"bigint\",\"start_with\":10}",
            .last_value = 12,
        },
    });
    defer std.testing.allocator.free(upsert_cmd);
    const upsert_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = upsert_cmd },
    });
    defer std.testing.allocator.free(upsert_entries);

    const allocate_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_swap_sequence = .{
            .sequence_id = sequence_id,
            .expected_last_value = 12,
            .next_last_value = 13,
            .allocation_id = 9001,
        },
    });
    defer std.testing.allocator.free(allocate_cmd);
    const stale_allocate_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_swap_sequence = .{
            .sequence_id = sequence_id,
            .expected_last_value = 12,
            .next_last_value = 99,
            .allocation_id = 9002,
        },
    });
    defer std.testing.allocator.free(stale_allocate_cmd);
    const allocate_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = allocate_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_allocate_cmd },
    });
    defer std.testing.allocator.free(allocate_entries);

    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_sequence = .{ .sequence_id = sequence_id },
    });
    defer std.testing.allocator.free(remove_cmd);
    const remove_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(remove_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 1,
            .entries_bytes = upsert_entries,
        });

        const sequences = try store.listSequences(std.testing.allocator, 41);
        defer store.freeSequences(std.testing.allocator, sequences);
        try std.testing.expectEqual(@as(usize, 1), sequences.len);
        try std.testing.expectEqual(sequence_id, sequences[0].sequence_id);
        try std.testing.expectEqualStrings("usage_id_seq", sequences[0].name);
        try std.testing.expectEqualStrings("tenant", sequences[0].database_name);
        try std.testing.expectEqualStrings("billing", sequences[0].namespace_name);
        try std.testing.expectEqualStrings("{\"as_type\":\"bigint\",\"start_with\":10}", sequences[0].options_json);
        try std.testing.expectEqual(@as(i64, 12), sequences[0].last_value);

        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 3,
            .entries_bytes = allocate_entries,
        });
        const allocated = try store.listSequences(std.testing.allocator, 41);
        defer store.freeSequences(std.testing.allocator, allocated);
        try std.testing.expectEqual(@as(usize, 1), allocated.len);
        try std.testing.expectEqual(@as(i64, 13), allocated[0].last_value);
        try std.testing.expectEqual(@as(u128, 9001), allocated[0].last_allocation_id);
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const sequences = try store.listSequences(std.testing.allocator, 41);
        defer store.freeSequences(std.testing.allocator, sequences);
        try std.testing.expectEqual(@as(usize, 1), sequences.len);
        try std.testing.expectEqual(@as(i64, 13), sequences[0].last_value);
        try std.testing.expectEqual(@as(u128, 9001), sequences[0].last_allocation_id);

        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = remove_entries,
        });
        const removed = try store.listSequences(std.testing.allocator, 41);
        defer store.freeSequences(std.testing.allocator, removed);
        try std.testing.expectEqual(@as(usize, 0), removed.len);
    }
}

test "metadata raft apply store persists secondary index rebuild work ranges across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-secondary-index-rebuild-ranges", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const declared_left_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_secondary_index_rebuild_range = .{
            .table_id = 41,
            .index_name = "orders_status_idx",
            .index_generation = 42,
            .start_row_key = "",
            .end_row_key = "order:m",
            .group_id = 4101,
        },
    });
    defer std.testing.allocator.free(declared_left_cmd);
    const declared_right_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_secondary_index_rebuild_range = .{
            .table_id = 41,
            .index_name = "orders_status_idx",
            .index_generation = 42,
            .start_row_key = "order:m",
            .end_row_key = null,
            .group_id = 4102,
        },
    });
    defer std.testing.allocator.free(declared_right_cmd);
    const begin_left_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_secondary_index_rebuild_range = .{
            .selector = .{
                .table_id = 41,
                .index_name = "orders_status_idx",
                .index_generation = 42,
                .start_row_key = "",
            },
            .lease_owner = "worker-a",
            .lease_expires_at_ms = 1234,
        },
    });
    defer std.testing.allocator.free(begin_left_cmd);
    const finish_left_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_secondary_index_rebuild_range = .{
            .selector = .{
                .table_id = 41,
                .index_name = "orders_status_idx",
                .index_generation = 42,
                .start_row_key = "",
            },
            .completed_row_count = 17,
            .progress_row_key = "order:m",
        },
    });
    defer std.testing.allocator.free(finish_left_cmd);
    const begin_right_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_secondary_index_rebuild_range = .{
            .selector = .{
                .table_id = 41,
                .index_name = "orders_status_idx",
                .index_generation = 42,
                .start_row_key = "order:m",
            },
            .lease_owner = "worker-b",
            .lease_expires_at_ms = 5678,
        },
    });
    defer std.testing.allocator.free(begin_right_cmd);
    const invalidate_right_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .invalidate_secondary_index_rebuild_range = .{
            .selector = .{
                .table_id = 41,
                .index_name = "orders_status_idx",
                .index_generation = 42,
                .start_row_key = "order:m",
            },
            .last_error = "schema generation moved",
        },
    });
    defer std.testing.allocator.free(invalidate_right_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = declared_left_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = declared_right_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = begin_left_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = finish_left_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = begin_right_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = invalidate_right_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 7,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const ranges = try reopened.listSecondaryIndexRebuildRanges(std.testing.allocator, 41);
    defer reopened.freeSecondaryIndexRebuildRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);

    var saw_ready = false;
    var saw_invalid = false;
    for (ranges) |record| {
        try std.testing.expectEqual(@as(u64, 41), record.table_id);
        try std.testing.expectEqualStrings("orders_status_idx", record.index_name);
        try std.testing.expectEqual(@as(u64, 42), record.index_generation);
        if (std.mem.eql(u8, record.start_row_key, "")) {
            try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_ready, record.state);
            try std.testing.expectEqual(@as(u64, 17), record.completed_row_count);
            try std.testing.expectEqualStrings("order:m", record.progress_row_key);
            try std.testing.expectEqualStrings("", record.lease_owner);
            saw_ready = true;
        } else if (std.mem.eql(u8, record.start_row_key, "order:m")) {
            try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_invalid, record.state);
            try std.testing.expectEqualStrings("schema generation moved", record.last_error);
            try std.testing.expectEqual(@as(u32, 1), record.attempts);
            try std.testing.expectEqualStrings("", record.lease_owner);
            saw_invalid = true;
        }
    }
    try std.testing.expect(saw_ready and saw_invalid);
}

test "metadata raft apply store persists schema rewrite jobs across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-schema-rewrite-jobs", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const status_expr: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{
            .{ .kind = .field, .field = "status" },
        },
    };
    const rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9101,
            .table_id = 41,
            .group_id = 9001,
            .schema_generation = 42,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .end_row_key = null,
            .target_column = "status_norm",
            .expression = status_expr,
        },
    });
    defer std.testing.allocator.free(rewrite_cmd);
    const full_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9104,
            .table_id = 41,
            .group_id = 9004,
            .schema_generation = 42,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "order:a",
            .end_row_key = "order:m",
            .full_row_rewrite = true,
        },
    });
    defer std.testing.allocator.free(full_rewrite_cmd);
    const row_plan_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9105,
            .table_id = 41,
            .group_id = 9005,
            .schema_generation = 42,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "order:m",
            .rewrite_renames = &.{.{ .old_path = "status", .new_path = "state" }},
            .rewrite_drops = &.{"legacy_status"},
        },
    });
    defer std.testing.allocator.free(row_plan_rewrite_cmd);
    const begin_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9101,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 1234,
        },
    });
    defer std.testing.allocator.free(begin_rewrite_cmd);
    const finish_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_schema_rewrite_job = .{
            .job_id = 9101,
            .lease_owner = "worker-a",
            .completed_row_count = 17,
            .progress_row_key = "order:z",
        },
    });
    defer std.testing.allocator.free(finish_rewrite_cmd);
    const removed_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9102,
            .table_id = 41,
            .group_id = 9002,
            .schema_generation = 42,
            .action = "validate",
            .reason = "constraints",
            .start_row_key = "",
            .end_row_key = null,
        },
    });
    defer std.testing.allocator.free(removed_cmd);
    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_schema_rewrite_job = .{ .job_id = 9102 },
    });
    defer std.testing.allocator.free(remove_cmd);
    const invalidated_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9103,
            .table_id = 41,
            .group_id = 9003,
            .schema_generation = 42,
            .action = "validate",
            .reason = "constraints",
            .start_row_key = "",
            .end_row_key = null,
        },
    });
    defer std.testing.allocator.free(invalidated_cmd);
    const begin_invalidated_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9103,
            .lease_owner = "worker-b",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(begin_invalidated_cmd);
    const invalidate_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .invalidate_schema_rewrite_job = .{
            .job_id = 9103,
            .lease_owner = "worker-b",
            .last_error = "schema generation moved",
        },
    });
    defer std.testing.allocator.free(invalidate_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = rewrite_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = begin_rewrite_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = finish_rewrite_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = removed_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = remove_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = invalidated_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = begin_invalidated_cmd },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = invalidate_cmd },
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = full_rewrite_cmd },
        .{ .term = 1, .index = 11, .entry_type = .normal, .data = row_plan_rewrite_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 11,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 41);
    defer reopened.freeSchemaRewriteJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 4), jobs.len);

    var saw_ready = false;
    var saw_invalid = false;
    var saw_full_rewrite = false;
    var saw_row_plan = false;
    for (jobs) |job| {
        try std.testing.expectEqual(@as(u64, 41), job.table_id);
        try std.testing.expectEqual(@as(u64, 42), job.schema_generation);
        if (job.job_id == 9101) {
            try std.testing.expectEqualStrings("rewrite", job.action);
            try std.testing.expectEqualStrings("row_images", job.reason);
            try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_ready, job.state);
            try std.testing.expectEqualStrings("status_norm", job.target_column);
            try std.testing.expect(job.expression != null);
            try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.lower, job.expression.?.kind);
            try std.testing.expectEqual(@as(usize, 1), job.expression.?.operands.len);
            try std.testing.expectEqualStrings("status", job.expression.?.operands[0].field);
            try std.testing.expectEqualStrings("", job.lease_owner);
            try std.testing.expectEqual(@as(u64, 0), job.lease_expires_at_ms);
            try std.testing.expectEqual(@as(u32, 1), job.attempts);
            try std.testing.expectEqual(@as(u64, 17), job.completed_row_count);
            try std.testing.expectEqualStrings("order:z", job.progress_row_key);
            saw_ready = true;
        } else if (job.job_id == 9103) {
            try std.testing.expectEqualStrings("validate", job.action);
            try std.testing.expectEqualStrings("constraints", job.reason);
            try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_invalid, job.state);
            try std.testing.expectEqualStrings("schema generation moved", job.last_error);
            saw_invalid = true;
        } else if (job.job_id == 9104) {
            try std.testing.expectEqualStrings("rewrite", job.action);
            try std.testing.expectEqualStrings("row_images", job.reason);
            try std.testing.expect(job.full_row_rewrite);
            try std.testing.expectEqualStrings("order:a", job.start_row_key);
            try std.testing.expectEqualStrings("order:m", job.end_row_key.?);
            saw_full_rewrite = true;
        } else if (job.job_id == 9105) {
            try std.testing.expectEqualStrings("rewrite", job.action);
            try std.testing.expectEqualStrings("row_images", job.reason);
            try std.testing.expect(!job.full_row_rewrite);
            try std.testing.expectEqual(@as(usize, 1), job.rewrite_renames.len);
            try std.testing.expectEqualStrings("status", job.rewrite_renames[0].old_path);
            try std.testing.expectEqualStrings("state", job.rewrite_renames[0].new_path);
            try std.testing.expectEqual(@as(usize, 1), job.rewrite_drops.len);
            try std.testing.expectEqualStrings("legacy_status", job.rewrite_drops[0]);
            saw_row_plan = true;
        }
    }
    try std.testing.expect(saw_ready and saw_invalid and saw_full_rewrite and saw_row_plan);
}

test "metadata raft apply store applies table updates with schema rewrite jobs atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-update-schema-rewrite", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    const schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json);
    const update_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .apply_table_catalog_update_with_schema_rewrite_jobs = .{
            .table = .{
                .table_id = 51,
                .name = "orders",
                .schema_json = schema_json,
            },
            .schema_rewrite_jobs = &.{
                .{
                    .job_id = 9501,
                    .table_id = 51,
                    .group_id = 9001,
                    .schema_generation = generation,
                    .action = "validate",
                    .reason = "constraints",
                    .start_row_key = "",
                },
            },
        },
    });
    defer std.testing.allocator.free(update_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = update_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 1,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const tables = try reopened.listTables(std.testing.allocator, 51);
    defer reopened.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings(schema_json, tables[0].schema_json);

    const jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 51);
    defer reopened.freeSchemaRewriteJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 1), jobs.len);
    try std.testing.expectEqual(@as(u64, 9501), jobs[0].job_id);
    try std.testing.expectEqual(@as(u64, generation), jobs[0].schema_generation);
}

test "metadata raft apply store applies table catalog batch updates with schema rewrite jobs atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-batch-update-schema-rewrite", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    const orders_schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const invoices_schema_json =
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const update_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .apply_table_catalog_batch_update_with_schema_rewrite_jobs = .{
            .tables = &.{
                .{ .table_id = 51, .name = "orders", .schema_json = orders_schema_json },
                .{ .table_id = 52, .name = "invoices", .schema_json = invoices_schema_json },
            },
            .schema_rewrite_jobs = &.{
                .{
                    .job_id = 9501,
                    .table_id = 51,
                    .group_id = 9001,
                    .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(orders_schema_json),
                    .action = "validate",
                    .reason = "constraints",
                    .start_row_key = "",
                },
                .{
                    .job_id = 9502,
                    .table_id = 52,
                    .group_id = 9002,
                    .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(invoices_schema_json),
                    .action = "rewrite",
                    .reason = "row_images",
                    .start_row_key = "",
                    .full_row_rewrite = true,
                },
            },
        },
    });
    defer std.testing.allocator.free(update_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = update_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 1,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const tables = try reopened.listTables(std.testing.allocator, 51);
    defer reopened.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 2), tables.len);

    const jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 51);
    defer reopened.freeSchemaRewriteJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 2), jobs.len);
}

test "metadata raft apply store applies table catalog drop with child updates atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-drop-schema-rewrite", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    const child_schema_json =
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"parent_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const parent_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{ .table_id = 51, .name = "parents", .schema_json = "{\"version\":1}" },
    });
    defer std.testing.allocator.free(parent_table_cmd);
    const child_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{ .table_id = 52, .name = "children", .schema_json = "{\"version\":1}" },
    });
    defer std.testing.allocator.free(child_table_cmd);
    const sequence_id = metadata_table_manager.deriveSequenceId(metadata_table_manager.default_database_name, metadata_table_manager.default_namespace_name, "parents_id_seq");
    const parent_sequence_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_sequence = .{ .sequence_id = sequence_id, .name = "parents_id_seq" },
    });
    defer std.testing.allocator.free(parent_sequence_cmd);
    const parent_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{ .group_id = 9001, .table_id = 51, .start_key = "", .end_key = null },
    });
    defer std.testing.allocator.free(parent_range_cmd);
    const child_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{ .group_id = 9002, .table_id = 52, .start_key = "", .end_key = null },
    });
    defer std.testing.allocator.free(child_range_cmd);
    const drop_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .apply_table_catalog_drop_with_schema_rewrite_jobs = .{
            .table_id = 51,
            .sequence_ids = &.{sequence_id},
            .range_group_ids = &.{9001},
            .table_updates = &.{.{ .table_id = 52, .name = "children", .schema_json = child_schema_json }},
            .schema_rewrite_jobs = &.{
                .{
                    .job_id = 9501,
                    .table_id = 52,
                    .group_id = 9002,
                    .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(child_schema_json),
                    .action = "rewrite",
                    .reason = "drop_fk_parent",
                    .start_row_key = "",
                    .full_row_rewrite = true,
                },
            },
        },
    });
    defer std.testing.allocator.free(drop_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = parent_table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = child_table_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = parent_sequence_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = parent_range_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = child_range_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = drop_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 6,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const tables = try reopened.listTables(std.testing.allocator, 51);
    defer reopened.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 52), tables[0].table_id);
    try std.testing.expectEqualStrings(child_schema_json, tables[0].schema_json);

    const ranges = try reopened.listRanges(std.testing.allocator, 51);
    defer reopened.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 9002), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 52), ranges[0].table_id);

    const sequences = try reopened.listSequences(std.testing.allocator, 51);
    defer reopened.freeSequences(std.testing.allocator, sequences);
    try std.testing.expectEqual(@as(usize, 0), sequences.len);

    const jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 51);
    defer reopened.freeSchemaRewriteJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 1), jobs.len);
    try std.testing.expectEqual(@as(u64, 9501), jobs[0].job_id);
    try std.testing.expectEqual(@as(u64, 52), jobs[0].table_id);
}

test "metadata raft apply store recovers drop cascade through emptying and catalog cleanup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-drop-cascade-recoverable-cleanup", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const parent_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"parents_id_seq"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const child_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"children_id_seq"}},"parent_id":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const child_schema_after_drop =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"children_id_seq"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const parent_schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(parent_schema_json);
    const child_schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(child_schema_json);
    const parent_sequence_id = metadata_table_manager.deriveSequenceId("tenant", "billing", "parents_id_seq");
    const child_sequence_id = metadata_table_manager.deriveSequenceId("tenant", "billing", "children_id_seq");
    const sequence_options_json = "{\"start_with\":10,\"increment_by\":5}";

    const parent_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 51,
            .name = "parents",
            .database_name = "tenant",
            .namespace_name = "billing",
            .schema_json = parent_schema_json,
            .data_generation = 3,
        },
    });
    defer std.testing.allocator.free(parent_table_cmd);
    const child_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 52,
            .name = "children",
            .database_name = "tenant",
            .namespace_name = "billing",
            .schema_json = child_schema_json,
            .data_generation = 3,
        },
    });
    defer std.testing.allocator.free(child_table_cmd);
    const parent_sequence_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_sequence = .{
            .sequence_id = parent_sequence_id,
            .name = "parents_id_seq",
            .database_name = "tenant",
            .namespace_name = "billing",
            .options_json = sequence_options_json,
            .last_value = 105,
            .last_allocation_id = 90001,
        },
    });
    defer std.testing.allocator.free(parent_sequence_cmd);
    const child_sequence_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_sequence = .{
            .sequence_id = child_sequence_id,
            .name = "children_id_seq",
            .database_name = "tenant",
            .namespace_name = "billing",
            .options_json = sequence_options_json,
            .last_value = 205,
            .last_allocation_id = 90002,
        },
    });
    defer std.testing.allocator.free(child_sequence_cmd);
    const parent_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9001,
            .range_id = 9101,
            .table_id = 51,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(parent_range_cmd);
    const child_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9002,
            .range_id = 9102,
            .table_id = 52,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(child_range_cmd);
    const parent_empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9301,
            .table_id = 51,
            .group_id = 9001,
            .range_id = 9101,
            .schema_generation = parent_schema_generation,
            .data_generation = 3,
            .barrier_id = 7707,
            .start_row_key = "",
            .end_row_key = null,
            .affected_table_ids = &.{ 51, 52 },
            .restart_identity = true,
            .cascade = true,
            .state = metadata_table_manager.table_emptying_ready,
        },
    });
    defer std.testing.allocator.free(parent_empty_cmd);
    const child_empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9302,
            .table_id = 52,
            .group_id = 9002,
            .range_id = 9102,
            .schema_generation = child_schema_generation,
            .data_generation = 3,
            .barrier_id = 7707,
            .start_row_key = "",
            .end_row_key = null,
            .affected_table_ids = &.{ 51, 52 },
            .restart_identity = true,
            .cascade = true,
            .state = metadata_table_manager.table_emptying_ready,
        },
    });
    defer std.testing.allocator.free(child_empty_cmd);
    const promote_emptying_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_table_emptying_barrier = .{
            .job_ids = &.{ 9301, 9302 },
            .promotions = &.{
                .{ .table_id = 51, .target_generation = 4 },
                .{ .table_id = 52, .target_generation = 4 },
            },
        },
    });
    defer std.testing.allocator.free(promote_emptying_cmd);
    const drop_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .apply_table_catalog_drop_with_schema_rewrite_jobs = .{
            .table_id = 51,
            .sequence_ids = &.{parent_sequence_id},
            .range_group_ids = &.{9001},
            .table_updates = &.{.{
                .table_id = 52,
                .name = "children",
                .database_name = "tenant",
                .namespace_name = "billing",
                .schema_json = child_schema_after_drop,
                .data_generation = 4,
            }},
            .schema_rewrite_jobs = &.{
                .{
                    .job_id = 9502,
                    .table_id = 52,
                    .group_id = 9002,
                    .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(child_schema_after_drop),
                    .action = "rewrite",
                    .reason = "drop_fk_parent",
                    .start_row_key = "",
                    .end_row_key = null,
                    .full_row_rewrite = true,
                },
            },
        },
    });
    defer std.testing.allocator.free(drop_cmd);

    const setup_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = parent_table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = child_table_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = parent_sequence_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = child_sequence_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = parent_range_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = child_range_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = parent_empty_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = child_empty_cmd },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = promote_emptying_cmd },
    });
    defer std.testing.allocator.free(setup_entries);
    const drop_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = drop_cmd },
    });
    defer std.testing.allocator.free(drop_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 9,
            .entries_bytes = setup_entries,
        });
    }

    {
        var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer reopened.deinit();
        const tables = try reopened.listTables(std.testing.allocator, 51);
        defer reopened.freeTables(std.testing.allocator, tables);
        try std.testing.expectEqual(@as(usize, 2), tables.len);
        for (tables) |table| try std.testing.expectEqual(@as(u64, 4), table.data_generation);

        const jobs = try reopened.listTableEmptyingJobs(std.testing.allocator, 51);
        defer reopened.freeTableEmptyingJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 0), jobs.len);

        const sequences = try reopened.listSequences(std.testing.allocator, 51);
        defer reopened.freeSequences(std.testing.allocator, sequences);
        try std.testing.expectEqual(@as(usize, 2), sequences.len);
        for (sequences) |sequence| {
            try std.testing.expectEqual(@as(i64, 5), sequence.last_value);
            try std.testing.expectEqual(@as(u128, 0), sequence.last_allocation_id);
        }
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 10,
            .entries_bytes = drop_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const tables = try reopened.listTables(std.testing.allocator, 51);
    defer reopened.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 52), tables[0].table_id);
    try std.testing.expectEqual(@as(u64, 4), tables[0].data_generation);
    try std.testing.expectEqualStrings(child_schema_after_drop, tables[0].schema_json);

    const ranges = try reopened.listRanges(std.testing.allocator, 51);
    defer reopened.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 9002), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 52), ranges[0].table_id);

    const sequences = try reopened.listSequences(std.testing.allocator, 51);
    defer reopened.freeSequences(std.testing.allocator, sequences);
    try std.testing.expectEqual(@as(usize, 1), sequences.len);
    try std.testing.expectEqual(@as(u64, child_sequence_id), sequences[0].sequence_id);
    try std.testing.expectEqual(@as(i64, 5), sequences[0].last_value);
    try std.testing.expectEqual(@as(u128, 0), sequences[0].last_allocation_id);

    const rewrite_jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 51);
    defer reopened.freeSchemaRewriteJobs(std.testing.allocator, rewrite_jobs);
    try std.testing.expectEqual(@as(usize, 1), rewrite_jobs.len);
    try std.testing.expectEqual(@as(u64, 9502), rewrite_jobs[0].job_id);
    try std.testing.expectEqual(@as(u64, 52), rewrite_jobs[0].table_id);
    try std.testing.expectEqualStrings("drop_fk_parent", rewrite_jobs[0].reason);

    const emptying_jobs = try reopened.listTableEmptyingJobs(std.testing.allocator, 51);
    defer reopened.freeTableEmptyingJobs(std.testing.allocator, emptying_jobs);
    try std.testing.expectEqual(@as(usize, 0), emptying_jobs.len);
}

test "metadata raft apply store rejects table catalog drop with omitted table ranges" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-drop-omitted-range", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    const parent_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{ .table_id = 51, .name = "parents", .schema_json = "{\"version\":1}" },
    });
    defer std.testing.allocator.free(parent_table_cmd);
    const child_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{ .table_id = 52, .name = "children", .schema_json = "{\"version\":1}" },
    });
    defer std.testing.allocator.free(child_table_cmd);
    const parent_left_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{ .group_id = 9001, .table_id = 51, .start_key = "", .end_key = "m" },
    });
    defer std.testing.allocator.free(parent_left_range_cmd);
    const parent_right_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{ .group_id = 9003, .table_id = 51, .start_key = "m", .end_key = null },
    });
    defer std.testing.allocator.free(parent_right_range_cmd);
    const setup_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = parent_table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = child_table_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = parent_left_range_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = parent_right_range_cmd },
    });
    defer std.testing.allocator.free(setup_entries);

    const child_schema_json =
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"parent_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const bad_drop_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .apply_table_catalog_drop_with_schema_rewrite_jobs = .{
            .table_id = 51,
            .range_group_ids = &.{9001},
            .table_updates = &.{.{ .table_id = 52, .name = "children", .schema_json = child_schema_json }},
        },
    });
    defer std.testing.allocator.free(bad_drop_cmd);
    const bad_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = bad_drop_cmd },
    });
    defer std.testing.allocator.free(bad_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 51,
        .commit_index = 4,
        .entries_bytes = setup_entries,
    });
    try std.testing.expectError(error.UnknownRange, store.snapshotBuilder().applyBatch(.{
        .group_id = 51,
        .commit_index = 5,
        .entries_bytes = bad_entries,
    }));

    const tables = try store.listTables(std.testing.allocator, 51);
    defer store.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 2), tables.len);
    var saw_parent = false;
    var saw_original_child = false;
    for (tables) |table| {
        if (table.table_id == 51) saw_parent = true;
        if (table.table_id == 52 and std.mem.eql(u8, table.schema_json, "{\"version\":1}")) saw_original_child = true;
    }
    try std.testing.expect(saw_parent and saw_original_child);

    const ranges = try store.listRanges(std.testing.allocator, 51);
    defer store.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata raft apply store persists table emptying jobs across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-jobs", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const orders_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(orders_table_cmd);
    const items_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 42,
            .name = "order_items",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(items_table_cmd);
    const empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9201,
            .table_id = 41,
            .group_id = 9001,
            .schema_generation = 42,
            .data_generation = 7,
            .barrier_id = 7007,
            .affected_table_ids = &.{41},
            .restart_identity = true,
        },
    });
    defer std.testing.allocator.free(empty_cmd);
    const begin_empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_table_emptying_job = .{
            .job_id = 9201,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 1234,
        },
    });
    defer std.testing.allocator.free(begin_empty_cmd);
    const finish_empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_table_emptying_job = .{
            .job_id = 9201,
            .lease_owner = "worker-a",
            .completed_row_count = 17,
            .progress_row_key = "order:z",
        },
    });
    defer std.testing.allocator.free(finish_empty_cmd);
    const removed_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9202,
            .table_id = 41,
            .group_id = 9002,
            .schema_generation = 42,
            .affected_table_ids = &.{41},
        },
    });
    defer std.testing.allocator.free(removed_cmd);
    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_table_emptying_job = .{ .job_id = 9202 },
    });
    defer std.testing.allocator.free(remove_cmd);
    const invalidated_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9203,
            .table_id = 41,
            .group_id = 9003,
            .schema_generation = 42,
            .data_generation = 9,
            .barrier_id = 9009,
            .affected_table_ids = &.{ 41, 42 },
            .cascade = true,
        },
    });
    defer std.testing.allocator.free(invalidated_cmd);
    const begin_invalidated_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_table_emptying_job = .{
            .job_id = 9203,
            .lease_owner = "worker-b",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(begin_invalidated_cmd);
    const invalidate_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .invalidate_table_emptying_job = .{
            .job_id = 9203,
            .lease_owner = "worker-b",
            .last_error = "schema generation moved",
        },
    });
    defer std.testing.allocator.free(invalidate_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = orders_table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = items_table_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = empty_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = begin_empty_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = finish_empty_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = removed_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = remove_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = invalidated_cmd },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = begin_invalidated_cmd },
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = invalidate_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 10,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const jobs = try reopened.listTableEmptyingJobs(std.testing.allocator, 41);
    defer reopened.freeTableEmptyingJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 2), jobs.len);

    var saw_ready = false;
    var saw_invalid = false;
    for (jobs) |job| {
        try std.testing.expectEqual(@as(u64, 41), job.table_id);
        try std.testing.expectEqual(@as(u64, 42), job.schema_generation);
        if (job.job_id == 9201) {
            try std.testing.expectEqual(@as(u64, 7), job.data_generation);
            try std.testing.expectEqual(@as(u64, 7007), job.barrier_id);
            try std.testing.expect(job.restart_identity);
            try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, job.state);
            try std.testing.expectEqualStrings("", job.lease_owner);
            try std.testing.expectEqual(@as(u64, 0), job.lease_expires_at_ms);
            try std.testing.expectEqual(@as(u32, 1), job.attempts);
            try std.testing.expectEqual(@as(u64, 17), job.completed_row_count);
            try std.testing.expectEqualStrings("order:z", job.progress_row_key);
            try std.testing.expectEqual(@as(usize, 1), job.affected_table_ids.len);
            try std.testing.expectEqual(@as(u64, 41), job.affected_table_ids[0]);
            saw_ready = true;
        } else if (job.job_id == 9203) {
            try std.testing.expectEqual(@as(u64, 9), job.data_generation);
            try std.testing.expectEqual(@as(u64, 9009), job.barrier_id);
            try std.testing.expect(job.cascade);
            try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_invalid, job.state);
            try std.testing.expectEqualStrings("schema generation moved", job.last_error);
            try std.testing.expectEqual(@as(usize, 2), job.affected_table_ids.len);
            try std.testing.expectEqual(@as(u64, 41), job.affected_table_ids[0]);
            try std.testing.expectEqual(@as(u64, 42), job.affected_table_ids[1]);
            saw_invalid = true;
        }
    }
    try std.testing.expect(saw_ready and saw_invalid);
}

test "metadata raft apply store rejects noncanonical table emptying affected tables" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-noncanonical", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9201,
            .table_id = 41,
            .group_id = 9001,
            .schema_generation = 42,
            .data_generation = 7,
            .barrier_id = 7007,
            .affected_table_ids = &.{ 42, 41 },
        },
    });
    defer std.testing.allocator.free(empty_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = empty_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expectError(error.InvalidTableEmptyingJob, store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 1,
        .entries_bytes = encoded_entries,
    }));
}

test "metadata raft apply store rejects malformed table emptying jobs before storage" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-unknown-table", .{tmp.sub_path});
        defer std.testing.allocator.free(root);

        const empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
            .upsert_table_emptying_job = .{
                .job_id = 9201,
                .table_id = 41,
                .group_id = 9001,
                .schema_generation = 42,
                .data_generation = 7,
                .barrier_id = 7007,
                .affected_table_ids = &.{41},
            },
        });
        defer std.testing.allocator.free(empty_cmd);
        const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = empty_cmd },
        });
        defer std.testing.allocator.free(encoded_entries);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try std.testing.expectError(error.UnknownTable, store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 1,
            .entries_bytes = encoded_entries,
        }));
    }

    {
        const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-invalid-state", .{tmp.sub_path});
        defer std.testing.allocator.free(root);

        const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
            .upsert_table = .{
                .table_id = 41,
                .name = "orders",
                .schema_json = "{\"type\":\"object\"}",
            },
        });
        defer std.testing.allocator.free(table_cmd);
        const empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
            .upsert_table_emptying_job = .{
                .job_id = 9201,
                .table_id = 41,
                .group_id = 9001,
                .schema_generation = 42,
                .data_generation = 7,
                .barrier_id = 7007,
                .affected_table_ids = &.{41},
                .state = "queued",
            },
        });
        defer std.testing.allocator.free(empty_cmd);
        const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = empty_cmd },
        });
        defer std.testing.allocator.free(encoded_entries);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try std.testing.expectError(error.InvalidTableEmptyingJobState, store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 2,
            .entries_bytes = encoded_entries,
        }));
    }
}

test "metadata raft apply store persists schema and table emptying operator controls across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-schema-table-operator-controls", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const orders_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(orders_table_cmd);
    const items_table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 42,
            .name = "order_items",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(items_table_cmd);

    const schema_job_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9301,
            .table_id = 41,
            .group_id = 93001,
            .schema_generation = 42,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .end_row_key = null,
        },
    });
    defer std.testing.allocator.free(schema_job_cmd);
    const pause_schema_declared_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .pause_schema_rewrite_job = .{ .job_id = 9301, .reason = "operator pause before start" },
    });
    defer std.testing.allocator.free(pause_schema_declared_cmd);
    const resume_schema_declared_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .resume_schema_rewrite_job = .{ .job_id = 9301, .reason = "operator resume before start" },
    });
    defer std.testing.allocator.free(resume_schema_declared_cmd);
    const begin_schema_a_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9301,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(begin_schema_a_cmd);
    const pause_schema_running_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .pause_schema_rewrite_job = .{ .job_id = 9301, .reason = "operator pause during rewrite" },
    });
    defer std.testing.allocator.free(pause_schema_running_cmd);
    const resume_schema_running_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .resume_schema_rewrite_job = .{ .job_id = 9301, .reason = "operator resume after maintenance" },
    });
    defer std.testing.allocator.free(resume_schema_running_cmd);
    const begin_schema_b_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9301,
            .lease_owner = "worker-b",
            .now_ms = 3000,
            .lease_expires_at_ms = 4000,
        },
    });
    defer std.testing.allocator.free(begin_schema_b_cmd);
    const invalidate_schema_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .invalidate_schema_rewrite_job = .{
            .job_id = 9301,
            .lease_owner = "worker-b",
            .last_error = "schema generation moved",
        },
    });
    defer std.testing.allocator.free(invalidate_schema_cmd);
    const retry_schema_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .retry_schema_rewrite_job = .{ .job_id = 9301, .reason = "operator retry after invalidation" },
    });
    defer std.testing.allocator.free(retry_schema_cmd);
    const cancel_schema_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .cancel_schema_rewrite_job = .{ .job_id = 9301, .reason = "operator cancel" },
    });
    defer std.testing.allocator.free(cancel_schema_cmd);

    const table_emptying_job_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9401,
            .table_id = 41,
            .group_id = 94001,
            .schema_generation = 42,
            .data_generation = 7,
            .barrier_id = 7007,
            .affected_table_ids = &.{ 41, 42 },
            .cascade = true,
        },
    });
    defer std.testing.allocator.free(table_emptying_job_cmd);
    const pause_emptying_declared_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .pause_table_emptying_job = .{ .job_id = 9401, .reason = "operator pause before delete" },
    });
    defer std.testing.allocator.free(pause_emptying_declared_cmd);
    const resume_emptying_declared_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .resume_table_emptying_job = .{ .job_id = 9401, .reason = "operator resume before delete" },
    });
    defer std.testing.allocator.free(resume_emptying_declared_cmd);
    const begin_emptying_a_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_table_emptying_job = .{
            .job_id = 9401,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(begin_emptying_a_cmd);
    const pause_emptying_running_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .pause_table_emptying_job = .{ .job_id = 9401, .reason = "operator pause during delete" },
    });
    defer std.testing.allocator.free(pause_emptying_running_cmd);
    const resume_emptying_running_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .resume_table_emptying_job = .{ .job_id = 9401, .reason = "operator resume after maintenance" },
    });
    defer std.testing.allocator.free(resume_emptying_running_cmd);
    const begin_emptying_b_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_table_emptying_job = .{
            .job_id = 9401,
            .lease_owner = "worker-b",
            .now_ms = 3000,
            .lease_expires_at_ms = 4000,
        },
    });
    defer std.testing.allocator.free(begin_emptying_b_cmd);
    const invalidate_emptying_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .invalidate_table_emptying_job = .{
            .job_id = 9401,
            .lease_owner = "worker-b",
            .last_error = "schema generation moved",
        },
    });
    defer std.testing.allocator.free(invalidate_emptying_cmd);
    const retry_emptying_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .retry_table_emptying_job = .{ .job_id = 9401, .reason = "operator retry after invalidation" },
    });
    defer std.testing.allocator.free(retry_emptying_cmd);
    const cancel_emptying_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .cancel_table_emptying_job = .{ .job_id = 9401, .reason = "operator cancel" },
    });
    defer std.testing.allocator.free(cancel_emptying_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = orders_table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = items_table_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = schema_job_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = pause_schema_declared_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = resume_schema_declared_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = begin_schema_a_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = pause_schema_running_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = resume_schema_running_cmd },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = begin_schema_b_cmd },
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = invalidate_schema_cmd },
        .{ .term = 1, .index = 11, .entry_type = .normal, .data = retry_schema_cmd },
        .{ .term = 1, .index = 12, .entry_type = .normal, .data = cancel_schema_cmd },
        .{ .term = 1, .index = 13, .entry_type = .normal, .data = table_emptying_job_cmd },
        .{ .term = 1, .index = 14, .entry_type = .normal, .data = pause_emptying_declared_cmd },
        .{ .term = 1, .index = 15, .entry_type = .normal, .data = resume_emptying_declared_cmd },
        .{ .term = 1, .index = 16, .entry_type = .normal, .data = begin_emptying_a_cmd },
        .{ .term = 1, .index = 17, .entry_type = .normal, .data = pause_emptying_running_cmd },
        .{ .term = 1, .index = 18, .entry_type = .normal, .data = resume_emptying_running_cmd },
        .{ .term = 1, .index = 19, .entry_type = .normal, .data = begin_emptying_b_cmd },
        .{ .term = 1, .index = 20, .entry_type = .normal, .data = invalidate_emptying_cmd },
        .{ .term = 1, .index = 21, .entry_type = .normal, .data = retry_emptying_cmd },
        .{ .term = 1, .index = 22, .entry_type = .normal, .data = cancel_emptying_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 22,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();

    const schema_jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 41);
    defer reopened.freeSchemaRewriteJobs(std.testing.allocator, schema_jobs);
    try std.testing.expectEqual(@as(usize, 1), schema_jobs.len);
    try std.testing.expectEqual(@as(u64, 9301), schema_jobs[0].job_id);
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_canceled, schema_jobs[0].state);
    try std.testing.expectEqualStrings("", schema_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u64, 0), schema_jobs[0].lease_expires_at_ms);
    try std.testing.expectEqual(@as(u32, 2), schema_jobs[0].attempts);
    try std.testing.expectEqualStrings("operator cancel", schema_jobs[0].last_error);

    const emptying_jobs = try reopened.listTableEmptyingJobs(std.testing.allocator, 41);
    defer reopened.freeTableEmptyingJobs(std.testing.allocator, emptying_jobs);
    try std.testing.expectEqual(@as(usize, 1), emptying_jobs.len);
    try std.testing.expectEqual(@as(u64, 9401), emptying_jobs[0].job_id);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_canceled, emptying_jobs[0].state);
    try std.testing.expectEqualStrings("", emptying_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u64, 0), emptying_jobs[0].lease_expires_at_ms);
    try std.testing.expectEqual(@as(u32, 2), emptying_jobs[0].attempts);
    try std.testing.expectEqualStrings("operator cancel", emptying_jobs[0].last_error);
    try std.testing.expectEqual(@as(usize, 2), emptying_jobs[0].affected_table_ids.len);
    try std.testing.expectEqual(@as(u64, 41), emptying_jobs[0].affected_table_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), emptying_jobs[0].affected_table_ids[1]);
}

test "metadata raft apply store promotes table emptying barriers atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-promotion", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const schema_json = "{\"type\":\"object\"}";
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json);
    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = schema_json,
            .data_generation = 3,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9001,
            .range_id = 9101,
            .table_id = 41,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9201,
            .table_id = 41,
            .group_id = 9001,
            .schema_generation = schema_generation,
            .data_generation = 3,
            .barrier_id = 7007,
            .start_row_key = "",
            .end_row_key = null,
            .affected_table_ids = &.{41},
            .state = metadata_table_manager.table_emptying_ready,
        },
    });
    defer std.testing.allocator.free(empty_cmd);
    const promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_table_emptying_barrier = .{
            .job_ids = &.{9201},
            .promotions = &.{.{ .table_id = 41, .target_generation = 4 }},
        },
    });
    defer std.testing.allocator.free(promote_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = empty_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const tables = try reopened.listTables(std.testing.allocator, 41);
    defer reopened.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 4), tables[0].data_generation);

    const jobs = try reopened.listTableEmptyingJobs(std.testing.allocator, 41);
    defer reopened.freeTableEmptyingJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 0), jobs.len);
}

test "metadata raft apply store promotes restart identity barrier atomically resets sequences" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-restart-identity-promotion", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"usage_id_seq"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json);
    const sequence_id = metadata_table_manager.deriveSequenceId("tenant", "billing", "usage_id_seq");
    const sequence_options_json = "{\"start_with\":10,\"increment_by\":5}";
    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "usage_records",
            .database_name = "tenant",
            .namespace_name = "billing",
            .schema_json = schema_json,
            .data_generation = 3,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9001,
            .range_id = 9101,
            .table_id = 41,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const sequence_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_sequence = .{
            .sequence_id = sequence_id,
            .name = "usage_id_seq",
            .database_name = "tenant",
            .namespace_name = "billing",
            .options_json = sequence_options_json,
            .last_value = 5,
            .last_allocation_id = 1234,
        },
    });
    defer std.testing.allocator.free(sequence_cmd);
    const empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9201,
            .table_id = 41,
            .group_id = 9001,
            .range_id = 9101,
            .schema_generation = schema_generation,
            .data_generation = 3,
            .barrier_id = 7007,
            .start_row_key = "",
            .end_row_key = null,
            .affected_table_ids = &.{41},
            .restart_identity = true,
            .state = metadata_table_manager.table_emptying_ready,
        },
    });
    defer std.testing.allocator.free(empty_cmd);
    const promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_table_emptying_barrier = .{
            .job_ids = &.{9201},
            .promotions = &.{.{ .table_id = 41, .target_generation = 4 }},
        },
    });
    defer std.testing.allocator.free(promote_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = sequence_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = empty_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 5,
            .entries_bytes = encoded_entries,
        });
    }

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer reopened.deinit();
    const tables = try reopened.listTables(std.testing.allocator, 41);
    defer reopened.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 4), tables[0].data_generation);

    const sequences = try reopened.listSequences(std.testing.allocator, 41);
    defer reopened.freeSequences(std.testing.allocator, sequences);
    try std.testing.expectEqual(@as(usize, 1), sequences.len);
    try std.testing.expectEqual(@as(i64, 5), sequences[0].last_value);
    try std.testing.expectEqual(@as(u128, 0), sequences[0].last_allocation_id);

    const jobs = try reopened.listTableEmptyingJobs(std.testing.allocator, 41);
    defer reopened.freeTableEmptyingJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 0), jobs.len);
}

test "metadata raft apply store rejects incomplete table emptying barrier promotion" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-incomplete-promotion", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const schema_json = "{\"type\":\"object\"}";
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json);
    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = schema_json,
            .data_generation = 3,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const left_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9001,
            .range_id = 9101,
            .table_id = 41,
            .start_key = "",
            .end_key = "m",
        },
    });
    defer std.testing.allocator.free(left_range_cmd);
    const right_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9002,
            .range_id = 9102,
            .table_id = 41,
            .start_key = "m",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(right_range_cmd);
    const left_empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9201,
            .table_id = 41,
            .group_id = 9001,
            .schema_generation = schema_generation,
            .data_generation = 3,
            .barrier_id = 7007,
            .start_row_key = "",
            .end_row_key = "m",
            .affected_table_ids = &.{41},
            .state = metadata_table_manager.table_emptying_ready,
        },
    });
    defer std.testing.allocator.free(left_empty_cmd);
    const promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_table_emptying_barrier = .{
            .job_ids = &.{9201},
            .promotions = &.{.{ .table_id = 41, .target_generation = 4 }},
        },
    });
    defer std.testing.allocator.free(promote_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = left_range_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = right_range_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = left_empty_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expectError(error.InvalidTableEmptyingBarrierPromotion, store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 5,
        .entries_bytes = encoded_entries,
    }));
}

test "metadata raft apply store rejects stale range id table emptying barrier promotion" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-emptying-stale-range-id-promotion", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const schema_json = "{\"type\":\"object\"}";
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json);
    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = schema_json,
            .data_generation = 3,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 9001,
            .range_id = 9102,
            .table_id = 41,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const stale_range_empty_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table_emptying_job = .{
            .job_id = 9201,
            .table_id = 41,
            .group_id = 9001,
            .range_id = 9101,
            .schema_generation = schema_generation,
            .data_generation = 3,
            .barrier_id = 7007,
            .start_row_key = "",
            .end_row_key = null,
            .affected_table_ids = &.{41},
            .state = metadata_table_manager.table_emptying_ready,
        },
    });
    defer std.testing.allocator.free(stale_range_empty_cmd);
    const promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_table_emptying_barrier = .{
            .job_ids = &.{9201},
            .promotions = &.{.{ .table_id = 41, .target_generation = 4 }},
        },
    });
    defer std.testing.allocator.free(promote_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_range_empty_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expectError(error.InvalidTableEmptyingBarrierPromotion, store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 4,
        .entries_bytes = encoded_entries,
    }));
}

test "metadata raft apply store rejects stale schema rewrite lease owners" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-schema-rewrite-stale-lease", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = "{\"type\":\"object\"}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9101,
            .table_id = 41,
            .group_id = 9001,
            .schema_generation = 42,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .end_row_key = null,
        },
    });
    defer std.testing.allocator.free(rewrite_cmd);
    const begin_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9101,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(begin_cmd);
    const stale_finish_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_schema_rewrite_job = .{
            .job_id = 9101,
            .lease_owner = "worker-b",
            .completed_row_count = 17,
            .progress_row_key = "order:z",
        },
    });
    defer std.testing.allocator.free(stale_finish_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = rewrite_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = begin_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = stale_finish_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expectError(error.SchemaRewriteJobLeaseMismatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 4,
        .entries_bytes = encoded_entries,
    }));
}

test "metadata raft apply store promotes secondary index schema with compare and swap" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-secondary-index-promotion-cas", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const building_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"string","x-antfly-index":true,"x-antfly-index-lifecycle":"building","x-antfly-index-generation":42}}}}}}
    ;
    const stale_ready_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"string","x-antfly-index":true,"x-antfly-index-lifecycle":"bad-ready","x-antfly-index-generation":42}}}}}}
    ;
    const ready_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"string","x-antfly-index":true,"x-antfly-index-lifecycle":"ready","x-antfly-index-generation":42}}}}}}
    ;

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "orders",
            .schema_json = building_schema,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const stale_promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_secondary_index_ready = .{
            .table_id = 41,
            .index_name = "status",
            .expected_index_generation = 42,
            .expected_schema_json = "{\"stale\":true}",
            .promoted_table = .{
                .table_id = 41,
                .name = "orders",
                .schema_json = stale_ready_schema,
            },
        },
    });
    defer std.testing.allocator.free(stale_promote_cmd);
    const ready_promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .promote_secondary_index_ready = .{
            .table_id = 41,
            .index_name = "status",
            .expected_index_generation = 42,
            .expected_schema_json = building_schema,
            .promoted_table = .{
                .table_id = 41,
                .name = "orders",
                .schema_json = ready_schema,
            },
        },
    });
    defer std.testing.allocator.free(ready_promote_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = stale_promote_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = ready_promote_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });
    const tables = try store.listTables(std.testing.allocator, 41);
    defer store.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings(ready_schema, tables[0].schema_json);
}

test "metadata raft apply store compares and swaps table schema generically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-schema-cas", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const unvalidated_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"string"}}}}},"unique_constraints":[{"name":"uniq_email","columns":["email"],"validation_state":"unvalidated"}]}
    ;
    const stale_enforced_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"string"}}}}},"unique_constraints":[{"name":"uniq_email","columns":["email"],"validation_state":"bad-enforced"}]}
    ;
    const enforced_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"string"}}}}},"unique_constraints":[{"name":"uniq_email","columns":["email"],"validation_state":"enforced"}]}
    ;

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 51,
            .name = "users",
            .schema_json = unvalidated_schema,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const stale_cas_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_swap_table_schema = .{
            .table_id = 51,
            .expected_schema_json = "{\"stale\":true}",
            .promoted_table = .{
                .table_id = 51,
                .name = "users",
                .schema_json = stale_enforced_schema,
            },
        },
    });
    defer std.testing.allocator.free(stale_cas_cmd);
    const enforced_cas_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_swap_table_schema = .{
            .table_id = 51,
            .expected_schema_json = unvalidated_schema,
            .promoted_table = .{
                .table_id = 51,
                .name = "users",
                .schema_json = enforced_schema,
            },
        },
    });
    defer std.testing.allocator.free(enforced_cas_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = stale_cas_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = enforced_cas_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 51,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });
    const tables = try store.listTables(std.testing.allocator, 51);
    defer store.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings(enforced_schema, tables[0].schema_json);
}

test "metadata raft apply store gates table schema compare and swap on rewrite jobs" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const blocked_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-schema-cas-rewrite-blocked", .{tmp.sub_path});
    defer std.testing.allocator.free(blocked_root);
    const ready_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-schema-cas-rewrite-ready", .{tmp.sub_path});
    defer std.testing.allocator.free(ready_root);

    const rewrite_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"string"},"status_norm":{"type":"string"}}}}}}
    ;
    const promoted_schema =
        \\{"version":0,"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"string"},"status_norm":{"type":"string"}}}}},"rewrite_generation":"ready"}
    ;
    const rewrite_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(rewrite_schema);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 61,
            .name = "events",
            .schema_json = rewrite_schema,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const rewrite_job_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_rewrite_job = .{
            .job_id = 9101,
            .table_id = 61,
            .group_id = 9001,
            .schema_generation = rewrite_generation,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .end_row_key = null,
            .target_column = "status_norm",
            .expression = .{
                .kind = .lower,
                .operands = &.{.{ .kind = .field, .field = "status" }},
            },
        },
    });
    defer std.testing.allocator.free(rewrite_job_cmd);
    const finish_rewrite_job_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9101,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(finish_rewrite_job_cmd);
    const ready_rewrite_job_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_schema_rewrite_job = .{
            .job_id = 9101,
            .lease_owner = "worker-a",
            .completed_row_count = 42,
            .progress_row_key = "event:z",
        },
    });
    defer std.testing.allocator.free(ready_rewrite_job_cmd);
    const promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_swap_table_schema = .{
            .table_id = 61,
            .expected_schema_json = rewrite_schema,
            .promoted_table = .{
                .table_id = 61,
                .name = "events",
                .schema_json = promoted_schema,
            },
        },
    });
    defer std.testing.allocator.free(promote_cmd);

    const blocked_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = rewrite_job_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(blocked_entries);
    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = blocked_root });
        defer store.deinit();
        try std.testing.expectError(error.SchemaRewriteJobsIncomplete, store.snapshotBuilder().applyBatch(.{
            .group_id = 61,
            .commit_index = 3,
            .entries_bytes = blocked_entries,
        }));
    }

    const ready_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = rewrite_job_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = finish_rewrite_job_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = ready_rewrite_job_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(ready_entries);
    var ready_store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = ready_root });
    defer ready_store.deinit();
    try ready_store.snapshotBuilder().applyBatch(.{
        .group_id = 61,
        .commit_index = 5,
        .entries_bytes = ready_entries,
    });
    const tables = try ready_store.listTables(std.testing.allocator, 61);
    defer ready_store.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings(promoted_schema, tables[0].schema_json);
    const jobs = try ready_store.listSchemaRewriteJobs(std.testing.allocator, 61);
    defer ready_store.freeSchemaRewriteJobs(std.testing.allocator, jobs);
    try std.testing.expectEqual(@as(usize, 0), jobs.len);
}

test "metadata raft apply store replays create-or-replace schema rewrite workflow idempotently" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-create-or-replace-schema-workflow", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const old_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const replace_schema =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"status_norm":{"type":"keyword"},"amount":{"type":"number"}},"required":["id","status_norm"],"additionalProperties":false}}}}
    ;
    const replace_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(replace_schema);

    const apply_replace_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .apply_table_catalog_update_with_schema_rewrite_jobs = .{
            .table = .{
                .table_id = 81,
                .name = "usage_records",
                .schema_json = replace_schema,
                .read_schema_json = old_schema,
            },
            .schema_rewrite_jobs = &.{
                .{
                    .job_id = 9811,
                    .table_id = 81,
                    .group_id = 7001,
                    .schema_generation = replace_generation,
                    .action = "rewrite",
                    .reason = "row_images",
                    .start_row_key = "",
                    .end_row_key = null,
                    .full_row_rewrite = true,
                },
                .{
                    .job_id = 9812,
                    .table_id = 81,
                    .group_id = 7001,
                    .schema_generation = replace_generation,
                    .action = "validate",
                    .reason = "constraints",
                    .start_row_key = "",
                    .end_row_key = null,
                },
            },
        },
    });
    defer std.testing.allocator.free(apply_replace_cmd);
    const begin_rewrite_a_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9811,
            .lease_owner = "worker-a",
            .now_ms = 1000,
            .lease_expires_at_ms = 2000,
        },
    });
    defer std.testing.allocator.free(begin_rewrite_a_cmd);
    const invalidate_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .invalidate_schema_rewrite_job = .{
            .job_id = 9811,
            .lease_owner = "worker-a",
            .last_error = "replace rewrite owner moved",
        },
    });
    defer std.testing.allocator.free(invalidate_rewrite_cmd);
    const retry_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .retry_schema_rewrite_job = .{ .job_id = 9811, .reason = "operator retry create-or-replace rewrite" },
    });
    defer std.testing.allocator.free(retry_rewrite_cmd);
    const begin_rewrite_b_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9811,
            .lease_owner = "worker-b",
            .now_ms = 3000,
            .lease_expires_at_ms = 4000,
        },
    });
    defer std.testing.allocator.free(begin_rewrite_b_cmd);
    const finish_rewrite_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_schema_rewrite_job = .{
            .job_id = 9811,
            .lease_owner = "worker-b",
            .completed_row_count = 3,
            .progress_row_key = "usage:z",
        },
    });
    defer std.testing.allocator.free(finish_rewrite_cmd);
    const begin_validate_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_schema_rewrite_job = .{
            .job_id = 9812,
            .lease_owner = "validator-a",
            .now_ms = 5000,
            .lease_expires_at_ms = 6000,
        },
    });
    defer std.testing.allocator.free(begin_validate_cmd);
    const finish_validate_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_schema_rewrite_job = .{
            .job_id = 9812,
            .lease_owner = "validator-a",
            .completed_row_count = 3,
            .progress_row_key = "usage:z",
        },
    });
    defer std.testing.allocator.free(finish_validate_cmd);
    const promote_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_swap_table_schema = .{
            .table_id = 81,
            .expected_schema_json = replace_schema,
            .promoted_table = .{
                .table_id = 81,
                .name = "usage_records",
                .schema_json = replace_schema,
                .read_schema_json = "",
            },
        },
    });
    defer std.testing.allocator.free(promote_cmd);

    const apply_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = apply_replace_cmd },
    });
    defer std.testing.allocator.free(apply_entries);
    const complete_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = begin_rewrite_a_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = invalidate_rewrite_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = retry_rewrite_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = begin_rewrite_b_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = finish_rewrite_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = begin_validate_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = finish_validate_cmd },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(complete_entries);
    const promote_replay_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = promote_cmd },
    });
    defer std.testing.allocator.free(promote_replay_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 81,
            .commit_index = 1,
            .entries_bytes = apply_entries,
        });
    }

    {
        var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer reopened.deinit();
        const tables = try reopened.listTables(std.testing.allocator, 81);
        defer reopened.freeTables(std.testing.allocator, tables);
        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqualStrings(replace_schema, tables[0].schema_json);
        try std.testing.expectEqualStrings(old_schema, tables[0].read_schema_json);

        const jobs = try reopened.listSchemaRewriteJobs(std.testing.allocator, 81);
        defer reopened.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 2), jobs.len);
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 81,
            .commit_index = 9,
            .entries_bytes = complete_entries,
        });
    }

    var promoted = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer promoted.deinit();
    const tables = try promoted.listTables(std.testing.allocator, 81);
    defer promoted.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings(replace_schema, tables[0].schema_json);
    try std.testing.expectEqualStrings("", tables[0].read_schema_json);
    {
        const jobs = try promoted.listSchemaRewriteJobs(std.testing.allocator, 81);
        defer promoted.freeSchemaRewriteJobs(std.testing.allocator, jobs);
        try std.testing.expectEqual(@as(usize, 0), jobs.len);
    }

    try promoted.snapshotBuilder().applyBatch(.{
        .group_id = 81,
        .commit_index = 10,
        .entries_bytes = promote_replay_entries,
    });
    const replay_jobs = try promoted.listSchemaRewriteJobs(std.testing.allocator, 81);
    defer promoted.freeSchemaRewriteJobs(std.testing.allocator, replay_jobs);
    try std.testing.expectEqual(@as(usize, 0), replay_jobs.len);
}

test "metadata raft apply store rejects reserved data group ids in transition records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-invalid-data-group-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = group_ids.main_metadata_group_id,
            .table_id = 41,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const range_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(range_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 1,
        .entries_bytes = range_entries,
    }));

    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .source_group_id = 21,
            .destination_group_id = group_ids.main_metadata_group_id,
            .phase = .prepare,
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const split_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = split_cmd },
    });
    defer std.testing.allocator.free(split_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 2,
        .entries_bytes = split_entries,
    }));

    const merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = group_ids.main_metadata_group_id,
            .receiver_group_id = 30,
            .phase = .prepare,
        },
    });
    defer std.testing.allocator.free(merge_cmd);
    const merge_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = merge_cmd },
    });
    defer std.testing.allocator.free(merge_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 3,
        .entries_bytes = merge_entries,
    }));
}

test "metadata raft apply store notifies projection listeners for committed table and range changes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        table_signals: usize = 0,
        range_signals: usize = 0,
        last_table_id: u64 = 0,
        last_range_group_id: u64 = 0,

        fn onSignal(ptr: *anyopaque, signal: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (signal.kind) {
                .table => {
                    self.table_signals += 1;
                    self.last_table_id = signal.table_id;
                },
                .range => {
                    self.range_signals += 1;
                    self.last_table_id = signal.table_id;
                    self.last_range_group_id = signal.group_id;
                },
                else => {},
            }
        }
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 77,
            .name = "docs",
            .schema_json = "{}",
            .indexes_json = "{}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 1001,
            .table_id = 77,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addProjectionListener(.{
        .ptr = &capture,
        .vtable = &.{
            .on_projection_signal = Capture.onSignal,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 1), capture.table_signals);
    try std.testing.expectEqual(@as(usize, 1), capture.range_signals);
    try std.testing.expectEqual(@as(u64, 77), capture.last_table_id);
    try std.testing.expectEqual(@as(u64, 1001), capture.last_range_group_id);
}

test "metadata raft apply store notifies projection listeners for shuffle join lease changes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-shuffle-lease-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        shuffle_join_lease_signals: usize = 0,

        fn onSignal(ptr: *anyopaque, signal: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (signal.kind == .shuffle_join_lease) self.shuffle_join_lease_signals += 1;
        }
    };

    const lease_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    });
    defer std.testing.allocator.free(lease_cmd);
    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_shuffle_join_lease = .{
            .job_id = 77,
        },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = lease_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addProjectionListener(.{
        .ptr = &capture,
        .vtable = &.{
            .on_projection_signal = Capture.onSignal,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.shuffle_join_lease_signals);
}

test "metadata raft apply store preserves projected tables and ranges across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-projection-reopen-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 61,
            .name = "docs",
            .description = "docs table",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 6101,
            .table_id = 61,
            .start_key = "doc:a",
            .end_key = "doc:z",
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const fk_ref_low_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_foreign_key_ref_range = .{
            .child_table_id = 61,
            .constraint_name = "orders_customer_id_fkey",
            .parent_table_id = 62,
            .start_parent_key = "",
            .end_parent_key = "customer:m",
            .group_id = 6201,
            .topology_epoch = 11,
        },
    });
    defer std.testing.allocator.free(fk_ref_low_cmd);
    const fk_ref_high_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_foreign_key_ref_range = .{
            .child_table_id = 61,
            .constraint_name = "orders_customer_id_fkey",
            .parent_table_id = 62,
            .start_parent_key = "customer:m",
            .end_parent_key = null,
            .group_id = 6202,
            .topology_epoch = 12,
            .state = metadata_table_manager.foreign_key_ref_range_rebuilding,
        },
    });
    defer std.testing.allocator.free(fk_ref_high_cmd);
    const fk_ref_high_replace_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_foreign_key_ref_range = .{
            .child_table_id = 61,
            .constraint_name = "orders_customer_id_fkey",
            .parent_table_id = 62,
            .start_parent_key = "customer:m",
            .end_parent_key = null,
            .group_id = 6212,
            .topology_epoch = 22,
            .state = metadata_table_manager.foreign_key_ref_range_active,
        },
    });
    defer std.testing.allocator.free(fk_ref_high_replace_cmd);
    const fk_ref_low_remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_foreign_key_ref_range = .{
            .child_table_id = 61,
            .constraint_name = "orders_customer_id_fkey",
            .parent_table_id = 62,
            .start_parent_key = "",
        },
    });
    defer std.testing.allocator.free(fk_ref_low_remove_cmd);
    const fk_ref_lifecycle_upsert_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_foreign_key_ref_range = .{
            .child_table_id = 61,
            .constraint_name = "orders_account_id_fkey",
            .parent_table_id = 63,
            .start_parent_key = "",
            .end_parent_key = null,
            .group_id = 6301,
            .topology_epoch = 31,
        },
    });
    defer std.testing.allocator.free(fk_ref_lifecycle_upsert_cmd);
    const fk_ref_begin_rebuild_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_foreign_key_ref_range_rebuild = .{
            .child_table_id = 61,
            .constraint_name = "orders_account_id_fkey",
            .parent_table_id = 63,
            .start_parent_key = "",
        },
    });
    defer std.testing.allocator.free(fk_ref_begin_rebuild_cmd);
    const fk_ref_finish_rebuild_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_foreign_key_ref_range_rebuild = .{
            .child_table_id = 61,
            .constraint_name = "orders_account_id_fkey",
            .parent_table_id = 63,
            .start_parent_key = "",
        },
    });
    defer std.testing.allocator.free(fk_ref_finish_rebuild_cmd);
    const fk_ref_begin_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_foreign_key_ref_range_split = .{
            .selector = .{
                .child_table_id = 61,
                .constraint_name = "orders_account_id_fkey",
                .parent_table_id = 63,
                .start_parent_key = "",
            },
            .split_parent_key = "account:m",
            .left_group_id = 6301,
            .right_group_id = 6302,
        },
    });
    defer std.testing.allocator.free(fk_ref_begin_split_cmd);
    const fk_ref_finish_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_foreign_key_ref_range_split = .{
            .selector = .{
                .child_table_id = 61,
                .constraint_name = "orders_account_id_fkey",
                .parent_table_id = 63,
                .start_parent_key = "",
            },
            .split_parent_key = "account:m",
            .left_group_id = 6301,
            .right_group_id = 6302,
        },
    });
    defer std.testing.allocator.free(fk_ref_finish_split_cmd);
    const fk_ref_begin_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_foreign_key_ref_range_merge = .{
            .left_selector = .{
                .child_table_id = 61,
                .constraint_name = "orders_account_id_fkey",
                .parent_table_id = 63,
                .start_parent_key = "",
            },
            .right_start_parent_key = "account:m",
            .merged_group_id = 6301,
        },
    });
    defer std.testing.allocator.free(fk_ref_begin_merge_cmd);
    const fk_ref_finish_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_foreign_key_ref_range_merge = .{
            .left_selector = .{
                .child_table_id = 61,
                .constraint_name = "orders_account_id_fkey",
                .parent_table_id = 63,
                .start_parent_key = "",
            },
            .right_start_parent_key = "account:m",
            .merged_group_id = 6301,
        },
    });
    defer std.testing.allocator.free(fk_ref_finish_merge_cmd);
    const unique_low_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_unique_constraint_range = .{
            .table_id = 61,
            .constraint_name = "users_email_key",
            .start_encoded_value = "",
            .end_encoded_value = "email:m",
            .group_id = 6401,
            .topology_epoch = 41,
        },
    });
    defer std.testing.allocator.free(unique_low_cmd);
    const unique_high_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_unique_constraint_range = .{
            .table_id = 61,
            .constraint_name = "users_email_key",
            .start_encoded_value = "email:m",
            .end_encoded_value = null,
            .group_id = 6402,
            .topology_epoch = 42,
            .state = metadata_table_manager.unique_constraint_range_rebuilding,
        },
    });
    defer std.testing.allocator.free(unique_high_cmd);
    const unique_high_replace_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_unique_constraint_range = .{
            .table_id = 61,
            .constraint_name = "users_email_key",
            .start_encoded_value = "email:m",
            .end_encoded_value = null,
            .group_id = 6412,
            .topology_epoch = 52,
            .state = metadata_table_manager.unique_constraint_range_active,
        },
    });
    defer std.testing.allocator.free(unique_high_replace_cmd);
    const unique_low_remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_unique_constraint_range = .{
            .table_id = 61,
            .constraint_name = "users_email_key",
            .start_encoded_value = "",
        },
    });
    defer std.testing.allocator.free(unique_low_remove_cmd);
    const unique_lifecycle_upsert_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_unique_constraint_range = .{
            .table_id = 61,
            .constraint_name = "users_username_key",
            .start_encoded_value = "",
            .end_encoded_value = null,
            .group_id = 6501,
            .topology_epoch = 61,
        },
    });
    defer std.testing.allocator.free(unique_lifecycle_upsert_cmd);
    const unique_begin_rebuild_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_unique_constraint_range_rebuild = .{
            .table_id = 61,
            .constraint_name = "users_username_key",
            .start_encoded_value = "",
        },
    });
    defer std.testing.allocator.free(unique_begin_rebuild_cmd);
    const unique_finish_rebuild_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_unique_constraint_range_rebuild = .{
            .table_id = 61,
            .constraint_name = "users_username_key",
            .start_encoded_value = "",
        },
    });
    defer std.testing.allocator.free(unique_finish_rebuild_cmd);
    const unique_begin_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_unique_constraint_range_split = .{
            .selector = .{
                .table_id = 61,
                .constraint_name = "users_username_key",
                .start_encoded_value = "",
            },
            .split_encoded_value = "username:m",
            .left_group_id = 6501,
            .right_group_id = 6502,
        },
    });
    defer std.testing.allocator.free(unique_begin_split_cmd);
    const unique_finish_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_unique_constraint_range_split = .{
            .selector = .{
                .table_id = 61,
                .constraint_name = "users_username_key",
                .start_encoded_value = "",
            },
            .split_encoded_value = "username:m",
            .left_group_id = 6501,
            .right_group_id = 6502,
        },
    });
    defer std.testing.allocator.free(unique_finish_split_cmd);
    const unique_begin_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .begin_unique_constraint_range_merge = .{
            .left_selector = .{
                .table_id = 61,
                .constraint_name = "users_username_key",
                .start_encoded_value = "",
            },
            .right_start_encoded_value = "username:m",
            .merged_group_id = 6501,
        },
    });
    defer std.testing.allocator.free(unique_begin_merge_cmd);
    const unique_finish_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .finish_unique_constraint_range_merge = .{
            .left_selector = .{
                .table_id = 61,
                .constraint_name = "users_username_key",
                .start_encoded_value = "",
            },
            .right_start_encoded_value = "username:m",
            .merged_group_id = 6501,
        },
    });
    defer std.testing.allocator.free(unique_finish_merge_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = fk_ref_low_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = fk_ref_high_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = fk_ref_high_replace_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = fk_ref_low_remove_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = fk_ref_lifecycle_upsert_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = fk_ref_begin_rebuild_cmd },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = fk_ref_finish_rebuild_cmd },
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = fk_ref_begin_split_cmd },
        .{ .term = 1, .index = 11, .entry_type = .normal, .data = fk_ref_finish_split_cmd },
        .{ .term = 1, .index = 12, .entry_type = .normal, .data = fk_ref_begin_merge_cmd },
        .{ .term = 1, .index = 13, .entry_type = .normal, .data = fk_ref_finish_merge_cmd },
        .{ .term = 1, .index = 14, .entry_type = .normal, .data = unique_low_cmd },
        .{ .term = 1, .index = 15, .entry_type = .normal, .data = unique_high_cmd },
        .{ .term = 1, .index = 16, .entry_type = .normal, .data = unique_high_replace_cmd },
        .{ .term = 1, .index = 17, .entry_type = .normal, .data = unique_low_remove_cmd },
        .{ .term = 1, .index = 18, .entry_type = .normal, .data = unique_lifecycle_upsert_cmd },
        .{ .term = 1, .index = 19, .entry_type = .normal, .data = unique_begin_rebuild_cmd },
        .{ .term = 1, .index = 20, .entry_type = .normal, .data = unique_finish_rebuild_cmd },
        .{ .term = 1, .index = 21, .entry_type = .normal, .data = unique_begin_split_cmd },
        .{ .term = 1, .index = 22, .entry_type = .normal, .data = unique_finish_split_cmd },
        .{ .term = 1, .index = 23, .entry_type = .normal, .data = unique_begin_merge_cmd },
        .{ .term = 1, .index = 24, .entry_type = .normal, .data = unique_finish_merge_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 61,
            .commit_index = 6,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const tables = try store.listTables(std.testing.allocator, 61);
        defer store.freeTables(std.testing.allocator, tables);
        const ranges = try store.listRanges(std.testing.allocator, 61);
        defer store.freeRanges(std.testing.allocator, ranges);
        const fk_ref_ranges = try store.listForeignKeyReferenceRanges(std.testing.allocator, 61);
        defer store.freeForeignKeyReferenceRanges(std.testing.allocator, fk_ref_ranges);
        const unique_ranges = try store.listUniqueConstraintRanges(std.testing.allocator, 61);
        defer store.freeUniqueConstraintRanges(std.testing.allocator, unique_ranges);

        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqualStrings("docs", tables[0].name);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqual(@as(u64, 6101), ranges[0].group_id);
        try std.testing.expectEqual(@as(usize, 2), fk_ref_ranges.len);
        var saw_customer = false;
        var saw_account = false;
        for (fk_ref_ranges) |record| {
            try std.testing.expectEqual(@as(u64, 61), record.child_table_id);
            try std.testing.expectEqualStrings(metadata_table_manager.foreign_key_ref_range_active, record.state);
            if (std.mem.eql(u8, record.constraint_name, "orders_customer_id_fkey")) {
                try std.testing.expectEqual(@as(u64, 62), record.parent_table_id);
                try std.testing.expectEqualStrings("customer:m", record.start_parent_key);
                try std.testing.expect(record.end_parent_key == null);
                try std.testing.expectEqual(@as(u64, 6212), record.group_id);
                try std.testing.expectEqual(@as(u64, 22), record.topology_epoch);
                saw_customer = true;
            } else if (std.mem.eql(u8, record.constraint_name, "orders_account_id_fkey")) {
                try std.testing.expectEqual(@as(u64, 63), record.parent_table_id);
                try std.testing.expectEqualStrings("", record.start_parent_key);
                try std.testing.expect(record.end_parent_key == null);
                try std.testing.expectEqual(@as(u64, 6301), record.group_id);
                try std.testing.expect(record.topology_epoch > 31);
                saw_account = true;
            }
        }
        try std.testing.expect(saw_customer and saw_account);

        try std.testing.expectEqual(@as(usize, 2), unique_ranges.len);
        var saw_email = false;
        var saw_username = false;
        for (unique_ranges) |record| {
            try std.testing.expectEqual(@as(u64, 61), record.table_id);
            try std.testing.expectEqualStrings(metadata_table_manager.unique_constraint_range_active, record.state);
            if (std.mem.eql(u8, record.constraint_name, "users_email_key")) {
                try std.testing.expectEqualStrings("email:m", record.start_encoded_value);
                try std.testing.expect(record.end_encoded_value == null);
                try std.testing.expectEqual(@as(u64, 6412), record.group_id);
                try std.testing.expectEqual(@as(u64, 52), record.topology_epoch);
                saw_email = true;
            } else if (std.mem.eql(u8, record.constraint_name, "users_username_key")) {
                try std.testing.expectEqualStrings("", record.start_encoded_value);
                try std.testing.expect(record.end_encoded_value == null);
                try std.testing.expectEqual(@as(u64, 6501), record.group_id);
                try std.testing.expect(record.topology_epoch > 61);
                saw_username = true;
            }
        }
        try std.testing.expect(saw_email and saw_username);
    }
}

test "metadata raft apply store notifies committed key listeners for matched metadata prefixes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-key-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        matched: usize = 0,
        saw_table: bool = false,
        saw_range: bool = false,

        fn matches(ptr: *anyopaque, signal: CommittedKeySignal) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self;
            var table_prefix_buf: [96]u8 = undefined;
            const table_prefix = tablePrefixForGroup(&table_prefix_buf, signal.metadata_group_id) catch return false;
            if (std.mem.startsWith(u8, signal.key, table_prefix)) return true;

            var range_prefix_buf: [96]u8 = undefined;
            const range_prefix = rangePrefixForGroup(&range_prefix_buf, signal.metadata_group_id) catch return false;
            return std.mem.startsWith(u8, signal.key, range_prefix);
        }

        fn onKey(ptr: *anyopaque, signal: CommittedKeySignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.matched += 1;

            var table_prefix_buf: [96]u8 = undefined;
            const table_prefix = tablePrefixForGroup(&table_prefix_buf, signal.metadata_group_id) catch return;
            if (std.mem.startsWith(u8, signal.key, table_prefix)) {
                self.saw_table = true;
                return;
            }

            var range_prefix_buf: [96]u8 = undefined;
            const range_prefix = rangePrefixForGroup(&range_prefix_buf, signal.metadata_group_id) catch return;
            if (std.mem.startsWith(u8, signal.key, range_prefix)) self.saw_range = true;
        }
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 88,
            .name = "docs",
            .schema_json = "{}",
            .indexes_json = "{}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 2001,
            .table_id = 88,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addCommittedKeyListener(.{
        .ptr = &capture,
        .vtable = &.{
            .matches_key = Capture.matches,
            .on_committed_key = Capture.onKey,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.matched);
    try std.testing.expect(capture.saw_table);
    try std.testing.expect(capture.saw_range);
}

test "metadata.table record decoder accepts legacy table metadata encoding" {
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);

    try appendInt(std.testing.allocator, &encoded, u64, 41);
    try appendInt(std.testing.allocator, &encoded, u16, 5);
    try appendInt(std.testing.allocator, &encoded, u32, 2);
    try appendInt(std.testing.allocator, &encoded, u32, 4);
    try encoded.appendSlice(std.testing.allocator, "docs");
    try appendInt(std.testing.allocator, &encoded, u32, 10);
    try encoded.appendSlice(std.testing.allocator, "docs table");
    try appendInt(std.testing.allocator, &encoded, u32, 15);
    try encoded.appendSlice(std.testing.allocator, "{\"kind\":\"demo\"}");
    try appendInt(std.testing.allocator, &encoded, u32, 14);
    try encoded.appendSlice(std.testing.allocator, "{\"default\":{}}");
    try appendInt(std.testing.allocator, &encoded, u32, 8);
    try encoded.appendSlice(std.testing.allocator, "[\"seed\"]");
    try appendInt(std.testing.allocator, &encoded, u32, 4);
    try encoded.appendSlice(std.testing.allocator, "data");

    const decoded = try decodeTableRecord(std.testing.allocator, encoded.items);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqual(@as(u64, 41), decoded.table_id);
    try std.testing.expectEqualStrings("docs", decoded.name);
    try std.testing.expectEqualStrings(metadata_table_manager.default_database_name, decoded.database_name);
    try std.testing.expectEqualStrings(metadata_table_manager.default_namespace_name, decoded.namespace_name);
    try std.testing.expectEqualStrings("docs table", decoded.description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", decoded.schema_json);
    try std.testing.expectEqualStrings("", decoded.read_schema_json);
    try std.testing.expectEqualStrings("{}", decoded.foreign_key_validation_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", decoded.indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", decoded.replication_sources_json);
    try std.testing.expectEqualStrings("data", decoded.placement_role);
}

test "metadata.table record decoder round-trips read schema metadata" {
    const encoded = try encodeTableRecord(std.testing.allocator, .{
        .table_id = 41,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .foreign_key_validation_json = "{\"foreign_keys\":{\"fk\":{\"validation_state\":\"invalid\"}}}",
        .indexes_json = "{\"default\":{}}",
        .replication_sources_json = "[\"seed\"]",
        .placement_role = "data",
        .desired_replica_count = 5,
        .min_ranges = 2,
    });
    defer std.testing.allocator.free(encoded);

    const decoded = try decodeTableRecord(std.testing.allocator, encoded);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqualStrings(metadata_table_manager.default_database_name, decoded.database_name);
    try std.testing.expectEqualStrings(metadata_table_manager.default_namespace_name, decoded.namespace_name);
    try std.testing.expectEqualStrings("{\"version\":1}", decoded.schema_json);
    try std.testing.expectEqualStrings("{\"version\":0}", decoded.read_schema_json);
    try std.testing.expectEqualStrings("{\"foreign_keys\":{\"fk\":{\"validation_state\":\"invalid\"}}}", decoded.foreign_key_validation_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", decoded.indexes_json);
}

test "metadata.table record decoder round-trips catalog identity" {
    const encoded = try encodeTableRecord(std.testing.allocator, .{
        .table_id = 41,
        .name = "invoices",
        .database_name = "tenant_ops",
        .namespace_name = "billing",
        .description = "tenant billing invoices",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .foreign_key_validation_json = "{}",
        .indexes_json = "{\"default\":{}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
        .tablespace_name = "hot",
        .desired_replica_count = 5,
        .min_ranges = 2,
        .data_generation = 42,
    });
    defer std.testing.allocator.free(encoded);

    const decoded = try decodeTableRecord(std.testing.allocator, encoded);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqual(@as(u64, 41), decoded.table_id);
    try std.testing.expectEqualStrings("tenant_ops", decoded.database_name);
    try std.testing.expectEqualStrings("billing", decoded.namespace_name);
    try std.testing.expectEqualStrings("invoices", decoded.name);
    try std.testing.expectEqualStrings("tenant billing invoices", decoded.description);
    try std.testing.expectEqualStrings("hot", decoded.tablespace_name);
    try std.testing.expectEqual(@as(u64, 42), decoded.data_generation);
}

test "metadata catalog identity transition commands round-trip database and namespace records" {
    const database_id = metadata_table_manager.deriveDatabaseId("tenant_ops");
    const namespace_id = metadata_table_manager.deriveNamespaceId(database_id, "billing");

    const database_encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_database = .{
            .database_id = database_id,
            .name = "tenant_ops",
            .settings_json = "{\"timezone\":\"UTC\"}",
        },
    });
    defer std.testing.allocator.free(database_encoded);

    var database_decoded = (try decodeTransitionCommand(std.testing.allocator, database_encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer database_decoded.deinit(std.testing.allocator);
    switch (database_decoded) {
        .upsert_database => |record| {
            try std.testing.expectEqual(database_id, record.database_id);
            try std.testing.expectEqualStrings("tenant_ops", record.name);
            try std.testing.expectEqualStrings("{\"timezone\":\"UTC\"}", record.settings_json);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }

    const namespace_encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_namespace = .{
            .namespace_id = namespace_id,
            .database_id = database_id,
            .name = "billing",
        },
    });
    defer std.testing.allocator.free(namespace_encoded);

    var namespace_decoded = (try decodeTransitionCommand(std.testing.allocator, namespace_encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer namespace_decoded.deinit(std.testing.allocator);
    switch (namespace_decoded) {
        .upsert_namespace => |record| {
            try std.testing.expectEqual(namespace_id, record.namespace_id);
            try std.testing.expectEqual(database_id, record.database_id);
            try std.testing.expectEqualStrings("billing", record.name);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata schema progress transition command round-trips" {
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
            .schema_version = 3,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);

    switch (decoded) {
        .upsert_schema_progress => |record| {
            try std.testing.expectEqual(@as(u64, 41), record.table_id);
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            try std.testing.expectEqual(@as(u32, 3), record.schema_version);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata reallocation request transition command round-trips" {
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_reallocation_request = .{
            .requested_at_ms = 42_000,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);

    switch (decoded) {
        .upsert_reallocation_request => |record| {
            try std.testing.expectEqual(@as(u64, 42_000), record.requested_at_ms);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata extension lifecycle transition command round-trips" {
    const command: TransitionCommand = .{
        .apply_extension_lifecycle = .{
            .upsert_tables = &.{.{
                .table_id = 7,
                .name = "memories",
                .indexes_json = "{\"memory_text\":{\"type\":\"full_text\"}}",
            }},
            .upsert_installed_extensions = &.{.{
                .name = "memoryaf",
                .package_name = "memoryaf",
                .package_version = "1.0.0",
                .package_digest = "sha256:abc",
                .scope = .{ .kind = .table, .table_name = "memories" },
                .status = .ready,
            }},
            .remove_installed_extensions = &.{"old_memoryaf"},
            .upsert_extension_members = &.{.{
                .extension_name = "memoryaf",
                .scope = .{ .kind = .table, .table_name = "memories" },
                .object_kind = .index,
                .object_name = "memory_text",
                .table_name = "memories",
                .owner_metadata_json = "{\"type\":\"full_text\"}",
            }},
            .remove_extension_members = &.{.{
                .extension_name = "memoryaf",
                .object_kind = .mcp_tool,
                .object_name = "old_recall",
            }},
            .upsert_extension_dependencies = &.{.{
                .extension_name = "memoryaf",
                .required_extension_name = "core",
                .package_name = "antfly_core",
            }},
            .remove_extension_dependencies = &.{.{
                .extension_name = "memoryaf",
                .required_extension_name = "old_core",
                .package_name = "old_core",
            }},
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .apply_extension_lifecycle);
    const delta = decoded.?.apply_extension_lifecycle;
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_tables.len);
    try std.testing.expectEqualStrings("memories", delta.upsert_tables[0].name);
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_installed_extensions.len);
    try std.testing.expectEqualStrings("memoryaf", delta.upsert_installed_extensions[0].name);
    try std.testing.expectEqual(@as(usize, 1), delta.remove_installed_extensions.len);
    try std.testing.expectEqualStrings("old_memoryaf", delta.remove_installed_extensions[0]);
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_extension_members.len);
    try std.testing.expectEqual(.index, delta.upsert_extension_members[0].object_kind);
    try std.testing.expectEqual(@as(usize, 1), delta.remove_extension_members.len);
    try std.testing.expectEqualStrings("old_recall", delta.remove_extension_members[0].object_name);
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_extension_dependencies.len);
    try std.testing.expectEqualStrings("antfly_core", delta.upsert_extension_dependencies[0].package_name);
    try std.testing.expectEqual(@as(usize, 1), delta.remove_extension_dependencies.len);
}

test "metadata raft apply store projects schema progress records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-schema-progress-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const progress_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
            .schema_version = 3,
        },
    });
    defer std.testing.allocator.free(progress_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = progress_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listSchemaProgress(std.testing.allocator, 41);
        defer store.freeSchemaProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 1), progress.len);
        try std.testing.expectEqual(@as(u64, 41), progress[0].table_id);
        try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
        try std.testing.expectEqual(@as(u32, 3), progress[0].schema_version);
    }

    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
        },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_remove = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 5, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_remove);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 5,
            .entries_bytes = encoded_remove,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listSchemaProgress(std.testing.allocator, 41);
        defer store.freeSchemaProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 0), progress.len);
    }
}

test "metadata restore progress transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_restore_progress = .{
            .table_id = 41,
            .node_id = 7,
            .group_id = 4101,
            .backup_id = "snap1",
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_restore_progress);
    try std.testing.expectEqual(@as(u64, 41), decoded.?.upsert_restore_progress.table_id);
    try std.testing.expectEqual(@as(u64, 7), decoded.?.upsert_restore_progress.node_id);
    try std.testing.expectEqual(@as(u64, 4101), decoded.?.upsert_restore_progress.group_id);
    try std.testing.expectEqualStrings("snap1", decoded.?.upsert_restore_progress.backup_id);
}

test "metadata replication source status transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_replication_source_status = .{
            .table_id = 41,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .cutover_mode = "exported_snapshot",
            .phase = "snapshot",
            .checkpoint = "lsn:0/16B6A50",
            .prepared_checkpoint = "lsn:0/16B6A50",
            .last_error = "",
            .failure_class = "terminal",
            .lag_records = 12,
            .lag_millis = 34,
            .consecutive_failures = 4,
            .last_source_commit_at_ms = 120,
            .last_success_at_ms = 123,
            .last_change_applied_at_ms = 124,
            .updated_at_ms = 555,
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_replication_source_status);
    try std.testing.expectEqual(@as(u64, 41), decoded.?.upsert_replication_source_status.table_id);
    try std.testing.expectEqual(@as(u32, 0), decoded.?.upsert_replication_source_status.source_ordinal);
    try std.testing.expectEqualStrings("postgres", decoded.?.upsert_replication_source_status.source_kind);
    try std.testing.expectEqualStrings("users", decoded.?.upsert_replication_source_status.external_table);
    try std.testing.expectEqualStrings("exported_snapshot", decoded.?.upsert_replication_source_status.cutover_mode);
    try std.testing.expectEqualStrings("snapshot", decoded.?.upsert_replication_source_status.phase);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", decoded.?.upsert_replication_source_status.prepared_checkpoint);
    try std.testing.expectEqualStrings("terminal", decoded.?.upsert_replication_source_status.failure_class);
    try std.testing.expectEqual(@as(u64, 34), decoded.?.upsert_replication_source_status.lag_millis);
    try std.testing.expectEqual(@as(u64, 4), decoded.?.upsert_replication_source_status.consecutive_failures);
    try std.testing.expectEqual(@as(u64, 120), decoded.?.upsert_replication_source_status.last_source_commit_at_ms);
    try std.testing.expectEqual(@as(u64, 123), decoded.?.upsert_replication_source_status.last_success_at_ms);
    try std.testing.expectEqual(@as(u64, 124), decoded.?.upsert_replication_source_status.last_change_applied_at_ms);
}

test "metadata shuffle join lease transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_shuffle_join_lease);
    try std.testing.expectEqual(@as(u64, 77), decoded.?.upsert_shuffle_join_lease.job_id);
    try std.testing.expectEqual(@as(u64, 202), decoded.?.upsert_shuffle_join_lease.owner_group_id);
    try std.testing.expectEqual(@as(u64, 9_999), decoded.?.upsert_shuffle_join_lease.expires_at_ms);
}

test "metadata raft apply store projects shuffle join lease records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-shuffle-lease-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const lease_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    });
    defer std.testing.allocator.free(lease_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = lease_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const lease = (try store.getShuffleJoinLease(41, 77)).?;
        try std.testing.expectEqual(@as(u64, 77), lease.job_id);
        try std.testing.expectEqual(@as(u64, 202), lease.owner_group_id);
        try std.testing.expectEqual(@as(u64, 9_999), lease.expires_at_ms);

        const leases = try store.listShuffleJoinLeases(std.testing.allocator, 41);
        defer store.freeShuffleJoinLeases(std.testing.allocator, leases);
        try std.testing.expectEqual(@as(usize, 1), leases.len);
    }
}

test "metadata raft apply store projects restore progress records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-restore-progress-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const progress_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_progress = .{
            .table_id = 41,
            .node_id = 7,
            .group_id = 4101,
            .backup_id = "snap1",
        },
    });
    defer std.testing.allocator.free(progress_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = progress_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listRestoreProgress(std.testing.allocator, 41);
        defer store.freeRestoreProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 1), progress.len);
        try std.testing.expectEqual(@as(u64, 41), progress[0].table_id);
        try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
        try std.testing.expectEqual(@as(u64, 4101), progress[0].group_id);
        try std.testing.expectEqualStrings("snap1", progress[0].backup_id);
    }
}

test "metadata raft apply store projects replication source status records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-replication-source-status-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const status_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replication_source_status = .{
            .table_id = 41,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .phase = "streaming",
            .checkpoint = "lsn:0/16B6B10",
            .prepared_checkpoint = "lsn:0/16B6A50",
            .last_error = "",
            .failure_class = "retryable",
            .lag_records = 3,
            .lag_millis = 21,
            .consecutive_failures = 2,
            .last_source_commit_at_ms = 333,
            .last_success_at_ms = 444,
            .last_change_applied_at_ms = 555,
            .updated_at_ms = 777,
        },
    });
    defer std.testing.allocator.free(status_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = status_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 2,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const statuses = try store.listReplicationSourceStatuses(std.testing.allocator, 41);
        defer store.freeReplicationSourceStatuses(std.testing.allocator, statuses);
        try std.testing.expectEqual(@as(usize, 1), statuses.len);
        try std.testing.expectEqual(@as(u64, 41), statuses[0].table_id);
        try std.testing.expectEqual(@as(u32, 0), statuses[0].source_ordinal);
        try std.testing.expectEqualStrings("postgres", statuses[0].source_kind);
        try std.testing.expectEqualStrings("users", statuses[0].external_table);
        try std.testing.expectEqualStrings("streaming", statuses[0].phase);
        try std.testing.expectEqualStrings("lsn:0/16B6A50", statuses[0].prepared_checkpoint);
        try std.testing.expectEqualStrings("retryable", statuses[0].failure_class);
        try std.testing.expectEqual(@as(u64, 3), statuses[0].lag_records);
        try std.testing.expectEqual(@as(u64, 21), statuses[0].lag_millis);
        try std.testing.expectEqual(@as(u64, 2), statuses[0].consecutive_failures);
        try std.testing.expectEqual(@as(u64, 333), statuses[0].last_source_commit_at_ms);
        try std.testing.expectEqual(@as(u64, 444), statuses[0].last_success_at_ms);
        try std.testing.expectEqual(@as(u64, 555), statuses[0].last_change_applied_at_ms);
    }
}

test "metadata raft apply store projects placement intents from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const intent_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replica_intent = .{
            .record = .{
                .group_id = 5101,
                .replica_id = 2,
                .local_node_id = 7,
                .bootstrap_mode = .fetch_snapshot,
                .metadata_version = 3,
                .snapshot_bootstrap = .{
                    .from_node_id = 3,
                    .term = 8,
                    .snapshot_id = "snap-5101",
                    .uri = "http://127.0.0.1:7777/raft/v1/snapshot/fetch/snap-5101",
                },
            },
            .store_id = 44,
            .peer_node_ids = &.{ 7, 8, 9 },
        },
    });
    defer std.testing.allocator.free(intent_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = intent_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const intents = try store.listPlacementIntents(std.testing.allocator, 51);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expectEqual(@as(u64, 5101), intents[0].record.group_id);
        try std.testing.expectEqual(@as(u64, 7), intents[0].record.local_node_id);
        try std.testing.expectEqual(@as(u64, 44), intents[0].store_id);
        try std.testing.expectEqual(@as(usize, 3), intents[0].peer_node_ids.len);
        try std.testing.expectEqual(raft_catalog.ReplicaBootstrapMode.fetch_snapshot, intents[0].record.bootstrap_mode);
        try std.testing.expect(intents[0].record.snapshot_bootstrap != null);
        try std.testing.expectEqual(@as(u64, 3), intents[0].record.snapshot_bootstrap.?.from_node_id);
        try std.testing.expectEqual(@as(u64, 8), intents[0].record.snapshot_bootstrap.?.term);
        try std.testing.expectEqualStrings("snap-5101", intents[0].record.snapshot_bootstrap.?.snapshot_id);
    }
}

test "metadata raft apply store projects backup restore bootstrap source in placement intents" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-backup-source-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const intent_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replica_intent = .{
            .record = .{
                .group_id = 5201,
                .replica_id = 2,
                .local_node_id = 7,
                .bootstrap_mode = .fetch_snapshot,
                .metadata_version = 4,
                .backup_restore_bootstrap = .{
                    .backup_id = "snap-5201",
                    .location = "file:///tmp/backups",
                    .snapshot_path = "snap-5201/groups/5201",
                },
            },
            .store_id = 45,
            .peer_node_ids = &.{ 7, 8, 9 },
        },
    });
    defer std.testing.allocator.free(intent_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = intent_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 52,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const intents = try store.listPlacementIntents(std.testing.allocator, 52);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expect(intents[0].record.backup_restore_bootstrap != null);
        try std.testing.expectEqualStrings("snap-5201", intents[0].record.backup_restore_bootstrap.?.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", intents[0].record.backup_restore_bootstrap.?.location);
        try std.testing.expectEqualStrings("snap-5201/groups/5201", intents[0].record.backup_restore_bootstrap.?.snapshot_path);
    }
}

test "metadata state machine projects transitions through metadata apply store" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sm-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const SinkRecorder = struct {
        last_index: u64 = 0,

        fn sink(self: *@This()) raft_state_machine.AppliedIndexSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .set_applied_index = setAppliedIndex,
                },
            };
        }

        fn setAppliedIndex(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_index = index;
        }
    };

    var sink = SinkRecorder{};
    var sm = raft_state_machine.MetadataStateMachine{
        .alloc = std.testing.allocator,
        .applied_sink = sink.sink(),
        .snapshot_builder = store.snapshotBuilder(),
    };

    const cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 701,
            .source_group_id = 41,
            .destination_group_id = 42,
            .phase = .bootstrap_peer,
        },
    });
    defer std.testing.allocator.free(cmd);

    try sm.stateMachine().applyReady(41, &.{
        .{ .term = 3, .index = 12, .entry_type = .normal, .data = cmd },
    }, &.{});

    const splits = try store.listSplitTransitions(std.testing.allocator, 41);
    defer store.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 701), splits[0].transition_id);
    try std.testing.expectEqual(@as(u64, 12), sink.last_index);
}

test "metadata raft apply store runtime status codec preserves document identity telemetry" {
    const alloc = std.testing.allocator;

    var runtime_statuses = [_]metadata.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .table_name = "docs",
        .group_id = 10,
        .store_id = 20,
        .node_id = 30,
        .source = "background_refresh",
        .freshness = "fresh",
        .doc_identity = .{
            .namespace_table_id = 1,
            .namespace_shard_id = 10,
            .namespace_range_id = 1001,
            .next_ordinal = 44,
            .allocated_ordinals = 43,
            .ordinal_capacity_remaining = 123,
            .rebuild_required = true,
            .state_rows = 42,
            .live_ordinals = 40,
            .tombstone_ordinals = 2,
            .min_created_generation = 11,
            .max_created_generation = 17,
            .min_deleted_generation = 15,
            .max_deleted_generation = 18,
            .scanned_primary_docs = 41,
            .primary_docs_missing_ordinals = 1,
            .primary_docs_with_tombstone_ordinals = 1,
            .complete = true,
        },
        .doc_set_planning = .{
            .resolved_set_count = 9,
            .ordinal_list_count = 8,
            .ordinal_list_docs = 7,
            .missing_ordinal_coverage_count = 6,
            .stale_identity_generation_rejection_count = 5,
        },
    }};

    const encoded = try encodeStoreRecord(alloc, .{
        .store_id = 20,
        .node_id = 30,
        .runtime_statuses = runtime_statuses[0..],
    });
    defer alloc.free(encoded);

    const decoded = try decodeStoreRecord(alloc, encoded);
    defer metadata_table_manager.freeStore(alloc, decoded);

    try std.testing.expectEqual(@as(usize, 1), decoded.runtime_statuses.len);
    const status = decoded.runtime_statuses[0];
    try std.testing.expectEqual(@as(u64, 1), status.doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 10), status.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 1001), status.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 44), status.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 43), status.doc_identity.allocated_ordinals);
    try std.testing.expect(status.doc_identity.rebuild_required);
    try std.testing.expect(status.doc_identity.complete);
    try std.testing.expectEqual(@as(u64, 9), status.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 8), status.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 7), status.doc_set_planning.ordinal_list_docs);
    try std.testing.expectEqual(@as(u64, 6), status.doc_set_planning.missing_ordinal_coverage_count);
    try std.testing.expectEqual(@as(u64, 5), status.doc_set_planning.stale_identity_generation_rejection_count);
}

test "metadata apply store replay is idempotent when applied watermark lags WAL state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-apply-replay-idempotent", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try raft_storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 202, 1);
    defer layout.deinit(std.testing.allocator);

    const cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 902,
            .source_group_id = 51,
            .destination_group_id = 52,
            .phase = .bootstrap_peer,
        },
    });
    defer std.testing.allocator.free(cmd);

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();

        const entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 5, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);

        try wal_state.groupStorage().persistReady(202, .{
            .hard_state = .{ .current_term = 5, .voted_for = 1, .commit_index = 1 },
            .entries = entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.MetadataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(202, &.{
            .{ .term = 5, .index = 1, .entry_type = .normal, .data = cmd },
        }, &.{});
    }

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();
        try std.testing.expectEqual(@as(u64, 0), wal_state.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 202,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = wal_state.appliedIndex(),
        }, wal_state.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.MetadataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(202, rd.committed_entries, &.{});

        const batch = (try store.latestBatch(202)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 1), batch.commit_index);

        const splits = try store.listSplitTransitions(std.testing.allocator, 202);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        try std.testing.expectEqual(@as(usize, 1), splits.len);
        try std.testing.expectEqual(@as(u64, 902), splits[0].transition_id);
        try std.testing.expectEqual(@as(u64, 51), splits[0].source_group_id);
        try std.testing.expectEqual(@as(u64, 52), splits[0].destination_group_id);
    }
}
