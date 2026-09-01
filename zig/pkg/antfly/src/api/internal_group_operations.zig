// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral operations for internal group coordination.

const std = @import("std");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const batch_api = @import("batch.zig");
const distributed_txn = @import("distributed_txn.zig");
const distributed_graph = @import("distributed_graph.zig");
const db_mod = @import("../storage/db/mod.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const metadata_mod = @import("../metadata/domain.zig");
const operation = @import("operation.zig");
const raft_mod = @import("../raft/mod.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_write_source.zig");
const query_api = @import("query.zig");
const runtime_preflight = @import("../storage/db/runtime_preflight.zig");
const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
const platform_time = @import("antfly_platform").time;

pub const Error = operation.ApiError || error{
    TopologyChanged,
    IdentityReadGenerationChanged,
    HierarchyCursorStale,
    DocIdentityNamespaceMismatch,
    StorageReadTemporarilyUnavailable,
    QueryCandidateBudgetExceeded,
    GraphExploredEdgesBudgetExceeded,
    GraphExploredEdgeBytesBudgetExceeded,
    GroupLeaderUnavailable,
    PreDecisionDeadlineExceeded,
    TransactionPreDecisionOutcomeUnknown,
    RaftBatchWriteOutcomeUnknown,
    DecisionConflict,
    TransactionConflict,
    EnrichmentWaitCanceled,
    EnrichmentWaitTimeout,
    EnrichmentRetryInProgress,
    EnrichmentWorkerFailed,
    RepairCanceled,
    InvalidRepairCancelToken,
};

pub const RepairCancellationLookup = struct {
    ptr: *anyopaque,
    is_requested_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, u64, ?[]const u8) anyerror!bool,

    fn isRequested(self: @This(), alloc: std.mem.Allocator, table_name: []const u8, job_id: u64, attempt_id: u64, base_uri: ?[]const u8) !bool {
        return self.is_requested_fn(self.ptr, alloc, table_name, job_id, attempt_id, base_uri);
    }
};

pub const RoutedRaftBatchWriter = struct {
    ptr: *anyopaque,
    write_fn: *const fn (
        *anyopaque,
        std.mem.Allocator,
        u64,
        []const u8,
        db_mod.types.BatchRequest,
        internal_batch_forwarding.Context,
        CancellationToken,
    ) anyerror!?void,

    fn write(self: @This(), alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, input: db_mod.types.BatchRequest, forwarding: internal_batch_forwarding.Context, cancellation: CancellationToken) !?void {
        return self.write_fn(self.ptr, alloc, group_id, table_name, input, forwarding, cancellation);
    }
};

pub const BatchValidator = struct {
    ptr: *anyopaque,
    validate_fn: *const fn (*anyopaque, []const u8, []const db_mod.types.BatchWrite) anyerror!void,

    fn validate(self: BatchValidator, table_name: []const u8, writes: []const db_mod.types.BatchWrite) !void {
        return self.validate_fn(self.ptr, table_name, writes);
    }
};

pub const TxnValidator = struct {
    ptr: *anyopaque,
    validate_fn: *const fn (*anyopaque, []const u8, []const db_mod.types.TransactionWrite) anyerror!void,

    fn validate(self: TxnValidator, table_name: []const u8, writes: []const db_mod.types.TransactionWrite) !void {
        return self.validate_fn(self.ptr, table_name, writes);
    }
};

pub const LookupInput = struct {
    group_id: u64,
    table_name: []const u8,
    key: []const u8,
    options: db_mod.types.LookupOptions = .{},
    consistency: raft_mod.ReadConsistency = .read_index,
};

pub const Operations = struct {
    reads: ?table_reads.TableReadSource,
    shard_db_adapter: ?metadata_mod.ShardDbAdapter,
    writes: ?table_writes.TableWriteSource = null,
    shard_ops: ?raft_mod.ShardOperationAdapter = null,
    batch_validator: ?BatchValidator = null,
    reject_unrouted_batch: bool = false,
    txn_validator: ?TxnValidator = null,
    repair_cancellation_lookup: ?RepairCancellationLookup = null,
    routed_raft_batch_writer: ?RoutedRaftBatchWriter = null,

    pub fn corruptEmbeddingArtifact(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) Error!void {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        _ = (writes.corruptEmbeddingArtifact(alloc, table_name, doc_key, index_name) catch |err| switch (err) {
            error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse return error.NotFound;
    }

    pub fn observeSplit(
        self: Operations,
        request: operation.RequestContext,
        group_id: u64,
        record: @import("../metadata/transition_state.zig").SplitTransitionRecord,
    ) Error!@import("../metadata/transition_state.zig").SplitObservation {
        try request.ensureActive();
        const ops = self.shard_ops orelse return error.NotFound;
        if (group_id != record.source_group_id and group_id != record.destination_group_id)
            return error.InvalidArgument;
        var observation = ops.observeSplit(record) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownSplitRuntime, error.MissingSplitRuntime => return error.NotFound,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.SplitSourceProjectionNotReady,
            error.DurableRootIncarnationUnavailable,
            error.AutoBulkIngestBusy,
            error.ApplyStoreGroupRetired,
            error.ApplyStoreShuttingDown,
            => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        };
        if (group_id == record.source_group_id) observation.source_local_leader = true;
        if (group_id == record.destination_group_id) observation.destination_local_leader = true;
        return observation;
    }

    pub fn observeMerge(
        self: Operations,
        request: operation.RequestContext,
        group_id: u64,
        record: @import("../metadata/transition_state.zig").MergeTransitionRecord,
    ) Error!@import("../metadata/transition_state.zig").MergeObservation {
        try request.ensureActive();
        const ops = self.shard_ops orelse return error.NotFound;
        if (group_id != record.donor_group_id and group_id != record.receiver_group_id)
            return error.InvalidArgument;
        var observation = ops.observeMerge(record) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownMergeRuntime, error.MissingMergeRuntime => return error.NotFound,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.LeaderUnavailable, error.GroupLeaderUnavailable => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        };
        if (group_id == record.donor_group_id) observation.donor_local_leader = true;
        if (group_id == record.receiver_group_id) observation.receiver_local_leader = true;
        return observation;
    }

    pub fn executeTransition(
        self: Operations,
        request: operation.RequestContext,
        group_id: u64,
        action: @import("../metadata/domain.zig").TransitionAction,
    ) Error!void {
        try request.ensureActive();
        const ops = self.shard_ops orelse return error.NotFound;
        if (!transitionActionMatchesGroup(action, group_id)) return error.InvalidArgument;
        ops.execute(action) catch |err| switch (err) {
            error.UnknownGroup,
            error.UnknownSplitRuntime,
            error.UnknownMergeRuntime,
            error.MissingSplitRuntime,
            error.MissingMergeRuntime,
            => return error.NotFound,
            error.TopologyChanged => return error.TopologyChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.MetadataSnapshotUnavailable,
            error.SplitSourceProjectionNotReady,
            error.SplitSourceProjectionAdvanced,
            error.DurableRootIncarnationUnavailable,
            error.AutoBulkIngestBusy,
            error.ApplyStoreGroupRetired,
            error.ApplyStoreShuttingDown,
            error.BackgroundOwnerClosing,
            error.BackgroundOwnerClosed,
            error.TransitionOperationBusy,
            => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        };
    }

    pub fn batch(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: db_mod.types.BatchRequest,
    ) Error!batch_api.BatchResult {
        try request.ensureActive();
        if (self.reject_unrouted_batch) return error.Unsupported;
        const writes = self.writes orelse return error.NotFound;
        const validator = self.batch_validator orelse return error.Unavailable;
        validator.validate(table_name, input.writes) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidArgument,
            else => {
                std.log.err("group-local Raft batch validation failed group_id={} table={s} err={s}", .{
                    group_id,
                    table_name,
                    @errorName(err),
                });
                return error.Internal;
            },
        };
        _ = (writes.batchGroupLocal(alloc, group_id, table_name, input) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.RaftBatchWriteOutcomeUnknown => return error.RaftBatchWriteOutcomeUnknown,
            error.EnrichmentWaitCanceled => return error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout => return error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress => return error.EnrichmentRetryInProgress,
            error.EnrichmentWorkerFailed => return error.EnrichmentWorkerFailed,
            error.LeaderUnavailable, error.GroupLeaderUnavailable, error.MetadataSnapshotUnavailable => return error.GroupLeaderUnavailable,
            else => {
                std.log.err("group-local Raft batch failed group_id={} table={s} err={s}", .{
                    group_id,
                    table_name,
                    @errorName(err),
                });
                return error.Internal;
            },
        }) orelse return error.NotFound;
        return .{
            .inserted = @intCast(input.writes.len),
            .deleted = @intCast(input.deletes.len),
            .transformed = @intCast(input.transforms.len),
        };
    }

    pub fn routedBatch(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: db_mod.types.BatchRequest,
        forwarding: internal_batch_forwarding.Context,
    ) Error!batch_api.BatchResult {
        try request.ensureActive();
        const validator = self.batch_validator orelse return error.Unavailable;
        validator.validate(table_name, input.writes) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidArgument,
            else => {
                std.log.err("routed Raft batch validation failed group_id={} table={s} err={s}", .{
                    group_id,
                    table_name,
                    @errorName(err),
                });
                return error.Internal;
            },
        };
        const writer = self.routed_raft_batch_writer orelse return error.Unavailable;
        _ = (writer.write(alloc, group_id, table_name, input, forwarding, request.cancellation) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.RaftBatchWriteOutcomeUnknown => return error.RaftBatchWriteOutcomeUnknown,
            error.EnrichmentWaitCanceled => return error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout => return error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress => return error.EnrichmentRetryInProgress,
            error.EnrichmentWorkerFailed => return error.EnrichmentWorkerFailed,
            error.LeaderUnavailable, error.GroupLeaderUnavailable, error.MetadataSnapshotUnavailable => return error.GroupLeaderUnavailable,
            else => {
                std.log.err("routed Raft batch failed group_id={} table={s} err={s}", .{
                    group_id,
                    table_name,
                    @errorName(err),
                });
                return error.Internal;
            },
        }) orelse return error.NotFound;
        return .{
            .inserted = @intCast(input.writes.len),
            .deleted = @intCast(input.deletes.len),
            .transformed = @intCast(input.transforms.len),
        };
    }

    pub fn txnBegin(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_txn.TxnBeginRequest) Error!void {
        try ensurePreDecisionRequestActive(request);
        const writes = self.writes orelse return error.NotFound;
        const supports_pre_decision_context =
            writes.vtable.txn_begin_group_local_with_pre_decision_context != null;
        _ = (writes.txnBeginGroupLocalWithPreDecisionContext(alloc, group_id, table_name, input.txn_id, input.begin_timestamp, input.topology_epoch, input.retain_terminal, input.participants, .{
            .deadline_ns = request.deadline_ns,
            .cancellation = request.cancellation,
        }) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidArgument,
            error.Canceled, error.Cancelled => return error.Canceled,
            error.Timeout, error.DeadlineExceeded => return error.TransactionPreDecisionOutcomeUnknown,
            error.PreDecisionDeadlineExceeded => {
                if (!supports_pre_decision_context or request.deadline_ns == null)
                    return error.TransactionPreDecisionOutcomeUnknown;
                return error.PreDecisionDeadlineExceeded;
            },
            error.DecisionConflict => return error.DecisionConflict,
            error.TopologyChanged => return error.TopologyChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TxnNotFound => return error.NotFound,
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.MetadataSnapshotUnavailable,
            => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        }) orelse return error.NotFound;
    }

    pub fn txnPrepare(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_txn.TxnPrepareRequest) Error!void {
        try ensurePreDecisionRequestActive(request);
        const writes = self.writes orelse return error.NotFound;
        const supports_pre_decision_context =
            writes.vtable.txn_prepare_group_local_with_pre_decision_context != null;
        const validator = self.txn_validator orelse return error.Unavailable;
        validator.validate(table_name, input.req.writes) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidArgument,
            else => return error.Internal,
        };
        _ = (writes.txnPrepareGroupLocalWithPreDecisionContext(alloc, group_id, table_name, input.txn_id, input.topology_epoch, input.req, .{
            .deadline_ns = request.deadline_ns,
            .cancellation = request.cancellation,
        }) catch |err| switch (err) {
            error.Canceled, error.Cancelled => return error.Canceled,
            error.Timeout, error.DeadlineExceeded => return error.TransactionPreDecisionOutcomeUnknown,
            error.PreDecisionDeadlineExceeded => {
                if (!supports_pre_decision_context or request.deadline_ns == null)
                    return error.TransactionPreDecisionOutcomeUnknown;
                return error.PreDecisionDeadlineExceeded;
            },
            error.TopologyChanged => return error.TopologyChanged,
            error.VersionConflict, error.IntentConflict => return error.TransactionConflict,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TxnNotFound => return error.NotFound,
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.MetadataSnapshotUnavailable,
            => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        }) orelse return error.NotFound;
    }

    pub fn txnResolve(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_txn.TxnResolveRequest) Error!void {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        _ = (writes.txnResolveGroupLocalWithCancellation(alloc, group_id, table_name, input.txn_id, input.status, input.commit_version, input.topology_epoch, input.sync_level, request.cancellation) catch |err| switch (err) {
            error.DecisionConflict => return error.DecisionConflict,
            error.TopologyChanged => return error.TopologyChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.EnrichmentWaitCanceled => return error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout => return error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress => return error.EnrichmentRetryInProgress,
            error.EnrichmentWorkerFailed => return error.EnrichmentWorkerFailed,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TxnNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse return error.NotFound;
    }

    pub fn txnStatus(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) Error!db_mod.types.TxnStatus {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        return (writes.txnStatusGroupLocal(alloc, group_id, table_name, txn_id) catch |err| switch (err) {
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TxnNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn txnAcknowledge(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_txn.TxnAcknowledgeRequest) Error!void {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        _ = (writes.txnAcknowledgeGroupLocal(alloc, group_id, table_name, input.txn_id, input.participant) catch |err| switch (err) {
            error.InvalidParticipant, error.DecisionConflict => return error.DecisionConflict,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TxnNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse return error.NotFound;
    }

    pub fn updateDocumentArtifactChildRangePlacement(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) Error!void {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        const handled = (writes.updateDocumentArtifactChildRangePlacementGroupLocal(
            alloc,
            group_id,
            table_name,
            doc_key,
            artifact_name,
            update,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse return error.NotFound;
        if (!handled) return error.NotFound;
    }

    pub fn applyDocumentArtifactChildRangeBatch(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) Error!u64 {
        try request.ensureActive();
        try validateDocumentArtifactChildRangeBatchScope(alloc, doc_key, artifact_name, child_batch);
        const writes = self.writes orelse return error.NotFound;
        return (writes.applyDocumentArtifactChildRangeBatchGroupLocal(
            alloc,
            group_id,
            table_name,
            doc_key,
            artifact_name,
            child_batch,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn reprocessDocumentArtifact(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) Error!void {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        const handled = (writes.reprocessDocumentArtifactGroupLocal(
            alloc,
            group_id,
            table_name,
            doc_key,
            artifact_name,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse return error.NotFound;
        if (!handled) return error.NotFound;
    }

    /// The returned result owns its nested allocations and must be deinitialized
    /// with the same allocator by the caller.
    pub fn reprocessDocumentArtifactRange(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        input: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) Error!db_mod.types.DocumentArtifactTableReprocessResult {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        return (writes.reprocessDocumentArtifactRangeGroupLocal(
            alloc,
            group_id,
            table_name,
            artifact_name,
            input,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// The returned result owns its nested allocations and must be deinitialized
    /// with the same allocator by the caller.
    pub fn listArtifactRepairIssues(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: db_mod.types.ArtifactRepairListRequest,
    ) Error!db_mod.types.ArtifactRepairListResult {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        return (writes.listArtifactRepairIssuesGroupLocal(alloc, group_id, table_name, input) catch |err| switch (err) {
            error.InvalidArgument => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// The returned result owns its nested allocations and must be deinitialized
    /// with the same allocator by the caller.
    pub fn repairArtifactIssues(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: db_mod.types.ArtifactRepairRunRequest,
    ) Error!db_mod.types.ArtifactRepairResult {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        var probe: RepairCancelProbe = undefined;
        var options: db_mod.types.ArtifactRepairRunOptions = .{};
        if (input.repair_job_id != null or input.repair_attempt_id != null) {
            const job_id = input.repair_job_id orelse return error.InvalidRepairCancelToken;
            const attempt_id = input.repair_attempt_id orelse return error.InvalidRepairCancelToken;
            probe = .{
                .alloc = alloc,
                .lookup = self.repair_cancellation_lookup orelse return error.Unavailable,
                .table_name = table_name,
                .job_id = job_id,
                .attempt_id = attempt_id,
                .base_uri = input.repair_cancel_base_uri,
            };
            options.cancel_check = .{ .ptr = &probe, .is_requested = RepairCancelProbe.check };
        }
        return (writes.repairArtifactIssuesGroupLocalControlled(alloc, group_id, table_name, input, options) catch |err| switch (err) {
            error.Canceled => return error.RepairCanceled,
            error.InvalidArgument => return error.InvalidArgument,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    fn transitionActionMatchesGroup(action: @import("../metadata/domain.zig").TransitionAction, group_id: u64) bool {
        return switch (action) {
            .none => group_id == 0,
            .prepare_split_source => |op| group_id == op.source_group_id,
            .start_split_source => |op| group_id == op.source_group_id,
            .bootstrap_split_destination => |op| group_id == op.source_group_id or group_id == op.destination_group_id,
            .catch_up_split_destination => |op| group_id == op.source_group_id or group_id == op.destination_group_id,
            .finalize_split_source => |op| group_id == op.source_group_id,
            .rollback_split => |op| group_id == op.source_group_id,
            .accept_merge_receiver => |op| group_id == op.receiver_group_id,
            .catch_up_merge_receiver => |op| group_id == op.receiver_group_id,
            .finalize_merge => |op| group_id == op.receiver_group_id,
            .rollback_merge => |op| group_id == op.receiver_group_id,
        };
    }

    pub fn lookup(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        input: LookupInput,
    ) Error!table_reads.LookupResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        var options = input.options;
        options.execution_deadline_ns = request.deadline_ns;
        options.cancellation = request.cancellation;
        const result = reads.lookupGroupLocal(
            alloc,
            input.group_id,
            input.table_name,
            input.key,
            options,
            input.consistency,
        ) catch |err| return mapLookupError(err);
        return result orelse error.NotFound;
    }

    /// The returned manifest owns its nested allocations and must be
    /// deinitialized with the same allocator by the caller.
    pub fn documentArtifactManifest(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) Error!db_mod.types.DocumentArtifactManifest {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.documentArtifactManifestGroupLocal(alloc, group_id, table_name, doc_key, artifact_name, .read_index) catch |err| switch (err) {
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// The returned list owns its nested allocations and must be
    /// deinitialized with the same allocator by the caller.
    pub fn documentArtifactManifests(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
    ) Error!db_mod.types.DocumentArtifactManifestList {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.documentArtifactManifestsGroupLocal(alloc, group_id, table_name, doc_key, .read_index) catch |err| switch (err) {
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.UnknownGroup, error.TableNotFound, error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// The returned NDJSON response is owned by `alloc`.
    pub fn scan(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        from: []const u8,
        to: []const u8,
        options: db_mod.types.ScanOptions,
    ) Error!table_reads.ScanResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.scanGroupLocal(alloc, group_id, table_name, from, to, options, .read_index) catch |err| switch (err) {
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// Execute a schema-routed group-local query. The returned response owns
    /// its JSON buffer and must be deinitialized with `alloc`.
    pub fn query(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: db_mod.types.SearchRequest,
    ) Error!query_api.QueryResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.queryGroupLocal(alloc, group_id, table_name, input, .read_index) catch |err| switch (err) {
            error.HierarchyCursorStale => return error.HierarchyCursorStale,
            error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return error.InvalidArgument,
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.UnknownGroup, error.TableNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// Execute a schema-routed group-local query preflight. The returned
    /// summary owns its nested allocations and must be deinitialized with
    /// `alloc`.
    pub fn queryPreflight(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: db_mod.types.SearchRequest,
        max_work: u32,
    ) Error!runtime_preflight.RuntimePreflightSummary {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.preflightQueryGroupLocal(alloc, group_id, table_name, input, .read_index, max_work) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return error.InvalidArgument,
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.UnknownGroup, error.TableNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn graphExpand(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_graph.GraphExpandRequest) Error!distributed_graph.GraphExpandResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.graphExpandGroupLocal(alloc, group_id, table_name, input, .read_index) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return error.InvalidArgument,
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.UnknownGroup, error.TableNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn graphHydrate(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_graph.GraphHydrateRequest) Error!distributed_graph.GraphHydrateResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.graphHydrateGroupLocal(alloc, group_id, table_name, input, .read_index) catch |err| switch (err) {
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.UnknownGroup, error.TableNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn graphEdges(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, input: distributed_graph.GraphEdgesRequest) Error!distributed_graph.GraphEdgesResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.graphEdgesGroupLocal(alloc, group_id, table_name, input, .read_index) catch |err| switch (err) {
            error.InvalidQueryRequest, error.IndexNotFound => return error.InvalidArgument,
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            error.GraphExploredEdgesBudgetExceeded => return error.GraphExploredEdgesBudgetExceeded,
            error.GraphExploredEdgeBytesBudgetExceeded => return error.GraphExploredEdgeBytesBudgetExceeded,
            error.UnknownGroup, error.TableNotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn textStats(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, body: []const u8) Error!query_api.QueryResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.textStatsGroupLocal(alloc, group_id, table_name, body) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidArgument,
            error.TableNotFound, error.UnknownGroup => return error.NotFound,
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    pub fn algebraicPartials(self: Operations, alloc: std.mem.Allocator, request: operation.RequestContext, group_id: u64, table_name: []const u8, body: []const u8) Error!query_api.QueryResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        return (reads.algebraicPartialsGroupLocal(alloc, group_id, table_name, body) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidArgument,
            error.TableNotFound, error.UnknownGroup => return error.NotFound,
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            else => return error.Internal,
        }) orelse error.NotFound;
    }

    /// The returned key, when present, is owned by `alloc`.
    pub fn medianKey(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
    ) Error!?[]u8 {
        try request.ensureActive();
        const adapter = self.shard_db_adapter orelse return error.NotFound;
        return adapter.fetchMedianKey(alloc, group_id) catch |err| switch (err) {
            error.UnknownGroup => error.NotFound,
            error.UnsupportedOperation => error.Unsupported,
            else => error.Internal,
        };
    }
};

fn mapLookupError(err: anyerror) Error {
    return switch (err) {
        error.Timeout => error.DeadlineExceeded,
        error.Cancelled, error.Canceled => error.Canceled,
        error.TopologyChanged => error.TopologyChanged,
        error.IdentityReadGenerationChanged => error.IdentityReadGenerationChanged,
        error.DocIdentityNamespaceMismatch => error.DocIdentityNamespaceMismatch,
        error.NotLeader,
        error.LeaderUnavailable,
        error.GroupLeaderUnavailable,
        error.UnknownGroup,
        error.ReadUnavailable,
        => error.GroupLeaderUnavailable,
        error.PersistentDescriptorAdmissionExhausted,
        error.ResourceBudgetExceeded,
        error.WriterLocked,
        error.LsmRootWriterAlreadyOpen,
        error.ResidentDbRetryRequired,
        error.StorageReadTemporarilyUnavailable,
        => error.StorageReadTemporarilyUnavailable,
        error.TableNotFound, error.NotFound => error.NotFound,
        else => error.Internal,
    };
}

fn ensurePreDecisionRequestActive(request: operation.RequestContext) Error!void {
    request.ensureActive() catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        // This check runs before a participant callback can admit a mutation,
        // so it is safe to give the deadline failure a stronger identity.
        error.DeadlineExceeded => return error.PreDecisionDeadlineExceeded,
        else => return error.Internal,
    };
}

test "internal transaction operations preserve pre-decision leader unavailability" {
    const Source = struct {
        fn iface() table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .txn_begin_group_local = txnBegin,
                    .txn_prepare_group_local = txnPrepare,
                },
            };
        }

        fn batch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
        ) anyerror!?void {
            return null;
        }

        fn txnBegin(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            _: u64,
            _: bool,
            _: []const []const u8,
        ) anyerror!?void {
            return error.LeaderUnavailable;
        }

        fn txnPrepare(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            _: db_mod.types.TransactionIntentRequest,
        ) anyerror!?void {
            return error.MetadataSnapshotUnavailable;
        }
    };

    const LegacyDeadlineSource = struct {
        fn iface() table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .txn_begin_group_local = txnBegin,
                    .txn_prepare_group_local = txnPrepare,
                },
            };
        }

        fn batch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
        ) anyerror!?void {
            return null;
        }

        fn txnBegin(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            _: u64,
            _: bool,
            _: []const []const u8,
        ) anyerror!?void {
            return error.DeadlineExceeded;
        }

        fn txnPrepare(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            _: db_mod.types.TransactionIntentRequest,
        ) anyerror!?void {
            return error.Timeout;
        }
    };

    const ContextDeadlineSource = struct {
        failure: anyerror,

        fn iface(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .txn_begin_group_local_with_pre_decision_context = txnBegin,
                    .txn_prepare_group_local_with_pre_decision_context = txnPrepare,
                },
            };
        }

        fn batch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
        ) anyerror!?void {
            return null;
        }

        fn txnBegin(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            _: u64,
            _: bool,
            _: []const []const u8,
            _: distributed_txn.PreDecisionContext,
        ) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }

        fn txnPrepare(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            _: db_mod.types.TransactionIntentRequest,
            _: distributed_txn.PreDecisionContext,
        ) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }
    };

    const Validator = struct {
        fn validate(_: *anyopaque, _: []const u8, _: []const db_mod.types.TransactionWrite) anyerror!void {}
    };

    const operations = Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = Source.iface(),
        .txn_validator = .{ .ptr = undefined, .validate_fn = Validator.validate },
    };
    const txn_id = [_]u8{0x42} ** 16;
    try std.testing.expectError(error.GroupLeaderUnavailable, operations.txnBegin(
        std.testing.allocator,
        .{},
        7,
        "docs",
        .{ .txn_id = txn_id, .begin_timestamp = 1, .participants = &.{"table2:docs:group:7"} },
    ));
    try std.testing.expectError(error.GroupLeaderUnavailable, operations.txnPrepare(
        std.testing.allocator,
        .{},
        7,
        "docs",
        .{ .txn_id = txn_id, .req = .{} },
    ));

    const legacy_deadline_operations = Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = LegacyDeadlineSource.iface(),
        .txn_validator = .{ .ptr = undefined, .validate_fn = Validator.validate },
    };
    try std.testing.expectError(error.TransactionPreDecisionOutcomeUnknown, legacy_deadline_operations.txnBegin(
        std.testing.allocator,
        .{},
        7,
        "docs",
        .{ .txn_id = txn_id, .begin_timestamp = 1, .participants = &.{"table2:docs:group:7"} },
    ));
    try std.testing.expectError(error.TransactionPreDecisionOutcomeUnknown, legacy_deadline_operations.txnPrepare(
        std.testing.allocator,
        .{},
        7,
        "docs",
        .{ .txn_id = txn_id, .req = .{} },
    ));

    var context_deadline_source = ContextDeadlineSource{ .failure = error.Timeout };
    const context_deadline_operations = Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = context_deadline_source.iface(),
        .txn_validator = .{ .ptr = undefined, .validate_fn = Validator.validate },
    };
    const active_deadline = operation.RequestContext{ .deadline_ns = std.math.maxInt(u64) };
    try std.testing.expectError(error.TransactionPreDecisionOutcomeUnknown, context_deadline_operations.txnBegin(
        std.testing.allocator,
        active_deadline,
        7,
        "docs",
        .{ .txn_id = txn_id, .begin_timestamp = 1, .participants = &.{"table2:docs:group:7"} },
    ));
    context_deadline_source.failure = error.DeadlineExceeded;
    try std.testing.expectError(error.TransactionPreDecisionOutcomeUnknown, context_deadline_operations.txnPrepare(
        std.testing.allocator,
        active_deadline,
        7,
        "docs",
        .{ .txn_id = txn_id, .req = .{} },
    ));
    context_deadline_source.failure = error.PreDecisionDeadlineExceeded;
    try std.testing.expectError(error.TransactionPreDecisionOutcomeUnknown, context_deadline_operations.txnBegin(
        std.testing.allocator,
        .{},
        7,
        "docs",
        .{ .txn_id = txn_id, .begin_timestamp = 1, .participants = &.{"table2:docs:group:7"} },
    ));
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, context_deadline_operations.txnBegin(
        std.testing.allocator,
        active_deadline,
        7,
        "docs",
        .{ .txn_id = txn_id, .begin_timestamp = 1, .participants = &.{"table2:docs:group:7"} },
    ));
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, context_deadline_operations.txnPrepare(
        std.testing.allocator,
        active_deadline,
        7,
        "docs",
        .{ .txn_id = txn_id, .req = .{} },
    ));
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, operations.txnBegin(
        std.testing.allocator,
        .{ .deadline_ns = 1 },
        7,
        "docs",
        .{ .txn_id = txn_id, .begin_timestamp = 1, .participants = &.{"table2:docs:group:7"} },
    ));
}

const RepairCancelProbe = struct {
    alloc: std.mem.Allocator,
    lookup: RepairCancellationLookup,
    table_name: []const u8,
    job_id: u64,
    attempt_id: u64,
    base_uri: ?[]const u8,
    cached_requested: bool = false,
    last_check_ns: u64 = 0,

    fn check(ptr: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.cached_requested) return true;
        const now_ns = platform_time.monotonicNs();
        if (self.last_check_ns != 0 and now_ns -| self.last_check_ns < 100 * std.time.ns_per_ms) return false;
        self.last_check_ns = now_ns;
        const requested = self.lookup.isRequested(self.alloc, self.table_name, self.job_id, self.attempt_id, self.base_uri) catch return false;
        self.cached_requested = requested;
        return requested;
    }
};

const DocumentArtifactChildKeyPrefixes = struct {
    unit: []u8,
    chunk: []u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.unit);
        alloc.free(self.chunk);
        self.* = undefined;
    }
};

fn documentArtifactChildKeyPrefixesAlloc(alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8) !DocumentArtifactChildKeyPrefixes {
    var unit = std.ArrayListUnmanaged(u8).empty;
    errdefer unit.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&unit, alloc, doc_key);
    try unit.append(alloc, internal_keys.artifact_kind);
    try internal_keys.appendEncodedComponent(&unit, alloc, "asset");
    try internal_keys.appendEncodedComponent(&unit, alloc, artifact_name);
    try unit.append(alloc, internal_keys.document_unit_record_kind);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    errdefer chunk.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&chunk, alloc, doc_key);
    try chunk.append(alloc, internal_keys.artifact_kind);
    try internal_keys.appendEncodedComponent(&chunk, alloc, "chunk");
    try internal_keys.appendEncodedComponent(&chunk, alloc, artifact_name);
    try chunk.append(alloc, internal_keys.document_unit_record_kind);

    const owned_unit = try unit.toOwnedSlice(alloc);
    errdefer alloc.free(owned_unit);
    return .{ .unit = owned_unit, .chunk = try chunk.toOwnedSlice(alloc) };
}

fn validateDocumentArtifactChildRangeBatchScope(
    alloc: std.mem.Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
) Error!void {
    var prefixes = documentArtifactChildKeyPrefixesAlloc(alloc, doc_key, artifact_name) catch return error.Internal;
    defer prefixes.deinit(alloc);
    const matches = struct {
        fn call(p: DocumentArtifactChildKeyPrefixes, key: []const u8) bool {
            return std.mem.startsWith(u8, key, p.unit) or std.mem.startsWith(u8, key, p.chunk);
        }
    }.call;
    for (child_batch.artifact_writes) |write| if (!matches(prefixes, write.key)) return error.InvalidArgument;
    for (child_batch.artifact_delete_keys) |key| if (!matches(prefixes, key)) return error.InvalidArgument;
    for (child_batch.documents) |doc| if (!matches(prefixes, doc.key)) return error.InvalidArgument;
    for (child_batch.dense_embeddings) |embedding| if (embedding.artifact_key) |key| {
        if (!matches(prefixes, key)) return error.InvalidArgument;
    };
    for (child_batch.sparse_embeddings) |embedding| if (embedding.artifact_key) |key| {
        if (!matches(prefixes, key)) return error.InvalidArgument;
    };
}

test "typed routed batch preserves forwarding cancellation and identity conflicts" {
    const State = struct {
        cancellation_signal: *const std.atomic.Value(bool),
        calls: usize = 0,
        fail_identity: bool = false,
        visibility_error: ?anyerror = null,

        fn validate(ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.BatchWrite) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self;
            try std.testing.expectEqualStrings("documents", table_name);
            try std.testing.expectEqual(@as(usize, 0), writes.len);
        }

        fn write(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            _: db_mod.types.BatchRequest,
            forwarding: internal_batch_forwarding.Context,
            cancellation: CancellationToken,
        ) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as(u64, 17), group_id);
            try std.testing.expectEqualStrings("documents", table_name);
            try std.testing.expectEqual(@as(u32, 425), forwarding.remaining_ms);
            try std.testing.expectEqual(@as(u8, 1), forwarding.forwards_remaining);
            try std.testing.expect(!forwarding.campaign_allowed);
            try std.testing.expect(cancellation.ptr == @as(*const anyopaque, @ptrCast(self.cancellation_signal)));
            try std.testing.expect(!cancellation.isCancelled());
            if (self.fail_identity) return error.DocIdentityNamespaceMismatch;
            if (self.visibility_error) |err| return err;
            return {};
        }
    };

    var cancelled = std.atomic.Value(bool).init(false);
    var state = State{ .cancellation_signal = &cancelled };
    const operations = Operations{
        .reads = null,
        .shard_db_adapter = null,
        .batch_validator = .{ .ptr = &state, .validate_fn = State.validate },
        .routed_raft_batch_writer = .{ .ptr = &state, .write_fn = State.write },
    };
    const forwarding: internal_batch_forwarding.Context = .{
        .remaining_ms = 425,
        .forwards_remaining = 1,
        .campaign_allowed = false,
    };
    const request: operation.RequestContext = .{
        .cancellation = CancellationToken.fromAtomic(&cancelled),
    };

    const result = try operations.routedBatch(
        std.testing.allocator,
        request,
        17,
        "documents",
        .{},
        forwarding,
    );
    try std.testing.expectEqual(@as(u32, 0), result.inserted);
    try std.testing.expectEqual(@as(usize, 1), state.calls);

    state.fail_identity = true;
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, operations.routedBatch(
        std.testing.allocator,
        request,
        17,
        "documents",
        .{},
        forwarding,
    ));
    try std.testing.expectEqual(@as(usize, 2), state.calls);

    state.fail_identity = false;
    state.visibility_error = error.EnrichmentRetryInProgress;
    try std.testing.expectError(error.EnrichmentRetryInProgress, operations.routedBatch(
        std.testing.allocator,
        request,
        17,
        "documents",
        .{},
        forwarding,
    ));
    state.visibility_error = error.EnrichmentWorkerFailed;
    try std.testing.expectError(error.EnrichmentWorkerFailed, operations.routedBatch(
        std.testing.allocator,
        request,
        17,
        "documents",
        .{},
        forwarding,
    ));
    try std.testing.expectEqual(@as(usize, 4), state.calls);
}

test "typed internal query workers preserve identity generation validation" {
    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{ .ptr = undefined, .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = publicQuery,
                .query_group_local = groupQuery,
                .graph_expand_group_local = graphExpand,
            } };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return null;
        }

        fn publicQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return null;
        }

        fn groupQuery(_: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqual(@as(u64, 17), group_id);
            try std.testing.expectEqualStrings("documents", table_name);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
            if (req.hierarchy_children != null) return error.HierarchyCursorStale;
            return error.UnsupportedQueryRequest;
        }

        fn graphExpand(_: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: distributed_graph.GraphExpandRequest, consistency: raft_mod.ReadConsistency) !?distributed_graph.GraphExpandResponse {
            try std.testing.expectEqual(@as(u64, 17), group_id);
            try std.testing.expectEqualStrings("documents", table_name);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
            return error.UnsupportedQueryRequest;
        }
    };

    const operations = Operations{
        .reads = FakeReads.source(),
        .shard_db_adapter = null,
    };

    try std.testing.expectError(error.InvalidArgument, operations.query(
        std.testing.allocator,
        .{},
        17,
        "documents",
        .{ .identity_read_generation = 12345 },
    ));
    try std.testing.expectError(error.HierarchyCursorStale, operations.query(
        std.testing.allocator,
        .{},
        17,
        "documents",
        .{
            .identity_read_generation = 12345,
            .hierarchy_children = .{ .parent_id = "doc:a" },
        },
    ));

    var frontier: [0]distributed_graph.GraphFrontierItem = .{};
    var exclude_nodes: [0]distributed_graph.GraphNodeIdentity = .{};
    var exclude_edges: [0][]u8 = .{};
    try std.testing.expectError(error.InvalidArgument, operations.graphExpand(
        std.testing.allocator,
        .{},
        17,
        "documents",
        .{
            .name = @constCast("graph"),
            .index_name = @constCast("graph-index"),
            .frontier = frontier[0..],
            .exclude_nodes = exclude_nodes[0..],
            .exclude_edges = exclude_edges[0..],
            .params = .{},
            .identity_read_generation = 12345,
        },
    ));
}

test "typed internal group reads preserve retryable resident storage failures" {
    const alloc = std.testing.allocator;
    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{ .ptr = undefined, .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = publicQuery,
                .query_group_local = groupQuery,
                .preflight_query_group_local = groupPreflight,
                .scan_group_local = groupScan,
                .text_stats_group_local = auxiliary,
                .algebraic_partials_group_local = auxiliary,
                .document_artifact_manifest_group_local = artifact,
                .document_artifact_manifests_group_local = artifacts,
            } };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return null;
        }

        fn publicQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return null;
        }

        fn groupQuery(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.StorageReadTemporarilyUnavailable;
        }

        fn groupPreflight(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency, _: u32) !?runtime_preflight.RuntimePreflightSummary {
            return error.StorageReadTemporarilyUnavailable;
        }

        fn groupScan(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.StorageReadTemporarilyUnavailable;
        }

        fn auxiliary(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            return error.StorageReadTemporarilyUnavailable;
        }

        fn artifact(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8, _: []const u8, _: raft_mod.ReadConsistency) !?db_mod.types.DocumentArtifactManifest {
            return error.StorageReadTemporarilyUnavailable;
        }

        fn artifacts(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8, _: raft_mod.ReadConsistency) !?db_mod.types.DocumentArtifactManifestList {
            return error.StorageReadTemporarilyUnavailable;
        }
    };

    const operations = Operations{ .reads = FakeReads.source(), .shard_db_adapter = null };
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.scan(alloc, .{}, 7, "docs", "", "", .{}));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.query(alloc, .{}, 7, "docs", .{}));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.queryPreflight(alloc, .{}, 7, "docs", .{}, 0));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.textStats(alloc, .{}, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.algebraicPartials(alloc, .{}, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.documentArtifactManifest(alloc, .{}, 7, "docs", "doc:a", "chunks"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, operations.documentArtifactManifests(alloc, .{}, 7, "docs", "doc:a"));
    try std.testing.expectEqual(error.GroupLeaderUnavailable, mapLookupError(error.NotLeader));
    try std.testing.expectEqual(error.GroupLeaderUnavailable, mapLookupError(error.UnknownGroup));
    try std.testing.expectEqual(
        error.StorageReadTemporarilyUnavailable,
        mapLookupError(error.ResourceBudgetExceeded),
    );
    try std.testing.expectEqual(error.Internal, mapLookupError(error.CorruptInput));
}

test "internal group reads are callable without an HTTP request" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        fn reads() table_reads.TableReadSource {
            return .{ .ptr = undefined, .vtable = &.{
                .lookup = publicLookup,
                .scan = scan,
                .query = query,
                .lookup_group_local = groupLookup,
            } };
        }

        fn shardDb() metadata_mod.ShardDbAdapter {
            return .{ .ptr = undefined, .vtable = &.{
                .fetch_median_key = medianKey,
                .schema_index_ready = schemaIndexReady,
            } };
        }

        fn publicLookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return null;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?@import("query.zig").QueryResponse {
            return null;
        }

        fn groupLookup(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8, _: db_mod.types.LookupOptions, consistency: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("documents", table_name);
            try std.testing.expectEqualStrings("doc:a", key);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            return .{ .json = try inner_alloc.dupe(u8, "{\"title\":\"alpha\"}"), .version = 42 };
        }

        fn medianKey(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            return try inner_alloc.dupe(u8, "doc:m");
        }

        fn schemaIndexReady(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: u64, _: u32, _: u32) !bool {
            return true;
        }
    };

    const operations = Operations{ .reads = Fake.reads(), .shard_db_adapter = Fake.shardDb() };
    var lookup = try operations.lookup(alloc, .{}, .{
        .group_id = 7,
        .table_name = "documents",
        .key = "doc:a",
    });
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 42), lookup.version);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", lookup.json);

    const median = (try operations.medianKey(alloc, .{}, 7)).?;
    defer alloc.free(median);
    try std.testing.expectEqualStrings("doc:m", median);
    try std.testing.expectError(
        error.NotFound,
        operations.corruptEmbeddingArtifact(alloc, .{}, "documents", "doc:a", "embedding"),
    );
}
