// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const runnable_abi = @import("antfly_runtime_abi").runnable_abi;
const runnable_fanout = @import("runnable_fanout_owner.zig");
const participant_window = @import("runnable_participant_window.zig");
const platform_time = @import("antfly_platform").time;
const db_mod = @import("antfly_local_sources").storage_db_selected_root.db;
const transactions_mod = @import("antfly_local_sources").storage_transactions;
const tracing = @import("antfly_local_sources").tracing_antfly_trace_writer;
const http_common = @import("../raft/transport/http_common.zig");
const raft_host = @import("../raft/host.zig");
const http_client_mod = @import("http_client.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const table_writes = @import("antfly_local_sources").api_table_write_source;
const contract = @import("antfly_local_sources").api_distributed_txn_contract;
const integrity_wire = @import("antfly_local_sources").api_relational_integrity_wire;
const integrity_activation = @import("antfly_local_sources").storage_db_relational_integrity_activation_contract;
const integrity_retirement = @import("antfly_local_sources").storage_db_relational_integrity_retirement_contract;

pub const table_participant_prefix = @import("antfly_local_sources").api_local_transaction_contract.table_participant_prefix;

const table_participant_v2_prefix = @import("antfly_local_sources").api_local_transaction_contract.table_participant_v2_prefix;

const table_participant_v3_prefix = @import("antfly_local_sources").api_local_transaction_contract.table_participant_v3_prefix;

pub const group_participant_marker = @import("antfly_local_sources").api_local_transaction_contract.group_participant_marker;

pub const preparation_protocol = @import("antfly_local_sources").api_prepared_transaction_protocol;

pub const TxnBeginRequest = struct {
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    topology_epoch: u64 = 0,
    retain_terminal: bool = false,
    participants: []const []const u8,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

test "distributed txn range guard wire preserves absent and exact counters" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const tracking = @import("antfly_local_sources").storage_range_protection;
            var index_id: [tracking.index_id_bytes]u8 = @splat(0);
            index_id[7] = 9;
            const proofs = [_]tracking.Proof{
                .{ .bucket = 0, .generation = null },
                .{ .bucket = 256, .generation = 9007199254740993 },
                .{ .bucket = tracking.index_bucket_sentinel, .generation = 7, .index = .{ .id = index_id, .digest = @splat(0xa5) } },
            };
            const request: TxnPrepareRequest = .{ .txn_id = @splat(1), .topology_epoch = 3, .route_fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 7, .topology_epoch = 3, .route = .{ .group_id = 9, .range_id = 10, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 10 } } }, .req = .{ .range_guards = &proofs } };
            const bytes = try encodeTxnPrepareRequest(alloc, request);
            defer alloc.free(bytes);
            var parsed = try parseTxnPrepareRequest(alloc, bytes);
            defer freeTxnPrepareRequest(alloc, &parsed);
            try std.testing.expectEqualDeep(&proofs, parsed.req.range_guards);
            try std.testing.expectEqualDeep(request.route_fence, parsed.route_fence);
            const prefix = "[\"range-prepare-v1\",";
            try std.testing.expect(std.mem.startsWith(u8, bytes, prefix));
            // Legacy readers reject the outer array; upgraded readers also
            // reject stripping its version marker while retaining guards.
            if (parseTxnPrepareRequest(alloc, bytes[prefix.len .. bytes.len - 1])) |value| {
                var unexpected = value;
                freeTxnPrepareRequest(alloc, &unexpected);
                return error.TestExpectedError;
            } else |err| if (err != error.InvalidTxnRequest) return err;
            const batch_bytes = try @import("antfly_local_sources").api_batch.encodeBatchRequest(alloc, .{ .transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 3 } }, .range_guards = &proofs });
            defer alloc.free(batch_bytes);
            var batch = try @import("antfly_local_sources").api_batch.parseInternalBatchRequest(alloc, batch_bytes);
            defer batch.deinit(alloc);
            try std.testing.expectEqualDeep(&proofs, batch.req.range_guards);
            if (@import("antfly_local_sources").api_batch.parseBatchRequest(alloc, batch_bytes)) |value| {
                var unexpected = value;
                unexpected.deinit(alloc);
                return error.TestExpectedError;
            } else |err| if (err != error.InvalidBatchRequest) return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

pub const TxnPrepareRequest = struct {
    route_fence: ?@import("../metadata/api.zig").CatalogRouteFence = null,
    range_guards_owner: ?std.json.Parsed([]const @import("antfly_local_sources").storage_range_protection.Proof) = null,
    txn_id: db_mod.types.TxnId,
    topology_epoch: u64 = 0,
    req: db_mod.types.TransactionIntentRequest,
    /// Internal parser ownership only; never serialized as a request field.
    integrity_commands_owner: ?std.json.Parsed([]const integrity_wire.Command) = null,
    relational_activation_owner: ?std.json.Parsed(integrity_activation.Command) = null,
    relational_retirement_owner: ?std.json.Parsed(integrity_retirement.Command) = null,
    relational_index_maintenance_owner: ?std.json.Parsed(@import("antfly_local_sources").storage_db_relational_index_maintenance_contract.Command) = null,
};

pub const TxnResolveRequest = struct {
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
    txn_id: db_mod.types.TxnId,
    status: db_mod.types.TxnStatus,
    commit_version: u64,
    /// Non-zero only during the initial commit-resolution pass. Participant
    /// recovery and aborts must remain possible after a topology transition
    /// has already published.
    topology_epoch: u64 = 0,
    sync_level: db_mod.types.SyncLevel = .propose,
};

pub const TxnStatusResponse = struct {
    status: db_mod.types.TxnStatus,
};
pub const TxnStatusRequest = contract.TxnStatusRequest;

pub const TxnAcknowledgeRequest = struct {
    txn_id: db_mod.types.TxnId,
    participant: []const u8,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

pub const TxnAcknowledgeManyRequest = struct {
    txn_id: db_mod.types.TxnId,
    participants: []const []const u8,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

pub fn acknowledgeGroupLocalWithRequest(writes: table_writes.TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest, cancellation: db_mod.types.CancellationToken) !?void {
    try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
    if (req.restore_staging_scope != null) {
        if (req.restore_staging_plan_id == null) return error.InvalidTxnRequest;
        try cancellation.check();
        const result = try writes.batchGroupLocal(alloc, group_id, table_name, .{
            .restore_staging_scope = req.restore_staging_scope,
            .restore_staging_plan_id = req.restore_staging_plan_id,
            .sync_level = .write,
            .transaction = .{ .acknowledge = .{ .txn_id = req.txn_id, .participant = req.participant } },
        });
        try cancellation.check();
        return result;
    }
    try cancellation.check();
    const result = try writes.txnAcknowledgeGroupLocal(alloc, group_id, table_name, req.txn_id, req.participant);
    try cancellation.check();
    return result;
}

fn validateAcknowledgementMany(req: TxnAcknowledgeManyRequest) !void {
    try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
    if (req.restore_staging_scope != null and req.restore_staging_plan_id == null) return error.InvalidTxnRequest;
    if (req.participants.len == 0 or req.participants.len > 64) return error.InvalidTxnRequest;
    for (req.participants) |participant| if (parseParticipantRef(participant) == null) return error.InvalidTxnRequest;
}

pub fn acknowledgeManyGroupLocalWithRequest(writes: table_writes.TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeManyRequest, cancellation: db_mod.types.CancellationToken) !?void {
    try validateAcknowledgementMany(req);
    try cancellation.check();
    const result = try writes.batchGroupLocal(alloc, group_id, table_name, .{
        .restore_staging_scope = req.restore_staging_scope,
        .restore_staging_plan_id = req.restore_staging_plan_id,
        .sync_level = .write,
        .transaction = .{ .acknowledge_many = .{ .txn_id = req.txn_id, .participants = req.participants } },
    });
    try cancellation.check();
    return result;
}

/// Hidden owner resolution retains its authenticated descriptor lookup identity
/// through the canonical batch route. Ordinary transactions keep their existing
/// cancellation-aware participant callback and serving topology checks.
pub fn resolveGroupLocalWithRequest(writes: table_writes.TableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken) !?void {
    try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
    if (req.restore_staging_scope != null) {
        try cancellation.check();
        const result = try writes.batchGroupLocal(alloc, group_id, table_name, .{
            .restore_staging_scope = req.restore_staging_scope,
            .restore_staging_plan_id = req.restore_staging_plan_id,
            .sync_level = req.sync_level,
            .transaction = .{ .resolve = .{ .txn_id = req.txn_id, .status = req.status, .commit_version = req.commit_version } },
        });
        try cancellation.check();
        return result;
    }
    return writes.txnResolveGroupLocalWithCancellation(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, cancellation);
}

pub const TableCommitRequest = @import("antfly_local_sources").api_local_transaction_contract.TableCommitRequest;

pub const CommitConflict = @import("antfly_local_sources").api_local_transaction_contract.CommitConflict;

pub const ParticipantPhase = contract.ParticipantPhase;
pub const CommitOutcome = @import("antfly_local_sources").api_local_transaction_contract.CommitOutcome;

pub const CommitControl = contract.CommitControl;
pub const PreDecisionContext = @import("antfly_local_sources").api_local_transaction_contract.PreDecisionContext;

pub const pre_decision_server_response_reserve_ms = contract.pre_decision_server_response_reserve_ms;

pub const ParticipantWorker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    /// One call-local worker authority. Parallel tasks replace this with their
    /// independent child token; the original coordinator is never shared.
    execution_runnable: ?runnable_abi.Borrowed = null,
    preparation: ?preparation_protocol.Requirement = null,
    preparation_context: PreDecisionContext = .{},

    fn checkExecution(self: ParticipantWorker) !void {
        if (self.execution_runnable) |borrow| try borrow.checkLifetime();
    }

    fn supportsIndependent(self: ParticipantWorker) bool {
        if (self.preparation != null) return self.vtable.required_begin_group != null and self.vtable.required_prepare_group != null and self.vtable.resolve_group_with_execution != null;
        return self.vtable.begin_group_with_execution != null and self.vtable.prepare_group_with_execution != null and
            self.vtable.resolve_group_with_execution != null;
    }

    pub const VTable = struct {
        preparation_capabilities: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, PreDecisionContext, ?runnable_abi.Borrowed) anyerror!preparation_protocol.Capabilities = null,
        required_begin_group: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnBeginRequest, preparation_protocol.Requirement, PreDecisionContext, ?runnable_abi.Borrowed) anyerror!void = null,
        required_prepare_group: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnPrepareRequest, preparation_protocol.Requirement, PreDecisionContext, ?runnable_abi.Borrowed) anyerror!void = null,
        seal_group: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, preparation_protocol.SealRequest, PreDecisionContext, ?runnable_abi.Borrowed) anyerror!preparation_protocol.SealReceipt = null,
        preparation_status_group: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnStatusRequest, u64, ?runnable_abi.Borrowed) anyerror!PreparationDecision = null,
        status_group_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnStatusRequest, ?u64, runnable_abi.Borrowed) anyerror!db_mod.types.TxnStatus = null,
        resolve_group_until_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnResolveRequest, u64, runnable_abi.Borrowed) anyerror!void = null,
        acknowledge_group_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnAcknowledgeRequest, runnable_abi.Borrowed) anyerror!void = null,
        acknowledge_many_group_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnAcknowledgeManyRequest, runnable_abi.Borrowed) anyerror!void = null,
        begin_group_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnBeginRequest, runnable_abi.Borrowed) anyerror!void = null,
        prepare_group_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnPrepareRequest, runnable_abi.Borrowed) anyerror!void = null,
        resolve_group_with_execution: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnResolveRequest, db_mod.types.CancellationToken, runnable_abi.Borrowed) anyerror!void = null,
        status_group_scoped: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) anyerror!db_mod.types.TxnStatus = null,
        begin_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnBeginRequest,
        ) anyerror!void,
        prepare_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnPrepareRequest,
        ) anyerror!void,
        resolve_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnResolveRequest,
        ) anyerror!void,
        status_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!db_mod.types.TxnStatus,
        acknowledge_group: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnAcknowledgeRequest,
        ) anyerror!void = null,
        acknowledge_many_group: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeManyRequest) anyerror!void = null,
        resolve_group_with_cancellation: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnResolveRequest,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!void = null,
        /// Post-decision recovery must not inherit client cancellation, but it
        /// must retain a hard operational bound. The absolute monotonic
        /// deadline is propagated through routing and transport rather than
        /// checked only between potentially blocking attempts.
        resolve_group_until: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnResolveRequest,
            deadline_ns: u64,
        ) anyerror!void = null,
        status_group_until: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
            deadline_ns: u64,
        ) anyerror!db_mod.types.TxnStatus = null,
    };

    pub const PreparationDecision = preparation_protocol.Decision;

    fn forPreparationParticipant(self: ParticipantWorker, participant: *const ParticipantTxn) ParticipantWorker {
        var worker = self;
        if (worker.preparation != null) {
            worker.preparation_context.restore_staging_scope = participant.restore_staging_scope;
            worker.preparation_context.restore_staging_plan_id = participant.restore_staging_plan_id;
            worker.preparation_context.route_fence = participant.route_fence;
        }
        return worker;
    }

    fn preparationContext(self: ParticipantWorker) PreDecisionContext {
        var context = self.preparation_context;
        context.execution_runnable = self.execution_runnable;
        return context;
    }

    fn negotiatePreparation(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8) !void {
        try self.checkExecution();
        const requirement = self.preparation orelse return error.InvalidPreparationRequirement;
        if (self.vtable.required_begin_group == null or self.vtable.required_prepare_group == null or self.vtable.seal_group == null or self.vtable.preparation_status_group == null) return error.PreparedTransactionProtocolUnavailable;
        if (self.execution_runnable != null and (self.vtable.resolve_group_with_execution == null or self.vtable.resolve_group_until_with_execution == null)) return error.PreparedTransactionProtocolUnavailable;
        const callback = self.vtable.preparation_capabilities orelse return error.PreparedTransactionProtocolUnavailable;
        const capabilities = try callback(self.ptr, alloc, group_id, table_name, self.preparationContext(), self.preparationContext().execution_runnable);
        try self.checkExecution();
        try capabilities.require(requirement);
    }

    fn statusForPreparation(self: ParticipantWorker, alloc: std.mem.Allocator, participant: ParticipantTxn, txn_id: db_mod.types.TxnId, deadline: u64) !db_mod.types.TxnStatus {
        const request = participant.statusRequest(txn_id);
        const requirement = self.preparation orelse return self.statusGroupWithRequest(alloc, participant.group_id, participant.table_name, request, deadline);
        try self.checkExecution();
        const callback = self.vtable.preparation_status_group orelse return error.PreparedTransactionProtocolUnavailable;
        const locator = try participantIdForGroupScoped(alloc, participant.table_name, participant.group_id, participant.restore_staging_scope, participant.restore_staging_plan_id);
        defer alloc.free(locator);
        const expected = try preparationRequest(txn_id, requirement, participant, locator, .{ .deadline_ns = deadline });
        const decision = try callback(self.ptr, alloc, participant.group_id, participant.table_name, request, deadline, self.preparationContext().execution_runnable);
        try self.checkExecution();
        return decision.validate(requirement, expected.binding());
    }

    fn sealGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: preparation_protocol.SealRequest) !void {
        try self.checkExecution();
        const callback = self.vtable.seal_group orelse return error.PreparedTransactionProtocolUnavailable;
        const receipt = try callback(self.ptr, alloc, group_id, table_name, request, self.preparationContext(), self.preparationContext().execution_runnable);
        try self.checkExecution();
        try receipt.validate(request);
    }

    pub fn beginGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        try self.checkExecution();
        if (self.preparation) |requirement| {
            const callback = self.vtable.required_begin_group orelse return error.PreparedTransactionProtocolUnavailable;
            return callback(self.ptr, alloc, group_id, table_name, req, requirement, self.preparationContext(), self.preparationContext().execution_runnable);
        }
        if (self.execution_runnable) |borrow| {
            const callback = self.vtable.begin_group_with_execution orelse return error.UnsupportedOperation;
            return callback(self.ptr, alloc, group_id, table_name, req, borrow);
        }

        try self.vtable.begin_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn prepareGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        try self.checkExecution();
        if (self.preparation) |requirement| {
            const callback = self.vtable.required_prepare_group orelse return error.PreparedTransactionProtocolUnavailable;
            return callback(self.ptr, alloc, group_id, table_name, req, requirement, self.preparationContext(), self.preparationContext().execution_runnable);
        }
        if (self.execution_runnable) |borrow| {
            const callback = self.vtable.prepare_group_with_execution orelse return error.UnsupportedOperation;
            return callback(self.ptr, alloc, group_id, table_name, req, borrow);
        }

        try self.vtable.prepare_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn resolveGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        if (self.execution_runnable != null) return self.resolveGroupWithCancellation(alloc, group_id, table_name, req, .none);
        try self.vtable.resolve_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn resolveGroupWithCancellation(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken) !void {
        try self.checkExecution();
        if (self.execution_runnable) |borrow| {
            const callback = self.vtable.resolve_group_with_execution orelse return error.UnsupportedOperation;
            return callback(self.ptr, alloc, group_id, table_name, req, cancellation, borrow);
        }
        const resolve = self.vtable.resolve_group_with_cancellation orelse
            return try self.resolveGroup(alloc, group_id, table_name, req);
        try resolve(self.ptr, alloc, group_id, table_name, req, cancellation);
    }

    pub fn statusGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        try self.checkExecution();
        if (self.execution_runnable != null) return self.statusGroupWithRequest(alloc, group_id, table_name, .{ .txn_id = txn_id }, null);
        return try self.vtable.status_group(self.ptr, alloc, group_id, table_name, txn_id);
    }

    pub fn statusGroupWithRequest(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
        try self.checkExecution();
        try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        if (self.execution_runnable) |borrow| {
            const callback = self.vtable.status_group_with_execution orelse return error.CommitDecisionUnknown;
            return callback(self.ptr, alloc, group_id, table_name, req, deadline_ns, borrow);
        }
        if (req.restore_staging_scope != null) {
            if (req.restore_staging_plan_id == null) return error.InvalidTxnRequest;
            const callback = self.vtable.status_group_scoped orelse return error.CommitDecisionUnknown;
            return callback(self.ptr, alloc, group_id, table_name, req, deadline_ns);
        }
        return if (deadline_ns) |deadline| self.statusGroupUntil(alloc, group_id, table_name, req.txn_id, deadline) else self.statusGroup(alloc, group_id, table_name, req.txn_id);
    }

    pub fn resolveGroupUntil(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
        try self.checkExecution();
        try ensureDecisionRecoveryDeadline(deadline_ns);
        if (self.execution_runnable) |borrow| {
            const callback = self.vtable.resolve_group_until_with_execution orelse return error.CommitDecisionUnknown;
            return callback(self.ptr, alloc, group_id, table_name, req, deadline_ns, borrow);
        }
        // A legacy implementation cannot prove that its routing or transport
        // honors this deadline. Fail closed instead of turning a bounded
        // recovery operation back into an unbounded request.
        const resolve = self.vtable.resolve_group_until orelse return error.CommitDecisionUnknown;
        try resolve(self.ptr, alloc, group_id, table_name, req, deadline_ns);
    }

    pub fn statusGroupUntil(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
        try self.checkExecution();
        try ensureDecisionRecoveryDeadline(deadline_ns);
        if (self.execution_runnable != null) return self.statusGroupWithRequest(alloc, group_id, table_name, .{ .txn_id = txn_id }, deadline_ns);
        const status = self.vtable.status_group_until orelse return error.CommitDecisionUnknown;
        return try status(self.ptr, alloc, group_id, table_name, txn_id, deadline_ns);
    }

    pub fn acknowledgeGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest) !void {
        try self.checkExecution();
        if (self.execution_runnable) |borrow| {
            const callback = self.vtable.acknowledge_group_with_execution orelse return error.UnsupportedOperation;
            return callback(self.ptr, alloc, group_id, table_name, req, borrow);
        }
        const acknowledge = self.vtable.acknowledge_group orelse return;
        try acknowledge(self.ptr, alloc, group_id, table_name, req);
    }
    pub fn acknowledgeManyGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeManyRequest) !void {
        try self.checkExecution();
        if (req.participants.len == 0 or req.participants.len > 64) return error.InvalidTxnRequest;
        if (self.execution_runnable) |borrow| {
            if (self.vtable.acknowledge_many_group_with_execution) |callback| {
                const delivered = delivered: {
                    callback(self.ptr, alloc, group_id, table_name, req, borrow) catch |err| switch (err) {
                        error.UnsupportedOperation, error.UnsupportedRaftBatchProtocolVersion => break :delivered false,
                        else => return err,
                    };
                    break :delivered true;
                };
                if (delivered) return;
            }
        } else if (self.vtable.acknowledge_many_group) |callback| {
            const delivered = delivered: {
                callback(self.ptr, alloc, group_id, table_name, req) catch |err| switch (err) {
                    // Acknowledgements are idempotent. Mixed-version hosts retain
                    // the original per-participant path; uncertain errors retain
                    // recovery debt rather than asserting delivery.
                    error.UnsupportedOperation, error.UnsupportedRaftBatchProtocolVersion => break :delivered false,
                    else => return err,
                };
                break :delivered true;
            };
            if (delivered) return;
        }
        for (req.participants) |participant| try self.acknowledgeGroup(alloc, group_id, table_name, .{ .txn_id = req.txn_id, .participant = participant, .restore_staging_scope = req.restore_staging_scope, .restore_staging_plan_id = req.restore_staging_plan_id });
    }
};

pub const RecoveryResolver = struct {
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    owner_id: []const u8 = "api",
    lease_owned: bool = false,
    interval_ms: u64 = 10,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,
    local_participant: ?[]const u8 = null,

    pub fn config(self: *const RecoveryResolver) db_mod.transaction_runtime.Config {
        return @import("../storage/server_transaction_recovery.zig").configFor(RecoveryResolver, @constCast(self), serverConfig);
    }

    pub fn serverConfig(self: *const RecoveryResolver) @import("../storage/server_transaction_recovery.zig").Config {
        return .{
            .enabled = true,
            .lease_owned = self.lease_owned,
            .owner_id = self.owner_id,
            .interval_ms = self.interval_ms,
            .cutoff_ns = self.cutoff_ns,
            .resolver_ctx = @constCast(self),
            .resolve_participant_fn = resolve,
            .local_participant = self.local_participant,
        };
    }

    fn resolve(
        ctx_ptr: *anyopaque,
        txn_id: db_mod.types.TxnId,
        participant: []const u8,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !void {
        const self: *RecoveryResolver = @ptrCast(@alignCast(ctx_ptr));
        try resolveParticipant(self.alloc, self.worker, participant, txn_id, status, commit_version);
    }
};

pub const HostedParticipantWorker = struct {
    const default_pre_decision_timeout_ms: u32 = 20_000;
    /// The data-Raft write path may spend its complete bounded window finding
    /// a leader before it can return the authenticated `not-proposed` proof.
    /// Keep the outer HTTP deadline strictly later so request admission,
    /// response serialization, and transport cannot erase that proof at the
    /// exact timeout boundary and turn a safe replica retry into an ambiguous
    /// post-send failure.
    const pre_decision_response_reserve_ms: u32 = 1_000;
    const default_pre_decision_attempt_timeout_ms: u32 =
        contract.max_pre_decision_server_budget_ms + pre_decision_response_reserve_ms;

    const PreDecisionAttemptBudget = struct {
        client_timeout_ms: u32,
        server_budget_ms: u32,
    };

    catalog: table_catalog.CatalogSource,
    router: table_router.HostedGroupRouter,
    writes: table_writes.TableWriteSource,
    executor: http_common.RequestExecutor,
    internal_service_secret: ?[]const u8 = null,
    internal_service_issuer: ?[]const u8 = null,
    /// One process-local deadline covers this worker's entire pre-decision
    /// lifetime: routing, all participants, BEGIN, PREPARE and replica retries.
    /// Per-attempt caps leave time for another replica after a black-holed peer.
    pre_decision_context: PreDecisionContext = .{},
    pre_decision_timeout_ms: u32 = default_pre_decision_timeout_ms,
    pre_decision_attempt_timeout_ms: u32 = default_pre_decision_attempt_timeout_ms,
    execution_wait: ?@import("antfly_local_sources").common_execution_wait.Boundary = null,
    execution_deadline_ns: ?u64 = null,

    pub fn init(
        catalog: table_catalog.CatalogSource,
        router: table_router.HostedGroupRouter,
        writes: table_writes.TableWriteSource,
        executor: http_common.RequestExecutor,
    ) HostedParticipantWorker {
        return initWithPreDecisionBudget(catalog, router, writes, executor, .{});
    }

    pub fn initWithPreDecisionBudget(
        catalog: table_catalog.CatalogSource,
        router: table_router.HostedGroupRouter,
        writes: table_writes.TableWriteSource,
        executor: http_common.RequestExecutor,
        budget: @import("antfly_local_sources").api_routing_budget.RoutingBudget,
    ) HostedParticipantWorker {
        // Freeze before catalog routing or any participant contact. Every
        // operation receives this same ceiling; later work cannot reset it.
        const deadline = executor.monotonicNs() +| @as(u64, default_pre_decision_timeout_ms) * std.time.ns_per_ms;
        return .{
            .catalog = catalog,
            .router = router,
            .writes = writes,
            .executor = executor,
            .pre_decision_context = (@as(PreDecisionContext, .{ .deadline_ns = budget.deadline_ns, .deadline_io = budget.io, .cancellation = budget.cancellation })).boundDeadline(deadline, executor.clock_io),
        };
    }

    pub fn withInternalServiceAuth(self: *HostedParticipantWorker, secret: ?[]const u8, issuer: ?[]const u8) *HostedParticipantWorker {
        self.internal_service_secret = secret;
        self.internal_service_issuer = issuer;
        return self;
    }

    fn httpClient(self: *HostedParticipantWorker, alloc: std.mem.Allocator) http_client_mod.ApiHttpClient {
        var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
        client.execution_wait = self.execution_wait;
        _ = client.withInternalServiceAuth(self.internal_service_secret, self.internal_service_issuer);
        return client;
    }

    pub fn worker(self: *HostedParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .vtable = &.{
                .status_group_with_execution = statusGroupWithExecution,
                .resolve_group_until_with_execution = resolveGroupUntilWithExecution,
                .acknowledge_group_with_execution = acknowledgeGroupWithExecution,
                .begin_group_with_execution = beginGroupWithExecution,
                .prepare_group_with_execution = prepareGroupWithExecution,
                .resolve_group_with_execution = resolveGroupWithExecution,
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .resolve_group = resolveGroup,
                .resolve_group_with_cancellation = resolveGroupWithCancellation,
                .resolve_group_until = resolveGroupUntil,
                .status_group = statusGroup,
                .status_group_scoped = statusGroupScoped,
                .status_group_until = statusGroupUntil,
                .acknowledge_group = acknowledgeGroup,
                .acknowledge_many_group = acknowledgeManyGroup,
                .acknowledge_many_group_with_execution = acknowledgeManyGroupWithExecution,
            },
        };
    }

    fn statusGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, maximum: ?u64, borrow: runnable_abi.Borrowed) !db_mod.types.TxnStatus {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        const result = try statusGroupWithin(&view, alloc, group_id, table_name, req, try completionDeadline(borrow, maximum));
        try borrow.checkLifetime();
        return result;
    }

    fn resolveGroupUntilWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, maximum: u64, borrow: runnable_abi.Borrowed) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        view.pre_decision_context.execution_runnable = borrow;
        try resolveGroupWithin(&view, alloc, group_id, table_name, req, .none, try completionDeadline(borrow, maximum));
        try borrow.checkLifetime();
    }

    fn acknowledgeGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        view.execution_deadline_ns = try completionDeadline(borrow, null);
        try acknowledgeGroup(&view, alloc, group_id, table_name, req);
        try borrow.checkLifetime();
    }

    fn acknowledgeManyGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeManyRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        view.execution_deadline_ns = try completionDeadline(borrow, null);
        try acknowledgeManyGroup(&view, alloc, group_id, table_name, req);
        try borrow.checkLifetime();
    }

    fn beginGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        view.pre_decision_context.execution_runnable = borrow;
        try beginGroup(&view, alloc, group_id, table_name, req);
    }

    fn prepareGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        view.pre_decision_context.execution_runnable = borrow;
        try prepareGroup(&view, alloc, group_id, table_name, req);
    }

    fn resolveGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken, borrow: runnable_abi.Borrowed) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        // Sample the destination clock first. The original remaining duration
        // cannot be extended by translating it or beginning another RPC.
        const deadline = try completionDeadline(borrow, null);
        var view = self.*;
        var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
        view.execution_wait = wait.boundary();
        view.pre_decision_context.execution_runnable = borrow;
        try resolveGroupWithin(&view, alloc, group_id, table_name, req, cancellation, deadline);
    }

    fn beginGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        const deadline_ns = self.preDecisionDeadlineNs() catch |err| {
            if (err == error.Timeout) return error.PreDecisionNotProposed;
            return err;
        };
        var route = (table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader) catch |err|
            return preDecisionSetupNotProposed(group_id, "initial-route", err)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        self.checkPreDecisionDeadline(deadline_ns) catch |err| {
            if (err == error.Timeout) return error.PreDecisionNotProposed;
            return err;
        };
        const attempted_node_id = switch (route) {
            .local => self.router.localNodeId(),
            .remote => |remote| remote.node_id,
        };
        switch (route) {
            .local => {
                var context = self.localPreDecisionContext(deadline_ns) catch |err| {
                    if (err == error.Timeout) return error.PreDecisionNotProposed;
                    return err;
                };
                context.restore_staging_scope = req.restore_staging_scope;
                context.restore_staging_plan_id = req.restore_staging_plan_id;
                const result = self.writes.txnBeginGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.retain_terminal, req.participants, context) catch |err| {
                    if (!isLocalPreDecisionCandidateMiss(err, self.writes.vtable.txn_begin_group_local_with_pre_decision_context != null)) return err;
                    return try self.beginGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, null, deadline_ns);
                };
                if (result == null)
                    return try self.beginGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, null, deadline_ns);
            },
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = encodeTxnBeginRequest(alloc, req) catch |err|
                    return preDecisionSetupNotProposed(group_id, "initial-request-encoding", err);
                defer alloc.free(body);
                var delivery_tracker: http_common.RequestDeliveryTracker = .{};
                const budget = self.remainingPreDecisionAttemptBudget(deadline_ns) catch |err| {
                    if (err == error.Timeout) return error.PreDecisionNotProposed;
                    return err;
                };
                const outcome = client.fetchGroupTxnBeginOutcomeWithDeliveryTracking(
                    remote.base_uri,
                    group_id,
                    table_name,
                    body,
                    &delivery_tracker,
                    budget.client_timeout_ms,
                    budget.server_budget_ms,
                ) catch |err| {
                    if (!shouldTryAnotherPreDecisionAttempt(err, &delivery_tracker)) return err;
                    if (!isPreDecisionTransportUnavailable(err))
                        logPreDecisionSetupFailure(group_id, attempted_node_id, "initial-request", err);
                    return try self.beginGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, body, deadline_ns);
                };
                if (outcome != .applied)
                    return try self.beginGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, body, deadline_ns);
            },
        }
    }

    fn prepareGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        const deadline_ns = try self.preDecisionDeadlineNs();
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        try self.checkPreDecisionDeadline(deadline_ns);
        const attempted_node_id = switch (route) {
            .local => self.router.localNodeId(),
            .remote => |remote| remote.node_id,
        };
        switch (route) {
            .local => {
                var context = try self.localPreDecisionContext(deadline_ns);
                context.route_fence = req.route_fence;
                const result = self.writes.txnPrepareGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req, context) catch |err| {
                    std.log.warn("transaction prepare candidate failed group_id={} node_id={} phase=local err={s}", .{ group_id, attempted_node_id, @errorName(err) });
                    if (!isLocalPreDecisionCandidateMiss(err, self.writes.vtable.txn_prepare_group_local_with_pre_decision_context != null)) return err;
                    return try self.prepareGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, null, deadline_ns);
                };
                if (result == null)
                    return try self.prepareGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, null, deadline_ns);
            },
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = try encodeTxnPrepareRequest(alloc, req);
                defer alloc.free(body);
                var delivery_tracker: http_common.RequestDeliveryTracker = .{};
                const budget = try self.remainingPreDecisionAttemptBudget(deadline_ns);
                const outcome = client.fetchGroupTxnPrepareOutcomeWithDeliveryTracking(
                    remote.base_uri,
                    group_id,
                    table_name,
                    body,
                    &delivery_tracker,
                    budget.client_timeout_ms,
                    budget.server_budget_ms,
                ) catch |err| {
                    std.log.warn("transaction prepare candidate failed group_id={} node_id={} phase=remote err={s}", .{ group_id, attempted_node_id, @errorName(err) });
                    if (!shouldTryAnotherPreDecisionAttempt(err, &delivery_tracker)) return err;
                    return try self.prepareGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, body, deadline_ns);
                };
                if (outcome != .applied)
                    return try self.prepareGroupFromCandidates(alloc, group_id, table_name, req, attempted_node_id, body, deadline_ns);
            },
        }
    }

    fn beginGroupFromCandidates(
        self: *HostedParticipantWorker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: TxnBeginRequest,
        attempted_node_id: u64,
        encoded_body: ?[]const u8,
        deadline_ns: u64,
    ) !void {
        self.checkPreDecisionDeadline(deadline_ns) catch |err| {
            if (err == error.Timeout) return error.PreDecisionNotProposed;
            return err;
        };
        const node_ids = (self.router.groupNodeIds(alloc, group_id) catch |err|
            return preDecisionSetupNotProposed(group_id, "candidate-discovery", err)) orelse return error.PreDecisionNotProposed;
        defer alloc.free(node_ids);
        var owned_body: ?[]u8 = null;
        defer if (owned_body) |body| alloc.free(body);
        const body = encoded_body orelse blk: {
            var has_candidate = false;
            for (node_ids) |node_id| {
                if (node_id != attempted_node_id) {
                    has_candidate = true;
                    break;
                }
            }
            if (!has_candidate) return error.PreDecisionNotProposed;
            const value = encodeTxnBeginRequest(alloc, req) catch |err|
                return preDecisionSetupNotProposed(group_id, "candidate-request-encoding", err);
            owned_body = value;
            break :blk value;
        };
        for (node_ids) |node_id| {
            if (node_id == attempted_node_id) continue;
            if (try self.beginGroupAtNode(alloc, group_id, table_name, req, node_id, body, deadline_ns)) return;
        }
        // Every contacted candidate either supplied an authenticated proof or
        // failed before delivery. Preserve that stronger fact for coordinator
        // abort accounting; GroupLeaderUnavailable can also represent an
        // executor error after ambiguous delivery and must remain distinct.
        return error.PreDecisionNotProposed;
    }

    fn beginGroupAtNode(
        self: *HostedParticipantWorker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: TxnBeginRequest,
        node_id: u64,
        body: []const u8,
        deadline_ns: u64,
    ) !bool {
        self.checkPreDecisionDeadline(deadline_ns) catch |err| {
            if (err == error.Timeout) return false;
            return err;
        };
        if (node_id == self.router.localNodeId()) {
            if (self.router.localStatus(group_id) != .active) return false;
            var context = self.localPreDecisionContext(deadline_ns) catch |err| {
                if (err == error.Timeout) return false;
                return err;
            };
            context.restore_staging_scope = req.restore_staging_scope;
            context.restore_staging_plan_id = req.restore_staging_plan_id;
            const result = self.writes.txnBeginGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.retain_terminal, req.participants, context) catch |err| {
                if (isLocalPreDecisionCandidateMiss(err, self.writes.vtable.txn_begin_group_local_with_pre_decision_context != null)) return false;
                return err;
            };
            return result != null;
        }
        if (self.router.nodeStatus(node_id, group_id)) |status| {
            if (status != .active) return false;
        }
        const base_uri = (self.router.nodeBaseUriForGroup(alloc, group_id, node_id) catch |err| {
            logPreDecisionSetupFailure(group_id, node_id, "candidate-route", err);
            return false;
        }) orelse return false;
        defer alloc.free(base_uri);
        var client = self.httpClient(alloc);
        var delivery_tracker: http_common.RequestDeliveryTracker = .{};
        const budget = self.remainingPreDecisionAttemptBudget(deadline_ns) catch |err| {
            if (err == error.Timeout) return false;
            return err;
        };
        const outcome = client.fetchGroupTxnBeginOutcomeWithDeliveryTracking(
            base_uri,
            group_id,
            table_name,
            body,
            &delivery_tracker,
            budget.client_timeout_ms,
            budget.server_budget_ms,
        ) catch |err| {
            if (shouldTryAnotherPreDecisionAttempt(err, &delivery_tracker)) {
                if (!isPreDecisionTransportUnavailable(err))
                    logPreDecisionSetupFailure(group_id, node_id, "candidate-request", err);
                return false;
            }
            return err;
        };
        return outcome == .applied;
    }

    fn prepareGroupFromCandidates(
        self: *HostedParticipantWorker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: TxnPrepareRequest,
        attempted_node_id: u64,
        encoded_body: ?[]const u8,
        deadline_ns: u64,
    ) !void {
        try self.checkPreDecisionDeadline(deadline_ns);
        const node_ids = (try self.router.groupNodeIds(alloc, group_id)) orelse return error.GroupLeaderUnavailable;
        defer alloc.free(node_ids);
        var owned_body: ?[]u8 = null;
        defer if (owned_body) |body| alloc.free(body);
        const body = encoded_body orelse blk: {
            var has_candidate = false;
            for (node_ids) |node_id| {
                if (node_id != attempted_node_id) {
                    has_candidate = true;
                    break;
                }
            }
            if (!has_candidate) return error.GroupLeaderUnavailable;
            const value = try encodeTxnPrepareRequest(alloc, req);
            owned_body = value;
            break :blk value;
        };
        for (node_ids) |node_id| {
            if (node_id == attempted_node_id) continue;
            if (try self.prepareGroupAtNode(alloc, group_id, table_name, req, node_id, body, deadline_ns)) return;
        }
        return error.GroupLeaderUnavailable;
    }

    fn prepareGroupAtNode(
        self: *HostedParticipantWorker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: TxnPrepareRequest,
        node_id: u64,
        body: []const u8,
        deadline_ns: u64,
    ) !bool {
        try self.checkPreDecisionDeadline(deadline_ns);
        if (node_id == self.router.localNodeId()) {
            if (self.router.localStatus(group_id) != .active) return false;
            var context = try self.localPreDecisionContext(deadline_ns);
            context.route_fence = req.route_fence;
            const result = self.writes.txnPrepareGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req, context) catch |err| {
                if (isLocalPreDecisionCandidateMiss(err, self.writes.vtable.txn_prepare_group_local_with_pre_decision_context != null)) return false;
                return err;
            };
            return result != null;
        }
        if (self.router.nodeStatus(node_id, group_id)) |status| {
            if (status != .active) return false;
        }
        const base_uri = (try self.router.nodeBaseUriForGroup(alloc, group_id, node_id)) orelse return false;
        defer alloc.free(base_uri);
        var client = self.httpClient(alloc);
        var delivery_tracker: http_common.RequestDeliveryTracker = .{};
        const budget = try self.remainingPreDecisionAttemptBudget(deadline_ns);
        const outcome = client.fetchGroupTxnPrepareOutcomeWithDeliveryTracking(
            base_uri,
            group_id,
            table_name,
            body,
            &delivery_tracker,
            budget.client_timeout_ms,
            budget.server_budget_ms,
        ) catch |err| {
            if (shouldTryAnotherPreDecisionAttempt(err, &delivery_tracker)) return false;
            return err;
        };
        return outcome == .applied;
    }

    fn preDecisionDeadlineNs(self: *const HostedParticipantWorker) !u64 {
        if (self.pre_decision_timeout_ms == 0 or self.pre_decision_attempt_timeout_ms == 0)
            return error.Timeout;
        const duration_ns = @as(u64, self.pre_decision_timeout_ms) *| std.time.ns_per_ms;
        const context = self.pre_decision_context.boundDeadline(self.executor.monotonicNs() +| duration_ns, self.executor.clock_io);
        const deadline = context.deadline_ns.?;
        try self.checkPreDecisionDeadline(deadline);
        return deadline;
    }

    fn checkPreDecisionDeadline(self: *const HostedParticipantWorker, deadline_ns: u64) !void {
        try self.pre_decision_context.cancellation.check();
        try ensurePreDecisionDeadline(deadline_ns, self.executor.monotonicNs());
    }

    fn remainingPreDecisionAttemptBudget(self: *const HostedParticipantWorker, deadline_ns: u64) !PreDecisionAttemptBudget {
        try self.pre_decision_context.cancellation.check();
        const remaining_ms = try remainingPreDecisionTimeoutMs(deadline_ns, self.executor.monotonicNs());
        const client_timeout_ms = @min(remaining_ms, self.pre_decision_attempt_timeout_ms);
        if (client_timeout_ms <= pre_decision_response_reserve_ms + contract.pre_decision_server_response_reserve_ms)
            return error.Timeout;
        return .{
            .client_timeout_ms = client_timeout_ms,
            .server_budget_ms = @min(
                contract.max_pre_decision_server_budget_ms,
                client_timeout_ms - pre_decision_response_reserve_ms,
            ),
        };
    }

    fn localPreDecisionContext(self: *const HostedParticipantWorker, deadline_ns: u64) !PreDecisionContext {
        const now = self.executor.monotonicNs();
        const budget = try self.remainingPreDecisionAttemptBudget(deadline_ns);
        const duration_ns = @as(u64, budget.server_budget_ms) *| std.time.ns_per_ms;
        return self.pre_decision_context.boundDeadline(@min(deadline_ns, now +| duration_ns), self.executor.clock_io);
    }

    fn resolveGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        try resolveGroupWithCancellation(ptr, alloc, group_id, table_name, req, .none);
    }

    fn resolveGroupWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken) !void {
        try resolveGroupWithin(ptr, alloc, group_id, table_name, req, cancellation, null);
    }

    fn resolveGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
        try resolveGroupWithin(ptr, alloc, group_id, table_name, req, .none, deadline_ns);
    }

    fn resolveGroupWithin(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken, deadline_ns: ?u64) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        var deadline_cancellation = DecisionRecoveryCancellation{ .deadline_ns = deadline_ns orelse 0, .cancellation = cancellation };
        const operation_cancellation = if (deadline_ns != null) deadline_cancellation.token() else cancellation;
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        switch (route) {
            .local => {
                const result = if (self.pre_decision_context.execution_runnable) |borrow|
                    try self.writes.txnResolveGroupLocalWithExecution(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, .{
                        .execution_runnable = borrow,
                        .cancellation = operation_cancellation,
                        .restore_staging_scope = req.restore_staging_scope,
                        .restore_staging_plan_id = req.restore_staging_plan_id,
                    })
                else
                    try resolveGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, operation_cancellation);
                _ = result orelse return error.UnknownGroup;
            },
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = try encodeTxnResolveRequest(alloc, req);
                defer alloc.free(body);
                // The semantic callback remains process-local. Adapt it to the
                // HTTP executor's cancellation contract so disconnecting this
                // RPC also signals the remote request context. If delivery is
                // ambiguous, the coordinator probes the same transaction ID
                // before deciding whether abort is still legal.
                var request_cancellation = http_common.RequestCancellation.fromToken(operation_cancellation);
                var response = if (deadline_ns) |deadline|
                    try client.fetchGroupTxnResolveWithControlAndTimeout(
                        remote.base_uri,
                        group_id,
                        table_name,
                        body,
                        try remainingDeadlineTimeoutMs(deadline),
                        &request_cancellation,
                    )
                else
                    try client.fetchGroupTxnResolveWithControl(
                        remote.base_uri,
                        group_id,
                        table_name,
                        body,
                        if (operation_cancellation.ptr != null) &request_cancellation else null,
                    );
                response.deinit(alloc);
            },
        }
    }

    fn statusGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        return try statusGroupWithin(ptr, alloc, group_id, table_name, .{ .txn_id = txn_id }, null);
    }

    fn statusGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
        return try statusGroupWithin(ptr, alloc, group_id, table_name, .{ .txn_id = txn_id }, deadline_ns);
    }

    fn statusGroupScoped(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
        return statusGroupWithin(ptr, alloc, group_id, table_name, req, deadline_ns);
    }

    fn statusGroupWithin(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
        const txn_id = req.txn_id;
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        return switch (route) {
            .local => if (req.restore_staging_scope != null)
                (try self.writes.txnStatusGroupLocalWithRequest(alloc, group_id, table_name, req, .{ .deadline_ns = deadline_ns })) orelse error.UnknownGroup
            else if (deadline_ns) |deadline|
                (try self.writes.txnStatusGroupAuthoritativeLocalUntil(alloc, group_id, table_name, txn_id, deadline)) orelse error.UnknownGroup
            else
                (try self.writes.txnStatusGroupAuthoritativeLocal(alloc, group_id, table_name, txn_id)) orelse error.UnknownGroup,
            .remote => |remote| blk: {
                var client = self.httpClient(alloc);
                const body = try encodeTxnStatusRequestWithScope(alloc, req);
                defer alloc.free(body);
                var response = if (deadline_ns) |deadline|
                    try client.fetchGroupTxnStatusWithTimeout(
                        remote.base_uri,
                        group_id,
                        table_name,
                        body,
                        try remainingDeadlineTimeoutMs(deadline),
                    )
                else
                    try client.fetchGroupTxnStatus(remote.base_uri, group_id, table_name, body);
                defer response.deinit(alloc);
                const parsed = try parseTxnStatusResponse(alloc, response.body);
                break :blk parsed.status;
            },
        };
    }

    fn acknowledgeManyGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeManyRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        try validateAcknowledgementMany(req);
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        var deadline_control: DecisionRecoveryCancellation = .{ .deadline_ns = self.execution_deadline_ns orelse 0 };
        const cancellation = if (self.execution_deadline_ns != null) deadline_control.token() else db_mod.types.CancellationToken.none;
        try cancellation.check();
        switch (route) {
            .local => _ = (try acknowledgeManyGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, cancellation)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = try encodeTxnAcknowledgeManyRequest(alloc, req);
                defer alloc.free(body);
                var response = if (self.execution_deadline_ns) |deadline|
                    try client.fetchGroupTxnAcknowledgeManyWithTimeout(remote.base_uri, group_id, table_name, body, try remainingDeadlineTimeoutMs(deadline))
                else
                    try client.fetchGroupTxnAcknowledgeMany(remote.base_uri, group_id, table_name, body);
                response.deinit(alloc);
            },
        }
    }

    fn acknowledgeGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        var deadline_control: DecisionRecoveryCancellation = .{ .deadline_ns = self.execution_deadline_ns orelse 0 };
        const cancellation = if (self.execution_deadline_ns != null) deadline_control.token() else db_mod.types.CancellationToken.none;
        try cancellation.check();
        switch (route) {
            .local => _ = (try acknowledgeGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, cancellation)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = try encodeTxnAcknowledgeRequest(alloc, req);
                defer alloc.free(body);
                var response = if (self.execution_deadline_ns) |deadline|
                    try client.fetchGroupTxnAcknowledgeWithTimeout(remote.base_uri, group_id, table_name, body, try remainingDeadlineTimeoutMs(deadline))
                else
                    try client.fetchGroupTxnAcknowledge(remote.base_uri, group_id, table_name, body);
                response.deinit(alloc);
            },
        }
    }
};

fn isPreDecisionLeaderUnavailable(err: anyerror) bool {
    return switch (err) {
        error.GroupLeaderUnavailable,
        error.LeaderUnavailable,
        error.MetadataSnapshotUnavailable,
        => true,
        else => false,
    };
}

fn isPreDecisionCandidateMiss(err: anyerror) bool {
    return isPreDecisionLeaderUnavailable(err) or err == error.UnknownGroup;
}

fn isLocalPreDecisionCandidateMiss(err: anyerror, supports_pre_decision_context: bool) bool {
    if (isPreDecisionCandidateMiss(err)) return true;
    // Only the context-aware callback contract can emit this typed proof from
    // a checked boundary before admitting the mutation. Generic deadline and
    // timeout errors remain ambiguous across rolling-version fallbacks.
    return supports_pre_decision_context and err == error.PreDecisionDeadlineExceeded;
}

fn beginDefinitelyCreatedNoState(err: anyerror) bool {
    return err == error.UnknownGroup or err == error.PreDecisionNotProposed;
}

fn retainedBeginOutcomeUnknown(err: anyerror) bool {
    return err == error.RaftBatchWriteOutcomeUnknown or
        err == error.UnexpectedHttpStatus or
        err == error.ClientShuttingDown or
        isPreDecisionTransportUnavailable(err);
}

fn isPreDecisionTransportUnavailable(err: anyerror) bool {
    return switch (err) {
        error.Timeout,
        error.ConnectionTimeout,
        error.ConnectionTimedOut,
        error.ConnectionFailed,
        error.ConnectionRefused,
        error.ConnectionReset,
        error.ConnectionResetByPeer,
        error.ConnectionClosed,
        error.NetworkUnreachable,
        error.NetworkDown,
        error.HostUnreachable,
        error.DnsResolutionFailed,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.TlsHandshakeFailed,
        error.TlsCertificateError,
        error.TlsError,
        => true,
        else => false,
    };
}

fn shouldTryAnotherPreDecisionAttempt(
    err: anyerror,
    delivery_tracker: *const http_common.RequestDeliveryTracker,
) bool {
    const delivery = delivery_tracker.load();
    // An executor or client that explicitly proves no bytes were sent may
    // safely retry regardless of the local failure class. Unknown delivery is
    // fail-closed except for connection refusal, which occurs before a TCP
    // connection exists and therefore cannot have delivered the request.
    return delivery == .not_sent or
        (delivery == .unknown and err == error.ConnectionRefused);
}

fn preDecisionSetupNotProposed(group_id: u64, stage: []const u8, err: anyerror) error{PreDecisionNotProposed} {
    std.log.warn("transaction begin not proposed after local setup failure group_id={} stage={s} err={s}", .{
        group_id,
        stage,
        @errorName(err),
    });
    return error.PreDecisionNotProposed;
}

fn logPreDecisionSetupFailure(group_id: u64, node_id: u64, stage: []const u8, err: anyerror) void {
    std.log.warn("transaction begin skipped replica after local setup failure group_id={} node_id={} stage={s} err={s}", .{
        group_id,
        node_id,
        stage,
        @errorName(err),
    });
}

fn ensurePreDecisionDeadline(deadline_ns: u64, now_ns: u64) !void {
    if (now_ns >= deadline_ns) return error.Timeout;
}

fn completionDeadline(borrow: runnable_abi.Borrowed, maximum: ?u64) !u64 {
    const before = platform_time.monotonicNs();
    const inherited = before +| try borrow.completionRemainingNs();
    return if (maximum) |limit| @min(inherited, limit) else inherited;
}

fn ensureDecisionRecoveryDeadline(deadline_ns: u64) !void {
    if (platform_time.monotonicNs() >= deadline_ns) return error.CommitDecisionUnknown;
}

const DecisionRecoveryCancellation = struct {
    deadline_ns: u64,
    cancellation: db_mod.types.CancellationToken = .none,

    fn token(self: *const DecisionRecoveryCancellation) db_mod.types.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = isCancelled };
    }

    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *const DecisionRecoveryCancellation = @ptrCast(@alignCast(ptr));
        return self.cancellation.isCancelled() or platform_time.monotonicNs() >= self.deadline_ns;
    }
};

fn remainingDeadlineTimeoutMs(deadline_ns: u64) !u32 {
    const now_ns = platform_time.monotonicNs();
    if (now_ns >= deadline_ns) return error.Timeout;
    const remaining_ns = deadline_ns - now_ns;
    const rounded_ms = @max(@as(u64, 1), std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1);
    return @intCast(@min(rounded_ms, @as(u64, std.math.maxInt(u32))));
}

fn remainingPreDecisionTimeoutMs(deadline_ns: u64, now_ns: u64) !u32 {
    if (now_ns >= deadline_ns) return error.Timeout;
    const remaining_ns = deadline_ns - now_ns;
    return @intCast(@min(std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1, std.math.maxInt(u32)));
}

pub const LocalTableWriteParticipantWorker = struct {
    writes: table_writes.TableWriteSource,
    pre_decision_context: PreDecisionContext = .{},

    pub fn init(writes: table_writes.TableWriteSource) LocalTableWriteParticipantWorker {
        return .{ .writes = writes };
    }

    pub fn worker(self: *LocalTableWriteParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .vtable = &.{
                .status_group_with_execution = statusGroupWithExecution,
                .resolve_group_until_with_execution = resolveGroupUntilWithExecution,
                .acknowledge_group_with_execution = acknowledgeGroupWithExecution,
                .begin_group_with_execution = beginGroupWithExecution,
                .prepare_group_with_execution = prepareGroupWithExecution,
                .resolve_group_with_execution = resolveGroupWithExecution,
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .resolve_group = resolveGroup,
                .resolve_group_with_cancellation = resolveGroupWithCancellation,
                .resolve_group_until = resolveGroupUntil,
                .status_group = statusGroup,
                .status_group_scoped = statusGroupScoped,
                .status_group_until = statusGroupUntil,
                .acknowledge_group = acknowledgeGroup,
            },
        };
    }

    fn statusGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, maximum: ?u64, borrow: runnable_abi.Borrowed) !db_mod.types.TxnStatus {
        const deadline = try completionDeadline(borrow, maximum);
        const result = if (req.restore_staging_scope != null)
            try statusGroupScoped(ptr, alloc, group_id, table_name, req, deadline)
        else
            try statusGroupUntil(ptr, alloc, group_id, table_name, req.txn_id, deadline);
        try borrow.checkLifetime();
        return result;
    }

    fn resolveGroupUntilWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, maximum: u64, borrow: runnable_abi.Borrowed) !void {
        var deadline: DecisionRecoveryCancellation = .{ .deadline_ns = try completionDeadline(borrow, maximum) };
        try resolveGroupWithExecution(ptr, alloc, group_id, table_name, req, deadline.token(), borrow);
        try borrow.checkLifetime();
    }

    fn acknowledgeGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        var control: CompletionCancellation = .{ .borrow = borrow, .foreground = .none };
        _ = (try acknowledgeGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, .{ .ptr = &control, .check_fn = CompletionCancellation.check })) orelse return error.UnknownGroup;
        try borrow.checkLifetime();
    }

    fn beginGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        view.pre_decision_context.execution_runnable = borrow;
        try beginGroup(&view, alloc, group_id, table_name, req);
    }

    fn prepareGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest, borrow: runnable_abi.Borrowed) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        var view = self.*;
        view.pre_decision_context.execution_runnable = borrow;
        try prepareGroup(&view, alloc, group_id, table_name, req);
    }

    const CompletionCancellation = struct {
        borrow: runnable_abi.Borrowed,
        foreground: db_mod.types.CancellationToken,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            try self.borrow.checkLifetime();
            try self.foreground.check();
        }
    };
    fn resolveGroupWithExecution(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken, borrow: runnable_abi.Borrowed) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        var control: CompletionCancellation = .{ .borrow = borrow, .foreground = cancellation };
        try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
        _ = (try self.writes.txnResolveGroupLocalWithExecution(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, .{
            .execution_runnable = borrow,
            .cancellation = .{ .ptr = &control, .check_fn = CompletionCancellation.check },
            .restore_staging_scope = req.restore_staging_scope,
            .restore_staging_plan_id = req.restore_staging_plan_id,
        })) orelse return error.UnknownGroup;
    }

    fn beginGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        var context = self.pre_decision_context;
        context.restore_staging_scope = req.restore_staging_scope;
        context.restore_staging_plan_id = req.restore_staging_plan_id;
        _ = (try self.writes.txnBeginGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.retain_terminal, req.participants, context)) orelse return error.UnknownGroup;
    }

    fn prepareGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        var context = self.pre_decision_context;
        context.route_fence = req.route_fence;
        _ = (try self.writes.txnPrepareGroupLocalWithPreDecisionContext(
            alloc,
            group_id,
            table_name,
            req.txn_id,
            req.topology_epoch,
            req.req,
            context,
        )) orelse return error.UnknownGroup;
    }

    fn resolveGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        try resolveGroupWithCancellation(ptr, alloc, group_id, table_name, req, .none);
    }

    fn resolveGroupWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try resolveGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, cancellation)) orelse return error.UnknownGroup;
    }

    fn resolveGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
        try ensureDecisionRecoveryDeadline(deadline_ns);
        var deadline_cancellation = DecisionRecoveryCancellation{ .deadline_ns = deadline_ns };
        try resolveGroupWithCancellation(ptr, alloc, group_id, table_name, req, deadline_cancellation.token());
    }

    fn statusGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.txnStatusGroupLinearizable(alloc, group_id, table_name, txn_id)) orelse error.UnknownGroup;
    }

    fn statusGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
        try ensureDecisionRecoveryDeadline(deadline_ns);
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.txnStatusGroupLinearizableUntil(
            alloc,
            group_id,
            table_name,
            txn_id,
            deadline_ns,
        )) orelse error.UnknownGroup;
    }

    fn statusGroupScoped(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.txnStatusGroupLocalWithRequest(alloc, group_id, table_name, req, .{ .deadline_ns = deadline_ns })) orelse error.UnknownGroup;
    }

    fn acknowledgeGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try acknowledgeGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, .none)) orelse return error.UnknownGroup;
    }
};

test "distributed txn local participant prepare preserves the guarded catalog route fence" {
    const Probe = struct {
        prepared: bool = false,

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{ .ptr = self, .vtable = &.{
                .batch = batch,
                .txn_prepare_group_local_with_pre_decision_context = prepare,
            } };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return error.TestUnexpectedBatch;
        }

        fn prepare(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            _: []const u8,
            _: db_mod.types.TxnId,
            _: u64,
            req: db_mod.types.TransactionIntentRequest,
            context: PreDecisionContext,
        ) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9), group_id);
            try std.testing.expectEqual(@as(usize, 1), req.range_guards.len);
            try std.testing.expectEqual(@as(u64, 9), context.route_fence.?.route.group_id);
            self.prepared = true;
            return {};
        }
    };

    var probe = Probe{};
    var worker = LocalTableWriteParticipantWorker.init(probe.source());
    const fence: @import("../metadata/api.zig").CatalogRouteFence = .{
        .metadata_group_id = 1,
        .metadata_incarnation = @splat('1'),
        .catalog_revision = 2,
        .table_id = 7,
        .topology_epoch = 3,
        .route = .{ .group_id = 9, .range_id = 10, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 10 } },
    };
    try worker.worker().prepareGroup(std.testing.allocator, 9, "docs", .{
        .txn_id = @splat(1),
        .topology_epoch = 3,
        .route_fence = fence,
        .req = .{ .range_guards = &.{.{ .bucket = 100, .generation = 7 }} },
    });
    try std.testing.expect(probe.prepared);
}

pub const ExecuteResult = contract.ExecuteResult;

pub const ExecuteOptions = struct {
    /// Internal opt-in only. Production entry points must establish migration,
    /// participant readiness and producer support before selecting this policy.
    // Derive once from the original request with forSyncLevel, then retain it
    // independently of later acknowledgement/visibility levels.
    preparation_requirement: ?preparation_protocol.Requirement = null,
    participant_plan_store: ?contract.ParticipantPlanStore = null,
    /// Preserve the terminal coordinator decision for an externally supplied
    /// transaction ID's retry window.
    retain_terminal: bool = false,
    /// Only callers with an externally reusable transaction ID can safely
    /// surface a post-decision error. An ephemeral caller would retry under a
    /// new ID and reapply non-idempotent transforms.
    report_post_commit_failure: bool = true,
    /// Optional process-owned executor for independent participant RPCs. The
    /// coordinator decision remains ordered, while follower admission,
    /// prepare, and phase-two delivery use bounded concurrent windows.
    fanout_io: ?std.Io = null,
    max_parallel_participants: usize = 8,
    /// Borrowed invocation-owned CPU team and bounded child allocator. Reserve
    /// before any participant contact; retain through commit/abort and joins.
    team: ?*@import("antfly_runtime_abi").runnable_abi.Team = null,
    /// Consulted only by participant implementations after a committed
    /// transaction mutation is durable, never during begin or prepare.
    post_commit_cancellation: db_mod.types.CancellationToken = .none,
    /// Original admission authority used by coordinator metadata capture.
    pre_decision_context: PreDecisionContext = .{},
    /// Internal invocation-owned state, populated before participant contact.
    independent_participants: bool = false,
    participant_owners: ?[]runnable_fanout.Owner = null,
    reusable_participants: bool = false,

    fn participantOwner(self: ExecuteOptions, index: usize, start: usize) ?*runnable_fanout.Owner {
        return if (self.participant_owners) |owners| &owners[index - start] else null;
    }

    fn participantAllocator(self: ExecuteOptions) std.mem.Allocator {
        return if (self.team) |team| team.allocator else std.heap.page_allocator;
    }
};

const ParticipantFanoutSlot = struct {
    err: ?anyerror = null,
    acknowledgement_err: ?anyerror = null,
    acknowledgement_ready: bool = false,
    /// Begin failures other than a definite routing miss may have applied
    /// before their response failed and therefore require an abort delivery.
    may_have_transaction_state: bool = false,
    propagation_pending: bool = false,

    fn reset(self: *ParticipantFanoutSlot) void {
        self.* = .{};
    }
};

/// Participant implementations expose the same post-commit visibility state
/// through two boundaries: local workers retain the storage error, while the
/// internal HTTP protocol normalizes retryable visibility to the public
/// transaction error. Keep the semantic classification in one place so a
/// transported 202 cannot be mistaken for failed phase-two delivery.
fn isPostCommitVisibilityError(err: anyerror) bool {
    return switch (err) {
        error.CommitVisibilityNotSatisfied,
        error.EnrichmentWaitCanceled,
        error.EnrichmentWaitTimeout,
        error.EnrichmentRetryInProgress,
        error.EnrichmentWorkerFailed,
        => true,
        else => false,
    };
}

fn isTerminalVisibilityRepair(err: anyerror) bool {
    return err == error.EnrichmentWorkerFailed;
}

fn reserveParticipantTeam(options: *ExecuteOptions, participants: usize) !void {
    if (options.team) |team| {
        if (options.independent_participants) {
            try team.reserveIndependent();
            return;
        }
        const children = if (options.fanout_io != null) @min(@max(options.max_parallel_participants, 1), participants) else 0;
        try team.reserve(children);
        if (team.childCapacity() == 0) options.fanout_io = null;
    }
}

fn fanoutWidth(options: ExecuteOptions, participant_count: usize) usize {
    if (options.fanout_io == null or participant_count <= 1) return 1;
    const requested = @min(@max(options.max_parallel_participants, 1), participant_count);
    if (options.independent_participants) return requested;
    return if (options.team) |team| @min(requested, @max(team.childCapacity(), 1)) else requested;
}

fn reserveFanoutWindow(options: ExecuteOptions, start: usize, end: usize) !?participant_window.Window {
    if (!options.independent_participants) return null;
    const owners = options.participant_owners orelse return error.InvalidRunnableTransition;
    if (end - start > owners.len) return error.InvalidRunnableTransition;
    const team = options.team orelse return error.InvalidRunnableTransition;
    var window = if (options.reusable_participants)
        try participant_window.Window.reuse(team, owners[0 .. end - start])
    else
        try participant_window.Window.reserve(team, owners[0 .. end - start], options.fanout_io orelse return error.InvalidRunnableTransition, 0);
    errdefer window.retire();
    try window.park();
    return window;
}

fn joinFanoutWindow(window: *?participant_window.Window, group: *std.Io.Group, io: std.Io, slots: []ParticipantFanoutSlot, committed: bool) void {
    if (window.*) |*owned| {
        owned.join(group, io) catch |err| {
            for (slots) |*slot| {
                if (slot.err == null) slot.err = err;
                if (committed) slot.propagation_pending = true;
            }
        };
    } else awaitFanout(group, io);
}

fn activateParticipant(worker: ParticipantWorker, owner: ?*runnable_fanout.Owner) !ParticipantWorker {
    var view = worker;
    if (owner) |child| view.execution_runnable = (try child.activate()) orelse return error.InvalidRunnableTransition;
    return view;
}

fn awaitFanout(group: *std.Io.Group, io: std.Io) void {
    // Group.await only reports cancellation after every submitted task has
    // finished. Transaction coordination is not itself cancelable once a
    // participant RPC has started, so consume that signal after joining.
    group.await(io) catch {};
}

pub fn executeCrossGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    table_name: []const u8,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    req: db_mod.types.TransactionIntentRequest,
    trace_writer: ?tracing.AntflyTraceWriter,
) !ExecuteResult {
    if (req.range_guards.len != 0) return error.InvalidTxnRequest;
    const tables = [_]TableCommitRequest{.{
        .table_name = table_name,
        .row_policy_principal_proof = req.row_policy_principal_proof,
        .row_policy_database = req.row_policy_database,
        .row_policy_admitted_at_seconds = req.row_policy_admitted_at_seconds,
        .writes = req.writes,
        .deletes = req.deletes,
        .transforms = req.transforms,
        .predicates = req.predicates,
        .integrity = req.integrity,
        .integrity_commands = req.integrity_commands,
        .relational_activation = req.relational_activation,
        .relational_retirement = req.relational_retirement,
        .relational_index_maintenance = req.relational_index_maintenance,
        .schema_version = req.schema_version,
        .relational_schema_version = req.relational_schema_version,
        .relational_integrity_generation_set = req.relational_integrity_generation_set,
        .restore_staging_scope = req.restore_staging_scope,
        .restore_staging_plan_id = req.restore_staging_plan_id,
        .relational_repair = req.relational_repair,
    }};
    const outcome = try executeMultiTableCommit(
        alloc,
        catalog,
        worker,
        txn_id,
        begin_timestamp,
        commit_version,
        &tables,
        .propose,
        trace_writer,
    );
    return switch (outcome) {
        .committed => |committed| .{ .participant_count = committed.participant_count },
        .conflict => error.IntentConflict,
    };
}

pub fn executeMultiTableCommit(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    tables: []const TableCommitRequest,
    sync_level: db_mod.types.SyncLevel,
    trace_writer: ?tracing.AntflyTraceWriter,
) !CommitOutcome {
    return executeMultiTableCommitWithOptions(
        alloc,
        catalog,
        worker,
        txn_id,
        begin_timestamp,
        commit_version,
        tables,
        sync_level,
        trace_writer,
        .{},
    );
}

pub fn executeMultiTableCommitWithOptions(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    tables: []const TableCommitRequest,
    sync_level: db_mod.types.SyncLevel,
    trace_writer: ?tracing.AntflyTraceWriter,
    options: ExecuteOptions,
) !CommitOutcome {
    return executeMultiTableCommitOnce(alloc, catalog, worker, txn_id, begin_timestamp, commit_version, tables, sync_level, trace_writer, options);
}

const RoutedParticipant = struct {
    table: TableCommitRequest,
    group_id: u64,
    topology_epoch: u64,
    route_fence: ?@import("../metadata/api.zig").CatalogRouteFence = null,
    range_guards: []const @import("antfly_local_sources").storage_range_protection.Proof = &.{},
};

const RoutedPlan = struct {
    version: u32 = 1,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    requirement: ?preparation_protocol.Requirement,
    participants: []const RoutedParticipant,
};

fn parseRoutedPlan(alloc: std.mem.Allocator, bytes: []const u8, txn_id: db_mod.types.TxnId, timestamp: u64, requirement: ?preparation_protocol.Requirement) !std.json.Parsed(RoutedPlan) {
    var parsed = try std.json.parseFromSlice(RoutedPlan, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.version != 1 or !std.mem.eql(u8, &txn_id, &parsed.value.txn_id) or parsed.value.begin_timestamp != timestamp or !std.meta.eql(parsed.value.requirement, requirement)) return error.InvalidTxnRequest;
    for (parsed.value.participants, 0..) |entry, index| {
        if (entry.group_id == 0 or entry.table.table_name.len == 0 or entry.table.range_guards.len != 0) return error.InvalidTxnRequest;
        try validateRestorePlan(entry.table.restore_staging_scope, entry.table.restore_staging_plan_id);
        if (entry.route_fence) |fence| {
            try fence.validate();
            if (fence.route.group_id != entry.group_id or fence.topology_epoch != entry.topology_epoch) return error.InvalidTxnRequest;
        }
        if (requirement != null and entry.route_fence == null) return error.InvalidTxnRequest;
        for (parsed.value.participants[0..index]) |prior| {
            if (prior.group_id == entry.group_id and std.mem.eql(u8, prior.table.table_name, entry.table.table_name)) return error.InvalidTxnRequest;
        }
    }
    return parsed;
}

fn encodeRoutedPlan(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, timestamp: u64, requirement: ?preparation_protocol.Requirement, participants: []const ParticipantTxn) ![]u8 {
    const entries = try alloc.alloc(RoutedParticipant, participants.len);
    defer alloc.free(entries);
    for (participants, entries) |participant, *entry| {
        var table: TableCommitRequest = .{ .table_name = participant.table_name };
        inline for (@typeInfo(TableCommitRequest).@"struct".field_names) |name| {
            if (comptime !std.mem.eql(u8, name, "range_guards")) {
                if (comptime @TypeOf(@field(participant, name)) == @TypeOf(@field(table, name))) {
                    @field(table, name) = @field(participant, name);
                } else @field(table, name) = @field(participant, name).items;
            }
        }
        entry.* = .{ .table = table, .group_id = participant.group_id, .topology_epoch = participant.topology_epoch, .route_fence = participant.route_fence, .range_guards = participant.range_guards.items };
    }
    const Wire = struct {
        plan: RoutedPlan,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            try @import("antfly_local_sources").storage_db_relational_integrity_json.write(self.plan, stream);
        }
    };
    return std.json.Stringify.valueAlloc(alloc, Wire{ .plan = .{ .txn_id = txn_id, .begin_timestamp = timestamp, .requirement = requirement, .participants = entries } }, .{});
}

// CommitOutcome borrows its table/key labels from the caller's sealed request,
// not from the temporary decoded cohort. Keep that lifetime contract on retries.
const OriginalPlanKeys = std.StringHashMapUnmanaged([]const u8);

fn originalPlanKeys(alloc: std.mem.Allocator, tables: []const TableCommitRequest, budget: table_catalog.RoutingBudget) !OriginalPlanKeys {
    var keys: OriginalPlanKeys = .empty;
    errdefer keys.deinit(alloc);
    var index: usize = 0;
    for (tables) |table| {
        inline for (.{ "writes", "predicates", "deletes" }) |field| {
            for (@field(table, field)) |item| {
                try budget.checkpointIndex(index);
                index += 1;
                const key = if (comptime std.mem.eql(u8, field, "deletes")) item else item.key;
                try keys.put(alloc, key, key);
            }
        }
    }
    try budget.checkpoint();
    return keys;
}

fn appendRoutedParticipant(alloc: std.mem.Allocator, participants: *std.ArrayListUnmanaged(ParticipantTxn), entry: RoutedParticipant, tables: []const TableCommitRequest, keys: *const OriginalPlanKeys) !void {
    const name = for (tables) |table| {
        if (std.mem.eql(u8, table.table_name, entry.table.table_name)) break table.table_name;
    } else return error.InvalidTxnRequest;
    var participant: ParticipantTxn = .{ .table_name = name, .group_id = entry.group_id, .topology_epoch = entry.topology_epoch, .route_fence = entry.route_fence };
    errdefer participant.deinit(alloc);
    inline for (@typeInfo(TableCommitRequest).@"struct".field_names) |field| {
        if (comptime !std.mem.eql(u8, field, "range_guards") and !std.mem.eql(u8, field, "table_name")) {
            if (comptime @TypeOf(@field(participant, field)) == @TypeOf(@field(entry.table, field))) {
                @field(participant, field) = @field(entry.table, field);
            } else try @field(participant, field).appendSlice(alloc, @field(entry.table, field));
        }
    }
    try participant.range_guards.appendSlice(alloc, entry.range_guards);
    for (participant.writes.items) |*write| write.key = keys.get(write.key) orelse return error.InvalidTxnRequest;
    for (participant.predicates.items) |*predicate| predicate.key = keys.get(predicate.key) orelse return error.InvalidTxnRequest;
    for (participant.deletes.items) |*key| key.* = keys.get(key.*) orelse return error.InvalidTxnRequest;
    try participants.append(alloc, participant);
}

fn routeCommitParticipants(alloc: std.mem.Allocator, catalog: table_catalog.CatalogSource, worker: ParticipantWorker, tables: []const TableCommitRequest, routing_budget: table_catalog.RoutingBudget, participants: *std.ArrayListUnmanaged(ParticipantTxn)) !void {
    var routing_work_index: usize = 0;
    for (tables) |table| {
        try routing_budget.checkpoint();
        var routing = (try table_catalog.transactionRoutingSnapshotWithBudget(alloc, catalog, table.table_name, routing_budget)) orelse return error.TableNotFound;
        defer routing.deinit(alloc);
        const topology_epoch = routing.topology_epoch;

        try @import("antfly_local_sources").api_range_read_guards.validate(table.range_guards);
        for (table.range_guards) |owner| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const fence = owner.fence;
            if (routing.snapshot.status.metadata_group_id != fence.metadata_group_id or
                !std.meta.eql(routing.snapshot.status.metadata_incarnation, fence.metadata_incarnation) or topology_epoch != fence.topology_epoch)
                return error.TopologyChanged;
            const range = for (routing.ranges) |candidate| {
                if (candidate.group_id == fence.route.group_id) break candidate;
            } else return error.TopologyChanged;
            const manager = @import("../metadata/table_manager.zig");
            if (range.table_id != fence.table_id or
                manager.rangeDocIdentityRangeId(range.*) != fence.route.range_id or
                fence.route.identity_namespace.table_id != range.table_id or
                fence.route.identity_namespace.shard_id != manager.rangeDocIdentityShardId(range.*) or
                fence.route.identity_namespace.range_id != manager.rangeDocIdentityRangeId(range.*)) return error.TopologyChanged;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, fence.route.group_id, topology_epoch);
            participant.route_fence = fence;
            for (owner.proofs) |proof| try participant.range_guards.append(alloc, proof);
        }

        for (table.writes) |write| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const group_id = routing.resolveGroupForKey(write.key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            try participant.writes.append(alloc, write);
        }
        for (table.deletes) |key| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const group_id = routing.resolveGroupForKey(key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            try participant.deletes.append(alloc, key);
        }
        for (table.predicates) |predicate| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const group_id = routing.resolveGroupForKey(predicate.key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            try participant.predicates.append(alloc, predicate);
        }
        for (table.transforms) |transform| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const group_id = routing.resolveGroupForKey(transform.key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            try participant.transforms.append(alloc, transform);
        }
        for (table.integrity) |operation| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const group_id = routing.resolveGroupForKey(operation.routing_key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            try participant.integrity.append(alloc, operation);
        }
        for (table.integrity_commands) |command| {
            try routing_budget.checkpointIndex(routing_work_index);
            routing_work_index += 1;
            const group_id = routing.resolveGroupForKey(&command.address.routing) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            try participant.integrity_commands.append(alloc, command);
        }
        if (table.relational_activation) |command| {
            const group_id = routing.resolveGroupForKey(command.routing_key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            if (participant.relational_activation != null) return error.InvalidTxnRequest;
            participant.relational_activation = command;
        }
        if (table.relational_retirement) |command| {
            const group_id = routing.resolveGroupForKey(command.routing_key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            if (participant.relational_retirement != null) return error.InvalidTxnRequest;
            participant.relational_retirement = command;
        }
        if (table.relational_index_maintenance) |command| {
            const group_id = routing.resolveGroupForKey(command.routing_key) orelse return error.UnknownGroup;
            if (group_id != command.owner_group_id) return error.PreparedGenerationChanged;
            const participant = try ensureParticipantTxn(alloc, participants, table.table_name, group_id, topology_epoch);
            if (participant.relational_index_maintenance != null) return error.InvalidTxnRequest;
            participant.relational_index_maintenance = command;
        }
        for (participants.items) |*participant| {
            if (!std.mem.eql(u8, participant.table_name, table.table_name)) continue;
            if (worker.preparation != null) {
                // Required preparation needs a pinned routing capability; an
                // administrative snapshot cannot manufacture its revision.
                if (catalog.vtable.route_fence == null or catalog.vtable.route_identity == null) return error.PreparedTransactionProtocolUnavailable;
                try routing_budget.checkpoint();
                var routes = try table_catalog.routedGroupsSnapshotUntil(alloc, catalog, table.table_name, &.{participant.group_id}, catalog.deadlineFrom(routing_budget));
                defer routes.deinit(alloc);
                try routing_budget.checkpoint();
                const fence = routes.fenceAt(0);
                if (fence.metadata_group_id != routing.snapshot.status.metadata_group_id or
                    !std.meta.eql(fence.metadata_incarnation, routing.snapshot.status.metadata_incarnation) or fence.topology_epoch != topology_epoch)
                    return error.TopologyChanged;
                const range = for (routing.ranges) |candidate| {
                    if (candidate.group_id == participant.group_id) break candidate;
                } else return error.TopologyChanged;
                const manager = @import("../metadata/table_manager.zig");
                if (range.table_id != fence.table_id or manager.rangeDocIdentityRangeId(range.*) != fence.route.range_id or
                    manager.rangeDocIdentityShardId(range.*) != fence.route.identity_namespace.shard_id) return error.TopologyChanged;
                if (participant.route_fence) |prior| {
                    if (prior.catalog_revision != fence.catalog_revision or !std.meta.eql(prior.route, fence.route)) return error.TopologyChanged;
                }
                participant.route_fence = fence;
            }
            if (participant.row_policy_principal_proof.len != 0 and
                (!std.mem.eql(u8, participant.row_policy_principal_proof, table.row_policy_principal_proof) or
                    !std.mem.eql(u8, participant.row_policy_database, table.row_policy_database) or
                    participant.row_policy_admitted_at_seconds != table.row_policy_admitted_at_seconds))
                return error.InvalidTxnRequest;
            participant.row_policy_principal_proof = table.row_policy_principal_proof;
            participant.row_policy_database = table.row_policy_database;
            participant.row_policy_admitted_at_seconds = table.row_policy_admitted_at_seconds;
            if (participant.relational_schema_version) |existing| {
                if (table.relational_schema_version) |requested| {
                    if (existing != requested) return error.InvalidTxnRequest;
                }
            } else participant.relational_schema_version = table.relational_schema_version;
            if (participant.schema_version) |existing| {
                if (table.schema_version) |requested| if (existing != requested) return error.CatalogGenerationChanged;
            } else participant.schema_version = table.schema_version;
            if (participant.relational_integrity_generation_set) |existing| {
                const requested = table.relational_integrity_generation_set orelse return error.PreparedGenerationChanged;
                if (!std.mem.eql(u8, &existing, &requested)) return error.PreparedGenerationChanged;
            } else participant.relational_integrity_generation_set = table.relational_integrity_generation_set;
            participant.relational_repair = table.relational_repair;
            const routed_restore_scope = (try catalog.restoreScopeForGroup(participant.table_name, participant.group_id)) orelse table.restore_staging_scope;
            if (participant.restore_staging_scope) |existing| {
                const requested = routed_restore_scope orelse return error.RestoreStagingChanged;
                if (!std.mem.eql(u8, &existing, &requested)) return error.RestoreStagingChanged;
            } else participant.restore_staging_scope = routed_restore_scope;
            const routed_restore_plan = (try catalog.restorePlanForGroup(participant.table_name, participant.group_id)) orelse table.restore_staging_plan_id;
            if (participant.restore_staging_plan_id) |existing| {
                const requested = routed_restore_plan orelse return error.RestoreStagingChanged;
                if (!std.mem.eql(u8, &existing, &requested)) return error.RestoreStagingChanged;
            } else participant.restore_staging_plan_id = routed_restore_plan;
            try validateRestorePlan(participant.restore_staging_scope, participant.restore_staging_plan_id);
        }
    }
}

fn executeMultiTableCommitOnce(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    original_worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    tables: []const TableCommitRequest,
    sync_level: db_mod.types.SyncLevel,
    trace_writer: ?tracing.AntflyTraceWriter,
    requested_options: ExecuteOptions,
) !CommitOutcome {
    var options = requested_options;
    var worker = original_worker;
    worker.preparation = options.preparation_requirement;
    worker.preparation_context = options.pre_decision_context;
    worker.preparation_context.execution_runnable = null;
    if (worker.preparation != null) {
        if (worker.execution_runnable == null) worker.execution_runnable = options.pre_decision_context.execution_runnable;
        try worker.checkExecution();
    }
    options.independent_participants = false;
    options.participant_owners = null;
    options.reusable_participants = false;
    var participants = std.ArrayListUnmanaged(ParticipantTxn).empty;
    defer {
        for (participants.items) |*participant| participant.deinit(alloc);
        participants.deinit(alloc);
    }

    const routing_budget: table_catalog.RoutingBudget = .{ .deadline_ns = options.pre_decision_context.deadline_ns, .io = options.pre_decision_context.deadline_io, .cancellation = options.pre_decision_context.cancellation };
    var retained_plan: ?std.json.Parsed(RoutedPlan) = null;
    defer if (retained_plan) |*plan| plan.deinit();
    var plan_store = options.participant_plan_store;
    if (plan_store != null and !options.retain_terminal) return error.InvalidTxnRequest;
    try routing_budget.checkpoint();
    if (plan_store) |store| switch (try store.load(store.ptr, alloc, txn_id)) {
        .legacy => plan_store = null,
        .unplanned => {},
        .planned => |bytes| {
            defer alloc.free(bytes);
            retained_plan = try parseRoutedPlan(alloc, bytes, txn_id, begin_timestamp, worker.preparation);
        },
    };
    if (retained_plan == null) {
        try routeCommitParticipants(alloc, catalog, worker, tables, routing_budget, &participants);
        if (plan_store) |store| {
            const bytes = try encodeRoutedPlan(alloc, txn_id, begin_timestamp, worker.preparation, participants.items);
            defer alloc.free(bytes);
            const selected = try store.seal(store.ptr, alloc, txn_id, bytes);
            defer alloc.free(selected);
            // A concurrent publisher may have won with different routing.
            // Discard our speculative grouping before any participant contact.
            retained_plan = try parseRoutedPlan(alloc, selected, txn_id, begin_timestamp, worker.preparation);
            for (participants.items) |*participant| participant.deinit(alloc);
            participants.clearRetainingCapacity();
        }
    }
    if (retained_plan) |plan| {
        var keys = try originalPlanKeys(alloc, tables, routing_budget);
        defer keys.deinit(alloc);
        for (plan.value.participants, 0..) |entry, index| {
            try routing_budget.checkpointIndex(index);
            try appendRoutedParticipant(alloc, &participants, entry, tables, &keys);
        }
    }

    const participant_ids = try alloc.alloc([]const u8, participants.items.len);
    var participant_ids_initialized: usize = 0;
    defer {
        for (participant_ids[0..participant_ids_initialized]) |participant_id| alloc.free(@constCast(participant_id));
        alloc.free(participant_ids);
    }
    for (participants.items, 0..) |participant, i| {
        participant_ids[i] = try participantIdForGroupScoped(alloc, participant.table_name, participant.group_id, participant.restore_staging_scope, participant.restore_staging_plan_id);
        participant_ids_initialized += 1;
    }

    // Allocate every coordination slot before contacting a participant. No
    // allocator failure may be introduced after the commit decision becomes
    // durable, where returning an ordinary server error would invite an unsafe
    // stateless retry.
    const fanout_slots = try alloc.alloc(ParticipantFanoutSlot, participants.items.len);
    defer alloc.free(fanout_slots);
    for (fanout_slots) |*slot| slot.reset();

    // This is before the first participant RPC and before registering abort
    // obligations. A denied team never falls back to unscheduled child work.
    if (options.team) |team| {
        if (options.fanout_io != null and worker.supportsIndependent() and team.borrow.reserve_child != null and
            try team.borrow.currentClass() != .control)
        {
            _ = try team.borrow.completionRemainingNs();
            options.independent_participants = true;
            const owners = try team.allocator.alloc(runnable_fanout.Owner, @min(@max(options.max_parallel_participants, 1), participants.items.len));
            @memset(owners, .{});
            options.participant_owners = owners;
            options.reusable_participants = true;
        }
    }
    defer if (options.participant_owners) |owners| {
        for (owners) |*owner| {
            std.debug.assert(!owner.running);
            owner.retire();
        }
        options.participantAllocator().free(owners);
    };
    try reserveParticipantTeam(&options, participants.items.len);
    defer if (options.team) |team| team.finish();
    if (options.independent_participants) {
        worker.execution_runnable = options.team.?.borrow;
        // Reserve all reusable packets before coordinator BEGIN. A denied
        // operation/byte/allocator budget cannot create transaction state.
        for (options.participant_owners.?) |*owner| try owner.reserve(options.team.?.borrow, options.fanout_io.?, 0);
    }

    var recovery_budget: @import("decision_recovery_budget.zig").Budget = .{ .now = platform_time.monotonicNs, .maximum_ns = coordinator_resolution_timeout_ns };
    var begun_count: usize = 0;
    var abort_on_error = true;
    var resume_committed = false;
    if (worker.preparation != null or retained_plan != null) {
        // A retained retry must establish the durable contract before asking
        // for fresh admission. Matching committed decisions propagate even if
        // a participant can no longer admit a new required transaction.
        if (options.retain_terminal and participants.items.len != 0) {
            try routing_budget.checkpoint();
            try worker.checkExecution();
            const first = participants.items[0];
            const deadline = try recovery_budget.deadline(if (options.team) |team| team.borrow else null);
            const decision = worker.statusForPreparation(alloc, first, txn_id, deadline) catch |err| switch (err) {
                error.TxnNotFound => null,
                error.PreparationRequirementMismatch, error.PreparationContractConflict => return err,
                else => return error.CommitDecisionUnknown,
            };
            if (decision) |durable| switch (durable) {
                .committed => {
                    resume_committed = true;
                    abort_on_error = false;
                },
                .aborted => return .{ .conflict = participantDecisionConflict(first, .begin) },
                .pending => {},
            };
        }
        if (!resume_committed and worker.preparation != null) for (participants.items) |participant| {
            try routing_budget.checkpoint();
            if (participant.route_fence == null) return error.PreparedTransactionProtocolUnavailable;
            try worker.forPreparationParticipant(&participant).negotiatePreparation(alloc, participant.group_id, participant.table_name);
        };
    }
    errdefer {
        if (abort_on_error) {
            if (trace_writer) |tw| {
                tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
            }
            if (begun_count > 0) abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, if (options.retain_terminal) participants.items.len else begun_count, fanout_slots, options) catch {};
        }
    }

    try routing_budget.checkpoint();
    if (participants.items.len > 0 and !resume_committed) coordinator_begin: {
        const participant = participants.items[0];
        worker.forPreparationParticipant(&participant).beginGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .begin_timestamp = begin_timestamp,
            .topology_epoch = participant.topology_epoch,
            .retain_terminal = options.retain_terminal,
            .participants = participant_ids,
            .restore_staging_scope = participant.restore_staging_scope,
            .restore_staging_plan_id = participant.restore_staging_plan_id,
        }) catch |err| {
            if (options.retain_terminal or err == error.DecisionConflict) {
                // A stable transaction ID may be retried after the coordinator
                // durably committed but before the client observed success.
                // Forwarded Raft apply errors can lose their domain identity;
                // even a failed/not-proposed BEGIN says nothing about an older
                // execution of this ID. Probe the authoritative decision before
                // attempting abort or reporting a terminal conflict.
                // Resume commit-only propagation instead of treating that
                // terminal record as a failed fresh begin.
                const recovery_deadline = recovery_budget.deadline(if (options.team) |team| team.borrow else null) catch {
                    abort_on_error = false;
                    return error.CommitDecisionUnknown;
                };
                const status = worker.statusForPreparation(
                    alloc,
                    participant,
                    txn_id,
                    recovery_deadline,
                ) catch |status_err| switch (status_err) {
                    error.TxnNotFound => null,
                    else => {
                        abort_on_error = false;
                        return error.CommitDecisionUnknown;
                    },
                };
                if (status) |observed| switch (observed) {
                    .committed => {
                        resume_committed = true;
                        abort_on_error = false;
                        break :coordinator_begin;
                    },
                    .aborted => {
                        abort_on_error = false;
                        return .{ .conflict = participantDecisionConflict(participant, .begin) };
                    },
                    .pending => {},
                };
            }
            if (options.retain_terminal and retainedBeginOutcomeUnknown(err)) {
                // BEGIN is idempotent for the same stable ID and participant
                // set. Preserve a pending record and let the session retry;
                // aborting here turns a slow/unknown Raft reply into a
                // permanent 409 on the next commit attempt.
                abort_on_error = false;
                return error.CommitDecisionUnknown;
            }
            if (!options.retain_terminal and (err == error.UnknownGroup or err == error.PreDecisionNotProposed)) {
                abort_on_error = false;
                return .{ .conflict = participantUnavailableConflict(participant, .begin) };
            }
            // The failed call may have applied before its response failed,
            // so include it in abort delivery. A retained ID may also have
            // prepared followers in an earlier execution: only fresh IDs can
            // use this invocation's contact evidence to elide phase two.
            abort_on_error = false;
            try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, if (options.retain_terminal) participants.items.len else 1, fanout_slots, options);
            std.log.warn("transaction begin failed table={s} group_id={} err={s}", .{
                participant.table_name, participant.group_id, @errorName(err),
            });
            return error.TransactionBeginFailed;
        };
        begun_count = 1;
    }

    if (!resume_committed and participants.items.len > 1) {
        runBeginFanout(
            worker,
            txn_id,
            begin_timestamp,
            participants.items,
            participant_ids,
            fanout_slots,
            options,
        );
        if (firstFanoutError(fanout_slots[1..])) |failure_offset| {
            const participant_index = failure_offset + 1;
            const failure = fanout_slots[participant_index].err.?;
            if (options.retain_terminal and retainedBeginOutcomeUnknown(failure)) {
                // Every contacted participant may have persisted BEGIN. A
                // stable-ID retry can safely finish those idempotent begins.
                abort_on_error = false;
                return error.CommitDecisionUnknown;
            }
            if (!beginDefinitelyCreatedNoState(failure)) {
                const participant = participants.items[participant_index];
                std.log.warn("transaction begin failed table={s} group_id={} err={s}", .{
                    participant.table_name, participant.group_id, @errorName(failure),
                });
            }
            abort_on_error = false;
            try abortParticipantsWithContactMask(
                alloc,
                worker,
                txn_id,
                commit_version,
                participants.items,
                participant_ids,
                fanout_slots,
                options.retain_terminal,
                options,
            );
            return switch (failure) {
                error.UnknownGroup, error.PreDecisionNotProposed => .{ .conflict = participantUnavailableConflict(participants.items[participant_index], .begin) },
                else => error.TransactionBeginFailed,
            };
        }
        begun_count = participants.items.len;
    }

    if (!resume_committed) {
        runPrepareFanout(worker, txn_id, participants.items, fanout_slots, options);
        if (firstFanoutError(fanout_slots)) |participant_index| {
            const participant = participants.items[participant_index];
            const err = fanout_slots[participant_index].err.?;
            switch (err) {
                error.IntentConflict, error.VersionConflict, error.UniqueConstraintViolation, error.ForeignKeyParentMissing, error.ForeignKeyReferenced => {
                    if (trace_writer) |tw| {
                        tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
                    }
                    abort_on_error = false;
                    try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                    return .{ .conflict = participantConflict(participant, err) };
                },
                error.UnknownGroup,
                error.RaftBatchWriteOutcomeUnknown,
                error.ClientShuttingDown,
                => {
                    // No commit decision has been attempted. An uncertain
                    // prepare can leave intents, but a confirmed coordinator
                    // abort fences them permanently. Only after that durable
                    // decision may an ephemeral caller start a fresh attempt.
                    // Failure to prove abort still propagates unchanged.
                    abort_on_error = false;
                    try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                    return .{ .conflict = participantUnavailableConflict(participant, .prepare) };
                },
                else => {
                    std.log.warn("transaction prepare failed table={s} group_id={} err={s}", .{
                        participant.table_name, participant.group_id, @errorName(err),
                    });
                    abort_on_error = false;
                    try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                    // The durable abort proves this prepare cannot commit.
                    // Preserve a typed retryable result without claiming
                    // not-proposed for the individual participant RPC. An
                    // uncertain abort above retains AbortDecisionNotDurable.
                    return if (err == error.StorageReadTemporarilyUnavailable)
                        error.TransactionPrepareAbortedUnavailable
                    else
                        err;
                },
            }
        }
    }

    if (!resume_committed and worker.preparation != null) {
        sealPreparedParticipants(alloc, worker, txn_id, participants.items, participant_ids, routing_budget) catch |err| {
            abort_on_error = false;
            try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
            return err;
        };
    }

    // Preparing can overlap metadata publication. Recheck both transition
    // admission and the exact range epoch before the coordinator participant
    // records the irreversible commit decision.
    if (!resume_committed) {
        for (participants.items, 0..) |participant, i| {
            var already_checked = false;
            for (participants.items[0..i]) |prior| {
                if (std.mem.eql(u8, prior.table_name, participant.table_name)) {
                    already_checked = true;
                    break;
                }
            }
            if (already_checked) continue;
            table_catalog.validateTransactionTopologyEpoch(alloc, catalog, participant.table_name, participant.topology_epoch) catch |err| {
                abort_on_error = false;
                try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                return err;
            };
        }
    }

    // Visibility and propagation can both remain pending (for example, when
    // the coordinator's write becomes durable but its visibility barrier
    // fails and a follower was only proposed). Track them independently so a
    // later phase-two outcome cannot hide an earlier recovery obligation.
    var visibility_pending = false;
    var visibility_retry_pending = false;
    var visibility_repair_required = false;
    var propagation_pending = false;
    var propagation_failed = false;
    if (participants.items.len > 0) {
        const participant = participants.items[0];
        const coordinator_sync_level: db_mod.types.SyncLevel = if (sync_level == .propose) .write else sync_level;
        worker.resolveGroupWithCancellation(alloc, participant.group_id, participant.table_name, .{
            .restore_staging_scope = participant.restore_staging_scope,
            .restore_staging_plan_id = participant.restore_staging_plan_id,
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
            // Fence every first-pass resolution against the topology pinned at
            // admission. Recovery deliberately uses epoch zero: a durable
            // commit decision must remain resolvable after a topology change.
            .topology_epoch = if (!resume_committed) participant.topology_epoch else 0,
            // The first participant is the transaction decision record. It must
            // be committed and applied before any other participant can learn a
            // commit decision, even when the public request selected the faster
            // proposal-only acknowledgement level. Later participants retain
            // the caller's requested visibility contract and are recoverable
            // from the durable coordinator decision.
            .sync_level = coordinator_sync_level,
        }, options.post_commit_cancellation) catch |err| switch (err) {
            error.DecisionConflict => {
                if (trace_writer) |tw| {
                    tw.traceEvent(&.{
                        .name = "ResolveDecisionConflict",
                        .txn_id = txn_id,
                        .shard_id = "",
                        .timestamp = commit_version,
                        .reason = "participant decision conflict",
                    });
                }
                abort_on_error = false;
                try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                return .{ .conflict = participantDecisionConflict(participant, .resolve) };
            },
            error.TxnNotFound, error.InvalidTxnRecord => {
                if (trace_writer) |tw| {
                    tw.traceEvent(&.{
                        .name = "ResolveTornTransactionState",
                        .txn_id = txn_id,
                        .shard_id = "",
                        .timestamp = commit_version,
                        .reason = "participant transaction state missing",
                    });
                }
                // The decision participant has no durable transaction record,
                // so no commit decision exists yet.
                abort_on_error = false;
                try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                return .{ .conflict = participantTornStateConflict(participant, .resolve) };
            },
            else => {
                // A resolve can report an error after its local atomic commit
                // (for example while mirroring or waiting for an index). Read
                // the participant record before deciding whether abort is
                // still legal.
                const durable_status = resolveCoordinatorDecisionAfterFailureWithinBudget(
                    alloc,
                    worker,
                    participant,
                    txn_id,
                    commit_version,
                    coordinator_sync_level,
                    err,
                    &recovery_budget,
                    if (options.team) |team| team.borrow else null,
                ) catch |status_err| {
                    // The outcome is uncertain. Recovery will consult the
                    // participant record; aborting here could contradict a
                    // commit that already became durable.
                    abort_on_error = false;
                    std.log.warn("transaction commit decision remains unknown table={s} group_id={} err={s}", .{
                        participant.table_name, participant.group_id, @errorName(status_err),
                    });
                    return error.CommitDecisionUnknown;
                };
                switch (durable_status) {
                    .committed => {
                        abort_on_error = false;
                        // The decision is durable, but the requested sync or
                        // visibility barrier did not complete. Preserve that
                        // failure while finishing best-effort propagation.
                        std.log.warn("transaction commit visibility barrier failed table={s} group_id={} err={s}", .{
                            participant.table_name, participant.group_id, @errorName(err),
                        });
                        visibility_pending = true;
                        visibility_repair_required = err == error.EnrichmentWorkerFailed;
                        visibility_retry_pending = err != error.EnrichmentWorkerFailed;
                    },
                    .pending => {
                        // Once the commit submission may have crossed the
                        // Raft proposal boundary, pending means "not observed
                        // yet", never "safe to abort". The same decision can
                        // be retried idempotently; the opposite decision is
                        // permanently forbidden on this path.
                        abort_on_error = false;
                        return error.CommitDecisionUnknown;
                    },
                    .aborted => {
                        abort_on_error = false;
                        try abortParticipantsWithOptions(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len, fanout_slots, options);
                        return .{ .conflict = participantDecisionConflict(participant, .resolve) };
                    },
                }
            },
        };
        // The first participant is the durable transaction decision. Once it
        // commits, all remaining retries are commit-only and must never enter
        // the abort cleanup path.
        abort_on_error = false;
    }

    runResolveFollowerFanout(
        worker,
        txn_id,
        commit_version,
        participants.items,
        participant_ids,
        sync_level,
        resume_committed,
        fanout_slots,
        options,
    );
    const follower_start: usize = @min(fanout_slots.len, 1);
    for (fanout_slots[follower_start..], follower_start..) |slot, participant_index| {
        if (slot.err) |err| {
            const participant = participants.items[participant_index];
            const visibility_error = isPostCommitVisibilityError(err);
            if (trace_writer) |tw| {
                tw.traceEvent(&.{
                    .name = if (visibility_error)
                        "ResolveVisibilityPending"
                    else if (err == error.DecisionConflict)
                        "ResolveDecisionConflict"
                    else
                        "ResolveParticipantFailure",
                    .txn_id = txn_id,
                    .shard_id = "",
                    .timestamp = commit_version,
                    .reason = @errorName(err),
                });
            }
            if (visibility_error) {
                std.log.warn("transaction participant visibility deferred table={s} group_id={} err={s}", .{
                    participant.table_name,
                    participant.group_id,
                    @errorName(err),
                });
                // Participant resolution reports these outcomes only after
                // its mutation is durable. They are visibility results, not
                // evidence that phase-two delivery failed.
                visibility_pending = true;
                visibility_repair_required = visibility_repair_required or
                    isTerminalVisibilityRepair(err);
                visibility_retry_pending = visibility_retry_pending or
                    !isTerminalVisibilityRepair(err);
            } else {
                std.log.warn("transaction commit propagation failed table={s} group_id={} err={s}", .{
                    participant.table_name,
                    participant.group_id,
                    @errorName(err),
                });
                propagation_failed = true;
            }
        }
        if (slot.acknowledgement_err) |err| {
            const participant = participants.items[participant_index];
            std.log.warn("transaction participant acknowledgement deferred table={s} group_id={} err={s}", .{
                participant.table_name,
                participant.group_id,
                @errorName(err),
            });
        }
        propagation_pending = propagation_pending or slot.propagation_pending;
    }

    if (trace_writer) |tw| {
        tw.traceEvent(&.{ .name = "CommitTransaction", .txn_id = txn_id, .shard_id = "", .timestamp = commit_version });
    }

    var result: ExecuteResult = .{
        .participant_count = participants.items.len,
        .coordinator_group_id = if (participants.items.len > 0) participants.items[0].group_id else null,
        .coordinator_table_name = if (participants.items.len > 0) participants.items[0].table_name else null,
    };
    if (options.report_post_commit_failure) {
        // Recovery and retryable visibility remain live work even when a
        // different participant also needs repair. Report live obligations
        // first so callers do not incorrectly treat the transaction as final.
        // Callers requesting structured outcomes receive every pending flag.
        if (propagation_failed) return error.CommitPropagationIncomplete;
        if (propagation_pending) return error.CommitPropagationIncomplete;
        if (visibility_retry_pending) return error.CommitVisibilityNotSatisfied;
        if (visibility_repair_required) return error.EnrichmentWorkerFailed;
    }
    result.visibility_pending = visibility_pending;
    result.visibility_retry_pending = visibility_retry_pending;
    result.visibility_repair_required = visibility_repair_required;
    result.propagation_pending = propagation_pending;
    if (visibility_pending) {
        std.log.warn("transaction commit acknowledged with deferred visibility txn_id={x}", .{txn_id});
    }
    if (propagation_pending) {
        std.log.warn("transaction commit acknowledged with deferred propagation txn_id={x}", .{txn_id});
    }

    return .{ .committed = result };
}

const coordinator_resolution_timeout_ns: u64 = 5 * std.time.ns_per_s;
const coordinator_resolution_retry_ns: u64 = 25 * std.time.ns_per_ms;

/// Resolve an ambiguous coordinator submission without changing transaction
/// identity. Status is probed after every failed submission; retries are
/// idempotent and occur only on this failure path, keeping the normal commit
/// path at one Raft round trip.
fn resolveCoordinatorDecisionAfterFailure(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: ParticipantTxn,
    txn_id: db_mod.types.TxnId,
    commit_version: u64,
    sync_level: db_mod.types.SyncLevel,
    initial_resolve_error: anyerror,
    cancellation: db_mod.types.CancellationToken,
) !db_mod.types.TxnStatus {
    _ = cancellation;
    var budget: @import("decision_recovery_budget.zig").Budget = .{ .now = platform_time.monotonicNs, .maximum_ns = coordinator_resolution_timeout_ns };
    return resolveCoordinatorDecisionAfterFailureWithinBudget(alloc, worker, participant, txn_id, commit_version, sync_level, initial_resolve_error, &budget, null);
}

fn resolveCoordinatorDecisionAfterFailureWithinBudget(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: ParticipantTxn,
    txn_id: db_mod.types.TxnId,
    commit_version: u64,
    sync_level: db_mod.types.SyncLevel,
    initial_resolve_error: anyerror,
    budget: *@import("decision_recovery_budget.zig").Budget,
    completion: ?@import("antfly_runtime_abi").runnable_abi.Borrowed,
) !db_mod.types.TxnStatus {
    return resolveCoordinatorDecisionAfterFailureChecked(alloc, worker, participant, txn_id, commit_version, sync_level, initial_resolve_error, try budget.deadline(completion), budget, completion);
}

fn resolveCoordinatorDecisionAfterFailureUntil(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: ParticipantTxn,
    txn_id: db_mod.types.TxnId,
    commit_version: u64,
    sync_level: db_mod.types.SyncLevel,
    initial_resolve_error: anyerror,
    deadline_ns: u64,
) !db_mod.types.TxnStatus {
    return resolveCoordinatorDecisionAfterFailureChecked(alloc, worker, participant, txn_id, commit_version, sync_level, initial_resolve_error, deadline_ns, null, null);
}

fn resolveCoordinatorDecisionAfterFailureChecked(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: ParticipantTxn,
    txn_id: db_mod.types.TxnId,
    commit_version: u64,
    sync_level: db_mod.types.SyncLevel,
    initial_resolve_error: anyerror,
    initial_deadline_ns: u64,
    budget: ?*@import("decision_recovery_budget.zig").Budget,
    completion: ?@import("antfly_runtime_abi").runnable_abi.Borrowed,
) !db_mod.types.TxnStatus {
    var deadline_ns = initial_deadline_ns;
    var attempts: usize = 1;
    var last_resolve_error = initial_resolve_error;
    var last_status_error: ?anyerror = null;
    while (true) {
        if (budget) |retained| deadline_ns = try retained.deadline(completion);
        const status: ?db_mod.types.TxnStatus = worker.statusForPreparation(
            alloc,
            participant,
            txn_id,
            deadline_ns,
        ) catch |status_err| status_failure: {
            if (worker.preparation != null) switch (status_err) {
                error.PreparationRequirementMismatch, error.PreparationContractConflict, error.PreparedTransactionProtocolUnavailable => return error.CommitDecisionUnknown,
                else => {},
            };
            last_status_error = status_err;
            break :status_failure null;
        };
        if (status) |observed| switch (observed) {
            .committed, .aborted => return observed,
            .pending => {},
        };
        if (platform_time.monotonicNs() >= deadline_ns) {
            std.log.warn("transaction commit decision recovery exhausted table={s} group_id={} attempts={} resolve_err={s} status_err={s}", .{
                participant.table_name,
                participant.group_id,
                attempts,
                @errorName(last_resolve_error),
                if (last_status_error) |status_err| @errorName(status_err) else "none",
            });
            return error.CommitDecisionUnknown;
        }
        if (budget) |retained| deadline_ns = try retained.deadline(completion);
        attempts += 1;
        // Do not inherit request cancellation after an ambiguous submission:
        // cancellation cannot prove the first commit was not accepted. This
        // bounded recovery loop has its own deadline and repeats only the
        // exact same idempotent decision.
        worker.resolveGroupUntil(alloc, participant.group_id, participant.table_name, .{
            .restore_staging_scope = participant.restore_staging_scope,
            .restore_staging_plan_id = participant.restore_staging_plan_id,
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
            .topology_epoch = participant.topology_epoch,
            .sync_level = sync_level,
        }, deadline_ns) catch |retry_err| {
            last_resolve_error = retry_err;
            const now_ns = platform_time.monotonicNs();
            if (now_ns < deadline_ns) try sleepRecoveryNs(@min(coordinator_resolution_retry_ns, deadline_ns - now_ns), completion);
            continue;
        };
        return .committed;
    }
}

const ParticipantTxn = struct {
    row_policy_principal_proof: []const u8 = "",
    row_policy_database: []const u8 = "",
    row_policy_admitted_at_seconds: i64 = 0,
    route_fence: ?@import("../metadata/api.zig").CatalogRouteFence = null,
    range_guards: std.ArrayListUnmanaged(@import("antfly_local_sources").storage_range_protection.Proof) = .empty,
    schema_version: ?u32 = null,
    table_name: []const u8,
    group_id: u64,
    topology_epoch: u64,
    relational_schema_version: ?u32 = null,
    relational_integrity_generation_set: ?[32]u8 = null,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
    relational_repair: bool = false,
    writes: std.ArrayListUnmanaged(db_mod.types.TransactionWrite) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    transforms: std.ArrayListUnmanaged(db_mod.types.DocumentTransform) = .empty,
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,
    integrity: std.ArrayListUnmanaged(db_mod.types.TransactionIntegrityOperation) = .empty,
    integrity_commands: std.ArrayListUnmanaged(integrity_wire.Command) = .empty,
    relational_activation: ?integrity_activation.Command = null,
    relational_retirement: ?integrity_retirement.Command = null,
    relational_index_maintenance: ?@import("antfly_local_sources").storage_db_relational_index_maintenance_contract.Command = null,

    fn statusRequest(self: ParticipantTxn, txn_id: db_mod.types.TxnId) TxnStatusRequest {
        return .{ .txn_id = txn_id, .restore_staging_scope = self.restore_staging_scope, .restore_staging_plan_id = self.restore_staging_plan_id };
    }

    pub fn deinit(self: *ParticipantTxn, alloc: std.mem.Allocator) void {
        self.range_guards.deinit(alloc);
        self.writes.deinit(alloc);
        self.deletes.deinit(alloc);
        self.transforms.deinit(alloc);
        self.predicates.deinit(alloc);
        self.integrity.deinit(alloc);
        self.integrity_commands.deinit(alloc);
        self.* = undefined;
    }
};

const BeginFanoutTask = struct {
    fn run(
        allocator: std.mem.Allocator,
        original_worker: ParticipantWorker,
        participant: *const ParticipantTxn,
        participant_ids: []const []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        retain_terminal: bool,
        slot: *ParticipantFanoutSlot,
        owner: ?*runnable_fanout.Owner,
    ) void {
        var worker = activateParticipant(original_worker, owner) catch |err| {
            slot.err = err;
            return;
        };
        worker = worker.forPreparationParticipant(participant);
        defer if (owner) |child| child.park();
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        worker.beginGroup(arena.allocator(), participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .begin_timestamp = begin_timestamp,
            .topology_epoch = participant.topology_epoch,
            .retain_terminal = retain_terminal,
            .participants = participant_ids,
            .restore_staging_scope = participant.restore_staging_scope,
            .restore_staging_plan_id = participant.restore_staging_plan_id,
        }) catch |err| {
            slot.err = err;
            slot.may_have_transaction_state = !beginDefinitelyCreatedNoState(err);
            return;
        };
        slot.may_have_transaction_state = true;
    }
};

fn runBeginFanout(
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    slots: []ParticipantFanoutSlot,
    options: ExecuteOptions,
) void {
    std.debug.assert(participants.len == participant_ids.len and participants.len == slots.len);
    for (slots) |*slot| slot.reset();
    if (participants.len <= 1) return;
    const width = fanoutWidth(options, participants.len - 1);
    var start: usize = 1;
    while (start < participants.len) : (start += width) {
        const end = @min(start + width, participants.len);
        var window = reserveFanoutWindow(options, start, end) catch |err| {
            for (slots[start..end]) |*slot| {
                slot.err = err;
            }
            return;
        };
        defer if (window) |*owned| owned.retire();
        if (options.fanout_io) |io| {
            var group: std.Io.Group = .init;
            for (start..end) |i| {
                group.concurrent(io, BeginFanoutTask.run, .{
                    options.participantAllocator(),
                    worker,
                    &participants[i],
                    participant_ids,
                    txn_id,
                    begin_timestamp,
                    options.retain_terminal,
                    &slots[i],
                    options.participantOwner(i, start),
                }) catch BeginFanoutTask.run(
                    options.participantAllocator(),
                    worker,
                    &participants[i],
                    participant_ids,
                    txn_id,
                    begin_timestamp,
                    options.retain_terminal,
                    &slots[i],
                    options.participantOwner(i, start),
                );
            }
            joinFanoutWindow(&window, &group, io, slots[start..end], false);
        } else {
            for (start..end) |i| BeginFanoutTask.run(
                options.participantAllocator(),
                worker,
                &participants[i],
                participant_ids,
                txn_id,
                begin_timestamp,
                options.retain_terminal,
                &slots[i],
                options.participantOwner(i, start),
            );
        }
        if (firstFanoutError(slots[start..end]) != null) break;
    }
}

const PrepareFanoutTask = struct {
    fn run(
        allocator: std.mem.Allocator,
        original_worker: ParticipantWorker,
        participant: *const ParticipantTxn,
        txn_id: db_mod.types.TxnId,
        slot: *ParticipantFanoutSlot,
        owner: ?*runnable_fanout.Owner,
    ) void {
        var worker = activateParticipant(original_worker, owner) catch |err| {
            slot.err = err;
            return;
        };
        worker = worker.forPreparationParticipant(participant);
        defer if (owner) |child| child.park();
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        worker.prepareGroup(arena.allocator(), participant.group_id, participant.table_name, .{
            .route_fence = participant.route_fence,
            .txn_id = txn_id,
            .topology_epoch = participant.topology_epoch,
            .req = .{
                .row_policy_principal_proof = participant.row_policy_principal_proof,
                .row_policy_database = participant.row_policy_database,
                .row_policy_admitted_at_seconds = participant.row_policy_admitted_at_seconds,
                .writes = participant.writes.items,
                .deletes = participant.deletes.items,
                .transforms = participant.transforms.items,
                .predicates = participant.predicates.items,
                .integrity = participant.integrity.items,
                .integrity_commands = participant.integrity_commands.items,
                .range_guards = participant.range_guards.items,
                .relational_activation = participant.relational_activation,
                .relational_retirement = participant.relational_retirement,
                .relational_index_maintenance = participant.relational_index_maintenance,
                .relational_schema_version = participant.relational_schema_version,
                .schema_version = participant.schema_version,
                .relational_integrity_generation_set = participant.relational_integrity_generation_set,
                .restore_staging_scope = participant.restore_staging_scope,
                .restore_staging_plan_id = participant.restore_staging_plan_id,
                .relational_repair = participant.relational_repair,
            },
        }) catch |err| {
            slot.err = err;
        };
    }
};

fn runPrepareFanout(
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    participants: []const ParticipantTxn,
    slots: []ParticipantFanoutSlot,
    options: ExecuteOptions,
) void {
    std.debug.assert(participants.len == slots.len);
    for (slots) |*slot| slot.reset();
    const width = fanoutWidth(options, participants.len);
    var start: usize = 0;
    while (start < participants.len) : (start += width) {
        const end = @min(start + width, participants.len);
        var window = reserveFanoutWindow(options, start, end) catch |err| {
            for (slots[start..end]) |*slot| {
                slot.err = err;
            }
            return;
        };
        defer if (window) |*owned| owned.retire();
        if (options.fanout_io) |io| {
            var group: std.Io.Group = .init;
            for (start..end) |i| {
                group.concurrent(io, PrepareFanoutTask.run, .{ options.participantAllocator(), worker, &participants[i], txn_id, &slots[i], options.participantOwner(i, start) }) catch
                    PrepareFanoutTask.run(options.participantAllocator(), worker, &participants[i], txn_id, &slots[i], options.participantOwner(i, start));
            }
            joinFanoutWindow(&window, &group, io, slots[start..end], false);
        } else {
            for (start..end) |i| PrepareFanoutTask.run(options.participantAllocator(), worker, &participants[i], txn_id, &slots[i], options.participantOwner(i, start));
        }
        if (firstFanoutError(slots[start..end]) != null) break;
    }
}

const ResolveFollowerFanoutTask = struct {
    fn run(
        allocator: std.mem.Allocator,
        original_worker: ParticipantWorker,
        coordinator: *const ParticipantTxn,
        participant: *const ParticipantTxn,
        participant_id: []const u8,
        txn_id: db_mod.types.TxnId,
        commit_version: u64,
        sync_level: db_mod.types.SyncLevel,
        resume_committed: bool,
        cancellation: db_mod.types.CancellationToken,
        slot: *ParticipantFanoutSlot,
        owner: ?*runnable_fanout.Owner,
    ) void {
        const worker = activateParticipant(original_worker, owner) catch |err| {
            slot.err = err;
            slot.propagation_pending = true;
            return;
        };
        defer if (owner) |child| child.park();
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        worker.resolveGroupWithCancellation(arena.allocator(), participant.group_id, participant.table_name, .{
            .restore_staging_scope = participant.restore_staging_scope,
            .restore_staging_plan_id = participant.restore_staging_plan_id,
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
            .topology_epoch = if (resume_committed) 0 else participant.topology_epoch,
            .sync_level = sync_level,
        }, cancellation) catch |err| {
            slot.err = err;
            // These errors are emitted only after the participant mutation is
            // durable; only its requested derived-visibility barrier remains.
            // The participant must still be acknowledged below: visibility
            // debt is not phase-two delivery debt, and leaving the participant
            // enlisted would strand topology transitions after the stable
            // coordinator record is released.
            const post_commit_visibility = isPostCommitVisibilityError(err);
            if (!post_commit_visibility) {
                slot.propagation_pending = true;
                return;
            }
        };
        if (sync_level == .propose) {
            // Proposal acceptance can be lost on leadership change. Retain the
            // coordinator enlistment until recovery delivers at write level.
            slot.propagation_pending = true;
            return;
        }
        slot.acknowledgement_ready = true;
        _ = coordinator;
        _ = participant_id;
    }
};

fn runResolveFollowerFanout(
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    commit_version: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    sync_level: db_mod.types.SyncLevel,
    resume_committed: bool,
    slots: []ParticipantFanoutSlot,
    options: ExecuteOptions,
) void {
    std.debug.assert(participants.len == participant_ids.len and participants.len == slots.len);
    for (slots) |*slot| slot.reset();
    if (participants.len <= 1) return;
    const width = fanoutWidth(options, participants.len - 1);
    var start: usize = 1;
    while (start < participants.len) : (start += width) {
        const end = @min(start + width, participants.len);
        var window = reserveFanoutWindow(options, start, end) catch |err| {
            for (slots[start..]) |*slot| {
                slot.err = err;
                slot.propagation_pending = true;
            }
            return;
        };
        defer if (window) |*owned| owned.retire();
        if (options.fanout_io) |io| {
            var group: std.Io.Group = .init;
            for (start..end) |i| {
                group.concurrent(io, ResolveFollowerFanoutTask.run, .{
                    options.participantAllocator(),
                    worker,
                    &participants[0],
                    &participants[i],
                    participant_ids[i],
                    txn_id,
                    commit_version,
                    sync_level,
                    resume_committed,
                    options.post_commit_cancellation,
                    &slots[i],
                    options.participantOwner(i, start),
                }) catch ResolveFollowerFanoutTask.run(
                    options.participantAllocator(),
                    worker,
                    &participants[0],
                    &participants[i],
                    participant_ids[i],
                    txn_id,
                    commit_version,
                    sync_level,
                    resume_committed,
                    options.post_commit_cancellation,
                    &slots[i],
                    options.participantOwner(i, start),
                );
            }
            joinFanoutWindow(&window, &group, io, slots[start..end], true);
        } else {
            for (start..end) |i| ResolveFollowerFanoutTask.run(
                options.participantAllocator(),
                worker,
                &participants[0],
                &participants[i],
                participant_ids[i],
                txn_id,
                commit_version,
                sync_level,
                resume_committed,
                options.post_commit_cancellation,
                &slots[i],
                options.participantOwner(i, start),
            );
        }
        acknowledgeFollowerWindow(options.participantAllocator(), worker, txn_id, participants, participant_ids, slots, start, end, true);
    }
}

fn preparationRequest(txn_id: db_mod.types.TxnId, requirement: preparation_protocol.Requirement, participant: ParticipantTxn, locator: []const u8, budget: table_catalog.RoutingBudget) !preparation_protocol.SealRequest {
    try budget.checkpoint();
    var hash = std.crypto.hash.Blake3.init(.{});
    var offset: usize = 0;
    while (offset < locator.len) {
        try budget.checkpoint();
        const n = @min(@as(usize, 4096), locator.len - offset);
        hash.update(locator[offset..][0..n]);
        offset += n;
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const fence = participant.route_fence orelse return error.PreparedTransactionProtocolUnavailable;
    try fence.validate();
    if (fence.route.group_id != participant.group_id or fence.topology_epoch != participant.topology_epoch) return error.PreparationContractConflict;
    return .{ .txn_id = txn_id, .group_id = participant.group_id, .topology_epoch = participant.topology_epoch, .participant_digest = digest, .route_digest = preparation_protocol.routeDigest(fence), .requirement = requirement };
}

fn sealPreparedParticipants(alloc: std.mem.Allocator, worker: ParticipantWorker, txn_id: db_mod.types.TxnId, participants: []const ParticipantTxn, participant_ids: []const []const u8, budget: table_catalog.RoutingBudget) !void {
    for (participants, participant_ids) |participant, locator| {
        const request = try preparationRequest(txn_id, worker.preparation.?, participant, locator, budget);
        try worker.forPreparationParticipant(&participant).sealGroup(alloc, participant.group_id, participant.table_name, request);
    }
}

fn firstFanoutError(slots: []const ParticipantFanoutSlot) ?usize {
    for (slots, 0..) |slot, i| if (slot.err != null) return i;
    return null;
}

fn ensureParticipantTxn(
    alloc: std.mem.Allocator,
    grouped: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    group_id: u64,
    topology_epoch: u64,
) !*ParticipantTxn {
    for (grouped.items) |*participant| {
        if (participant.group_id == group_id and std.mem.eql(u8, participant.table_name, table_name)) {
            if (participant.topology_epoch != topology_epoch) return error.TopologyChanged;
            return participant;
        }
    }
    try grouped.append(alloc, .{ .table_name = table_name, .group_id = group_id, .topology_epoch = topology_epoch });
    return &grouped.items[grouped.items.len - 1];
}

pub const participantIdForGroup = @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup;

pub const ParticipantRef = @import("antfly_local_sources").api_local_transaction_contract.ParticipantRef;

pub const participantIdForGroupScoped = @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroupScoped;

pub const parseParticipantRef = @import("antfly_local_sources").api_local_transaction_contract.parseParticipantRef;

pub fn resolveParticipant(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: []const u8,
    txn_id: db_mod.types.TxnId,
    status: db_mod.types.TxnStatus,
    commit_version: u64,
) !void {
    const ref = parseParticipantRef(participant) orelse return error.InvalidParticipant;
    try worker.resolveGroup(alloc, ref.group_id, ref.table_name, .{
        .restore_staging_scope = ref.restore_staging_scope,
        .restore_staging_plan_id = ref.restore_staging_plan_id,
        .txn_id = txn_id,
        .status = status,
        .commit_version = commit_version,
        // Recovery acknowledges this participant immediately after the call.
        // It therefore requires a committed/applied decision, not mere Raft
        // proposal acceptance.
        .sync_level = .write,
    });
}

pub fn encodeTxnIdHex(txn_id: db_mod.types.TxnId) [32]u8 {
    var out: [32]u8 = undefined;
    const hex = "0123456789abcdef";
    for (txn_id, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

pub fn parseTxnIdHex(text: []const u8) !db_mod.types.TxnId {
    if (text.len != 32) return error.InvalidTxnId;
    var out: db_mod.types.TxnId = undefined;
    for (0..16) |i| {
        out[i] = try std.fmt.parseInt(u8, text[i * 2 ..][0..2], 16);
    }
    return out;
}

pub fn encodeTxnBeginRequest(alloc: std.mem.Allocator, req: TxnBeginRequest) ![]u8 {
    try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
    const txn_hex = encodeTxnIdHex(req.txn_id);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"txn_id\":\"");
    try out.appendSlice(alloc, &txn_hex);
    try out.appendSlice(alloc, "\",\"begin_timestamp\":");
    const begin_timestamp = try std.fmt.allocPrint(alloc, "{d}", .{req.begin_timestamp});
    defer alloc.free(begin_timestamp);
    try out.appendSlice(alloc, begin_timestamp);
    try out.appendSlice(alloc, ",\"topology_epoch\":");
    const epoch = try std.fmt.allocPrint(alloc, "{d}", .{req.topology_epoch});
    defer alloc.free(epoch);
    try out.appendSlice(alloc, epoch);
    try out.appendSlice(alloc, ",\"retain_terminal\":");
    try out.appendSlice(alloc, if (req.retain_terminal) "true" else "false");
    try out.appendSlice(alloc, ",\"participants\":[");
    for (req.participants, 0..) |participant, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(participant, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "]");
    try appendRestorePlan(alloc, &out, req.restore_staging_plan_id);
    if (req.restore_staging_scope) |scope| {
        try out.appendSlice(alloc, ",\"restore_staging_scope\":");
        const encoded = try integrity_wire.encodeGenerationSet(alloc, scope);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "}");
    return try out.toOwnedSlice(alloc);
}

pub fn encodeTxnPrepareRequest(alloc: std.mem.Allocator, req: TxnPrepareRequest) ![]u8 {
    try validateRestorePlan(req.req.restore_staging_scope, req.req.restore_staging_plan_id);
    const txn_hex = encodeTxnIdHex(req.txn_id);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const guarded = req.req.range_guards.len != 0;
    const row_semantics = @import("antfly_local_sources").api_batch.requiresRowSemanticsEnvelope(req.req.writes, req.req.predicates);
    if (row_semantics) try out.appendSlice(alloc, "[\"row-semantics-prepare-v1\",") else if (guarded) try out.appendSlice(alloc, "[\"range-prepare-v1\",");
    try out.appendSlice(alloc, "{\"txn_id\":\"");
    try out.appendSlice(alloc, &txn_hex);
    try out.appendSlice(alloc, "\",\"topology_epoch\":");
    const epoch = try std.fmt.allocPrint(alloc, "{d}", .{req.topology_epoch});
    defer alloc.free(epoch);
    try out.appendSlice(alloc, epoch);
    if (req.route_fence) |fence| {
        const bytes = try std.json.Stringify.valueAlloc(alloc, fence, .{});
        defer alloc.free(bytes);
        try out.appendSlice(alloc, ",\"route_fence\":");
        try out.appendSlice(alloc, bytes);
    }
    try out.appendSlice(alloc, ",\"writes\":[");
    for (req.req.writes, 0..) |write, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"key\":{f},\"value\":{s}",
            .{ std.json.fmt(write.key, .{}), write.value },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
        if (write.json_null_fields.len != 0) {
            const fields = try std.json.Stringify.valueAlloc(alloc, write.json_null_fields, .{});
            defer alloc.free(fields);
            try out.appendSlice(alloc, ",\"json_null_fields\":");
            try out.appendSlice(alloc, fields);
        }
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"deletes\":[");
    for (req.req.deletes, 0..) |key, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"transforms\":[");
    for (req.req.transforms, 0..) |transform, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(transform.key, .{})});
        defer alloc.free(encoded_key);
        try out.appendSlice(alloc, "{\"key\":");
        try out.appendSlice(alloc, encoded_key);
        try out.appendSlice(alloc, ",\"operations\":[");
        for (transform.operations, 0..) |op, op_index| {
            if (op_index > 0) try out.append(alloc, ',');
            const encoded_op = try std.fmt.allocPrint(
                alloc,
                "{{\"op\":{f},\"path\":{f}",
                .{ std.json.fmt(db_mod.types.transformOpText(op.op), .{}), std.json.fmt(op.path, .{}) },
            );
            defer alloc.free(encoded_op);
            try out.appendSlice(alloc, encoded_op);
            if (op.value_json) |value_json| {
                try out.appendSlice(alloc, ",\"value\":");
                try out.appendSlice(alloc, value_json);
            }
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
        if (transform.upsert) try out.appendSlice(alloc, ",\"upsert\":true");
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"predicates\":[");
    for (req.req.predicates, 0..) |predicate, i| {
        if (i > 0) try out.append(alloc, ',');
        var digest_hex: [64]u8 = undefined;
        if (predicate.expected_content_digest) |digest| digest_hex = std.fmt.bytesToHex(digest, .lower);
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{
            .key = predicate.key,
            .expected_version = predicate.expected_version,
            .unique_absence = if (predicate.unique_absence) @as(?bool, true) else null,
            .expected_content_digest = if (predicate.expected_content_digest != null) @as(?[]const u8, &digest_hex) else null,
        }, .{ .emit_null_optional_fields = false });
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"integrity\":");
    try integrity_wire.append(alloc, &out, req.req.integrity);
    try out.appendSlice(alloc, ",\"integrity_commands\":");
    try integrity_wire.appendCommands(alloc, &out, req.req.integrity_commands);
    try out.appendSlice(alloc, ",\"range_guards\":");
    const range_json = try std.json.Stringify.valueAlloc(alloc, req.req.range_guards, .{});
    defer alloc.free(range_json);
    try out.appendSlice(alloc, range_json);
    if (req.req.relational_activation) |activation| {
        const encoded = try std.json.Stringify.valueAlloc(alloc, activation, .{});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, ",\"relational_activation\":");
        try out.appendSlice(alloc, encoded);
    }
    if (req.req.relational_retirement) |retirement| {
        const encoded = try std.json.Stringify.valueAlloc(alloc, retirement, .{});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, ",\"relational_retirement\":");
        try out.appendSlice(alloc, encoded);
    }
    if (req.req.relational_index_maintenance) |retirement| {
        const encoded = try std.json.Stringify.valueAlloc(alloc, retirement, .{});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, ",\"relational_index_maintenance\":");
        try out.appendSlice(alloc, encoded);
    }
    if (req.req.relational_schema_version) |version| {
        const field = try std.fmt.allocPrint(alloc, ",\"relational_schema_version\":{d}", .{version});
        defer alloc.free(field);
        try out.appendSlice(alloc, field);
    }
    if (req.req.schema_version) |version| try out.print(alloc, ",\"schema_version\":{d}", .{version});
    if (req.req.relational_repair) try out.appendSlice(alloc, ",\"relational_repair\":true");
    try appendRestorePlan(alloc, &out, req.req.restore_staging_plan_id);
    if (req.req.restore_staging_scope) |scope| {
        try out.appendSlice(alloc, ",\"restore_staging_scope\":");
        const encoded = try integrity_wire.encodeGenerationSet(alloc, scope);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (req.req.relational_integrity_generation_set) |generation_set| {
        try out.appendSlice(alloc, ",\"relational_integrity_generation_set\":");
        const encoded = try integrity_wire.encodeGenerationSet(alloc, generation_set);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    if (guarded or row_semantics) try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeTxnResolveRequest(alloc: std.mem.Allocator, req: TxnResolveRequest) ![]u8 {
    try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
    const txn_hex = encodeTxnIdHex(req.txn_id);
    const status_text = switch (req.status) {
        .pending => "pending",
        .committed => "committed",
        .aborted => "aborted",
    };
    const base = try std.fmt.allocPrint(
        alloc,
        "{{\"txn_id\":\"{s}\",\"status\":\"{s}\",\"commit_version\":{d},\"topology_epoch\":{d},\"sync_level\":\"{s}\"",
        .{ &txn_hex, status_text, req.commit_version, req.topology_epoch, @tagName(req.sync_level) },
    );
    defer alloc.free(base);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, base);
    try appendRestorePlan(alloc, &out, req.restore_staging_plan_id);
    if (req.restore_staging_scope) |scope| {
        try out.appendSlice(alloc, ",\"restore_staging_scope\":");
        const encoded = try integrity_wire.encodeGenerationSet(alloc, scope);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

const validateRestorePlan = @import("antfly_local_sources").api_local_transaction_contract.validateRestorePlan;

fn appendRestorePlan(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), plan_id: ?[16]u8) !void {
    if (plan_id) |id| {
        try out.appendSlice(alloc, ",\"restore_staging_plan_id\":\"");
        try out.appendSlice(alloc, &encodeTxnIdHex(id));
        try out.append(alloc, '"');
    }
}

fn parseRestorePlan(obj: std.json.ObjectMap, scope: ?[32]u8) !?[16]u8 {
    const value = obj.get("restore_staging_plan_id") orelse return null;
    const id = try parseTxnIdHex(switch (value) {
        .string => |text| text,
        else => return error.InvalidTxnRequest,
    });
    try validateRestorePlan(scope, id);
    return id;
}

pub fn encodeTxnStatusRequest(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    return encodeTxnStatusRequestWithScope(alloc, .{ .txn_id = txn_id });
}

fn appendRestoreAuthority(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), scope: ?[32]u8, plan_id: ?[16]u8) !void {
    try validateRestorePlan(scope, plan_id);
    if ((scope == null) != (plan_id == null)) return error.InvalidTxnRequest;
    try appendRestorePlan(alloc, out, plan_id);
    if (scope) |digest| {
        try out.appendSlice(alloc, ",\"restore_staging_scope\":");
        const encoded = try integrity_wire.encodeGenerationSet(alloc, digest);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
}

pub fn encodeTxnStatusRequestWithScope(alloc: std.mem.Allocator, req: TxnStatusRequest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"txn_id\":\"");
    try out.appendSlice(alloc, &encodeTxnIdHex(req.txn_id));
    try out.append(alloc, '"');
    try appendRestoreAuthority(alloc, &out, req.restore_staging_scope, req.restore_staging_plan_id);
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

pub fn encodeTxnAcknowledgeRequest(alloc: std.mem.Allocator, req: TxnAcknowledgeRequest) ![]u8 {
    const txn_hex = encodeTxnIdHex(req.txn_id);
    const base = try std.fmt.allocPrint(
        alloc,
        "{{\"txn_id\":\"{s}\",\"participant\":{f}",
        .{ &txn_hex, std.json.fmt(req.participant, .{}) },
    );
    defer alloc.free(base);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, base);
    try appendRestoreAuthority(alloc, &out, req.restore_staging_scope, req.restore_staging_plan_id);
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

pub fn encodeTxnAcknowledgeManyRequest(alloc: std.mem.Allocator, req: TxnAcknowledgeManyRequest) ![]u8 {
    try validateAcknowledgementMany(req);
    const txn_hex = encodeTxnIdHex(req.txn_id);
    const base = try std.fmt.allocPrint(alloc, "{{\"txn_id\":\"{s}\",\"participants\":{f}", .{ &txn_hex, std.json.fmt(req.participants, .{}) });
    defer alloc.free(base);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, base);
    try appendRestoreAuthority(alloc, &out, req.restore_staging_scope, req.restore_staging_plan_id);
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

pub fn encodeTxnStatusResponse(alloc: std.mem.Allocator, response: TxnStatusResponse) ![]u8 {
    const status_text = switch (response.status) {
        .pending => "pending",
        .committed => "committed",
        .aborted => "aborted",
    };
    return try std.fmt.allocPrint(alloc, "{{\"status\":\"{s}\"}}", .{status_text});
}

pub fn parseTxnBeginRequest(alloc: std.mem.Allocator, body: []const u8) !TxnBeginRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const txn_id = try parseTxnIdHex(requireString(obj, "txn_id"));
    const begin_timestamp = try requireU64(obj, "begin_timestamp");
    const participants_value = obj.get("participants") orelse return error.InvalidTxnRequest;
    const participants = switch (participants_value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    const out = try alloc.alloc([]const u8, participants.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |participant| alloc.free(@constCast(participant));
        if (out.len > 0) alloc.free(out);
    }
    for (participants.items, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, switch (item) {
            .string => |s| s,
            else => return error.InvalidTxnRequest,
        });
        initialized += 1;
    }
    const restore_scope = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    return .{
        .txn_id = txn_id,
        .begin_timestamp = begin_timestamp,
        .restore_staging_scope = restore_scope,
        .restore_staging_plan_id = try parseRestorePlan(obj, restore_scope),
        .topology_epoch = try optionalU64(obj, "topology_epoch"),
        .retain_terminal = if (obj.get("retain_terminal")) |value| switch (value) {
            .bool => |flag| flag,
            else => return error.InvalidTxnRequest,
        } else false,
        .participants = out,
    };
}

pub fn freeTxnBeginRequest(alloc: std.mem.Allocator, req: *TxnBeginRequest) void {
    for (req.participants) |participant| alloc.free(@constCast(participant));
    if (req.participants.len > 0) alloc.free(req.participants);
    req.* = undefined;
}

pub fn parseTxnPrepareRequest(alloc: std.mem.Allocator, body: []const u8) !TxnPrepareRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    const wrapped = parsed.value == .array;
    var row_semantics = false;
    const root = if (wrapped) blk: {
        const entries = parsed.value.array.items;
        if (entries.len != 2 or entries[0] != .string) return error.InvalidTxnRequest;
        row_semantics = std.mem.eql(u8, entries[0].string, "row-semantics-prepare-v1");
        if (!row_semantics and !std.mem.eql(u8, entries[0].string, "range-prepare-v1")) return error.InvalidTxnRequest;
        break :blk entries[1];
    } else parsed.value;
    const obj = switch (root) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const txn_id = try parseTxnIdHex(requireString(obj, "txn_id"));
    const writes = try parseTxnWrites(alloc, obj.get("writes") orelse return error.InvalidTxnRequest);
    errdefer freeTxnWrites(alloc, writes);
    const deletes = try parseTxnDeletes(alloc, obj.get("deletes") orelse return error.InvalidTxnRequest);
    errdefer freeTxnDeletes(alloc, deletes);
    const transforms = try parseTxnTransforms(alloc, obj.get("transforms") orelse return error.InvalidTxnRequest);
    errdefer freeTxnTransforms(alloc, transforms);
    const predicates = try parseTxnPredicates(alloc, obj.get("predicates") orelse return error.InvalidTxnRequest);
    errdefer freeTxnPredicates(alloc, predicates);
    const integrity = if (obj.get("integrity")) |value| try integrity_wire.parse(alloc, value) else &.{};
    errdefer integrity_wire.free(alloc, integrity);
    var integrity_commands_owner = if (obj.get("integrity_commands")) |value| try integrity_wire.parseCommands(alloc, value) else null;
    errdefer if (integrity_commands_owner) |*owner| owner.deinit();
    var range_guards_owner = if (obj.get("range_guards")) |value| try std.json.parseFromValue([]const @import("antfly_local_sources").storage_range_protection.Proof, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (range_guards_owner) |*owner| owner.deinit();
    if (range_guards_owner) |owner| if (owner.value.len > @import("antfly_local_sources").api_range_read_guards.max_proofs) return error.InvalidTxnRequest;
    var relational_activation_owner = if (obj.get("relational_activation")) |value| try std.json.parseFromValue(integrity_activation.Command, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (relational_activation_owner) |*owner| owner.deinit();
    var relational_retirement_owner = if (obj.get("relational_retirement")) |value| try std.json.parseFromValue(integrity_retirement.Command, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (relational_retirement_owner) |*owner| owner.deinit();
    var relational_index_maintenance_owner = if (obj.get("relational_index_maintenance")) |value| try std.json.parseFromValue(@import("antfly_local_sources").storage_db_relational_index_maintenance_contract.Command, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (relational_index_maintenance_owner) |*owner| owner.deinit();
    const relational_schema_version: ?u32 = if (obj.get("relational_schema_version")) |_| blk: {
        const version = try optionalU64(obj, "relational_schema_version");
        if (version > std.math.maxInt(u32)) return error.InvalidTxnRequest;
        break :blk @intCast(version);
    } else null;
    const schema_version: ?u32 = if (obj.get("schema_version")) |_| blk: {
        const version = try optionalU64(obj, "schema_version");
        break :blk std.math.cast(u32, version) orelse return error.InvalidTxnRequest;
    } else null;
    const generation_set: ?[32]u8 = if (obj.get("relational_integrity_generation_set")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const restore_staging_scope: ?[32]u8 = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const relational_repair = if (obj.get("relational_repair")) |value| switch (value) {
        .bool => |flag| flag,
        else => return error.InvalidTxnRequest,
    } else false;
    var route_fence = if (obj.get("route_fence")) |value| try std.json.parseFromValue(@import("../metadata/api.zig").CatalogRouteFence, alloc, value, .{}) else null;
    defer if (route_fence) |*fence| fence.deinit();
    if (range_guards_owner != null and range_guards_owner.?.value.len != 0 and route_fence == null) return error.InvalidTxnRequest;
    const has_guards = range_guards_owner != null and range_guards_owner.?.value.len != 0;
    if (has_guards and !wrapped) return error.InvalidTxnRequest;
    if (wrapped and !row_semantics and !has_guards) return error.InvalidTxnRequest;
    if (@import("antfly_local_sources").api_batch.requiresRowSemanticsEnvelope(writes, predicates) and !row_semantics) return error.InvalidTxnRequest;
    return .{
        .route_fence = if (route_fence) |fence| fence.value else null,
        .txn_id = txn_id,
        .topology_epoch = try optionalU64(obj, "topology_epoch"),
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .predicates = predicates,
            .integrity = integrity,
            .integrity_commands = if (integrity_commands_owner) |owner| owner.value else &.{},
            .range_guards = if (range_guards_owner) |owner| owner.value else &.{},
            .relational_activation = if (relational_activation_owner) |owner| owner.value else null,
            .relational_retirement = if (relational_retirement_owner) |owner| owner.value else null,
            .relational_index_maintenance = if (relational_index_maintenance_owner) |owner| owner.value else null,
            .relational_schema_version = relational_schema_version,
            .schema_version = schema_version,
            .relational_integrity_generation_set = generation_set,
            .restore_staging_scope = restore_staging_scope,
            .restore_staging_plan_id = try parseRestorePlan(obj, restore_staging_scope),
            .relational_repair = relational_repair,
        },
        .integrity_commands_owner = integrity_commands_owner,
        .range_guards_owner = range_guards_owner,
        .relational_activation_owner = relational_activation_owner,
        .relational_retirement_owner = relational_retirement_owner,
        .relational_index_maintenance_owner = relational_index_maintenance_owner,
    };
}

pub fn freeTxnPrepareRequest(alloc: std.mem.Allocator, req: *TxnPrepareRequest) void {
    if (req.range_guards_owner) |*owner| owner.deinit();
    freeTxnWrites(alloc, req.req.writes);
    freeTxnDeletes(alloc, req.req.deletes);
    freeTxnTransforms(alloc, req.req.transforms);
    freeTxnPredicates(alloc, req.req.predicates);
    integrity_wire.free(alloc, req.req.integrity);
    if (req.integrity_commands_owner) |*owner| owner.deinit();
    if (req.relational_activation_owner) |*owner| owner.deinit();
    if (req.relational_retirement_owner) |*owner| owner.deinit();
    if (req.relational_index_maintenance_owner) |*owner| owner.deinit();
    req.* = undefined;
}

test "distributed txn prepare preserves exact numeric row and transform payloads" {
    const alloc = std.testing.allocator;
    const row = "{\"id\":9007199254740993.0,\"max\":9223372036854775807e0}";
    const request: TxnPrepareRequest = .{ .txn_id = @splat(1), .topology_epoch = std.math.maxInt(u64), .req = .{
        .writes = &.{.{ .key = "row", .value = row }},
        .transforms = &.{.{ .key = "other", .operations = &.{.{ .op = .set, .path = "id", .value_json = "9007199254740993.0" }} }},
        .relational_integrity_generation_set = @splat(255),
    } };
    const bytes = try encodeTxnPrepareRequest(alloc, request);
    defer alloc.free(bytes);
    var parsed = try parseTxnPrepareRequest(alloc, bytes);
    defer freeTxnPrepareRequest(alloc, &parsed);
    try std.testing.expectEqualStrings(row, parsed.req.writes[0].value);
    try std.testing.expectEqualStrings("9007199254740993.0", parsed.req.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(request.topology_epoch, parsed.topology_epoch);
    try std.testing.expectEqual(request.req.relational_integrity_generation_set, parsed.req.relational_integrity_generation_set);
}

test "SQL document schema epoch survives distributed prepare transport" {
    const alloc = std.testing.allocator;
    for ([_]u32{ 0, 9, std.math.maxInt(u32) }) |version| {
        const encoded = try encodeTxnPrepareRequest(alloc, .{ .txn_id = @splat(1), .req = .{ .schema_version = version, .deletes = &.{"row"} } });
        defer alloc.free(encoded);
        var parsed = try parseTxnPrepareRequest(alloc, encoded);
        defer freeTxnPrepareRequest(alloc, &parsed);
        try std.testing.expectEqual(@as(?u32, version), parsed.req.schema_version);
        try std.testing.expect(parsed.req.relational_schema_version == null);
    }
}

test "distributed txn prepare preserves JSON null provenance and rejects invalid fields" {
    const alloc = std.testing.allocator;
    const encoded = try encodeTxnPrepareRequest(alloc, .{ .txn_id = @splat(1), .req = .{
        .writes = &.{.{ .key = "row", .value = "{\"j\":null,\"sql_null\":null}", .json_null_fields = &.{"j"} }},
    } });
    defer alloc.free(encoded);
    var parsed = try parseTxnPrepareRequest(alloc, encoded);
    defer freeTxnPrepareRequest(alloc, &parsed);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.writes[0].json_null_fields.len);
    try std.testing.expectEqualStrings("j", parsed.req.writes[0].json_null_fields[0]);
    for ([_][]const u8{
        "{\"value\":{\"j\":null},\"key\":\"row\",\"json_null_fields\":[\"missing\"]}",
        "{\"value\":{\"j\":1},\"key\":\"row\",\"json_null_fields\":[\"j\"]}",
        "{\"value\":{\"j\":null},\"key\":\"row\",\"json_null_fields\":[\"j\",\"j\"]}",
    }) |write| {
        const body = try std.fmt.allocPrint(alloc, "{{\"txn_id\":\"01010101010101010101010101010101\",\"writes\":[{s}],\"deletes\":[],\"transforms\":[],\"predicates\":[]}}", .{write});
        defer alloc.free(body);
        try std.testing.expectError(error.InvalidTxnRequest, parseTxnPrepareRequest(alloc, body));
    }
}

test "distributed txn index maintenance prepare roundtrips owned exact observation" {
    const alloc = std.testing.allocator;
    const command = @import("antfly_local_sources").storage_db_relational_index_maintenance_contract.Command{
        .action = .repair,
        .table_id = 7,
        .owner_group_id = 11,
        .schema_version = 2,
        .index_name = "by_id",
        .generation = 8,
        .slot = 1,
        .owner = @splat(0xff),
        .comparison = @splat(0x80),
        .expected_progress_digest = @splat(0xfe),
        .expected_maintenance_epoch = 3,
        .routing_key = "\x00\xff",
    };
    const request = TxnPrepareRequest{ .txn_id = @splat(1), .topology_epoch = 9, .req = .{ .relational_schema_version = 2, .relational_index_maintenance = command } };
    const bytes = try encodeTxnPrepareRequest(alloc, request);
    defer alloc.free(bytes);
    var parsed = try parseTxnPrepareRequest(alloc, bytes);
    defer freeTxnPrepareRequest(alloc, &parsed);
    try std.testing.expectEqualDeep(command, parsed.req.relational_index_maintenance.?);
    try std.testing.expectEqual(request.topology_epoch, parsed.topology_epoch);
    var zero = request;
    zero.req.relational_schema_version = 0;
    zero.req.relational_index_maintenance.?.schema_version = 0;
    const zero_bytes = try encodeTxnPrepareRequest(alloc, zero);
    defer alloc.free(zero_bytes);
    var zero_parsed = try parseTxnPrepareRequest(alloc, zero_bytes);
    defer freeTxnPrepareRequest(alloc, &zero_parsed);
    try std.testing.expectEqual(@as(?u32, 0), zero_parsed.req.relational_schema_version);
    try std.testing.expectEqual(@as(u32, 0), zero_parsed.req.relational_index_maintenance.?.schema_version);
    try std.testing.expectError(error.InvalidTxnRequest, parseTxnPrepareRequest(alloc, "{\"txn_id\":\"01010101010101010101010101010101\",\"writes\":[],\"deletes\":[],\"transforms\":[],\"predicates\":[],\"relational_schema_version\":4294967296}"));
}

test "distributed txn prepare roundtrips activation checkpoint and schema fence" {
    const alloc = std.testing.allocator;
    const request: TxnPrepareRequest = .{
        .txn_id = @splat(1),
        .topology_epoch = 9,
        .req = .{
            .restore_staging_scope = @splat(0xfe),
            .restore_staging_plan_id = @splat(0x81),
            .relational_schema_version = 7,
            .relational_integrity_generation_set = @as([32]u8, @splat(9)),
            .relational_repair = true,
            .relational_activation = .{ .routing_key = "\xff\x00", .expected = "\xfe\x00", .next = "\xfd\x00", .retry = true },
            .relational_retirement = .{ .routing_key = "\xff\x00", .expected = "\xfe\x00", .next = "\xfd\x00" },
        },
    };
    const bytes = try encodeTxnPrepareRequest(alloc, request);
    defer alloc.free(bytes);
    var parsed = try parseTxnPrepareRequest(alloc, bytes);
    defer freeTxnPrepareRequest(alloc, &parsed);
    try std.testing.expectEqual(request.req.relational_schema_version, parsed.req.relational_schema_version);
    try std.testing.expectEqualDeep(request.req.restore_staging_scope, parsed.req.restore_staging_scope);
    try std.testing.expectEqualDeep(request.req.restore_staging_plan_id, parsed.req.restore_staging_plan_id);
    try std.testing.expectEqual(request.req.relational_integrity_generation_set, parsed.req.relational_integrity_generation_set);
    try std.testing.expect(parsed.req.relational_repair);
    try std.testing.expectEqualDeep(request.req.relational_activation, parsed.req.relational_activation);
    try std.testing.expectEqualDeep(request.req.relational_retirement, parsed.req.relational_retirement);
}

test "distributed txn restore plan identity survives begin resolve and private batch transport" {
    const alloc = std.testing.allocator;
    const scope: [32]u8 = @splat(0xfe);
    const plan: [16]u8 = @splat(0x81);
    const begin: TxnBeginRequest = .{ .txn_id = @splat(7), .begin_timestamp = 1, .participants = &.{"table:docs:group:2"}, .restore_staging_scope = scope, .restore_staging_plan_id = plan };
    const begin_bytes = try encodeTxnBeginRequest(alloc, begin);
    defer alloc.free(begin_bytes);
    var parsed_begin = try parseTxnBeginRequest(alloc, begin_bytes);
    defer freeTxnBeginRequest(alloc, &parsed_begin);
    try std.testing.expectEqualDeep(begin.restore_staging_scope, parsed_begin.restore_staging_scope);
    try std.testing.expectEqualDeep(begin.restore_staging_plan_id, parsed_begin.restore_staging_plan_id);
    const resolve: TxnResolveRequest = .{ .txn_id = begin.txn_id, .status = .committed, .commit_version = 2, .restore_staging_scope = scope, .restore_staging_plan_id = plan };
    const resolve_bytes = try encodeTxnResolveRequest(alloc, resolve);
    defer alloc.free(resolve_bytes);
    try std.testing.expectEqualDeep(resolve, try parseTxnResolveRequest(alloc, resolve_bytes));
    const status_request: TxnStatusRequest = .{ .txn_id = begin.txn_id, .restore_staging_scope = scope, .restore_staging_plan_id = plan };
    const status_bytes = try encodeTxnStatusRequestWithScope(alloc, status_request);
    defer alloc.free(status_bytes);
    try std.testing.expectEqualDeep(status_request, try parseTxnStatusRequestWithScope(alloc, status_bytes));
    // The old bare-ID parser must not silently discard private authority.
    try std.testing.expectError(error.InvalidTxnRequest, parseTxnStatusRequest(alloc, status_bytes));
    const scoped_participant = try participantIdForGroupScoped(alloc, "docs:group:odd", 2, scope, plan);
    defer alloc.free(scoped_participant);
    const ack: TxnAcknowledgeRequest = .{ .txn_id = begin.txn_id, .participant = scoped_participant, .restore_staging_scope = scope, .restore_staging_plan_id = plan };
    const ack_bytes = try encodeTxnAcknowledgeRequest(alloc, ack);
    defer alloc.free(ack_bytes);
    var decoded_ack = try parseTxnAcknowledgeRequest(alloc, ack_bytes);
    defer freeTxnAcknowledgeRequest(alloc, &decoded_ack);
    try std.testing.expectEqualDeep(ack, decoded_ack);
    const ref = parseParticipantRef(scoped_participant).?;
    try std.testing.expectEqualDeep(ack.restore_staging_scope, ref.restore_staging_scope);
    try std.testing.expectEqualDeep(ack.restore_staging_plan_id, ref.restore_staging_plan_id);
    try std.testing.expectEqualStrings("docs:group:odd", ref.table_name);
    try std.testing.expect(parseParticipantRef(scoped_participant[0 .. scoped_participant.len - 1]) == null);
    const malformed = try std.fmt.allocPrint(alloc, "{s}0", .{scoped_participant});
    defer alloc.free(malformed);
    try std.testing.expect(parseParticipantRef(malformed) == null);
    const Capture = struct {
        fn ordinary(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return error.UnexpectedServingTableRoute;
        }
        fn apply(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.BatchRequest) !?void {
            const calls: *usize = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 2), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualDeep(@as([32]u8, @splat(0xfe)), req.restore_staging_scope.?);
            try std.testing.expectEqualDeep(@as([16]u8, @splat(0x81)), req.restore_staging_plan_id.?);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.transaction.?.resolve.status);
            calls.* += 1;
            return {};
        }
    };
    var calls: usize = 0;
    const writes: table_writes.TableWriteSource = .{ .ptr = &calls, .vtable = &.{ .batch = Capture.ordinary, .batch_group_local = Capture.apply } };
    try std.testing.expect(try resolveGroupLocalWithRequest(writes, alloc, 2, "docs", resolve, .none) != null);
    try std.testing.expectEqual(@as(usize, 1), calls);
    var unscoped = begin;
    unscoped.restore_staging_scope = null;
    try std.testing.expectError(error.InvalidTxnRequest, encodeTxnBeginRequest(alloc, unscoped));
    unscoped = begin;
    unscoped.restore_staging_plan_id = @splat(0);
    try std.testing.expectError(error.InvalidTxnRequest, encodeTxnBeginRequest(alloc, unscoped));

    const batch = @import("antfly_local_sources").api_batch;
    const mutation: db_mod.types.BatchRequest = .{ .restore_staging_scope = scope, .restore_staging_plan_id = plan, .transaction = .{ .resolve = .{ .txn_id = begin.txn_id, .status = .committed, .commit_version = 2 } } };
    const bytes = try batch.encodeBatchRequest(alloc, mutation);
    defer alloc.free(bytes);
    var decoded = try batch.parseInternalBatchRequest(alloc, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualDeep(mutation.restore_staging_plan_id, decoded.req.restore_staging_plan_id);
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseBatchRequest(alloc, bytes));
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseBatchRequest(alloc, "{\"_restore_staging_plan_id\":\"81818181818181818181818181818181\"}"));
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseInternalBatchRequest(alloc, "{\"_restore_staging_plan_id\":\"81818181818181818181818181818181\"}"));
}

test "distributed txn scoped participant recovery survives LSM reopen without resident authority" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/scoped-participant-recovery", .{tmp.sub_path});
    defer alloc.free(path);
    const txn_id: db_mod.types.TxnId = @splat(0x71);
    {
        var db = try db_mod.DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        const participant = try participantIdForGroupScoped(alloc, "restored", 77, @splat(0xfe), @splat(0x81));
        defer alloc.free(participant);
        _ = try db.beginTransactionWithIdAndParticipantsCreatedAtAndRole(txn_id, 1_000, 1_000, &.{participant}, true);
        try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "row", .value = "{}" }} });
        try db.resolveTransactionIntents(txn_id, .committed, 2_000);
    }
    const Recorder = struct {
        calls: usize = 0,
        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            return error.UnexpectedCall;
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedCall;
        }
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedUnscopedStatus;
        }
        fn resolve(ptr: *anyopaque, allocator: std.mem.Allocator, group: u64, name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 77), group);
            try std.testing.expectEqualStrings("restored", name);
            try std.testing.expectEqualDeep(@as([32]u8, @splat(0xfe)), req.restore_staging_scope.?);
            try std.testing.expectEqualDeep(@as([16]u8, @splat(0x81)), req.restore_staging_plan_id.?);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            // Status and ACK recovery derive their authority only from the
            // deserialized durable participant, after all resident state died.
            const observation: TxnStatusRequest = .{ .txn_id = req.txn_id, .restore_staging_scope = req.restore_staging_scope, .restore_staging_plan_id = req.restore_staging_plan_id };
            const status_bytes = try encodeTxnStatusRequestWithScope(allocator, observation);
            defer allocator.free(status_bytes);
            try std.testing.expectEqualDeep(observation, try parseTxnStatusRequestWithScope(allocator, status_bytes));
            const participant = try participantIdForGroupScoped(allocator, name, group, req.restore_staging_scope, req.restore_staging_plan_id);
            defer allocator.free(participant);
            const acknowledgement: TxnAcknowledgeRequest = .{ .txn_id = req.txn_id, .participant = participant, .restore_staging_scope = req.restore_staging_scope, .restore_staging_plan_id = req.restore_staging_plan_id };
            const bytes = try encodeTxnAcknowledgeRequest(allocator, acknowledgement);
            defer allocator.free(bytes);
            var parsed = try parseTxnAcknowledgeRequest(allocator, bytes);
            defer freeTxnAcknowledgeRequest(allocator, &parsed);
            try std.testing.expectEqualDeep(acknowledgement, parsed);
            self.calls += 1;
        }
    };
    var recorder: Recorder = .{};
    var resolver: RecoveryResolver = .{ .alloc = alloc, .worker = .{ .ptr = &recorder, .vtable = &.{ .begin_group = Recorder.begin, .prepare_group = Recorder.prepare, .resolve_group = Recorder.resolve, .status_group = Recorder.status } }, .lease_owned = true };
    var reopened = try db_mod.DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    defer reopened.close();
    const stats = try @import("../storage/server_transaction_recovery.zig").runDbRecoveryOnce(&reopened, resolver.serverConfig());
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqual(@as(u64, 1), stats.notification_successes);
    try std.testing.expectError(error.TxnNotFound, reopened.getTransactionStatus(txn_id));
}

pub fn parseTxnResolveRequest(alloc: std.mem.Allocator, body: []const u8) !TxnResolveRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const restore_scope = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    return .{
        .restore_staging_scope = restore_scope,
        .restore_staging_plan_id = try parseRestorePlan(obj, restore_scope),
        .txn_id = try parseTxnIdHex(requireString(obj, "txn_id")),
        .status = parseTxnStatus(requireString(obj, "status")) orelse return error.InvalidTxnRequest,
        .commit_version = try requireU64(obj, "commit_version"),
        .topology_epoch = try optionalU64(obj, "topology_epoch"),
        .sync_level = if (obj.get("sync_level")) |value| db_mod.types.parsePublicSyncLevelText(switch (value) {
            .string => |text| text,
            else => return error.InvalidTxnRequest,
        }) orelse return error.InvalidTxnRequest else .propose,
    };
}

pub fn parseTxnStatusRequest(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.TxnId {
    const parsed = try parseTxnStatusRequestWithScope(alloc, body);
    if (parsed.restore_staging_scope != null) return error.InvalidTxnRequest;
    return parsed.txn_id;
}

pub fn parseTxnStatusRequestWithScope(alloc: std.mem.Allocator, body: []const u8) !TxnStatusRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const scope: ?[32]u8 = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const plan_id = try parseRestorePlan(obj, scope);
    if ((scope == null) != (plan_id == null)) return error.InvalidTxnRequest;
    return .{ .txn_id = try parseTxnIdHex(requireString(obj, "txn_id")), .restore_staging_scope = scope, .restore_staging_plan_id = plan_id };
}

pub fn parseTxnAcknowledgeRequest(alloc: std.mem.Allocator, body: []const u8) !TxnAcknowledgeRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const participant = requireString(obj, "participant");
    if (participant.len == 0 or parseParticipantRef(participant) == null) return error.InvalidTxnRequest;
    const scope: ?[32]u8 = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const plan_id = try parseRestorePlan(obj, scope);
    if ((scope == null) != (plan_id == null)) return error.InvalidTxnRequest;
    return .{
        .restore_staging_scope = scope,
        .restore_staging_plan_id = plan_id,
        .txn_id = try parseTxnIdHex(requireString(obj, "txn_id")),
        .participant = try alloc.dupe(u8, participant),
    };
}

pub fn freeTxnAcknowledgeRequest(alloc: std.mem.Allocator, req: *TxnAcknowledgeRequest) void {
    alloc.free(@constCast(req.participant));
    req.* = undefined;
}

pub fn parseTxnAcknowledgeManyRequest(alloc: std.mem.Allocator, body: []const u8) !TxnAcknowledgeManyRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |value| value,
        else => return error.InvalidTxnRequest,
    };
    if (obj.get("participant") != null) return error.InvalidTxnRequest;
    const values = obj.get("participants") orelse return error.InvalidTxnRequest;
    if (values != .array or values.array.items.len == 0 or values.array.items.len > 64) return error.InvalidTxnRequest;
    for (values.array.items) |value| if (value != .string or parseParticipantRef(value.string) == null) return error.InvalidTxnRequest;
    const scope: ?[32]u8 = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const plan_id = try parseRestorePlan(obj, scope);
    if ((scope == null) != (plan_id == null)) return error.InvalidTxnRequest;
    const txn_id = try parseTxnIdHex(requireString(obj, "txn_id"));
    const participants = try alloc.alloc([]const u8, values.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (participants[0..initialized]) |participant| alloc.free(participant);
        alloc.free(participants);
    }
    for (values.array.items, participants) |value, *participant| {
        participant.* = try alloc.dupe(u8, value.string);
        initialized += 1;
    }
    return .{ .txn_id = txn_id, .participants = participants, .restore_staging_scope = scope, .restore_staging_plan_id = plan_id };
}

pub fn freeTxnAcknowledgeManyRequest(alloc: std.mem.Allocator, req: *TxnAcknowledgeManyRequest) void {
    for (req.participants) |participant| alloc.free(participant);
    alloc.free(req.participants);
    req.* = undefined;
}

pub fn parseTxnStatusResponse(alloc: std.mem.Allocator, body: []const u8) !TxnStatusResponse {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    return .{ .status = parseTxnStatus(requireString(obj, "status")) orelse return error.InvalidTxnRequest };
}

fn parseTxnWrites(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransactionWrite {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    const out = try alloc.alloc(db_mod.types.TransactionWrite, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
            for (write.json_null_fields) |field| alloc.free(field);
            if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
        }
        if (out.len > 0) alloc.free(out);
    }
    for (arr.items, 0..) |item, i| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTxnRequest,
        };
        const key = try alloc.dupe(u8, requireString(obj, "key"));
        errdefer alloc.free(key);
        const raw_value = obj.get("value") orelse return error.InvalidTxnRequest;
        const encoded_value = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(raw_value, .{})});
        errdefer alloc.free(encoded_value);
        var fields: std.ArrayListUnmanaged([]const u8) = .empty;
        defer fields.deinit(alloc);
        if (obj.get("json_null_fields")) |names| {
            if (names != .array or names.array.items.len > 256 or raw_value != .object) return error.InvalidTxnRequest;
            for (names.array.items) |name| {
                if (name != .string or name.string.len == 0) return error.InvalidTxnRequest;
                const cell = raw_value.object.get(name.string) orelse return error.InvalidTxnRequest;
                if (cell != .null) return error.InvalidTxnRequest;
                for (fields.items) |prior| if (std.mem.eql(u8, prior, name.string)) return error.InvalidTxnRequest;
                try fields.append(alloc, name.string);
            }
        }
        const owned_fields = try db_mod.types.cloneJsonNullFields(alloc, fields.items);
        out[i] = .{
            .key = key,
            .value = encoded_value,
            .json_null_fields = owned_fields,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.TransactionWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
        for (write.json_null_fields) |field| alloc.free(field);
        if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
    }
    if (writes.len > 0) alloc.free(@constCast(writes));
}

fn parseTxnDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![]const []const u8 {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    const out = try alloc.alloc([]const u8, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(@constCast(key));
        if (out.len > 0) alloc.free(out);
    }
    for (arr.items, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, switch (item) {
            .string => |s| s,
            else => return error.InvalidTxnRequest,
        });
        initialized += 1;
    }
    return out;
}

fn freeTxnDeletes(alloc: std.mem.Allocator, deletes: []const []const u8) void {
    for (deletes) |key| alloc.free(@constCast(key));
    if (deletes.len > 0) alloc.free(@constCast(deletes));
}

fn parseTxnTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.DocumentTransform {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.DocumentTransform, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(@constCast(transform.operations));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTxnRequest,
        };
        const key = requireString(obj, "key");
        if (key.len == 0) return error.InvalidTxnRequest;
        const operations_value = obj.get("operations") orelse return error.InvalidTxnRequest;
        const operations_arr = switch (operations_value) {
            .array => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        var ops = try alloc.alloc(db_mod.types.TransformOp, operations_arr.items.len);
        var ops_initialized: usize = 0;
        errdefer {
            for (ops[0..ops_initialized]) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (ops.len > 0) alloc.free(ops);
        }
        for (operations_arr.items, 0..) |op_item, i| {
            const op_obj = switch (op_item) {
                .object => |inner| inner,
                else => return error.InvalidTxnRequest,
            };
            const op_text = requireString(op_obj, "op");
            const path = requireString(op_obj, "path");
            if (op_text.len == 0 or path.len == 0) return error.InvalidTxnRequest;
            ops[i] = .{
                .op = parseTransformOpType(op_text) orelse return error.InvalidTxnRequest,
                .path = try alloc.dupe(u8, path),
                .value_json = if (op_obj.get("value")) |raw_value| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(raw_value, .{})}) else null,
            };
            ops_initialized += 1;
            db_mod.transform.validateDocumentTransform(alloc, .{
                .key = key,
                .operations = ops[i .. i + 1],
            }) catch return error.InvalidTxnRequest;
        }
        const upsert = if (obj.get("upsert")) |upsert_value| switch (upsert_value) {
            .bool => |flag| flag,
            .null => false,
            else => return error.InvalidTxnRequest,
        } else false;
        out[initialized] = .{
            .key = try alloc.dupe(u8, key),
            .operations = ops,
            .upsert = upsert,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnTransforms(alloc: std.mem.Allocator, transforms: []const db_mod.types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        for (transform.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (transform.operations.len > 0) alloc.free(@constCast(transform.operations));
    }
    if (transforms.len > 0) alloc.free(@constCast(transforms));
}

fn parseTransformOpType(text: []const u8) ?db_mod.types.TransformOpType {
    if (std.mem.eql(u8, text, "$set")) return .set;
    if (std.mem.eql(u8, text, "$setOnInsert")) return .set_on_insert;
    if (std.mem.eql(u8, text, "$unset")) return .unset;
    if (std.mem.eql(u8, text, "$inc")) return .inc;
    if (std.mem.eql(u8, text, "$push")) return .push;
    if (std.mem.eql(u8, text, "$pull")) return .pull;
    if (std.mem.eql(u8, text, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, text, "$min")) return .min;
    if (std.mem.eql(u8, text, "$max")) return .max;
    return null;
}

fn parseTxnPredicates(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransactionVersionPredicate {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    const out = try alloc.alloc(db_mod.types.TransactionVersionPredicate, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| alloc.free(@constCast(predicate.key));
        if (out.len > 0) alloc.free(out);
    }
    for (arr.items, 0..) |item, i| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTxnRequest,
        };
        const expected_version = try requireU64(obj, "expected_version");
        var content_digest: ?[32]u8 = null;
        if (obj.get("expected_content_digest")) |encoded| {
            if (encoded != .string or encoded.string.len != 64 or expected_version == 0) return error.InvalidTxnRequest;
            var digest: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest, encoded.string) catch return error.InvalidTxnRequest;
            content_digest = digest;
        }
        const unique_absence = if (obj.get("unique_absence")) |flag| if (flag == .bool and (expected_version == 0 or !flag.bool)) flag.bool else return error.InvalidTxnRequest else false;
        out[i] = .{
            .key = try alloc.dupe(u8, requireString(obj, "key")),
            .expected_version = expected_version,
            .expected_content_digest = content_digest,
            .unique_absence = unique_absence,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.TransactionVersionPredicate) void {
    for (predicates) |predicate| alloc.free(@constCast(predicate.key));
    if (predicates.len > 0) alloc.free(@constCast(predicates));
}

fn requireString(obj: std.json.ObjectMap, key: []const u8) []const u8 {
    const value = obj.get(key) orelse return "";
    return switch (value) {
        .string => |s| s,
        else => "",
    };
}

fn parseU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| if (integer >= 0) @intCast(integer) else error.InvalidTxnRequest,
        .number_string => |text| std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidTxnRequest,
        else => error.InvalidTxnRequest,
    };
}

fn requireU64(obj: std.json.ObjectMap, key: []const u8) !u64 {
    return try parseU64(obj.get(key) orelse return error.InvalidTxnRequest);
}

fn optionalU64(obj: std.json.ObjectMap, key: []const u8) !u64 {
    return try parseU64(obj.get(key) orelse return 0);
}

fn parseTxnStatus(text: []const u8) ?db_mod.types.TxnStatus {
    if (std.mem.eql(u8, text, "pending")) return .pending;
    if (std.mem.eql(u8, text, "committed")) return .committed;
    if (std.mem.eql(u8, text, "aborted")) return .aborted;
    return null;
}

fn abortParticipants(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    attempted_count: usize,
) !void {
    const slots = try alloc.alloc(ParticipantFanoutSlot, participants.len);
    defer alloc.free(slots);
    for (slots) |*slot| slot.reset();
    return abortParticipantsWithOptions(alloc, worker, txn_id, timestamp, participants, participant_ids, attempted_count, slots, .{});
}

fn abortParticipantsWithOptions(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    attempted_count: usize,
    slots: []ParticipantFanoutSlot,
    options: ExecuteOptions,
) !void {
    if (participants.len == 0) return;
    std.debug.assert(participant_ids.len == participants.len);
    std.debug.assert(attempted_count > 0 and attempted_count <= participants.len);

    // The first participant is the coordinator. Do not report an aborted
    // transaction until its abort decision is durable; otherwise a prepared
    // transaction can be stranded forever while the client is told it lost.
    const coordinator = participants[0];
    worker.resolveGroup(alloc, coordinator.group_id, coordinator.table_name, .{
        .restore_staging_scope = coordinator.restore_staging_scope,
        .restore_staging_plan_id = coordinator.restore_staging_plan_id,
        .txn_id = txn_id,
        .status = .aborted,
        .commit_version = timestamp,
        // An abort is a transaction decision just like a commit. Do not tell
        // the client it lost until the coordinator decision is committed and
        // applied; follower delivery remains recoverable from that record.
        .sync_level = .write,
    }) catch {
        const status = worker.statusGroupWithRequest(
            alloc,
            coordinator.group_id,
            coordinator.table_name,
            coordinator.statusRequest(txn_id),
            null,
        ) catch return error.AbortDecisionNotDurable;
        if (status != .aborted) return error.AbortDecisionNotDurable;
    };

    // The coordinator decision must be durable before any independent cleanup.
    runAbortFollowerFanout(worker, txn_id, timestamp, participants, participant_ids, attempted_count, false, slots, options);
}

fn abortParticipantsWithContactMask(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    slots: []ParticipantFanoutSlot,
    retained: bool,
    options: ExecuteOptions,
) !void {
    if (participants.len == 0) return;
    std.debug.assert(participant_ids.len == participants.len and slots.len == participants.len);
    // Contact evidence is invocation-local, not transaction-local. In a
    // retained replay even a definitely unproposed BEGIN can have old intents.
    // Resolve the entire durable cohort; unavailable followers remain enlisted.
    if (retained) return abortParticipantsWithOptions(alloc, worker, txn_id, timestamp, participants, participant_ids, participants.len, slots, options);

    const coordinator = participants[0];
    worker.resolveGroup(alloc, coordinator.group_id, coordinator.table_name, .{
        .restore_staging_scope = coordinator.restore_staging_scope,
        .restore_staging_plan_id = coordinator.restore_staging_plan_id,
        .txn_id = txn_id,
        .status = .aborted,
        .commit_version = timestamp,
        .sync_level = .write,
    }) catch {
        const status = worker.statusGroupWithRequest(alloc, coordinator.group_id, coordinator.table_name, coordinator.statusRequest(txn_id), null) catch
            return error.AbortDecisionNotDurable;
        if (status != .aborted) return error.AbortDecisionNotDurable;
    };

    runAbortFollowerFanout(worker, txn_id, timestamp, participants, participant_ids, null, true, slots, options);
}

const AbortFollowerFanoutTask = struct {
    fn run(
        allocator: std.mem.Allocator,
        original_worker: ParticipantWorker,
        coordinator: *const ParticipantTxn,
        participant: *const ParticipantTxn,
        participant_id: []const u8,
        txn_id: db_mod.types.TxnId,
        timestamp: u64,
        resolve: bool,
        allow_missing_state: bool,
        slot: *ParticipantFanoutSlot,
        owner: ?*runnable_fanout.Owner,
    ) void {
        slot.reset();
        const worker = activateParticipant(original_worker, owner) catch |err| {
            slot.err = err;
            return;
        };
        defer if (owner) |child| child.park();
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        if (resolve) worker.resolveGroup(arena.allocator(), participant.group_id, participant.table_name, .{
            .restore_staging_scope = participant.restore_staging_scope,
            .restore_staging_plan_id = participant.restore_staging_plan_id,
            .txn_id = txn_id,
            .status = .aborted,
            .commit_version = timestamp,
            .sync_level = .write,
        }) catch |err| {
            if (!allow_missing_state or err != error.TxnNotFound) {
                slot.err = err;
                return;
            }
        };
        // Join the independently proven resolutions before constructing a
        // bounded coordinator command. Failed followers retain recovery debt.
        slot.acknowledgement_ready = true;
        _ = coordinator;
        _ = participant_id;
    }
};

fn acknowledgeFollowerWindow(allocator: std.mem.Allocator, worker: ParticipantWorker, txn_id: db_mod.types.TxnId, participants: []const ParticipantTxn, participant_ids: []const []const u8, slots: []ParticipantFanoutSlot, start: usize, end: usize, committed: bool) void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var ids: [64][]const u8 = undefined;
    var indexes: [64]usize = undefined;
    var count: usize = 0;
    for (start..end) |i| {
        if (!slots[i].acknowledgement_ready) continue;
        ids[count] = participant_ids[i];
        indexes[count] = i;
        count += 1;
        if (count == ids.len) {
            worker.acknowledgeManyGroup(arena.allocator(), participants[0].group_id, participants[0].table_name, .{ .txn_id = txn_id, .participants = ids[0..count], .restore_staging_scope = participants[0].restore_staging_scope, .restore_staging_plan_id = participants[0].restore_staging_plan_id }) catch |err| {
                for (indexes[0..count]) |index| {
                    slots[index].acknowledgement_err = err;
                    if (committed) slots[index].propagation_pending = true;
                }
            };
            count = 0;
        }
    }
    if (count != 0) worker.acknowledgeManyGroup(arena.allocator(), participants[0].group_id, participants[0].table_name, .{ .txn_id = txn_id, .participants = ids[0..count], .restore_staging_scope = participants[0].restore_staging_scope, .restore_staging_plan_id = participants[0].restore_staging_plan_id }) catch |err| {
        for (indexes[0..count]) |index| {
            slots[index].acknowledgement_err = err;
            if (committed) slots[index].propagation_pending = true;
        }
    };
}

fn runAbortFollowerFanout(
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    attempted_count: ?usize,
    allow_missing_state: bool,
    slots: []ParticipantFanoutSlot,
    options: ExecuteOptions,
) void {
    std.debug.assert(participants.len == participant_ids.len and participants.len == slots.len);
    if (participants.len <= 1) return;
    const width = fanoutWidth(options, participants.len - 1);
    var start: usize = 1;
    while (start < participants.len) : (start += width) {
        const end = @min(start + width, participants.len);
        var window = reserveFanoutWindow(options, start, end) catch |err| {
            for (slots[start..end]) |*slot| {
                slot.err = err;
            }
            return;
        };
        defer if (window) |*owned| owned.retire();
        var group: std.Io.Group = .init;
        for (start..end) |i| {
            // Copy contact evidence before the worker resets its result slot.
            const resolve = if (attempted_count) |count| i < count else slots[i].may_have_transaction_state;
            if (options.fanout_io) |io| {
                group.concurrent(io, AbortFollowerFanoutTask.run, .{ options.participantAllocator(), worker, &participants[0], &participants[i], participant_ids[i], txn_id, timestamp, resolve, allow_missing_state, &slots[i], options.participantOwner(i, start) }) catch
                    AbortFollowerFanoutTask.run(options.participantAllocator(), worker, &participants[0], &participants[i], participant_ids[i], txn_id, timestamp, resolve, allow_missing_state, &slots[i], options.participantOwner(i, start));
            } else {
                AbortFollowerFanoutTask.run(options.participantAllocator(), worker, &participants[0], &participants[i], participant_ids[i], txn_id, timestamp, resolve, allow_missing_state, &slots[i], options.participantOwner(i, start));
            }
        }
        if (options.fanout_io) |io| joinFanoutWindow(&window, &group, io, slots[start..end], false);
        acknowledgeFollowerWindow(options.participantAllocator(), worker, txn_id, participants, participant_ids, slots, start, end, false);
    }
    for (slots[1..], 1..) |slot, i| {
        if (slot.err) |err| std.log.warn("transaction abort delivery failed table={s} group_id={} err={s}", .{ participants[i].table_name, participants[i].group_id, @errorName(err) });
        if (slot.acknowledgement_err) |err| std.log.warn("transaction abort acknowledgement deferred table={s} group_id={} err={s}", .{ participants[0].table_name, participants[0].group_id, @errorName(err) });
    }
}

fn participantConflict(participant: ParticipantTxn, cause: anyerror) CommitConflict {
    const reason: ?contract.CommitConflictReason = switch (cause) {
        error.UniqueConstraintViolation => .unique_constraint_violation,
        error.ForeignKeyParentMissing => .foreign_key_parent_missing,
        error.ForeignKeyReferenced => .foreign_key_referenced,
        else => null,
    };
    if (reason != null or participant.integrity.items.len != 0 or participant.integrity_commands.items.len != 0 or participant.relational_activation != null or participant.relational_retirement != null or participant.relational_index_maintenance != null) return .{
        .table_name = participant.table_name,
        .key = "",
        .message = if (reason) |value| switch (value) {
            .unique_constraint_violation => "unique constraint violation",
            .foreign_key_parent_missing => "referenced parent does not exist",
            .foreign_key_referenced => "parent is still referenced",
        } else "relational dependency changed; retry the mutation",
        .group_id = participant.group_id,
        .phase = .prepare,
        .reason = reason,
        .retryable = reason == null,
    };
    if (participant.predicates.items.len > 0) {
        return .{
            .table_name = participant.table_name,
            .key = participant.predicates.items[0].key,
            .message = "version conflict",
            .group_id = participant.group_id,
            .phase = .prepare,
        };
    }
    if (participant.writes.items.len > 0) {
        return .{
            .table_name = participant.table_name,
            .key = participant.writes.items[0].key,
            .message = "intent conflict",
            .group_id = participant.group_id,
            .phase = .prepare,
        };
    }
    if (participant.deletes.items.len > 0) {
        return .{
            .table_name = participant.table_name,
            .key = participant.deletes.items[0],
            .message = "intent conflict",
            .group_id = participant.group_id,
            .phase = .prepare,
        };
    }
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "transaction conflict",
        .group_id = participant.group_id,
        .phase = .prepare,
    };
}

fn participantUnavailableConflict(participant: ParticipantTxn, phase: ParticipantPhase) CommitConflict {
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "participant unavailable",
        .group_id = participant.group_id,
        .phase = phase,
    };
}

fn participantDecisionConflict(participant: ParticipantTxn, phase: ParticipantPhase) CommitConflict {
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "decision conflict",
        .group_id = participant.group_id,
        .phase = phase,
    };
}

fn participantTornStateConflict(participant: ParticipantTxn, phase: ParticipantPhase) CommitConflict {
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "transaction state missing",
        .group_id = participant.group_id,
        .phase = phase,
    };
}

fn sleepRecoveryNs(duration_ns: u64, completion: ?runnable_abi.Borrowed) !void {
    var wait: runnable_fanout.RemoteWait = .{ .borrow = completion };
    if (wait.boundary()) |boundary| {
        try boundary.beginWait();
        sleepNs(duration_ns);
        try boundary.endWait();
    } else sleepNs(duration_ns);
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

pub const implementation_tests = implementationTests();
fn implementationTests() type {
    if (!@import("builtin").is_test or @import("storage_source_options").control_only) return struct {};
    const Suite = struct {
        test "distributed txn acknowledgement windows batch only proven followers and preserve recovery debt" {
            const Recorder = struct {
                batches: usize = 0,
                singles: usize = 0,
                members: usize = 0,
                failure: ?anyerror = null,
                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status, .acknowledge_group = single, .acknowledge_many_group = many } };
                }
                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
                fn single(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.singles += 1;
                }
                fn many(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnAcknowledgeManyRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.batches += 1;
                    try std.testing.expect(req.participants.len <= 64);
                    for (req.participants) |id| try std.testing.expect(!std.mem.eql(u8, id, "excluded"));
                    if (self.failure) |err| return err;
                    self.members += req.participants.len;
                }
            };
            var recorder: Recorder = .{};
            const participants: [132]ParticipantTxn = @splat(.{ .table_name = "docs", .group_id = 7, .topology_epoch = 1 });
            var ids: [132][]const u8 = @splat("eligible");
            ids[2] = "excluded";
            var slots: [132]ParticipantFanoutSlot = @splat(.{ .acknowledgement_ready = true });
            slots[2] = .{ .err = error.GroupLeaderUnavailable };
            const txn: db_mod.types.TxnId = @splat(1);
            acknowledgeFollowerWindow(std.testing.allocator, recorder.worker(), txn, &participants, &ids, &slots, 1, slots.len, true);
            try std.testing.expectEqual(@as(usize, 3), recorder.batches);
            try std.testing.expectEqual(@as(usize, 130), recorder.members);
            try std.testing.expectEqual(@as(usize, 0), recorder.singles);
            recorder.failure = error.ConnectionRefused;
            acknowledgeFollowerWindow(std.testing.allocator, recorder.worker(), txn, &participants, &ids, &slots, 1, 4, true);
            try std.testing.expectEqual(@as(?anyerror, error.ConnectionRefused), slots[1].acknowledgement_err);
            try std.testing.expect(slots[1].propagation_pending and slots[3].propagation_pending);
            try std.testing.expect(slots[2].acknowledgement_err == null);
            try std.testing.expectEqual(@as(usize, 0), recorder.singles);
            recorder.failure = error.UnsupportedOperation;
            try recorder.worker().acknowledgeManyGroup(std.testing.allocator, 7, "docs", .{ .txn_id = txn, .participants = &.{ "one", "two" } });
            try std.testing.expectEqual(@as(usize, 2), recorder.singles);
        }
        test "distributed txn prepare preserves exact content observations" {
            const alloc = std.testing.allocator;
            const encoded = try encodeTxnPrepareRequest(alloc, .{
                .txn_id = @splat(1),
                .req = .{ .predicates = &.{.{ .key = "row", .expected_version = std.math.maxInt(u64), .expected_content_digest = @splat(11) }} },
            });
            defer alloc.free(encoded);
            var decoded = try parseTxnPrepareRequest(alloc, encoded);
            defer freeTxnPrepareRequest(alloc, &decoded);
            try std.testing.expectEqual(std.math.maxInt(u64), decoded.req.predicates[0].expected_version);
            try std.testing.expectEqual(@as([32]u8, @splat(11)), decoded.req.predicates[0].expected_content_digest.?);
            for ([_][]const u8{ "null", "[]", "[256]", "\"not-a-digest\"" }) |digest| {
                const malformed = try std.fmt.allocPrint(alloc, "{{\"txn_id\":\"01010101010101010101010101010101\",\"writes\":[],\"deletes\":[],\"transforms\":[],\"predicates\":[{{\"key\":\"row\",\"expected_version\":1,\"expected_content_digest\":{s}}}]}}", .{digest});
                defer alloc.free(malformed);
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnPrepareRequest(alloc, malformed));
            }
        }

        test "db transaction recovery runtime resolves table-group participants through distributed txn resolver" {
            const alloc = std.testing.allocator;

            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-txn-recovery-db", .{tmp.sub_path});
            defer alloc.free(path);

            const Recorder = struct {
                calls: usize = 0,
                committed_calls: usize = 0,
                aborted_calls: usize = 0,
                last_group_id: u64 = 0,
                last_status: ?db_mod.types.TxnStatus = null,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqualStrings("docs", table_name);
                    try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
                    self.calls += 1;
                    self.last_group_id = group_id;
                    self.last_status = req.status;
                    switch (req.status) {
                        .committed => self.committed_calls += 1,
                        .aborted => self.aborted_calls += 1,
                        else => {},
                    }
                }
            };

            var recorder = Recorder{};
            var resolver = RecoveryResolver{
                .alloc = alloc,
                .worker = recorder.worker(),
                .lease_owned = true,
                .interval_ms = 250,
            };
            var db = try db_mod.DB.open(alloc, path, .{
                .transaction_recovery = resolver.config(),
            });
            defer db.close();

            const participant = try participantIdForGroup(alloc, "docs", 77);
            defer alloc.free(participant);
            const txn_id = try db.beginTransactionWithParticipants(1_000, &.{participant});
            try db.writeTransaction(txn_id, .{
                .writes = &.{.{ .key = "doc:recover", .value = "{\"title\":\"value\"}" }},
            });
            try db.resolveTransactionIntents(txn_id, .committed, 2_000);

            var attempts: usize = 0;
            while (attempts < 200) : (attempts += 1) {
                const status = db.getTransactionStatus(txn_id);
                if (status) |_| {} else |err| {
                    if (err == transactions_mod.TxnError.TxnNotFound) break;
                    return err;
                }
                sleepNs(5 * std.time.ns_per_ms);
            }

            const stats = try db.stats(alloc);
            defer db_mod.types.freeDBStats(alloc, stats);
            try std.testing.expect(stats.transaction_recovery.notification_attempts > 0);
            try std.testing.expect(stats.transaction_recovery.notification_successes > 0);
            try std.testing.expect(recorder.calls > 0);
            try std.testing.expectEqual(@as(u64, 77), recorder.last_group_id);
            try std.testing.expect(recorder.committed_calls > 0);
            try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
        }

        test "db one-shot transaction recovery resolves table-group participants through distributed txn resolver" {
            const alloc = std.testing.allocator;

            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-txn-recovery-once-db", .{tmp.sub_path});
            defer alloc.free(path);

            const Recorder = struct {
                calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqualStrings("docs", table_name);
                    try std.testing.expectEqual(@as(u64, 88), group_id);
                    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
                    try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
                    self.calls += 1;
                }
            };

            var recorder = Recorder{};
            var resolver = RecoveryResolver{
                .alloc = alloc,
                .worker = recorder.worker(),
                .lease_owned = true,
            };
            var db = try db_mod.DB.open(alloc, path, .{});
            defer db.close();

            const participant = try participantIdForGroup(alloc, "docs", 88);
            defer alloc.free(participant);
            const txn_id = try db.beginTransactionWithParticipants(1_000, &.{participant});
            try db.writeTransaction(txn_id, .{
                .writes = &.{.{ .key = "doc:recover-once", .value = "{\"title\":\"value\"}" }},
            });
            try db.resolveTransactionIntents(txn_id, .committed, 2_000);

            const stats = try @import("../storage/server_transaction_recovery.zig").runDbRecoveryOnce(&db, resolver.serverConfig());
            try std.testing.expect(stats.notification_attempts > 0);
            try std.testing.expect(stats.notification_successes > 0);
            try std.testing.expectEqual(@as(usize, 1), recorder.calls);
            try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
        }

        test "db one-shot transaction recovery does not auto-abort fresh pending transactions by default" {
            const alloc = std.testing.allocator;

            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-txn-recovery-fresh-pending-db", .{tmp.sub_path});
            defer alloc.free(path);

            const Recorder = struct {
                calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.calls += 1;
                }
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
            };

            var recorder = Recorder{};
            var resolver = RecoveryResolver{
                .alloc = alloc,
                .worker = recorder.worker(),
                .lease_owned = true,
            };
            var db = try db_mod.DB.open(alloc, path, .{});
            defer db.close();

            const participant = try participantIdForGroup(alloc, "docs", 99);
            defer alloc.free(participant);
            const txn_id = try db.beginTransactionWithParticipants(1_000, &.{participant});
            try db.writeTransaction(txn_id, .{
                .writes = &.{.{ .key = "doc:fresh-pending", .value = "{\"title\":\"value\"}" }},
            });

            const stats = try @import("../storage/server_transaction_recovery.zig").runDbRecoveryOnce(&db, resolver.serverConfig());
            try std.testing.expectEqual(@as(u64, 0), stats.notification_attempts);
            try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
            try std.testing.expectEqual(@as(usize, 0), recorder.calls);
            try std.testing.expectEqual(db_mod.types.TxnStatus.pending, try db.getTransactionStatus(txn_id));
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = implementation_tests;
}

// Shared control tests belong to the consumer root, even when the physical
// implementation imports these contracts to implement its own operations.
pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const test_owner_root = @import("antfly_source_root");
    if (@hasDecl(test_owner_root, "implementation_tests_only") and test_owner_root.implementation_tests_only) return struct {};
    const Suite = struct {
        test "hosted participant bulk acknowledgements use typed transport and only definite legacy fallback" {
            const FakeRouter = struct {
                fn iface() table_router.HostedGroupRouter {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .local_node_id = localNodeId,
                            .local_status = localStatus,
                            .group_leader_node_id = groupLeaderNodeId,
                            .group_node_ids = groupNodeIds,
                            .node_status = nodeStatus,
                            .node_base_uri = nodeBaseUri,
                        },
                    };
                }

                fn localNodeId(_: *anyopaque) u64 {
                    return 99;
                }

                fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
                    return .absent;
                }

                fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
                    return 1;
                }

                fn groupNodeIds(_: *anyopaque, alloc: std.mem.Allocator, _: u64, _: table_router.RouteBudget) ![]u64 {
                    return try alloc.dupe(u64, &.{ 1, 2, 3 });
                }

                fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
                    return if (node_id >= 1 and node_id <= 3) .active else .absent;
                }

                fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
                    return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
                }
            };
            const Executor = struct {
                status: u16 = 200,
                unknown: bool = false,
                transport_failure: bool = false,
                many: usize = 0,
                singles: usize = 0,
                fn iface(self: *@This()) http_common.RequestExecutor {
                    return .{ .ptr = self, .vtable = &.{ .execute = execute } };
                }
                fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(http_common.Method.POST, req.method);
                    if (std.mem.endsWith(u8, req.uri, "/txn-acknowledge-many")) {
                        self.many += 1;
                        var decoded = try parseTxnAcknowledgeManyRequest(alloc, req.body);
                        defer freeTxnAcknowledgeManyRequest(alloc, &decoded);
                        try std.testing.expectEqual(@as(usize, 2), decoded.participants.len);
                        try std.testing.expectEqualStrings("table2:00000004:docs:8", decoded.participants[0]);
                        if (self.transport_failure) return error.ConnectionResetByPeer;
                        if (self.unknown) return http_route_helpers.textResponseWithHeaders(alloc, self.status, "write outcome unknown", &.{.{ .name = internal_batch_forwarding.outcome_header, .value = internal_batch_forwarding.outcome_unknown_v1 }});
                        return .{ .status = self.status };
                    }
                    try std.testing.expect(std.mem.endsWith(u8, req.uri, "/txn-acknowledge"));
                    self.singles += 1;
                    var decoded = try parseTxnAcknowledgeRequest(alloc, req.body);
                    defer freeTxnAcknowledgeRequest(alloc, &decoded);
                    try std.testing.expectEqualStrings(if (self.singles == 1) "table2:00000004:docs:8" else "table2:00000004:docs:9", decoded.participant);
                    return .{ .status = 200 };
                }
            };
            const req: TxnAcknowledgeManyRequest = .{ .txn_id = @splat(1), .participants = &.{ "table2:00000004:docs:8", "table2:00000004:docs:9" } };
            for ([_]u16{ 200, 404, 405, 400 }) |status| {
                var executor: Executor = .{ .status = status };
                var hosted = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, executor.iface());
                const result = hosted.worker().acknowledgeManyGroup(std.testing.allocator, 7, "docs", req);
                if (status == 400) try std.testing.expectError(error.UnexpectedHttpStatus, result) else try result;
                try std.testing.expectEqual(@as(usize, 1), executor.many);
                try std.testing.expectEqual(@as(usize, if (status == 404 or status == 405) 2 else 0), executor.singles);
            }
            for ([_]u16{ 404, 405, 409 }) |status| {
                var executor: Executor = .{ .status = status, .unknown = true };
                var hosted = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, executor.iface());
                try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, hosted.worker().acknowledgeManyGroup(std.testing.allocator, 7, "docs", req));
                try std.testing.expectEqual(@as(usize, 1), executor.many);
                try std.testing.expectEqual(@as(usize, 0), executor.singles);
            }
            var executor: Executor = .{ .transport_failure = true };
            var hosted = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, executor.iface());
            try std.testing.expectError(error.ConnectionResetByPeer, hosted.worker().acknowledgeManyGroup(std.testing.allocator, 7, "docs", req));
            try std.testing.expectEqual(@as(usize, 0), executor.singles);
        }

        test "distributed txn bulk acknowledgement codec owns bounded participant identities and restore authority" {
            const Harness = struct {
                fn roundTrip(alloc: std.mem.Allocator) !void {
                    const ids = [_][]const u8{ "table2:00000004:docs:8", "table2:00000004:docs:9", "table2:00000004:docs:8" };
                    const encoded = try encodeTxnAcknowledgeManyRequest(alloc, .{ .txn_id = @splat(1), .participants = &ids, .restore_staging_scope = @splat(2), .restore_staging_plan_id = @splat(3) });
                    defer alloc.free(encoded);
                    var decoded = try parseTxnAcknowledgeManyRequest(alloc, encoded);
                    defer freeTxnAcknowledgeManyRequest(alloc, &decoded);
                    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(1))), &decoded.txn_id);
                    try std.testing.expectEqual(@as([32]u8, @splat(2)), decoded.restore_staging_scope.?);
                    try std.testing.expectEqual(@as([16]u8, @splat(3)), decoded.restore_staging_plan_id.?);
                    for (ids, decoded.participants) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
                }
            };
            try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.roundTrip, .{});
            for ([_][]const u8{ "[]", "[1]", "[\"bad\"]" }) |participants| {
                const malformed = try std.fmt.allocPrint(std.testing.allocator, "{{\"txn_id\":\"01010101010101010101010101010101\",\"participants\":{s}}}", .{participants});
                defer std.testing.allocator.free(malformed);
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnAcknowledgeManyRequest(std.testing.allocator, malformed));
            }
            const oversized: [65][]const u8 = @splat("table2:00000004:docs:8");
            try std.testing.expectError(error.InvalidTxnRequest, encodeTxnAcknowledgeManyRequest(std.testing.allocator, .{ .txn_id = @splat(1), .participants = &oversized }));
            try std.testing.expectError(error.InvalidTxnRequest, parseTxnAcknowledgeManyRequest(std.testing.allocator, "{\"txn_id\":\"01010101010101010101010101010101\",\"participants\":[\"table2:00000004:docs:8\"],\"participant\":\"table2:00000004:docs:8\"}"));
        }

        test "transaction attempt budgets follow the borrowed transport clock" {
            var vopr_io = try @import("vopr").vopr_io.VoprIo.init(.{ .monotonic_ns = 7 * std.time.ns_per_s });
            defer vopr_io.deinit();
            const borrow = @import("antfly_runtime_abi").io_abi.Borrow.init(&vopr_io.io());
            const worker = HostedParticipantWorker{
                .catalog = undefined,
                .router = undefined,
                .writes = undefined,
                .executor = .{ .ptr = undefined, .vtable = undefined, .clock_io = borrow },
                .pre_decision_timeout_ms = 4_000,
                .pre_decision_attempt_timeout_ms = 3_000,
            };
            const deadline = try worker.preDecisionDeadlineNs();
            try std.testing.expectEqual(@as(u64, 11 * std.time.ns_per_s), deadline);
            try std.testing.expectEqual(@as(u32, 2_000), (try worker.remainingPreDecisionAttemptBudget(deadline)).server_budget_ms);
            const local = try worker.localPreDecisionContext(deadline);
            try std.testing.expectEqual(@as(?u64, 9 * std.time.ns_per_s), local.deadline_ns);
            try std.testing.expect(local.deadline_io.?.userdata == borrow.userdata);
            vopr_io.monotonic_ns += 2 * std.time.ns_per_s;
            try std.testing.expectEqual(@as(u32, 1_000), (try worker.remainingPreDecisionAttemptBudget(deadline)).server_budget_ms);
            vopr_io.monotonic_ns += 2 * std.time.ns_per_s;
            try std.testing.expectError(error.Timeout, worker.remainingPreDecisionAttemptBudget(deadline));
        }

        test "hosted participant shares one pre-decision budget across begin prepare and fanout" {
            var io = try @import("vopr").vopr_io.VoprIo.init(.{ .monotonic_ns = 7 * std.time.ns_per_s });
            defer io.deinit();
            const borrow = @import("antfly_runtime_abi").io_abi.Borrow.init(&io.io());
            const executor: http_common.RequestExecutor = .{ .ptr = undefined, .vtable = undefined, .clock_io = borrow };
            var worker = HostedParticipantWorker.init(undefined, undefined, undefined, executor);
            const deadline = try worker.preDecisionDeadlineNs();
            io.monotonic_ns += 10 * std.time.ns_per_s;
            try std.testing.expectEqual(deadline, try worker.preDecisionDeadlineNs());
            try std.testing.expect((try worker.localPreDecisionContext(deadline)).deadline_ns.? <= deadline);
            io.monotonic_ns += 11 * std.time.ns_per_s;
            // Both operations must reject before accessing routing or contacts.
            try std.testing.expectError(error.PreDecisionNotProposed, worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{ .txn_id = @splat(1), .begin_timestamp = 1, .participants = &.{} }));
            try std.testing.expectError(error.Timeout, worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{ .txn_id = @splat(1), .req = .{} }));
            var canceled = std.atomic.Value(bool).init(false);
            var bounded = HostedParticipantWorker.initWithPreDecisionBudget(undefined, undefined, undefined, executor, .{ .deadline_ns = @as(u64, @intCast(io.monotonic_ns)) + 3 * std.time.ns_per_s, .io = borrow, .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled) });
            const parent_deadline = try bounded.preDecisionDeadlineNs();
            io.monotonic_ns += std.time.ns_per_s;
            try std.testing.expectEqual(parent_deadline, try bounded.preDecisionDeadlineNs());
            canceled.store(true, .seq_cst);
            try std.testing.expectError(error.Canceled, bounded.worker().beginGroup(std.testing.allocator, 7, "docs", .{ .txn_id = @splat(1), .begin_timestamp = 1, .participants = &.{} }));
            try std.testing.expectError(error.Canceled, bounded.worker().prepareGroup(std.testing.allocator, 7, "docs", .{ .txn_id = @splat(1), .req = .{} }));
        }

        test "distributed txn classifies local and transported visibility outcomes identically" {
            inline for (.{
                error.CommitVisibilityNotSatisfied,
                error.EnrichmentWaitCanceled,
                error.EnrichmentWaitTimeout,
                error.EnrichmentRetryInProgress,
                error.EnrichmentWorkerFailed,
            }) |err| {
                try std.testing.expect(isPostCommitVisibilityError(err));
            }
            try std.testing.expect(!isPostCommitVisibilityError(error.GroupLeaderUnavailable));
            try std.testing.expect(isTerminalVisibilityRepair(error.EnrichmentWorkerFailed));
            try std.testing.expect(!isTerminalVisibilityRepair(error.CommitVisibilityNotSatisfied));
        }

        test "hosted participant attempt deadline preserves the server outcome window" {
            try std.testing.expectEqual(
                contract.max_pre_decision_server_budget_ms,
                internal_batch_forwarding.max_remaining_ms,
            );
            try std.testing.expect(
                HostedParticipantWorker.default_pre_decision_attempt_timeout_ms >
                    contract.max_pre_decision_server_budget_ms,
            );
            try std.testing.expectEqual(
                HostedParticipantWorker.pre_decision_response_reserve_ms,
                HostedParticipantWorker.default_pre_decision_attempt_timeout_ms -
                    contract.max_pre_decision_server_budget_ms,
            );
        }

        test "hosted participant rediscovery retries only pre-decision leader unavailability" {
            const FakeRouter = struct {
                fn iface() table_router.HostedGroupRouter {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .local_node_id = localNodeId,
                            .local_status = localStatus,
                            .group_leader_node_id = groupLeaderNodeId,
                            .group_node_ids = groupNodeIds,
                            .node_status = nodeStatus,
                            .node_base_uri = nodeBaseUri,
                        },
                    };
                }

                fn localNodeId(_: *anyopaque) u64 {
                    return 99;
                }

                fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
                    return .absent;
                }

                fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
                    return 1;
                }

                fn groupNodeIds(_: *anyopaque, alloc: std.mem.Allocator, _: u64, _: table_router.RouteBudget) ![]u64 {
                    return try alloc.dupe(u64, &.{ 1, 2, 3 });
                }

                fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
                    return if (node_id >= 1 and node_id <= 3) .active else .absent;
                }

                fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
                    return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
                }
            };

            const FakeExecutor = struct {
                const FirstOutcome = enum {
                    marked_not_proposed,
                    unmarked_unavailable,
                    not_sent_transport,
                    post_send_transport,
                    not_sent_timeout,
                    post_send_timeout,
                    unknown_timeout,
                    not_sent_local_failure,
                    post_send_local_failure,
                    unknown_group,
                    unmarked_unknown_group,
                    forged_leader_unavailable,
                    forged_unknown_group,
                };

                first_outcome: ?FirstOutcome = .marked_not_proposed,
                first_expected_node_id: u64 = 1,
                fallback_expected_node_id: u64 = 2,
                expect_service_auth: bool = false,
                calls: usize = 0,
                first_body: [4096]u8 = undefined,
                first_body_len: usize = 0,
                first_body_ptr: ?[*]const u8 = null,
                first_timeout_ms: ?u32 = null,

                fn iface(self: *@This()) http_common.RequestExecutor {
                    return .{ .ptr = self, .vtable = &.{ .execute = execute } };
                }

                fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.calls += 1;
                    const timeout_ms = req.timeout_ms orelse return error.TestExpectedBoundedTimeout;
                    try std.testing.expect(timeout_ms > 0 and timeout_ms <= HostedParticipantWorker.default_pre_decision_attempt_timeout_ms);
                    const server_budget_raw = req.header(contract.pre_decision_remaining_ms_header) orelse
                        return error.TestExpectedServerBudget;
                    const server_budget_ms = try std.fmt.parseUnsigned(u32, server_budget_raw, 10);
                    try std.testing.expect(server_budget_ms > contract.pre_decision_server_response_reserve_ms);
                    try std.testing.expect(server_budget_ms <= contract.max_pre_decision_server_budget_ms);
                    try std.testing.expect(server_budget_ms + HostedParticipantWorker.pre_decision_response_reserve_ms <= timeout_ms);
                    var service_auth_headers: usize = 0;
                    for (req.headers) |header| {
                        if (!std.ascii.eqlIgnoreCase(header.name, "X-Antfly-Trusted-Principal")) continue;
                        service_auth_headers += 1;
                        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, header.value, "."));
                    }
                    try std.testing.expectEqual(@as(usize, if (self.expect_service_auth) 1 else 0), service_auth_headers);
                    if (self.calls == 1) {
                        var expected_uri_buf: [64]u8 = undefined;
                        const expected_uri = try std.fmt.bufPrint(&expected_uri_buf, "http://node-{d}/", .{self.first_expected_node_id});
                        try std.testing.expect(std.mem.indexOf(u8, req.uri, expected_uri) != null);
                        try std.testing.expect(req.body.len <= self.first_body.len);
                        @memcpy(self.first_body[0..req.body.len], req.body);
                        self.first_body_len = req.body.len;
                        self.first_body_ptr = req.body.ptr;
                        self.first_timeout_ms = timeout_ms;
                        const first_outcome = self.first_outcome orelse return .{ .status = 200 };
                        return switch (first_outcome) {
                            .marked_not_proposed => try http_route_helpers.textResponseWithHeaders(
                                alloc,
                                503,
                                "group leader unavailable",
                                &.{.{
                                    .name = contract.pre_decision_outcome_header,
                                    .value = contract.pre_decision_not_proposed_v1,
                                }},
                            ),
                            .unmarked_unavailable => try http_route_helpers.textResponse(alloc, 503, "proxy unavailable"),
                            .not_sent_transport => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markNotSent();
                                return error.ConnectionRefused;
                            },
                            .post_send_transport => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markMayHaveBeenSent();
                                return error.ConnectionRefused;
                            },
                            .not_sent_timeout => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markNotSent();
                                return error.Timeout;
                            },
                            .post_send_timeout => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markMayHaveBeenSent();
                                return error.Timeout;
                            },
                            // A conforming executor may leave delivery unknown when
                            // it cannot identify its send boundary precisely.
                            .unknown_timeout => return error.Timeout,
                            .not_sent_local_failure => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markNotSent();
                                return error.TestPreDecisionSetupFailure;
                            },
                            .post_send_local_failure => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markMayHaveBeenSent();
                                return error.TestPreDecisionSetupFailure;
                            },
                            .unknown_group => try http_route_helpers.textResponseWithHeaders(
                                alloc,
                                404,
                                "not found",
                                &.{.{
                                    .name = contract.pre_decision_outcome_header,
                                    .value = contract.pre_decision_not_proposed_v1,
                                }},
                            ),
                            .unmarked_unknown_group => try http_route_helpers.textResponse(alloc, 404, "not found"),
                            .forged_leader_unavailable => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markMayHaveBeenSent();
                                return error.GroupLeaderUnavailable;
                            },
                            .forged_unknown_group => {
                                const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                                tracker.markMayHaveBeenSent();
                                return error.UnknownGroup;
                            },
                        };
                    }
                    try std.testing.expectEqual(@as(usize, 2), self.calls);
                    try std.testing.expect(timeout_ms <= self.first_timeout_ms.?);
                    var expected_uri_buf: [64]u8 = undefined;
                    const expected_uri = try std.fmt.bufPrint(&expected_uri_buf, "http://node-{d}/", .{self.fallback_expected_node_id});
                    try std.testing.expect(std.mem.indexOf(u8, req.uri, expected_uri) != null);
                    try std.testing.expectEqualStrings(self.first_body[0..self.first_body_len], req.body);
                    try std.testing.expect(req.body.ptr == self.first_body_ptr.?);
                    return .{ .status = 200 };
                }
            };

            const AllNotProposedExecutor = struct {
                calls: usize = 0,

                fn iface(self: *@This()) http_common.RequestExecutor {
                    return .{ .ptr = self, .vtable = &.{ .execute = execute } };
                }

                fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.calls += 1;
                    _ = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
                    _ = req.header(contract.pre_decision_remaining_ms_header) orelse
                        return error.TestExpectedServerBudget;
                    return try http_route_helpers.textResponseWithHeaders(
                        alloc,
                        503,
                        "group leader unavailable",
                        &.{.{
                            .name = contract.pre_decision_outcome_header,
                            .value = contract.pre_decision_not_proposed_v1,
                        }},
                    );
                }
            };

            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            var begin_executor = FakeExecutor{};
            var begin_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, begin_executor.iface());
            try begin_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), begin_executor.calls);

            var prepare_executor = FakeExecutor{};
            var prepare_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, prepare_executor.iface());
            try prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{ .writes = &.{.{ .key = "doc:1", .value = "{}" }} },
            });
            try std.testing.expectEqual(@as(usize, 2), prepare_executor.calls);

            var authenticated_executor = FakeExecutor{ .expect_service_auth = true };
            var authenticated_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, authenticated_executor.iface());
            _ = authenticated_worker.withInternalServiceAuth("cluster-secret", "cluster-a");
            try authenticated_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), authenticated_executor.calls);

            var ambiguous_executor = FakeExecutor{ .first_outcome = .unmarked_unavailable };
            var ambiguous_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, ambiguous_executor.iface());
            try std.testing.expectError(error.UnexpectedHttpStatus, ambiguous_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), ambiguous_executor.calls);

            var not_sent_executor = FakeExecutor{ .first_outcome = .not_sent_transport };
            var not_sent_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, not_sent_executor.iface());
            try not_sent_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), not_sent_executor.calls);

            var post_send_executor = FakeExecutor{ .first_outcome = .post_send_transport };
            var post_send_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, post_send_executor.iface());
            try std.testing.expectError(error.ConnectionRefused, post_send_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), post_send_executor.calls);

            var not_sent_timeout_executor = FakeExecutor{ .first_outcome = .not_sent_timeout };
            var not_sent_timeout_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, not_sent_timeout_executor.iface());
            try not_sent_timeout_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), not_sent_timeout_executor.calls);

            var post_send_timeout_executor = FakeExecutor{ .first_outcome = .post_send_timeout };
            var post_send_timeout_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, post_send_timeout_executor.iface());
            try std.testing.expectError(error.Timeout, post_send_timeout_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), post_send_timeout_executor.calls);

            var unknown_timeout_executor = FakeExecutor{ .first_outcome = .unknown_timeout };
            var unknown_timeout_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, unknown_timeout_executor.iface());
            try std.testing.expectError(error.Timeout, unknown_timeout_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), unknown_timeout_executor.calls);

            var not_sent_local_failure_executor = FakeExecutor{ .first_outcome = .not_sent_local_failure };
            var not_sent_local_failure_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, not_sent_local_failure_executor.iface());
            try not_sent_local_failure_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), not_sent_local_failure_executor.calls);

            var post_send_local_failure_executor = FakeExecutor{ .first_outcome = .post_send_local_failure };
            var post_send_local_failure_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, post_send_local_failure_executor.iface());
            try std.testing.expectError(error.TestPreDecisionSetupFailure, post_send_local_failure_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), post_send_local_failure_executor.calls);

            var forged_leader_executor = FakeExecutor{ .first_outcome = .forged_leader_unavailable };
            var forged_leader_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, forged_leader_executor.iface());
            try std.testing.expectError(error.GroupLeaderUnavailable, forged_leader_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), forged_leader_executor.calls);

            var forged_missing_executor = FakeExecutor{ .first_outcome = .forged_unknown_group };
            var forged_missing_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, forged_missing_executor.iface());
            try std.testing.expectError(error.UnknownGroup, forged_missing_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), forged_missing_executor.calls);

            var missing_executor = FakeExecutor{ .first_outcome = .unknown_group };
            var missing_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, missing_executor.iface());
            try missing_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), missing_executor.calls);

            var unmarked_missing_executor = FakeExecutor{ .first_outcome = .unmarked_unknown_group };
            var unmarked_missing_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, unmarked_missing_executor.iface());
            try std.testing.expectError(error.UnexpectedHttpStatus, unmarked_missing_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), unmarked_missing_executor.calls);

            var exhausted_begin_executor = AllNotProposedExecutor{};
            var exhausted_begin_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, exhausted_begin_executor.iface());
            try std.testing.expectError(error.PreDecisionNotProposed, exhausted_begin_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 3), exhausted_begin_executor.calls);

            var expired_candidates_executor = AllNotProposedExecutor{};
            var expired_candidates_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, expired_candidates_executor.iface());
            try std.testing.expectError(error.PreDecisionNotProposed, expired_candidates_worker.beginGroupFromCandidates(
                std.testing.allocator,
                7,
                "docs",
                .{
                    .txn_id = txn_id,
                    .begin_timestamp = 42,
                    .participants = &.{"table2:docs:group:7"},
                },
                1,
                null,
                0,
            ));
            try std.testing.expectEqual(@as(usize, 0), expired_candidates_executor.calls);

            const CandidateSetupRouter = struct {
                fail_group_nodes: bool = false,
                fail_node_uri: ?u64 = null,

                fn iface(self: *@This()) table_router.HostedGroupRouter {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .local_node_id = localNodeId,
                            .local_status = localStatus,
                            .group_leader_node_id = groupLeaderNodeId,
                            .group_node_ids = groupNodeIds,
                            .node_status = nodeStatus,
                            .node_base_uri = nodeBaseUri,
                        },
                    };
                }

                fn localNodeId(_: *anyopaque) u64 {
                    return 99;
                }

                fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
                    return .absent;
                }

                fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
                    return 1;
                }

                fn groupNodeIds(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64, _: table_router.RouteBudget) ![]u64 {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (self.fail_group_nodes) return error.TestCandidateDiscoveryFailure;
                    return try alloc.dupe(u64, &.{ 1, 2, 3 });
                }

                fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
                    return if (node_id >= 1 and node_id <= 3) .active else .absent;
                }

                fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (self.fail_node_uri == node_id) return error.TestCandidateRouteFailure;
                    return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
                }
            };

            var discovery_router = CandidateSetupRouter{ .fail_group_nodes = true };
            var discovery_executor = FakeExecutor{};
            var discovery_worker = HostedParticipantWorker.init(undefined, discovery_router.iface(), undefined, discovery_executor.iface());
            try std.testing.expectError(error.PreDecisionNotProposed, discovery_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 1), discovery_executor.calls);

            var initial_route_router = CandidateSetupRouter{ .fail_node_uri = 1 };
            var initial_route_executor = FakeExecutor{};
            var initial_route_worker = HostedParticipantWorker.init(undefined, initial_route_router.iface(), undefined, initial_route_executor.iface());
            try std.testing.expectError(error.PreDecisionNotProposed, initial_route_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 0), initial_route_executor.calls);

            var initial_encoding_executor = FakeExecutor{};
            var initial_encoding_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, initial_encoding_executor.iface());
            var initial_encoding_failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
            try std.testing.expectError(error.PreDecisionNotProposed, initial_encoding_worker.worker().beginGroup(initial_encoding_failing.allocator(), 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 0), initial_encoding_executor.calls);

            var candidate_route_router = CandidateSetupRouter{ .fail_node_uri = 2 };
            var candidate_route_executor = FakeExecutor{ .fallback_expected_node_id = 3 };
            var candidate_route_worker = HostedParticipantWorker.init(undefined, candidate_route_router.iface(), undefined, candidate_route_executor.iface());
            try candidate_route_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 2), candidate_route_executor.calls);

            var encoding_failure_executor = AllNotProposedExecutor{};
            var encoding_failure_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, encoding_failure_executor.iface());
            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
            try std.testing.expectError(error.PreDecisionNotProposed, encoding_failure_worker.beginGroupFromCandidates(
                failing.allocator(),
                7,
                "docs",
                .{
                    .txn_id = txn_id,
                    .begin_timestamp = 42,
                    .participants = &.{"table2:docs:group:7"},
                },
                1,
                null,
                std.math.maxInt(u64),
            ));
            try std.testing.expectEqual(@as(usize, 0), encoding_failure_executor.calls);

            var exhausted_prepare_executor = AllNotProposedExecutor{};
            var exhausted_prepare_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, exhausted_prepare_executor.iface());
            try std.testing.expectError(error.GroupLeaderUnavailable, exhausted_prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{ .writes = &.{.{ .key = "doc:1", .value = "{}" }} },
            }));
            try std.testing.expectEqual(@as(usize, 3), exhausted_prepare_executor.calls);

            const LocalMissRouter = struct {
                fn iface() table_router.HostedGroupRouter {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .local_node_id = localNodeId,
                            .local_status = localStatus,
                            .group_leader_node_id = groupLeaderNodeId,
                            .group_node_ids = groupNodeIds,
                            .node_status = nodeStatus,
                            .node_base_uri = nodeBaseUri,
                        },
                    };
                }

                fn localNodeId(_: *anyopaque) u64 {
                    return 99;
                }

                fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
                    return .active;
                }

                fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
                    return 99;
                }

                fn groupNodeIds(_: *anyopaque, alloc: std.mem.Allocator, _: u64, _: table_router.RouteBudget) ![]u64 {
                    return try alloc.dupe(u64, &.{ 99, 2 });
                }

                fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
                    return if (node_id == 2) .active else .absent;
                }

                fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
                    return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
                }
            };

            const NullWrites = struct {
                fn source() table_writes.TableWriteSource {
                    return .{ .ptr = undefined, .vtable = &.{
                        .batch = batch,
                        .txn_begin_group_local = begin,
                        .txn_prepare_group_local = prepare,
                        .txn_begin_group_local_with_pre_decision_context = beginWithContext,
                        .txn_prepare_group_local_with_pre_decision_context = prepareWithContext,
                    } };
                }

                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) anyerror!?void {
                    return error.TestExpectedContextAwareBegin;
                }

                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
                    return error.TestExpectedContextAwarePrepare;
                }
                fn beginWithContext(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, context: PreDecisionContext) anyerror!?void {
                    try std.testing.expect(context.deadline_ns != null);
                    try context.cancellation.check();
                    return null;
                }
                fn prepareWithContext(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest, context: PreDecisionContext) anyerror!?void {
                    try std.testing.expect(context.deadline_ns != null);
                    try context.cancellation.check();
                    return null;
                }
            };

            const DeadlineWrites = struct {
                fn source() table_writes.TableWriteSource {
                    return .{ .ptr = undefined, .vtable = &.{
                        .batch = batch,
                        .txn_begin_group_local = begin,
                        .txn_prepare_group_local = prepare,
                        .txn_begin_group_local_with_pre_decision_context = beginWithContext,
                        .txn_prepare_group_local_with_pre_decision_context = prepareWithContext,
                    } };
                }

                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) anyerror!?void {
                    return error.TestExpectedContextAwareBegin;
                }

                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
                    return error.TestExpectedContextAwarePrepare;
                }

                fn beginWithContext(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, _: PreDecisionContext) anyerror!?void {
                    return error.PreDecisionDeadlineExceeded;
                }

                fn prepareWithContext(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest, _: PreDecisionContext) anyerror!?void {
                    return error.PreDecisionDeadlineExceeded;
                }
            };

            const AmbiguousContextWrites = struct {
                failure: anyerror,

                fn source(self: *@This()) table_writes.TableWriteSource {
                    return .{ .ptr = self, .vtable = &.{
                        .batch = batch,
                        .txn_begin_group_local_with_pre_decision_context = beginWithContext,
                        .txn_prepare_group_local_with_pre_decision_context = prepareWithContext,
                    } };
                }

                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }

                fn beginWithContext(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, _: PreDecisionContext) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    return self.failure;
                }

                fn prepareWithContext(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest, _: PreDecisionContext) anyerror!?void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    return self.failure;
                }
            };

            const LegacyDeadlineWrites = struct {
                fn source() table_writes.TableWriteSource {
                    return .{ .ptr = undefined, .vtable = &.{
                        .batch = batch,
                        .txn_begin_group_local = begin,
                        .txn_prepare_group_local = prepare,
                    } };
                }

                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) anyerror!?void {
                    return error.DeadlineExceeded;
                }

                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
                    return error.DeadlineExceeded;
                }
            };

            var local_miss_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var local_miss_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), NullWrites.source(), local_miss_executor.iface());
            try local_miss_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 1), local_miss_executor.calls);

            var local_prepare_miss_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var local_prepare_miss_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), NullWrites.source(), local_prepare_miss_executor.iface());
            try local_prepare_miss_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{ .writes = &.{.{ .key = "doc:1", .value = "{}" }} },
            });
            try std.testing.expectEqual(@as(usize, 1), local_prepare_miss_executor.calls);

            var local_deadline_begin_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var local_deadline_begin_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), DeadlineWrites.source(), local_deadline_begin_executor.iface());
            try local_deadline_begin_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            });
            try std.testing.expectEqual(@as(usize, 1), local_deadline_begin_executor.calls);

            var local_deadline_prepare_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var local_deadline_prepare_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), DeadlineWrites.source(), local_deadline_prepare_executor.iface());
            try local_deadline_prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{ .writes = &.{.{ .key = "doc:1", .value = "{}" }} },
            });
            try std.testing.expectEqual(@as(usize, 1), local_deadline_prepare_executor.calls);

            var ambiguous_context_writes = AmbiguousContextWrites{ .failure = error.DeadlineExceeded };
            var ambiguous_context_begin_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var ambiguous_context_begin_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), ambiguous_context_writes.source(), ambiguous_context_begin_executor.iface());
            try std.testing.expectError(error.DeadlineExceeded, ambiguous_context_begin_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 0), ambiguous_context_begin_executor.calls);

            ambiguous_context_writes.failure = error.Timeout;
            var ambiguous_context_prepare_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var ambiguous_context_prepare_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), ambiguous_context_writes.source(), ambiguous_context_prepare_executor.iface());
            try std.testing.expectError(error.Timeout, ambiguous_context_prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{ .writes = &.{.{ .key = "doc:1", .value = "{}" }} },
            }));
            try std.testing.expectEqual(@as(usize, 0), ambiguous_context_prepare_executor.calls);

            var legacy_deadline_begin_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var legacy_deadline_begin_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), LegacyDeadlineWrites.source(), legacy_deadline_begin_executor.iface());
            try std.testing.expectError(error.UnsupportedOperation, legacy_deadline_begin_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 0), legacy_deadline_begin_executor.calls);

            var legacy_deadline_prepare_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var legacy_deadline_prepare_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), LegacyDeadlineWrites.source(), legacy_deadline_prepare_executor.iface());
            try std.testing.expectError(error.UnsupportedOperation, legacy_deadline_prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{ .writes = &.{.{ .key = "doc:1", .value = "{}" }} },
            }));
            try std.testing.expectEqual(@as(usize, 0), legacy_deadline_prepare_executor.calls);

            var expired_executor = FakeExecutor{};
            var expired_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, expired_executor.iface());
            expired_worker.pre_decision_timeout_ms = 0;
            try std.testing.expectError(error.PreDecisionNotProposed, expired_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 0), expired_executor.calls);

            var expired_prepare_executor = FakeExecutor{};
            var expired_prepare_worker = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, expired_prepare_executor.iface());
            expired_prepare_worker.pre_decision_timeout_ms = 0;
            try std.testing.expectError(error.Timeout, expired_prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .req = .{},
            }));
            try std.testing.expectEqual(@as(usize, 0), expired_prepare_executor.calls);
        }

        test "distributed txn retries an ambiguous coordinator decision under the same id" {
            const Recorder = struct {
                resolve_calls: usize = 0,
                status_calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                        .resolve_group_until = resolveUntil,
                        .status_group_until = statusUntil,
                    } };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.resolve_calls += 1;
                    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
                    try std.testing.expectEqual(@as(u64, 10_001), req.commit_version);
                    try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
                }
                fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.status_calls += 1;
                    return error.InjectedStatusFailure;
                }
                fn resolveUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return try resolve(ptr, alloc, group_id, table_name, req);
                }
                fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return try status(ptr, alloc, group_id, table_name, txn_id);
                }
            };

            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const status = try resolveCoordinatorDecisionAfterFailure(
                std.testing.allocator,
                recorder.worker(),
                .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 9 },
                txn_id,
                10_001,
                // Every ambiguous coordinator retry must preserve the durable barrier
                // selected for the original decision submission.
                .write,
                error.InjectedResolveFailure,
                .none,
            );
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, status);
            try std.testing.expectEqual(@as(usize, 1), recorder.status_calls);
            try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
        }

        test "distributed txn bounds unresolved coordinator decision retries" {
            const Recorder = struct {
                resolve_calls: usize = 0,
                status_calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                        .resolve_group_until = resolveUntil,
                        .status_group_until = statusUntil,
                    } };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.resolve_calls += 1;
                    return error.InjectedResolveFailure;
                }
                fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.status_calls += 1;
                    return error.InjectedStatusFailure;
                }
                fn resolveUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return try resolve(ptr, alloc, group_id, table_name, req);
                }
                fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return try status(ptr, alloc, group_id, table_name, txn_id);
                }
            };

            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("ffeeddccbbaa99887766554433221100");
            try std.testing.expectError(error.CommitDecisionUnknown, resolveCoordinatorDecisionAfterFailureUntil(
                std.testing.allocator,
                recorder.worker(),
                .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 9 },
                txn_id,
                10_001,
                .write,
                error.InjectedResolveFailure,
                platform_time.monotonicNs() + 10 * std.time.ns_per_ms,
            ));
            try std.testing.expect(recorder.status_calls > 0);
            try std.testing.expect(recorder.resolve_calls > 0);
        }

        test "distributed txn propagates one absolute deadline through ambiguous decision recovery" {
            const Recorder = struct {
                expected_deadline_ns: u64,
                status_until_calls: usize = 0,
                resolve_until_calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                        .resolve_group_until = resolveUntil,
                        .status_group_until = statusUntil,
                    } };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
                    return error.LegacyResolveMustNotRun;
                }
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return error.LegacyStatusMustNotRun;
                }
                fn statusUntil(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.status_until_calls += 1;
                    try std.testing.expectEqual(self.expected_deadline_ns, deadline_ns);
                    return error.InjectedStatusFailure;
                }
                fn resolveUntil(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.resolve_until_calls += 1;
                    try std.testing.expectEqual(self.expected_deadline_ns, deadline_ns);
                    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
                }
            };

            const deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s;
            var recorder = Recorder{ .expected_deadline_ns = deadline_ns };
            const status = try resolveCoordinatorDecisionAfterFailureUntil(
                std.testing.allocator,
                recorder.worker(),
                .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 9 },
                try parseTxnIdHex("00112233445566778899aabbccddeeff"),
                10_001,
                .write,
                error.InjectedResolveFailure,
                deadline_ns,
            );
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, status);
            try std.testing.expectEqual(@as(usize, 1), recorder.status_until_calls);
            try std.testing.expectEqual(@as(usize, 1), recorder.resolve_until_calls);

            const LocalProbe = struct {
                expected_deadline_ns: u64,
                calls: usize = 0,

                fn source(self: *@This()) table_writes.TableWriteSource {
                    return .{ .ptr = self, .vtable = &.{
                        .batch = batch,
                        .txn_status_group_linearizable_until = statusUntil,
                    } };
                }

                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
                    return error.TestUnexpectedBatch;
                }

                fn statusUntil(
                    ptr: *anyopaque,
                    _: std.mem.Allocator,
                    _: u64,
                    _: []const u8,
                    _: db_mod.types.TxnId,
                    observed_deadline_ns: u64,
                ) !?db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.calls += 1;
                    try std.testing.expectEqual(self.expected_deadline_ns, observed_deadline_ns);
                    return .committed;
                }
            };
            var local_probe = LocalProbe{ .expected_deadline_ns = deadline_ns };
            var local_worker = LocalTableWriteParticipantWorker.init(local_probe.source());
            try std.testing.expectEqual(
                db_mod.types.TxnStatus.committed,
                try local_worker.worker().statusGroupUntil(
                    std.testing.allocator,
                    7001,
                    "docs",
                    try parseTxnIdHex("00112233445566778899aabbccddeeff"),
                    deadline_ns,
                ),
            );
            try std.testing.expectEqual(@as(usize, 1), local_probe.calls);
        }

        test "distributed txn abort fanout preserves durable ordering contact evidence and bounded recovery" {
            const vopr = @import("vopr");
            const alloc = std.testing.allocator;
            const Mode = enum { normal, lost_coordinator_reply, unconfirmed_coordinator, contact_mask, retained };
            for ([_]Mode{ .normal, .lost_coordinator_reply, .unconfirmed_coordinator, .contact_mask, .retained }) |mode| {
                var sim = try vopr.vopr_io.VoprIo.init(.{ .required = .of(&.{ .clock_read, .sleep, .task_scheduling, .synchronization }) });
                defer sim.deinit();
                const participants = [_]ParticipantTxn{
                    .{ .table_name = "docs", .group_id = 1001, .topology_epoch = 1 },
                    .{ .table_name = "docs", .group_id = 1002, .topology_epoch = 1 },
                    .{ .table_name = "docs", .group_id = 1003, .topology_epoch = 1 },
                    .{ .table_name = "docs", .group_id = 1004, .topology_epoch = 1 },
                    .{ .table_name = "docs", .group_id = 1005, .topology_epoch = 1 },
                };
                const ids = [_][]const u8{ "1", "2", "3", "4", "5" };
                var slots: [5]ParticipantFanoutSlot = @splat(.{ .may_have_transaction_state = true });
                slots[1].may_have_transaction_state = false;
                const Recorder = struct {
                    io: std.Io,
                    mode: Mode,
                    participants: []const ParticipantTxn,
                    ids: []const []const u8,
                    slots: []ParticipantFanoutSlot,
                    durable_coordinator: bool = false,
                    active: usize = 0,
                    peak: usize = 0,
                    overlap: std.Io.Event = .unset,
                    invoked: [5]bool = @splat(false),
                    resolved: [5]bool = @splat(false),
                    acknowledged: [5]bool = @splat(false),
                    failure: ?anyerror = null,
                    fn worker(self: *@This()) ParticipantWorker {
                        return .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status, .acknowledge_group = acknowledge } };
                    }
                    fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                    fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                    fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, req: TxnResolveRequest) !void {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, req.status);
                        try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
                        const i: usize = @intCast(group - 1001);
                        self.invoked[i] = true;
                        if (i == 0) {
                            try self.io.sleep(.fromMilliseconds(5), .awake);
                            if (self.mode == .unconfirmed_coordinator) return error.Timeout;
                            self.durable_coordinator = true;
                            if (self.mode == .lost_coordinator_reply) return error.Timeout;
                            return;
                        }
                        try std.testing.expect(self.durable_coordinator);
                        self.active += 1;
                        defer self.active -= 1;
                        self.peak = @max(self.peak, self.active);
                        // Prove overlap through a handshake, not the scheduler's
                        // choice between a new task and an advancing timer.
                        // Sequential fanout now deadlocks this test explicitly.
                        // The first wave may contain a single contacted owner
                        // in contact-mask mode; the final wave always has two.
                        if (i >= 3) {
                            if (self.active == 2) self.overlap.set(self.io);
                            try self.overlap.wait(self.io);
                        }
                        try self.io.sleep(.fromMilliseconds(2), .awake);
                        if (i == 2) {
                            if (self.mode == .contact_mask) {
                                self.resolved[i] = true; // Definitely no participant state.
                                return error.TxnNotFound;
                            }
                            return error.LeaderUnavailable;
                        }
                        self.resolved[i] = true;
                    }
                    fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        return if (self.durable_coordinator) .aborted else .pending;
                    }
                    fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, req: TxnAcknowledgeRequest) !void {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        try std.testing.expectEqual(@as(u64, 1001), group);
                        try std.testing.expect(self.durable_coordinator);
                        const i: usize = req.participant[0] - '1';
                        if (!(self.mode == .contact_mask and i == 1)) try std.testing.expect(self.resolved[i]);
                        if (i == 4) return error.TestAckLost;
                        self.acknowledged[i] = true;
                    }
                    fn run(self: *@This()) void {
                        const txn_id = parseTxnIdHex("00112233445566778899aabbccddeeff") catch unreachable;
                        const options: ExecuteOptions = .{ .fanout_io = self.io, .max_parallel_participants = 2 };
                        if (self.mode == .contact_mask or self.mode == .retained) {
                            abortParticipantsWithContactMask(alloc, self.worker(), txn_id, 77, self.participants, self.ids, self.slots, self.mode == .retained, options) catch |err| {
                                self.failure = err;
                            };
                        } else {
                            abortParticipantsWithOptions(alloc, self.worker(), txn_id, 77, self.participants, self.ids, self.participants.len, self.slots, options) catch |err| {
                                self.failure = err;
                            };
                        }
                    }
                };
                var recorder = Recorder{ .io = sim.io(), .mode = mode, .participants = &participants, .ids = &ids, .slots = &slots };
                _ = sim.io().async(Recorder.run, .{&recorder});
                var enabled: vopr.transition.List = .{};
                defer enabled.deinit(alloc);
                var events: vopr.event.Sink = .{};
                defer events.deinit(alloc);
                while (!sim.scheduler().quiescent()) {
                    enabled.items.clearRetainingCapacity();
                    try sim.scheduler().enumerateReady(&enabled, alloc);
                    try enabled.canonicalize();
                    if (enabled.items.items.len == 0) return error.VoprAbortFanoutDeadlock;
                    try sim.scheduler().executeReady(enabled.items.items[0].id, &events, alloc);
                }
                if (mode == .unconfirmed_coordinator) {
                    try std.testing.expectEqual(@as(?anyerror, error.AbortDecisionNotDurable), recorder.failure);
                    try std.testing.expectEqual(@as(usize, 0), recorder.peak);
                    for (recorder.invoked[1..]) |invoked| try std.testing.expect(!invoked);
                } else {
                    try std.testing.expectEqual(@as(?anyerror, null), recorder.failure);
                    try std.testing.expectEqual(@as(usize, 2), recorder.peak);
                    try std.testing.expectEqual(mode != .contact_mask, recorder.invoked[1]);
                    try std.testing.expectEqual(mode == .contact_mask, recorder.acknowledged[2]);
                    try std.testing.expect(recorder.acknowledged[1] and recorder.acknowledged[3]);
                    try std.testing.expect(!recorder.acknowledged[4]);
                    try std.testing.expectEqual(@as(?anyerror, error.TestAckLost), slots[4].acknowledgement_err);
                }
                try sim.ensureNoCapabilityViolation();
            }
        }

        test "distributed txn participant team bounds CPU and child allocations" {
            const runtime = @import("antfly_runtime_abi");
            const execution = @import("runnable_owner_execution.zig");
            const Recorder = struct {
                owner: *execution.State,
                minimum_slots: usize,
                active: std.atomic.Value(usize) = .init(0),
                peak: std.atomic.Value(usize) = .init(0),
                calls: std.atomic.Value(usize) = .init(0),
                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
                fn prepare(raw: *anyopaque, allocator: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expectEqual(self.minimum_slots, self.owner.owner.scheduler.stats().runnable_slots);
                    _ = self.calls.fetchAdd(1, .monotonic);
                    const current = self.active.fetchAdd(1, .acq_rel) + 1;
                    defer _ = self.active.fetchSub(1, .acq_rel);
                    var peak = self.peak.load(.monotonic);
                    while (current > peak) peak = self.peak.cmpxchgWeak(peak, current, .monotonic, .monotonic) orelse break;
                    const allocation = try allocator.alloc(u8, 32);
                    defer allocator.free(allocation);
                    sleepNs(10 * std.time.ns_per_ms);
                }
                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status } };
                }
                fn now() u64 {
                    return 0;
                }
            };
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
            defer io_impl.deinit();
            const io = io_impl.io();
            var owner: execution.State = .{ .owner_alloc = std.testing.allocator, .owner = try @import("antfly_local_sources").common_request_admission.runnable_scheduler.NodeOwner.init(.{ .slots = 4, .max_operations = 8 }), .native_now_ns = Recorder.now };
            defer owner.owner.tryDeinit() catch @panic("owner leaked");
            const port = owner.port();
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            var handle: ?*anyopaque = null;
            try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.write), .retained_bytes = 71, .out_handle = &handle }).isOk());
            defer port.finish(handle.?);
            const retained = owner.owner.scheduler.stats().retained_bytes;
            const participants = [_]ParticipantTxn{
                .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7002, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7003, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7004, .topology_epoch = 1 },
            };
            var slots: [participants.len]ParticipantFanoutSlot = undefined;
            var team: runtime.runnable_abi.Team = .{ .borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .write), .maximum_slots = 3, .allocator = std.testing.allocator };
            var options: ExecuteOptions = .{ .fanout_io = io, .max_parallel_participants = 8, .team = &team };
            try reserveParticipantTeam(&options, participants.len);
            var recorder: Recorder = .{ .owner = &owner, .minimum_slots = 3 };
            runPrepareFanout(recorder.worker(), @splat(1), &participants, &slots, options);
            try std.testing.expectEqual(@as(usize, 4), recorder.calls.load(.monotonic));
            try std.testing.expectEqual(@as(usize, 2), recorder.peak.load(.monotonic));
            for (slots) |slot| try std.testing.expectEqual(@as(?anyerror, null), slot.err);
            try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
            try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
            team.finish();
            try port.resizeTeam(handle.?, 1);
            // A zero-capacity owned child allocator must fail rather than use
            // the previous page-allocator fallback. Single-slot teams execute
            // serially on the parent, with no unscheduled task submission.
            var storage: [0]u8 = .{};
            var fixed = std.heap.FixedBufferAllocator.init(&storage);
            var serial: runtime.runnable_abi.Team = .{ .borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .write), .maximum_slots = 1, .allocator = fixed.allocator() };
            options = .{ .fanout_io = io, .max_parallel_participants = 8, .team = &serial };
            try reserveParticipantTeam(&options, participants.len);
            try std.testing.expect(options.fanout_io == null);
            recorder.minimum_slots = 1;
            runPrepareFanout(recorder.worker(), @splat(2), &participants, &slots, options);
            try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), slots[0].err);
            try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
            serial.finish();
        }

        test "distributed txn participant fanout is bounded and concurrent" {
            const Recorder = struct {
                active: std.atomic.Value(usize) = .init(0),
                peak: std.atomic.Value(usize) = .init(0),
                calls: std.atomic.Value(usize) = .init(0),

                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                    } };
                }

                fn updatePeak(self: *@This(), current: usize) void {
                    var observed = self.peak.load(.monotonic);
                    while (current > observed) {
                        observed = self.peak.cmpxchgWeak(observed, current, .monotonic, .monotonic) orelse return;
                    }
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    const current = self.active.fetchAdd(1, .acq_rel) + 1;
                    self.updatePeak(current);
                    _ = self.calls.fetchAdd(1, .monotonic);
                    sleepNs(10 * std.time.ns_per_ms);
                    _ = self.active.fetchSub(1, .acq_rel);
                }
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
            };

            var recorder = Recorder{};
            const participants = [_]ParticipantTxn{
                .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7002, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7003, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7004, .topology_epoch = 1 },
            };
            var slots: [participants.len]ParticipantFanoutSlot = undefined;
            var io_impl = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
            defer io_impl.deinit();

            runPrepareFanout(
                recorder.worker(),
                try parseTxnIdHex("00112233445566778899aabbccddeeff"),
                &participants,
                &slots,
                .{ .fanout_io = io_impl.io(), .max_parallel_participants = 2 },
            );
            try std.testing.expectEqual(@as(usize, participants.len), recorder.calls.load(.acquire));
            try std.testing.expectEqual(@as(usize, 2), recorder.peak.load(.acquire));
            try std.testing.expect(firstFanoutError(&slots) == null);
        }

        test "distributed txn participant ids preserve embedded group markers" {
            const alloc = std.testing.allocator;

            const table_name = "docs:group:shadow";
            const participant = try participantIdForGroup(alloc, table_name, 42);
            defer alloc.free(participant);

            const parsed = parseParticipantRef(participant) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings(table_name, parsed.table_name);
            try std.testing.expectEqual(@as(u64, 42), parsed.group_id);

            const legacy = parseParticipantRef("table:docs:group:42") orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings("docs", legacy.table_name);
            try std.testing.expectEqual(@as(u64, 42), legacy.group_id);
        }

        test "txn prepare parser preserves raw JSON object values" {
            const alloc = std.testing.allocator;
            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const body = try encodeTxnPrepareRequest(alloc, .{
                .txn_id = txn_id,
                .topology_epoch = 7,
                .req = .{
                    .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
                },
            });
            defer alloc.free(body);

            var parsed = try parseTxnPrepareRequest(alloc, body);
            defer freeTxnPrepareRequest(alloc, &parsed);

            try std.testing.expectEqual(@as(usize, 1), parsed.req.writes.len);
            try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", parsed.req.writes[0].value);
        }

        test "txn prepare parser round-trips transforms" {
            const alloc = std.testing.allocator;
            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const body = try encodeTxnPrepareRequest(alloc, .{
                .txn_id = txn_id,
                .topology_epoch = 7,
                .req = .{
                    .transforms = &.{.{
                        .key = "doc:a",
                        .operations = &.{
                            .{ .op = .set, .path = "status", .value_json = "\"updated\"" },
                            .{ .op = .min, .path = "priority", .value_json = "2" },
                            .{ .op = .max, .path = "version", .value_json = "3" },
                        },
                        .upsert = true,
                    }},
                },
            });
            defer alloc.free(body);

            var parsed = try parseTxnPrepareRequest(alloc, body);
            defer freeTxnPrepareRequest(alloc, &parsed);

            try std.testing.expectEqual(@as(usize, 1), parsed.req.transforms.len);
            try std.testing.expect(parsed.req.transforms[0].upsert);
            try std.testing.expectEqualStrings("doc:a", parsed.req.transforms[0].key);
            try std.testing.expectEqual(db_mod.types.TransformOpType.set, parsed.req.transforms[0].operations[0].op);
            try std.testing.expectEqualStrings("\"updated\"", parsed.req.transforms[0].operations[0].value_json.?);
            try std.testing.expectEqual(db_mod.types.TransformOpType.min, parsed.req.transforms[0].operations[1].op);
            try std.testing.expectEqualStrings("2", parsed.req.transforms[0].operations[1].value_json.?);
        }

        test "transaction request parsers release owned prefixes after malformed input" {
            const alloc = std.testing.allocator;
            const malformed_begin_requests = [_][]const u8{
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":1,"topology_epoch":2,"participants":["table2:4:docs:group:7",7]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":1,"topology_epoch":2,"retain_terminal":"invalid","participants":["table2:4:docs:group:7"]}
                ,
            };
            for (malformed_begin_requests) |body| {
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnBeginRequest(alloc, body));
            }

            const malformed_prepare_requests = [_][]const u8{
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":2,"writes":[{"key":"doc:a","value":{"title":"alpha"}},{"key":"doc:b"}],"deletes":[],"transforms":[],"predicates":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":2,"writes":[{"key":"doc:a","value":{"title":"alpha"}}],"deletes":["doc:b",7],"transforms":[],"predicates":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":2,"writes":[{"key":"doc:a","value":{"title":"alpha"}}],"deletes":["doc:b"],"transforms":[{"key":"doc:c","operations":[{"op":"$set","path":"status","value":"ready"}],"upsert":"invalid"}],"predicates":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":2,"writes":[{"key":"doc:a","value":{"title":"alpha"}}],"deletes":["doc:b"],"transforms":[{"key":"doc:c","operations":[{"op":"$set","path":"status","value":"ready"}],"upsert":true}],"predicates":[{"key":"doc:d","expected_version":1},7]}
                ,
            };
            for (malformed_prepare_requests) |body| {
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnPrepareRequest(alloc, body));
            }
        }

        test "transaction request parsers reject invalid unsigned integers and accept legacy epochs" {
            const alloc = std.testing.allocator;
            const malformed_begin_requests = [_][]const u8{
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":2,"participants":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":"1","topology_epoch":2,"participants":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":-1,"topology_epoch":2,"participants":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":1,"topology_epoch":-1,"participants":[]}
                ,
            };
            for (malformed_begin_requests) |body| {
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnBeginRequest(alloc, body));
            }

            const malformed_prepare_requests = [_][]const u8{
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":"2","writes":[],"deletes":[],"transforms":[],"predicates":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":-1,"writes":[],"deletes":[],"transforms":[],"predicates":[]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","writes":[],"deletes":[],"transforms":[],"predicates":[{"key":"doc:a"}]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","writes":[],"deletes":[],"transforms":[],"predicates":[{"key":"doc:a","expected_version":"1"}]}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","writes":[],"deletes":[],"transforms":[],"predicates":[{"key":"doc:a","expected_version":-1}]}
                ,
            };
            for (malformed_prepare_requests) |body| {
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnPrepareRequest(alloc, body));
            }

            const malformed_resolve_requests = [_][]const u8{
                \\{"txn_id":"00112233445566778899aabbccddeeff","status":"committed"}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","status":"committed","commit_version":"1"}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","status":"committed","commit_version":-1}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","status":"committed","commit_version":1,"topology_epoch":"2"}
                ,
                \\{"txn_id":"00112233445566778899aabbccddeeff","status":"committed","commit_version":1,"topology_epoch":-1}
                ,
            };
            for (malformed_resolve_requests) |body| {
                try std.testing.expectError(error.InvalidTxnRequest, parseTxnResolveRequest(alloc, body));
            }

            var legacy_begin = try parseTxnBeginRequest(
                alloc,
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":1,"participants":[]}
                ,
            );
            defer freeTxnBeginRequest(alloc, &legacy_begin);
            try std.testing.expectEqual(@as(u64, 0), legacy_begin.topology_epoch);

            var legacy_prepare = try parseTxnPrepareRequest(
                alloc,
                \\{"txn_id":"00112233445566778899aabbccddeeff","writes":[],"deletes":[],"transforms":[],"predicates":[]}
                ,
            );
            defer freeTxnPrepareRequest(alloc, &legacy_prepare);
            try std.testing.expectEqual(@as(u64, 0), legacy_prepare.topology_epoch);

            var max_begin = try parseTxnBeginRequest(
                alloc,
                \\{"txn_id":"00112233445566778899aabbccddeeff","begin_timestamp":18446744073709551615,"topology_epoch":18446744073709551615,"participants":[]}
                ,
            );
            defer freeTxnBeginRequest(alloc, &max_begin);
            try std.testing.expectEqual(std.math.maxInt(u64), max_begin.begin_timestamp);
            try std.testing.expectEqual(std.math.maxInt(u64), max_begin.topology_epoch);

            var max_prepare = try parseTxnPrepareRequest(
                alloc,
                \\{"txn_id":"00112233445566778899aabbccddeeff","topology_epoch":18446744073709551615,"writes":[],"deletes":[],"transforms":[],"predicates":[{"key":"doc:a","expected_version":18446744073709551615}]}
                ,
            );
            defer freeTxnPrepareRequest(alloc, &max_prepare);
            try std.testing.expectEqual(std.math.maxInt(u64), max_prepare.topology_epoch);
            try std.testing.expectEqual(std.math.maxInt(u64), max_prepare.req.predicates[0].expected_version);

            const max_resolve = try parseTxnResolveRequest(
                alloc,
                \\{"txn_id":"00112233445566778899aabbccddeeff","status":"committed","commit_version":18446744073709551615,"topology_epoch":18446744073709551615}
                ,
            );
            try std.testing.expectEqual(std.math.maxInt(u64), max_resolve.commit_version);
            try std.testing.expectEqual(std.math.maxInt(u64), max_resolve.topology_epoch);
        }

        test "txn resolve codec preserves sync level and accepts legacy requests" {
            const alloc = std.testing.allocator;
            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const encoded = try encodeTxnResolveRequest(alloc, .{
                .txn_id = txn_id,
                .status = .committed,
                .commit_version = 42,
                .topology_epoch = 7,
                .sync_level = .full_index,
            });
            defer alloc.free(encoded);
            const decoded = try parseTxnResolveRequest(alloc, encoded);
            try std.testing.expectEqual(@as(u64, 7), decoded.topology_epoch);
            try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, decoded.sync_level);

            const legacy = try parseTxnResolveRequest(
                alloc,
                "{\"txn_id\":\"00112233445566778899aabbccddeeff\",\"status\":\"committed\",\"commit_version\":42}",
            );
            try std.testing.expectEqual(@as(u64, 0), legacy.topology_epoch);
            try std.testing.expectEqual(db_mod.types.SyncLevel.propose, legacy.sync_level);
        }

        test "txn acknowledgement codec preserves participant identity" {
            const alloc = std.testing.allocator;
            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const encoded = try encodeTxnAcknowledgeRequest(alloc, .{
                .txn_id = txn_id,
                .participant = "table2:00000004:docs:7002",
            });
            defer alloc.free(encoded);
            var decoded = try parseTxnAcknowledgeRequest(alloc, encoded);
            defer freeTxnAcknowledgeRequest(alloc, &decoded);
            try std.testing.expectEqualSlices(u8, &txn_id, &decoded.txn_id);
            try std.testing.expectEqualStrings("table2:00000004:docs:7002", decoded.participant);
        }

        test "distributed txn abort durably resolves attempted participants and acknowledges untouched participants" {
            const participants = [_]ParticipantTxn{
                .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7002, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 7003, .topology_epoch = 1 },
            };
            const participant_ids = [_][]const u8{
                "table2:00000004:docs:7001",
                "table2:00000004:docs:7002",
                "table2:00000004:docs:7003",
            };
            const txn_id = try parseTxnIdHex("abcdefabcdefabcdefabcdefabcdefab");

            const Recorder = struct {
                resolved_groups: [3]u64 = undefined,
                resolved_count: usize = 0,
                acknowledgements: [2][]const u8 = undefined,
                acknowledgement_count: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                        .acknowledge_group = acknowledge,
                    } };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, req.status);
                    try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
                    self.resolved_groups[self.resolved_count] = group_id;
                    self.resolved_count += 1;
                }
                fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnAcknowledgeRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(@as(u64, 7001), group_id);
                    self.acknowledgements[self.acknowledgement_count] = req.participant;
                    self.acknowledgement_count += 1;
                }
            };

            var partial = Recorder{};
            try abortParticipants(
                std.testing.allocator,
                partial.worker(),
                txn_id,
                10_001,
                &participants,
                &participant_ids,
                1,
            );
            try std.testing.expectEqual(@as(usize, 1), partial.resolved_count);
            try std.testing.expectEqual(@as(u64, 7001), partial.resolved_groups[0]);
            try std.testing.expectEqual(@as(usize, 2), partial.acknowledgement_count);
            try std.testing.expectEqualStrings(participant_ids[1], partial.acknowledgements[0]);
            try std.testing.expectEqualStrings(participant_ids[2], partial.acknowledgements[1]);

            var fully_begun = Recorder{};
            try abortParticipants(
                std.testing.allocator,
                fully_begun.worker(),
                txn_id,
                10_001,
                &participants,
                &participant_ids,
                participants.len,
            );
            try std.testing.expectEqual(@as(usize, 3), fully_begun.resolved_count);
            try std.testing.expectEqual(@as(usize, 2), fully_begun.acknowledgement_count);
        }

        test "distributed txn coordinator groups by range and commits all participants" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                            .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                begins: std.ArrayListUnmanaged(u64) = .empty,
                prepares: std.ArrayListUnmanaged(u64) = .empty,
                read_only_prepares: usize = 0,
                integrity_prepares: usize = 0,
                semantic_prepares: usize = 0,
                activation_prepares: usize = 0,
                range_prepares: usize = 0,
                coordinator_group: u64 = 7001,
                resolves: std.ArrayListUnmanaged(struct {
                    group_id: u64,
                    status: db_mod.types.TxnStatus,
                    sync_level: db_mod.types.SyncLevel,
                }) = .empty,
                acknowledgements: std.ArrayListUnmanaged(u64) = .empty,

                pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
                    self.begins.deinit(alloc);
                    self.prepares.deinit(alloc);
                    self.resolves.deinit(alloc);
                    self.acknowledgements.deinit(alloc);
                }

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                            .acknowledge_group = acknowledge,
                        },
                    };
                }

                fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnBeginRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(@as(usize, 2), req.participants.len);
                    try self.begins.append(std.testing.allocator, group_id);
                }

                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expect(req.req.writes.len + req.req.deletes.len + req.req.predicates.len + req.req.integrity.len + req.req.range_guards.len > 0);
                    if (req.req.range_guards.len != 0) {
                        try std.testing.expectEqual(@as(u64, 7002), group_id);
                        try std.testing.expectEqual(@as(?u64, 9007199254740993), req.req.range_guards[0].generation);
                        self.range_prepares += 1;
                    }
                    if (req.req.integrity.len != 0) {
                        // Physical metadata keys sort on the first range, but their
                        // explicit claim routing key must choose the second owner.
                        try std.testing.expectEqual(@as(u64, 7002), group_id);
                        try std.testing.expectEqualStrings("\x00\x00claim", req.req.integrity[0].key);
                        self.integrity_prepares += 1;
                    }
                    if (req.req.integrity_commands.len != 0) {
                        try std.testing.expectEqual(@as(u64, 7002), group_id);
                        try std.testing.expectEqual(@as(?u32, 77), req.req.relational_schema_version);
                        try std.testing.expectEqual(@as(?[32]u8, @as([32]u8, @splat(8))), req.req.relational_integrity_generation_set);
                        self.semantic_prepares += 1;
                    }
                    if (req.req.relational_activation) |checkpoint| {
                        try std.testing.expectEqual(@as(u64, 7002), group_id);
                        try std.testing.expectEqualStrings("doc:z", checkpoint.routing_key);
                        try std.testing.expectEqualStrings("progress", checkpoint.next);
                        self.activation_prepares += 1;
                    }
                    if (req.req.writes.len == 0 and req.req.deletes.len == 0) self.read_only_prepares += 1;
                    try self.prepares.append(std.testing.allocator, group_id);
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expect(req.topology_epoch != 0);
                    try self.resolves.append(std.testing.allocator, .{
                        .group_id = group_id,
                        .status = req.status,
                        .sync_level = req.sync_level,
                    });
                }

                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }

                fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnAcknowledgeRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(self.coordinator_group, group_id);
                    try std.testing.expectEqualStrings(if (group_id == 7001) "table2:00000004:docs:7002" else "table2:00000004:docs:7001", req.participant);
                    try self.acknowledgements.append(std.testing.allocator, group_id);
                }
            };

            var recorder = Recorder{};
            defer recorder.deinit(std.testing.allocator);
            const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const outcome = try executeMultiTableCommitWithOptions(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                txn_id,
                10_000,
                10_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                    .predicates = &.{
                        .{ .key = "doc:a", .expected_version = 1 },
                        .{ .key = "doc:z", .expected_version = 2 },
                    },
                }},
                .propose,
                null,
                .{ .report_post_commit_failure = false },
            );
            const result = switch (outcome) {
                .committed => |committed| committed,
                .conflict => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(@as(usize, 2), result.participant_count);
            try std.testing.expect(result.propagation_pending);
            try std.testing.expectEqual(@as(usize, 2), recorder.begins.items.len);
            try std.testing.expectEqual(@as(usize, 2), recorder.prepares.items.len);
            try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
            try std.testing.expectEqual(@as(usize, 0), recorder.acknowledgements.items.len);
            for (recorder.resolves.items) |resolved| try std.testing.expectEqual(db_mod.types.TxnStatus.committed, resolved.status);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, recorder.resolves.items[0].sync_level);
            try std.testing.expectEqual(db_mod.types.SyncLevel.propose, recorder.resolves.items[1].sync_level);

            const durable_txn_id = try parseTxnIdHex("10112233445566778899aabbccddeeff");
            const durable_outcome = try executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                durable_txn_id,
                20_000,
                20_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .write,
                null,
            );
            try std.testing.expect(durable_outcome == .committed);
            try std.testing.expect(!durable_outcome.committed.propagation_pending);
            try std.testing.expectEqual(@as(usize, 1), recorder.acknowledgements.items.len);
            try std.testing.expectEqual(@as(usize, 4), recorder.resolves.items.len);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, recorder.resolves.items[2].sync_level);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, recorder.resolves.items[3].sync_level);

            // A parent dependency may route to a shard with no user writes. It still
            // needs a prepare vote and terminal resolution; dropping this participant
            // would bypass the durable shared read guard used by relational mutations.
            recorder.coordinator_group = 7002;
            const dependent_outcome = try executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                try parseTxnIdHex("20112233445566778899aabbccddeeff"),
                30_000,
                30_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{.{ .key = "doc:z", .value = "{\"child\":true}" }},
                    .predicates = &.{.{ .key = "doc:a", .expected_version = 500 }},
                }},
                .write,
                null,
            );
            try std.testing.expect(dependent_outcome == .committed);
            try std.testing.expectEqual(@as(usize, 2), dependent_outcome.committed.participant_count);
            try std.testing.expectEqual(@as(usize, 1), recorder.read_only_prepares);
            try std.testing.expectEqual(@as(usize, 6), recorder.resolves.items.len);

            recorder.coordinator_group = 7001;
            var routed_address = try @import("antfly_local_sources").storage_db_relational_integrity_contract.Address.init(@as([16]u8, @splat(1)), "tuple");
            // This transport test deliberately supplies an explicit routing digest;
            // native address validation is separately tested at the storage boundary.
            routed_address.routing = @as([32]u8, @splat('z'));
            const claim_outcome = try executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                try parseTxnIdHex("30112233445566778899aabbccddeeff"),
                40_000,
                40_001,
                &.{.{
                    .table_name = "docs",
                    .relational_schema_version = 77,
                    .relational_integrity_generation_set = @as([32]u8, @splat(8)),
                    .writes = &.{.{ .key = "doc:a", .value = "{}" }},
                    .integrity = &.{.{ .routing_key = "doc:z", .key = "\x00\x00claim", .kind = .guard, .expected_value = "live" }},
                    .integrity_commands = &.{.{ .address = routed_address, .operation = .{ .check_owner = .{ .parent_table = "docs", .parent_key = "doc:a" } } }},
                    .relational_activation = .{ .routing_key = "doc:z", .expected = null, .next = "progress" },
                }},
                .write,
                null,
            );
            try std.testing.expect(claim_outcome == .committed);
            try std.testing.expectEqual(@as(usize, 2), claim_outcome.committed.participant_count);
            try std.testing.expectEqual(@as(usize, 1), recorder.integrity_prepares);
            try std.testing.expectEqual(@as(usize, 1), recorder.semantic_prepares);
            try std.testing.expectEqual(@as(usize, 1), recorder.activation_prepares);
            var routing = (try table_catalog.transactionRoutingSnapshot(std.testing.allocator, FakeCatalog.iface(), "docs")).?;
            defer routing.deinit(std.testing.allocator);
            const manager = @import("../metadata/table_manager.zig");
            const owner = routing.ranges[1].*;
            var observation: @import("antfly_local_sources").api_range_read_guards.OwnerRangeProof = .{
                .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 9, .table_id = 7, .topology_epoch = routing.topology_epoch, .route = .{ .group_id = 7002, .range_id = manager.rangeDocIdentityRangeId(owner), .identity_namespace = .{ .table_id = 7, .shard_id = manager.rangeDocIdentityShardId(owner), .range_id = manager.rangeDocIdentityRangeId(owner) } } },
                .proofs = &.{.{ .bucket = 100, .generation = 9007199254740993 }},
            };
            recorder.coordinator_group = 7002;
            const guarded = try executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), try parseTxnIdHex("40112233445566778899aabbccddeeff"), 50_000, 50_001, &.{.{ .table_name = "docs", .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .range_guards = (&observation)[0..1] }}, .write, null);
            try std.testing.expect(guarded == .committed);
            try std.testing.expectEqual(@as(usize, 2), guarded.committed.participant_count);
            try std.testing.expectEqual(@as(usize, 1), recorder.range_prepares);
            const prior_begins = recorder.begins.items.len;
            observation.fence.metadata_incarnation = @splat('2');
            try std.testing.expectError(error.TopologyChanged, executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), try parseTxnIdHex("50112233445566778899aabbccddeeff"), 60_000, 60_001, &.{.{ .table_name = "docs", .range_guards = (&observation)[0..1] }}, .write, null));
            try std.testing.expectEqual(prior_begins, recorder.begins.items.len);
        }

        test "stable distributed transaction retry resumes a durable commit decision" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                            .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                completion_ns: u64 = 0,
                expected_txn: ?db_mod.types.TxnId = null,
                begin_error: anyerror = error.DecisionConflict,
                failed_begin_group: u64 = 7001,
                status_error: ?anyerror = null,
                observed_status: db_mod.types.TxnStatus = .committed,
                follower_resolve_error: ?anyerror = null,
                follower_resolved: bool = false,
                follower_acknowledged: bool = false,
                expect_live_topology: bool = false,
                begin_calls: usize = 0,
                prepare_calls: usize = 0,
                resolve_calls: usize = 0,
                status_calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                            .status_group_until = statusUntil,
                            .acknowledge_group = acknowledge,
                        },
                    };
                }

                fn begin(ptr: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, req: TxnBeginRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.begin_calls += 1;
                    if (self.expected_txn) |expected| try std.testing.expectEqualSlices(u8, &expected, &req.txn_id);
                    try std.testing.expect(req.retain_terminal);
                    if (group == self.failed_begin_group) return self.begin_error;
                }

                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.prepare_calls += 1;
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.resolve_calls += 1;
                    try std.testing.expectEqual(if (self.observed_status == .pending) db_mod.types.TxnStatus.aborted else self.observed_status, req.status);
                    if (self.expect_live_topology) {
                        try std.testing.expect(req.topology_epoch != 0);
                    } else {
                        try std.testing.expectEqual(@as(u64, 0), req.topology_epoch);
                    }
                    if (group == 7002) {
                        if (self.follower_resolve_error) |err| return err;
                        self.follower_resolved = true;
                    }
                }

                fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnAcknowledgeRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (std.mem.endsWith(u8, req.participant, ":7002")) {
                        try std.testing.expect(self.follower_resolved);
                        self.follower_acknowledged = true;
                    }
                }

                fn check(_: *anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
                    return .ok;
                }
                fn phase(_: *anyopaque, _: c_int) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
                    return .ok;
                }
                fn resize(_: *anyopaque, slots: usize) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
                    return if (slots == 1) .ok else @import("antfly_runtime_abi").error_abi.statusFromError(error.InvalidRunnableDemand);
                }
                fn remaining(raw: *anyopaque, out: *u64) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    out.* = self.completion_ns;
                    return .ok;
                }

                fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, txn: db_mod.types.TxnId, deadline: u64) !db_mod.types.TxnStatus {
                    try ensureDecisionRecoveryDeadline(deadline);
                    return status(ptr, alloc, group, table, txn);
                }

                fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.status_calls += 1;
                    if (self.status_error) |err| return err;
                    return self.observed_status;
                }
            };

            // An ambiguous coordinator BEGIN must never fall back to an
            // unbounded probe when accepted completion authority is missing or
            // exhausted, and must retain the exact original transaction ID.
            for ([_]?u64{ null, 0, 20 * std.time.ns_per_s }) |remaining_ns| {
                const runtime = @import("antfly_runtime_abi");
                const txn_id: db_mod.types.TxnId = @splat(9);
                var recorder: Recorder = .{ .completion_ns = remaining_ns orelse 0, .expected_txn = txn_id };
                var team: runtime.runnable_abi.Team = .{
                    .borrow = .{ .context = &recorder, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .checkpoint = Recorder.check, .park = Recorder.phase, .enter = Recorder.phase, .resize_team = Recorder.resize, .completion_remaining_ns = if (remaining_ns != null) Recorder.remaining else null },
                    .maximum_slots = 1,
                    .allocator = std.testing.allocator,
                };
                const outcome = executeMultiTableCommitWithOptions(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), txn_id, 10_000, 10_001, &.{.{ .table_name = "docs", .transforms = &.{ .{ .key = "doc:a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} }, .{ .key = "doc:z", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} } } }}, .write, null, .{ .retain_terminal = true, .team = &team });
                if (remaining_ns != null and remaining_ns.? > 0) {
                    try std.testing.expect((try outcome) == .committed);
                    try std.testing.expectEqual(@as(usize, 1), recorder.status_calls);
                    // Revisit coordinator materialization, then its follower.
                    try std.testing.expectEqual(@as(usize, 2), recorder.resolve_calls);
                } else {
                    try std.testing.expectError(error.CommitDecisionUnknown, outcome);
                    try std.testing.expectEqual(@as(usize, 0), recorder.status_calls);
                    try std.testing.expectEqual(@as(usize, 0), recorder.resolve_calls);
                }
                try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
                try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
            }

            for ([_]anyerror{ error.DecisionConflict, error.RaftBatchWriteOutcomeUnknown, error.UnexpectedHttpStatus, error.Timeout, error.UnknownGroup, error.PreDecisionNotProposed }) |begin_error| {
                var recorder = Recorder{ .begin_error = begin_error };
                const txn_id = try parseTxnIdHex("0123456789abcdeffedcba9876543210");
                const outcome = try executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    FakeCatalog.iface(),
                    recorder.worker(),
                    txn_id,
                    10_000,
                    10_001,
                    &.{.{
                        .table_name = "docs",
                        .transforms = &.{
                            .{
                                .key = "doc:a",
                                .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
                            },
                            .{
                                .key = "doc:z",
                                .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
                            },
                        },
                    }},
                    .write,
                    null,
                    .{ .retain_terminal = true },
                );
                try std.testing.expect(outcome == .committed);
                try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
                try std.testing.expectEqual(@as(usize, 1), recorder.status_calls);
                try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
                try std.testing.expectEqual(@as(usize, 2), recorder.resolve_calls);
                // An unavailable decision is not permission to abort a possibly
                // committed stable transaction, even if BEGIN was not proposed.
                recorder = .{ .begin_error = begin_error, .status_error = error.LeaderUnavailable };
                try std.testing.expectError(error.CommitDecisionUnknown, executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    FakeCatalog.iface(),
                    recorder.worker(),
                    txn_id,
                    10_000,
                    10_001,
                    &.{.{ .table_name = "docs", .writes = &.{.{ .key = "doc:a", .value = "{}" }} }},
                    .write,
                    null,
                    .{ .retain_terminal = true },
                ));
                try std.testing.expectEqual(@as(usize, 1), recorder.status_calls);
                try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
                try std.testing.expectEqual(@as(usize, 0), recorder.resolve_calls);
            }
            // A lost BEGIN outcome on a stable session ID must remain
            // retryable. In particular, an unknown Raft apply result is not
            // evidence that the session should be durably aborted.
            for ([_]u64{ 7001, 7002 }) |failed_group| {
                var recorder = Recorder{
                    .failed_begin_group = failed_group,
                    .begin_error = error.RaftBatchWriteOutcomeUnknown,
                    .observed_status = .pending,
                };
                const txn_id = try parseTxnIdHex("0123456789abcdeffedcba9876543210");
                const request = &[_]TableCommitRequest{.{ .table_name = "docs", .writes = &.{
                    .{ .key = "doc:a", .value = "{}" },
                    .{ .key = "doc:z", .value = "{}" },
                } }};
                try std.testing.expectError(error.CommitDecisionUnknown, executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    FakeCatalog.iface(),
                    recorder.worker(),
                    txn_id,
                    10_000,
                    10_001,
                    request,
                    .write,
                    null,
                    .{ .retain_terminal = true },
                ));
                try std.testing.expectEqual(@as(usize, 0), recorder.resolve_calls);
                recorder.failed_begin_group = 0;
                recorder.observed_status = .committed;
                recorder.expect_live_topology = true;
                const resumed = try executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    FakeCatalog.iface(),
                    recorder.worker(),
                    txn_id,
                    10_000,
                    10_001,
                    request,
                    .write,
                    null,
                    .{ .retain_terminal = true },
                );
                try std.testing.expect(resumed == .committed);
            }
            // Model an earlier interrupted execution with a prepared follower.
            // Neither coordinator BEGIN failure nor a follower's explicit
            // not-proposed result proves that old participant has no intents.
            for ([_]u64{ 7001, 7002 }) |failed_group| {
                for ([_]?anyerror{ null, error.Timeout, error.TxnNotFound }) |resolve_error| {
                    var recorder = Recorder{ .failed_begin_group = failed_group, .begin_error = error.PreDecisionNotProposed, .observed_status = .pending, .follower_resolve_error = resolve_error };
                    const result = executeMultiTableCommitWithOptions(
                        std.testing.allocator,
                        FakeCatalog.iface(),
                        recorder.worker(),
                        @splat(7),
                        10_000,
                        10_001,
                        &.{.{ .table_name = "docs", .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" } } }},
                        .write,
                        null,
                        .{ .retain_terminal = true },
                    );
                    if (failed_group == 7001) try std.testing.expectError(error.TransactionBeginFailed, result) else try std.testing.expect((try result) == .conflict);
                    try std.testing.expectEqual(@as(usize, 2), recorder.resolve_calls);
                    try std.testing.expectEqual(resolve_error == null, recorder.follower_resolved);
                    try std.testing.expectEqual(resolve_error == null, recorder.follower_acknowledged);
                }
            }
        }

        test "distributed txn coordinator aborts only participants that may have begun" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                            .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                fail_begin: bool = false,
                definite_begin_miss_group_id: ?u64 = null,
                prepare_failure: anyerror = error.IntentConflict,
                abort_failure: bool = false,
                observed_status: db_mod.types.TxnStatus = .pending,
                resolves: std.ArrayListUnmanaged(db_mod.types.TxnStatus) = .empty,

                pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
                    self.resolves.deinit(alloc);
                }

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, _: TxnBeginRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (self.definite_begin_miss_group_id == group_id) return error.PreDecisionNotProposed;
                    if (self.fail_begin and group_id == 7002) return error.InjectedBeginFailure;
                }

                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (!self.fail_begin and group_id == 7002) return self.prepare_failure;
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try self.resolves.append(std.testing.allocator, req.status);
                    try std.testing.expectEqual(.write, req.sync_level);
                    if (self.abort_failure) return error.InjectedAbortFailure;
                }

                fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    return self.observed_status;
                }
            };

            var recorder = Recorder{};
            defer recorder.deinit(std.testing.allocator);
            const txn_id = try parseTxnIdHex("ffeeddccbbaa99887766554433221100");
            try std.testing.expectError(error.IntentConflict, executeCrossGroup(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                "docs",
                txn_id,
                10_000,
                10_001,
                .{
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                },
                null,
            ));
            try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
            for (recorder.resolves.items) |status| try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, status);

            recorder.resolves.clearRetainingCapacity();
            recorder.fail_begin = true;
            try std.testing.expectError(error.TransactionBeginFailed, executeCrossGroup(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                "docs",
                txn_id,
                10_000,
                10_001,
                .{
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                },
                null,
            ));
            try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
            for (recorder.resolves.items) |status| try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, status);

            recorder.resolves.clearRetainingCapacity();
            recorder.fail_begin = false;
            recorder.definite_begin_miss_group_id = 7001;
            try std.testing.expectError(error.IntentConflict, executeCrossGroup(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                "docs",
                txn_id,
                10_000,
                10_001,
                .{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }} },
                null,
            ));
            try std.testing.expectEqual(@as(usize, 0), recorder.resolves.items.len);

            recorder.definite_begin_miss_group_id = 7002;
            try std.testing.expectError(error.IntentConflict, executeCrossGroup(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                "docs",
                txn_id,
                10_000,
                10_001,
                .{ .writes = &.{
                    .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                    .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                } },
                null,
            ));
            // Only the coordinator, which did begin, receives the durable abort. The
            // follower's explicit not-proposed result must not create phase-two work.
            try std.testing.expectEqual(@as(usize, 1), recorder.resolves.items.len);
            try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, recorder.resolves.items[0]);

            recorder.definite_begin_miss_group_id = null;
            const tables = [_]TableCommitRequest{.{ .table_name = "docs", .writes = &.{
                .{ .key = "doc:a", .value = "{\"count\":0}" },
                .{ .key = "doc:z", .value = "{\"count\":0}" },
            } }};
            for ([_]anyerror{ error.UniqueConstraintViolation, error.ForeignKeyParentMissing, error.ForeignKeyReferenced }, [_]contract.CommitConflictReason{ .unique_constraint_violation, .foreign_key_parent_missing, .foreign_key_referenced }) |failure, reason| {
                recorder.prepare_failure = failure;
                recorder.resolves.clearRetainingCapacity();
                recorder.abort_failure = false;
                recorder.observed_status = .aborted;
                const outcome = try executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), txn_id, 10_000, 10_001, &tables, .write, null);
                try std.testing.expect(outcome == .conflict);
                try std.testing.expectEqual(reason, outcome.conflict.reason.?);
                try std.testing.expect(!outcome.conflict.retryable);
                try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
                for (recorder.resolves.items) |status| try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, status);
            }
            for ([_]anyerror{ error.RaftBatchWriteOutcomeUnknown, error.ClientShuttingDown }) |prepare_failure| {
                recorder.prepare_failure = prepare_failure;
                for ([_]bool{ false, true }) |abort_failure| {
                    recorder.resolves.clearRetainingCapacity();
                    recorder.abort_failure = abort_failure;
                    recorder.observed_status = .aborted;
                    const outcome = try executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), txn_id, 10_000, 10_001, &tables, .write, null);
                    try std.testing.expect(outcome == .conflict);
                    try std.testing.expectEqual(.prepare, outcome.conflict.phase.?);
                    try std.testing.expectEqual(@as(?u64, 7002), outcome.conflict.group_id);
                    for (recorder.resolves.items) |status| try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, status);
                }
                // An ambiguous abort or a committed decision can never authorize a
                // fresh transaction, even when the original failure was in prepare.
                for ([_]db_mod.types.TxnStatus{ .pending, .committed }) |observed_status| {
                    recorder.observed_status = observed_status;
                    try std.testing.expectError(error.AbortDecisionNotDurable, executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), txn_id, 10_000, 10_001, &tables, .write, null));
                }
            }
            recorder.prepare_failure = error.StorageReadTemporarilyUnavailable;
            recorder.abort_failure = false;
            recorder.observed_status = .aborted;
            recorder.resolves.clearRetainingCapacity();
            try std.testing.expectError(error.TransactionPrepareAbortedUnavailable, executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), txn_id, 10_000, 10_001, &tables, .write, null));
            try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
            recorder.abort_failure = true;
            recorder.observed_status = .pending;
            try std.testing.expectError(error.AbortDecisionNotDurable, executeMultiTableCommit(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), txn_id, 10_000, 10_001, &tables, .write, null));
        }

        test "distributed txn coordinator never restarts a transaction id on topology change" {
            const FakeCatalog = struct {
                call_count: usize = 0,

                fn iface(self: *@This()) table_catalog.CatalogSource {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(ptr: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.call_count += 1;
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        // Transaction admission now checks active transitions before
                        // pinning and resolving the range epoch.
                        .ranges = if (self.call_count <= 3)
                            @constCast((&[_]metadata_table_manager.RangeRecord{
                                .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                                .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                            })[0..])
                        else
                            @constCast((&[_]metadata_table_manager.RangeRecord{
                                .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:n" },
                                .{ .group_id = 7002, .table_id = 7, .start_key = "doc:n", .end_key = null },
                            })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                prepare_calls: usize = 0,
                resolved_sync_level: db_mod.types.SyncLevel = .propose,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}

                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.prepare_calls += 1;
                    if (self.prepare_calls == 1) {
                        try std.testing.expect(req.topology_epoch != 0);
                        return error.TopologyChanged;
                    }
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.resolved_sync_level = req.sync_level;
                }

                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
            };

            var catalog = FakeCatalog{};
            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("11112222333344445555666677778888");
            try std.testing.expectError(error.TopologyChanged, executeMultiTableCommit(
                std.testing.allocator,
                catalog.iface(),
                recorder.worker(),
                txn_id,
                10_000,
                10_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"z\"}" }},
                }},
                .full_index,
                null,
            ));
            try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
            // Abort decisions must be durable before the coordinator reports the
            // prepare failure; an earlier participant may already have begun.
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, recorder.resolved_sync_level);
        }

        test "distributed txn coordinator returns topology failure without retry" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                fn worker() ParticipantWorker {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    return error.TopologyChanged;
                }
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
            };

            const txn_id = try parseTxnIdHex("99990000111122223333444455556666");
            try std.testing.expectError(error.TopologyChanged, executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                Recorder.worker(),
                txn_id,
                10_000,
                10_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
                }},
                .propose,
                null,
            ));
        }

        test "distributed txn coordinator returns unknown group without restarting the transaction" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                begin_calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.begin_calls += 1;
                    return error.UnknownGroup;
                }

                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
            };

            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("aaaabbbbccccddddeeeeffff00001111");
            const outcome = try executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                txn_id,
                10_000,
                10_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
                }},
                .propose,
                null,
            );
            try std.testing.expect(outcome == .conflict);
            try std.testing.expectEqualStrings("participant unavailable", outcome.conflict.message);
            try std.testing.expectEqualStrings("docs", outcome.conflict.table_name);
            try std.testing.expectEqual(@as(?u64, 7001), outcome.conflict.group_id);
            try std.testing.expectEqual(.begin, outcome.conflict.phase.?);
            try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
        }

        test "distributed txn coordinator never aborts after durable commit decision" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{ .ptr = undefined, .vtable = &.{
                        .admin_snapshot = adminSnapshot,
                        .free_admin_snapshot = freeAdminSnapshot,
                    } };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                            .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                first_committed: bool = false,
                abort_calls: usize = 0,
                second_conflict: bool = false,
                retry_ambiguous_coordinator: bool = false,
                worker_failure: bool = false,
                follower_retry_pending: bool = false,
                follower_transported_visibility_pending: bool = false,
                acknowledgement_failure: bool = false,
                acknowledgement_calls: usize = 0,
                coordinator_resolve_calls: usize = 0,
                coordinator_retry_sync_level: ?db_mod.types.SyncLevel = null,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                        .resolve_group_until = resolveUntil,
                        .status_group_until = statusUntil,
                        .acknowledge_group = acknowledge,
                    } };
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (req.status == .aborted) {
                        self.abort_calls += 1;
                        return;
                    }
                    if (group_id == 7001) {
                        self.coordinator_resolve_calls += 1;
                        if (self.retry_ambiguous_coordinator) {
                            if (self.coordinator_resolve_calls == 1) return error.InjectedPostCommitAckFailure;
                            self.coordinator_retry_sync_level = req.sync_level;
                            self.first_committed = true;
                            return;
                        }
                        self.first_committed = true;
                        if (self.worker_failure) return error.EnrichmentWorkerFailed;
                        return error.InjectedPostCommitAckFailure;
                    }
                    if (self.second_conflict) return error.DecisionConflict;
                    if (self.follower_retry_pending) return error.EnrichmentRetryInProgress;
                    if (self.follower_transported_visibility_pending) return error.CommitVisibilityNotSatisfied;
                }
                fn status(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (group_id == 7001 and self.retry_ambiguous_coordinator) return .pending;
                    if (group_id == 7001 and self.first_committed) return .committed;
                    return .pending;
                }
                fn resolveUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return try resolve(ptr, alloc, group_id, table_name, req);
                }
                fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return try status(ptr, alloc, group_id, table_name, txn_id);
                }
                fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.acknowledgement_calls += 1;
                    if (self.acknowledgement_failure) return error.InjectedAcknowledgementFailure;
                }
            };

            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("1234567890abcdef1234567890abcdef");
            // Proposal-only follower delivery remains live propagation debt and takes
            // precedence over the coordinator's retryable visibility result.
            try std.testing.expectError(error.CommitPropagationIncomplete, executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                txn_id,
                10_000,
                10_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .propose,
                null,
            ));
            try std.testing.expect(recorder.first_committed);
            try std.testing.expectEqual(@as(usize, 0), recorder.abort_calls);

            // Permanent visibility failure remains distinct from ordinary deferred
            // visibility so clients can request repair instead of polling forever.
            recorder = .{ .worker_failure = true };
            const repair_txn_id = try parseTxnIdHex("1234567890abcdef0011223344556677");
            const repair = try executeMultiTableCommitWithOptions(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                repair_txn_id,
                15_000,
                15_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .enrichments,
                null,
                .{ .report_post_commit_failure = false },
            );
            try std.testing.expect(repair == .committed);
            try std.testing.expect(repair.committed.visibility_pending);
            try std.testing.expect(!repair.committed.visibility_retry_pending);
            try std.testing.expect(repair.committed.visibility_repair_required);
            try std.testing.expectEqual(@as(usize, 0), recorder.abort_calls);

            // Repair on one participant must not hide retryable visibility debt on
            // another. Stable sessions keep recovery active until that live barrier
            // clears, while retaining the independent repair signal.
            recorder = .{ .worker_failure = true, .follower_retry_pending = true };
            const mixed_txn_id = try parseTxnIdHex("1234567890abcdef8899aabbccddeeff");
            const mixed = try executeMultiTableCommitWithOptions(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                mixed_txn_id,
                17_000,
                17_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .enrichments,
                null,
                .{ .report_post_commit_failure = false },
            );
            try std.testing.expect(mixed == .committed);
            try std.testing.expect(mixed.committed.visibility_retry_pending);
            try std.testing.expect(mixed.committed.visibility_repair_required);
            try std.testing.expect(!mixed.committed.propagation_pending);
            try std.testing.expectEqual(@as(usize, 1), recorder.acknowledgement_calls);

            // A remote participant normalizes its typed HTTP 202 to
            // CommitVisibilityNotSatisfied. It is the same durable visibility outcome
            // as the local EnrichmentRetryInProgress spelling and must still release
            // the coordinator enlistment.
            recorder = .{ .follower_transported_visibility_pending = true };
            const transported_txn_id = try parseTxnIdHex("1234567890abcdeffedcba0987654321");
            const transported = try executeMultiTableCommitWithOptions(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                transported_txn_id,
                17_500,
                17_501,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .enrichments,
                null,
                .{ .report_post_commit_failure = false },
            );
            try std.testing.expect(transported == .committed);
            try std.testing.expect(transported.committed.visibility_retry_pending);
            try std.testing.expect(!transported.committed.propagation_pending);
            try std.testing.expectEqual(@as(usize, 1), recorder.acknowledgement_calls);

            // The durable follower is still acknowledged when its visibility wait is
            // pending. If acknowledgement itself fails, propagation recovery takes
            // precedence over the retryable visibility result.
            recorder = .{ .follower_retry_pending = true, .acknowledgement_failure = true };
            const acknowledgement_txn_id = try parseTxnIdHex("1234567890abcdef7766554433221100");
            try std.testing.expectError(error.CommitPropagationIncomplete, executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                acknowledgement_txn_id,
                18_000,
                18_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .enrichments,
                null,
            ));
            try std.testing.expectEqual(@as(usize, 1), recorder.acknowledgement_calls);

            // A contradictory/missing follower after the coordinator decision is a
            // committed transaction with incomplete propagation, never an abort.
            recorder = .{ .second_conflict = true };
            const second_txn_id = try parseTxnIdHex("abcdef1234567890abcdef1234567890");
            try std.testing.expectError(error.CommitPropagationIncomplete, executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                second_txn_id,
                20_000,
                20_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .propose,
                null,
            ));
            try std.testing.expect(recorder.first_committed);
            try std.testing.expectEqual(@as(usize, 0), recorder.abort_calls);

            // Callers using an ephemeral server-generated ID must not receive a
            // retryable failure after the decision is durable: a retry would use a new
            // ID and could apply transforms twice.
            recorder = .{ .second_conflict = true };
            const ephemeral_txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
            const ephemeral = try executeMultiTableCommitWithOptions(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                ephemeral_txn_id,
                30_000,
                30_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .propose,
                null,
                .{ .report_post_commit_failure = false },
            );
            try std.testing.expect(ephemeral == .committed);
            try std.testing.expect(ephemeral.committed.propagation_pending);
            try std.testing.expect(ephemeral.committed.visibility_pending);
            try std.testing.expect(recorder.first_committed);
            try std.testing.expectEqual(@as(usize, 0), recorder.abort_calls);

            // An ambiguous proposal-only coordinator submission is retried under the
            // effective write barrier. Treating proposal acceptance as committed here
            // could let a follower commit after leadership loss discards the decision.
            recorder = .{ .retry_ambiguous_coordinator = true };
            const ambiguous_txn_id = try parseTxnIdHex("fedcba0987654321fedcba0987654321");
            const ambiguous = try executeMultiTableCommitWithOptions(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                ambiguous_txn_id,
                40_000,
                40_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{
                        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                        .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
                    },
                }},
                .propose,
                null,
                .{ .report_post_commit_failure = false },
            );
            try std.testing.expect(ambiguous == .committed);
            try std.testing.expectEqual(@as(?db_mod.types.SyncLevel, .write), recorder.coordinator_retry_sync_level);
            try std.testing.expectEqual(@as(usize, 0), recorder.abort_calls);
        }

        test "distributed txn coordinator surfaces resolve decision conflicts deterministically" {
            const FakeCatalog = struct {
                fn iface() table_catalog.CatalogSource {
                    return .{
                        .ptr = undefined,
                        .vtable = &.{
                            .admin_snapshot = adminSnapshot,
                            .free_admin_snapshot = freeAdminSnapshot,
                        },
                    };
                }

                fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
                    const metadata_table_manager = @import("../metadata/table_manager.zig");
                    const raft_reconciler = @import("../raft/reconciler.zig");
                    const metadata_transition_state = @import("../metadata/transition_state.zig");
                    return .{
                        .status = .{ .metadata_group_id = 1, .metrics = .{} },
                        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                        })[0..]),
                        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                    };
                }

                fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
            };

            const Recorder = struct {
                begin_calls: usize = 0,
                prepare_calls: usize = 0,
                resolve_calls: usize = 0,
                abort_calls: usize = 0,

                fn worker(self: *@This()) ParticipantWorker {
                    return .{
                        .ptr = self,
                        .vtable = &.{
                            .begin_group = begin,
                            .prepare_group = prepare,
                            .resolve_group = resolve,
                            .status_group = status,
                        },
                    };
                }

                fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.begin_calls += 1;
                }

                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.prepare_calls += 1;
                }

                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(@as(u64, 7001), group_id);
                    try std.testing.expectEqualStrings("docs", table_name);
                    if (req.status == .aborted) {
                        self.abort_calls += 1;
                        return;
                    }
                    self.resolve_calls += 1;
                    return error.DecisionConflict;
                }

                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .pending;
                }
            };

            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("11112222333344445555666677778888");
            const outcome = try executeMultiTableCommit(
                std.testing.allocator,
                FakeCatalog.iface(),
                recorder.worker(),
                txn_id,
                10_000,
                10_001,
                &.{.{
                    .table_name = "docs",
                    .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
                }},
                .propose,
                null,
            );
            try std.testing.expect(outcome == .conflict);
            try std.testing.expectEqualStrings("decision conflict", outcome.conflict.message);
            try std.testing.expectEqualStrings("docs", outcome.conflict.table_name);
            try std.testing.expectEqual(@as(?u64, 7001), outcome.conflict.group_id);
            try std.testing.expectEqual(.resolve, outcome.conflict.phase.?);
            try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
            try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
            try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
            try std.testing.expectEqual(@as(usize, 1), recorder.abort_calls);
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}

test "distributed txn prepare JSON null and insert preconditions survive allocation failures" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const encoded = try encodeTxnPrepareRequest(alloc, .{ .txn_id = @splat(1), .req = .{ .writes = &.{.{ .key = "row", .value = "{\"j\":null}", .json_null_fields = &.{"j"} }}, .predicates = &.{.{ .key = "row", .expected_version = 0, .unique_absence = true }} } });
            defer alloc.free(encoded);
            var parsed = try parseTxnPrepareRequest(alloc, encoded);
            defer freeTxnPrepareRequest(alloc, &parsed);
            try std.testing.expectEqualStrings("j", parsed.req.writes[0].json_null_fields[0]);
            try std.testing.expect(parsed.req.predicates[0].unique_absence);
            const prefix = "[\"row-semantics-prepare-v1\",";
            try std.testing.expect(std.mem.startsWith(u8, encoded, prefix));
            // Released decoders accept only an object (or the known range
            // marker); they reject this new envelope before parsing writes.
            var legacy = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
            defer legacy.deinit();
            try std.testing.expect(legacy.value == .array);
            if (parseTxnPrepareRequest(alloc, encoded[prefix.len .. encoded.len - 1])) |value| {
                var unexpected = value;
                freeTxnPrepareRequest(alloc, &unexpected);
                return error.TestExpectedError;
            } else |err| if (err != error.InvalidTxnRequest) return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "distributed txn independently scheduled windows retain physical children in all four phases" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const allocation = @import("../common/allocation_budget.zig");
    const writes = @import("runnable_write_team.zig");
    const Clock = struct {
        var current: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return current.load(.acquire);
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Recorder = struct {
        scheduler: *native.Scheduler,
        root_context: *anyopaque,
        io: std.Io,
        calls: std.atomic.Value(usize) = .init(0),
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        health_checked: std.atomic.Value(bool) = .init(false),
        acknowledgements: std.atomic.Value(usize) = .init(0),
        expire_on_second_return: bool = false,
        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            return error.TestUnscheduledParticipant;
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.TestUnscheduledParticipant;
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.TestUnscheduledParticipant;
        }
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
        fn beginControlled(raw: *anyopaque, allocator: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest, borrow: runnable_abi.Borrowed) !void {
            try perform(raw, allocator, borrow);
        }
        fn prepareControlled(raw: *anyopaque, allocator: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest, borrow: runnable_abi.Borrowed) !void {
            try perform(raw, allocator, borrow);
        }
        fn resolveControlled(raw: *anyopaque, allocator: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest, _: db_mod.types.CancellationToken, borrow: runnable_abi.Borrowed) !void {
            try std.testing.expect(req.status == .committed or req.status == .aborted);
            try perform(raw, allocator, borrow);
        }
        fn acknowledgeControlled(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, req: TxnAcknowledgeRequest, borrow: runnable_abi.Borrowed) !void {
            try borrow.checkLifetime();
            try acknowledge(raw, alloc, group, table, req);
        }
        fn acknowledge(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.acknowledgements.fetchAdd(1, .monotonic);
        }
        fn perform(raw: *anyopaque, allocator: std.mem.Allocator, borrow: runnable_abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(borrow.context != self.root_context);
            try std.testing.expectEqual(@as(usize, 1), borrow.cpu_slots);
            try std.testing.expectEqual(runnable_abi.Class.write, try borrow.currentClass());
            const stats = self.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 1), stats.runnable_slots - stats.control_slots);
            try std.testing.expect(stats.child_operations > 0);
            const call = self.calls.fetchAdd(1, .acq_rel) + 1;
            const active = self.active.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.active.fetchSub(1, .acq_rel);
            var peak = self.peak.load(.monotonic);
            while (active > peak) peak = self.peak.cmpxchgWeak(peak, active, .monotonic, .monotonic) orelse break;
            const scratch = try allocator.dupe(u8, "bounded participant RPC");
            defer allocator.free(scratch);
            var wait: runnable_fanout.RemoteWait = .{ .borrow = borrow };
            const boundary = wait.boundary().?;
            try boundary.beginWait();
            if (!self.health_checked.swap(true, .acq_rel)) {
                var health: native.Execution = .{};
                defer health.finish();
                try health.start(self.scheduler, .{ .io = self.io, .allow_wait = false }, .control);
                try health.checkpoint();
                health.finish();
            }
            const slept = self.io.sleep(.fromMilliseconds(10), .awake);
            try boundary.endWait();
            try slept;
            try borrow.checkLifetime();
            if (self.expire_on_second_return and call == 2) Clock.current.store(20 * std.time.ns_per_s, .release);
        }
        fn worker(self: *@This()) ParticipantWorker {
            return .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status, .acknowledge_group = acknowledge, .acknowledge_group_with_execution = acknowledgeControlled, .begin_group_with_execution = beginControlled, .prepare_group_with_execution = prepareControlled, .resolve_group_with_execution = resolveControlled } };
        }
    };
    for ([_]bool{ false, true }) |expire| {
        Clock.current.store(0, .release);
        defer Clock.current.store(0, .release);
        const io = std.testing.io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = 64 * 1024 };
        var canceled: std.atomic.Value(bool) = .init(false);
        var root: native.Execution = .{};
        defer root.finish();
        try root.start(&scheduler, .{ .io = io, .retained_bytes = 256, .native_now_ns = Clock.now, .deadline_ns = 5 * std.time.ns_per_s, .cancellation = .{ .ptr = &canceled, .requested = Clock.canceled } }, .write);
        var bridge = try writes.Completion.init(&root, memory.allocator());
        var team = try bridge.team();
        try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 20 * std.time.ns_per_s });
        try team.borrow.activateCompletion();
        Clock.current.store(6 * std.time.ns_per_s, .release);
        canceled.store(true, .release);
        const participants = [_]ParticipantTxn{
            .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 1 },
            .{ .table_name = "docs", .group_id = 7002, .topology_epoch = 1 },
            .{ .table_name = "docs", .group_id = 7003, .topology_epoch = 1 },
        };
        const ids = [_][]const u8{ "coordinator", "follower-1", "follower-2" };
        var owners: [3]runnable_fanout.Owner = @splat(.{});
        var slots: [3]ParticipantFanoutSlot = undefined;
        var options: ExecuteOptions = .{ .team = &team, .fanout_io = io, .max_parallel_participants = 2, .independent_participants = true, .participant_owners = &owners, .reusable_participants = true };
        try reserveParticipantTeam(&options, participants.len);
        defer team.finish();
        for (&owners) |*owner| try owner.reserve(team.borrow, io, 0);
        defer for (&owners) |*owner| owner.retire();
        const retained_receivers = memory.used.load(.acquire);
        var recorder: Recorder = .{ .scheduler = &scheduler, .root_context = team.borrow.context, .io = io, .expire_on_second_return = expire };
        var worker = recorder.worker();
        worker.execution_runnable = team.borrow;
        runBeginFanout(worker, @splat(1), 1, &participants, &ids, &slots, options);
        try std.testing.expectEqual(@as(usize, 2), recorder.calls.load(.acquire));
        for (slots[1..]) |slot| try std.testing.expect(slot.may_have_transaction_state);
        if (expire) {
            for (slots[1..]) |slot| try std.testing.expectEqual(@as(?anyerror, error.DeadlineExceeded), slot.err);
            try std.testing.expectError(error.DeadlineExceeded, worker.resolveGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .status = .aborted, .commit_version = 2 }));
        } else {
            for (slots[1..]) |slot| try std.testing.expect(slot.err == null);
            try std.testing.expectEqual(@as(usize, 2), recorder.peak.load(.acquire));
            runPrepareFanout(worker, @splat(1), &participants, &slots, options);
            for (slots) |slot| try std.testing.expect(slot.err == null);
            try std.testing.expectEqual(@as(usize, 5), recorder.calls.load(.acquire));
            runResolveFollowerFanout(worker, @splat(1), 2, &participants, &ids, .write, false, &slots, options);
            for (slots[1..]) |slot| try std.testing.expect(slot.err == null and !slot.propagation_pending);
            try std.testing.expectEqual(@as(usize, 7), recorder.calls.load(.acquire));
            try std.testing.expectEqual(@as(usize, 2), recorder.acknowledgements.load(.acquire));
            runAbortFollowerFanout(worker, @splat(1), 3, &participants, &ids, participants.len, false, &slots, options);
            for (slots[1..]) |slot| try std.testing.expect(slot.err == null);
            try std.testing.expectEqual(@as(usize, 9), recorder.calls.load(.acquire));
            try std.testing.expectEqual(@as(usize, 4), recorder.acknowledgements.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
        }
        try std.testing.expectEqual(@as(usize, 3), root.entry.?.children);
        try std.testing.expectEqual(retained_receivers, memory.used.load(.acquire));
        for (&owners) |*owner| owner.retire();
        try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
        for (owners) |owner| try std.testing.expect(owner.child == null and !owner.running);
        try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
    }
}

test "distributed txn independently scheduled local worker preserves call authority and original admission context" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const writes = @import("runnable_write_team.zig");
    const Fake = struct {
        child_context: ?*anyopaque = null,
        calls: usize = 0,
        expected: PreDecisionContext = .{},
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return error.TestUnexpectedLegacyMutation;
        }
        fn begin(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, context: PreDecisionContext) !?void {
            return check(raw, context);
        }
        fn prepare(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest, context: PreDecisionContext) !?void {
            return check(raw, context);
        }
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64, _: u64, _: db_mod.types.SyncLevel, context: PreDecisionContext) !?void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.child_context, context.execution_runnable.?.context);
            // Completion authority is not reconstructed from the foreground fence.
            try std.testing.expect(context.deadline_ns == null and context.deadline_io == null);
            try context.execution_runnable.?.checkLifetime();
            try context.cancellation.check();
            self.calls += 1;
            return {};
        }
        fn check(raw: *anyopaque, context: PreDecisionContext) !?void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.child_context, context.execution_runnable.?.context);
            try std.testing.expectEqualDeep(self.expected.deadline_ns, context.deadline_ns);
            try std.testing.expectEqualDeep(self.expected.deadline_io, context.deadline_io);
            try std.testing.expectEqualDeep(self.expected.cancellation, context.cancellation);
            try context.execution_runnable.?.checkLifetime();
            try context.cancellation.check();
            self.calls += 1;
            return {};
        }
    };
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var scheduler = try native.Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    defer scheduler.close();
    var root: native.Execution = .{};
    defer root.finish();
    try root.start(&scheduler, .{ .io = io }, .write);
    var bridge = try writes.Completion.init(&root, std.testing.allocator);
    var team = try bridge.team();
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s });
    try team.borrow.activateCompletion();
    var child: runnable_abi.Child = undefined;
    try team.borrow.reserveChild(&.{ .io = &io_borrow, .out_child = &child });
    defer child.deinit();
    try child.activate();
    var canceled: std.atomic.Value(bool) = .init(false);
    var fake: Fake = .{ .child_context = child.borrow.context, .expected = .{ .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s, .deadline_io = io_borrow, .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled) } };
    var local: LocalTableWriteParticipantWorker = .{ .writes = .{ .ptr = &fake, .vtable = &.{ .batch = Fake.batch, .txn_begin_group_local_with_pre_decision_context = Fake.begin, .txn_prepare_group_local_with_pre_decision_context = Fake.prepare, .txn_resolve_group_local_with_execution = Fake.resolve } }, .pre_decision_context = fake.expected };
    var worker = local.worker();
    worker.execution_runnable = child.borrow;
    try worker.beginGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .begin_timestamp = 1, .participants = &.{} });
    try worker.prepareGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .req = .{} });
    try worker.resolveGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .status = .committed, .commit_version = 2 });
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expect(local.pre_decision_context.execution_runnable == null);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, worker.beginGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .begin_timestamp = 1, .participants = &.{} }));
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "distributed txn independently scheduled hosted worker rejects stopped coordinator before routing" {
    const runtime = @import("antfly_runtime_abi");
    const Fake = struct {
        fn lifetime(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.DeadlineExceeded);
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("stopped coordinator must not acquire CPU");
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("stopped coordinator must not transfer CPU");
        }
    };
    var marker: u8 = 0;
    var hosted: HostedParticipantWorker = .{ .catalog = undefined, .router = undefined, .writes = undefined, .executor = undefined };
    var worker = hosted.worker();
    try std.testing.expect(worker.supportsIndependent());
    worker.execution_runnable = .{ .context = &marker, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .check_lifetime = Fake.lifetime, .checkpoint = Fake.checkpoint, .park = Fake.transition, .enter = Fake.transition };
    try std.testing.expectError(error.DeadlineExceeded, worker.beginGroup(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .begin_timestamp = 1, .participants = &.{} }));
    try std.testing.expectError(error.DeadlineExceeded, worker.prepareGroup(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .req = .{} }));
    try std.testing.expectError(error.DeadlineExceeded, worker.resolveGroup(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .status = .aborted, .commit_version = 2 }));
    try std.testing.expectError(error.DeadlineExceeded, worker.statusGroup(std.testing.allocator, 1, "docs", @splat(1)));
    try std.testing.expectError(error.DeadlineExceeded, worker.acknowledgeGroup(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .participant = "follower" }));
}

test "distributed txn independently scheduled coordinator reserves reusable packets before first contact" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const allocation = @import("../common/allocation_budget.zig");
    const writes = @import("runnable_write_team.zig");
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        root: *native.Execution,
        memory: *allocation.Shared,
        root_context: *anyopaque,
        begins: std.atomic.Value(usize) = .init(0),
        prepares: std.atomic.Value(usize) = .init(0),
        resolves: std.atomic.Value(usize) = .init(0),
        acknowledgements: usize = 0,
        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            return error.TestUnscheduledParticipant;
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.TestUnscheduledParticipant;
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.TestUnscheduledParticipant;
        }
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
        fn check(self: *@This(), borrow: runnable_abi.Borrowed) !void {
            try borrow.checkLifetime();
            try std.testing.expectEqual(@as(usize, 1), borrow.cpu_slots);
            try std.testing.expectEqual(runnable_abi.Class.write, try borrow.currentClass());
            try std.testing.expectEqual(@as(usize, 2), self.root.entry.?.children);
            try std.testing.expect(self.memory.used.load(.acquire) > 0);
        }
        fn beginControlled(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, _: TxnBeginRequest, borrow: runnable_abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.check(borrow);
            try std.testing.expectEqual(group == 7001, borrow.context == self.root_context);
            _ = self.begins.fetchAdd(1, .acq_rel);
        }
        fn prepareControlled(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest, borrow: runnable_abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.check(borrow);
            try std.testing.expect(borrow.context != self.root_context);
            _ = self.prepares.fetchAdd(1, .acq_rel);
        }
        fn resolveControlled(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, req: TxnResolveRequest, _: db_mod.types.CancellationToken, borrow: runnable_abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.check(borrow);
            try std.testing.expectEqual(group == 7001, borrow.context == self.root_context);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            _ = self.resolves.fetchAdd(1, .acq_rel);
        }
        fn acknowledgeControlled(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, req: TxnAcknowledgeRequest, borrow: runnable_abi.Borrowed) !void {
            try borrow.checkLifetime();
            try acknowledge(raw, alloc, group, table, req);
        }
        fn acknowledge(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 7001), group);
            try std.testing.expectEqual(@as(usize, 2), self.root.entry.?.children);
            self.acknowledgements += 1;
        }
        fn worker(self: *@This()) ParticipantWorker {
            return .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status, .acknowledge_group = acknowledge, .acknowledge_group_with_execution = acknowledgeControlled, .begin_group_with_execution = beginControlled, .prepare_group_with_execution = prepareControlled, .resolve_group_with_execution = resolveControlled } };
        }
    };
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    const Case = enum { success, allocator_denied, operation_denied };
    for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_operations = if (case == .operation_denied) 4 else 16 });
        defer scheduler.close();
        var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = if (case == .allocator_denied) 0 else 64 * 1024 };
        var root: native.Execution = .{};
        defer root.finish();
        try root.start(&scheduler, .{ .io = io, .retained_bytes = 256 }, .write);
        var bridge = try writes.Completion.init(&root, memory.allocator());
        var team = try bridge.team();
        try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s });
        try team.borrow.activateCompletion();
        var recorder: Recorder = .{ .root = &root, .memory = &memory, .root_context = team.borrow.context };
        const outcome = executeMultiTableCommitWithOptions(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), @splat(1), 10000, 10001, &.{.{ .table_name = "docs", .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" } } }}, .write, null, .{ .team = &team, .fanout_io = io, .max_parallel_participants = 2 });
        if (case == .success) {
            const result = try outcome;
            try std.testing.expect(result == .committed);
            try std.testing.expect(!result.committed.propagation_pending);
            try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
            try std.testing.expectEqual(@as(usize, 2), recorder.begins.load(.acquire));
            try std.testing.expectEqual(@as(usize, 2), recorder.prepares.load(.acquire));
            try std.testing.expectEqual(@as(usize, 2), recorder.resolves.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), recorder.acknowledgements);
            try std.testing.expect(team.state == .joined);
        } else {
            try std.testing.expectError(if (case == .allocator_denied) error.OutOfMemory else error.SchedulerFull, outcome);
            try std.testing.expectEqual(@as(usize, 0), recorder.begins.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), recorder.prepares.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), recorder.resolves.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), recorder.acknowledgements);
            if (case == .allocator_denied) {
                try std.testing.expect(team.state == .fresh);
            } else {
                try std.testing.expect(team.state == .joined);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
        try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
    }
}

test "distributed txn independently scheduled hosted coordinator waits preserve original budget and health progress" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const writes = @import("runnable_write_team.zig");
    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .group_node_ids = groupNodeIds,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 99;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn groupNodeIds(_: *anyopaque, alloc: std.mem.Allocator, _: u64, _: table_router.RouteBudget) ![]u64 {
            return try alloc.dupe(u64, &.{ 1, 2, 3 });
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
            return if (node_id >= 1 and node_id <= 3) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
        }
    };

    const Clock = struct {
        var current: u64 = 0;
        fn now() u64 {
            return current;
        }
    };
    const Executor = struct {
        scheduler: *native.Scheduler,
        root: *native.Execution,
        ceiling_ms: u32 = 14000,
        calls: usize = 0,
        many: usize = 0,
        fail_resume: bool = false,
        uncertain_many: bool = false,
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const timeout = request.timeout_ms orelse return error.TestMissingTimeout;
            try std.testing.expect(timeout > 0 and timeout <= self.ceiling_ms);
            const wait = request.execution_wait orelse return error.TestMissingExecutionWait;
            try wait.beginWait();
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            var health: native.Execution = .{};
            defer health.finish();
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try health.checkpoint();
            health.finish();
            if (self.fail_resume) Clock.current = 20 * std.time.ns_per_s;
            try wait.endWait();
            try std.testing.expectEqual(native.Class.write, try self.root.retainedClass());
            if (std.mem.endsWith(u8, request.uri, "/txn-acknowledge-many")) {
                self.many += 1;
                if (self.uncertain_many) return http_route_helpers.textResponseWithHeaders(alloc, 404, "write outcome unknown", &.{.{ .name = internal_batch_forwarding.outcome_header, .value = internal_batch_forwarding.outcome_unknown_v1 }});
                return .{ .status = 404, .body = try alloc.dupe(u8, "not found") };
            }
            return .{ .status = 200, .body = try alloc.dupe(u8, if (std.mem.endsWith(u8, request.uri, "/txn-status")) "{\"status\":\"committed\"}" else "") };
        }
    };
    Clock.current = 0;
    defer Clock.current = 0;
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var root: native.Execution = .{};
    defer root.finish();
    try root.start(&scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 5 * std.time.ns_per_s, .retained_bytes = 256 }, .write);
    var bridge = try writes.Completion.init(&root, std.testing.allocator);
    var team = try bridge.team();
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 20 * std.time.ns_per_s });
    try team.borrow.activateCompletion();
    Clock.current = 6 * std.time.ns_per_s;
    var executor: Executor = .{ .scheduler = &scheduler, .root = &root };
    var hosted = HostedParticipantWorker.init(undefined, FakeRouter.iface(), undefined, executor.iface());
    var worker = hosted.worker();
    worker.execution_runnable = team.borrow;
    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, try worker.statusGroup(std.testing.allocator, 7001, "docs", @splat(1)));
    Clock.current = 10 * std.time.ns_per_s;
    executor.ceiling_ms = 10000;
    try worker.acknowledgeManyGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .participants = &.{ "table2:00000004:docs:7002", "table2:00000004:docs:7003" } });
    try std.testing.expectEqual(@as(usize, 1), executor.many);
    try std.testing.expectEqual(@as(usize, 4), executor.calls);
    executor.uncertain_many = true;
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, worker.acknowledgeManyGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .participants = &.{"table2:00000004:docs:7002"} }));
    // An uncertain batch cannot be converted into claimed per-participant ACKs.
    try std.testing.expectEqual(@as(usize, 5), executor.calls);
    executor.uncertain_many = false;
    const retry_limit = platform_time.monotonicNs() + 2 * std.time.ns_per_s;
    executor.ceiling_ms = 2000;
    try worker.resolveGroupUntil(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .status = .committed, .commit_version = 2 }, retry_limit);
    try std.testing.expect(hosted.execution_wait == null and hosted.execution_deadline_ns == null);
    try sleepRecoveryNs(std.time.ns_per_ms, team.borrow);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    executor.ceiling_ms = 10000;
    executor.fail_resume = true;
    try std.testing.expectError(error.DeadlineExceeded, worker.acknowledgeGroup(std.testing.allocator, 7001, "docs", .{ .txn_id = @splat(1), .participant = "table2:00000004:docs:7002" }));
    const before = executor.calls;
    try std.testing.expectError(error.DeadlineExceeded, worker.statusGroup(std.testing.allocator, 7001, "docs", @splat(1)));
    try std.testing.expectEqual(before, executor.calls);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    root.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "distributed txn independently scheduled recovery backoff attempts original resumption exactly once" {
    const runtime = @import("antfly_runtime_abi");
    const Probe = struct {
        parks: usize = 0,
        enters: usize = 0,
        fn ok(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return .ok;
        }
        fn current(_: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
            out.* = @backingInt(runnable_abi.Class.write);
            return .ok;
        }
        fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(reason == @backingInt(runnable_abi.Wait.resource));
            self.parks += 1;
            return .ok;
        }
        fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(class == @backingInt(runnable_abi.Class.write));
            self.enters += 1;
            return runtime.error_abi.statusFromError(error.SchedulerClosed);
        }
    };
    var probe: Probe = .{};
    const borrow: runnable_abi.Borrowed = .{ .context = &probe, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .checkpoint = Probe.ok, .check_lifetime = Probe.ok, .current_class = Probe.current, .park = Probe.park, .enter = Probe.enter };
    try std.testing.expectError(error.SchedulerClosed, sleepRecoveryNs(0, borrow));
    try std.testing.expectEqual(@as(usize, 1), probe.parks);
    try std.testing.expectEqual(@as(usize, 1), probe.enters);
}

test "distributed txn independently scheduled coordinator rejects legacy unscoped callback fallback" {
    const runtime = @import("antfly_runtime_abi");
    const Probe = struct {
        calls: usize = 0,
        fn ok(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("unexpected CPU transition");
        }
        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            @panic("unexpected legacy BEGIN");
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            @panic("unexpected legacy prepare");
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            @panic("unexpected legacy resolve");
        }
        fn status(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return .pending;
        }
        fn ack(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
        fn ackMany(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeManyRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
    };
    var probe: Probe = .{};
    const worker: ParticipantWorker = .{ .ptr = &probe, .execution_runnable = .{ .context = &probe, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .checkpoint = Probe.ok, .check_lifetime = Probe.ok, .park = Probe.transition, .enter = Probe.transition }, .vtable = &.{ .begin_group = Probe.begin, .prepare_group = Probe.prepare, .resolve_group = Probe.resolve, .status_group = Probe.status, .acknowledge_group = Probe.ack, .acknowledge_many_group = Probe.ackMany } };
    const deadline = platform_time.monotonicNs() + std.time.ns_per_s;
    try std.testing.expectError(error.CommitDecisionUnknown, worker.statusGroup(std.testing.allocator, 1, "docs", @splat(1)));
    try std.testing.expectError(error.CommitDecisionUnknown, worker.statusGroupUntil(std.testing.allocator, 1, "docs", @splat(1), deadline));
    try std.testing.expectError(error.CommitDecisionUnknown, worker.resolveGroupUntil(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .status = .committed, .commit_version = 2 }, deadline));
    try std.testing.expectError(error.UnsupportedOperation, worker.acknowledgeGroup(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .participant = "table2:00000004:docs:2" }));
    try std.testing.expectError(error.UnsupportedOperation, worker.acknowledgeManyGroup(std.testing.allocator, 1, "docs", .{ .txn_id = @splat(1), .participants = &.{"table2:00000004:docs:2"} }));
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
}
const PreparedCoordinatorFixture = struct {
    const ready: preparation_protocol.Capabilities = .{
        .protocol = 1,
        .requirement_format = 1,
        .effects_format = 1,
        .minimum_reader = 7,
        .permanent_reader = 7,
        .catalog_envelope = 7,
        .durable_begin = true,
        .durable_seal = true,
        .recovery_consumes_effects = true,
        .retained_contract = true,
        .bound_retained_contract = true,
        .catalog_migration_complete = true,
        .import_policy_fenced = true,
        .deferred = true,
        .synchronous = true,
    };
    const Event = enum { capability, begin, prepare, seal, commit, abort };
    events: [64]Event = undefined,
    count: usize = 0,
    states: [2]db_mod.types.TxnStatus = .{ .pending, .pending },
    begins: usize = 0,
    prepares: usize = 0,
    seals: usize = 0,
    commits: usize = 0,
    aborts: usize = 0,
    blocked: u64 = 0,
    race_legacy_after_begin: bool = false,
    fail_prepare: u64 = 0,
    fail_seal: u64 = 0,
    mismatch_seal: bool = false,
    mode: preparation_protocol.Mode = .synchronous,
    decision: ?ParticipantWorker.PreparationDecision = null,
    saved_bindings: [2]?@import("antfly_local_sources").storage_prepared_transaction_effects.Binding = .{ null, null },
    status_override: ?ParticipantWorker.PreparationDecision = null,
    typed_status_calls: usize = 0,
    legacy_status_calls: usize = 0,
    fail_commit_reply: bool = false,
    plan_store: ?contract.ParticipantPlanStore = null,
    catalog_override: ?table_catalog.CatalogSource = null,
    fn event(self: *@This(), e: Event) void {
        self.events[self.count] = e;
        self.count += 1;
    }
    fn cast(raw: *anyopaque) *@This() {
        return @ptrCast(@alignCast(raw));
    }
    fn catalog() table_catalog.CatalogSource {
        return .{ .ptr = undefined, .vtable = &.{ .admin_snapshot = snapshot, .free_admin_snapshot = freeSnapshot, .route_fence = routeFence, .route_identity = routeIdentity } };
    }
    fn snapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .metrics = .{} },
            .tables = @constCast((&[_]@import("../metadata/table_manager.zig").TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
            .ranges = @constCast((&[_]@import("../metadata/table_manager.zig").RangeRecord{
                .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
            })[0..]),
            .stores = @constCast((&[_]@import("../metadata/table_manager.zig").StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]@import("../raft/reconciler.zig").PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]@import("../metadata/transition_state.zig").SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]@import("../metadata/transition_state.zig").MergeTransitionRecord{})[0..]),
        };
    }
    fn freeSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    fn routeFence(_: *anyopaque, group: u64) !?@import("../metadata/api.zig").CatalogRouteFence {
        var captured = (try table_catalog.transactionRoutingSnapshot(std.testing.allocator, catalog(), "docs")).?;
        defer captured.deinit(std.testing.allocator);
        return .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 1, .table_id = 7, .topology_epoch = captured.topology_epoch, .route = .{ .group_id = group, .range_id = group, .identity_namespace = .{ .table_id = 7, .shard_id = group, .range_id = group } } };
    }
    fn routeIdentity(_: *anyopaque, table: []const u8, group: u64) !?@import("../metadata/api.zig").CatalogIdentityNamespace {
        if (!std.mem.eql(u8, table, "docs")) return null;
        return .{ .table_id = 7, .shard_id = group, .range_id = group };
    }

    fn capability(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, _: PreDecisionContext, _: ?runnable_abi.Borrowed) !preparation_protocol.Capabilities {
        const self = cast(raw);
        self.event(.capability);
        if (group == self.blocked) return .{ .protocol = 1 };
        return ready;
    }
    fn beginLegacy(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
        return error.TestLegacyFallback;
    }
    fn prepareLegacy(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
        return error.TestLegacyFallback;
    }
    fn savedBinding(group: u64) !@import("antfly_local_sources").storage_prepared_transaction_effects.Binding {
        const locator = try participantIdForGroup(std.testing.allocator, "docs", group);
        defer std.testing.allocator.free(locator);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(locator, &digest, .{});
        const fence = (try routeFence(undefined, group)).?;
        return .{ .group_id = group, .topology_epoch = fence.topology_epoch, .participant_digest = digest, .route_digest = preparation_protocol.routeDigest(fence) };
    }
    fn begin(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: TxnBeginRequest, requirement: preparation_protocol.Requirement, context: PreDecisionContext, _: ?runnable_abi.Borrowed) !void {
        const self = cast(raw);
        try std.testing.expectEqual(self.mode, requirement.mode);
        try std.testing.expectEqual(@as(usize, 2), self.count - self.begins);
        self.event(.begin);
        self.begins += 1;
        const locator = try participantIdForGroupScoped(alloc, table, group, request.restore_staging_scope, request.restore_staging_plan_id);
        defer alloc.free(locator);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(locator, &digest, .{});
        self.saved_bindings[if (group == 7001) 0 else 1] = .{ .group_id = group, .topology_epoch = request.topology_epoch, .participant_digest = digest, .route_digest = preparation_protocol.routeDigest(context.route_fence.?) };
        if (self.race_legacy_after_begin) return error.DecisionConflict;
    }
    fn prepare(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, _: TxnPrepareRequest, requirement: preparation_protocol.Requirement, _: PreDecisionContext, _: ?runnable_abi.Borrowed) !void {
        const self = cast(raw);
        try std.testing.expectEqual(self.mode, requirement.mode);
        self.event(.prepare);
        self.prepares += 1;
        if (group == self.fail_prepare) return error.TestProducerFailed;
    }
    fn seal(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, request: preparation_protocol.SealRequest, _: PreDecisionContext, _: ?runnable_abi.Borrowed) !preparation_protocol.SealReceipt {
        const self = cast(raw);
        try std.testing.expectEqual(@as(usize, 2), self.prepares);
        try std.testing.expectEqual(self.mode, request.requirement.mode);
        self.event(.seal);
        if (group == self.fail_seal) return error.TestSealFailed;
        self.seals += 1;
        var receipt: preparation_protocol.SealReceipt = .{ .request = request, .intent_revision = 1, .intent_digest = @splat(1), .catalog_digest = @splat(2), .frame_digest = @splat(3) };
        if (self.mismatch_seal) receipt.request.topology_epoch +%= 1;
        return receipt;
    }
    fn resolve(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, request: TxnResolveRequest) !void {
        const self = cast(raw);
        self.states[if (group == 7001) 0 else 1] = request.status;
        if (request.status == .committed) {
            if (self.decision == null or self.decision.?.status != .committed) try std.testing.expectEqual(@as(usize, 2), self.seals);
            self.event(.commit);
            self.commits += 1;
            if (group == 7001) {
                const prior = self.decision;
                self.decision = .{ .status = .committed, .requirement = if (prior) |old| old.requirement else .{ .mode = self.mode }, .binding = self.saved_bindings[0] orelse if (prior) |old| old.binding else null };
                if (self.fail_commit_reply) {
                    self.fail_commit_reply = false;
                    return error.Timeout;
                }
            }
        } else {
            self.event(.abort);
            self.aborts += 1;
            self.decision = .{ .status = .aborted, .requirement = null };
        }
    }
    fn status(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        const self = cast(raw);
        self.legacy_status_calls += 1;
        return if (group == 7001 and self.decision != null) self.decision.?.status else self.states[if (group == 7001) 0 else 1];
    }
    fn resolveUntil(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: TxnResolveRequest, _: u64) !void {
        return resolve(raw, alloc, group, table, request);
    }
    fn statusUntil(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, txn_id: db_mod.types.TxnId, _: u64) !db_mod.types.TxnStatus {
        return status(raw, alloc, group, table, txn_id);
    }
    fn resolveExecution(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: TxnResolveRequest, _: db_mod.types.CancellationToken, borrow: runnable_abi.Borrowed) !void {
        try borrow.checkLifetime();
        return resolve(raw, alloc, group, table, request);
    }
    fn resolveUntilExecution(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: TxnResolveRequest, _: u64, borrow: runnable_abi.Borrowed) !void {
        try borrow.checkLifetime();
        return resolve(raw, alloc, group, table, request);
    }
    fn preparedStatus(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnStatusRequest, _: u64, _: ?runnable_abi.Borrowed) !ParticipantWorker.PreparationDecision {
        const self = cast(raw);
        self.typed_status_calls += 1;
        if (self.status_override) |value| return value;
        if (self.race_legacy_after_begin and self.begins != 0) return .{ .status = .committed, .requirement = null };
        return self.decision orelse return error.TxnNotFound;
    }
    fn acknowledge(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {}
    fn worker(self: *@This()) ParticipantWorker {
        return .{ .ptr = self, .vtable = &.{ .begin_group = beginLegacy, .prepare_group = prepareLegacy, .resolve_group = resolve, .status_group = status, .acknowledge_group = acknowledge, .resolve_group_until = resolveUntil, .status_group_until = statusUntil, .resolve_group_with_execution = resolveExecution, .resolve_group_until_with_execution = resolveUntilExecution, .preparation_capabilities = capability, .required_begin_group = begin, .required_prepare_group = prepare, .seal_group = seal, .preparation_status_group = preparedStatus } };
    }
    fn run(self: *@This(), level: db_mod.types.SyncLevel, retained: bool) !CommitOutcome {
        return executeMultiTableCommitWithOptions(std.testing.allocator, self.catalog_override orelse catalog(), self.worker(), @splat(1), 10000, 10001, &.{.{ .table_name = "docs", .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" } } }}, level, null, .{ .preparation_requirement = .{ .mode = self.mode }, .participant_plan_store = self.plan_store, .retain_terminal = retained, .report_post_commit_failure = false });
    }
};

test "distributed txn prepared protocol negotiates every peer and seals all before first commit" {
    for (std.meta.tags(db_mod.types.SyncLevel)) |level| {
        var fixture: PreparedCoordinatorFixture = .{ .mode = preparation_protocol.forSyncLevel(level).mode };
        const result = try fixture.run(level, false);
        try std.testing.expect(result == .committed);
        try std.testing.expectEqual(level == .propose, result.committed.propagation_pending);
        try std.testing.expectEqual(@as(usize, 2), fixture.begins);
        try std.testing.expectEqual(@as(usize, 2), fixture.prepares);
        try std.testing.expectEqual(@as(usize, 2), fixture.seals);
        try std.testing.expectEqual(@as(usize, 2), fixture.commits);
        try std.testing.expectEqual(@as(usize, 0), fixture.aborts);
    }
}

test "distributed txn prepared protocol refuses codec-only peers before begin" {
    var fixture: PreparedCoordinatorFixture = .{ .blocked = 7002 };
    try std.testing.expectError(error.PreparedTransactionProtocolUnavailable, fixture.run(.enrichments, false));
    try std.testing.expectEqual(@as(usize, 0), fixture.begins);
    try std.testing.expectEqual(@as(usize, 0), fixture.aborts);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
}

test "distributed txn prepared protocol producer seal failure and stale receipt precede decision" {
    for (0..3) |failure| {
        var fixture: PreparedCoordinatorFixture = .{ .fail_prepare = if (failure == 0) 7002 else 0, .fail_seal = if (failure == 1) 7002 else 0, .mismatch_seal = failure == 2 };
        const result = fixture.run(.full_index, false);
        if (failure == 0) try std.testing.expectError(error.TestProducerFailed, result) else if (failure == 1) try std.testing.expectError(error.TestSealFailed, result) else try std.testing.expectError(error.PreparedEffectsMismatch, result);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
        try std.testing.expect(fixture.aborts != 0);
    }
}

test "distributed txn prepared protocol retained terminal decisions never upgrade or require new readiness" {
    var committed: PreparedCoordinatorFixture = .{ .blocked = 7001, .decision = .{ .status = .committed, .requirement = .{ .mode = .synchronous }, .binding = try PreparedCoordinatorFixture.savedBinding(7001) } };
    try std.testing.expect((try committed.run(.full_index, true)) == .committed);
    try std.testing.expectEqual(@as(usize, 0), committed.begins);
    try std.testing.expectEqual(@as(usize, 0), committed.prepares);
    try std.testing.expectEqual(@as(usize, 0), committed.seals);
    try std.testing.expectEqual(@as(usize, 2), committed.commits);
    var weaker: PreparedCoordinatorFixture = .{ .blocked = 7001, .decision = .{ .status = .committed, .requirement = .{ .mode = .synchronous }, .binding = try PreparedCoordinatorFixture.savedBinding(7001) } };
    try std.testing.expect((try weaker.run(.write, true)) == .committed);
    try std.testing.expectEqual(@as(usize, 0), weaker.begins);
    try std.testing.expectEqual(@as(usize, 2), weaker.commits);
    for ([_]?preparation_protocol.Requirement{ null, .{ .mode = .deferred } }) |requirement| {
        var legacy: PreparedCoordinatorFixture = .{ .decision = .{ .status = .committed, .requirement = requirement } };
        try std.testing.expectError(error.PreparationRequirementMismatch, legacy.run(.full_index, true));
        try std.testing.expectEqual(@as(usize, 0), legacy.count);
    }
    var aborted: PreparedCoordinatorFixture = .{ .decision = .{ .status = .aborted, .requirement = null } };
    try std.testing.expect((try aborted.run(.full_index, true)) == .conflict);
    try std.testing.expectEqual(@as(usize, 0), aborted.count);
}

test "distributed txn prepared protocol framing rejects stripped JSON altered phase and corrupt bytes" {
    const alloc = std.testing.allocator;
    const payload = try encodeTxnBeginRequest(alloc, .{ .txn_id = @splat(0), .begin_timestamp = 1, .participants = &.{} });
    defer alloc.free(payload);
    for (std.meta.tags(preparation_protocol.Phase)) |phase| {
        const bytes = try preparation_protocol.encode(alloc, phase, .{ .mode = .synchronous }, payload, null);
        defer alloc.free(bytes);
        const decoded = try preparation_protocol.decode(bytes, phase, null);
        try std.testing.expectEqual(preparation_protocol.Mode.synchronous, decoded.requirement.mode);
        try std.testing.expectEqualStrings(payload, decoded.payload);
        try std.testing.expectError(error.InvalidPreparationRequirement, preparation_protocol.decode(payload, phase, null));
        if (parseTxnBeginRequest(alloc, bytes)) |value| {
            var unexpected = value;
            freeTxnBeginRequest(alloc, &unexpected);
            return error.TestExpectedError;
        } else |_| {}
        for (0..bytes.len) |i| {
            const corrupted = try alloc.dupe(u8, bytes);
            defer alloc.free(corrupted);
            corrupted[i] ^= 1;
            if (preparation_protocol.decode(corrupted, phase, null)) |_| return error.TestExpectedError else |_| {}
            if (preparation_protocol.decode(bytes[0..i], phase, null)) |_| return error.TestExpectedError else |_| {}
        }
        const other: preparation_protocol.Phase = if (phase == .begin) .prepare else .begin;
        try std.testing.expectError(error.InvalidPreparationRequirement, preparation_protocol.decode(bytes, other, null));
    }
}

test "distributed txn prepared protocol framing preserves every original rejection and allocation failure" {
    const admission = @import("antfly_local_sources").common_read_admission;
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const n = self.calls;
            self.calls += 1;
            if (n == self.fail_at) return self.cause;
        }
    };
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const encoded = try preparation_protocol.encode(alloc, .prepare, .{ .mode = .deferred }, "payload", null);
            defer alloc.free(encoded);
            _ = try preparation_protocol.decode(encoded, .prepare, null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const payload: [10000]u8 = @splat('x');
    var baseline: Control = .{};
    const initial: admission.Payload = .{ .ptr = &baseline, .vtable = &.{ .checkpoint = Control.check } };
    const encoded = try preparation_protocol.encode(std.testing.allocator, .prepare, .{ .mode = .deferred }, &payload, admission.Port.init(&initial));
    defer std.testing.allocator.free(encoded);
    _ = try preparation_protocol.decode(encoded, .prepare, admission.Port.init(&initial));
    for ([_]anyerror{ error.NotFound, error.Canceled, error.MemoryLimitExceeded }) |cause| for (0..baseline.calls) |fail_at| {
        var state: Control = .{ .fail_at = fail_at, .cause = cause };
        const control: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Control.check } };
        const result = preparation_protocol.encode(std.testing.allocator, .prepare, .{ .mode = .deferred }, &payload, admission.Port.init(&control));
        if (result) |bytes| {
            defer std.testing.allocator.free(bytes);
            try std.testing.expectError(cause, preparation_protocol.decode(bytes, .prepare, admission.Port.init(&control)));
        } else |err| try std.testing.expectEqual(cause, err);
    };
}

test "distributed txn prepared protocol honors supplied authority and detaches it deliberately for recovery" {
    const runtime = @import("antfly_runtime_abi");
    const Stop = struct {
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.Canceled);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("stopped caller cannot enter or park");
        }
    };
    var fixture: PreparedCoordinatorFixture = .{};
    const borrow: runnable_abi.Borrowed = .{ .context = &fixture, .cpu_slots = 1, .parent_class = @backingInt(runnable_abi.Class.write), .check_lifetime = Stop.check, .checkpoint = Stop.check, .park = Stop.phase, .enter = Stop.phase };
    try std.testing.expectError(error.Canceled, executeMultiTableCommitWithOptions(std.testing.allocator, undefined, fixture.worker(), @splat(1), 1, 2, &.{}, .write, null, .{ .preparation_requirement = .{ .mode = .synchronous }, .pre_decision_context = .{ .execution_runnable = borrow } }));
    try std.testing.expectEqual(@as(usize, 0), fixture.count);
    var detached = fixture.worker();
    detached.preparation = .{ .mode = .synchronous };
    detached.preparation_context.execution_runnable = borrow;
    detached.execution_runnable = null;
    try detached.checkExecution();
    try std.testing.expect(detached.preparationContext().execution_runnable == null);
}

test "distributed txn prepared protocol requires each migration and durable capability" {
    inline for (.{ "durable_begin", "durable_seal", "recovery_consumes_effects", "retained_contract", "bound_retained_contract", "catalog_migration_complete", "import_policy_fenced", "synchronous" }) |field| {
        var capabilities = PreparedCoordinatorFixture.ready;
        @field(capabilities, field) = false;
        try std.testing.expectError(error.PreparedTransactionProtocolUnavailable, capabilities.require(.{ .mode = .synchronous }));
    }
    inline for (.{ "minimum_reader", "permanent_reader", "catalog_envelope" }) |field| {
        var capabilities = PreparedCoordinatorFixture.ready;
        @field(capabilities, field) = 6;
        try std.testing.expectError(error.PreparedTransactionProtocolUnavailable, capabilities.require(.{ .mode = .synchronous }));
    }
}

test "distributed txn prepared protocol seal binds transaction participant route and exact mode" {
    const request: preparation_protocol.SealRequest = .{ .txn_id = @splat(1), .group_id = 2, .topology_epoch = 3, .participant_digest = @splat(4), .route_digest = @splat(5), .requirement = .{ .mode = .synchronous } };
    const receipt: preparation_protocol.SealReceipt = .{ .request = request, .intent_revision = 0, .intent_digest = @splat(6), .catalog_digest = @splat(7), .frame_digest = @splat(8) };
    try receipt.validate(request);
    for (0..6) |field| {
        var wrong = receipt;
        switch (field) {
            0 => wrong.request.txn_id[0] ^= 1,
            1 => wrong.request.group_id += 1,
            2 => wrong.request.topology_epoch += 1,
            3 => wrong.request.participant_digest[0] ^= 1,
            4 => wrong.request.route_digest[0] ^= 1,
            5 => wrong.request.requirement.mode = .deferred,
            else => unreachable,
        }
        try std.testing.expectError(error.PreparedEffectsMismatch, wrong.validate(request));
    }
}

test "distributed txn prepared protocol missing aware callbacks cannot reach legacy begin" {
    inline for (.{ "required_begin_group", "required_prepare_group", "seal_group", "preparation_status_group", "preparation_capabilities" }) |field| {
        var fixture: PreparedCoordinatorFixture = .{};
        var worker = fixture.worker();
        var callbacks = worker.vtable.*;
        @field(callbacks, field) = null;
        worker.vtable = &callbacks;
        worker.preparation = .{ .mode = .synchronous };
        try std.testing.expectError(error.PreparedTransactionProtocolUnavailable, worker.negotiatePreparation(std.testing.allocator, 7001, "docs"));
        try std.testing.expectEqual(@as(usize, 0), fixture.count);
    }
}

test "distributed txn prepared protocol header is fixed and route digest excludes local admission" {
    const encoded = try preparation_protocol.encode(std.testing.allocator, .prepare, .{ .mode = .synchronous }, "abc", null);
    defer std.testing.allocator.free(encoded);
    const header = [_]u8{ 'A', 'T', 'P', 'W', 1, 0, 2, 1, 3, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectEqual(@as(usize, 51), encoded.len);
    try std.testing.expectEqualSlices(u8, &header, encoded[0..16]);
    try std.testing.expectEqualStrings("abc", encoded[16..19]);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(encoded[0..19], &digest, .{});
    try std.testing.expectEqualSlices(u8, &digest, encoded[19..]);
    var fence = (try PreparedCoordinatorFixture.routeFence(undefined, 7001)).?;
    const expected = preparation_protocol.routeDigest(fence);
    fence.admission_deadline_ns = 1234;
    try std.testing.expectEqualSlices(u8, &expected, &preparation_protocol.routeDigest(fence));
    fence.catalog_revision += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &preparation_protocol.routeDigest(fence)));
}

test "distributed txn prepared protocol ambiguous begin cannot recover through legacy status" {
    var fixture: PreparedCoordinatorFixture = .{ .race_legacy_after_begin = true };
    try std.testing.expectError(error.CommitDecisionUnknown, fixture.run(.full_index, true));
    try std.testing.expectEqual(@as(usize, 1), fixture.begins);
    try std.testing.expectEqual(@as(usize, 0), fixture.prepares);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), fixture.aborts);
}

test "distributed txn original cohort survives routing loss and failed publication never begins" {
    const Store = struct {
        bytes: ?[]u8 = null,
        fail: bool = false,
        reads: usize = 0,
        writes: usize = 0,
        fn load(raw: *anyopaque, alloc: std.mem.Allocator, _: db_mod.types.TxnId) !contract.ParticipantPlanStore.Loaded {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return if (self.bytes) |bytes| .{ .planned = try alloc.dupe(u8, bytes) } else .unplanned;
        }
        fn seal(raw: *anyopaque, alloc: std.mem.Allocator, _: db_mod.types.TxnId, bytes: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.writes += 1;
            if (self.fail) return error.TestPlanStoreFailed;
            if (self.bytes == null) self.bytes = try std.testing.allocator.dupe(u8, bytes);
            return alloc.dupe(u8, self.bytes.?);
        }
        fn port(self: *@This()) contract.ParticipantPlanStore {
            return .{ .ptr = self, .load = load, .seal = seal };
        }
        fn unavailable(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            return error.TestCatalogUnavailable;
        }
    };
    for (std.meta.tags(preparation_protocol.Mode)) |mode| {
        var store: Store = .{ .fail = true };
        defer if (store.bytes) |bytes| std.testing.allocator.free(bytes);
        var first: PreparedCoordinatorFixture = .{ .mode = mode, .plan_store = store.port() };
        try std.testing.expectError(error.TestPlanStoreFailed, first.run(.write, true));
        try std.testing.expectEqual(@as(usize, 0), first.begins);
        store.fail = false;
        try std.testing.expect((try first.run(.write, true)) == .committed);
        try std.testing.expectEqual(@as(usize, 2), first.begins);
        const durable = try parseRoutedPlan(std.testing.allocator, store.bytes.?, @splat(1), 10000, .{ .mode = mode });
        defer durable.deinit();
        try std.testing.expectEqual(@as(usize, 2), durable.value.participants.len);
        try std.testing.expectEqual(@as(u64, 7001), durable.value.participants[0].group_id);
        try std.testing.expectEqual(@as(u64, 7002), durable.value.participants[1].group_id);
        const unavailable: table_catalog.CatalogSource = .{ .ptr = &store, .vtable = &.{ .admin_snapshot = Store.unavailable, .free_admin_snapshot = PreparedCoordinatorFixture.freeSnapshot } };
        var retry: PreparedCoordinatorFixture = .{ .mode = mode, .plan_store = store.port(), .catalog_override = unavailable, .blocked = 7001, .decision = .{ .status = .committed, .requirement = .{ .mode = mode }, .binding = try PreparedCoordinatorFixture.savedBinding(7001) } };
        const result = try retry.run(.write, true);
        try std.testing.expect(result == .committed);
        // The returned label still borrows the caller's request after the
        // temporary decoded cohort and its arena have been destroyed.
        try std.testing.expectEqualStrings("docs", result.committed.coordinator_table_name.?);
        try std.testing.expectEqual(@as(u64, 7001), result.committed.coordinator_group_id.?);
        try std.testing.expectEqual(@as(usize, 0), retry.begins);
        try std.testing.expectEqual(@as(usize, 0), retry.prepares);
        try std.testing.expectEqual(@as(usize, 2), retry.commits);
        try std.testing.expectEqual(@as(usize, 2), store.writes);
        var aborted: PreparedCoordinatorFixture = .{ .mode = mode, .plan_store = store.port(), .catalog_override = unavailable, .decision = .{ .status = .aborted, .requirement = null } };
        try std.testing.expect((try aborted.run(.write, true)) == .conflict);
        try std.testing.expectEqual(@as(usize, 0), aborted.begins);
        const other: preparation_protocol.Requirement = .{ .mode = if (mode == .synchronous) .deferred else .synchronous };
        try std.testing.expectError(error.InvalidTxnRequest, parseRoutedPlan(std.testing.allocator, store.bytes.?, @splat(2), 10000, .{ .mode = mode }));
        try std.testing.expectError(error.InvalidTxnRequest, parseRoutedPlan(std.testing.allocator, store.bytes.?, @splat(1), 10001, .{ .mode = mode }));
        try std.testing.expectError(error.InvalidTxnRequest, parseRoutedPlan(std.testing.allocator, store.bytes.?, @splat(1), 10000, other));
    }
}

test "distributed txn original cohort binary codec retires every allocation and preserves hidden identity" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var participant: ParticipantTxn = .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 9, .restore_staging_scope = @splat(1), .restore_staging_plan_id = @splat(2), .schema_version = 3, .row_policy_principal_proof = "\x00\xffproof", .row_policy_database = "default", .route_fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 4, .table_id = 7, .topology_epoch = 9, .route = .{ .group_id = 7001, .range_id = 7001, .identity_namespace = .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 } }, .admission_deadline_ns = 17 } };
            defer participant.deinit(alloc);
            const table: TableCommitRequest = .{ .table_name = "docs", .writes = &.{.{ .key = "\x00\xffkey", .value = "{}" }}, .predicates = &.{.{ .key = "\x00\xffkey", .expected_version = 5, .expected_content_digest = @splat(6) }}, .deletes = &.{"\x00\xffdeleted"} };
            try participant.writes.appendSlice(alloc, table.writes);
            try participant.predicates.appendSlice(alloc, table.predicates);
            try participant.deletes.appendSlice(alloc, table.deletes);
            try participant.range_guards.append(alloc, .{ .bucket = 0, .generation = 8 });
            const bytes = try encodeRoutedPlan(alloc, @splat(1), 10000, .{ .mode = .deferred }, &.{participant});
            defer alloc.free(bytes);
            var decoded = try parseRoutedPlan(alloc, bytes, @splat(1), 10000, .{ .mode = .deferred });
            defer decoded.deinit();
            try std.testing.expectEqual(@as(?u64, null), decoded.value.participants[0].route_fence.?.admission_deadline_ns);
            var restored = std.ArrayListUnmanaged(ParticipantTxn).empty;
            defer {
                for (restored.items) |*entry| entry.deinit(alloc);
                restored.deinit(alloc);
            }
            var keys = try originalPlanKeys(alloc, &.{table}, .{});
            defer keys.deinit(alloc);
            try appendRoutedParticipant(alloc, &restored, decoded.value.participants[0], &.{table}, &keys);
            const value = restored.items[0];
            try std.testing.expectEqual(participant.restore_staging_scope, value.restore_staging_scope);
            try std.testing.expectEqual(participant.restore_staging_plan_id, value.restore_staging_plan_id);
            try std.testing.expectEqualStrings(participant.row_policy_principal_proof, value.row_policy_principal_proof);
            try std.testing.expectEqualDeep(participant.range_guards.items, value.range_guards.items);
            try std.testing.expectEqualStrings(table.writes[0].key, value.writes.items[0].key);
            try std.testing.expectEqual(table.predicates[0].expected_content_digest, value.predicates.items[0].expected_content_digest);
            try std.testing.expect(value.writes.items[0].key.ptr == table.writes[0].key.ptr);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "distributed txn original cohort resumes legacy commits without fresh admission" {
    const Store = struct {
        bytes: []const u8,
        fn load(raw: *anyopaque, alloc: std.mem.Allocator, _: db_mod.types.TxnId) !contract.ParticipantPlanStore.Loaded {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .planned = try alloc.dupe(u8, self.bytes) };
        }
        fn seal(_: *anyopaque, _: std.mem.Allocator, _: db_mod.types.TxnId, _: []const u8) ![]u8 {
            return error.TestUnexpectedSeal;
        }
        fn unavailable(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            return error.TestCatalogUnavailable;
        }
    };
    const alloc = std.testing.allocator;
    const bytes = try encodeRoutedPlan(alloc, @splat(1), 10000, null, &.{ .{ .table_name = "docs", .group_id = 7001, .topology_epoch = 17 }, .{ .table_name = "docs", .group_id = 7002, .topology_epoch = 17 } });
    defer alloc.free(bytes);
    var store: Store = .{ .bytes = bytes };
    var fixture: PreparedCoordinatorFixture = .{ .decision = .{ .status = .committed, .requirement = null } };
    const unavailable: table_catalog.CatalogSource = .{ .ptr = &store, .vtable = &.{ .admin_snapshot = Store.unavailable, .free_admin_snapshot = PreparedCoordinatorFixture.freeSnapshot } };
    const result = try executeMultiTableCommitWithOptions(alloc, unavailable, fixture.worker(), @splat(1), 10000, 10001, &.{.{ .table_name = "docs" }}, .write, null, .{ .retain_terminal = true, .participant_plan_store = .{ .ptr = &store, .load = Store.load, .seal = Store.seal } });
    try std.testing.expect(result == .committed);
    try std.testing.expectEqual(@as(usize, 2), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), fixture.begins);
    try std.testing.expectEqual(@as(usize, 0), fixture.aborts);
}

test "distributed txn prepared protocol retained decisions reject every binding drift before mutation" {
    const good = try PreparedCoordinatorFixture.savedBinding(7001);
    for (std.meta.tags(preparation_protocol.Mode)) |mode| {
        for (0..5) |failure| {
            var changed = good;
            switch (failure) {
                0 => {},
                1 => changed.group_id += 1,
                2 => changed.topology_epoch +%= 1,
                3 => changed.participant_digest[0] ^= 1,
                4 => changed.route_digest[0] ^= 1,
                else => unreachable,
            }
            for ([_]db_mod.types.TxnStatus{ .pending, .committed }) |state| {
                var fixture: PreparedCoordinatorFixture = .{ .mode = mode, .decision = .{ .status = state, .requirement = .{ .mode = mode }, .binding = if (failure == 0) null else changed } };
                try std.testing.expectError(error.PreparationContractConflict, fixture.run(.write, true));
                try std.testing.expectEqual(@as(usize, 0), fixture.begins);
                try std.testing.expectEqual(@as(usize, 0), fixture.commits);
                try std.testing.expectEqual(@as(usize, 0), fixture.aborts);
                try std.testing.expectEqual(@as(usize, 0), fixture.legacy_status_calls);
            }
        }
    }
}

test "distributed txn prepared protocol ambiguous commit requires saved binding and never falls back to legacy status" {
    const good = try PreparedCoordinatorFixture.savedBinding(7001);
    for (std.meta.tags(preparation_protocol.Mode)) |mode| {
        var accepted: PreparedCoordinatorFixture = .{ .mode = mode, .fail_commit_reply = true };
        try std.testing.expect((try accepted.run(.write, false)) == .committed);
        try std.testing.expectEqual(@as(usize, 1), accepted.typed_status_calls);
        try std.testing.expectEqual(@as(usize, 0), accepted.legacy_status_calls);
        try std.testing.expectEqualDeep(good, accepted.decision.?.binding.?);
        try std.testing.expectEqual(@as(usize, 2), accepted.commits);
        for (0..6) |failure| {
            var changed = good;
            switch (failure) {
                0, 1 => {},
                2 => changed.group_id += 1,
                3 => changed.topology_epoch +%= 1,
                4 => changed.participant_digest[0] ^= 1,
                5 => changed.route_digest[0] ^= 1,
                else => unreachable,
            }
            var rejected: PreparedCoordinatorFixture = .{ .mode = mode, .fail_commit_reply = true, .status_override = .{ .status = .committed, .requirement = if (failure == 0) null else .{ .mode = mode }, .binding = if (failure == 1) null else changed } };
            try std.testing.expectError(error.CommitDecisionUnknown, rejected.run(.write, false));
            try std.testing.expectEqual(@as(usize, 1), rejected.commits);
            try std.testing.expectEqual(@as(usize, 0), rejected.aborts);
            try std.testing.expectEqual(@as(usize, 0), rejected.legacy_status_calls);
            try std.testing.expectEqual(@as(usize, 1), rejected.typed_status_calls);
        }
    }
}

test "distributed txn prepared protocol recovery status binds hidden scope and keeps terminal aborts" {
    const alloc = std.testing.allocator;
    const fence = (try PreparedCoordinatorFixture.routeFence(undefined, 7001)).?;
    var participant: ParticipantTxn = .{ .table_name = "docs", .group_id = 7001, .topology_epoch = fence.topology_epoch, .route_fence = fence, .restore_staging_scope = @splat(1), .restore_staging_plan_id = @splat(2) };
    const locator = try participantIdForGroupScoped(alloc, participant.table_name, participant.group_id, participant.restore_staging_scope, participant.restore_staging_plan_id);
    defer alloc.free(locator);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(locator, &digest, .{});
    const saved: @import("antfly_local_sources").storage_prepared_transaction_effects.Binding = .{ .group_id = 7001, .topology_epoch = fence.topology_epoch, .participant_digest = digest, .route_digest = preparation_protocol.routeDigest(fence) };
    for (std.meta.tags(preparation_protocol.Mode)) |mode| {
        var fixture: PreparedCoordinatorFixture = .{ .decision = .{ .status = .committed, .requirement = .{ .mode = mode }, .binding = saved } };
        var worker = fixture.worker();
        worker.preparation = .{ .mode = mode };
        const deadline = platform_time.monotonicNs() + std.time.ns_per_s;
        try std.testing.expectEqual(db_mod.types.TxnStatus.committed, try worker.statusForPreparation(alloc, participant, @splat(1), deadline));
        participant.restore_staging_plan_id.?[0] ^= 1;
        try std.testing.expectError(error.PreparationContractConflict, worker.statusForPreparation(alloc, participant, @splat(1), deadline));
        participant.restore_staging_plan_id.?[0] ^= 1;
        participant.restore_staging_scope.?[0] ^= 1;
        try std.testing.expectError(error.PreparationContractConflict, worker.statusForPreparation(alloc, participant, @splat(1), deadline));
        participant.restore_staging_scope.?[0] ^= 1;
        fixture.decision = .{ .status = .aborted, .requirement = null, .binding = saved };
        try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, try worker.statusForPreparation(alloc, participant, @splat(1), deadline));
        participant.restore_staging_scope.?[0] ^= 1;
        try std.testing.expectError(error.PreparationContractConflict, worker.statusForPreparation(alloc, participant, @splat(1), deadline));
        participant.restore_staging_scope.?[0] ^= 1;
        fixture.decision = .{ .status = .aborted, .requirement = null };
        try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, try worker.statusForPreparation(alloc, participant, @splat(1), deadline));
        try std.testing.expectEqual(@as(usize, 0), fixture.legacy_status_calls);
    }
}
