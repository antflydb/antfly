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
const platform_time = @import("antfly_platform").time;
const db_mod = @import("../storage/db/selected_root.zig").db;
const transactions_mod = @import("../storage/transactions.zig");
const tracing = @import("../tracing/antfly_trace_writer.zig");
const http_common = @import("../raft/transport/http_common.zig");
const raft_host = @import("../raft/host.zig");
const http_client_mod = @import("http_client.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const table_writes = @import("table_write_source.zig");
const contract = @import("distributed_txn_contract.zig");
const integrity_wire = @import("relational_integrity_wire.zig");
const integrity_activation = @import("../storage/db/relational_integrity_activation_contract.zig");
const integrity_retirement = @import("../storage/db/relational_integrity_retirement_contract.zig");

pub const table_participant_prefix = "table:";
const table_participant_v2_prefix = "table2:";
const table_participant_v3_prefix = "table3:";
pub const group_participant_marker = ":group:";

pub const TxnBeginRequest = struct {
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    topology_epoch: u64 = 0,
    retain_terminal: bool = false,
    participants: []const []const u8,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

pub const TxnPrepareRequest = struct {
    txn_id: db_mod.types.TxnId,
    topology_epoch: u64 = 0,
    req: db_mod.types.TransactionIntentRequest,
    /// Internal parser ownership only; never serialized as a request field.
    integrity_commands_owner: ?std.json.Parsed([]const integrity_wire.Command) = null,
    relational_activation_owner: ?std.json.Parsed(integrity_activation.Command) = null,
    relational_retirement_owner: ?std.json.Parsed(integrity_retirement.Command) = null,
    relational_index_maintenance_owner: ?std.json.Parsed(@import("../storage/db/relational_index_maintenance_contract.zig").Command) = null,
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
    return writes.txnAcknowledgeGroupLocal(alloc, group_id, table_name, req.txn_id, req.participant);
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

pub const TableCommitRequest = contract.TableCommitRequest;
pub const CommitConflict = contract.CommitConflict;
pub const ParticipantPhase = contract.ParticipantPhase;
pub const CommitOutcome = contract.CommitOutcome;
pub const PreDecisionContext = contract.PreDecisionContext;
pub const pre_decision_server_response_reserve_ms = contract.pre_decision_server_response_reserve_ms;

pub const ParticipantWorker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pre_decision_context: ?PreDecisionContext = null,
    /// Production adapters opt into bounded recovery. Null explicitly keeps
    /// the legacy custom-worker contract; it is never an automatic fallback.
    recovery_timeout_ns: ?u64 = null,
    recovery_deadline_ns: ?u64 = null,

    pub const VTable = struct {
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
        begin_group_with_context: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnBeginRequest, PreDecisionContext) anyerror!void = null,
        prepare_group_with_context: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnPrepareRequest, PreDecisionContext) anyerror!void = null,
        resolve_first_decision_with_context: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnResolveRequest, PreDecisionContext) anyerror!void = null,
        acknowledge_group_until: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnAcknowledgeRequest, u64) anyerror!void = null,
        resolve_group_until_with_cancellation: ?*const fn (*anyopaque, std.mem.Allocator, u64, []const u8, TxnResolveRequest, u64, db_mod.types.CancellationToken) anyerror!void = null,
    };

    pub fn startRecovery(self: ParticipantWorker) ParticipantWorker {
        var bounded = self;
        bounded.pre_decision_context = null;
        if (bounded.recovery_deadline_ns == null) if (bounded.recovery_timeout_ns) |timeout| {
            bounded.recovery_deadline_ns = platform_time.monotonicNs() +| timeout;
        };
        return bounded;
    }

    pub fn beginGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        if (self.pre_decision_context) |context| {
            ensurePreDecisionContextActive(context) catch return error.PreDecisionNotProposed;
            const call = self.vtable.begin_group_with_context orelse return error.PreDecisionNotProposed;
            return try call(self.ptr, alloc, group_id, table_name, req, context);
        }
        try self.vtable.begin_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn prepareGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        if (self.pre_decision_context) |context| {
            try ensurePreDecisionContextActive(context);
            const call = self.vtable.prepare_group_with_context orelse return error.PreDecisionNotProposed;
            return try call(self.ptr, alloc, group_id, table_name, req, context);
        }
        try self.vtable.prepare_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn resolveGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        if (self.recovery_deadline_ns) |deadline| return self.resolveGroupUntil(alloc, group_id, table_name, req, deadline);
        try self.vtable.resolve_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn resolveGroupWithCancellation(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken) !void {
        if (self.recovery_deadline_ns) |deadline| {
            if (cancellation.ptr == null) return self.resolveGroupUntil(alloc, group_id, table_name, req, deadline);
            try ensureDecisionRecoveryDeadline(deadline);
            const callback = self.vtable.resolve_group_until_with_cancellation orelse return error.CommitPropagationIncomplete;
            return try callback(self.ptr, alloc, group_id, table_name, req, deadline, cancellation);
        }
        const resolve = self.vtable.resolve_group_with_cancellation orelse
            return try self.resolveGroup(alloc, group_id, table_name, req);
        try resolve(self.ptr, alloc, group_id, table_name, req, cancellation);
    }

    fn resolveFirstDecision(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken, resume_committed: bool) !void {
        if (!resume_committed) if (self.pre_decision_context) |context| {
            ensurePreDecisionContextActive(context) catch return error.PreDecisionNotProposed;
            const callback = self.vtable.resolve_first_decision_with_context orelse return error.PreDecisionNotProposed;
            return try callback(self.ptr, alloc, group_id, table_name, req, context);
        };
        return try self.resolveGroupWithCancellation(alloc, group_id, table_name, req, cancellation);
    }

    pub fn statusGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        if (self.recovery_deadline_ns) |deadline| return self.statusGroupUntil(alloc, group_id, table_name, txn_id, deadline);
        return try self.vtable.status_group(self.ptr, alloc, group_id, table_name, txn_id);
    }

    pub fn statusGroupWithRequest(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
        try validateRestorePlan(req.restore_staging_scope, req.restore_staging_plan_id);
        const effective_deadline_ns: ?u64 = if (self.recovery_deadline_ns) |recovery_deadline|
            if (deadline_ns) |request_deadline| @min(recovery_deadline, request_deadline) else recovery_deadline
        else
            deadline_ns;
        if (effective_deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        if (req.restore_staging_scope != null) {
            if (req.restore_staging_plan_id == null) return error.InvalidTxnRequest;
            const callback = self.vtable.status_group_scoped orelse return error.CommitDecisionUnknown;
            return callback(self.ptr, alloc, group_id, table_name, req, effective_deadline_ns);
        }
        return if (effective_deadline_ns) |deadline| self.statusGroupUntil(alloc, group_id, table_name, req.txn_id, deadline) else self.statusGroup(alloc, group_id, table_name, req.txn_id);
    }

    pub fn resolveGroupUntil(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
        try ensureDecisionRecoveryDeadline(deadline_ns);
        // A legacy implementation cannot prove that its routing or transport
        // honors this deadline. Fail closed instead of turning a bounded
        // recovery operation back into an unbounded request.
        const resolve = self.vtable.resolve_group_until orelse return error.CommitDecisionUnknown;
        try resolve(self.ptr, alloc, group_id, table_name, req, deadline_ns);
    }

    pub fn statusGroupUntil(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
        try ensureDecisionRecoveryDeadline(deadline_ns);
        const status = self.vtable.status_group_until orelse return error.CommitDecisionUnknown;
        return try status(self.ptr, alloc, group_id, table_name, txn_id, deadline_ns);
    }

    pub fn acknowledgeGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest) !void {
        if (self.recovery_deadline_ns) |deadline| {
            if (platform_time.monotonicNs() >= deadline) return error.Timeout;
            const callback = self.vtable.acknowledge_group_until orelse return error.CommitPropagationIncomplete;
            return try callback(self.ptr, alloc, group_id, table_name, req, deadline);
        }
        const acknowledge = self.vtable.acknowledge_group orelse return;
        try acknowledge(self.ptr, alloc, group_id, table_name, req);
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
    const default_pre_decision_timeout_ms: u32 = contract.default_transaction_admission_timeout_ms;
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
    /// One process-local deadline covers routing, serialization, and every
    /// candidate contacted by a single begin/prepare operation. Individual
    /// attempts are capped so one black-holed peer cannot consume the entire
    /// rediscovery window before another replica is tried.
    pre_decision_timeout_ms: u32 = default_pre_decision_timeout_ms,
    pre_decision_attempt_timeout_ms: u32 = default_pre_decision_attempt_timeout_ms,
    pre_decision_context: ?PreDecisionContext = null,

    pub fn init(
        catalog: table_catalog.CatalogSource,
        router: table_router.HostedGroupRouter,
        writes: table_writes.TableWriteSource,
        executor: http_common.RequestExecutor,
    ) HostedParticipantWorker {
        return .{
            .catalog = catalog,
            .router = router,
            .writes = writes,
            .executor = executor,
        };
    }

    pub fn withInternalServiceAuth(self: *HostedParticipantWorker, secret: ?[]const u8, issuer: ?[]const u8) *HostedParticipantWorker {
        self.internal_service_secret = secret;
        self.internal_service_issuer = issuer;
        return self;
    }

    fn httpClient(self: *HostedParticipantWorker, alloc: std.mem.Allocator) http_client_mod.ApiHttpClient {
        var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
        _ = client.withInternalServiceAuth(self.internal_service_secret, self.internal_service_issuer);
        return client;
    }

    pub fn worker(self: *HostedParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .recovery_timeout_ns = contract.default_transaction_recovery_timeout_ns,
            .vtable = &.{
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .begin_group_with_context = beginGroupWithContext,
                .prepare_group_with_context = prepareGroupWithContext,
                .resolve_first_decision_with_context = resolveFirstDecisionWithContext,
                .resolve_group = resolveGroup,
                .resolve_group_with_cancellation = resolveGroupWithCancellation,
                .resolve_group_until = resolveGroupUntil,
                .resolve_group_until_with_cancellation = resolveGroupUntilWithCancellation,
                .status_group = statusGroup,
                .status_group_scoped = statusGroupScoped,
                .status_group_until = statusGroupUntil,
                .acknowledge_group = acknowledgeGroup,
                .acknowledge_group_until = acknowledgeGroupUntil,
            },
        };
    }

    fn beginGroupWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest, context: PreDecisionContext) !void {
        var bounded: HostedParticipantWorker = @as(*HostedParticipantWorker, @ptrCast(@alignCast(ptr))).*;
        bounded.pre_decision_context = context;
        bounded.router = bounded.router.withBudget(.{
            .clock = .{ .deadline_ns = context.deadline_ns, .io = context.deadline_io },
            .cancellation = context.cancellation,
        });
        try beginGroup(&bounded, alloc, group_id, table_name, req);
    }

    fn prepareGroupWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest, context: PreDecisionContext) !void {
        var bounded: HostedParticipantWorker = @as(*HostedParticipantWorker, @ptrCast(@alignCast(ptr))).*;
        bounded.pre_decision_context = context;
        bounded.router = bounded.router.withBudget(.{
            .clock = .{ .deadline_ns = context.deadline_ns, .io = context.deadline_io },
            .cancellation = context.cancellation,
        });
        try prepareGroup(&bounded, alloc, group_id, table_name, req);
    }

    fn preDecisionNowNs(self: *const HostedParticipantWorker) u64 {
        if (self.pre_decision_context) |context| return (table_catalog.RoutingBudget{ .io = context.deadline_io }).nowNs();
        return self.executor.monotonicNs();
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
        ensurePreDecisionDeadline(deadline_ns, self.preDecisionNowNs()) catch |err| {
            if (err == error.Timeout) return error.PreDecisionNotProposed;
            return err;
        };
        const attempted_node_id = switch (route) {
            .local => self.router.localNodeId(),
            .remote => |remote| remote.node_id,
        };
        switch (route) {
            .local => {
                const context = self.localPreDecisionContext(deadline_ns) catch |err| {
                    if (err == error.Timeout) return error.PreDecisionNotProposed;
                    return err;
                };
                if (self.pre_decision_context != null and (self.writes.vtable.txn_begin_group_local_with_pre_decision_context == null or self.writes.vtable.txn_decide_group_local_with_pre_decision_context == null)) return error.PreDecisionNotProposed;
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
        try ensurePreDecisionDeadline(deadline_ns, self.preDecisionNowNs());
        const attempted_node_id = switch (route) {
            .local => self.router.localNodeId(),
            .remote => |remote| remote.node_id,
        };
        switch (route) {
            .local => {
                if (self.pre_decision_context != null and self.writes.vtable.txn_prepare_group_local_with_pre_decision_context == null) return error.PreDecisionNotProposed;
                const result = self.writes.txnPrepareGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req, try self.localPreDecisionContext(deadline_ns)) catch |err| {
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
        ensurePreDecisionDeadline(deadline_ns, self.preDecisionNowNs()) catch |err| {
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
        ensurePreDecisionDeadline(deadline_ns, self.preDecisionNowNs()) catch |err| {
            if (err == error.Timeout) return false;
            return err;
        };
        if (node_id == self.router.localNodeId()) {
            if (self.router.localStatus(group_id) != .active) return false;
            const context = self.localPreDecisionContext(deadline_ns) catch |err| {
                if (err == error.Timeout) return false;
                return err;
            };
            if (self.pre_decision_context != null and (self.writes.vtable.txn_begin_group_local_with_pre_decision_context == null or self.writes.vtable.txn_decide_group_local_with_pre_decision_context == null)) return error.PreDecisionNotProposed;
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
        try ensurePreDecisionDeadline(deadline_ns, self.preDecisionNowNs());
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
        try ensurePreDecisionDeadline(deadline_ns, self.preDecisionNowNs());
        if (node_id == self.router.localNodeId()) {
            if (self.router.localStatus(group_id) != .active) return false;
            if (self.pre_decision_context != null and self.writes.vtable.txn_prepare_group_local_with_pre_decision_context == null) return error.PreDecisionNotProposed;
            const result = self.writes.txnPrepareGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req, try self.localPreDecisionContext(deadline_ns)) catch |err| {
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
        const configured = self.preDecisionNowNs() +| duration_ns;
        if (self.pre_decision_context) |context| {
            try ensurePreDecisionContextActive(context);
            if (context.deadline_ns) |deadline| return @min(configured, deadline);
        }
        return configured;
    }

    fn remainingPreDecisionAttemptBudget(self: *const HostedParticipantWorker, deadline_ns: u64) !PreDecisionAttemptBudget {
        if (self.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
        const remaining_ms = try remainingPreDecisionTimeoutMs(deadline_ns, self.preDecisionNowNs());
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
        const budget = try self.remainingPreDecisionAttemptBudget(deadline_ns);
        const duration_ns = @as(u64, budget.server_budget_ms) *| std.time.ns_per_ms;
        return .{
            .deadline_ns = @min(deadline_ns, self.preDecisionNowNs() +| duration_ns),
            .deadline_io = if (self.pre_decision_context) |context| context.deadline_io else self.executor.clock_io,
            .cancellation = if (self.pre_decision_context) |context| context.cancellation else .none,
        };
    }

    fn resolveFirstDecisionWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, context: PreDecisionContext) !void {
        var bounded: HostedParticipantWorker = @as(*HostedParticipantWorker, @ptrCast(@alignCast(ptr))).*;
        bounded.pre_decision_context = context;
        bounded.router = bounded.router.withBudget(.{ .clock = .{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }, .cancellation = context.cancellation });
        const deadline = bounded.preDecisionDeadlineNs() catch return error.PreDecisionNotProposed;
        var route = (table_router.resolveGroupRoute(alloc, bounded.catalog, bounded.router, group_id, .prefer_leader) catch return error.PreDecisionNotProposed) orelse return error.PreDecisionNotProposed;
        defer route.deinit(alloc);
        ensurePreDecisionContextActive(context) catch return error.PreDecisionNotProposed;
        switch (route) {
            .local => _ = (try bounded.writes.txnDecideGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, context)) orelse return error.PreDecisionNotProposed,
            .remote => |remote| {
                const body = encodeTxnResolveRequest(alloc, req) catch return error.PreDecisionNotProposed;
                defer alloc.free(body);
                const budget = bounded.remainingPreDecisionAttemptBudget(deadline) catch return error.PreDecisionNotProposed;
                var client = bounded.httpClient(alloc);
                var tracker: http_common.RequestDeliveryTracker = .{};
                const DeadlineCancellation = struct {
                    context: PreDecisionContext,
                    fn check(raw: *const anyopaque) !void {
                        const self: *const @This() = @ptrCast(@alignCast(raw));
                        try ensurePreDecisionContextActive(self.context);
                    }
                    fn isCancelled(raw: *const anyopaque) bool {
                        check(raw) catch return true;
                        return false;
                    }
                };
                var scope: DeadlineCancellation = .{ .context = context };
                var cancellation = http_common.RequestCancellation.fromToken(.{ .ptr = &scope, .check_fn = DeadlineCancellation.check, .is_cancelled_fn = DeadlineCancellation.isCancelled });
                const result = client.fetchGroupTxnDecideWithContext(remote.base_uri, group_id, table_name, body, &tracker, budget.client_timeout_ms, budget.server_budget_ms, &cancellation) catch |err| {
                    if (tracker.load() == .not_sent) return error.PreDecisionNotProposed;
                    return err;
                };
                if (result == .not_proposed) return error.PreDecisionNotProposed;
            },
        }
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

    fn resolveGroupUntilWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64, cancellation: db_mod.types.CancellationToken) !void {
        try resolveGroupWithin(ptr, alloc, group_id, table_name, req, cancellation, deadline_ns);
    }

    fn resolveGroupWithin(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken, deadline_ns: ?u64) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        var deadline_cancellation = DecisionRecoveryCancellation{ .deadline_ns = deadline_ns orelse 0, .other = cancellation };
        const operation_cancellation = if (deadline_ns != null) deadline_cancellation.token() else cancellation;
        const router = if (deadline_ns) |deadline| self.router.withBudget(.{ .clock = .{ .deadline_ns = deadline }, .cancellation = operation_cancellation }) else self.router;
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        if (deadline_ns) |deadline| try ensureDecisionRecoveryDeadline(deadline);
        switch (route) {
            .local => {
                _ = (if (deadline_ns) |deadline|
                    try self.writes.txnResolveGroupLocalUntil(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, deadline, cancellation)
                else
                    try self.writes.txnResolveGroupLocalWithCancellation(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, operation_cancellation)) orelse return error.UnknownGroup;
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
        const router = if (deadline_ns) |deadline| self.router.withBudget(.{ .clock = .{ .deadline_ns = deadline } }) else self.router;
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
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

    fn acknowledgeGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest, deadline_ns: u64) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        try ensureDecisionRecoveryDeadline(deadline_ns);
        var scope: DecisionRecoveryCancellation = .{ .deadline_ns = deadline_ns };
        const router = self.router.withBudget(.{ .clock = .{ .deadline_ns = deadline_ns }, .cancellation = scope.token() });
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        try ensureDecisionRecoveryDeadline(deadline_ns);
        switch (route) {
            .local => _ = (try self.writes.txnAcknowledgeGroupLocalUntil(alloc, group_id, table_name, req.txn_id, req.participant, deadline_ns)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = try encodeTxnAcknowledgeRequest(alloc, req);
                defer alloc.free(body);
                var cancellation = http_common.RequestCancellation.fromToken(scope.token());
                var response = try client.fetchGroupTxnAcknowledgeWithControlAndTimeout(remote.base_uri, group_id, table_name, body, try remainingDeadlineTimeoutMs(deadline_ns), &cancellation);
                response.deinit(alloc);
            },
        }
    }

    fn acknowledgeGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        switch (route) {
            .local => _ = (try acknowledgeGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, .none)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = self.httpClient(alloc);
                const body = try encodeTxnAcknowledgeRequest(alloc, req);
                defer alloc.free(body);
                var response = try client.fetchGroupTxnAcknowledge(remote.base_uri, group_id, table_name, body);
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

pub const ensurePreDecisionContextActive = contract.ensurePreDecisionContextActive;

fn ensurePreDecisionDeadline(deadline_ns: u64, now_ns: u64) !void {
    if (now_ns >= deadline_ns) return error.Timeout;
}

fn ensureDecisionRecoveryDeadline(deadline_ns: u64) !void {
    if (platform_time.monotonicNs() >= deadline_ns) return error.CommitDecisionUnknown;
}

const DecisionRecoveryCancellation = struct {
    deadline_ns: u64,
    other: db_mod.types.CancellationToken = .none,

    fn token(self: *const DecisionRecoveryCancellation) db_mod.types.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = isCancelled };
    }

    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *const DecisionRecoveryCancellation = @ptrCast(@alignCast(ptr));
        return platform_time.monotonicNs() >= self.deadline_ns or self.other.isCancelled();
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

    pub fn init(writes: table_writes.TableWriteSource) LocalTableWriteParticipantWorker {
        return .{ .writes = writes };
    }

    pub fn worker(self: *LocalTableWriteParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .recovery_timeout_ns = contract.default_transaction_recovery_timeout_ns,
            .vtable = &.{
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .begin_group_with_context = beginGroupWithContext,
                .prepare_group_with_context = prepareGroupWithContext,
                .resolve_first_decision_with_context = resolveFirstDecisionWithContext,
                .resolve_group = resolveGroup,
                .resolve_group_with_cancellation = resolveGroupWithCancellation,
                .resolve_group_until = resolveGroupUntil,
                .resolve_group_until_with_cancellation = resolveGroupUntilWithCancellation,
                .status_group = statusGroup,
                .status_group_scoped = statusGroupScoped,
                .status_group_until = statusGroupUntil,
                .acknowledge_group = acknowledgeGroup,
                .acknowledge_group_until = acknowledgeGroupUntil,
            },
        };
    }

    fn beginGroupWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest, context: PreDecisionContext) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        try ensurePreDecisionContextActive(context);
        if (self.writes.vtable.txn_begin_group_local_with_pre_decision_context == null or self.writes.vtable.txn_decide_group_local_with_pre_decision_context == null) return error.PreDecisionNotProposed;
        _ = (try self.writes.txnBeginGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.retain_terminal, req.participants, context)) orelse return error.UnknownGroup;
    }

    fn prepareGroupWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest, context: PreDecisionContext) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        try ensurePreDecisionContextActive(context);
        if (self.writes.vtable.txn_prepare_group_local_with_pre_decision_context == null) return error.PreDecisionNotProposed;
        _ = (try self.writes.txnPrepareGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req, context)) orelse return error.UnknownGroup;
    }

    fn resolveFirstDecisionWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, context: PreDecisionContext) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnDecideGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, context)) orelse return error.PreDecisionNotProposed;
    }

    fn beginGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnBeginGroupLocalWithPreDecisionContext(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.retain_terminal, req.participants, .{ .restore_staging_scope = req.restore_staging_scope, .restore_staging_plan_id = req.restore_staging_plan_id })) orelse return error.UnknownGroup;
    }

    fn prepareGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnPrepareGroupLocal(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req)) orelse return error.UnknownGroup;
    }

    fn resolveGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        try resolveGroupWithCancellation(ptr, alloc, group_id, table_name, req, .none);
    }

    fn resolveGroupWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, cancellation: db_mod.types.CancellationToken) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try resolveGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, cancellation)) orelse return error.UnknownGroup;
    }

    fn resolveGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64) !void {
        return try resolveGroupUntilWithCancellation(ptr, alloc, group_id, table_name, req, deadline_ns, .none);
    }

    fn resolveGroupUntilWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest, deadline_ns: u64, cancellation: db_mod.types.CancellationToken) !void {
        try ensureDecisionRecoveryDeadline(deadline_ns);
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnResolveGroupLocalUntil(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version, req.topology_epoch, req.sync_level, deadline_ns, cancellation)) orelse return error.UnknownGroup;
    }

    fn statusGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.txnStatusGroupLinearizable(alloc, group_id, table_name, txn_id)) orelse error.UnknownGroup;
    }

    fn statusGroupScoped(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.txnStatusGroupLocalWithRequest(alloc, group_id, table_name, req, .{ .deadline_ns = deadline_ns })) orelse error.UnknownGroup;
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

    fn acknowledgeGroupUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest, deadline_ns: u64) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnAcknowledgeGroupLocalUntil(alloc, group_id, table_name, req.txn_id, req.participant, deadline_ns)) orelse return error.UnknownGroup;
    }

    fn acknowledgeGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnAcknowledgeRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try acknowledgeGroupLocalWithRequest(self.writes, alloc, group_id, table_name, req, .none)) orelse return error.UnknownGroup;
    }
};

pub const ExecuteResult = contract.ExecuteResult;

pub const ExecuteOptions = struct {
    /// One original ingress deadline and clock span routing and every begin/
    /// prepare wave. Null preserves the legacy per-operation contract. Cleanup
    /// and known-decision recovery deliberately do not borrow this context.
    pre_decision_context: ?PreDecisionContext = null,
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
    /// Consulted only by participant implementations after a committed
    /// transaction mutation is durable, never during begin or prepare.
    post_commit_cancellation: db_mod.types.CancellationToken = .none,
};

const ParticipantFanoutSlot = struct {
    err: ?anyerror = null,
    acknowledgement_err: ?anyerror = null,
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

fn fanoutWidth(options: ExecuteOptions, participant_count: usize) usize {
    if (options.fanout_io == null or participant_count <= 1) return 1;
    return @min(@max(options.max_parallel_participants, 1), participant_count);
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
    const tables = [_]TableCommitRequest{.{
        .table_name = table_name,
        .writes = req.writes,
        .deletes = req.deletes,
        .transforms = req.transforms,
        .predicates = req.predicates,
        .integrity = req.integrity,
        .integrity_commands = req.integrity_commands,
        .relational_activation = req.relational_activation,
        .relational_retirement = req.relational_retirement,
        .relational_index_maintenance = req.relational_index_maintenance,
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
    options: ExecuteOptions,
) !CommitOutcome {
    var worker = original_worker;
    if (options.pre_decision_context) |context| {
        try ensurePreDecisionContextActive(context);
        if (worker.vtable.begin_group_with_context == null or worker.vtable.prepare_group_with_context == null or worker.vtable.resolve_first_decision_with_context == null)
            return error.PreDecisionNotProposed;
        worker.pre_decision_context = context;
    }
    var participants = std.ArrayListUnmanaged(ParticipantTxn).empty;
    defer {
        for (participants.items) |*participant| participant.deinit(alloc);
        participants.deinit(alloc);
    }

    for (tables) |table| {
        if (worker.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
        var routing = (try table_catalog.transactionRoutingSnapshot(alloc, catalog, table.table_name)) orelse return error.TableNotFound;
        defer routing.deinit(alloc);
        if (worker.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
        const topology_epoch = routing.topology_epoch;

        for (table.writes, 0..) |write, item_index| {
            if (item_index % 64 == 0) if (worker.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
            const group_id = routing.resolveGroupForKey(write.key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.writes.append(alloc, write);
        }
        for (table.deletes, 0..) |key, item_index| {
            if (item_index % 64 == 0) if (worker.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
            const group_id = routing.resolveGroupForKey(key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.deletes.append(alloc, key);
        }
        for (table.predicates, 0..) |predicate, item_index| {
            if (item_index % 64 == 0) if (worker.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
            const group_id = routing.resolveGroupForKey(predicate.key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.predicates.append(alloc, predicate);
        }
        for (table.transforms, 0..) |transform, item_index| {
            if (item_index % 64 == 0) if (worker.pre_decision_context) |context| try ensurePreDecisionContextActive(context);
            const group_id = routing.resolveGroupForKey(transform.key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.transforms.append(alloc, transform);
        }
        for (table.integrity) |operation| {
            const group_id = routing.resolveGroupForKey(operation.routing_key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.integrity.append(alloc, operation);
        }
        for (table.integrity_commands) |command| {
            const group_id = routing.resolveGroupForKey(&command.address.routing) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.integrity_commands.append(alloc, command);
        }
        if (table.relational_activation) |command| {
            const group_id = routing.resolveGroupForKey(command.routing_key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            if (participant.relational_activation != null) return error.InvalidTxnRequest;
            participant.relational_activation = command;
        }
        if (table.relational_retirement) |command| {
            const group_id = routing.resolveGroupForKey(command.routing_key) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            if (participant.relational_retirement != null) return error.InvalidTxnRequest;
            participant.relational_retirement = command;
        }
        if (table.relational_index_maintenance) |command| {
            const group_id = routing.resolveGroupForKey(command.routing_key) orelse return error.UnknownGroup;
            if (group_id != command.owner_group_id) return error.PreparedGenerationChanged;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            if (participant.relational_index_maintenance != null) return error.InvalidTxnRequest;
            participant.relational_index_maintenance = command;
        }
        for (participants.items) |*participant| {
            if (!std.mem.eql(u8, participant.table_name, table.table_name)) continue;
            if (participant.relational_schema_version) |existing| {
                if (table.relational_schema_version) |requested| {
                    if (existing != requested) return error.InvalidTxnRequest;
                }
            } else participant.relational_schema_version = table.relational_schema_version;
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

    var begun_count: usize = 0;
    var abort_on_error = true;
    var resume_committed = false;
    errdefer {
        if (abort_on_error) {
            if (trace_writer) |tw| {
                tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
            }
            if (begun_count > 0) abortParticipants(
                alloc,
                worker,
                txn_id,
                commit_version,
                participants.items,
                participant_ids,
                if (options.retain_terminal) participants.items.len else begun_count,
            ) catch {};
        }
    }

    if (participants.items.len > 0) coordinator_begin: {
        const participant = participants.items[0];
        worker.beginGroup(alloc, participant.group_id, participant.table_name, .{
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
                worker = worker.startRecovery();
                const status = worker.statusGroupWithRequest(
                    alloc,
                    participant.group_id,
                    participant.table_name,
                    participant.statusRequest(txn_id),
                    null,
                ) catch |status_err| switch (status_err) {
                    error.TxnNotFound => null,
                    else => return error.CommitDecisionUnknown,
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
            try abortParticipants(
                alloc,
                worker,
                txn_id,
                commit_version,
                participants.items,
                participant_ids,
                if (options.retain_terminal) participants.items.len else 1,
            );
            std.log.warn("transaction begin failed table={s} group_id={} err={s}", .{
                participant.table_name, participant.group_id, @errorName(err),
            });
            return error.TransactionBeginFailed;
        };
        begun_count = 1;
    }

    if (resume_committed) worker = worker.startRecovery();

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
                    try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
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
                    try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                    return .{ .conflict = participantUnavailableConflict(participant, .prepare) };
                },
                else => {
                    std.log.warn("transaction prepare failed table={s} group_id={} err={s}", .{
                        participant.table_name, participant.group_id, @errorName(err),
                    });
                    abort_on_error = false;
                    try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                    return err;
                },
            }
        }
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
            if (worker.pre_decision_context) |context| ensurePreDecisionContextActive(context) catch |err| {
                abort_on_error = false;
                if (participants.items.len > 0) try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                return err;
            };
            table_catalog.validateTransactionTopologyEpoch(alloc, catalog, participant.table_name, participant.topology_epoch) catch |err| {
                abort_on_error = false;
                try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                return err;
            };
        }
    }

    if (!resume_committed) if (worker.pre_decision_context) |context| ensurePreDecisionContextActive(context) catch |err| {
        abort_on_error = false;
        if (participants.items.len > 0) try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
        return err;
    };

    // Visibility and propagation can both remain pending (for example, when
    // the coordinator's write becomes durable but its visibility barrier
    // fails and a follower was only proposed). Track them independently so a
    // later phase-two outcome cannot hide an earlier recovery obligation.
    var recovery_worker: ?ParticipantWorker = if (resume_committed) worker else null;
    var visibility_pending = false;
    var visibility_retry_pending = false;
    var visibility_repair_required = false;
    var propagation_pending = false;
    var propagation_failed = false;
    if (participants.items.len > 0) {
        const participant = participants.items[0];
        const coordinator_sync_level: db_mod.types.SyncLevel = if (sync_level == .propose) .write else sync_level;
        worker.resolveFirstDecision(alloc, participant.group_id, participant.table_name, .{
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
        }, options.post_commit_cancellation, resume_committed) catch |err| switch (err) {
            error.PreDecisionNotProposed => {
                if (resume_committed) return resumedCommitPropagationFailure(participants.items, options);
                abort_on_error = false;
                // Only a first-submission rejection permits choosing abort.
                // A failure of that durable cleanup remains an error/unknown.
                try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                return error.PreDecisionNotProposed;
            },
            error.DecisionConflict => {
                if (resume_committed) return resumedCommitPropagationFailure(participants.items, options);
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
                try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                return .{ .conflict = participantDecisionConflict(participant, .resolve) };
            },
            error.TxnNotFound, error.InvalidTxnRecord => {
                if (resume_committed) return resumedCommitPropagationFailure(participants.items, options);
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
                try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
                return .{ .conflict = participantTornStateConflict(participant, .resolve) };
            },
            else => {
                // A resolve can report an error after its local atomic commit
                // (for example while mirroring or waiting for an index). Read
                // the participant record before deciding whether abort is
                // still legal.
                recovery_worker = worker.startRecovery();
                const durable_status = resolveCoordinatorDecisionAfterFailure(
                    alloc,
                    recovery_worker.?,
                    participant,
                    txn_id,
                    commit_version,
                    coordinator_sync_level,
                    err,
                    options.post_commit_cancellation,
                ) catch |status_err| {
                    if (resume_committed) return resumedCommitPropagationFailure(participants.items, options);
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
                        if (resume_committed) return resumedCommitPropagationFailure(participants.items, options);
                        // Once the commit submission may have crossed the
                        // Raft proposal boundary, pending means "not observed
                        // yet", never "safe to abort". The same decision can
                        // be retried idempotently; the opposite decision is
                        // permanently forbidden on this path.
                        abort_on_error = false;
                        return error.CommitDecisionUnknown;
                    },
                    .aborted => {
                        if (resume_committed) return resumedCommitPropagationFailure(participants.items, options);
                        abort_on_error = false;
                        try abortParticipants(alloc, worker, txn_id, commit_version, participants.items, participant_ids, participants.items.len);
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
        recovery_worker orelse worker.startRecovery(),
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

// Authoritative committed evidence is monotonic. A later inconsistent retry
// response cannot authorize abort, nor can it certify this attempt's requested
// decision metadata for further participant propagation. Leave durable recovery
// enlisted and preserve the known committed outcome for ephemeral callers.
fn resumedCommitPropagationFailure(participants: []const ParticipantTxn, options: ExecuteOptions) !CommitOutcome {
    std.debug.assert(participants.len != 0);
    if (options.report_post_commit_failure) return error.CommitPropagationIncomplete;
    return .{ .committed = .{
        .participant_count = participants.len,
        .coordinator_group_id = participants[0].group_id,
        .coordinator_table_name = participants[0].table_name,
        .propagation_pending = true,
    } };
}

const coordinator_resolution_timeout_ns: u64 = contract.default_transaction_recovery_timeout_ns;
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
    return try resolveCoordinatorDecisionAfterFailureUntil(
        alloc,
        worker,
        participant,
        txn_id,
        commit_version,
        sync_level,
        initial_resolve_error,
        worker.recovery_deadline_ns orelse platform_time.monotonicNs() +| coordinator_resolution_timeout_ns,
    );
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
    var attempts: usize = 1;
    var last_resolve_error = initial_resolve_error;
    var last_status_error: ?anyerror = null;
    while (true) {
        const status: ?db_mod.types.TxnStatus = worker.statusGroupWithRequest(
            alloc,
            participant.group_id,
            participant.table_name,
            participant.statusRequest(txn_id),
            deadline_ns,
        ) catch |status_err| status_failure: {
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
            if (now_ns < deadline_ns) sleepNs(@min(coordinator_resolution_retry_ns, deadline_ns - now_ns));
            continue;
        };
        return .committed;
    }
}

const ParticipantTxn = struct {
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
    relational_index_maintenance: ?@import("../storage/db/relational_index_maintenance_contract.zig").Command = null,

    fn statusRequest(self: ParticipantTxn, txn_id: db_mod.types.TxnId) TxnStatusRequest {
        return .{ .txn_id = txn_id, .restore_staging_scope = self.restore_staging_scope, .restore_staging_plan_id = self.restore_staging_plan_id };
    }

    fn deinit(self: *ParticipantTxn, alloc: std.mem.Allocator) void {
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
        worker: ParticipantWorker,
        participant: *const ParticipantTxn,
        participant_ids: []const []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        retain_terminal: bool,
        slot: *ParticipantFanoutSlot,
    ) void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
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
        if (options.fanout_io) |io| {
            var group: std.Io.Group = .init;
            for (start..end) |i| {
                group.concurrent(io, BeginFanoutTask.run, .{
                    worker,
                    &participants[i],
                    participant_ids,
                    txn_id,
                    begin_timestamp,
                    options.retain_terminal,
                    &slots[i],
                }) catch BeginFanoutTask.run(
                    worker,
                    &participants[i],
                    participant_ids,
                    txn_id,
                    begin_timestamp,
                    options.retain_terminal,
                    &slots[i],
                );
            }
            awaitFanout(&group, io);
        } else {
            for (start..end) |i| BeginFanoutTask.run(
                worker,
                &participants[i],
                participant_ids,
                txn_id,
                begin_timestamp,
                options.retain_terminal,
                &slots[i],
            );
        }
        if (firstFanoutError(slots[start..end]) != null) break;
    }
}

const PrepareFanoutTask = struct {
    fn run(
        worker: ParticipantWorker,
        participant: *const ParticipantTxn,
        txn_id: db_mod.types.TxnId,
        slot: *ParticipantFanoutSlot,
    ) void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        worker.prepareGroup(arena.allocator(), participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .topology_epoch = participant.topology_epoch,
            .req = .{
                .writes = participant.writes.items,
                .deletes = participant.deletes.items,
                .transforms = participant.transforms.items,
                .predicates = participant.predicates.items,
                .integrity = participant.integrity.items,
                .integrity_commands = participant.integrity_commands.items,
                .relational_activation = participant.relational_activation,
                .relational_retirement = participant.relational_retirement,
                .relational_index_maintenance = participant.relational_index_maintenance,
                .relational_schema_version = participant.relational_schema_version,
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
        if (options.fanout_io) |io| {
            var group: std.Io.Group = .init;
            for (start..end) |i| {
                group.concurrent(io, PrepareFanoutTask.run, .{ worker, &participants[i], txn_id, &slots[i] }) catch
                    PrepareFanoutTask.run(worker, &participants[i], txn_id, &slots[i]);
            }
            awaitFanout(&group, io);
        } else {
            for (start..end) |i| PrepareFanoutTask.run(worker, &participants[i], txn_id, &slots[i]);
        }
        if (firstFanoutError(slots[start..end]) != null) break;
    }
}

const ResolveFollowerFanoutTask = struct {
    fn run(
        worker: ParticipantWorker,
        coordinator: *const ParticipantTxn,
        participant: *const ParticipantTxn,
        participant_id: []const u8,
        txn_id: db_mod.types.TxnId,
        commit_version: u64,
        sync_level: db_mod.types.SyncLevel,
        resume_committed: bool,
        cancellation: db_mod.types.CancellationToken,
        slot: *ParticipantFanoutSlot,
    ) void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
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
        worker.acknowledgeGroup(arena.allocator(), coordinator.group_id, coordinator.table_name, .{
            .restore_staging_scope = coordinator.restore_staging_scope,
            .restore_staging_plan_id = coordinator.restore_staging_plan_id,
            .txn_id = txn_id,
            .participant = participant_id,
        }) catch |err| {
            slot.acknowledgement_err = err;
            slot.propagation_pending = true;
        };
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
        if (options.fanout_io) |io| {
            var group: std.Io.Group = .init;
            for (start..end) |i| {
                group.concurrent(io, ResolveFollowerFanoutTask.run, .{
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
                }) catch ResolveFollowerFanoutTask.run(
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
                );
            }
            awaitFanout(&group, io);
        } else {
            for (start..end) |i| ResolveFollowerFanoutTask.run(
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
            );
        }
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

pub fn participantIdForGroup(alloc: std.mem.Allocator, table_name: []const u8, group_id: u64) ![]u8 {
    if (table_name.len > std.math.maxInt(u32)) return error.TableNameTooLong;
    return try std.fmt.allocPrint(alloc, "{s}{x:0>8}:{s}:{d}", .{ table_participant_v2_prefix, table_name.len, table_name, group_id });
}

pub const ParticipantRef = struct {
    table_name: []const u8,
    group_id: u64,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

/// The existing durable participant set owns recovery routing. Hidden owners
/// add a fixed-size exact locator, so restart never depends on a resident cache
/// or a scan through all restore jobs. Ordinary participant IDs are unchanged.
pub fn participantIdForGroupScoped(alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, scope: ?[32]u8, plan_id: ?[16]u8) ![]u8 {
    if (scope == null and plan_id == null) return participantIdForGroup(alloc, table_name, group_id);
    try validateRestorePlan(scope, plan_id);
    if (scope == null or plan_id == null or table_name.len == 0 or table_name.len > std.math.maxInt(u32) or group_id == 0) return error.InvalidTxnRequest;
    return std.fmt.allocPrint(alloc, "{s}{x:0>8}:{s}:{d}:{s}:{s}", .{ table_participant_v3_prefix, table_name.len, table_name, group_id, std.fmt.bytesToHex(plan_id.?, .lower), std.fmt.bytesToHex(scope.?, .lower) });
}

pub fn parseParticipantRef(participant: []const u8) ?ParticipantRef {
    if (std.mem.startsWith(u8, participant, table_participant_v3_prefix)) {
        const body = participant[table_participant_v3_prefix.len..];
        if (body.len < 9 or body[8] != ':') return null;
        const table_name_len = std.fmt.parseUnsigned(u32, body[0..8], 16) catch return null;
        if (table_name_len == 0 or table_name_len > body.len - 9) return null;
        const group_separator = 9 + @as(usize, table_name_len);
        if (group_separator >= body.len or body[group_separator] != ':') return null;
        const suffix = body[group_separator + 1 ..];
        const group_end = std.mem.indexOfScalar(u8, suffix, ':') orelse return null;
        if (suffix.len - group_end != 1 + 32 + 1 + 64 or suffix[group_end + 33] != ':') return null;
        const group_id = std.fmt.parseUnsigned(u64, suffix[0..group_end], 10) catch return null;
        if (group_id == 0) return null;
        var plan: [16]u8 = undefined;
        var scope: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&plan, suffix[group_end + 1 ..][0..32]) catch return null;
        _ = std.fmt.hexToBytes(&scope, suffix[group_end + 34 ..]) catch return null;
        if (std.mem.allEqual(u8, &plan, 0)) return null;
        return .{ .table_name = body[9..group_separator], .group_id = group_id, .restore_staging_scope = scope, .restore_staging_plan_id = plan };
    }
    if (std.mem.startsWith(u8, participant, table_participant_v2_prefix)) {
        const body = participant[table_participant_v2_prefix.len..];
        if (body.len < 9 or body[8] != ':') return null;
        const table_name_len = std.fmt.parseUnsigned(u32, body[0..8], 16) catch return null;
        const table_start: usize = 9;
        const group_separator = table_start + @as(usize, table_name_len);
        if (body.len <= group_separator or body[group_separator] != ':') return null;
        const table_name = body[table_start..group_separator];
        if (table_name.len == 0) return null;
        const group_id = std.fmt.parseUnsigned(u64, body[group_separator + 1 ..], 10) catch return null;
        return .{ .table_name = table_name, .group_id = group_id };
    }

    if (!std.mem.startsWith(u8, participant, table_participant_prefix)) return null;
    const rest = participant[table_participant_prefix.len..];
    const marker_index = std.mem.indexOf(u8, rest, group_participant_marker) orelse return null;
    const table_name = rest[0..marker_index];
    if (table_name.len == 0) return null;
    const group_id = std.fmt.parseUnsigned(u64, rest[marker_index + group_participant_marker.len ..], 10) catch return null;
    return .{ .table_name = table_name, .group_id = group_id };
}

pub fn resolveParticipant(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: []const u8,
    txn_id: db_mod.types.TxnId,
    status: db_mod.types.TxnStatus,
    commit_version: u64,
) !void {
    const ref = parseParticipantRef(participant) orelse return error.InvalidParticipant;
    try worker.startRecovery().resolveGroup(alloc, ref.group_id, ref.table_name, .{
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
    try out.appendSlice(alloc, "{\"txn_id\":\"");
    try out.appendSlice(alloc, &txn_hex);
    try out.appendSlice(alloc, "\",\"topology_epoch\":");
    const epoch = try std.fmt.allocPrint(alloc, "{d}", .{req.topology_epoch});
    defer alloc.free(epoch);
    try out.appendSlice(alloc, epoch);
    try out.appendSlice(alloc, ",\"writes\":[");
    for (req.req.writes, 0..) |write, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"key\":{f},\"value\":{s}}}",
            .{ std.json.fmt(write.key, .{}), write.value },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
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
            .expected_content_digest = if (predicate.expected_content_digest != null) @as(?[]const u8, &digest_hex) else null,
        }, .{ .emit_null_optional_fields = false });
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"integrity\":");
    try integrity_wire.append(alloc, &out, req.req.integrity);
    try out.appendSlice(alloc, ",\"integrity_commands\":");
    try integrity_wire.appendCommands(alloc, &out, req.req.integrity_commands);
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

fn validateRestorePlan(scope: ?[32]u8, plan_id: ?[16]u8) !void {
    if (plan_id) |id| if (scope == null or std.mem.allEqual(u8, &id, 0)) return error.InvalidTxnRequest;
}

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
    const obj = switch (parsed.value) {
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
    var relational_activation_owner = if (obj.get("relational_activation")) |value| try std.json.parseFromValue(integrity_activation.Command, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (relational_activation_owner) |*owner| owner.deinit();
    var relational_retirement_owner = if (obj.get("relational_retirement")) |value| try std.json.parseFromValue(integrity_retirement.Command, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (relational_retirement_owner) |*owner| owner.deinit();
    var relational_index_maintenance_owner = if (obj.get("relational_index_maintenance")) |value| try std.json.parseFromValue(@import("../storage/db/relational_index_maintenance_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }) else null;
    errdefer if (relational_index_maintenance_owner) |*owner| owner.deinit();
    const relational_schema_version: ?u32 = if (obj.get("relational_schema_version")) |_| blk: {
        const version = try optionalU64(obj, "relational_schema_version");
        if (version > std.math.maxInt(u32)) return error.InvalidTxnRequest;
        break :blk @intCast(version);
    } else null;
    const generation_set: ?[32]u8 = if (obj.get("relational_integrity_generation_set")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const restore_staging_scope: ?[32]u8 = if (obj.get("restore_staging_scope")) |value| try integrity_wire.parseGenerationSet(value) else null;
    const relational_repair = if (obj.get("relational_repair")) |value| switch (value) {
        .bool => |flag| flag,
        else => return error.InvalidTxnRequest,
    } else false;
    return .{
        .txn_id = txn_id,
        .topology_epoch = try optionalU64(obj, "topology_epoch"),
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .predicates = predicates,
            .integrity = integrity,
            .integrity_commands = if (integrity_commands_owner) |owner| owner.value else &.{},
            .relational_activation = if (relational_activation_owner) |owner| owner.value else null,
            .relational_retirement = if (relational_retirement_owner) |owner| owner.value else null,
            .relational_index_maintenance = if (relational_index_maintenance_owner) |owner| owner.value else null,
            .relational_schema_version = relational_schema_version,
            .relational_integrity_generation_set = generation_set,
            .restore_staging_scope = restore_staging_scope,
            .restore_staging_plan_id = try parseRestorePlan(obj, restore_staging_scope),
            .relational_repair = relational_repair,
        },
        .integrity_commands_owner = integrity_commands_owner,
        .relational_activation_owner = relational_activation_owner,
        .relational_retirement_owner = relational_retirement_owner,
        .relational_index_maintenance_owner = relational_index_maintenance_owner,
    };
}

pub fn freeTxnPrepareRequest(alloc: std.mem.Allocator, req: *TxnPrepareRequest) void {
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

test "distributed txn index maintenance prepare roundtrips owned exact observation" {
    const alloc = std.testing.allocator;
    const command = @import("../storage/db/relational_index_maintenance_contract.zig").Command{
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
            .relational_integrity_generation_set = [_]u8{9} ** 32,
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

    const batch = @import("batch.zig");
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
    const stats = try reopened.runTransactionRecoveryOnce(resolver.config());
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
        out[i] = .{
            .key = key,
            .value = encoded_value,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.TransactionWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
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
        out[i] = .{
            .key = try alloc.dupe(u8, requireString(obj, "key")),
            .expected_version = expected_version,
            .expected_content_digest = content_digest,
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
    original_worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    attempted_count: usize,
) !void {
    if (participants.len == 0) return;
    const worker = original_worker.startRecovery();
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

    // Once the coordinator decision is durable, follower delivery is
    // idempotent recovery work and must not contradict that decision.
    for (participants[1..], 1..) |participant, participant_index| {
        if (participant_index < attempted_count) {
            worker.resolveGroup(alloc, participant.group_id, participant.table_name, .{
                .restore_staging_scope = participant.restore_staging_scope,
                .restore_staging_plan_id = participant.restore_staging_plan_id,
                .txn_id = txn_id,
                .status = .aborted,
                .commit_version = timestamp,
                // An acknowledgement removes the participant from durable
                // recovery, so phase two must be committed/applied first.
                .sync_level = .write,
            }) catch |err| {
                // Continue notifying later participants. The durable
                // coordinator record remains authoritative and recovery will
                // retry any unavailable attempted participant.
                std.log.warn("transaction abort delivery failed table={s} group_id={} err={s}", .{
                    participant.table_name,
                    participant.group_id,
                    @errorName(err),
                });
                continue;
            };
        }
        // Participants beyond attempted_count were never contacted. They have
        // no transaction state or intents and are safe to acknowledge directly
        // after the coordinator's abort decision is durable.
        worker.acknowledgeGroup(alloc, coordinator.group_id, coordinator.table_name, .{
            .restore_staging_scope = coordinator.restore_staging_scope,
            .restore_staging_plan_id = coordinator.restore_staging_plan_id,
            .txn_id = txn_id,
            .participant = participant_ids[participant_index],
        }) catch |err| {
            std.log.warn("transaction abort acknowledgement deferred table={s} group_id={} err={s}", .{
                participant.table_name,
                participant.group_id,
                @errorName(err),
            });
        };
    }
}

fn abortParticipantsWithContactMask(
    alloc: std.mem.Allocator,
    original_worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    participants: []const ParticipantTxn,
    participant_ids: []const []const u8,
    slots: []const ParticipantFanoutSlot,
    retained: bool,
) !void {
    if (participants.len == 0) return;
    const worker = original_worker.startRecovery();
    std.debug.assert(participant_ids.len == participants.len and slots.len == participants.len);
    // Contact evidence is invocation-local, not transaction-local. In a
    // retained replay even a definitely unproposed BEGIN can have old intents.
    // Resolve the entire durable cohort; unavailable followers remain enlisted.
    if (retained) return abortParticipants(alloc, worker, txn_id, timestamp, participants, participant_ids, participants.len);

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

    for (participants[1..], 1..) |participant, participant_index| {
        if (slots[participant_index].may_have_transaction_state) {
            worker.resolveGroup(alloc, participant.group_id, participant.table_name, .{
                .restore_staging_scope = participant.restore_staging_scope,
                .restore_staging_plan_id = participant.restore_staging_plan_id,
                .txn_id = txn_id,
                .status = .aborted,
                .commit_version = timestamp,
                .sync_level = .write,
            }) catch |err| {
                // An explicitly missing record proves the failed begin did not
                // create participant state. Other failures remain enlisted so
                // durable coordinator recovery can redeliver the abort.
                if (err != error.TxnNotFound) {
                    std.log.warn("transaction abort delivery failed table={s} group_id={} err={s}", .{
                        participant.table_name,
                        participant.group_id,
                        @errorName(err),
                    });
                    continue;
                }
            };
        }
        worker.acknowledgeGroup(alloc, coordinator.group_id, coordinator.table_name, .{
            .restore_staging_scope = coordinator.restore_staging_scope,
            .restore_staging_plan_id = coordinator.restore_staging_plan_id,
            .txn_id = txn_id,
            .participant = participant_ids[participant_index],
        }) catch |err| {
            std.log.warn("transaction abort acknowledgement deferred table={s} group_id={} err={s}", .{
                participant.table_name,
                participant.group_id,
                @errorName(err),
            });
        };
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
            try std.testing.expectEqual([_]u8{11} ** 32, decoded.req.predicates[0].expected_content_digest.?);
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

            const stats = try db.runTransactionRecoveryOnce(resolver.config());
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

            const stats = try db.runTransactionRecoveryOnce(resolver.config());
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
        test "distributed txn preserves original deadline across participant waves and cleanup" {
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
            const VoprIo = @import("vopr").vopr_io.VoprIo;
            const Recorder = struct {
                clock: *VoprIo,
                expected: PreDecisionContext,
                begins: usize = 0,
                prepares: usize = 0,
                aborts: usize = 0,
                acks: usize = 0,
                cancel: ?*std.atomic.Value(bool) = null,
                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = legacyBegin,
                        .prepare_group = legacyPrepare,
                        .begin_group_with_context = begin,
                        .prepare_group_with_context = prepare,
                        .resolve_first_decision_with_context = decide,
                        .resolve_group = resolve,
                        .status_group = status,
                        .acknowledge_group = acknowledge,
                    } };
                }
                fn legacyBegin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
                    return error.LegacyMustNotRun;
                }
                fn legacyPrepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    return error.LegacyMustNotRun;
                }
                fn check(self: *@This(), context: PreDecisionContext) !void {
                    try std.testing.expectEqual(self.expected.deadline_ns, context.deadline_ns);
                    try std.testing.expect(context.deadline_io.?.userdata == self.expected.deadline_io.?.userdata);
                    try std.testing.expect(context.cancellation.ptr == self.expected.cancellation.ptr);
                    try ensurePreDecisionContextActive(context);
                    self.clock.monotonic_ns += std.time.ns_per_s;
                }
                fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest, context: PreDecisionContext) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try self.check(context);
                    self.begins += 1;
                }
                fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest, context: PreDecisionContext) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try self.check(context);
                    self.prepares += 1;
                    if (self.cancel) |signal| signal.store(true, .release);
                }
                fn decide(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest, _: PreDecisionContext) !void {
                    return error.FirstDecisionMustNotRun;
                }
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, req.status);
                    // Cleanup remains usable after the caller's deadline/cancellation.
                    try std.testing.expect(self.clock.monotonic_ns >= self.expected.deadline_ns.? or self.expected.cancellation.isCancelled());
                    self.aborts += 1;
                }
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .aborted;
                }
                fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.acks += 1;
                }
            };
            inline for (.{ false, true }) |cancelled| {
                var clock = try VoprIo.init(.{ .monotonic_ns = std.time.ns_per_s });
                defer clock.deinit();
                var signal = std.atomic.Value(bool).init(false);
                const context: PreDecisionContext = .{
                    .deadline_ns = (if (cancelled) @as(u64, 100) else 4) * std.time.ns_per_s,
                    .deadline_io = @import("../runtime_io_abi.zig").Borrow.init(&clock.io()),
                    .cancellation = db_mod.types.CancellationToken.fromAtomic(&signal),
                };
                var recorder: Recorder = .{ .clock = &clock, .expected = context, .cancel = if (cancelled) &signal else null };
                const tables = [_]TableCommitRequest{.{ .table_name = "docs", .writes = &.{
                    .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" },
                } }};
                const expected_error = if (cancelled) error.Canceled else error.PreDecisionDeadlineExceeded;
                try std.testing.expectError(expected_error, executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    FakeCatalog.iface(),
                    recorder.worker(),
                    [_]u8{1} ** 16,
                    10_000,
                    10_001,
                    &tables,
                    .write,
                    null,
                    .{ .pre_decision_context = context, .max_parallel_participants = 1 },
                ));
                try std.testing.expectEqual(@as(usize, 2), recorder.begins);
                try std.testing.expectEqual(@as(usize, 1), recorder.prepares);
                try std.testing.expectEqual(@as(usize, 2), recorder.aborts);
                try std.testing.expectEqual(@as(usize, 1), recorder.acks);
                // An already-expired request does not even need a valid catalog.
                try std.testing.expectError(expected_error, executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    undefined,
                    recorder.worker(),
                    [_]u8{2} ** 16,
                    10_000,
                    10_001,
                    &tables,
                    .write,
                    null,
                    .{ .pre_decision_context = context },
                ));
                try std.testing.expectEqual(@as(usize, 2), recorder.begins);
            }
        }

        test "transaction first decision distinguishes rejected admission from accepted and resumed recovery" {
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
            const VoprIo = @import("vopr").vopr_io.VoprIo;
            const Recorder = struct {
                const Mode = enum { rejected, lost_response, accepted, resumed };
                mode: Mode,
                clock: *VoprIo,
                context: PreDecisionContext,
                first: usize = 0,
                aborts: usize = 0,
                recovery: usize = 0,
                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .vtable = &.{
                        .begin_group = legacyBegin,
                        .prepare_group = legacyPrepare,
                        .begin_group_with_context = begin,
                        .prepare_group_with_context = prepare,
                        .resolve_first_decision_with_context = decide,
                        .resolve_group = resolve,
                        .status_group = status,
                        .resolve_group_until = resolveUntil,
                        .status_group_until = statusUntil,
                    } };
                }
                fn legacyBegin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
                    return error.UnexpectedLegacy;
                }
                fn legacyPrepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    return error.UnexpectedLegacy;
                }
                fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest, _: PreDecisionContext) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (self.mode == .resumed) {
                        self.clock.monotonic_ns = self.context.deadline_ns.?;
                        return error.DecisionConflict;
                    }
                }
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest, _: PreDecisionContext) !void {}
                fn decide(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest, context: PreDecisionContext) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(self.context.deadline_ns, context.deadline_ns);
                    try std.testing.expect(context.deadline_io.?.userdata == self.context.deadline_io.?.userdata);
                    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
                    self.first += 1;
                    // Deadline expires inside routing/admission, before receipt or
                    // after acceptance depending on the callback's exact evidence.
                    self.clock.monotonic_ns = self.context.deadline_ns.?;
                    switch (self.mode) {
                        .rejected => return error.PreDecisionNotProposed,
                        .lost_response => return error.Timeout,
                        .accepted => {},
                        .resumed => return error.FirstDecisionMustNotRun,
                    }
                }
                fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expect(self.clock.monotonic_ns >= self.context.deadline_ns.?);
                    if (req.status == .aborted) {
                        try std.testing.expectEqual(Mode.rejected, self.mode);
                        self.aborts += 1;
                    } else {
                        try std.testing.expect(self.mode != .rejected);
                        self.recovery += 1;
                    }
                }
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return .committed;
                }
                fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, id: db_mod.types.TxnId, _: u64) !db_mod.types.TxnStatus {
                    return status(ptr, alloc, group, table, id);
                }
                fn resolveUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, req: TxnResolveRequest, _: u64) !void {
                    return resolve(ptr, alloc, group, table, req);
                }
            };
            inline for (std.meta.tags(Recorder.Mode)) |mode| {
                var clock = try VoprIo.init(.{ .monotonic_ns = std.time.ns_per_s });
                defer clock.deinit();
                const context: PreDecisionContext = .{ .deadline_ns = 10 * std.time.ns_per_s, .deadline_io = @import("../runtime_io_abi.zig").Borrow.init(&clock.io()) };
                var recorder: Recorder = .{ .mode = mode, .clock = &clock, .context = context };
                const tables = [_]TableCommitRequest{.{ .table_name = "docs", .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" } } }};
                const result = executeMultiTableCommitWithOptions(std.testing.allocator, FakeCatalog.iface(), recorder.worker(), [_]u8{3} ** 16, 10_000, 10_001, &tables, .write, null, .{ .pre_decision_context = context, .max_parallel_participants = 1, .report_post_commit_failure = false });
                if (mode == .rejected) {
                    try std.testing.expectError(error.PreDecisionNotProposed, result);
                    try std.testing.expectEqual(@as(usize, 2), recorder.aborts);
                } else {
                    const outcome = try result;
                    try std.testing.expectEqual(std.meta.Tag(CommitOutcome).committed, std.meta.activeTag(outcome));
                    try std.testing.expectEqual(@as(usize, 0), recorder.aborts);
                    try std.testing.expect(recorder.recovery >= 1);
                }
                try std.testing.expectEqual(@as(usize, if (mode == .resumed) 0 else 1), recorder.first);
            }
        }

        test "transaction attempt budgets follow the borrowed transport clock" {
            var vopr_io = try @import("vopr").vopr_io.VoprIo.init(.{ .monotonic_ns = 7 * std.time.ns_per_s });
            defer vopr_io.deinit();
            const borrow = @import("../runtime_io_abi.zig").Borrow.init(&vopr_io.io());
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
            var bounded = worker;
            bounded.pre_decision_context = .{ .deadline_ns = 9 * std.time.ns_per_s, .deadline_io = borrow };
            try std.testing.expectEqual(@as(u64, 9 * std.time.ns_per_s), try bounded.preDecisionDeadlineNs());
            try std.testing.expectEqual(@as(?u64, 8 * std.time.ns_per_s), (try bounded.localPreDecisionContext(9 * std.time.ns_per_s)).deadline_ns);
            vopr_io.monotonic_ns += 2 * std.time.ns_per_s;
            try std.testing.expectEqual(@as(u32, 1_000), (try worker.remainingPreDecisionAttemptBudget(deadline)).server_budget_ms);
            vopr_io.monotonic_ns += 2 * std.time.ns_per_s;
            try std.testing.expectError(error.Timeout, worker.remainingPreDecisionAttemptBudget(deadline));
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
                    } };
                }

                fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
                    return null;
                }

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) anyerror!?void {
                    return null;
                }

                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
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
            try std.testing.expectError(error.DeadlineExceeded, legacy_deadline_begin_worker.worker().beginGroup(std.testing.allocator, 7, "docs", .{
                .txn_id = txn_id,
                .begin_timestamp = 42,
                .participants = &.{"table2:docs:group:7"},
            }));
            try std.testing.expectEqual(@as(usize, 0), legacy_deadline_begin_executor.calls);

            var legacy_deadline_prepare_executor = FakeExecutor{ .first_outcome = null, .first_expected_node_id = 2 };
            var legacy_deadline_prepare_worker = HostedParticipantWorker.init(undefined, LocalMissRouter.iface(), LegacyDeadlineWrites.source(), legacy_deadline_prepare_executor.iface());
            try std.testing.expectError(error.DeadlineExceeded, legacy_deadline_prepare_worker.worker().prepareGroup(std.testing.allocator, 7, "docs", .{
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

        test "transaction recovery shares abort and acknowledgement budget independently of admission" {
            const Fixture = struct {
                deadline: ?u64 = null,
                resolves: usize = 0,
                statuses: usize = 0,
                acks: usize = 0,
                expire_on_ack: bool = false,
                expected_status: db_mod.types.TxnStatus = .aborted,
                fail_first_resolve: bool = true,
                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
                    return error.UnexpectedLegacyCall;
                }
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
                    return error.UnexpectedLegacyCall;
                }
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
                    return error.UnexpectedLegacyCall;
                }
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return error.UnexpectedLegacyCall;
                }
                fn observe(self: *@This(), deadline: u64) !void {
                    if (self.deadline) |original| try std.testing.expectEqual(original, deadline) else self.deadline = deadline;
                }
                fn resolveUntil(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest, deadline: u64) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.observe(deadline);
                    try std.testing.expectEqual(self.expected_status, req.status);
                    self.resolves += 1;
                    if (self.fail_first_resolve and self.resolves == 1) return error.Timeout;
                }
                fn statusUntil(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, deadline: u64) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.observe(deadline);
                    self.statuses += 1;
                    return .aborted;
                }
                fn ackUntil(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnAcknowledgeRequest, deadline: u64) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.observe(deadline);
                    self.acks += 1;
                    if (self.expire_on_ack) {
                        while (platform_time.monotonicNs() < deadline) std.atomic.spinLoopHint();
                        return error.Timeout;
                    }
                }
                fn worker(self: *@This()) ParticipantWorker {
                    return .{ .ptr = self, .recovery_timeout_ns = 10 * std.time.ns_per_ms, .pre_decision_context = .{ .deadline_ns = 0 }, .vtable = &.{
                        .begin_group = begin,
                        .prepare_group = prepare,
                        .resolve_group = resolve,
                        .status_group = status,
                        .resolve_group_until = resolveUntil,
                        .status_group_until = statusUntil,
                        .acknowledge_group_until = ackUntil,
                    } };
                }
            };
            const participants = [_]ParticipantTxn{
                .{ .table_name = "docs", .group_id = 1, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 2, .topology_epoch = 1 },
                .{ .table_name = "docs", .group_id = 3, .topology_epoch = 1 },
            };
            const ids = [_][]const u8{ "one", "two", "three" };
            const id = try parseTxnIdHex("abcdefabcdefabcdefabcdefabcdefab");
            var healthy: Fixture = .{};
            try abortParticipants(std.testing.allocator, healthy.worker(), id, 7, &participants, &ids, 3);
            try std.testing.expectEqual(@as(usize, 3), healthy.resolves);
            try std.testing.expectEqual(@as(usize, 1), healthy.statuses);
            try std.testing.expectEqual(@as(usize, 2), healthy.acks);
            const restarted = healthy.worker().startRecovery();
            try std.testing.expect(restarted.pre_decision_context == null);
            try std.testing.expectEqual(restarted.recovery_deadline_ns, restarted.startRecovery().recovery_deadline_ns);
            var expired: Fixture = .{ .expire_on_ack = true };
            try abortParticipants(std.testing.allocator, expired.worker(), id, 7, &participants, &ids, 3);
            try std.testing.expectEqual(@as(usize, 2), expired.resolves);
            try std.testing.expectEqual(@as(usize, 1), expired.acks);
            // The second follower remains enlisted: no resolve or ACK was dispatched after the same window expired.
            var committed: Fixture = .{ .expected_status = .committed, .fail_first_resolve = false, .expire_on_ack = true };
            const recovery = committed.worker().startRecovery();
            var first: ParticipantFanoutSlot = .{};
            ResolveFollowerFanoutTask.run(recovery, &participants[0], &participants[1], ids[1], id, 7, .write, true, .none, &first);
            try std.testing.expect(first.propagation_pending);
            try std.testing.expectEqual(error.Timeout, first.acknowledgement_err.?);
            var second: ParticipantFanoutSlot = .{};
            ResolveFollowerFanoutTask.run(recovery, &participants[0], &participants[2], ids[2], id, 7, .write, true, .none, &second);
            try std.testing.expect(second.propagation_pending);
            try std.testing.expectEqual(@as(usize, 1), committed.resolves);
            try std.testing.expectEqual(@as(usize, 1), committed.acks);
        }

        test "distributed txn recovery status requests keep the worker deadline" {
            const Recorder = struct {
                bounded_calls: usize = 0,
                scoped_calls: usize = 0,
                last_deadline_ns: ?u64 = null,

                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
                fn legacy(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return error.UnexpectedLegacyCall;
                }
                fn bounded(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.bounded_calls += 1;
                    self.last_deadline_ns = deadline_ns;
                    return .pending;
                }
                fn scoped(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnStatusRequest, deadline_ns: ?u64) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.scoped_calls += 1;
                    self.last_deadline_ns = deadline_ns;
                    return .pending;
                }
            };
            var recorder: Recorder = .{};
            const id = try parseTxnIdHex("abcdefabcdefabcdefabcdefabcdefab");
            const worker: ParticipantWorker = .{ .ptr = &recorder, .recovery_timeout_ns = std.time.ns_per_s, .vtable = &.{
                .begin_group = Recorder.begin,
                .prepare_group = Recorder.prepare,
                .resolve_group = Recorder.resolve,
                .status_group = Recorder.legacy,
                .status_group_until = Recorder.bounded,
                .status_group_scoped = Recorder.scoped,
            } };
            const recovery = worker.startRecovery();
            const recovery_deadline = recovery.recovery_deadline_ns.?;
            try std.testing.expectEqual(db_mod.types.TxnStatus.pending, try recovery.statusGroupWithRequest(std.testing.allocator, 1, "docs", .{ .txn_id = id }, null));
            try std.testing.expectEqual(recovery_deadline, recorder.last_deadline_ns.?);
            try std.testing.expectEqual(@as(usize, 1), recorder.bounded_calls);

            const shorter_deadline = recovery_deadline - std.time.ns_per_ms;
            try std.testing.expectEqual(db_mod.types.TxnStatus.pending, try recovery.statusGroupWithRequest(std.testing.allocator, 1, "docs", .{ .txn_id = id }, shorter_deadline));
            try std.testing.expectEqual(shorter_deadline, recorder.last_deadline_ns.?);
            const scoped_req: TxnStatusRequest = .{ .txn_id = id, .restore_staging_scope = [_]u8{1} ** 32, .restore_staging_plan_id = [_]u8{1} ** 16 };
            try std.testing.expectEqual(db_mod.types.TxnStatus.pending, try recovery.statusGroupWithRequest(std.testing.allocator, 1, "docs", scoped_req, null));
            try std.testing.expectEqual(recovery_deadline, recorder.last_deadline_ns.?);
            try std.testing.expectEqual(@as(usize, 1), recorder.scoped_calls);

            var expired = recovery;
            expired.recovery_deadline_ns = 0;
            try std.testing.expectError(error.CommitDecisionUnknown, expired.statusGroupWithRequest(std.testing.allocator, 1, "docs", .{ .txn_id = id }, null));
            try std.testing.expectError(error.CommitDecisionUnknown, expired.statusGroupWithRequest(std.testing.allocator, 1, "docs", scoped_req, null));
            try std.testing.expectEqual(@as(usize, 2), recorder.bounded_calls);
            try std.testing.expectEqual(@as(usize, 1), recorder.scoped_calls);
        }

        test "transaction recovery retains shutdown cancellation and rejects unbounded fallback" {
            var canceled = std.atomic.Value(bool).init(true);
            const token = db_mod.types.CancellationToken.fromAtomic(&canceled);
            const deadline = platform_time.monotonicNs() + std.time.ns_per_s;
            const scope = DecisionRecoveryCancellation{ .deadline_ns = deadline, .other = token };
            try std.testing.expectError(error.Canceled, scope.token().check());
            canceled.store(false, .release);
            try scope.token().check();
            const expired = DecisionRecoveryCancellation{ .deadline_ns = 0, .other = token };
            try std.testing.expectError(error.Canceled, expired.token().check());
            const Fixture = struct {
                fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
                fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
                fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
                    return error.UnexpectedLegacyCall;
                }
                fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    return error.UnexpectedLegacyCall;
                }
                fn combined(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest, until: u64, cancellation: db_mod.types.CancellationToken) !void {
                    try std.testing.expect(until > platform_time.monotonicNs());
                    try cancellation.check();
                }
            };
            const worker: ParticipantWorker = .{ .ptr = undefined, .recovery_deadline_ns = deadline, .vtable = &.{ .begin_group = Fixture.begin, .prepare_group = Fixture.prepare, .resolve_group = Fixture.resolve, .status_group = Fixture.status } };
            const id = try parseTxnIdHex("abcdefabcdefabcdefabcdefabcdefab");
            try std.testing.expectError(error.CommitPropagationIncomplete, worker.resolveGroupWithCancellation(std.testing.allocator, 1, "docs", .{ .txn_id = id, .status = .committed, .commit_version = 7 }, token));
            try std.testing.expectError(error.CommitPropagationIncomplete, worker.acknowledgeGroup(std.testing.allocator, 1, "docs", .{ .txn_id = id, .participant = "two" }));
            const combined_worker: ParticipantWorker = .{ .ptr = undefined, .recovery_deadline_ns = deadline, .vtable = &.{ .begin_group = Fixture.begin, .prepare_group = Fixture.prepare, .resolve_group = Fixture.resolve, .status_group = Fixture.status, .resolve_group_until_with_cancellation = Fixture.combined } };
            canceled.store(true, .release);
            try std.testing.expectError(error.Canceled, combined_worker.resolveGroupWithCancellation(std.testing.allocator, 1, "docs", .{ .txn_id = id, .status = .committed, .commit_version = 7 }, token));
            canceled.store(false, .release);
            try combined_worker.resolveGroupWithCancellation(std.testing.allocator, 1, "docs", .{ .txn_id = id, .status = .committed, .commit_version = 7 }, token);
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
                begins: std.ArrayListUnmanaged(u64) = .empty,
                prepares: std.ArrayListUnmanaged(u64) = .empty,
                read_only_prepares: usize = 0,
                integrity_prepares: usize = 0,
                semantic_prepares: usize = 0,
                activation_prepares: usize = 0,
                coordinator_group: u64 = 7001,
                resolves: std.ArrayListUnmanaged(struct {
                    group_id: u64,
                    status: db_mod.types.TxnStatus,
                    sync_level: db_mod.types.SyncLevel,
                }) = .empty,
                acknowledgements: std.ArrayListUnmanaged(u64) = .empty,

                fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
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
                    try std.testing.expect(req.req.writes.len + req.req.deletes.len + req.req.predicates.len + req.req.integrity.len > 0);
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
                        try std.testing.expectEqual(@as(?[32]u8, [_]u8{8} ** 32), req.req.relational_integrity_generation_set);
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
            var routed_address = try @import("../storage/db/relational_integrity_contract.zig").Address.init([_]u8{1} ** 16, "tuple");
            // This transport test deliberately supplies an explicit routing digest;
            // native address validation is separately tested at the storage boundary.
            routed_address.routing = [_]u8{'z'} ** 32;
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
                    .relational_integrity_generation_set = [_]u8{8} ** 32,
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
                resolve_error: ?anyerror = null,
                abort_calls: usize = 0,

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
                    if (req.status == .aborted) self.abort_calls += 1;
                    try std.testing.expectEqual(if (self.observed_status == .pending) db_mod.types.TxnStatus.aborted else self.observed_status, req.status);
                    if (self.resolve_error) |err| return err;
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

                fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.status_calls += 1;
                    if (self.status_calls > 1 and (if (self.resolve_error) |err| err == error.ConnectionResetByPeer else false)) return .aborted;
                    return self.observed_status;
                }

                fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId, deadline_ns: u64) !db_mod.types.TxnStatus {
                    try ensureDecisionRecoveryDeadline(deadline_ns);
                    return status(ptr, alloc, group_id, table_name, txn_id);
                }
            };

            var recorder = Recorder{};
            const txn_id = try parseTxnIdHex("0123456789abcdeffedcba9876543210");
            for ([_]anyerror{ error.DecisionConflict, error.RaftBatchWriteOutcomeUnknown, error.UnexpectedHttpStatus, error.Timeout, error.UnknownGroup, error.PreDecisionNotProposed }) |begin_error| {
                recorder = .{ .begin_error = begin_error };
                const outcome = try executeMultiTableCommitWithOptions(
                    std.testing.allocator,
                    FakeCatalog.iface(),
                    recorder.worker(),
                    txn_id,
                    10_000,
                    10_001,
                    &.{.{ .table_name = "docs", .writes = &.{
                        .{ .key = "doc:a", .value = "{}" },
                        .{ .key = "doc:z", .value = "{}" },
                    } }},
                    .write,
                    null,
                    .{ .retain_terminal = true },
                );
                try std.testing.expect(outcome == .committed);
                try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
                try std.testing.expectEqual(@as(usize, 1), recorder.status_calls);
                try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
                try std.testing.expectEqual(@as(usize, 2), recorder.resolve_calls);
            }
            // A committed status was already observed. Even a subsequent
            // conflicting/missing/corrupt coordinator response cannot authorize
            // abort or dispatch follower decisions with unconfirmed metadata.
            for ([_]anyerror{ error.DecisionConflict, error.TxnNotFound, error.InvalidTxnRecord, error.ConnectionResetByPeer }) |retry_error| {
                for ([_]bool{ true, false }) |report_failure| {
                    recorder = .{ .resolve_error = retry_error };
                    const resumed = executeMultiTableCommitWithOptions(
                        std.testing.allocator,
                        FakeCatalog.iface(),
                        recorder.worker(),
                        txn_id,
                        10_000,
                        10_001,
                        &.{.{ .table_name = "docs", .writes = &.{
                            .{ .key = "doc:a", .value = "{}" },
                            .{ .key = "doc:z", .value = "{}" },
                        } }},
                        .write,
                        null,
                        .{ .retain_terminal = true, .report_post_commit_failure = report_failure },
                    );
                    if (report_failure) {
                        try std.testing.expectError(error.CommitPropagationIncomplete, resumed);
                    } else {
                        const committed = try resumed;
                        try std.testing.expect(committed == .committed);
                        try std.testing.expect(committed.committed.propagation_pending);
                        try std.testing.expectEqual(@as(usize, 2), committed.committed.participant_count);
                        try std.testing.expectEqual(@as(?u64, 7001), committed.committed.coordinator_group_id);
                    }
                    try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
                    try std.testing.expectEqual(@as(usize, if (retry_error == error.ConnectionResetByPeer) 2 else 1), recorder.status_calls);
                    try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
                    try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
                    try std.testing.expectEqual(@as(usize, 0), recorder.abort_calls);
                }
            }
            {
                // A lost BEGIN outcome on a stable session ID must remain
                // retryable. In particular, an unknown Raft apply result is not
                // evidence that the session should be durably aborted.
                for ([_]u64{ 7001, 7002 }) |failed_group| {
                    var recovery_recorder = Recorder{
                        .failed_begin_group = failed_group,
                        .begin_error = error.RaftBatchWriteOutcomeUnknown,
                        .observed_status = .pending,
                    };
                    const recovery_txn_id = try parseTxnIdHex("0123456789abcdeffedcba9876543210");
                    const request = &[_]TableCommitRequest{.{ .table_name = "docs", .writes = &.{
                        .{ .key = "doc:a", .value = "{}" },
                        .{ .key = "doc:z", .value = "{}" },
                    } }};
                    try std.testing.expectError(error.CommitDecisionUnknown, executeMultiTableCommitWithOptions(
                        std.testing.allocator,
                        FakeCatalog.iface(),
                        recovery_recorder.worker(),
                        recovery_txn_id,
                        10_000,
                        10_001,
                        request,
                        .write,
                        null,
                        .{ .retain_terminal = true },
                    ));
                    try std.testing.expectEqual(@as(usize, 0), recovery_recorder.resolve_calls);
                    recovery_recorder.failed_begin_group = 0;
                    recovery_recorder.observed_status = .committed;
                    recovery_recorder.expect_live_topology = true;
                    const resumed = try executeMultiTableCommitWithOptions(
                        std.testing.allocator,
                        FakeCatalog.iface(),
                        recovery_recorder.worker(),
                        recovery_txn_id,
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
                        var recovery_recorder = Recorder{ .failed_begin_group = failed_group, .begin_error = error.PreDecisionNotProposed, .observed_status = .pending, .follower_resolve_error = resolve_error };
                        const result = executeMultiTableCommitWithOptions(
                            std.testing.allocator,
                            FakeCatalog.iface(),
                            recovery_recorder.worker(),
                            @splat(7),
                            10_000,
                            10_001,
                            &.{.{ .table_name = "docs", .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" } } }},
                            .write,
                            null,
                            .{ .retain_terminal = true },
                        );
                        if (failed_group == 7001) try std.testing.expectError(error.TransactionBeginFailed, result) else try std.testing.expect((try result) == .conflict);
                        try std.testing.expectEqual(@as(usize, 2), recovery_recorder.resolve_calls);
                        try std.testing.expectEqual(resolve_error == null, recovery_recorder.follower_resolved);
                        try std.testing.expectEqual(resolve_error == null, recovery_recorder.follower_acknowledged);
                    }
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

                fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
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
