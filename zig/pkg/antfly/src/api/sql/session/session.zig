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

//! Shared SQL protocol/HTTP control adapter. All state belongs to the durable
//! transaction registry; commit is the native coordinator, never HTTP replay.
const std = @import("std");
const sql = @import("antfly_local_sources").sql_session;
const transactions = @import("../../transactions.zig");
const clock = @import("antfly_local_sources").sql_catalog.Clock;
const http = @import("../../http_server.zig");
const operation = @import("antfly_local_sources").api_operation;
const connections = @import("connections.zig");
const platform_time = @import("antfly_platform").time;

pub const Coordinator = struct {
    server: *http.ApiHttpServer,
    identity: *?http.AuthenticatedIdentity,
    context: operation.RequestContext,
    /// Pgwire owns its connection overlay; HTTP instead resolves the durable
    /// session's current value inside the commit body's atomic publication.
    sync_level: ?@import("antfly_local_sources").storage_db_types.SyncLevel = null,
    /// The outer SQL control or PREPARE owner may lend its execution lease.
    /// Only that exact registry and transaction may reuse it; this coordinator
    /// never releases a borrowed lease or silently treats a busy lock as owned.
    borrowed_execution: ?*const transactions.SessionRegistry.CommitExecution = null,

    fn execution(self: *Coordinator, id: sql.Id) !?transactions.SessionRegistry.CommitExecution {
        if (self.borrowed_execution) |held| {
            if (!held.matches(&self.server.txn_sessions, id)) return error.InvalidSqlBackendResponse;
            return null;
        }
        return self.server.txn_sessions.tryAcquireCommitExecution(id) orelse error.SqlWriteCapacityUnavailable;
    }

    /// Rollback must cross the same durable scope/decision barrier as commit.
    /// An existing sealed decision cannot be replaced by a local abort.
    pub fn rollback(ptr: *anyopaque, _: sql.Scope, id: sql.Id) !void {
        const self: *Coordinator = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
        const lease = try self.execution(id);
        defer if (lease) |held| held.release();
        var state = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(self.server.alloc);
        if (state.terminal_abort) return;
        if (state.execution_started or state.terminal != null or state.terminal_prepare) return error.SqlTransactionOutcomeUnknown;
        const result = try @import("prepared_coordinator.zig").commitScoped(self.server, self.identity.*, self.context, id, &.{}, true);
        if (result.outcome != .aborted) return error.SqlTransactionOutcomeUnknown;
    }

    /// Publish SQL metadata and native control intent together. Recovery only
    /// drains native control; it never evaluates this SQL operation again.
    const SavepointPhase = enum { admission, session, source, executing, load, renewal, plan, publication, drain };

    pub fn savepoint(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8, action: transactions.SessionRegistry.SqlSavepointAction) !void {
        var phase: SavepointPhase = .admission;
        return savepointChecked(ptr, scope, id, name, action, &phase) catch |err| {
            if (comptime @import("builtin").mode == .debug) {
                const Once = struct {
                    var reported: std.atomic.Value(bool) = .init(false);
                };
                if (!Once.reported.swap(true, .monotonic)) {
                    const coordinator: *Coordinator = @ptrCast(@alignCast(ptr));
                    std.debug.print("native SQL savepoint phase={s} owner={} err={s}\n", .{ @tagName(phase), coordinator.context.sql_relation_owner != null, @errorName(err) });
                }
            }
            return err;
        };
    }

    fn savepointChecked(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8, action: transactions.SessionRegistry.SqlSavepointAction, phase: *SavepointPhase) !void {
        const self: *Coordinator = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
        const fence = self.context.sql_command_fence orelse return error.SqlConnectionBusy;
        const store = self.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        phase.* = .session;
        var state = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(self.server.alloc);
        const connection = state.connection_id orelse return error.SqlConnectionNotFound;
        const producer = @import("command_producer.zig");
        const io = self.server.embedding_provider_runtime.io;
        phase.* = .source;
        const instance = try self.server.sql_connection_admission.executionInstance(io);
        const frame: @import("advisory_execution.zig").Frame = .{ .source = store, .connection = connection, .principal = scope.principal, .instance = instance, .fence = fence.*, .actor = try @import("../recovery/authorization_recovery.zig").actor(self.server, self.identity.*) };
        var native: producer.NativePort = .{ .server = self.server, .context = self.context };
        for (0..8) |_| {
            phase.* = .executing;
            try frame.requireExecuting(self.server.alloc, platform_time.realtimeNs() / std.time.ns_per_ms);
            phase.* = .load;
            var owned = (try producer.load(store, self.server.alloc, connection)) orelse return error.SqlConnectionNotFound;
            defer owned.deinit();
            var nonce: [16]u8 = undefined;
            try io.randomSecure(&nonce);
            if (owned.value.pending != null and owned.value.pending.?.operation == .renew) {
                phase.* = .renewal;
                producer.advance(store, self.server.alloc, scope.principal, owned.value, instance, platform_time.realtimeNs() / std.time.ns_per_ms, nonce, native.port()) catch |err| switch (err) {
                    error.MetadataMutationOutcomeUnknown, error.SqlConnectionChanged => {},
                    else => return err,
                };
                continue;
            }
            phase.* = .plan;
            var plan = try producer.observeControlPlan(self.server.alloc, owned.value, instance, id, platform_time.realtimeNs() / std.time.ns_per_ms, nonce, native.port());
            try self.context.ensureActive();
            plan.now_ms = platform_time.realtimeNs() / std.time.ns_per_ms;
            phase.* = .publication;
            _ = (self.server.txn_sessions.changeSqlSavepoint(self.server.alloc, id, name, action, plan) catch |err| switch (err) {
                // A failed source CAS aborts the whole SQL publication. Only
                // that proven pre-publication failure permits replanning.
                error.SqlConnectionChanged => continue,
                else => return err,
            }) orelse return error.SqlSavepointNotFound;
            phase.* = .drain;
            try self.drainControl(scope.principal, connection, instance, owned.value.ticket.?);
            return;
        }
        return error.SqlConnectionBusy;
    }

    /// Publish failure and rollback intent together, then drain only control
    /// work under a bounded cleanup budget. The failed SQL shell stays attached.
    pub fn failed(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !void {
        const self: *Coordinator = @ptrCast(@alignCast(ptr));
        const old = self.context;
        defer self.context = old;
        self.context.cancellation = .none;
        self.context.deadline_io = null;
        self.context.deadline_ns = platform_time.monotonicNs() +| 3 * std.time.ns_per_s;
        const fence = self.context.sql_command_fence orelse return error.SqlConnectionBusy;
        const store = self.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        var state = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(self.server.alloc);
        if (state.execution_started or state.terminal != null or state.terminal_prepare or state.terminal_abort) return error.TransactionCommitSealed;
        const connection = state.connection_id orelse return error.SqlConnectionNotFound;
        const producer = @import("command_producer.zig");
        const io = self.server.embedding_provider_runtime.io;
        const instance = try self.server.sql_connection_admission.executionInstance(io);
        const frame: @import("advisory_execution.zig").Frame = .{ .source = store, .connection = connection, .principal = scope.principal, .instance = instance, .fence = fence.*, .actor = try @import("../recovery/authorization_recovery.zig").actor(self.server, self.identity.*) };
        var native: producer.NativePort = .{ .server = self.server, .context = self.context };
        while (true) {
            try self.context.ensureActive();
            const now = platform_time.realtimeNs() / std.time.ns_per_ms;
            try frame.requireExecuting(self.server.alloc, now);
            var owned = (try producer.load(store, self.server.alloc, connection)) orelse return error.SqlConnectionNotFound;
            defer owned.deinit();
            var nonce: [16]u8 = undefined;
            try io.randomSecure(&nonce);
            if (owned.value.control_effect != null) {
                try self.drainControl(scope.principal, connection, instance, owned.value.ticket.?);
                continue;
            }
            if (owned.value.pending != null and owned.value.pending.?.operation == .renew) {
                producer.advance(store, self.server.alloc, scope.principal, owned.value, instance, now, nonce, native.port()) catch |err| switch (err) {
                    error.MetadataMutationOutcomeUnknown, error.SqlConnectionChanged => {},
                    else => return err,
                };
                continue;
            }
            var cut = try native.port().snapshot(self.server.alloc, owned.value);
            defer cut.deinit();
            if (cut.value.owner) |owner| if (owner.waiting) |waiting| {
                const registration = owned.value.registration;
                _ = try native.port().submit(self.server.alloc, .{ .admission = .{ .head = cut.value.head, .nonce = nonce }, .incarnation = registration.lease.backend.incarnation, .standalone = registration.standalone, .database = registration.lease.database, .actor = owned.value.actor, .backend = registration.lease.backend, .operation = .{ .cancel = .{ .command = owned.value.ticket.?.command, .effect = waiting } } });
                continue;
            };
            var plan = try producer.observeControlPlan(self.server.alloc, owned.value, instance, id, now, nonce, native.port());
            plan.failure = true;
            self.server.txn_sessions.failSqlWithControl(self.server.alloc, id, plan) catch |err| switch (err) {
                error.SqlConnectionChanged => continue,
                else => return err,
            };
            try self.drainControl(scope.principal, connection, instance, owned.value.ticket.?);
            return;
        }
    }

    /// Metadata-first DISCARD: reset the logical connection and its resources
    /// together with a durable unlock-all intent, then drain native control.
    pub fn discard(self: *Coordinator, connection: [32]u8, expected_revision: u64) !void {
        try self.context.ensureActive();
        const fence = self.context.sql_command_fence orelse return error.SqlConnectionBusy;
        const store = self.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        const producer = @import("command_producer.zig");
        const io = self.server.embedding_provider_runtime.io;
        const principal = http.transactionPrincipal(self.identity.*) orelse "";
        const instance = try self.server.sql_connection_admission.executionInstance(io);
        const frame: @import("advisory_execution.zig").Frame = .{ .source = store, .connection = connection, .principal = principal, .instance = instance, .fence = fence.*, .actor = try @import("../recovery/authorization_recovery.zig").actor(self.server, self.identity.*) };
        var native: producer.NativePort = .{ .server = self.server, .context = self.context };
        for (0..8) |_| {
            try frame.requireExecuting(self.server.alloc, platform_time.realtimeNs() / std.time.ns_per_ms);
            var owned = (try producer.load(store, self.server.alloc, connection)) orelse return error.SqlConnectionNotFound;
            defer owned.deinit();
            var nonce: [16]u8 = undefined;
            try io.randomSecure(&nonce);
            if (owned.value.pending != null and owned.value.pending.?.operation == .renew) {
                producer.advance(store, self.server.alloc, principal, owned.value, instance, platform_time.realtimeNs() / std.time.ns_per_ms, nonce, native.port()) catch |err| switch (err) {
                    error.MetadataMutationOutcomeUnknown, error.SqlConnectionChanged => {},
                    else => return err,
                };
                continue;
            }
            var plan = try producer.observeControlPlan(self.server.alloc, owned.value, instance, null, platform_time.realtimeNs() / std.time.ns_per_ms, nonce, native.port());
            try self.context.ensureActive();
            plan.now_ms = platform_time.realtimeNs() / std.time.ns_per_ms;
            _ = connections.mutateWithCommand(store, .{ .discard_scoped = .{ .id = connection, .expected_revision = expected_revision, .plan = plan } }, principal, self.server.localSessionNodeId(), plan.now_ms, fence) catch |err| switch (err) {
                error.SqlConnectionChanged => {
                    // A different connection revision is not a heartbeat CAS
                    // race. Never overwrite another logical metadata change.
                    var logical = try connections.loadWithCommand(store, self.server.alloc, connection, principal, self.server.localSessionNodeId(), plan.now_ms, fence);
                    defer logical.deinit();
                    if (logical.value.revision != expected_revision) return err;
                    continue;
                },
                else => return err,
            };
            try self.drainControl(principal, connection, instance, owned.value.ticket.?);
            return;
        }
        return error.SqlConnectionBusy;
    }

    fn drainControl(self: *Coordinator, principal: []const u8, connection: [32]u8, instance: [16]u8, ticket: @import("command_producer.zig").Ticket) !void {
        const producer = @import("command_producer.zig");
        const store = self.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        const io = self.server.embedding_provider_runtime.io;
        var native: producer.NativePort = .{ .server = self.server, .context = self.context };
        var quanta: u8 = 0;
        while (true) {
            try self.context.ensureActive();
            var current = (try producer.load(store, self.server.alloc, connection)) orelse return error.SqlConnectionNotFound;
            defer current.deinit();
            if (current.value.recovery or current.value.phase != .executing or !std.meta.eql(current.value.ticket, @as(?producer.Ticket, ticket))) return error.SqlConnectionBusy;
            if (current.value.control_effect == null) return;
            var nonce: [16]u8 = undefined;
            try io.randomSecure(&nonce);
            const now_ms = platform_time.realtimeNs() / std.time.ns_per_ms;
            const progress = if (current.value.pending.?.operation != .renew and current.value.registration.lease.expires_at_ms > now_ms and current.value.registration.lease.expires_at_ms - now_ms <= producer.renew_before_ms)
                producer.renew(store, principal, current.value, instance, now_ms +| producer.execution_lease_ms, current.value.pending.?.admission.head, nonce, now_ms)
            else
                producer.advance(store, self.server.alloc, principal, current.value, instance, now_ms, nonce, native.port());
            progress catch |err| switch (err) {
                // Retry the already published control, including renewal CAS
                // races. SQL metadata publication must never run a second time.
                error.MetadataMutationOutcomeUnknown, error.SqlConnectionChanged => {},
                else => return err,
            };
            quanta += 1;
            if (quanta == 8) {
                try io.sleep(.fromMilliseconds(1), .awake);
                quanta = 0;
            }
        }
    }

    fn retire(self: *Coordinator, guard: transactions.Mutation, version: transactions.DurableSessionStore.Version, commit_settings: bool) !sql.CommitResult {
        const retired = self.server.txn_sessions.finishSqlCommitBeforeExecution(self.server.alloc, guard, version, commit_settings) catch |err| switch (err) {
            error.SessionLeaseLost => return err,
            else => false,
        };
        return .{ .outcome = if (!retired) .unknown else if (commit_settings) .committed else .aborted, .reconciliation_id = guard.id };
    }

    pub fn commit(ptr: *anyopaque, _: sql.Scope, guard: transactions.Mutation) !sql.CommitResult {
        const id = guard.id;
        const self: *Coordinator = @ptrCast(@alignCast(ptr));
        const server = self.server;
        const alloc = server.alloc;
        try self.context.ensureActive();
        const lease = try self.execution(id);
        defer if (lease) |held| held.release();
        const info = (try server.txn_sessions.readInfo(id)) orelse return error.SqlTransactionNotActive;
        if (info.version.owner != server.localSessionNodeId()) return error.SessionLeaseLost;
        if (info.terminal_abort) return .{ .outcome = .aborted, .reconciliation_id = id };
        // The owner must know the completion reference before any COMMIT
        // write can leave its outcome uncertain, including session retirement.
        if (self.context.sql_completion) |completion| try completion.prepare_transaction(completion.ptr, id);
        var lake_validation: @import("../execution/execution.zig").Adapter = .{ .server = server, .identity = self.identity, .context = self.context };
        lake_validation.validateAcceptedSerializable(alloc, id) catch |err| {
            if (err != error.SqlWriteConflict) return err;
            if (self.context.sql_command_fence != null) return @import("prepared_coordinator.zig").commitScoped(server, self.identity.*, self.context, id, &.{}, true);
            return self.retire(guard, info.version, false);
        };
        var request = (if (self.context.sql_command_fence) |fence| try server.txn_sessions.cloneScopedSqlCommitRequest(alloc, id, self.sync_level, fence) else server.txn_sessions.cloneOwnedCommitRequest(alloc, guard, null) catch |err| switch (err) {
            error.SessionWriteOutcomeUnknown => return .{ .outcome = .unknown, .reconciliation_id = id },
            else => return err,
        }) orelse {
            if (self.context.sql_command_fence != null) return @import("prepared_coordinator.zig").commitScoped(server, self.identity.*, self.context, id, &.{}, false);
            return self.retire(guard, info.version, true);
        };
        defer request.deinit(alloc);
        const frozen = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
        var authorization_arena = std.heap.ArenaAllocator.init(alloc);
        defer authorization_arena.deinit();
        var sql_scope = try @import("../execution/authorization_runtime.zig").TransactionScope.init(server, self.identity.*, self.context, authorization_arena.allocator(), request);
        defer if (sql_scope) |*authority_owner| authority_owner.deinit();
        if (!(try server.transactionRequestAuthorizedWithSql(self.identity.*, request, if (sql_scope) |*scope| scope else null))) return error.Forbidden;
        const commit_context = if (sql_scope) |*scope| scope.context() else self.context;
        const tables_view = try request.distributedTables(alloc);
        defer tables_view.deinit();
        const tables = tables_view.tables;
        try server.validateCommitTablesAgainstSchema(self.context, tables);
        if (try server.validateCommitReadSet(request)) |conflict| {
            if (self.context.sql_command_fence != null) {
                var outcome = try @import("prepared_coordinator.zig").commitScoped(server, self.identity.*, commit_context, id, &.{}, true);
                if (outcome.outcome == .aborted and conflict.reason == .unique_constraint_violation) outcome.constraint_failure = .unique_constraint_violation;
                return outcome;
            }
            var outcome = try self.retire(guard, request.session_version.?, false);
            if (outcome.outcome == .aborted and conflict.reason == .unique_constraint_violation) outcome.constraint_failure = .unique_constraint_violation;
            return outcome;
        }
        if (tables.len == 0) {
            if (self.context.sql_command_fence != null) return @import("prepared_coordinator.zig").commitScoped(server, self.identity.*, commit_context, id, &.{}, false);
            return self.retire(guard, request.session_version.?, true);
        }
        const source = server.table_writes orelse return error.UnsupportedSqlExecution;
        var prepared = try server.preparePublicCommitWithIntegrity(alloc, tables, commit_context);
        defer prepared.deinit();
        var proof_arena = std.heap.ArenaAllocator.init(alloc);
        defer proof_arena.deinit();
        const signed = try server.signCurrentRowPolicyMutationParticipants(proof_arena.allocator(), prepared.tables, commit_context);
        if (self.context.sql_command_fence != null) return @import("prepared_coordinator.zig").commitScoped(server, self.identity.*, commit_context, id, signed, false);
        var plan = (server.txn_sessions.sealExecutionPlan(alloc, guard, signed) catch |err| switch (err) {
            error.SessionWriteOutcomeUnknown => return .{ .outcome = .unknown, .reconciliation_id = id },
            else => return err,
        }) orelse return error.SqlTransactionNotActive;
        defer plan.deinit();
        // Once the durable plan is sealed, no error is safe to turn into a
        // replay invitation. Maintenance owns completion under this same id.
        const owner_tables = try @import("../catalog/relation_lifetimes.zig").attachOwnerMinters(server, proof_arena.allocator(), commit_context, plan.value);
        const execution_tables = try @import("../triggers/authority.zig").attachMinters(server, proof_arena.allocator(), commit_context, owner_tables);
        const outcome = (source.commitTransactionAttempt(alloc, id, info.begin_timestamp, execution_tables, frozen.sync_level, plan.firstAttempt(), self.context.cancellation) catch |err| {
            const rejection = transactions.classifyCommitRejection(plan.firstAttempt(), err);
            if (rejection == .fresh or rejection == .confirmed) {
                _ = (server.txn_sessions.recordSqlAttemptAbort(alloc, id, if (rejection == .fresh) plan.publication else null, transactions.SqlAbortAcknowledgement.fromCause(transactions.abortCause(err))) catch return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
                return .{ .outcome = .aborted, .abort_cause = transactions.abortCause(err), .reconciliation_id = id };
            }
            if (err == error.EnrichmentWorkerFailed) {
                // Persist terminal repair debt before exposing it. The legacy
                // error lacks coordinator metadata; maintenance retains the
                // sealed plan and recovers that acknowledgement under this id.
                _ = (server.txn_sessions.recordTerminalCommitWithRepair(alloc, id, .committed, true, null, null) catch return .{ .outcome = .committed_pending, .reconciliation_id = id }) orelse return .{ .outcome = .committed_pending, .reconciliation_id = id };
                return .{ .outcome = .committed_repair_required, .reconciliation_id = id };
            }
            return .{ .outcome = switch (err) {
                error.CommitVisibilityNotSatisfied, error.EnrichmentWaitCanceled, error.EnrichmentWaitTimeout, error.EnrichmentRetryInProgress, error.CommitPropagationIncomplete => .committed_pending,
                else => .unknown,
            }, .reconciliation_id = id };
        }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
        switch (outcome) {
            .conflict => |conflict| {
                // The storage coordinator has proved a definite abort, but
                // connection detachment is a separate fallible publication.
                // Persist the abort before responding so a lost detach can be
                // reconciled without treating a missing txn as an abort.
                const unknown: sql.CommitResult = .{ .outcome = .unknown, .reconciliation_id = id };
                const persisted = server.txn_sessions.recordAttemptAbortWithReason(alloc, id, plan.publication, conflict.reason);
                _ = (persisted catch return unknown) orelse return unknown;
                const reason: ?sql.ConstraintFailure = if (conflict.reason) |value| switch (value) {
                    .unique_constraint_violation => .unique_constraint_violation,
                    .foreign_key_parent_missing => .foreign_key_parent_missing,
                    .foreign_key_referenced => .foreign_key_referenced,
                } else null;
                return .{ .outcome = .aborted, .reconciliation_id = id, .constraint_failure = reason };
            },
            .committed => |committed| {
                const status = transactions.terminalCommitStatusForOutcome(committed.propagation_pending, committed.visibility_pending, committed.visibility_retry_pending, committed.visibility_repair_required);
                _ = (server.txn_sessions.recordTerminalCommitWithRepair(alloc, id, status, committed.visibility_repair_required, committed.coordinator_group_id, committed.coordinator_table_name) catch return .{ .outcome = .committed_pending, .reconciliation_id = id }) orelse return .{ .outcome = .committed_pending, .reconciliation_id = id };
                if (status == .committed) {
                    if (committed.coordinator_group_id) |group| {
                        const table = committed.coordinator_table_name orelse return .{ .outcome = .committed_pending, .reconciliation_id = id };
                        _ = (source.acknowledgeTransactionCommit(alloc, id, group, table) catch return .{ .outcome = .committed_pending, .reconciliation_id = id }) orelse return .{ .outcome = .committed_pending, .reconciliation_id = id };
                        _ = (server.txn_sessions.markTerminalCoordinatorAcknowledged(alloc, id) catch return .{ .outcome = .committed_pending, .reconciliation_id = id }) orelse return .{ .outcome = .committed_pending, .reconciliation_id = id };
                    }
                }
                return .{ .outcome = if (committed.visibility_repair_required) .committed_repair_required else if (status == .committed) .committed else .committed_pending, .reconciliation_id = id };
            },
        }
    }
};

pub const Adapter = struct {
    alloc: std.mem.Allocator,
    registry: *transactions.SessionRegistry,
    node_id: u64,
    admitted: ?transactions.Mutation = null,
    commit_context: *anyopaque,
    commit_fn: *const fn (*anyopaque, sql.Scope, transactions.Mutation) anyerror!sql.CommitResult,
    rollback_fn: ?*const fn (*anyopaque, sql.Scope, sql.Id) anyerror!void = null,
    savepoint_fn: ?*const fn (*anyopaque, sql.Scope, sql.Id, []const u8, transactions.SessionRegistry.SqlSavepointAction) anyerror!void = null,
    failed_fn: ?*const fn (*anyopaque, sql.Scope, sql.Id) anyerror!void = null,
    supports_range_guards: bool = false,
    observed_execution_started: bool = false,
    begin_prepared_completion_only: bool = false,
    rollback_for_commit: bool = false,
    failed_commit_completion: @FieldType(operation.RequestContext, "sql_completion") = null,
    connection_id: ?[32]u8 = null,
    connection_revision: ?u64 = null,
    command_fence: ?*const @import("../../../system_catalog/command_fence.zig").Fence = null,
    connection_overlay: []const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry = &.{},
    connection_search_path: ?@import("../../../pgwire/search_path.zig").Path = null,

    fn connectionMutation(self: *Adapter, scope: sql.Scope, action: connections.Action) !void {
        const store = self.registry.durable orelse return error.SqlConnectionUnavailable;
        _ = try connections.mutateWithCommand(store, action, scope.principal, self.node_id, platform_time.realtimeNs() / std.time.ns_per_ms, self.command_fence);
    }

    pub fn owner(self: *Adapter) sql.Owner {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn cast(ptr: *anyopaque) *Adapter {
        return @ptrCast(@alignCast(ptr));
    }

    fn check(self: *Adapter, scope: sql.Scope, id: sql.Id) !transactions.SessionRegistry.SqlState {
        const principal: ?[]const u8 = if (scope.principal.len == 0) null else scope.principal;
        var admission = try self.registry.admitSql(self.alloc, id, principal, self.node_id, self.connection_id, self.admitted);
        errdefer admission.state.deinit(self.alloc);
        self.admitted = admission.guard;
        const state = admission.state;
        if (!std.mem.eql(u8, state.metadata.database, scope.database) or !std.mem.eql(u8, state.metadata.namespace, scope.namespace)) return error.SqlTransactionNotActive;
        return state;
    }

    pub fn setCharacteristics(self: *Adapter, scope: sql.Scope, id: sql.Id, changes: sql.Characteristics) !void {
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if ((changes.isolation orelse state.metadata.isolation) != .read_committed and !self.supports_range_guards) return error.SqlRangeTrackingRequired;
        try self.registry.setSqlCharacteristics(self.alloc, id, changes);
    }

    fn begin(ptr: *anyopaque, scope: sql.Scope, options: sql.Begin) !sql.Transaction {
        const self = cast(ptr);
        // Stronger isolation requires native retained snapshots + range
        // validation, not a label on an ordinary staged transaction.
        if (options.isolation != .read_committed and !self.supports_range_guards) return error.SqlRangeTrackingRequired;
        if (options.deferrable and options.isolation == .serializable and options.mode == .read_only) return error.UnsupportedSqlShape;
        const txn_id = transactions.newSessionTxnId(self.node_id);
        if (self.connection_id) |connection| try self.connectionMutation(scope, .{ .begin = .{
            .id = connection,
            .expected_revision = self.connection_revision orelse return error.SqlConnectionChanged,
            .txn_id = txn_id,
        } });
        const info = self.registry.beginForPrincipalWithSettingsAndId(self.alloc, .{ .prepared_completion_only = self.begin_prepared_completion_only, .sql = .{
            .database = scope.database,
            .namespace = scope.namespace,
            .isolation = options.isolation,
            .mode = options.mode,
            .deferrable = options.deferrable,
            .search_path_active = self.connection_search_path,
            .search_path_committed = self.connection_search_path,
        } }, self.node_id, if (scope.principal.len == 0) null else scope.principal, self.connection_overlay, self.connection_id, txn_id) catch |err| {
            if (self.connection_id) |connection| self.connectionMutation(scope, .{ .abort_begin = .{ .id = connection, .txn_id = txn_id } }) catch {};
            return err;
        };
        if (self.connection_id) |connection| self.connectionMutation(scope, .{ .bind = .{ .id = connection, .txn_id = info.txn_id } }) catch |err| {
            // No transaction ID has escaped to the caller. A proven local
            // rollback permits release; otherwise retain the beginning fence
            // for explicit recovery instead of allowing DISCARD to race it.
            self.registry.rollbackConnectionBeforeExecution(info.txn_id, connection, info.version, .beginning) catch {};
            return err;
        };
        self.admitted = null;
        return .{ .id = info.txn_id, .state = .active, .isolation = options.isolation, .mode = options.mode, .started_at = try clock.Instant.fromUnixMicroseconds(@divFloor(@as(i128, info.begin_timestamp), 1000)), .write_version = try @import("../../distributed_txn.zig").commitVersion(info.begin_timestamp) };
    }

    fn inspect(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !sql.Transaction {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        self.observed_execution_started = state.execution_started;
        return inspected(id, state);
    }

    pub fn inspected(id: sql.Id, state: transactions.SessionRegistry.SqlState) sql.Transaction {
        return .{ .id = id, .isolation = state.metadata.isolation, .mode = state.metadata.mode, .savepoints = state.savepoints, .started_at = clock.Instant.fromUnixMicroseconds(@divFloor(@as(i128, state.begin_timestamp), 1000)) catch unreachable, .write_version = @import("../../distributed_txn.zig").commitVersion(state.begin_timestamp) catch unreachable, .state = if (state.terminal_prepare) .prepared else if (state.terminal_abort) .aborted else if (state.terminal != null) .committed else if (state.execution_started) .uncertain else if (state.metadata.failed) .failed else .active };
    }

    fn commit(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !sql.CommitResult {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (state.terminal_abort) {
            const reason: ?sql.ConstraintFailure = if (state.terminal_abort_reason) |value| switch (value) {
                .unique_constraint_violation => .unique_constraint_violation,
                .foreign_key_parent_missing => .foreign_key_parent_missing,
                .foreign_key_referenced => .foreign_key_referenced,
            } else null;
            return .{ .outcome = .aborted, .reconciliation_id = id, .constraint_failure = reason };
        }
        if (state.execution_started or state.terminal != null) {
            self.observed_execution_started = state.execution_started;
            return error.SqlTransactionOutcomeUnknown;
        }
        if (state.metadata.failed) return error.SqlTransactionAborted;
        const result = try self.commit_fn(self.commit_context, scope, self.admitted.?);
        if (self.connection_id) |connection| {
            const action: connections.Action = if (result.outcome == .unknown)
                .{ .uncertain = .{ .id = connection, .guard = self.admitted.? } }
            else
                .{ .terminal = .{ .id = connection, .txn_id = id } };
            // A committed decision must not turn into an apparent retryable
            // error merely because detachment failed. Retain the fence for
            // recovery; the caller still receives the proven decision.
            if (self.connectionMutation(scope, action)) |_| {
                if (action == .terminal) self.registry.evictDetachedSqlSession(id);
            } else |_| {}
        }
        return result;
    }

    fn rollback(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (self.command_fence != null) {
            const callback = self.rollback_fn orelse return error.SqlTransactionOutcomeUnknown;
            callback(self.commit_context, scope, id) catch |err| {
                if (err == error.SqlTransactionOutcomeUnknown) if (self.connection_id) |connection|
                    self.connectionMutation(scope, .{ .uncertain = .{ .id = connection, .guard = self.admitted.? } }) catch {};
                return err;
            };
            // A known ABORT is not complete at this SQL boundary until
            // its exact connection attachment is durably detached. A lost
            // detach reply retains same-ID reconciliation ownership.
            if (self.connection_id) |connection| try self.connectionMutation(scope, .{ .terminal = .{ .id = connection, .txn_id = id } });
            return;
        }
        if (self.failed_commit_completion) |completion| try completion.prepare_transaction(completion.ptr, id);
        const retirement = if (self.rollback_for_commit)
            self.registry.finishSqlCommitBeforeExecution(self.alloc, self.admitted.?, state.version, false)
        else
            self.registry.retireBeforeExecution(self.alloc, self.admitted.?, state.version, false);
        const retired = retirement catch |err| switch (err) {
            error.SessionWriteOutcomeUnknown => if (self.rollback_for_commit) return error.SqlTransactionOutcomeUnknown else return err,
            else => return err,
        };
        if (!retired)
            return error.SqlTransactionOutcomeUnknown;
    }

    fn savepoint(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (self.savepoint_fn) |callback| return callback(self.commit_context, scope, id, name, .create);
        _ = (try self.registry.createNamedSavepoint(self.alloc, self.admitted.?, name)) orelse return error.SqlTransactionNotActive;
    }

    fn rollbackTo(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (self.savepoint_fn) |callback| return callback(self.commit_context, scope, id, name, .rollback);
        _ = (try self.registry.rollbackToNamedSavepoint(self.alloc, self.admitted.?, name)) orelse return error.SqlSavepointNotFound;
    }

    fn release(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (self.savepoint_fn) |callback| return callback(self.commit_context, scope, id, name, .release);
        _ = (try self.registry.releaseNamedSavepoint(self.alloc, self.admitted.?, name)) orelse return error.SqlSavepointNotFound;
    }

    fn failed(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (self.failed_fn) |callback| return callback(self.commit_context, scope, id);
        try self.registry.setSqlFailed(self.alloc, self.admitted.?, true);
    }

    fn abandon(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) void {
        rollback(ptr, scope, id) catch {};
    }

    const vtable: sql.Owner.VTable = .{ .begin = begin, .inspect = inspect, .commit = commit, .rollback = rollback, .savepoint = savepoint, .rollback_to = rollbackTo, .release = release, .mark_failed = failed, .abandon = abandon };
};

test "SQL native session owns durable savepoint and failure state" {
    const alloc = std.testing.allocator;
    var registry = transactions.SessionRegistry.init(null);
    defer registry.deinit(alloc);
    var ignored: u8 = 0;
    const Commit = struct {
        fn run(_: *anyopaque, _: sql.Scope, guard: transactions.Mutation) !sql.CommitResult {
            return .{ .outcome = .unknown, .reconciliation_id = guard.id };
        }
    };
    var adapter = Adapter{ .alloc = alloc, .registry = &registry, .node_id = 1, .commit_context = &ignored, .commit_fn = Commit.run };
    var session = sql.Session{ .owner = adapter.owner(), .scope = .{ .principal = "alice" } };
    defer session.deinit();
    try std.testing.expectError(error.SqlRangeTrackingRequired, session.execute(.{ .begin = .{} }));
    _ = try session.execute(.{ .begin = .{ .isolation = .read_committed } });
    const started = (try session.statement(false)).started_at.?;
    try std.testing.expectEqual(@as(?i128, @as(i128, registry.getInfo(session.transaction_id.?).?.begin_timestamp / 1000) * 1000), started.unixNanoseconds());
    _ = try session.execute(.{ .savepoint = "one" });
    try session.statementFailed();
    try std.testing.expectEqual(sql.Status.failed, try session.status());
    _ = try session.execute(.{ .rollback_to = "one" });
    try std.testing.expectEqual(sql.Status.in_transaction, try session.status());
    _ = try session.execute(.{ .release = "one" });
    var state = (try registry.getSqlState(alloc, session.transaction_id.?)).?;
    defer state.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), state.savepoints);
    try std.testing.expectEqual(started.micros, (try session.statement(false)).started_at.?.micros);
    var other = sql.Session{ .owner = adapter.owner(), .scope = .{ .principal = "bob" } };
    try std.testing.expectError(error.SqlTransactionNotActive, other.attach(session.transaction_id.?));
    _ = try session.execute(.rollback);
    try std.testing.expectEqual(sql.Status.idle, try session.status());
}
