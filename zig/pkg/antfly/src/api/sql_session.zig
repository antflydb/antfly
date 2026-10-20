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

//! Shared SQL protocol/HTTP control adapter. All state belongs to the durable
//! transaction registry; commit is the native coordinator, never HTTP replay.
const distributed_txn = @import("distributed_txn.zig");
const std = @import("std");
const sql = @import("antfly_local_sources").sql_session;
const transactions = @import("transactions.zig");
const http = @import("http_server.zig");
const operation = @import("antfly_local_sources").api_operation;
const connections = @import("sql_connections.zig");
const platform_time = @import("antfly_platform").time;

pub const Coordinator = struct {
    server: *http.ApiHttpServer,
    identity: *?http.AuthenticatedIdentity,
    context: operation.RequestContext,

    pub fn commit(ptr: *anyopaque, _: sql.Scope, id: sql.Id) !sql.CommitResult {
        const self: *Coordinator = @ptrCast(@alignCast(ptr));
        const server = self.server;
        const alloc = server.controlAllocator();
        try self.context.ensureActive();
        const lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
        defer lease.release();
        const info = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
        var lake_validation: @import("sql_execution.zig").Adapter = .{ .server = server, .identity = self.identity, .context = self.context };
        lake_validation.validateAcceptedSerializable(alloc, id) catch |err| {
            if (err != error.SqlWriteConflict) return err;
            if (!server.txn_sessions.removeBeforeExecution(alloc, id)) return .{ .outcome = .unknown, .reconciliation_id = id };
            return .{ .outcome = .aborted, .reconciliation_id = id };
        };
        var request = (try server.txn_sessions.cloneCommitRequest(alloc, id, null)) orelse {
            if (!server.txn_sessions.commitBeforeExecution(alloc, id)) return .{ .outcome = .unknown, .reconciliation_id = id };
            return .{ .outcome = .committed, .reconciliation_id = id };
        };
        defer request.deinit(alloc);
        if (!(try server.transactionRequestAuthorized(self.identity.*, request))) return error.Forbidden;
        const tables = try request.distributedTables(alloc);
        defer if (tables.len != 0) alloc.free(tables);
        try server.validateCommitTablesAgainstSchema(self.context, tables);
        if (try server.validateCommitReadSet(request) != null) {
            if (!server.txn_sessions.removeBeforeExecution(alloc, id)) return .{ .outcome = .unknown, .reconciliation_id = id };
            return .{ .outcome = .aborted, .reconciliation_id = id };
        }
        if (tables.len == 0) {
            if (!server.txn_sessions.commitBeforeExecution(alloc, id)) return .{ .outcome = .unknown, .reconciliation_id = id };
            return .{ .outcome = .committed, .reconciliation_id = id };
        }
        const source = server.table_writes orelse return error.UnsupportedSqlExecution;
        var prepared = try server.preparePublicCommitWithIntegrity(alloc, tables, self.context);
        defer prepared.deinit();
        var proof_arena = std.heap.ArenaAllocator.init(alloc);
        defer proof_arena.deinit();
        const signed = try server.signCurrentRowPolicyMutationParticipants(proof_arena.allocator(), prepared.tables, self.context);
        // Refuse an unsupported execution capability before sealing durable
        // recovery debt; never silently switch to unaccounted fanout.
        var commit_control = distributed_txn.CommitControl.fromRequest(self.context);
        commit_control.participant_plan_store = server.txn_sessions.participantPlanStore();
        if (!source.supportsTransactionControl(commit_control)) return error.ExecutionWaitUnsupported;
        return self.completePrepared(id, info.begin_timestamp, info.sync_level, signed);
    }

    fn completePrepared(self: *Coordinator, id: sql.Id, begin_timestamp: u64, sync_level: @import("antfly_local_sources").storage_db_types.SyncLevel, signed: []const distributed_txn.TableCommitRequest) !sql.CommitResult {
        const server = self.server;
        if (self.context.execution_team) |team| {
            // This handoff moves one admitted CPU owner to a joined task; it
            // cannot race children that still borrow that owner's execution.
            if (team.state == .active) return error.InvalidRunnableTransition;
            const backend = server.cfg.backend_runtime orelse return error.SqlWriteCapacityUnavailable;
            const io = backend.io() orelse return error.SqlWriteCapacityUnavailable;
            const runtime = @import("antfly_runtime_abi");
            const borrowed_io = runtime.native_abi.IoBorrow.init(&io);
            var completion = PreparedCompletion{
                .coordinator = self,
                .id = id,
                .begin_timestamp = begin_timestamp,
                .sync_level = sync_level,
                .tables = signed,
            };
            // Allocate the task and reserve its finite completion fence before
            // publishing durable recovery debt. Both task admission and fence
            // admission fail with its gate closed, without sealing a plan.
            var task = try @import("runnable_completion_task.zig").Task.reserve(
                team.allocator,
                io,
                team.borrow,
                &.{
                    .io = &borrowed_io,
                    .deadline_ns = @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, io).nanoseconds))) +| 30 * std.time.ns_per_s,
                },
                &completion,
                PreparedCompletion.run,
            );
            defer task.deinit();
            try self.context.ensureActive();
            // The request, signed tables, registry lease, bounded allocator,
            // ingress bridge and execution stay alive through this join.
            try task.runAndJoin();
            return completion.result orelse error.InvalidRunnableTransition;
        }
        return self.commitPrepared(id, begin_timestamp, sync_level, signed);
    }

    const PreparedCompletion = struct {
        coordinator: *Coordinator,
        id: sql.Id,
        begin_timestamp: u64,
        sync_level: @import("antfly_local_sources").storage_db_types.SyncLevel,
        tables: []const distributed_txn.TableCommitRequest,
        result: ?sql.CommitResult = null,

        fn run(raw: *anyopaque, _: @import("antfly_runtime_abi").runnable_abi.Borrowed, _: std.Io) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.result = try self.coordinator.commitPrepared(self.id, self.begin_timestamp, self.sync_level, self.tables);
        }
    };

    fn commitPrepared(self: *Coordinator, id: sql.Id, begin_timestamp: u64, sync_level: @import("antfly_local_sources").storage_db_types.SyncLevel, signed: []const distributed_txn.TableCommitRequest) !sql.CommitResult {
        const server = self.server;
        const alloc = server.controlAllocator();
        const source = server.table_writes orelse return error.UnsupportedSqlExecution;
        // Completion activation does not replace the original proposal fence.
        // Recheck it in the child before sealing, and pass it unchanged to the
        // participant coordinator even after independent completion activates.
        try self.context.ensureActive();
        var commit_control = distributed_txn.CommitControl.fromRequest(self.context);
        commit_control.participant_plan_store = server.txn_sessions.participantPlanStore();
        var plan = (server.txn_sessions.sealExecutionPlan(alloc, id, signed) catch
            // Persistence may have succeeded before a later publication error.
            // Preserve this ID for authoritative reconciliation on every such
            // outcome instead of inviting replay with a fresh transaction.
            return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return error.SqlTransactionNotActive;
        defer plan.deinit();
        // Once the durable plan is sealed, no error is safe to turn into a
        // replay invitation. Maintenance owns completion under this same id.
        const outcome = (source.commitTransactionWithIdAndControl(alloc, id, begin_timestamp, plan.value, sync_level, self.context.cancellation, commit_control) catch |err| {
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
            .conflict => {
                // The storage coordinator has proved a definite abort, but
                // connection detachment is a separate fallible publication.
                // Persist the abort before responding so a lost detach can be
                // reconciled without treating a missing txn as an abort.
                _ = (server.txn_sessions.recordTerminalAbort(alloc, id) catch return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
                return .{ .outcome = .aborted, .reconciliation_id = id };
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
    commit_context: *anyopaque,
    commit_fn: *const fn (*anyopaque, sql.Scope, sql.Id) anyerror!sql.CommitResult,
    supports_range_guards: bool = false,
    connection_id: ?[32]u8 = null,
    connection_revision: ?u64 = null,
    connection_overlay: []const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry = &.{},

    fn connectionMutation(self: *Adapter, scope: sql.Scope, action: connections.Action) !void {
        const store = self.registry.durable orelse return error.SqlConnectionUnavailable;
        _ = try connections.mutate(store, action, scope.principal, self.node_id, platform_time.realtimeNs() / std.time.ns_per_ms);
    }

    pub fn owner(self: *Adapter) sql.Owner {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn cast(ptr: *anyopaque) *Adapter {
        return @ptrCast(@alignCast(ptr));
    }

    fn check(self: *Adapter, scope: sql.Scope, id: sql.Id) !transactions.SessionRegistry.SqlState {
        const principal: ?[]const u8 = if (scope.principal.len == 0) null else scope.principal;
        if (try self.registry.principalAccess(self.alloc, id, principal) != .allowed) return error.SqlTransactionNotActive;
        var state = (try self.registry.getSqlState(self.alloc, id)) orelse return error.SqlTransactionNotActive;
        errdefer state.deinit(self.alloc);
        if (!std.meta.eql(state.connection_id, self.connection_id)) return error.SqlConnectionNotFound;
        if (state.owner_node_id != self.node_id) return error.SessionLeaseLost;
        try self.registry.validateSqlLease(self.alloc, id, self.node_id);
        if (!std.mem.eql(u8, state.metadata.database, scope.database) or !std.mem.eql(u8, state.metadata.namespace, scope.namespace)) return error.SqlTransactionNotActive;
        return state;
    }

    fn begin(ptr: *anyopaque, scope: sql.Scope, options: sql.Begin) !sql.Transaction {
        const self = cast(ptr);
        // Stronger isolation requires native retained snapshots + range
        // validation, not a label on an ordinary staged transaction.
        if (options.isolation != .read_committed and !self.supports_range_guards) return error.UnsupportedSqlExecution;
        const txn_id = transactions.newSessionTxnId(self.node_id);
        if (self.connection_id) |connection| try self.connectionMutation(scope, .{ .begin = .{
            .id = connection,
            .expected_revision = self.connection_revision orelse return error.SqlConnectionChanged,
            .txn_id = txn_id,
        } });
        const info = self.registry.beginForPrincipalWithSettingsAndId(self.alloc, .{ .sql = .{
            .database = scope.database,
            .namespace = scope.namespace,
            .isolation = options.isolation,
            .mode = options.mode,
        } }, self.node_id, if (scope.principal.len == 0) null else scope.principal, self.connection_overlay, self.connection_id, txn_id) catch |err| {
            if (self.connection_id) |connection| self.connectionMutation(scope, .{ .abort_begin = .{ .id = connection, .txn_id = txn_id } }) catch {};
            return err;
        };
        if (self.connection_id) |connection| self.connectionMutation(scope, .{ .bind = .{ .id = connection, .txn_id = info.txn_id } }) catch |err| {
            // No transaction ID has escaped to the caller. A proven local
            // rollback permits release; otherwise retain the beginning fence
            // for explicit recovery instead of allowing DISCARD to race it.
            if (self.registry.removeBeforeExecution(self.alloc, info.txn_id))
                self.connectionMutation(scope, .{ .abort_begin = .{ .id = connection, .txn_id = info.txn_id } }) catch {};
            return err;
        };
        return .{ .id = info.txn_id, .state = .active, .isolation = options.isolation, .mode = options.mode };
    }

    fn inspect(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !sql.Transaction {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        return .{ .id = id, .isolation = state.metadata.isolation, .mode = state.metadata.mode, .savepoints = state.savepoints, .state = if (state.terminal_abort) .aborted else if (state.terminal != null) .committed else if (state.execution_started) .uncertain else if (state.metadata.failed) .failed else .active };
    }

    fn commit(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !sql.CommitResult {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (state.terminal_abort) return .{ .outcome = .aborted, .reconciliation_id = id };
        if (state.execution_started or state.terminal != null) return error.SqlTransactionOutcomeUnknown;
        if (state.metadata.failed) return error.SqlTransactionAborted;
        const result = try self.commit_fn(self.commit_context, scope, id);
        if (self.connection_id) |connection| {
            const action: connections.Action = if (result.outcome == .unknown)
                .{ .uncertain = .{ .id = connection, .txn_id = id } }
            else
                .{ .terminal = .{ .id = connection, .txn_id = id } };
            // A committed decision must not turn into an apparent retryable
            // error merely because detachment failed. Retain the fence for
            // recovery; the caller still receives the proven decision.
            self.connectionMutation(scope, action) catch {};
        }
        return result;
    }

    fn rollback(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (self.connection_id) |connection|
            try self.registry.rollbackConnectionBeforeExecution(self.alloc, id, connection)
        else if (!self.registry.removeBeforeExecution(self.alloc, id))
            return error.SqlTransactionOutcomeUnknown;
    }

    fn savepoint(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        _ = (try self.registry.createNamedSavepoint(self.alloc, id, name)) orelse return error.SqlTransactionNotActive;
    }

    fn rollbackTo(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        _ = (try self.registry.rollbackToNamedSavepoint(self.alloc, id, name)) orelse return error.SqlSavepointNotFound;
    }

    fn release(ptr: *anyopaque, scope: sql.Scope, id: sql.Id, name: []const u8) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        _ = (try self.registry.releaseNamedSavepoint(self.alloc, id, name)) orelse return error.SqlSavepointNotFound;
    }

    fn failed(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        try self.registry.setSqlFailed(self.alloc, id, true);
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
        fn run(_: *anyopaque, _: sql.Scope, id: sql.Id) !sql.CommitResult {
            return .{ .outcome = .unknown, .reconciliation_id = id };
        }
    };
    var adapter = Adapter{ .alloc = alloc, .registry = &registry, .node_id = 1, .commit_context = &ignored, .commit_fn = Commit.run };
    var session = sql.Session{ .owner = adapter.owner(), .scope = .{ .principal = "alice" } };
    defer session.deinit();
    try std.testing.expectError(error.UnsupportedSqlExecution, session.execute(.{ .begin = .{} }));
    _ = try session.execute(.{ .begin = .{ .isolation = .read_committed } });
    _ = try session.execute(.{ .savepoint = "one" });
    try session.statementFailed();
    try std.testing.expectEqual(sql.Status.failed, try session.status());
    _ = try session.execute(.{ .rollback_to = "one" });
    try std.testing.expectEqual(sql.Status.in_transaction, try session.status());
    _ = try session.execute(.{ .release = "one" });
    var state = (try registry.getSqlState(alloc, session.transaction_id.?)).?;
    defer state.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), state.savepoints);
    var other = sql.Session{ .owner = adapter.owner(), .scope = .{ .principal = "bob" } };
    try std.testing.expectError(error.SqlTransactionNotActive, other.attach(session.transaction_id.?));
    _ = try session.execute(.rollback);
    try std.testing.expectEqual(sql.Status.idle, try session.status());
}

test "SQL prepared completion reserves before sealing and preserves original proposal fences" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const types = @import("antfly_local_sources").storage_db_types;
    const background = @import("antfly_local_sources").storage_background_runtime;
    const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const runtime = @import("antfly_runtime_abi");
    const Fake = struct {
        expected: distributed_txn.CommitControl = .{},
        id: sql.Id = undefined,
        registry: *transactions.SessionRegistry = undefined,
        scheduler: *scheduler.Scheduler = undefined,
        signal: *std.atomic.Value(bool),
        cancel_after_acceptance: bool,
        expire_after_acceptance: bool,
        calls: usize = 0,
        var completion_now: std.atomic.Value(u64) = .init(0);
        fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = completion_now.load(.acquire) };
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.BatchRequest) !?void {
            return error.UnexpectedBatch;
        }
        fn commit(raw: *anyopaque, alloc: std.mem.Allocator, id: types.TxnId, _: u64, tables: []const distributed_txn.TableCommitRequest, _: types.SyncLevel, _: types.CancellationToken, control: distributed_txn.CommitControl) !?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var expected = self.expected;
            expected.participant_plan_store = self.registry.participantPlanStore();
            try std.testing.expectEqualDeep(expected, control);
            try std.testing.expectEqualDeep(self.id, id);
            try std.testing.expectEqualStrings("docs", tables[0].table_name);
            var plan = (try self.registry.getExecutionPlan(alloc, id)) orelse return error.ExpectedSealedPlan;
            defer plan.deinit();
            try std.testing.expectEqualStrings("docs", plan.value[0].table_name);
            const borrow = control.team.?.borrow;
            const checked = borrow.checkpoint(borrow.context);
            if (!checked.isOk()) return runtime.error_abi.errorFromStatus(checked);
            self.calls += 1;
            if (self.expire_after_acceptance) {
                completion_now.store(31 * std.time.ns_per_s, .release);
                const expired = borrow.checkpoint(borrow.context);
                if (!expired.isOk()) return runtime.error_abi.errorFromStatus(expired);
                return error.ExpectedCompletionExpiry;
            }
            if (self.cancel_after_acceptance) {
                self.signal.store(true, .release);
                const parked = borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource));
                if (!parked.isOk()) return runtime.error_abi.errorFromStatus(parked);
                try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
                var health: scheduler.Execution = .{};
                try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                health.finish();
                self.scheduler.close();
                const resumed = borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.write));
                if (!resumed.isOk()) return runtime.error_abi.errorFromStatus(resumed);
                const completed = borrow.checkpoint(borrow.context);
                if (!completed.isOk()) return runtime.error_abi.errorFromStatus(completed);
            }
            return .{ .committed = .{ .participant_count = 1 } };
        }
    };
    for (0..7) |scenario| {
        const alloc = std.testing.allocator;
        var threaded = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .limited(if (scenario == 5) 0 else 2) });
        defer threaded.deinit();
        Fake.completion_now.store(0, .release);
        var io_vtable = threaded.io().vtable.*;
        io_vtable.now = Fake.now;
        const io: std.Io = .{ .userdata = threaded.io().userdata, .vtable = &io_vtable };
        var backend = try background.BackendRuntime.init(alloc, .{ .backend = .manual, .borrowed_io = .{ .general = io } });
        defer backend.deinit();
        var memory_backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
        defer memory_backend.close();
        var store = try memory_backend.runtimeStore(alloc, .{ .name = "system/api-transaction-sessions" });
        defer store.deinit();
        var durable = transactions.DurableSessionStore.initRuntime(alloc, &store);
        var signal = std.atomic.Value(bool).init(false);
        var fake = Fake{ .signal = &signal, .cancel_after_acceptance = scenario == 2, .expire_after_acceptance = scenario == 6 };
        var server = http.ApiHttpServer.init(alloc, .{ .backend_runtime = &backend, .session_store = &durable }, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status } }, null, .{ .ptr = &fake, .vtable = &.{ .batch = Fake.batch, .commit_transaction_with_id_with_control = Fake.commit } });
        defer server.deinit();
        const info = try server.txn_sessions.begin(alloc, .{ .sync_level = .write }, 9);
        var request = try transactions.parseCommitRequest(alloc,
            \\{"read_set":[],"tables":{"docs":{"inserts":{"a":{"id":1}}}}}
        );
        defer request.deinit(alloc);
        var staged = (try server.txn_sessions.cloneCommitRequest(alloc, info.txn_id, &request)).?;
        defer staged.deinit(alloc);
        const lease = server.txn_sessions.tryAcquireCommitExecution(info.txn_id) orelse return error.ExpectedCommitLease;
        defer lease.release();
        var scheduling = try scheduler.Scheduler.init(.{ .slots = 3 });
        var execution: scheduler.Execution = .{};
        defer execution.finish();
        const original_deadline = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        try execution.start(&scheduling, .{ .io = std.testing.io, .deadline_ns = original_deadline, .native_now_ns = platform_time.monotonicNs, .cancellation = .{ .ptr = &signal, .requested = Fake.canceled } }, .write);
        var memory: @import("../common/allocation_budget.zig").Shared = .{ .child = alloc, .limit = if (scenario == 0) 0 else 16384 };
        var bridge = try @import("runnable_write_team.zig").Completion.init(&execution, memory.allocator());
        var team = try bridge.team();
        var identity: ?http.AuthenticatedIdentity = null;
        var coordinator = Coordinator{ .server = &server, .identity = &identity, .context = .{ .execution_team = &team, .deadline_ns = original_deadline, .cancellation = .fromAtomic(&signal) } };
        fake.expected = distributed_txn.CommitControl.fromRequest(coordinator.context);
        fake.id = info.txn_id;
        fake.registry = &server.txn_sessions;
        fake.scheduler = &scheduling;
        durable.fail_writes_for_test = scenario == 3;
        if (scenario == 4) signal.store(true, .release);
        const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .writes = &.{.{ .key = "a", .value = "{\"id\":1}" }} }};
        const result = coordinator.completePrepared(info.txn_id, info.begin_timestamp, info.sync_level, &tables);
        if (scenario == 0) {
            try std.testing.expectError(error.OutOfMemory, result);
        } else if (scenario == 4) {
            try std.testing.expectError(error.Canceled, result);
        } else if (scenario == 5) {
            try std.testing.expectError(error.ConcurrencyUnavailable, result);
        } else {
            const terminal = try result;
            try std.testing.expectEqual(if (scenario == 3 or scenario == 6) sql.CommitOutcome.unknown else sql.CommitOutcome.committed, terminal.outcome);
            try std.testing.expectEqualDeep(info.txn_id, terminal.reconciliation_id);
        }
        durable.fail_writes_for_test = false;
        try std.testing.expectEqual(@as(usize, if (scenario == 1 or scenario == 2 or scenario == 6) 1 else 0), fake.calls);
        if (fake.calls == 0) try std.testing.expect((try server.txn_sessions.getExecutionPlan(alloc, info.txn_id)) == null);
        if (scenario == 6) {
            var retained = (try server.txn_sessions.getExecutionPlan(alloc, info.txn_id)) orelse return error.ExpectedRecoveryPlan;
            defer retained.deinit();
            try std.testing.expectEqualStrings("docs", retained.value[0].table_name);
        }
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), scheduling.stats().operations);
    }
}
