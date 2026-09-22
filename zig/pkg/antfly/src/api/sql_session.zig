// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared SQL protocol/HTTP control adapter. All state belongs to the durable
//! transaction registry; commit is the native coordinator, never HTTP replay.
const std = @import("std");
const sql = @import("../sql/session.zig");
const transactions = @import("transactions.zig");
const http = @import("http_server.zig");
const operation = @import("operation.zig");

pub const Coordinator = struct {
    server: *http.ApiHttpServer,
    identity: *?http.AuthenticatedIdentity,
    context: operation.RequestContext,

    pub fn commit(ptr: *anyopaque, _: sql.Scope, id: sql.Id) !sql.CommitResult {
        const self: *Coordinator = @ptrCast(@alignCast(ptr));
        const server = self.server;
        const alloc = server.alloc;
        try self.context.ensureActive();
        const lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
        defer lease.release();
        const info = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
        var request = (try server.txn_sessions.cloneCommitRequest(alloc, id, null)) orelse {
            if (!server.txn_sessions.removeBeforeExecution(alloc, id)) return .{ .outcome = .unknown, .reconciliation_id = id };
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
            if (!server.txn_sessions.removeBeforeExecution(alloc, id)) return .{ .outcome = .unknown, .reconciliation_id = id };
            return .{ .outcome = .committed, .reconciliation_id = id };
        }
        const source = server.table_writes orelse return error.UnsupportedSqlExecution;
        var prepared = try server.preparePublicCommitWithIntegrity(alloc, tables, self.context);
        defer prepared.deinit();
        var plan = (try server.txn_sessions.sealExecutionPlan(alloc, id, prepared.tables)) orelse return error.SqlTransactionNotActive;
        defer plan.deinit();
        // Once the durable plan is sealed, no error is safe to turn into a
        // replay invitation. Maintenance owns completion under this same id.
        const outcome = (source.commitTransactionWithIdAndCancellation(alloc, id, info.begin_timestamp, plan.value, info.sync_level, self.context.cancellation) catch |err| {
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
                _ = server.txn_sessions.remove(alloc, id);
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
        const info = try self.registry.beginForPrincipal(self.alloc, .{ .sql = .{
            .database = scope.database,
            .namespace = scope.namespace,
            .isolation = options.isolation,
            .mode = options.mode,
        } }, self.node_id, if (scope.principal.len == 0) null else scope.principal);
        return .{ .id = info.txn_id, .state = .active, .isolation = options.isolation, .mode = options.mode };
    }

    fn inspect(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !sql.Transaction {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        return .{ .id = id, .isolation = state.metadata.isolation, .mode = state.metadata.mode, .savepoints = state.savepoints, .state = if (state.terminal != null) .committed else if (state.execution_started) .uncertain else if (state.metadata.failed) .failed else .active };
    }

    fn commit(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !sql.CommitResult {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (state.execution_started or state.terminal != null) return error.SqlTransactionOutcomeUnknown;
        if (state.metadata.failed) return error.SqlTransactionAborted;
        return self.commit_fn(self.commit_context, scope, id);
    }

    fn rollback(ptr: *anyopaque, scope: sql.Scope, id: sql.Id) !void {
        const self = cast(ptr);
        var state = try self.check(scope, id);
        defer state.deinit(self.alloc);
        if (!self.registry.removeBeforeExecution(self.alloc, id)) return error.SqlTransactionOutcomeUnknown;
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
