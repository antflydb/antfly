// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! SQL transaction control over a durable, principal-scoped native owner.
//! This module never buffers writes or implements isolation. The owner must
//! retain/read-fence snapshots, stage mutations, persist state and reconcile
//! distributed decisions. Every command reloads authoritative owner state.
const std = @import("std");

pub const Id = [16]u8;
pub const Isolation = enum { read_committed, repeatable_read, serializable };
pub const ReadMode = enum { read_write, read_only };
pub const State = enum { active, failed, committing, uncertain, committed, aborted };
pub const Status = enum(u8) { idle = 'I', in_transaction = 'T', failed = 'E' };
pub const Scope = struct { principal: []const u8, database: []const u8 = "default", namespace: []const u8 = "public" };
pub const Begin = struct { isolation: Isolation = .serializable, mode: ReadMode = .read_write };
pub const Transaction = struct { id: Id, state: State, isolation: Isolation, mode: ReadMode, savepoints: usize = 0 };
pub const CommitOutcome = enum { committed, committed_pending, committed_repair_required, aborted, unknown };
pub const CommitResult = struct { outcome: CommitOutcome, reconciliation_id: Id };
pub const Control = union(enum) { begin: Begin, commit, rollback, savepoint: []const u8, rollback_to: []const u8, release: []const u8 };
pub const Result = struct { status: Status, transaction_id: ?Id, commit: ?CommitResult = null };

pub const Owner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Must reject isolation modes whose native read/fence capabilities
        /// are absent. Successful begin is durable before returning the id.
        begin: *const fn (*anyopaque, Scope, Begin) anyerror!Transaction,
        inspect: *const fn (*anyopaque, Scope, Id) anyerror!Transaction,
        /// Errors prove no commit decision was started. Once proposed, return
        /// an explicit outcome, including unknown with its reconciliation id.
        commit: *const fn (*anyopaque, Scope, Id) anyerror!CommitResult,
        rollback: *const fn (*anyopaque, Scope, Id) anyerror!void,
        savepoint: *const fn (*anyopaque, Scope, Id, []const u8) anyerror!void,
        rollback_to: *const fn (*anyopaque, Scope, Id, []const u8) anyerror!void,
        release: *const fn (*anyopaque, Scope, Id, []const u8) anyerror!void,
        mark_failed: *const fn (*anyopaque, Scope, Id) anyerror!void,
        /// Disconnect never commits. Native durable cleanup retains ownership
        /// if a rollback cannot complete or a decision is already in progress.
        abandon: *const fn (*anyopaque, Scope, Id) void,
    };
};

/// One attached client/HTTP transaction handle. The surrounding session owner
/// serializes commands; native owner methods also fence concurrent principals
/// and stale leases. Names and staged data live only in that native owner.
pub const Session = struct {
    owner: Owner,
    scope: Scope,
    transaction_id: ?Id = null,
    max_savepoints: usize = 64,

    pub fn attach(self: *Session, id: Id) !void {
        if (self.transaction_id != null) return error.SqlTransactionAlreadyActive;
        const transaction = try self.owner.vtable.inspect(self.owner.ptr, self.scope, id);
        if (!std.mem.eql(u8, &transaction.id, &id)) return error.InvalidSqlBackendResponse;
        switch (transaction.state) {
            .committed, .aborted => return error.SqlTransactionNotActive,
            else => self.transaction_id = id,
        }
    }

    pub fn status(self: *Session) !Status {
        const id = self.transaction_id orelse return .idle;
        const current = try self.inspect(id);
        return switch (current.state) {
            .active => .in_transaction,
            .failed, .committing, .uncertain => .failed,
            .committed, .aborted => blk: {
                self.transaction_id = null;
                break :blk .idle;
            },
        };
    }

    pub fn statement(self: *Session, writes: bool) !Transaction {
        const current = try self.active(false);
        if (writes and current.mode == .read_only) return error.SqlReadOnlyTransaction;
        return current;
    }

    pub fn statementFailed(self: *Session) !void {
        const id = self.transaction_id orelse return;
        const current = try self.inspect(id);
        if (current.state == .active) try self.owner.vtable.mark_failed(self.owner.ptr, self.scope, id);
    }

    pub fn execute(self: *Session, control: Control) !Result {
        switch (control) {
            .begin => |options| {
                if (self.transaction_id != null) return error.SqlTransactionAlreadyActive;
                const current = try self.owner.vtable.begin(self.owner.ptr, self.scope, options);
                if (current.state != .active or current.isolation != options.isolation or current.mode != options.mode) {
                    self.owner.vtable.abandon(self.owner.ptr, self.scope, current.id);
                    return error.InvalidSqlBackendResponse;
                }
                self.transaction_id = current.id;
            },
            .commit => {
                if (self.transaction_id == null) return .{ .status = .idle, .transaction_id = null };
                const current = try self.active(true);
                if (current.state == .failed) {
                    try self.owner.vtable.rollback(self.owner.ptr, self.scope, current.id);
                    self.transaction_id = null;
                    return .{ .status = .idle, .transaction_id = null, .commit = .{ .outcome = .aborted, .reconciliation_id = current.id } };
                }
                const outcome = try self.owner.vtable.commit(self.owner.ptr, self.scope, current.id);
                // Preserve the only identity with which to reconcile an
                // uncertain commit. COMMIT is never automatically retried.
                if (outcome.outcome != .unknown) self.transaction_id = null;
                return .{ .status = if (outcome.outcome == .unknown) .failed else .idle, .transaction_id = self.transaction_id, .commit = outcome };
            },
            .rollback => {
                if (self.transaction_id == null) return .{ .status = .idle, .transaction_id = null };
                const current = try self.active(true);
                try self.owner.vtable.rollback(self.owner.ptr, self.scope, current.id);
                self.transaction_id = null;
            },
            .savepoint => |name| {
                try checkName(name);
                const current = try self.active(false);
                if (current.savepoints >= self.max_savepoints) return error.SavepointLimitExceeded;
                try self.owner.vtable.savepoint(self.owner.ptr, self.scope, current.id, name);
            },
            .rollback_to => |name| {
                try checkName(name);
                const current = try self.active(true);
                try self.owner.vtable.rollback_to(self.owner.ptr, self.scope, current.id, name);
            },
            .release => |name| {
                try checkName(name);
                const current = try self.active(false);
                try self.owner.vtable.release(self.owner.ptr, self.scope, current.id, name);
            },
        }
        return .{ .status = try self.status(), .transaction_id = self.transaction_id };
    }

    pub fn deinit(self: *Session) void {
        if (self.transaction_id) |id| self.owner.vtable.abandon(self.owner.ptr, self.scope, id);
        self.transaction_id = null;
    }

    fn active(self: *Session, allow_failed: bool) !Transaction {
        const current = try self.inspect(self.transaction_id orelse return error.SqlTransactionNotActive);
        switch (current.state) {
            .active => {},
            .failed => if (!allow_failed) return error.SqlTransactionAborted,
            .committing, .uncertain => return error.SqlTransactionOutcomeUnknown,
            .committed, .aborted => return error.SqlTransactionNotActive,
        }
        return current;
    }

    fn inspect(self: *Session, id: Id) !Transaction {
        const current = try self.owner.vtable.inspect(self.owner.ptr, self.scope, id);
        if (!std.mem.eql(u8, &id, &current.id)) return error.InvalidSqlBackendResponse;
        return current;
    }
};

fn checkName(name: []const u8) !void {
    if (name.len == 0 or name.len > 63) return error.InvalidSavepointName;
}

test "SQL session delegates durable state and never replays uncertain decisions" {
    const Fake = struct {
        state: State = .active,
        commits: usize = 0,
        abandons: usize = 0,
        fn begin(_: *anyopaque, _: Scope, _: Begin) !Transaction {
            return .{ .id = @splat(1), .state = .active, .isolation = .serializable, .mode = .read_write };
        }
        fn inspect(ptr: *anyopaque, scope: Scope, id: Id) !Transaction {
            if (!std.mem.eql(u8, scope.principal, "alice")) return error.Forbidden;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .id = id, .state = self.state, .isolation = .serializable, .mode = .read_write };
        }
        fn commit(ptr: *anyopaque, _: Scope, id: Id) !CommitResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.commits += 1;
            self.state = .uncertain;
            return .{ .outcome = .unknown, .reconciliation_id = id };
        }
        fn rollback(ptr: *anyopaque, _: Scope, _: Id) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.state = .aborted;
        }
        fn savepoint(_: *anyopaque, _: Scope, _: Id, _: []const u8) !void {}
        fn rollbackTo(ptr: *anyopaque, _: Scope, _: Id, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.state = .active;
        }
        fn fail(ptr: *anyopaque, _: Scope, _: Id) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.state = .failed;
        }
        fn abandon(ptr: *anyopaque, _: Scope, _: Id) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.abandons += 1;
        }
    };
    var native: Fake = .{};
    var session = Session{ .owner = .{ .ptr = &native, .vtable = &.{ .begin = Fake.begin, .inspect = Fake.inspect, .commit = Fake.commit, .rollback = Fake.rollback, .savepoint = Fake.savepoint, .rollback_to = Fake.rollbackTo, .release = Fake.savepoint, .mark_failed = Fake.fail, .abandon = Fake.abandon } }, .scope = .{ .principal = "alice" } };
    defer session.deinit();
    try std.testing.expectEqual(Status.idle, (try session.execute(.rollback)).status);
    try std.testing.expectEqual(Status.idle, (try session.execute(.commit)).status);
    try std.testing.expectEqual(Status.in_transaction, (try session.execute(.{ .begin = .{} })).status);
    _ = try session.execute(.{ .savepoint = "before" });
    try session.statementFailed();
    try std.testing.expectError(error.SqlTransactionAborted, session.statement(false));
    _ = try session.execute(.{ .rollback_to = "before" });
    _ = try session.statement(true);
    const outcome = try session.execute(.commit);
    try std.testing.expectEqual(CommitOutcome.unknown, outcome.commit.?.outcome);
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, session.execute(.commit));
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, session.execute(.rollback));
    try std.testing.expectEqual(@as(usize, 1), native.commits);
    session.scope.principal = "mallory";
    try std.testing.expectError(error.Forbidden, session.status());
    session.scope.principal = "alice";
    session.deinit();
    try std.testing.expectEqual(@as(usize, 1), native.abandons);
}
