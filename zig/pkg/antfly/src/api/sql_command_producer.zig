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

//! Durable source ownership for metadata command scopes. Control delivery may
//! resume after a crash; SQL execution may not. A ready command issues exactly
//! one execution permit through a local native CAS before any SQL is evaluated.
//! This is an outbox, not a transaction decision or an SQL replay log.
const std = @import("std");
const authority = @import("../system_catalog/lock_authority.zig");
const connections = @import("sql_connection_record.zig");
const transactions = @import("transactions.zig");
const Call = @import("../system_catalog/server_call.zig").Call;
const A = std.mem.Allocator;
pub const execution_lease_ms: u64 = 60000;
pub const renew_before_ms: u64 = execution_lease_ms * 2 / 3;
pub const Id = [16]u8;
const prefix = "\x00sql-command-producer-v1/";
const work_prefix = "\x00sql-command-producer-work-v1/";
const max_bytes = 32768;
pub const Phase = enum { registering, idle, beginning, ready, executing, finishing, fencing, closing, retired };

pub const Ticket = struct {
    instance: Id,
    command: authority.scopes.Command,
    body: [32]u8,
    fn validate(self: Ticket) !void {
        try self.command.validate();
        if (zero(self.instance) or zero(self.body)) return error.InvalidSqlConnection;
    }
};
pub const Record = struct {
    revision: u64 = 1,
    registration: authority.scopes.Registration,
    actor: authority.prepared.Actor,
    principal: []const u8,
    instance: Id,
    phase: Phase = .registering,
    next_command: u64 = 1,
    ticket: ?Ticket = null,
    pending: ?authority.Request = null,
    /// SQL metadata is already durable. Resume its exact native control only.
    control_effect: ?authority.scopes.ledger.Effect = null,
    /// No execution permit was taken. Recovery may finish begin/finish only.
    body_skipped: bool = false,
    recovery: bool = false,
    failure: ?authority.outcomes.Rejection = null,

    pub fn validate(self: Record, a: A) !void {
        try self.registration.validate(.{ .home = .metadata, .group = self.registration.group, .incarnation = self.registration.lease.backend.incarnation });
        try self.actor.validate();
        if (self.revision == 0 or self.next_command == 0 or zero(self.instance) or self.principal.len > 4096 or !std.unicode.utf8ValidateSlice(self.principal) or
            !std.mem.eql(u8, &self.registration.lease.principal, &try authority.principalDigest(a, self.actor.session_user))) return error.InvalidSqlConnection;
        if (self.ticket) |ticket| {
            try ticket.validate();
            if (!std.mem.eql(u8, &ticket.instance, &self.instance) or ticket.command.sequence != self.next_command) return error.InvalidSqlConnection;
        }
        switch (self.phase) {
            .registering, .idle, .retired => if (self.ticket != null) return error.InvalidSqlConnection,
            .beginning, .ready, .executing, .finishing => if (self.ticket == null) return error.InvalidSqlConnection,
            .fencing, .closing => {},
        }
        if (self.control_effect) |effect| {
            try effect.validate();
            if (!effect.backend.eql(self.registration.lease.backend) or self.ticket == null or self.pending == null or
                (self.phase != .executing and self.phase != .finishing) or effect.action != .control) return error.InvalidSqlConnection;
            switch (effect.action.control) {
                .rollback => |value| if (value.first_aborted == 0) return error.InvalidSqlConnection,
                .release_savepoint, .unlock_all => {},
                else => return error.InvalidSqlConnection,
            }
        }
        if (self.body_skipped and (self.ticket == null or (self.phase != .beginning and self.phase != .finishing and self.phase != .fencing and self.phase != .closing))) return error.InvalidSqlConnection;
        if (self.failure != null and !self.recovery) return error.InvalidSqlConnection;
        if (self.phase == .retired and (!self.recovery or self.pending != null)) return error.InvalidSqlConnection;
        if ((self.phase == .fencing or self.phase == .closing) and !self.recovery) return error.InvalidSqlConnection;
        if (self.pending) |request| {
            try request.validate();
            if (!request.backend.eql(self.registration.lease.backend) or request.database != self.registration.lease.database or
                request.standalone != self.registration.standalone or request.admission.command_fence != null or
                !std.meta.eql(request.actor.session_user, self.actor.session_user) or
                !std.meta.eql(request.actor.current_role, self.actor.current_role) or
                !std.mem.eql(u8, request.actor.credential, self.actor.credential)) return error.InvalidSqlConnection;
            switch (request.operation) {
                .register => |registration| if (self.phase != .registering or !std.meta.eql(registration, self.registration)) return error.InvalidSqlConnection,
                .begin => |command| if (self.phase != .beginning or !std.meta.eql(command, self.ticket.?.command)) return error.InvalidSqlConnection,
                .finish => |command| if (self.phase != .finishing or !std.meta.eql(command, self.ticket.?.command)) return error.InvalidSqlConnection,
                .effect => |input| if (self.control_effect == null or !std.meta.eql(input.effect, self.control_effect.?) or !std.meta.eql(input.command, self.ticket.?.command)) return error.InvalidSqlConnection,
                .cancel, .step => |input| {
                    if (!std.meta.eql(input.command, self.ticket.?.command)) return error.InvalidSqlConnection;
                    if (self.phase != .finishing and (self.phase != .executing or self.control_effect == null or !std.meta.eql(input.effect, self.control_effect.?))) return error.InvalidSqlConnection;
                },
                .fence => if (self.phase != .fencing) return error.InvalidSqlConnection,
                .renew => |renewal| if ((self.phase != .idle and self.phase != .ready and self.phase != .executing and self.phase != .finishing) or renewal.expected_expiry != self.registration.lease.expires_at_ms or renewal.expires_at_ms <= renewal.expected_expiry) return error.InvalidSqlConnection,
                .release => |transaction| {
                    if (self.phase != .closing and self.phase != .finishing) return error.InvalidSqlConnection;
                    // A normal completion can only deliver an independently
                    // sealed decision, never abandon a live transaction.
                    if (self.phase == .finishing and transaction.decision == null) return error.InvalidSqlConnection;
                },
                .close_step => if (self.phase != .closing) return error.InvalidSqlConnection,
                else => return error.InvalidSqlConnection,
            }
        } else switch (self.phase) {
            .registering, .beginning, .finishing, .fencing, .closing => return error.InvalidSqlConnection,
            else => {},
        }
    }
    fn controlRequest(self: Record, head: u64, nonce: Id, operation: @FieldType(authority.Request, "operation")) authority.Request {
        return .{ .admission = .{ .head = head, .nonce = nonce }, .incarnation = self.registration.lease.backend.incarnation, .standalone = self.registration.standalone, .database = self.registration.lease.database, .actor = self.actor, .backend = self.registration.lease.backend, .operation = operation };
    }
    fn fence(self: Record) authority.scopes.CommandFence {
        return .{ .group = self.registration.group, .backend = self.registration.lease.backend, .command = self.ticket.?.command, .database = self.registration.lease.database, .principal = self.registration.lease.principal, .standalone = self.registration.standalone };
    }
};
fn zero(bytes: anytype) bool {
    return std.mem.allEqual(u8, &bytes, 0);
}
fn key(comptime start: []const u8, id: [32]u8) [start.len + 32]u8 {
    var result: [start.len + 32]u8 = undefined;
    @memcpy(result[0..start.len], start);
    @memcpy(result[start.len..], &id);
    return result;
}
/// Read-only native observation captured by the executing SQL owner.
/// The existing source writer publishes it atomically with SQL metadata.
pub const ControlPlan = struct {
    prior: Record,
    head: u64,
    next_effect: u64,
    nonce: Id,
    now_ms: u64,
    transaction: ?Id,
};
pub fn observeControlPlan(a: A, prior: Record, instance: Id, transaction: ?Id, now_ms: u64, nonce: Id, port: Port) !ControlPlan {
    try prior.validate(a);
    if (prior.phase != .executing or prior.pending != null or prior.recovery or prior.control_effect != null or
        prior.registration.lease.expires_at_ms <= now_ms or !std.mem.eql(u8, &prior.instance, &instance)) return error.SqlConnectionBusy;
    var observed = try port.snapshot(a, prior);
    defer observed.deinit();
    const scope = observed.value.scope orelse return error.SqlConnectionNotFound;
    const owner = observed.value.owner orelse return error.InvalidSqlBackendResponse;
    if (scope.state != .active or scope.releasing or scope.registration.lease.expires_at_ms <= now_ms or
        !std.meta.eql(scope.command, @as(?authority.scopes.Command, prior.ticket.?.command)) or owner.state != .active or owner.waiting != null or owner.job != null) return error.SqlConnectionBusy;
    if (scope.transaction) |bound| if (bound.decision != null or transaction == null or !std.mem.eql(u8, &bound.id, &transaction.?)) return error.InvalidLockScope;
    if (!std.meta.eql(owner.transaction, if (scope.transaction) |bound| @as(?Id, bound.id) else null)) return error.InvalidLockScope;
    return .{ .prior = prior, .head = observed.value.head, .next_effect = owner.next_effect, .nonce = nonce, .now_ms = now_ms, .transaction = if (scope.transaction) |bound| bound.id else null };
}

pub fn stageSavepointTxn(a: A, txn: anytype, plan: ControlPlan, connection: [32]u8, transaction: Id, control: ?authority.scopes.ledger.Control) !void {
    try stageControlTxn(a, txn, plan, connection, .{ .transaction = transaction }, control);
}

/// DISCARD changes connection metadata without retiring this backend. Retain
/// unlock-all in the same source transaction; completion/recovery owns delivery.
pub fn stageDiscardTxn(a: A, txn: anytype, plan: ControlPlan, connection: [32]u8) !void {
    try stageControlTxn(a, txn, plan, connection, .idle, .unlock_all);
}

fn stageControlTxn(a: A, txn: anytype, plan: ControlPlan, connection: [32]u8, scope: union(enum) { transaction: Id, idle }, control: ?authority.scopes.ledger.Control) !void {
    try plan.prior.validate(a);
    const prior = plan.prior;
    if (prior.phase != .executing or prior.pending != null or prior.recovery or prior.control_effect != null or
        !std.mem.eql(u8, &prior.registration.connection, &connection) or prior.registration.lease.expires_at_ms <= plan.now_ms or zero(plan.nonce)) return error.SqlConnectionBusy;
    if (plan.transaction) |bound| {
        if (scope != .transaction) return error.ActiveSqlTransaction;
        if (!std.mem.eql(u8, &bound, &scope.transaction)) return error.InvalidLockScope;
    }
    var live = (try readTxn(a, txn, connection)) orelse return error.SqlConnectionNotFound;
    defer live.deinit();
    if (live.value.revision != prior.revision or !std.mem.eql(u8, &try digest(a, live.value), &try digest(a, prior))) return error.SqlConnectionChanged;
    var attached = try connections.loadTxn(txn, a, &connection);
    defer attached.deinit();
    try attached.value.requireOwner(&connection, prior.principal, prior.registration.lease.node, 0);
    switch (scope) {
        .transaction => |transaction| if (attached.value.state != .active or !std.meta.eql(attached.value.active_txn, @as(?Id, transaction))) return error.SqlTransactionNotActive,
        .idle => if (attached.value.state != .idle) return error.ActiveSqlTransaction,
    }
    if (control) |action| {
        switch (action) {
            .rollback => |value| if (scope != .transaction or value.first_aborted == 0 or !std.mem.eql(u8, &value.transaction, &scope.transaction)) return error.InvalidLockScope,
            .release_savepoint => |value| if (scope != .transaction or !std.mem.eql(u8, &value.transaction, &scope.transaction)) return error.InvalidLockScope,
            .unlock_all => if (scope != .idle) return error.InvalidLockScope,
            else => return error.InvalidLockScope,
        }
        // A SQL transaction without native binding has no transaction grants.
        // Session unlock-all still needs a native effect in an idle connection.
        if (scope == .transaction and plan.transaction == null) return;
        const effect: authority.scopes.ledger.Effect = .{ .backend = prior.registration.lease.backend, .sequence = plan.next_effect, .nonce = plan.nonce, .action = .{ .control = action } };
        try effect.validate();
        var next = prior;
        next.control_effect = effect;
        next.pending = next.controlRequest(plan.head, plan.nonce, .{ .effect = .{ .command = prior.ticket.?.command, .effect = effect } });
        next.revision = std.math.add(u64, prior.revision, 1) catch return error.SqlProgramLimitExceeded;
        try putTxn(a, txn, next);
    }
}

pub fn readTxn(a: A, txn: anytype, id: [32]u8) !?std.json.Parsed(Record) {
    const raw = txn.get(&key(prefix, id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len > max_bytes) return error.InvalidSqlConnection;
    var parsed = try std.json.parseFromSlice(Record, a, raw, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate(a);
    if (!std.mem.eql(u8, &parsed.value.registration.connection, &id)) return error.InvalidSqlConnection;
    return parsed;
}
/// Retirement must consult native ownership, even when the HTTP idle TTL has
/// expired. The native scope/binding remains the authoritative replay fence.
pub fn retirementReadyTxn(a: A, txn: anytype, id: [32]u8) !bool {
    var found = (try readTxn(a, txn, id)) orelse return true;
    defer found.deinit();
    return found.value.phase == .retired;
}
/// Connection reconciliation has independent begin/terminal proof. It may
/// publish only while this source has no executing SQL body. Finishing owns
/// control delivery only and cannot mint another body execution permit.
pub fn reconciliationReadyTxn(a: A, txn: anytype, id: [32]u8) !bool {
    var found = (try readTxn(a, txn, id)) orelse return true;
    defer found.deinit();
    if (found.value.phase == .finishing) return true;
    return (found.value.phase == .idle or found.value.phase == .retired) and found.value.pending == null and found.value.ticket == null;
}
pub fn requireUnusedTxn(a: A, txn: anytype, id: [32]u8) !void {
    var found = (try readTxn(a, txn, id)) orelse return;
    defer found.deinit();
    return error.SqlConnectionAlreadyExists;
}
/// Once native retirement is proven, reclaim the source outbox in the same
/// transaction that removes its connection. Native scope/binding fences remain;
/// no auxiliary per-connection tombstone must accumulate in the source store.
pub fn forgetRetiredTxn(a: A, txn: anytype, id: [32]u8) !void {
    var found = (try readTxn(a, txn, id)) orelse return;
    defer found.deinit();
    if (found.value.phase != .retired) return error.SqlConnectionBusy;
    try eraseTxn(txn, id);
}
fn eraseTxn(txn: anytype, id: [32]u8) !void {
    txn.delete(&key(work_prefix, id)) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try txn.delete(&key(prefix, id));
}
pub fn requireCommandTxn(a: A, txn: anytype, id: [32]u8, fence: ?*const authority.scopes.CommandFence, now_ms: u64) !void {
    var owned = (try readTxn(a, txn, id)) orelse {
        if (fence != null) return error.InvalidSqlConnection;
        return;
    };
    defer owned.deinit();
    const value = owned.value;
    if (value.phase == .retired) return error.SqlConnectionNotFound;
    if (value.phase != .executing or value.recovery or value.control_effect != null or value.registration.lease.expires_at_ms <= now_ms) return error.SqlConnectionBusy;
    const supplied = (fence orelse return error.SqlConnectionBusy).*;
    try supplied.validate();
    if (!std.meta.eql(value.fence(), supplied)) return error.SqlConnectionBusy;
}
fn putTxn(a: A, txn: anytype, value: Record) !void {
    try value.validate(a);
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > max_bytes) return error.SqlProgramLimitExceeded;
    const id = value.registration.connection;
    try txn.put(&key(prefix, id), bytes);
    if (value.phase == .retired) {
        txn.delete(&key(work_prefix, id)) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    } else try txn.put(&key(work_prefix, id), "");
}
pub fn load(store: *transactions.DurableSessionStore, a: A, id: [32]u8) !?std.json.Parsed(Record) {
    return loadMode(store, a, id, false);
}
fn readIndexedTxn(a: A, txn: anytype, id: [32]u8) !?std.json.Parsed(Record) {
    var found = try readTxn(a, txn, id);
    errdefer if (found) |*value| value.deinit();
    const indexed = if (txn.get(&key(work_prefix, id))) |bytes| blk: {
        if (bytes.len != 0) return error.InvalidSqlConnection;
        break :blk true;
    } else |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
    if (indexed != (found != null and found.?.value.phase != .retired)) return error.InvalidSqlConnection;
    return found;
}
fn uncertainConnectionTxn(store: *transactions.DurableSessionStore, a: A, txn: anytype, id: [32]u8) !bool {
    var found = connections.loadTxn(txn, a, &id) catch |err| switch (err) {
        error.SqlConnectionNotFound => return false,
        else => return err,
    };
    defer found.deinit();
    // An interrupted BEGIN can precede session/header publication. Finishing
    // has no live body, so its normal connection reconciliation owns aborting
    // that exact beginning attachment before admission can proceed.
    if (found.value.state == .uncertain or found.value.state == .beginning) return true;
    const transaction = found.value.active_txn orelse return false;
    // Sealing and native delivery precede the best-effort connection marker.
    // A failed marker write cannot let FINISH invalidate a still-arriving
    // admission. The compact fact and connection share this native read cut.
    return store.connectionExecutionPendingTxn(txn, transaction, id, found.value.principal, found.value.owner_node_id);
}
fn uncertainConnection(store: *transactions.DurableSessionStore, a: A, id: [32]u8) !bool {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try uncertainConnectionTxn(store, a, &txn, id);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try uncertainConnectionTxn(store, a, &txn, id);
        },
    };
}
fn loadMode(store: *transactions.DurableSessionStore, a: A, id: [32]u8, comptime indexed: bool) !?std.json.Parsed(Record) {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk if (indexed) try readIndexedTxn(a, &txn, id) else try readTxn(a, &txn, id);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk if (indexed) try readIndexedTxn(a, &txn, id) else try readTxn(a, &txn, id);
        },
    };
}
const Change = union(enum) {
    create: struct { value: Record, now_ms: u64 },
    replace: struct { revision: u64, prior_digest: [32]u8, next: Record },
    begin_observed: struct { revision: u64, prior_digest: [32]u8, next: Record },
    take: struct { revision: u64, prior_digest: [32]u8, ticket: Ticket },
};
fn change(store: *transactions.DurableSessionStore, id: [32]u8, principal: []const u8, node: u64, action: Change) !void {
    if (!store.supportsSharedMutations()) return error.SqlConnectionUnavailable;
    const mutex = store.mutationMutex();
    @import("antfly_platform").sync.lockYielding(mutex);
    defer mutex.unlock();
    if (store.fail_writes_for_test) return error.InjectedSessionStoreFailure;
    switch (store.backend) {
        .docstore => |backend| {
            var txn = try backend.beginWriteTxn();
            errdefer txn.abort();
            try changeTxn(store.alloc, &txn, id, principal, node, action);
            try txn.commit();
        },
        .runtime => |backend| {
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            try changeTxn(store.alloc, &txn, id, principal, node, action);
            try txn.commit();
        },
    }
}
fn changeTxn(a: A, txn: anytype, id: [32]u8, principal: []const u8, node: u64, action: Change) !void {
    var connection = connections.loadTxn(txn, a, &id) catch |err| switch (err) {
        error.SqlConnectionNotFound => null,
        else => return err,
    };
    defer if (connection) |*value| value.deinit();
    // Internal recovery may pass the idle TTL. This never grants public use.
    if (connection) |value| try value.value.requireOwner(&id, principal, node, 0);
    var previous = try readTxn(a, txn, id);
    defer if (previous) |*value| value.deinit();
    if (previous) |value| if (value.value.registration.lease.node != node or !std.mem.eql(u8, value.value.principal, principal)) return error.SqlConnectionNotFound;
    const next = switch (action) {
        .create => |input| blk: {
            const value = input.value;
            const live = (connection orelse return error.SqlConnectionNotFound).value;
            try live.requireOwner(&id, principal, node, input.now_ms);
            if (value.registration.lease.expires_at_ms <= input.now_ms) return error.SqlConnectionNotFound;
            if (previous != null) return error.SqlConnectionAlreadyExists;
            if (live.state != .idle or value.registration.lease.node != node or
                !std.mem.eql(u8, &value.registration.connection, &id) or value.phase != .registering or value.revision != 1) return error.InvalidSqlConnection;
            break :blk value;
        },
        .replace => |update| blk: {
            const old = if (previous) |value| value.value else return error.SqlConnectionNotFound;
            if (old.revision != update.revision or !std.mem.eql(u8, &try digest(a, old), &update.prior_digest)) return error.SqlConnectionChanged;
            if (connection == null and !update.next.recovery) return error.SqlConnectionNotFound;
            if (update.next.phase == .beginning) {
                const live = (connection orelse return error.SqlConnectionNotFound).value;
                if (live.state == .beginning or live.state == .uncertain) return error.SqlConnectionBusy;
            }
            // The metadata owner and source boot identity never change in place.
            if (!sameRegistration(old.registration, update.next.registration) or update.next.registration.lease.expires_at_ms < old.registration.lease.expires_at_ms or !std.mem.eql(u8, &old.instance, &update.next.instance) or
                update.next.revision != old.revision + 1) return error.InvalidSqlConnection;
            break :blk update.next;
        },
        .begin_observed => |update| blk: {
            const live = (connection orelse return error.SqlConnectionNotFound).value;
            const old = if (previous) |value| value.value else return error.SqlConnectionNotFound;
            if (old.revision != update.revision or !std.mem.eql(u8, &try digest(a, old), &update.prior_digest)) return error.SqlConnectionChanged;
            if (old.phase != .idle or old.pending != null or old.recovery or live.state == .beginning or live.state == .uncertain) return error.SqlConnectionBusy;
            if (update.next.phase != .beginning or !std.meta.eql(old.registration, update.next.registration) or
                old.next_command != update.next.next_command or update.next.revision != old.revision + 1) return error.InvalidSqlConnection;
            break :blk update.next;
        },
        .take => |claim| blk: {
            if (connection == null) return error.SqlConnectionNotFound;
            var old = if (previous) |value| value.value else return error.SqlConnectionNotFound;
            if (old.revision != claim.revision or !std.mem.eql(u8, &try digest(a, old), &claim.prior_digest) or old.pending != null or old.phase != .ready or old.recovery or old.body_skipped or !std.meta.eql(old.ticket, @as(?Ticket, claim.ticket))) return error.SqlConnectionBusy;
            old.phase = .executing;
            old.revision = std.math.add(u64, old.revision, 1) catch return error.SqlProgramLimitExceeded;
            break :blk old;
        },
    };
    if (!std.mem.eql(u8, next.principal, principal) or next.registration.lease.node != node) return error.InvalidSqlConnection;
    if (connection == null and next.phase == .retired) try eraseTxn(txn, id) else try putTxn(a, txn, next);
}
fn digest(a: A, record: Record) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(bytes);
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn replace(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record, next_value: Record) !void {
    var next = next_value;
    next.revision = std.math.add(u64, prior.revision, 1) catch return error.SqlProgramLimitExceeded;
    try change(store, prior.registration.connection, principal, prior.registration.lease.node, .{ .replace = .{ .revision = prior.revision, .prior_digest = try digest(store.alloc, prior), .next = next } });
}
pub fn create(store: *transactions.DurableSessionStore, principal: []const u8, registration: authority.scopes.Registration, actor: authority.prepared.Actor, instance: Id, head: u64, nonce: Id, now_ms: u64) !void {
    var value: Record = .{ .registration = registration, .actor = actor, .principal = principal, .instance = instance };
    value.pending = value.controlRequest(head, nonce, .{ .register = registration });
    try change(store, registration.connection, principal, registration.lease.node, .{ .create = .{ .value = value, .now_ms = now_ms } });
}
pub fn begin(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record, instance: Id, command: authority.scopes.Command, body: [32]u8, head: u64, nonce: Id, now_ms: u64) !Ticket {
    try prior.validate(store.alloc);
    if (prior.pending != null or prior.phase != .idle or prior.recovery or prior.registration.lease.expires_at_ms <= now_ms or command.sequence != prior.next_command) return error.SqlConnectionBusy;
    const ticket: Ticket = .{ .instance = instance, .command = command, .body = body };
    try ticket.validate();
    var next = prior;
    next.ticket = ticket;
    next.phase = .beginning;
    next.pending = next.controlRequest(head, nonce, .{ .begin = command });
    try replace(store, principal, prior, next);
    return ticket;
}
/// Observe the native idle scope and stage the next command in one local CAS.
/// This permits HTTP facade handoff without a separate adoption WAL commit.
/// An active/ready command is never reassigned, and the backend remains fixed.
pub fn beginObserved(store: *transactions.DurableSessionStore, a: A, principal: []const u8, prior: Record, instance: Id, command: authority.scopes.Command, body: [32]u8, nonce: Id, now_ms: u64, port: Port) !Ticket {
    try prior.validate(a);
    if (!store.supportsSharedMutations()) return error.SqlConnectionUnavailable;
    if (prior.phase != .idle or prior.pending != null or prior.recovery or prior.registration.lease.expires_at_ms <= now_ms or command.sequence != prior.next_command) return error.SqlConnectionBusy;
    const ticket: Ticket = .{ .instance = instance, .command = command, .body = body };
    try ticket.validate();
    var observed = try port.snapshot(a, prior);
    defer observed.deinit();
    const scope = observed.value.scope orelse return error.SqlConnectionBusy;
    const owner = observed.value.owner orelse return error.InvalidSqlBackendResponse;
    if (scope.state != .active or scope.command != null or scope.releasing or scope.next_command != prior.next_command or
        owner.state != .active or owner.waiting != null or owner.job != null) return error.SqlConnectionBusy;
    if (scope.transaction) |transaction| if (transaction.decision != null) return error.SqlConnectionBusy;
    var next = prior;
    next.instance = instance;
    next.ticket = ticket;
    next.phase = .beginning;
    next.pending = next.controlRequest(observed.value.head, nonce, .{ .begin = command });
    next.revision = std.math.add(u64, prior.revision, 1) catch return error.SqlProgramLimitExceeded;
    try change(store, prior.registration.connection, principal, prior.registration.lease.node, .{ .begin_observed = .{ .revision = prior.revision, .prior_digest = try digest(a, prior), .next = next } });
    return ticket;
}
/// Persist execution-start before returning authority. Neither retrying this
/// call nor reopening the source store can issue a second permit.
pub fn take(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record, instance: Id, ticket: Ticket, now_ms: u64) !authority.scopes.CommandFence {
    try prior.validate(store.alloc);
    try ticket.validate();
    if (!std.mem.eql(u8, &instance, &ticket.instance) or prior.registration.lease.expires_at_ms <= now_ms) return error.SqlConnectionBusy;
    try change(store, prior.registration.connection, principal, prior.registration.lease.node, .{ .take = .{ .revision = prior.revision, .prior_digest = try digest(store.alloc, prior), .ticket = ticket } });
    return prior.fence();
}
/// Cancellation before take cannot justify dropping an existing session's
/// locks. Persist a body-free begin/finish handoff instead of retiring it.
pub fn skipBeforeTake(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record, instance: Id, ticket: Ticket, nonce: Id) !void {
    try prior.validate(store.alloc);
    if (!std.mem.eql(u8, &instance, &ticket.instance) or prior.recovery or !std.meta.eql(prior.ticket, @as(?Ticket, ticket)) or
        (prior.phase != .beginning and prior.phase != .ready)) return error.SqlConnectionBusy;
    var next = prior;
    next.body_skipped = true;
    if (prior.phase == .ready) {
        next.phase = .finishing;
        next.pending = if (prior.pending != null and prior.pending.?.operation == .renew) prior.pending else next.controlRequest(0, nonce, .{ .finish = ticket.command });
    }
    try replace(store, principal, prior, next);
}

pub fn finish(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record, instance: Id, ticket: Ticket, head: u64, nonce: Id) !void {
    if (store.fail_command_finish_for_test.load(.acquire)) return error.InjectedSessionStoreFailure;
    try prior.validate(store.alloc);
    if (!std.mem.eql(u8, &instance, &ticket.instance) or (prior.pending != null and prior.control_effect == null and prior.pending.?.operation != .renew) or prior.phase != .executing or prior.recovery or !std.meta.eql(prior.ticket, @as(?Ticket, ticket))) return error.SqlConnectionBusy;
    var next = prior;
    next.phase = .finishing;
    // A renewal may already have escaped. Preserve its exact expiry proof
    // across cancellation; the worker drains it before the saved control.
    next.pending = if (prior.pending != null and prior.pending.?.operation == .renew) prior.pending else next.controlRequest(head, nonce, .{ .finish = ticket.command });
    try replace(store, principal, prior, next);
}
pub fn renew(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record, instance: Id, expires_at_ms: u64, head: u64, nonce: Id, now_ms: u64) !void {
    try prior.validate(store.alloc);
    // Replacing a control admission never replaces its immutable effect.
    // Its native receipt/job remains the authority after renewal completes.
    const replaceable = if (prior.pending) |pending| switch (pending.operation) {
        .effect => prior.control_effect != null,
        .step => prior.control_effect != null or prior.phase == .finishing,
        .finish, .cancel => prior.phase == .finishing,
        else => false,
    } else true;
    if (!replaceable or prior.recovery or (prior.phase != .idle and prior.phase != .ready and prior.phase != .executing and prior.phase != .finishing) or
        !std.mem.eql(u8, &prior.instance, &instance) or now_ms >= prior.registration.lease.expires_at_ms or expires_at_ms <= prior.registration.lease.expires_at_ms) return error.SqlConnectionBusy;
    var next = prior;
    next.pending = next.controlRequest(head, nonce, .{ .renew = .{ .expected_expiry = prior.registration.lease.expires_at_ms, .expires_at_ms = expires_at_ms, .observed_at_ms = now_ms } });
    try replace(store, principal, prior, next);
}

/// Local liveness is deliberately separate from durable native authority.
/// An admitted request without its exact taken command is not a heartbeat.
pub const Liveness = union(enum) {
    idle,
    command: authority.scopes.Command,
    control,

    fn permits(self: Liveness, prior: Record) bool {
        return switch (self) {
            .idle => prior.phase == .idle,
            .command => |command| prior.phase == .executing and prior.ticket != null and std.meta.eql(command, prior.ticket.?.command),
            .control => prior.phase == .finishing,
        };
    }
};

/// One observation and at most one delivery. Stage the exact renewal before
/// sending it; a lost reply or crash is resumed by advance, never by SQL replay.
/// Deliver in this quantum so a full directory sweep need not elapse between
/// staging a renewal and extending the native lease.
pub fn maintain(store: *transactions.DurableSessionStore, a: A, prior: Record, instance: Id, now_ms: u64, nonce: Id, port: Port, liveness: ?Liveness) !void {
    const eligible = liveness != null and liveness.?.permits(prior) and !prior.recovery and
        std.mem.eql(u8, &prior.instance, &instance) and prior.registration.lease.expires_at_ms > now_ms;
    const due = prior.registration.lease.expires_at_ms -| now_ms <= renew_before_ms;
    if (eligible and due and (prior.pending == null or prior.pending.?.operation != .renew)) {
        // A source CAS cannot revive an expired or fenced native owner. Observe
        // its incarnation and registration before retaining a new admission.
        var observed = try port.snapshot(a, prior);
        defer observed.deinit();
        const scope = observed.value.scope orelse return error.SqlConnectionNotFound;
        if (scope.state != .active or scope.registration.lease.expires_at_ms <= now_ms) {
            // Native fencing is authoritative even while the local expiry is
            // still in the future. Stop local SQL immediately; the next
            // quantum drains native retirement instead of retrying renewal.
            try requestRecovery(store, prior.principal, prior);
            return;
        }
        const expires_at_ms = now_ms +| execution_lease_ms;
        try renew(store, prior.principal, prior, instance, expires_at_ms, observed.value.head, nonce, now_ms);
        const pending = prior.controlRequest(observed.value.head, nonce, .{ .renew = .{
            .expected_expiry = prior.registration.lease.expires_at_ms,
            .expires_at_ms = expires_at_ms,
            .observed_at_ms = now_ms,
        } });
        if (try port.deliver(a, pending) == null) try acknowledgeRenewal(store, a, prior.principal, prior.registration.connection, pending);
        return;
    }
    try advance(store, a, prior.principal, prior, instance, now_ms, nonce, port);
}

/// An exact successful native receipt already proves this renewal. Publish
/// that proof locally without another native snapshot. Control/FINISH owners
/// retain their outbox and use the existing observation path to resume it.
fn acknowledgeRenewal(store: *transactions.DurableSessionStore, a: A, principal: []const u8, connection: [32]u8, request: authority.Request) !void {
    var owned = (try load(store, a, connection)) orelse return;
    defer owned.deinit();
    const prior = owned.value;
    if (prior.recovery or prior.control_effect != null or (prior.phase != .idle and prior.phase != .executing) or prior.pending == null or prior.pending.?.operation != .renew) return;
    if (!std.mem.eql(u8, principal, prior.principal) or !std.mem.eql(u8, &try authority.commandDigest(a, prior.pending.?), &try authority.commandDigest(a, request))) return;
    if (prior.registration.lease.expires_at_ms != request.operation.renew.expected_expiry) return error.InvalidSqlBackendResponse;
    var next = prior;
    next.registration.lease.expires_at_ms = request.operation.renew.expires_at_ms;
    next.pending = null;
    try replace(store, principal, prior, next);
}

fn sameRegistration(left: authority.scopes.Registration, right: authority.scopes.Registration) bool {
    var normalized = left;
    normalized.lease.expires_at_ms = right.lease.expires_at_ms;
    return std.meta.eql(normalized, right);
}

pub fn requestRecovery(store: *transactions.DurableSessionStore, principal: []const u8, prior: Record) !void {
    if (store.fail_command_recovery_once_for_test.swap(false, .acq_rel)) return error.InjectedSessionStoreFailure;
    try prior.validate(store.alloc);
    if (prior.recovery or prior.phase == .retired) return;
    var next = prior;
    next.recovery = true;
    try replace(store, principal, prior, next);
}

pub const Port = struct {
    ptr: *anyopaque,
    call: *const fn (*anyopaque, A, Call) anyerror![]u8,
    fn snapshot(self: Port, a: A, value: Record) !std.json.Parsed(authority.Snapshot) {
        const bytes = try self.call(self.ptr, a, .{ .sql_lock_read = .{ .incarnation = value.registration.lease.backend.incarnation, .standalone = value.registration.standalone, .actor = value.actor, .database = value.registration.lease.database, .backend = value.registration.lease.backend, .effect = value.control_effect } });
        defer a.free(bytes);
        if (bytes.len > max_bytes) return error.InvalidSqlBackendResponse;
        var parsed = try std.json.parseFromSlice(authority.Snapshot, a, bytes, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        if (!std.mem.eql(u8, &parsed.value.incarnation, &value.registration.lease.backend.incarnation) or parsed.value.standalone != value.registration.standalone or parsed.value.group != value.registration.group) return error.CatalogGenerationChanged;
        if (parsed.value.scope) |scope| {
            try scope.validate(.{ .home = .metadata, .group = value.registration.group, .incarnation = parsed.value.incarnation });
            if (!sameRegistration(scope.registration, value.registration)) return error.InvalidSqlBackendResponse;
            if (scope.registration.lease.expires_at_ms < value.registration.lease.expires_at_ms) return error.InvalidSqlBackendResponse;
            if (scope.registration.lease.expires_at_ms != value.registration.lease.expires_at_ms and !value.recovery) {
                const pending = value.pending orelse return error.InvalidSqlBackendResponse;
                if (pending.operation != .renew or scope.registration.lease.expires_at_ms != pending.operation.renew.expires_at_ms) return error.InvalidSqlBackendResponse;
            }
            // Recovery is irreversible. A renewal that was already in flight
            // may win after its pending delivery was replaced by closure. Its
            // observed expiry changes no backend identity and cannot strand
            // the source's unconditional fence behind a stale local lease.
            const owner = parsed.value.owner orelse return error.InvalidSqlBackendResponse;
            try owner.validate(.{ .home = .metadata, .group = value.registration.group, .incarnation = parsed.value.incarnation });
            if (!std.meta.eql(owner.lease, scope.registration.lease)) return error.InvalidSqlBackendResponse;
        } else if (parsed.value.owner != null) return error.InvalidSqlBackendResponse;
        if (parsed.value.next_work != null) return error.InvalidSqlBackendResponse;
        return parsed;
    }
    pub fn submit(self: Port, a: A, request: authority.Request) !authority.outcomes.Result {
        const bytes = try self.call(self.ptr, a, .{ .sql_lock_mutate = request });
        defer a.free(bytes);
        if (bytes.len > authority.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
        var parsed = try std.json.parseFromSlice(?authority.outcomes.Outcome, a, bytes, .{});
        defer parsed.deinit();
        const result = try (parsed.value orelse return error.MetadataMutationOutcomeUnknown).result();
        try result.validate();
        return result;
    }
    pub fn deliver(self: Port, a: A, request: authority.Request) !?authority.outcomes.Rejection {
        const observed = try self.call(self.ptr, a, .{ .sql_lock_outcome = request });
        defer a.free(observed);
        if (observed.len > authority.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
        var receipt = try std.json.parseFromSlice(?authority.outcomes.Outcome, a, observed, .{});
        defer receipt.deinit();
        if (receipt.value) |outcome| {
            if (outcome == .rejected) return outcome.rejected;
            _ = try outcome.result();
            return null;
        }
        const submitted = try self.call(self.ptr, a, .{ .sql_lock_mutate = request });
        defer a.free(submitted);
        if (submitted.len > authority.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
        var outcome = try std.json.parseFromSlice(?authority.outcomes.Outcome, a, submitted, .{});
        defer outcome.deinit();
        const found = outcome.value orelse return error.MetadataMutationOutcomeUnknown;
        if (found == .rejected) return found.rejected;
        _ = try found.result();
        return null;
    }
};

/// One native observation, at most one exact control delivery, and one local
/// CAS. Control progress never returns permission to evaluate SQL. Another
/// instance may deliver immutable control but cannot take an execution permit.
/// Only expiry or explicit recovery requests fence before decision-gated
/// cleanup; a different local facade is not proof that its owner is dead.
pub fn advance(store: *transactions.DurableSessionStore, a: A, principal: []const u8, prior: Record, instance: Id, now_ms: u64, nonce: Id, port: Port) !void {
    try prior.validate(a);
    if (prior.phase == .retired) return;
    if (zero(instance) or zero(nonce)) return error.InvalidSqlConnection;
    var next = prior;
    next.recovery = prior.recovery or prior.registration.lease.expires_at_ms <= now_ms;
    // Live idle/executing commands need no metadata round trip per worker tick.
    if (!next.recovery and prior.pending == null and (prior.phase == .idle or prior.phase == .ready or prior.phase == .executing)) return;
    var observed = try port.snapshot(a, prior);
    defer observed.deinit();
    const cut = observed.value;
    if (cut.scope) |scope| {
        next.registration = scope.registration;
        next.recovery = prior.recovery or scope.registration.lease.expires_at_ms <= now_ms;
        if (scope.state == .retired) {
            next.phase = .retired;
            next.body_skipped = false;
            next.control_effect = null;
            next.recovery = true;
            next.ticket = null;
            next.pending = null;
        } else if (scope.state == .fenced) {
            next.phase = .closing;
            next.control_effect = null;
            next.recovery = true;
            // Each successful quantum gets a fresh admission; replaying an
            // earlier journal result must never count as another close step.
            if (prior.phase == .closing and prior.pending != null and !expired(prior.pending.?, cut.head)) if (try port.deliver(a, prior.pending.?)) |failure| {
                if (next.failure == null) next.failure = failure;
            };
            next.pending = next.controlRequest(cut.head, nonce, if (scope.transaction) |transaction| .{ .release = transaction } else .close_step);
        } else if (next.recovery) {
            next.phase = .fencing;
            next.control_effect = null;
            if (prior.phase == .fencing and prior.pending != null and !expired(prior.pending.?, cut.head)) if (try port.deliver(a, prior.pending.?)) |failure| {
                if (next.failure == null) next.failure = failure;
            };
            next.pending = next.controlRequest(cut.head, nonce, .{ .fence = .close });
        } else if (prior.pending != null and prior.pending.?.operation == .renew) {
            if (scope.registration.lease.expires_at_ms == prior.pending.?.operation.renew.expires_at_ms) {
                next.pending = if (prior.control_effect) |effect| next.controlRequest(cut.head, nonce, .{ .effect = .{ .command = prior.ticket.?.command, .effect = effect } }) else if (prior.phase == .finishing) next.controlRequest(cut.head, nonce, .{ .finish = prior.ticket.?.command }) else null;
            } else if (expired(prior.pending.?, cut.head)) {
                next.pending = next.controlRequest(cut.head, nonce, prior.pending.?.operation);
            } else {
                if (try port.deliver(a, prior.pending.?)) |failure| {
                    next.failure = failure;
                    next.recovery = true;
                    try replace(store, principal, prior, next);
                } else try acknowledgeRenewal(store, a, principal, prior.registration.connection, prior.pending.?);
                return;
            }
        } else if (prior.control_effect) |effect| {
            if (!std.meta.eql(scope.command, @as(?authority.scopes.Command, prior.ticket.?.command))) return error.InvalidSqlBackendResponse;
            const owner = cut.owner orelse return error.InvalidSqlBackendResponse;
            if (cut.receipt) |receipt| {
                try receipt.requireFor(.{ .home = .metadata, .group = prior.registration.group, .incarnation = cut.incarnation }, effect);
                if (receipt.outcome == .completed) {
                    next.control_effect = null;
                    next.pending = if (prior.phase == .finishing) next.controlRequest(cut.head, nonce, .{ .finish = prior.ticket.?.command }) else null;
                    try replace(store, principal, prior, next);
                    return;
                }
                if (receipt.outcome != .pending) return error.InvalidSqlBackendResponse;
            }
            if (owner.next_effect != effect.sequence or owner.waiting != null) return error.InvalidSqlBackendResponse;
            if (owner.job) |job| if (!std.meta.eql(job.effect, effect)) return error.InvalidSqlBackendResponse;
            const operation: @FieldType(authority.Request, "operation") = if (owner.job != null)
                .{ .step = .{ .command = prior.ticket.?.command, .effect = effect } }
            else
                .{ .effect = .{ .command = prior.ticket.?.command, .effect = effect } };
            if (std.meta.eql(prior.pending.?.operation, operation) and !expired(prior.pending.?, cut.head)) {
                if (try port.deliver(a, prior.pending.?)) |failure| {
                    if (failure != .CatalogPublicationProofPending) {
                        next.failure = failure;
                        next.recovery = true;
                    }
                }
            }
            next.pending = next.controlRequest(cut.head, nonce, operation);
        } else switch (prior.phase) {
            .registering => {
                next.phase = .idle;
                next.pending = null;
                next.next_command = scope.next_command;
            },
            .beginning => {
                if (!std.meta.eql(scope.command, @as(?authority.scopes.Command, prior.ticket.?.command))) {
                    if (expired(prior.pending.?, cut.head)) {
                        next.pending = next.controlRequest(cut.head, nonce, prior.pending.?.operation);
                        try replace(store, principal, prior, next);
                    } else if (try port.deliver(a, prior.pending.?)) |failure| {
                        next.failure = failure;
                        next.recovery = true;
                        try replace(store, principal, prior, next);
                    }
                    return;
                }
                next.phase = if (prior.body_skipped) .finishing else .ready;
                next.pending = if (prior.body_skipped) next.controlRequest(cut.head, nonce, .{ .finish = prior.ticket.?.command }) else null;
            },
            .finishing => {
                // A returned SQL body may still await an independent durable
                // decision. Do not complete its command and admit a new body
                // merely because no native decision has been installed yet.
                if (try uncertainConnection(store, a, prior.registration.connection)) return;
                if (!std.meta.eql(scope.completed, @as(?authority.scopes.Command, prior.ticket.?.command))) {
                    // A terminal SQL receipt can precede native lock cleanup.
                    // Deliver one decision-gated release quantum through this
                    // source's exact outbox, independently of the work scanner.
                    // The accepting writer validates the sealed decision; a
                    // finishing command must never fence away session grants.
                    if (scope.transaction) |transaction| if (transaction.decision != null) {
                        if (!std.meta.eql(scope.command, @as(?authority.scopes.Command, prior.ticket.?.command))) return error.InvalidSqlBackendResponse;
                        const operation: @FieldType(authority.Request, "operation") = .{ .release = transaction };
                        if (std.meta.eql(prior.pending.?.operation, operation) and !expired(prior.pending.?, cut.head)) {
                            if (try port.deliver(a, prior.pending.?)) |failure| {
                                if (failure != .CatalogPublicationProofPending) {
                                    next.failure = failure;
                                    next.recovery = true;
                                }
                            }
                        }
                        next.pending = next.controlRequest(cut.head, nonce, operation);
                        try replace(store, principal, prior, next);
                        return;
                    };
                    if (scope.releasing) return error.InvalidSqlBackendResponse;
                    if (!std.meta.eql(scope.command, @as(?authority.scopes.Command, prior.ticket.?.command))) return error.InvalidSqlBackendResponse;
                    // Completion owns abandoned effect cleanup, not SQL replay.
                    // Each observation stages/delivers at most one exact native
                    // waiter cancellation or bounded control quantum. Earlier
                    // session grants survive cancellation of this command.
                    const owner = cut.owner orelse return error.InvalidSqlBackendResponse;
                    const cleanup: ?@FieldType(authority.Request, "operation") = if (owner.waiting) |waiting|
                        .{ .cancel = .{ .command = prior.ticket.?.command, .effect = waiting } }
                    else if (owner.job) |job|
                        .{ .step = .{ .command = prior.ticket.?.command, .effect = job.effect } }
                    else
                        null;
                    if (cleanup) |operation| {
                        if (std.meta.eql(prior.pending.?.operation, operation) and !expired(prior.pending.?, cut.head)) {
                            if (try port.deliver(a, prior.pending.?)) |failure| {
                                if (failure != .CatalogPublicationProofPending) {
                                    next.failure = failure;
                                    next.recovery = true;
                                }
                            }
                        }
                        next.pending = next.controlRequest(cut.head, nonce, operation);
                        try replace(store, principal, prior, next);
                        return;
                    }
                    if (prior.pending.?.operation != .finish) {
                        next.pending = next.controlRequest(cut.head, nonce, .{ .finish = prior.ticket.?.command });
                        try replace(store, principal, prior, next);
                        return;
                    }
                    if (expired(prior.pending.?, cut.head)) {
                        next.pending = next.controlRequest(cut.head, nonce, prior.pending.?.operation);
                        try replace(store, principal, prior, next);
                    } else if (try port.deliver(a, prior.pending.?)) |failure| {
                        if (failure == .CatalogPublicationProofPending) {
                            // A busy result is retained under its original
                            // nonce. Retry only this idempotent control with a
                            // fresh admission after the next native observation.
                            next.pending = next.controlRequest(cut.head, nonce, prior.pending.?.operation);
                            try replace(store, principal, prior, next);
                            return;
                        }
                        next.failure = failure;
                        next.recovery = true;
                        try replace(store, principal, prior, next);
                    }
                    return;
                }
                next.phase = .idle;
                next.body_skipped = false;
                next.ticket = null;
                next.pending = null;
                next.next_command = scope.next_command;
            },
            else => return error.InvalidSqlConnection,
        }
    } else {
        if (prior.phase != .registering) return error.InvalidSqlBackendResponse;
        // Scope records are retained. Absence beyond the admission window
        // proves an old registration can no longer arrive and create one.
        if (expired(prior.pending.?, cut.head)) {
            next.phase = .retired;
            next.recovery = true;
            next.pending = null;
        } else {
            if (try port.deliver(a, prior.pending.?)) |failure| {
                next.phase = .retired;
                next.recovery = true;
                next.failure = failure;
                next.pending = null;
            }
            if (next.recovery != prior.recovery or next.phase != prior.phase) try replace(store, principal, prior, next);
            return;
        }
    }
    try replace(store, principal, prior, next);
}

fn expired(request: authority.Request, head: u64) bool {
    return head >= request.admission.head and head - request.admission.head >= authority.outcomes.retained;
}

pub const WorkPage = struct { ids: [][32]u8, next_after: ?[32]u8 };
pub fn scanWork(store: *transactions.DurableSessionStore, a: A, after: ?[32]u8, limit: usize) !WorkPage {
    if (limit > 128) return error.SqlProgramLimitExceeded;
    var ids: std.ArrayListUnmanaged([32]u8) = .empty;
    errdefer ids.deinit(a);
    if (limit == 0) return .{ .ids = try ids.toOwnedSlice(a), .next_after = null };
    const Visitor = struct {
        a: A,
        ids: *@TypeOf(ids),
        limit: usize,
        fn run(raw: *anyopaque, address: []const u8, value: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (address.len != work_prefix.len + 32 or value.len != 0) return error.InvalidSqlConnection;
            const id: [32]u8 = address[work_prefix.len..][0..32].*;
            try self.ids.append(self.a, id);
            return self.ids.items.len < self.limit;
        }
    };
    var visitor: Visitor = .{ .a = a, .ids = &ids, .limit = limit };
    const start = if (after) |id| key(work_prefix, id) else null;
    try store.scanPrefixFromWithContext(work_prefix, if (start) |*address| address else null, &visitor, Visitor.run);
    const next_after = if (ids.items.len == limit) ids.items[ids.items.len - 1] else null;
    return .{ .ids = try ids.toOwnedSlice(a), .next_after = next_after };
}

/// Both standalone and routed metadata use the existing authenticated catalog
/// transport. It binds the frozen request body; this adapter never evaluates SQL.
pub const NativePort = struct {
    server: *@import("http_server.zig").ApiHttpServer,
    context: @import("antfly_local_sources").api_operation.RequestContext,
    pub fn port(self: *NativePort) Port {
        return .{ .ptr = self, .call = call };
    }
    fn call(raw: *anyopaque, a: A, input: Call) anyerror![]u8 {
        const self: *NativePort = @ptrCast(@alignCast(raw));
        var context = self.context;
        context.setting_admin = true;
        // Scope control cannot start a public SQL completion. Only the SQL
        // execution path, after taking its permit, may publish that admission.
        context.sql_completion = null;
        context.sql_completion_submission = null;
        try context.ensureActive();
        if (input == .sql_lock_mutate) if (!try @import("sql_data_locks.zig").beforeMetadataMutation(self.server, a, context, input.sql_lock_mutate)) return error.MetadataMutationOutcomeUnknown;
        return self.server.source.systemCatalog(a, context, input);
    }
};

/// One source owner per quantum. The caller retains an exclusive keyset cursor
/// and a time budget, so pending external decisions cannot starve other owners.
pub fn advanceWork(server: *@import("http_server.zig").ApiHttpServer, after: *?[32]u8) !bool {
    const store = server.txn_sessions.durable orelse return false;
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const page = try scanWork(store, a, after.*, 1);
    after.* = page.next_after;
    if (page.ids.len == 0) return false;
    try advanceConnection(server, a, page.ids[0], .none);
    return true;
}

/// The shared upkeep lane claims keys independently, then executes each owner
/// outside its cursor mutex. No request/credential storage is borrowed here.
pub fn advanceConnection(server: *@import("http_server.zig").ApiHttpServer, a: A, connection_id: [32]u8, cancellation: @import("antfly_local_sources").api_operation.CancellationToken) !void {
    const store = server.txn_sessions.durable orelse return;
    // The index scan and record read can straddle legitimate retirement. Read
    // the record and its index membership together before calling it corrupt.
    var owned = (try loadMode(store, a, connection_id, true)) orelse return;
    defer owned.deinit();
    const prior = owned.value;
    const io_for_signal = server.embedding_provider_runtime.io;
    const signal_command = if (prior.ticket) |ticket| if (server.sql_connection_admission.hasSignal(io_for_signal, .init(connection_id, prior.principal), ticket.command)) ticket.command else null else null;
    var signal_invalid = false;
    defer if (signal_command) |command| {
        if (signal_invalid) server.sql_connection_admission.publish(io_for_signal, .init(connection_id, prior.principal), command, null);
        if (load(store, a, connection_id)) |maybe| {
            if (maybe) |value| {
                var current = value;
                defer current.deinit();
                const live = !signal_invalid and current.value.phase == .executing and !current.value.recovery and current.value.ticket != null and std.meta.eql(current.value.ticket.?.command, command);
                server.sql_connection_admission.publish(io_for_signal, .init(connection_id, prior.principal), command, if (live) current.value.registration.lease.expires_at_ms else null);
            } else server.sql_connection_admission.publish(io_for_signal, .init(connection_id, prior.principal), command, null);
        } else |_| {}
    };
    // A shared physical session store does not confer another issuer's source
    // ownership. A different facade also isn't evidence of process death.
    if (prior.registration.lease.node != server.localSessionNodeId()) return;
    if (prior.recovery) {
        // This durable owner cannot admit another body. Discard unsealed local
        // staging independently of the request; sealed decisions remain owned
        // by the existing coordinator. Failed I/O retries on a later quantum.
        var connection = @import("sql_connections.zig").load(store, a, connection_id, prior.principal, prior.registration.lease.node, 0) catch |err| switch (err) {
            error.SqlConnectionNotFound => null,
            else => return err,
        };
        defer if (connection) |*value| value.deinit();
        if (connection) |value| {
            if (value.value.active_txn) |id| _ = server.txn_sessions.removeBeforeExecution(server.alloc, id);
        }
    }
    const time = @import("antfly_platform").time;
    if (prior.phase == .finishing and try uncertainConnection(store, a, prior.registration.connection)) {
        var reconciled = try @import("sql_connections.zig").loadReconciled(store, &server.txn_sessions, a, prior.registration.connection, prior.principal, prior.registration.lease.node, time.realtimeNs() / std.time.ns_per_ms);
        reconciled.deinit();
    }
    const io = server.embedding_provider_runtime.io;
    const instance = try server.sql_connection_admission.executionInstance(io);
    var nonce: Id = undefined;
    try io.randomSecure(&nonce);
    var native: NativePort = .{ .server = server, .context = .{ .deadline_ns = time.monotonicNs() +| std.time.ns_per_s, .cancellation = cancellation } };
    const now_ms = time.realtimeNs() / std.time.ns_per_ms;
    const due = prior.registration.lease.expires_at_ms > now_ms and prior.registration.lease.expires_at_ms - now_ms <= renew_before_ms;
    const live: ?Liveness = if (!due or !std.mem.eql(u8, &prior.instance, &instance) or prior.recovery or (prior.pending != null and prior.pending.?.operation == .renew)) null else switch (prior.phase) {
        .executing => if (try server.sql_connection_admission.executing(io, .init(prior.registration.connection, prior.principal), prior.ticket.?.command)) .{ .command = prior.ticket.?.command } else null,
        .finishing => .control,
        .idle => blk: {
            var connection = @import("sql_connections.zig").load(store, a, prior.registration.connection, prior.principal, prior.registration.lease.node, now_ms) catch |err| switch (err) {
                error.SqlConnectionNotFound => break :blk null,
                else => return err,
            };
            defer connection.deinit();
            if (connection.value.active_txn) |txn_id| {
                var session = (try server.txn_sessions.getSqlState(a, txn_id)) orelse break :blk null;
                defer session.deinit(a);
                if (!std.meta.eql(session.connection_id, @as(?[32]u8, prior.registration.connection)) or session.owner_node_id != prior.registration.lease.node or
                    session.execution_started or session.terminal != null or session.terminal_prepare or session.terminal_abort) break :blk null;
                server.txn_sessions.validateSqlLease(a, txn_id, prior.registration.lease.node) catch |err| switch (err) {
                    error.SessionLeaseLost => break :blk null,
                    else => return err,
                };
            }
            break :blk .idle;
        },
        else => null,
    };
    maintain(store, a, prior, instance, now_ms, nonce, native.port(), live) catch |err| {
        signal_invalid = switch (err) {
            error.CatalogGenerationChanged, error.Forbidden, error.InvalidSqlBackendResponse, error.SqlConnectionNotFound => true,
            else => false,
        };
        return err;
    };
    return;
}

test "SQL command producer native outbox aborts every staged write before WAL publication" {
    const native = @import("../metadata/storage/raft_apply_store.zig");
    const DocStore = @import("antfly_local_sources").storage_docstore.DocStore;
    const Fault = struct {
        txn: *DocStore.Txn,
        remaining: usize,
        pub fn put(self: *@This(), address: []const u8, bytes: []const u8) !void {
            if (self.remaining == 0) return error.InjectedStorageFailure;
            self.remaining -= 1;
            try self.txn.put(address, bytes);
        }
        pub fn delete(self: *@This(), address: []const u8) !void {
            if (self.remaining == 0) return error.InjectedStorageFailure;
            self.remaining -= 1;
            try self.txn.delete(address);
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-outbox-abort", .{tmp.sub_path});
    defer a.free(path);
    const connection: [32]u8 = @splat('a');
    const baseline: Record = .{
        .principal = "alice",
        .instance = @splat(1),
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .registration = .{ .connection = connection, .nonce = @splat(2), .standalone = true, .lease = .{ .backend = .{ .incarnation = @splat(3), .id = @splat(4), .fence = 1 }, .node = 0, .database = 1, .principal = try authority.principalDigest(a, .bootstrap), .expires_at_ms = 1000 } },
        .phase = .ready,
        .ticket = .{ .instance = @splat(1), .command = .{ .sequence = 1, .nonce = @splat(5), .executor = @splat(6) }, .body = @splat(7) },
    };
    {
        var metadata = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var txn = try metadata.store.beginWriteTxn();
        errdefer txn.abort();
        try putTxn(a, &txn, baseline);
        try txn.commit();
    }
    const Target = enum { executing, retired, erased };
    for ([_]Target{ .executing, .retired, .erased }) |target| for (0..2) |cut| {
        {
            var metadata = try native.RaftApplyStore.init(a, .{ .root_dir = path });
            defer metadata.deinit();
            var txn = try metadata.store.beginWriteTxn();
            var fault: Fault = .{ .txn = &txn, .remaining = cut };
            var next = baseline;
            next.phase = if (target == .executing) .executing else .retired;
            next.revision += 1;
            if (target != .executing) {
                next.ticket = null;
                next.recovery = true;
            }
            const result = if (target == .erased) eraseTxn(&fault, connection) else putTxn(a, &fault, next);
            txn.abort();
            try std.testing.expectError(error.InjectedStorageFailure, result);
        }
        // Abort means no permit or retirement can escape into the WAL, even
        // when the record write succeeded before the work-index write failed.
        var reopened = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer reopened.deinit();
        var txn = try reopened.store.beginReadTxn();
        defer txn.abort();
        var found = (try readTxn(a, &txn, connection)).?;
        defer found.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(found.arena.allocator(), baseline, .{}), try std.json.Stringify.valueAlloc(found.arena.allocator(), found.value, .{}));
        try std.testing.expectEqualStrings("", try txn.get(&key(work_prefix, connection)));
    };
}
