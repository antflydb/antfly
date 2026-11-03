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

//! Metadata authority adapter. External decisions remain in the shared native
//! prepared registry; neither expiry nor a claimed vote is resolution evidence.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const scopes = local.storage_pessimistic_lock_authority;
pub const locks = local.storage_pessimistic_lock_contract;
pub const prepared = @import("prepared_transactions.zig");
pub const outcomes = prepared.outcomes;
const A = std.mem.Allocator;
pub const max_bytes = 16384;
pub const decoder_version: u16 = @import("../metadata/topology_protocol.zig").sql_lock_authority_version;
pub const Ref = struct { backend: locks.Backend, command: scopes.Command };
pub const EffectInput = struct { command: scopes.Command, effect: scopes.ledger.Effect };
pub const Request = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    standalone: bool,
    database: u64,
    actor: prepared.Actor,
    backend: locks.Backend,
    operation: union(enum) {
        register: scopes.Registration,
        begin: scopes.Command,
        finish: scopes.Command,
        renew: struct { expected_expiry: u64, expires_at_ms: u64, observed_at_ms: u64 },
        fence: scopes.Fence,
        bind: struct { command: scopes.Command, transaction: [16]u8 },
        release: scopes.Transaction,
        effect: EffectInput,
        cancel: EffectInput,
        step: EffectInput,
        close_step,
    },
    pub fn validate(self: Request) !void {
        try self.admission.validate();
        try self.actor.validate();
        try self.backend.validate();
        if (self.database == 0 or !std.mem.eql(u8, &self.backend.incarnation, &self.incarnation)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .register => |value| if (!value.lease.backend.eql(self.backend) or value.standalone != self.standalone) return error.InvalidCatalogMutation,
            .effect, .cancel, .step => |value| {
                try value.command.validate();
                try value.effect.validate();
                if (!value.effect.backend.eql(self.backend)) return error.InvalidCatalogMutation;
                if (self.operation == .cancel and value.effect.action != .acquire) return error.InvalidCatalogMutation;
                if (self.operation == .step and value.effect.action != .control) return error.InvalidCatalogMutation;
                if (value.effect.action == .control) switch (value.effect.action.control) {
                    .prepare, .close => return error.InvalidCatalogMutation,
                    else => {},
                } else if (value.effect.action.class() != .advisory) return error.InvalidCatalogMutation;
            },
            .begin, .finish => |command| try command.validate(),
            .bind => |value| try value.command.validate(),
            .renew => |value| if (value.observed_at_ms >= value.expected_expiry) return error.CatalogGenerationChanged,
            .fence => |reason| if (reason == .expired and self.actor.session_user.kind != .bootstrap) return error.Forbidden,
            else => {},
        }
    }
};
pub fn principalDigest(a: A, principal: prepared.Principal) ![32]u8 {
    try principal.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, principal, .{});
    defer a.free(bytes);
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("antfly/sql-lock-principal/v1");
    hasher.update(bytes);
    var result: [32]u8 = undefined;
    hasher.final(&result);
    return result;
}
pub fn commandDigest(a: A, request: Request) ![32]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
pub fn semanticError(err: anyerror) anyerror {
    return switch (err) {
        error.LockOwnerBusy => error.CatalogPublicationProofPending,
        error.LockOwnerChanged, error.LockOwnerNotRegistered, error.LockEffectChanged, error.LockEffectOutOfOrder, error.LockEffectExpired => error.CatalogGenerationChanged,
        error.InvalidLockRecord => error.InvalidCatalogRecord,
        error.InvalidLockOwner, error.InvalidLockScope, error.InvalidLockEffect, error.InvalidLockResource => error.InvalidCatalogMutation,
        error.LockLimitExceeded => error.CatalogCommandTooLarge,
        else => err,
    };
}
pub const Context = struct {
    a: A,
    actor: prepared.Actor,
    database: u64,
    standalone: bool,
    maintenance: bool = false,
    fn requireOwner(self: Context, value: scopes.Record) !void {
        if (value.registration.standalone != self.standalone or value.registration.lease.database != self.database) return error.Forbidden;
        if (self.maintenance and self.actor.session_user.kind == .bootstrap and self.actor.current_role == null) return;
        if (!std.mem.eql(u8, &try principalDigest(self.a, self.actor.session_user), &value.registration.lease.principal)) return error.Forbidden;
    }
    pub fn requireRegistration(self: Context, _: anytype, ns: scopes.ledger.Namespace, value: scopes.Registration) !void {
        try value.validate(ns);
        try self.requireOwner(.{ .registration = value });
    }
    pub fn requireRead(self: Context, value: scopes.Record) !void {
        try self.requireOwner(value);
    }
    pub fn requireCommand(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record, _: scopes.Command) !void {
        try self.requireOwner(value);
    }
    pub fn requireTransaction(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record, id: [16]u8) !void {
        try self.requireOwner(value);
        if (std.mem.allEqual(u8, &id, 0)) return error.InvalidLockScope;
    }
    pub fn requireRenewal(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record, _: u64, _: u64) !void {
        try self.requireOwner(value);
    }
    pub fn requireFence(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record, _: scopes.Fence) !void {
        try self.requireOwner(value);
    }
    fn record(self: Context, txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, decision: scopes.DecisionRef) !prepared.Record {
        const found = try prepared.readOperation(self.a, txn, ns.group, decision.operation) orelse return error.InvalidPreparedTransactionPhase;
        const owner = try found.identity.owner(self.a);
        const transaction = value.transaction orelse return error.InvalidLockScope;
        const session = found.identity.session orelse return error.InvalidLockScope;
        const ref = found.identity.lock_scope orelse return error.InvalidLockScope;
        if (!std.mem.eql(u8, &owner.admission_digest, &decision.admission) or !std.mem.eql(u8, &found.identity.txn_id, &transaction.id) or
            found.identity.database_id != value.registration.lease.database or session.owner_node_id != value.registration.lease.node or
            session.connection_id == null or !std.mem.eql(u8, &session.connection_id.?, &value.registration.connection) or
            session.standalone != value.registration.standalone or !ref.backend.eql(value.registration.lease.backend)) return error.InvalidLockScope;
        return found;
    }
    pub fn requireSeal(self: Context, txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, decision: scopes.DecisionRef) !void {
        try self.requireOwner(value);
        const found = try self.record(txn, ns, value, decision);
        if (value.command == null or !std.meta.eql(found.identity.lock_scope.?.command, value.command.?)) return error.InvalidLockScope;
    }
    pub fn requireResolution(self: Context, txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, expected: scopes.Transaction) !void {
        try self.requireOwner(value);
        if (value.transaction == null and std.meta.eql(value.released_transaction, @as(?scopes.Transaction, expected))) return;
        if (!std.meta.eql(value.transaction, @as(?scopes.Transaction, expected))) return error.LockOwnerChanged;
        const decision = expected.decision orelse {
            // Active source rollback needs a command-bound local abort proof.
            // Expiry cleanup may retire an unsealed transaction only AFTER its
            // command was fenced, so no future native admission can install.
            if (value.state == .active) return error.LockOwnerBusy;
            return;
        };
        if (try prepared.readOperation(self.a, txn, ns.group, decision.operation)) |_| {
            const found = try self.record(txn, ns, value, decision);
            if (found.phase != .resolved and found.phase != .retired) return error.LockOwnerBusy;
            return;
        }
        const receipt = try prepared.readReceipt(self.a, txn, ns.group, decision.operation) orelse return error.LockOwnerBusy;
        if ((receipt.status != .committed and receipt.status != .aborted) or !std.mem.eql(u8, &receipt.owner.admission_digest, &decision.admission) or !std.mem.eql(u8, &receipt.txn_id, &expected.id)) return error.LockOwnerBusy;
    }
    pub fn requireRetirement(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record) !void {
        try self.requireOwner(value);
        if (value.transaction != null) return error.LockOwnerBusy;
    }
    pub fn requirePreparedHandoff(_: Context, _: anytype, _: scopes.ledger.Namespace, _: scopes.Record, _: scopes.ledger.Lease, _: scopes.ledger.Lease, _: [16]u8) !void {
        // Named handoff is activated only with an independently enrolled cohort.
        return error.InvalidLockScope;
    }
};
pub fn apply(a: A, txn: anytype, group: u64, request: Request) !void {
    try request.validate();
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = request.incarnation };
    const context: Context = .{ .a = a, .actor = request.actor, .database = request.database, .standalone = request.standalone, .maintenance = request.operation == .close_step or request.operation == .release or request.operation == .fence };
    switch (request.operation) {
        .register => |value| try scopes.register(a, txn, ns, value, context),
        .begin => |command| _ = try scopes.begin(a, txn, ns, request.backend, command, context),
        .finish => |command| try scopes.finish(a, txn, ns, request.backend, command, context),
        .bind => |value| try scopes.bindTransaction(a, txn, ns, request.backend, value.command, value.transaction, context),
        .release => |value| _ = try scopes.releaseTransaction(a, txn, ns, request.backend, value, context),
        .renew => |value| try scopes.renew(a, txn, ns, request.backend, value.expected_expiry, value.expires_at_ms, context),
        .fence => |reason| try scopes.fence(a, txn, ns, request.backend, reason, context),
        .effect => |value| _ = try scopes.applyEffect(a, txn, ns, request.backend, value.command, value.effect, context),
        .cancel => |value| _ = try scopes.cancelEffect(a, txn, ns, request.backend, value.command, value.effect, context),
        .step => |value| _ = try scopes.advanceEffect(a, txn, ns, request.backend, value.command, value.effect, context),
        .close_step => _ = try scopes.closeStep(a, txn, ns, request.backend, context),
    }
}

pub fn scopePrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_scope:{x:0>16}:", .{group});
}
pub fn workPrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_work:{x:0>16}:", .{group});
}
pub fn connectionPrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_connection:{x:0>16}:", .{group});
}
pub fn ledgerPrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_locks:{x:0>16}:", .{group});
}

pub const Query = struct {
    incarnation: [16]u8,
    standalone: bool,
    actor: prepared.Actor,
    database: u64,
    backend: ?locks.Backend = null,
    work: ?struct { observed_at_ms: u64, after: ?scopes.Work = null, wrap: bool = false } = null,
    effect: ?scopes.ledger.Effect = null,
    pub fn validate(self: Query) !void {
        try self.actor.validate();
        if ((self.backend == null) == (self.work == null) or (self.work == null and std.mem.allEqual(u8, &self.incarnation, 0))) return error.InvalidCatalogMutation;
        if (self.effect) |effect| {
            try effect.validate();
            if (self.backend == null or !effect.backend.eql(self.backend.?)) return error.InvalidCatalogMutation;
        }
        if (self.backend) |backend| {
            try backend.validate();
            if (self.database == 0 or !std.mem.eql(u8, &backend.incarnation, &self.incarnation)) return error.InvalidCatalogMutation;
        }
        if (self.work != null and (self.actor.session_user.kind != .bootstrap or self.actor.current_role != null)) return error.Forbidden;
    }
};
pub const Snapshot = struct {
    incarnation: [16]u8,
    standalone: bool,
    head: u64,
    scope: ?scopes.Record,
    owner: ?scopes.ledger.Owner,
    next_work: ?scopes.Work,
    receipt: ?scopes.ledger.Receipt = null,
};

pub const Observer = struct {
    registration: scopes.Registration,
    pub fn requireObservation(self: Observer, _: anytype, lease: scopes.ledger.Lease, effect: scopes.ledger.Effect) !void {
        if (!std.meta.eql(lease, self.registration.lease) or !effect.backend.eql(lease.backend)) return error.Forbidden;
    }
};
