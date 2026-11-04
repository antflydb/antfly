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

//! Private native lock commands share the ordinary owner WAL and apply fence.
//! The receiving leader independently captures Checked before proposal; apply
//! validates the committed cut without metadata IO or frontend lifetimes.
const std = @import("std");
const scopes = @import("pessimistic_lock_authority.zig");
const participant = @import("pessimistic_lock_participant.zig");
const row = @import("pessimistic_lock_row.zig");
const transactions = @import("transactions.zig");
const frozen = @import("pessimistic_lock_transaction.zig");
const Identity = @import("db/doc_identity_namespace.zig").Namespace;
const A = std.mem.Allocator;
const ledger = scopes.ledger;

pub const Checked = struct {
    source: scopes.Record,
    participant: scopes.Data.Participant,
    admitted_at_ms: u64,

    pub fn validate(self: Checked, admission: participant.Admission) !void {
        try admission.validate();
        const ns: ledger.Namespace = .{ .home = .metadata, .group = admission.source.group, .incarnation = admission.source.lease.backend.incarnation };
        try self.source.validate(ns);
        try self.participant.validate(ns, admission.source.lease.backend);
        var normalized = admission;
        normalized.source.lease.expires_at_ms = self.participant.admission.source.lease.expires_at_ms;
        if (normalized.purpose != .execution or !std.meta.eql(normalized, self.participant.admission) or self.admitted_at_ms == 0) return error.LockOwnerChanged;
        var registration = admission.source;
        registration.lease.expires_at_ms = self.source.registration.lease.expires_at_ms;
        if (!std.meta.eql(registration, self.source.registration)) return error.LockOwnerChanged;
        const directory = self.source.data orelse return error.LockOwnerChanged;
        if (std.mem.indexOfScalar(u64, &directory.slots, admission.generation) == null) return error.LockOwnerChanged;
        const transaction = self.source.transaction orelse return error.InvalidLockScope;
        if (!std.mem.eql(u8, &transaction.id, &admission.transaction)) return error.InvalidLockScope;
    }
    pub fn requireExecuting(self: Checked, admission: participant.Admission) !void {
        try self.validate(admission);
        if (self.source.state != .active or self.source.releasing or !self.participant.command_open or
            self.source.registration.lease.expires_at_ms <= self.admitted_at_ms or
            !std.meta.eql(self.source.command, @as(?scopes.Command, admission.parent_command)) or
            self.source.transaction.?.decision != null) return error.LockOwnerBusy;
    }
};

pub const Command = struct {
    admission: participant.Admission,
    /// Supplied only by the accepting leader, never from a public SQL body.
    checked: ?Checked = null,
    operation: union(enum) {
        begin,
        acquire: ledger.Effect,
        row: struct { effect: ledger.Effect, key: []const u8, witness: row.Witness, wait: @FieldType(row.Request, "wait") },
        cancel: struct { effect: ledger.Effect, key: ?[]const u8 = null },
        control: ledger.Effect,
        cancel_wait: ledger.Effect,
        finish,
        close,
        prepare: scopes.DecisionRef,
        resolve: scopes.DecisionRef,
    },
    pub fn validate(self: Command) !void {
        try self.admission.validate();
        switch (self.operation) {
            .acquire => |effect| try (participant.Acquisition{ .admission = self.admission, .effect = effect }).validate(),
            .row => |value| try self.rowRequest(value).validate(),
            .cancel => |value| try (participant.Acquisition{ .admission = self.admission, .effect = value.effect, .physical_key = value.key }).validate(),
            .cancel_wait => |effect| {
                try effect.validate();
                if (effect.action != .acquire or !effect.backend.eql((try self.admission.registration()).lease.backend)) return error.InvalidLockEffect;
            },
            .control => |effect| try (participant.SavepointControl{ .admission = self.admission, .effect = effect }).validate(),
            .prepare, .resolve => |decision| try decision.validate(),
            else => {},
        }
    }
    pub fn rowRequest(self: Command, value: @FieldType(@FieldType(Command, "operation"), "row")) row.Request {
        return .{ .acquisition = .{ .admission = self.admission, .effect = value.effect, .physical_key = value.key }, .witness = value.witness, .wait = value.wait };
    }
};

pub const Query = struct { admission: participant.Admission, effect: ?ledger.Effect = null, row_request: ?row.Request = null };
pub const Snapshot = struct {
    scope: ?scopes.Record,
    owner: ?ledger.Owner,
    receipt: ?ledger.Receipt = null,
    row_result: ?row.Result = null,
    resource: ?ledger.ResourceStamp = null,
    frozen: ?frozen.Record = null,
};

pub fn snapshot(a: A, txn: anytype, receiving: Identity, query: Query) !Snapshot {
    try query.admission.validate();
    try query.admission.binding.requireIdentity(receiving);
    const ns = query.admission.binding.namespace;
    const backend = (try query.admission.registration()).lease.backend;
    var result: Snapshot = .{ .scope = try scopes.read(a, txn, ns, backend), .owner = try ledger.readOwner(a, txn, ns, backend), .frozen = try frozen.read(a, txn, query.admission.transaction) };
    const observed = result.scope orelse return result;
    const context: Observer = .{ .admission = query.admission, .registration = observed.registration };
    if (query.row_request) |request| {
        if (query.effect != null or !std.meta.eql(request.acquisition.admission, query.admission)) return error.InvalidLockEffect;
        result.row_result = try row.observe(a, txn, receiving, request, context);
        if (result.row_result) |value| result.receipt = value.receipt;
    } else if (query.effect) |effect| {
        result.receipt = try ledger.observe(a, txn, ns, effect, context);
        if (effect.action == .acquire) result.resource = try ledger.readResourceStamp(a, txn, ns, effect.action.resource());
    }
    return result;
}
const Observer = struct {
    admission: participant.Admission,
    registration: scopes.Registration,
    pub fn requireObservation(self: @This(), _: anytype, lease: ledger.Lease, effect: ledger.Effect) !void {
        if (!std.meta.eql(lease, self.registration.lease) or !lease.backend.eql(effect.backend) or !effect.backend.eql((try self.admission.registration()).lease.backend)) return error.LockOwnerChanged;
    }
    pub fn requireDataObservation(self: @This(), _: anytype, value: participant.Admission, _: ledger.Effect) !void {
        if (!std.meta.eql(value, self.admission)) return error.LockOwnerChanged;
    }
};

/// All operations run in the caller's single native writer. The caller aborts
/// on error and stages its Raft receipt and standby outbox in this same writer.
pub fn apply(a: A, txn: anytype, receiving: Identity, group: u64, mode: @import("schema.zig").StorageMode, command: Command) !void {
    try command.validate();
    try command.admission.binding.requireIdentity(receiving);
    if (command.admission.binding.namespace.group != group) return error.IdentityNamespaceMismatch;
    const context: Authority = .{ .a = a, .command = command };
    switch (command.operation) {
        .begin => _ = try participant.admit(a, txn, receiving, command.admission, context),
        .acquire => |effect| _ = try participant.acquire(a, txn, receiving, .{ .admission = command.admission, .effect = effect }, context),
        .row => |value| _ = try row.acquire(a, txn, receiving, mode, command.rowRequest(value), context),
        .cancel => |value| _ = try participant.cancel(a, txn, receiving, .{ .admission = command.admission, .effect = value.effect, .physical_key = value.key }, context),
        .control => |effect| _ = try participant.controlStep(a, txn, receiving, .{ .admission = command.admission, .effect = effect }, context),
        .cancel_wait => |effect| try participant.cancelWaiting(a, txn, receiving, command.admission, effect, context),
        .finish => try participant.finish(a, txn, command.admission, context),
        .close => {
            try participant.fence(a, txn, receiving, command.admission, context);
            const backend = (try command.admission.registration()).lease.backend;
            const ns = command.admission.binding.namespace;
            const source = try scopes.read(a, txn, ns, backend) orelse return error.InvalidLockRecord;
            if (source.transaction) |transaction| {
                _ = try scopes.releaseTransaction(a, txn, ns, backend, transaction, context);
            } else _ = try scopes.closeStep(a, txn, ns, backend, context);
        },
        .prepare => |decision| _ = try participant.prepareStep(a, txn, receiving, command.admission, decision, context),
        .resolve => |decision| {
            if (try participant.resolvePreparedStep(a, txn, receiving, command.admission.binding, decision, command.admission.transaction, context)) {
                const backend = (try command.admission.registration()).lease.backend;
                try participant.fence(a, txn, receiving, command.admission, context);
                if (try scopes.closeStep(a, txn, command.admission.binding.namespace, backend, context)) {
                    const native = try transactions.externalParticipant(txn, command.admission.transaction) orelse return error.ExternalTransactionDecisionChanged;
                    try frozen.complete(a, txn, command.admission.transaction, native.owner orelse return error.ExternalTransactionDecisionChanged);
                }
            }
        },
    }
}

const Authority = struct {
    a: A,
    command: Command,
    fn requireSame(self: @This(), value: participant.Admission) !void {
        if (!std.meta.eql(value, self.command.admission)) return error.LockOwnerChanged;
    }
    pub fn requireDataAdmission(self: @This(), _: anytype, value: participant.Admission) !void {
        try self.requireSame(value);
        try (self.command.checked orelse return error.Forbidden).requireExecuting(value);
    }
    pub fn requireDataCommand(self: @This(), txn: anytype, value: participant.Admission, _: scopes.Record) !void {
        try self.requireDataAdmission(txn, value);
    }
    pub fn requireDataResource(self: @This(), txn: anytype, value: participant.Admission, key: ?[]const u8) !void {
        try self.requireDataAdmission(txn, value);
        // Receiving routing admission checks the opaque key's range; the
        // native resource codec independently checks its canonical framing.
        if (key) |physical| _ = try value.binding.row(physical);
    }
    pub fn requireDataEffect(self: @This(), txn: anytype, value: participant.Admission, _: ledger.Effect) !void {
        try self.requireDataAdmission(txn, value);
    }
    pub fn requireDataObservation(self: @This(), _: anytype, value: participant.Admission, _: ledger.Effect) !void {
        try self.requireSame(value);
    }
    pub fn requireDataControl(self: @This(), txn: anytype, value: participant.Admission, _: ledger.Effect) !void {
        try self.requireDataAdmission(txn, value);
    }
    fn requireNativeDecision(self: @This(), txn: anytype, selected: scopes.DecisionRef, terminal: bool) !void {
        const ref = try frozen.read(self.a, txn, self.command.admission.transaction) orelse return error.ExternalTransactionDecisionChanged;
        if (!std.meta.eql(ref.ref.admission, self.command.admission) or !std.meta.eql(ref.ref.decision, selected)) return error.ExternalTransactionDecisionChanged;
        const native = try transactions.externalParticipant(txn, self.command.admission.transaction) orelse return error.ExternalTransactionDecisionChanged;
        const owner = native.owner orelse return error.ExternalTransactionDecisionChanged;
        try ref.ref.validate(self.command.admission.transaction, owner);
        if ((native.status != .pending) != terminal) return error.LockOwnerBusy;
    }
    pub fn requireDataPreparedAdmission(self: @This(), txn: anytype, value: participant.Admission, selected: scopes.DecisionRef, _: scopes.Record) !void {
        try self.requireSame(value);
        try self.requireNativeDecision(txn, selected, false);
    }
    pub fn requirePreparedDataResolution(self: @This(), txn: anytype, binding: participant.Binding, selected: scopes.DecisionRef, id: [16]u8, _: ledger.Lease) !void {
        if (!binding.eql(self.command.admission.binding) or !std.mem.eql(u8, &id, &self.command.admission.transaction)) return error.LockOwnerChanged;
        try self.requireNativeDecision(txn, selected, true);
    }
    pub fn requireDataFence(self: @This(), txn: anytype, value: participant.Admission) !void {
        try self.requireSame(value);
        if (self.command.operation == .resolve) return self.requireNativeDecision(txn, self.command.operation.resolve, true);
        const checked = self.command.checked orelse return error.Forbidden;
        try checked.validate(value);
        if ((checked.source.state == .active or checked.source.transaction.?.decision != null) and !checked.source.releasing) return error.LockOwnerBusy;
        if (try transactions.externalParticipant(txn, value.transaction)) |native| {
            if (native.status != .aborted or native.prepared) return error.LockOwnerBusy;
        }
    }
    pub fn requireResolution(self: @This(), txn: anytype, _: ledger.Namespace, _: scopes.Record, value: scopes.Transaction) !void {
        if (!std.mem.eql(u8, &value.id, &self.command.admission.transaction) or value.decision != null) return error.LockOwnerBusy;
        try self.requireDataFence(txn, self.command.admission);
    }
    pub fn requirePreparedHandoff(_: @This(), _: anytype, _: ledger.Namespace, _: scopes.Record, _: ledger.Lease, _: ledger.Lease, _: [16]u8) !void {
        return error.Forbidden;
    }
    pub fn requireRetirement(self: @This(), txn: anytype, _: ledger.Namespace, _: scopes.Record) !void {
        try self.requireDataFence(txn, self.command.admission);
    }
};

pub fn validateBatch(input: @import("db/types.zig").BatchRequest) !void {
    const command = input.sql_lock orelse return error.InvalidBatchRequest;
    try command.validate();
    const empty: @import("db/types.zig").BatchRequest = .{};
    inline for (@typeInfo(@TypeOf(input)).@"struct".field_names, @typeInfo(@TypeOf(input)).@"struct".field_types) |name, T| {
        if (comptime !std.mem.eql(u8, name, "sql_lock") and !std.mem.eql(u8, name, "timestamp_ns") and !std.mem.eql(u8, name, "sync_level")) {
            if (comptime @typeInfo(T) == .pointer and @typeInfo(T).pointer.size == .slice) {
                if (@field(input, name).len != 0) return error.InvalidBatchRequest;
            } else if (!std.meta.eql(@field(input, name), @field(empty, name))) return error.InvalidBatchRequest;
        }
    }
}
