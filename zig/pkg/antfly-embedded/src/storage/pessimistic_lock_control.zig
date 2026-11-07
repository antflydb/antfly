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
const command_control = @import("pessimistic_lock_command_control.zig");
const Identity = @import("db/doc_identity_namespace.zig").Namespace;
const A = std.mem.Allocator;
const ledger = scopes.ledger;

/// Deterministic private-command failures advance the committed Raft entry.
/// Corrupt persisted records remain storage failures and are never consumed.
pub const Rejection = error{ InvalidLockClass, InvalidLockEffect, InvalidLockOwner, InvalidLockResource, InvalidLockScope, LockEffectChanged, LockEffectExpired, LockEffectOutOfOrder, LockLimitExceeded, LockOwnerBusy, LockOwnerChanged, LockOwnerNotRegistered, LockPlanLimitExceeded };

pub const Checked = struct {
    source: scopes.Record,
    participant: scopes.Data.Participant,
    admitted_at_ms: u64,
    failed_transaction: bool = false,

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

/// An immutable prepared manifest selects an already-existing native image.
/// This envelope grants no execution rights: the receiving owner separately
/// rejoins its current Boot and checks its stored external transaction/ref.
pub const NativeControl = struct {
    route: @import("../metadata/catalog_route_contract.zig").CatalogRouteFence,
    owner: transactions.ExternalDecisionOwner,
    decision: scopes.DecisionRef,

    pub fn validate(self: NativeControl, admission: participant.Admission) !void {
        try self.route.validate();
        if (self.route.route.execution != .native_local or self.route.native_participant == null or
            self.route.route.group_id != admission.binding.namespace.group or
            self.route.table_id != admission.binding.identity.table_id or
            self.route.route.identity_namespace.shard_id != admission.binding.identity.shard_id or
            self.route.route.identity_namespace.range_id != admission.binding.identity.range_id)
            return error.ExternalTransactionDecisionChanged;
        try (frozen.Ref{ .admission = admission, .decision = self.decision }).validate(admission.transaction, self.owner);
    }
};

/// A private accepting-owner projection of an admitted abandoned TEMP child.
/// Metadata has authenticated the immutable abandonment checkpoint and selected
/// this actual table-retirement fence and old directory participant. This is
/// never an executing OwnerGrant and may not revoke the successor backend.
const RetirementAdmission = participant.Admission;
pub const RetirementControl = struct {
    route: @import("../metadata/catalog_route_contract.zig").CatalogRouteFence,
    scope_id: [16]u8,
    scope_revision: u64,
    abandonment_digest: [32]u8,
    physical_fence: @import("db/relational_integrity_topology_contract.zig").Fence,
    participant: scopes.Data.Participant,

    pub fn validate(self: RetirementControl, admission: RetirementAdmission) !void {
        try self.route.validate();
        _ = try self.physical_fence.encode();
        const native = if (self.route.route.execution == .native_local) self.route.route.execution.native_local else return error.LockOwnerChanged;
        if (native.kind != .temporary or self.route.native_participant == null or
            self.scope_revision == 0 or std.mem.allEqual(u8, &self.scope_id, 0) or
            std.mem.allEqual(u8, &self.abandonment_digest, 0) or
            self.route.route.group_id != admission.binding.namespace.group or
            self.route.table_id != admission.binding.identity.table_id or
            (self.route.route.identity_namespace.table_id != admission.binding.identity.table_id or self.route.route.identity_namespace.shard_id != admission.binding.identity.shard_id or self.route.route.identity_namespace.range_id != admission.binding.identity.range_id) or
            self.physical_fence.owner_group_id != self.route.route.group_id or
            !self.physical_fence.namespace.eql(admission.binding.identity)) return error.LockOwnerChanged;
        switch (self.physical_fence.role) {
            .document_retirement_source, .child_generation_source, .child_generation_dual => {},
            else => return error.LockOwnerChanged,
        }
        const owner = native.owner orelse return error.LockOwnerChanged;
        if (owner.issuer_node_id != admission.source.lease.node or
            !std.mem.eql(u8, &owner.connection_id, &admission.source.connection) or
            !std.mem.eql(u8, &owner.execution_instance, &admission.parent_command.executor)) return error.LockOwnerChanged;
        if (!std.meta.eql(self.route.route, self.participant.spec.route.route) or self.route.table_id != self.participant.spec.route.table_id or
            !std.meta.eql(self.route.native_participant, self.participant.spec.route.native_participant)) return error.LockOwnerChanged;
        const ns: ledger.Namespace = .{ .home = .metadata, .group = admission.source.group, .incarnation = admission.source.lease.backend.incarnation };
        try self.participant.validate(ns, admission.source.lease.backend);
        var normalized = admission;
        normalized.source.lease.expires_at_ms = self.participant.admission.source.lease.expires_at_ms;
        if (!std.meta.eql(normalized, self.participant.admission)) return error.LockOwnerChanged;
    }
    pub fn requireFence(self: RetirementControl, txn: anytype, admission: RetirementAdmission) !void {
        try self.validate(admission);
        const actual = (try @import("db/relational_integrity_topology.zig").current(txn)) orelse return error.IntegrityTopologyFenceMissing;
        if (!actual.eql(self.physical_fence)) return error.IntegrityTopologyChanged;
    }
};

pub const Command = struct {
    admission: participant.Admission,
    /// Supplied only by the accepting leader, never from a public SQL body.
    checked: ?Checked = null,
    native_control: ?NativeControl = null,
    retirement_control: ?RetirementControl = null,
    operation: Operation,

    /// A named transport type keeps native layout fingerprints independent of
    /// compiler-generated anonymous-type identities in separate archives.
    pub const Operation = union(enum) {
        begin,
        acquire: ledger.Effect,
        row: struct { effect: ledger.Effect, key: []const u8, witness: row.Witness, wait: @FieldType(row.Request, "wait") },
        cancel: struct { effect: ledger.Effect, key: ?[]const u8 = null },
        control: ledger.Effect,
        bound_control: command_control.Request,
        cancel_wait: ledger.Effect,
        finish,
        close,
        prepare: scopes.DecisionRef,
        resolve: scopes.DecisionRef,
    };
    pub fn validate(self: Command) !void {
        try self.admission.validate();
        if (self.native_control) |control| {
            try control.validate(self.admission);
            if (self.checked != null) return error.InvalidLockScope;
            switch (self.operation) {
                .prepare, .resolve => |decision| if (!std.meta.eql(decision, control.decision)) return error.ExternalTransactionDecisionChanged,
                else => return error.InvalidLockScope,
            }
        }
        if (self.retirement_control) |retirement| {
            if (self.checked != null or self.native_control != null or self.operation != .close) return error.InvalidLockScope;
            try retirement.validate(self.admission);
        }
        switch (self.operation) {
            .acquire => |effect| try (participant.Acquisition{ .admission = self.admission, .effect = effect }).validate(),
            .row => |value| try self.rowRequest(value).validate(),
            .cancel => |value| try (participant.Acquisition{ .admission = self.admission, .effect = value.effect, .physical_key = value.key }).validate(),
            .cancel_wait => |effect| {
                try effect.validate();
                if (effect.action != .acquire or !effect.backend.eql((try self.admission.registration()).lease.backend)) return error.InvalidLockEffect;
            },
            .bound_control => |value| try value.validate(self.admission),
            .control => |effect| try (participant.SavepointControl{ .admission = self.admission, .effect = effect }).validate(),
            .prepare, .resolve => |decision| try decision.validate(),
            else => {},
        }
    }
    /// Native admission has no ordered replay clock. Recheck the authenticated
    /// execution lease before waiting and again under the actual writer. Cleanup
    /// intentionally accepts expired sources through its separate fence authority.
    pub fn requireLiveExecution(self: Command, now_ms: u64) !void {
        switch (self.operation) {
            .begin, .acquire, .row, .cancel, .control, .bound_control, .cancel_wait => {
                const checked = self.checked orelse return error.Forbidden;
                try checked.requireExecuting(self.admission);
                if (checked.source.registration.lease.expires_at_ms <= now_ms) return error.LockEffectExpired;
            },
            .finish, .close, .prepare, .resolve => {},
        }
    }
    pub fn rowRequest(self: Command, value: @FieldType(@FieldType(Command, "operation"), "row")) row.Request {
        return .{ .acquisition = .{ .admission = self.admission, .effect = value.effect, .physical_key = value.key }, .witness = value.witness, .wait = value.wait };
    }
    pub fn nativeJsonSkipField(self: Command, comptime field: []const u8) bool {
        return ((comptime std.mem.eql(u8, field, "native_control")) and self.native_control == null) or
            ((comptime std.mem.eql(u8, field, "retirement_control")) and self.retirement_control == null);
    }
    pub fn jsonStringify(self: Command, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Command).@"struct".field_names) |field| if (!self.nativeJsonSkipField(field)) {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        };
        try writer.endObject();
    }
};

pub const Query = struct {
    admission: participant.Admission,
    native_control: ?NativeControl = null,
    retirement_control: ?RetirementControl = null,
    effect: ?ledger.Effect = null,
    row_request: ?row.Request = null,
    command_control: bool = false,
    intent_key: ?[]const u8 = null,
    pub fn validate(self: Query) !void {
        try self.admission.validate();
        if (self.native_control) |control| {
            try control.validate(self.admission);
            if (self.effect != null or self.row_request != null or self.command_control or self.intent_key != null) return error.InvalidLockScope;
        }
        if (self.retirement_control) |retirement| {
            if (self.native_control != null or self.effect != null or self.row_request != null or self.command_control or self.intent_key != null) return error.InvalidLockScope;
            try retirement.validate(self.admission);
        }
    }
    pub fn nativeJsonSkipField(self: Query, comptime field: []const u8) bool {
        return ((comptime std.mem.eql(u8, field, "native_control")) and self.native_control == null) or
            ((comptime std.mem.eql(u8, field, "retirement_control")) and self.retirement_control == null);
    }
    pub fn jsonStringify(self: Query, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Query).@"struct".field_names) |field| if (!self.nativeJsonSkipField(field)) {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        };
        try writer.endObject();
    }
};
pub const Snapshot = struct {
    scope: ?scopes.Record,
    owner: ?ledger.Owner,
    receipt: ?ledger.Receipt = null,
    row_result: ?row.Result = null,
    resource: ?ledger.ResourceStamp = null,
    frozen: ?frozen.Record = null,
    prepared_owner: ?ledger.Owner = null,
    bound_control: ?ledger.Effect = null,
    intent_owner: ?transactions.TxnId = null,
};

pub fn snapshot(a: A, txn: anytype, receiving: Identity, query: Query) !Snapshot {
    try query.validate();
    try query.admission.binding.requireIdentity(receiving);
    if (query.retirement_control) |retirement| try retirement.requireFence(txn, query.admission);
    const ns = query.admission.binding.namespace;
    const backend = (try query.admission.registration()).lease.backend;
    var result: Snapshot = .{ .scope = try scopes.read(a, txn, ns, backend), .owner = try ledger.readOwner(a, txn, ns, backend), .frozen = try frozen.read(a, txn, query.admission.transaction) };
    if (result.frozen) |record| {
        if (query.native_control) |control| {
            if (!std.meta.eql(record.ref, frozen.Ref{ .admission = query.admission, .decision = control.decision })) {
                return error.ExternalTransactionDecisionChanged;
            }
        }
        result.prepared_owner = try ledger.readOwner(a, txn, ns, try scopes.preparedBackend(ns, record.ref.decision));
    }
    if (query.command_control) result.bound_control = try command_control.effect(a, txn, query.admission);
    if (query.intent_key) |key| {
        _ = try query.admission.binding.row(key);
        result.intent_owner = try transactions.pendingIntentOwner(a, txn, key, query.admission.transaction);
    }
    const observed = result.scope orelse return result;
    const context: Observer = .{ .admission = query.admission, .registration = observed.registration };
    if (query.row_request) |request| {
        if (query.effect != null or !std.meta.eql(request.acquisition.admission, query.admission)) return error.InvalidLockEffect;
        result.row_result = try row.observe(a, txn, receiving, request, context);
        if (result.row_result) |value| result.receipt = value.receipt;
        result.resource = try ledger.readResourceStamp(a, txn, ns, request.acquisition.effect.action.resource());
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
        .bound_control => |value| _ = try command_control.apply(a, txn, receiving, command.admission, value, context),
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
            try frozen.adoptAbortedIfMissing(a, txn, receiving, .{ .admission = command.admission, .decision = decision });
            if ((try frozen.read(a, txn, command.admission.transaction)).?.retired) return;
            const source = try scopes.read(a, txn, command.admission.binding.namespace, (try command.admission.registration()).lease.backend) orelse return error.LockOwnerChanged;
            const native = try transactions.externalParticipant(txn, command.admission.transaction) orelse return error.ExternalTransactionDecisionChanged;
            const destination = try ledger.readOwner(a, txn, command.admission.binding.namespace, try scopes.preparedBackend(command.admission.binding.namespace, decision));
            if (native.status == .aborted and destination == null and (source.transaction == null or source.transaction.?.decision == null)) {
                try participant.fence(a, txn, receiving, command.admission, context);
                const backend = (try command.admission.registration()).lease.backend;
                if (source.transaction) |transaction| {
                    _ = try scopes.releaseTransaction(a, txn, command.admission.binding.namespace, backend, transaction, context);
                } else if (try scopes.closeStep(a, txn, command.admission.binding.namespace, backend, context)) {
                    try frozen.complete(a, txn, command.admission.transaction, native.owner orelse return error.ExternalTransactionDecisionChanged);
                }
                return;
            }
            // An abort can race BEGIN or an unfinished transfer. Complete that
            // admitted custody operation before releasing it; source grants
            // must never be abandoned in the gap between the two owners.
            if (destination == null or destination.?.state == .incoming) {
                _ = try participant.prepareStep(a, txn, receiving, command.admission, decision, context);
                return;
            }
            if (try participant.resolvePreparedStep(a, txn, receiving, command.admission.binding, decision, command.admission.transaction, context)) {
                const backend = (try command.admission.registration()).lease.backend;
                try participant.fence(a, txn, receiving, command.admission, context);
                if (try scopes.closeStep(a, txn, command.admission.binding.namespace, backend, context)) {
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
    pub fn requireDataAdmission(self: @This(), txn: anytype, value: participant.Admission) !void {
        try self.requireSame(value);
        try (self.command.checked orelse return error.Forbidden).requireExecuting(value);
        // A previously captured execution cut cannot reopen a transaction
        // after its native terminal decision, including ABORT before BEGIN.
        if (try transactions.externalParticipant(txn, value.transaction)) |native|
            if (native.status != .pending) return error.LockOwnerBusy;
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
    pub fn requireDataControl(self: @This(), txn: anytype, value: participant.Admission, effect: ledger.Effect) !void {
        try self.requireDataAdmission(txn, value);
        if (effect.action == .control and effect.action.control == .rollback and effect.action.control.rollback.first_aborted == 0) {
            // Only a durable failed SQL shell can release an executing root.
            // An admitted native decision instead owns phase-two cleanup.
            const checked = self.command.checked orelse return error.LockOwnerChanged;
            if (!checked.failed_transaction or self.command.operation != .bound_control or !self.command.operation.bound_control.failed_root or
                try transactions.externalParticipant(txn, value.transaction) != null or try frozen.read(self.a, txn, value.transaction) != null) return error.LockOwnerBusy;
        }
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
        try self.requireNativeDecision(txn, selected, self.command.operation == .resolve);
    }
    pub fn requirePreparedDataResolution(self: @This(), txn: anytype, binding: participant.Binding, selected: scopes.DecisionRef, id: [16]u8, _: ledger.Lease) !void {
        if (!binding.eql(self.command.admission.binding) or !std.mem.eql(u8, &id, &self.command.admission.transaction)) return error.LockOwnerChanged;
        try self.requireNativeDecision(txn, selected, true);
    }
    pub fn requireDataFence(self: @This(), txn: anytype, value: participant.Admission) !void {
        try self.requireSame(value);
        if (self.command.operation == .resolve) return self.requireNativeDecision(txn, self.command.operation.resolve, true);
        if (self.command.retirement_control) |retirement| {
            try retirement.requireFence(txn, value);
            // Prepared custody resolves only through the actual frozen external
            // decision owner. No missing/terminal record is an abort permit.
            if (try transactions.externalParticipant(txn, value.transaction) != null or
                try frozen.read(self.a, txn, value.transaction) != null) return error.LockOwnerBusy;
            return;
        }
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

test "SQL relation lifetime frozen native lock controls bind decision route and reject executing effects" {
    const a = std.testing.allocator;
    const incarnation: [16]u8 = @splat(1);
    const metadata_incarnation = std.fmt.bytesToHex(incarnation, .lower);
    const admission: participant.Admission = .{
        .purpose = .resolution,
        .binding = .{ .namespace = .{ .home = .data, .group = 7, .incarnation = incarnation }, .identity = .{ .table_id = 3, .shard_id = 7, .range_id = 8 } },
        .source = .{ .group = 1, .lease = .{ .backend = .{ .incarnation = incarnation, .id = @splat(2), .fence = 1 }, .database = 4, .node = 5, .principal = @splat(6), .expires_at_ms = 1000 }, .connection = @splat('a'), .nonce = @splat(8), .standalone = false },
        .transaction = @splat(9),
        .generation = 1,
        .parent_command = .{ .sequence = 1, .nonce = @splat(10), .executor = @splat(11) },
        .serial = 1,
    };
    const capture: NativeControl = .{
        .route = .{ .metadata_group_id = 1, .metadata_incarnation = metadata_incarnation, .catalog_revision = 2, .table_id = 3, .topology_epoch = 1, .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 3, .shard_id = 7, .range_id = 8 }, .execution = .{ .native_local = .{ .kind = .unlogged, .owner_node_id = 5, .owner_store_id = 6, .generation_instance = @splat(12), .data_generation = 1 } } }, .native_participant = .{ .physical_root_incarnation = 13, .table_id = 3, .shard_id = 7, .range_id = 8, .data_generation = 1 } },
        .owner = .{ .metadata_incarnation = metadata_incarnation, .operation_id = @splat(14), .admission_digest = @splat(15), .expires_at_unix_ns = 1000 },
        .decision = .{ .operation = @splat(14), .admission = @splat(15) },
    };
    const command: Command = .{ .admission = admission, .native_control = capture, .operation = .{ .prepare = capture.decision } };
    try command.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Command, a, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    try std.testing.expect(std.meta.eql(capture, parsed.value.native_control.?));
    var wrong = command;
    wrong.native_control.?.owner.operation_id[0] ^= 1;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, wrong.validate());
    wrong = command;
    wrong.operation = .begin;
    try std.testing.expectError(error.InvalidLockScope, wrong.validate());
    wrong = command;
    wrong.native_control.?.route.native_participant = null;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, wrong.validate());
    var query: Query = .{ .admission = admission, .native_control = capture };
    try query.validate();
    query.command_control = true;
    try std.testing.expectError(error.InvalidLockScope, query.validate());
    query = .{ .admission = admission, .native_control = capture, .intent_key = "user-row" };
    try std.testing.expectError(error.InvalidLockScope, query.validate());
    const ordinary: Command = .{ .admission = admission, .operation = .{ .resolve = capture.decision } };
    const legacy = try std.json.Stringify.valueAlloc(a, ordinary, .{});
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "native_control") == null);
    const legacy_shape = try std.json.Stringify.valueAlloc(a, .{ .admission = ordinary.admission, .checked = ordinary.checked, .operation = ordinary.operation }, .{});
    defer a.free(legacy_shape);
    try std.testing.expectEqualStrings(legacy_shape, legacy);
    const ordinary_query: Query = .{ .admission = admission, .intent_key = "user-row" };
    const lookup_wire = try std.json.Stringify.valueAlloc(a, ordinary_query, .{});
    defer a.free(lookup_wire);
    try std.testing.expect(std.mem.indexOf(u8, lookup_wire, "\"intent_key\":\"user-row\"") != null);
}
