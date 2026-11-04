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
pub const decoder_version: u16 = @import("../metadata/topology_protocol.zig").sql_native_lock_custody_version;
pub const Ref = struct { backend: locks.Backend, command: scopes.Command };
pub const EffectInput = struct { command: scopes.Command, effect: scopes.ledger.Effect };
const DataRetirement = struct { generation: u64, native: local.storage_pessimistic_lock_control.Snapshot };
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
        data_enroll: struct { command: scopes.Command, transaction: [16]u8, spec: scopes.Data.Spec },
        data_ack: struct { generation: u64, command: scopes.Command, native: local.storage_pessimistic_lock_control.Snapshot },
        data_retire: DataRetirement,
        data_handoff: DataRetirement,
        release: scopes.Transaction,
        /// Exact unadmitted purpose, retained locally before dispatch.
        /// Native journal absence/rejection, not that local claim, authorizes cleanup.
        release_unadmitted: prepared.Mutation,
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
            .data_enroll => |value| {
                try value.command.validate();
                try value.spec.validate(.{ .home = .metadata, .group = value.spec.route.metadata_group_id, .incarnation = self.incarnation });
                if (std.mem.allEqual(u8, &value.transaction, 0)) return error.InvalidCatalogMutation;
            },
            .data_ack => |value| {
                if (value.generation == 0) return error.InvalidCatalogMutation;
                try value.command.validate();
            },
            .data_retire, .data_handoff => |value| if (value.generation == 0) return error.InvalidCatalogMutation,
            .release_unadmitted => |original| {
                try original.validate();
                if (original.operation != .admit or !std.mem.eql(u8, &original.incarnation, &self.incarnation) or original.standalone != self.standalone or original.database_id != self.database) return error.InvalidCatalogMutation;
                const ref = original.operation.admit.identity.lock_scope orelse return error.InvalidCatalogMutation;
                if (!ref.backend.eql(self.backend)) return error.InvalidCatalogMutation;
            },
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
    unadmitted: ?prepared.Mutation = null,
    prepared_operation: ?[16]u8 = null,
    /// Populated only after the accepting metadata writer checks the current
    /// catalog binding, schema, physical route and transition fences.
    enrollment: ?scopes.Data.Spec = null,
    data_proof: ?local.storage_pessimistic_lock_control.Snapshot = null,
    handoff: bool = false,
    fn nativeParticipant(self: Context, root: scopes.Record, part: scopes.Data.Participant) !struct { scope: scopes.Record, owner: scopes.ledger.Owner } {
        try self.requireOwner(root);
        const native = self.data_proof orelse return error.Forbidden;
        const scope = native.scope orelse return error.LockOwnerBusy;
        const owner = native.owner orelse return error.LockOwnerBusy;
        const ns = part.admission.binding.namespace;
        try scope.validate(ns);
        try owner.validate(ns);
        var registration = try part.admission.registration();
        registration.lease.expires_at_ms = scope.registration.lease.expires_at_ms;
        if (!std.meta.eql(registration, scope.registration) or !std.meta.eql(owner.lease, scope.registration.lease)) return error.LockOwnerChanged;
        return .{ .scope = scope, .owner = owner };
    }
    pub fn requireDataCompletion(self: Context, _: anytype, _: scopes.ledger.Namespace, root: scopes.Record, part: scopes.Data.Participant) !void {
        const native = try self.nativeParticipant(root, part);
        if (native.scope.command != null or !std.meta.eql(native.scope.completed, @as(?scopes.Command, try part.admission.command())) or native.owner.waiting != null or native.owner.job != null) return error.LockOwnerBusy;
    }
    pub fn requireDataRetirement(self: Context, txn: anytype, ns: scopes.ledger.Namespace, root: scopes.Record, part: scopes.Data.Participant) !void {
        const native = try self.nativeParticipant(root, part);
        if (self.handoff) {
            const proof = self.data_proof.?;
            const frozen = proof.frozen orelse return error.LockOwnerBusy;
            const prepared_record = try prepared.readOperation(self.a, txn, ns.group, frozen.ref.decision.operation) orelse return error.CatalogPublicationProofPending;
            const ref = prepared_record.identity.lock_scope orelse return error.InvalidLockScope;
            if (!ref.backend.eql(root.registration.lease.backend) or !std.mem.eql(u8, &prepared_record.identity.txn_id, &part.admission.transaction)) return error.InvalidLockScope;
            var found = false;
            for (prepared_record.participants) |item| if (try item.lockReference(prepared_record.identity, self.a)) |expected| {
                if (std.meta.eql(expected, frozen.ref)) {
                    found = true;
                    break;
                }
            };
            if (!found or frozen.ref.admission.generation != part.admission.generation or !frozen.ref.admission.binding.eql(part.admission.binding)) return error.InvalidLockScope;
            const destination = proof.prepared_owner orelse return error.LockOwnerBusy;
            const expected_lease = try scopes.preparedLease(part.admission.binding.namespace, root.registration.lease.database, root.registration.lease.node, frozen.ref.decision);
            if (!std.meta.eql(destination.lease, expected_lease) or destination.state != .prepared or destination.job != null or destination.waiting != null or
                !std.meta.eql(destination.transaction, @as(?[16]u8, prepared_record.identity.txn_id)) or native.scope.transaction != null or native.owner.resources != 0 or
                !std.meta.eql(native.scope.released_transaction, @as(?scopes.Transaction, .{ .id = prepared_record.identity.txn_id, .decision = frozen.ref.decision }))) return error.LockOwnerBusy;
            return;
        }
        if (native.scope.state != .retired or native.scope.transaction != null or native.owner.state != .retired or native.owner.resources != 0 or native.owner.waiting != null or native.owner.job != null) return error.LockOwnerBusy;
        if (self.data_proof.?.frozen) |frozen| if (!frozen.retired or !std.mem.eql(u8, &frozen.ref.admission.transaction, &part.admission.transaction)) return error.LockOwnerBusy;
    }
    pub fn requireDataEnrollment(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record, spec: scopes.Data.Spec) !void {
        try self.requireOwner(value);
        const checked = self.enrollment orelse return error.Forbidden;
        if (!std.mem.eql(u8, try std.json.Stringify.valueAlloc(self.a, checked, .{}), try std.json.Stringify.valueAlloc(self.a, spec, .{}))) return error.Forbidden;
    }
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
    fn requireUnadmitted(self: Context, txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, expected: scopes.Transaction, original: prepared.Mutation) !void {
        const identity = original.operation.admit.identity;
        const session = identity.session orelse return error.InvalidLockScope;
        const ref = identity.lock_scope orelse return error.InvalidLockScope;
        if (expected.decision != null or !std.mem.eql(u8, &expected.id, &identity.txn_id) or
            !ref.backend.eql(value.registration.lease.backend) or session.connection_id == null or
            !std.mem.eql(u8, &session.connection_id.?, &value.registration.connection) or session.owner_node_id != value.registration.lease.node or
            session.standalone != value.registration.standalone or identity.database_id != value.registration.lease.database or
            !std.mem.eql(u8, &try principalDigest(self.a, original.actor.session_user), &value.registration.lease.principal)) return error.InvalidLockScope;
        if (value.state == .active and !std.meta.eql(value.command, @as(?scopes.Command, ref.command))) return error.LockOwnerChanged;
        // These point reads and the release share the accepting native writer.
        // An admitted or delivered purpose can only use its real decision.
        if (try prepared.readOperation(self.a, txn, ns.group, identity.operation_id) != null or
            try prepared.readReceipt(self.a, txn, ns.group, identity.operation_id) != null) return error.LockOwnerBusy;
        const journal: prepared.outcomes.Store(@TypeOf(txn.*)) = .{ .txn = txn, .group = ns.group, .incarnation = ns.incarnation };
        const outcome = try journal.lookup(self.a, original.admission, try prepared.commandDigest(self.a, original));
        if (outcome) |known| {
            if (known != .rejected) return error.LockOwnerBusy;
        } else {
            // Eviction alone is not a proof. The immutable old admission head
            // must permanently prevent a delayed admission of this same body.
            const head = try journal.head(self.a);
            if (head < original.admission.head or head - original.admission.head < prepared.outcomes.retained) return error.LockOwnerBusy;
        }
    }
    pub fn requireResolution(self: Context, txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, expected: scopes.Transaction) !void {
        try self.requireOwner(value);
        if (self.unadmitted) |original| try self.requireUnadmitted(txn, ns, value, expected, original);
        if (value.transaction == null and std.meta.eql(value.released_transaction, @as(?scopes.Transaction, expected))) return;
        if (!std.meta.eql(value.transaction, @as(?scopes.Transaction, expected))) return error.LockOwnerChanged;
        const decision = expected.decision orelse {
            // Active rollback needs an independently checked rejected admission.
            // Expiry cleanup may retire an unsealed transaction only AFTER its
            // command was fenced, so no future native admission can install.
            if (value.state == .active and self.unadmitted == null) return error.LockOwnerBusy;
            return;
        };
        if (try prepared.readOperation(self.a, txn, ns.group, decision.operation)) |_| {
            const found = try self.record(txn, ns, value, decision);
            if (found.phase != .resolved and found.phase != .retired) return error.LockOwnerBusy;
            return;
        }
        const receipt = try prepared.readReceipt(self.a, txn, ns.group, decision.operation) orelse return error.LockOwnerBusy;
        if (receipt.progress == .resolving or (receipt.status != .committed and receipt.status != .aborted) or !std.mem.eql(u8, &receipt.owner.admission_digest, &decision.admission) or !std.mem.eql(u8, &receipt.txn_id, &expected.id)) return error.LockOwnerBusy;
    }
    pub fn requireRetirement(self: Context, _: anytype, _: scopes.ledger.Namespace, value: scopes.Record) !void {
        try self.requireOwner(value);
        if (value.transaction != null) return error.LockOwnerBusy;
    }
    pub fn requirePreparedHandoff(self: Context, txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, from: scopes.ledger.Lease, to: scopes.ledger.Lease, id: [16]u8) !void {
        try self.requireOwner(value);
        const transaction = value.transaction orelse return error.InvalidLockScope;
        const found = try self.record(txn, ns, value, transaction.decision orelse return error.InvalidLockScope);
        if (!found.hasPreparedLocks() or (found.phase != .locking and !(found.phase == .resolved and found.decision.?.status == .aborted)) or
            !std.meta.eql(from, value.registration.lease) or !std.mem.eql(u8, &found.identity.txn_id, &id) or
            !std.meta.eql(to, try found.identity.preparedLockLease(self.a, ns.group))) return error.InvalidLockScope;
    }
    pub fn requirePreparedResolution(self: Context, txn: anytype, ns: scopes.ledger.Namespace, lease: scopes.ledger.Lease, id: [16]u8) !void {
        const found = try prepared.readOperation(self.a, txn, ns.group, self.prepared_operation orelse return error.InvalidLockScope) orelse return error.InvalidLockScope;
        if (!found.hasPreparedLocks() or found.phase != .resolved or found.identity.database_id != self.database or
            !std.mem.eql(u8, &found.identity.txn_id, &id) or !std.meta.eql(lease, try found.identity.preparedLockLease(self.a, ns.group))) return error.InvalidLockScope;
        try found.identity.session.?.requireRealm(self.standalone);
    }
};
pub fn apply(a: A, txn: anytype, group: u64, request: Request) !?outcomes.LockEffect {
    return applyWithEnrollment(a, txn, group, request, null);
}
pub fn applyWithEnrollment(a: A, txn: anytype, group: u64, request: Request, checked: ?scopes.Data.Spec) !?outcomes.LockEffect {
    try request.validate();
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = request.incarnation };
    const context: Context = .{ .a = a, .actor = request.actor, .database = request.database, .standalone = request.standalone, .maintenance = request.operation == .close_step or request.operation == .release or request.operation == .release_unadmitted or request.operation == .fence or request.operation == .data_ack or request.operation == .data_retire or request.operation == .data_handoff, .unadmitted = if (request.operation == .release_unadmitted) request.operation.release_unadmitted else null, .enrollment = checked };
    var checked_context = context;
    checked_context.handoff = request.operation == .data_handoff;
    checked_context.data_proof = switch (request.operation) {
        .data_ack => |value| value.native,
        .data_retire, .data_handoff => |value| value.native,
        else => null,
    };
    var receipt: ?scopes.ledger.Receipt = null;
    switch (request.operation) {
        .register => |value| try scopes.register(a, txn, ns, value, context),
        .begin => |command| _ = try scopes.begin(a, txn, ns, request.backend, command, context),
        .finish => |command| try scopes.finish(a, txn, ns, request.backend, command, context),
        .bind => |value| try scopes.bindTransaction(a, txn, ns, request.backend, value.command, value.transaction, context),
        .data_enroll => |value| _ = try scopes.Data.enroll(a, txn, ns, request.backend, value.command, value.transaction, value.spec, context),
        .data_ack => |value| try scopes.Data.acknowledge(a, txn, ns, request.backend, value.generation, value.command, checked_context),
        .data_retire, .data_handoff => |value| try scopes.Data.retire(a, txn, ns, request.backend, value.generation, checked_context),
        .release => |value| _ = try scopes.releaseTransaction(a, txn, ns, request.backend, value, context),
        .release_unadmitted => |original| _ = try scopes.releaseTransaction(a, txn, ns, request.backend, .{ .id = original.operation.admit.identity.txn_id }, context),
        .renew => |value| try scopes.renew(a, txn, ns, request.backend, value.expected_expiry, value.expires_at_ms, context),
        .fence => |reason| try scopes.fence(a, txn, ns, request.backend, reason, context),
        .effect => |value| receipt = try scopes.applyEffect(a, txn, ns, request.backend, value.command, value.effect, context),
        .cancel => |value| receipt = try scopes.cancelEffect(a, txn, ns, request.backend, value.command, value.effect, context),
        .step => |value| receipt = try scopes.advanceEffect(a, txn, ns, request.backend, value.command, value.effect, context),
        .close_step => _ = try scopes.closeStep(a, txn, ns, request.backend, context),
    }
    const result = receipt orelse return null;
    const effect = switch (request.operation) {
        .effect, .cancel, .step => |value| value.effect,
        else => unreachable,
    };
    return .{ .receipt = result, .wait_resource = if (result.outcome == .pending and effect.action == .acquire) (try scopes.ledger.readResourceStamp(a, txn, ns, effect.action.resource())) orelse return error.InvalidLockRecord else null };
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
    /// Bounded ingress anchor: one database lookup, no backend/work scan.
    discovery: ?[]const u8 = null,
    participant: ?union(enum) {
        generation: u64,
        route: struct { spec: scopes.Data.Spec, transaction: [16]u8 },
    } = null,
    pub fn validate(self: Query) !void {
        try self.actor.validate();
        const selectors = @as(u8, @intFromBool(self.backend != null)) + @as(u8, @intFromBool(self.work != null)) + @as(u8, @intFromBool(self.discovery != null));
        if (selectors != 1 or (self.backend != null and std.mem.allEqual(u8, &self.incarnation, 0))) return error.InvalidCatalogMutation;
        if (self.discovery) |name| {
            if (self.database != 0 or name.len == 0 or name.len > 256 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidCatalogMutation;
        }
        if (self.participant != null and (self.backend == null or self.effect != null)) return error.InvalidCatalogMutation;
        if (self.participant) |selector| switch (selector) {
            .generation => |generation| if (generation == 0) return error.InvalidCatalogMutation,
            .route => |route| {
                if (std.mem.allEqual(u8, &route.transaction, 0)) return error.InvalidCatalogMutation;
                try route.spec.validate(.{ .home = .metadata, .group = route.spec.route.metadata_group_id, .incarnation = self.incarnation });
            },
        };
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
    group: u64 = local.common_group_ids.main_metadata_group_id,
    incarnation: [16]u8,
    standalone: bool,
    head: u64,
    scope: ?scopes.Record,
    owner: ?scopes.ledger.Owner,
    next_work: ?scopes.Work,
    receipt: ?scopes.ledger.Receipt = null,
    wait_resource: ?scopes.ledger.ResourceStamp = null,
    database_id: ?u64 = null,
    participant: ?scopes.Data.Participant = null,
};

pub const Observer = struct {
    registration: scopes.Registration,
    pub fn requireObservation(self: Observer, _: anytype, lease: scopes.ledger.Lease, effect: scopes.ledger.Effect) !void {
        if (!std.meta.eql(lease, self.registration.lease) or !effect.backend.eql(lease.backend)) return error.Forbidden;
    }
};
