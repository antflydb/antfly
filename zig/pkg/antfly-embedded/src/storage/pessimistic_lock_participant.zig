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

//! Native data participation derives a transaction-local backend from the
//! metadata source and physical route. This is an identity contract, not an
//! admission certificate: the accepting service independently joins the durable
//! metadata enrollment, live source command, native route and capability floor.
const std = @import("std");
const scopes = @import("pessimistic_lock_authority.zig");
pub const Binding = @import("pessimistic_lock_resource.zig").Binding;

pub const Admission = struct {
    pub const Purpose = enum { execution, resolution };
    purpose: Purpose,
    binding: Binding,
    source: scopes.Registration,
    transaction: [16]u8,
    /// Monotone enrollment identity allocated by the metadata participant
    /// directory. Re-entering a route after savepoint retirement must not
    /// reuse the earlier data backend or its permanently fenced tombstone.
    generation: u64,
    parent_command: scopes.Command,
    /// Per-participant command serial. Global SQL commands may never touch
    /// this group, so their sequence cannot serve as the data journal serial.
    serial: u64,

    pub fn validate(self: Admission) !void {
        try self.binding.validate();
        try self.source.validate(.{ .home = .metadata, .group = self.source.group, .incarnation = self.binding.namespace.incarnation });
        try self.parent_command.validate();
        if (std.mem.allEqual(u8, &self.transaction, 0) or self.generation == 0 or self.generation == std.math.maxInt(u64) or self.serial == 0 or self.serial == std.math.maxInt(u64)) return error.InvalidLockScope;
    }
    pub fn sourceFence(self: Admission) !scopes.CommandFence {
        try self.validate();
        return .{ .group = self.source.group, .backend = self.source.lease.backend, .command = self.parent_command, .database = self.source.lease.database, .principal = self.source.lease.principal, .standalone = self.source.standalone };
    }
    pub fn command(self: Admission) !scopes.Command {
        try self.validate();
        return .{ .sequence = self.serial, .nonce = self.parent_command.nonce, .executor = self.parent_command.executor };
    }
    pub fn registration(self: Admission) !scopes.Registration {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-lock-participant/v1");
        hash.update(&self.source.lease.backend.incarnation);
        hash.update(&self.source.lease.backend.id);
        inline for (.{ self.source.group, self.source.lease.backend.fence, self.source.lease.database, self.source.lease.node, self.generation }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .little);
            hash.update(&bytes);
        }
        hash.update(&self.source.lease.principal);
        hash.update(&self.source.connection);
        hash.update(&self.source.nonce);
        hash.update(&.{@intFromBool(self.source.standalone)});
        hash.update(&try self.binding.encode());
        hash.update(&self.transaction);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        var result = self.source;
        result.group = self.binding.namespace.group;
        result.lease.backend.id = digest[0..16].*;
        // Data connection bindings live for one participant transaction. The
        // original logical connection remains metadata-owned and may visit
        // arbitrarily many tables over its lifetime without reusing tombstones.
        result.connection = std.fmt.bytesToHex(digest[0..16].*, .lower);
        result.nonce = digest[16..32].*;
        try result.validate(self.binding.namespace);
        return result;
    }
    pub fn requireRegistration(self: Admission, observed: scopes.Registration) !void {
        const expected = try self.registration();
        if (!std.meta.eql(expected, observed)) return error.LockOwnerChanged;
    }
};

/// Runs in one accepting native write transaction. Abort that transaction on
/// every error, including after an authority callback. The caller validates the
/// durable metadata enrollment and receiving route independently of this body.
pub fn admit(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, admission: Admission, authority: anytype) !scopes.Begin {
    try admission.validate();
    try admission.binding.requireIdentity(receiving);
    try authority.requireDataAdmission(txn, admission);
    try admission.binding.install(txn, receiving);
    const expected = try admission.registration();
    const ns = admission.binding.namespace;
    const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = admission, .authority = authority };
    if (try scopes.read(backing, txn, ns, expected.lease.backend)) |prior| {
        var normalized = expected;
        normalized.lease.expires_at_ms = prior.registration.lease.expires_at_ms;
        if (!std.meta.eql(prior.registration, normalized)) return error.LockOwnerChanged;
        if (expected.lease.expires_at_ms > prior.registration.lease.expires_at_ms)
            try scopes.renew(backing, txn, ns, expected.lease.backend, prior.registration.lease.expires_at_ms, expected.lease.expires_at_ms, context);
    } else try scopes.register(backing, txn, ns, expected, context);
    const result = try scopes.begin(backing, txn, ns, expected.lease.backend, try admission.command(), context);
    // A delayed completed BEGIN cannot replace the origin of a later command.
    if (result != .completed) {
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const bytes = try std.json.Stringify.valueAlloc(arena.allocator(), admission, .{});
        if (bytes.len > 4096) return error.InvalidLockRecord;
        var buffer: [160]u8 = undefined;
        try txn.put(try originKey(&buffer, admission), bytes);
    }
    return result;
}

pub const Acquisition = struct {
    admission: Admission,
    effect: scopes.ledger.Effect,
    /// null names the physical table; a row key is independently checked by
    /// native ownership validation before hashing or granting its resource.
    physical_key: ?[]const u8 = null,

    pub fn validate(self: Acquisition) !void {
        try self.admission.validate();
        if (self.admission.purpose != .execution) return error.InvalidLockEffect;
        try self.effect.validate();
        const registration = try self.admission.registration();
        if (!self.effect.backend.eql(registration.lease.backend)) return error.InvalidLockOwner;
        const request = switch (self.effect.action) {
            .acquire, .try_acquire => |value| value,
            else => return error.InvalidLockEffect,
        };
        if (request.scope != .transaction or !std.mem.eql(u8, &request.scope.transaction.id, &self.admission.transaction)) return error.InvalidLockScope;
        const digest = if (self.physical_key) |key| blk: {
            if (request.mode.class() != .row) return error.InvalidLockClass;
            break :blk try self.admission.binding.row(key);
        } else blk: {
            if (request.mode.class() != .table) return error.InvalidLockClass;
            break :blk try self.admission.binding.table();
        };
        if (!std.mem.eql(u8, &digest, &request.resource)) return error.InvalidLockResource;
    }
};

pub fn acquire(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, request: Acquisition, authority: anytype) !scopes.ledger.Receipt {
    try request.validate();
    try request.admission.binding.requireIdentity(receiving);
    try authority.requireDataResource(txn, request.admission, request.physical_key);
    // Historical native receipts are observations, not a renewed grant. Only
    // pending/new effects require the still-live accepting command below.
    if (try observe(backing, txn, receiving, request, authority)) |receipt| if (receipt.outcome != .pending) return receipt;
    try requireOrigin(backing, txn, request.admission);
    const ns = request.admission.binding.namespace;
    const registration = try request.admission.registration();
    const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = request.admission, .authority = authority };
    try scopes.bindTransaction(backing, txn, ns, registration.lease.backend, try request.admission.command(), request.admission.transaction, context);
    return scopes.ledger.apply(backing, txn, ns, request.effect, context);
}

/// An exact retained outcome may be observed after FINISH or retirement. It
/// proves the historical effect only; callers must retain their source permit
/// and current participant command before using a row or executing SQL.
pub fn observe(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, request: Acquisition, authority: anytype) !?scopes.ledger.Receipt {
    try request.validate();
    try request.admission.binding.requireIdentity(receiving);
    const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = request.admission, .authority = authority };
    return scopes.ledger.observe(backing, txn, request.admission.binding.namespace, request.effect, context);
}

pub fn finish(backing: std.mem.Allocator, txn: anytype, admission: Admission, authority: anytype) !void {
    try admission.validate();
    try requireOrigin(backing, txn, admission);
    const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = admission, .authority = authority };
    try scopes.finish(backing, txn, admission.binding.namespace, (try admission.registration()).lease.backend, try admission.command(), context);
}

/// Savepoint controls use the existing ledger job and command receipt. A
/// response loss resumes the exact effect; it never creates a second control.
/// Root rollback and prepared handoff require independent resolution authority.
pub const SavepointControl = struct {
    admission: Admission,
    effect: scopes.ledger.Effect,
    failed_root: bool = false,

    pub fn validate(self: SavepointControl) !void {
        try self.admission.validate();
        try self.effect.validate();
        if (self.admission.purpose != .execution or !self.effect.backend.eql((try self.admission.registration()).lease.backend) or self.effect.action != .control) return error.InvalidLockEffect;
        if (self.failed_root and (self.effect.action.control != .rollback or self.effect.action.control.rollback.first_aborted != 0)) return error.InvalidLockEffect;
        switch (self.effect.action.control) {
            .rollback => |value| if ((value.first_aborted == 0 and !self.failed_root) or !std.mem.eql(u8, &value.transaction, &self.admission.transaction)) return error.InvalidLockScope,
            .release_savepoint => |value| if (!std.mem.eql(u8, &value.transaction, &self.admission.transaction)) return error.InvalidLockScope,
            else => return error.InvalidLockEffect,
        }
    }
};

pub fn controlStep(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, request: SavepointControl, authority: anytype) !scopes.ledger.Receipt {
    try request.validate();
    try request.admission.binding.requireIdentity(receiving);
    try authority.requireDataControl(txn, request.admission, request.effect);
    try requireOrigin(backing, txn, request.admission);
    const ns = request.admission.binding.namespace;
    const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = request.admission, .authority = authority, .failed_root = request.failed_root };
    if (try scopes.ledger.observe(backing, txn, ns, request.effect, context)) |receipt| {
        if (receipt.outcome != .pending) return receipt;
        return scopes.ledger.step(backing, txn, ns, request.effect, context);
    }
    return scopes.ledger.apply(backing, txn, ns, request.effect, context);
}

/// Cancels only the immutable queued acquisition. Finished or fenced commands
/// are cleaned by retirement, whose independent authority cannot execute SQL.
pub fn cancel(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, request: Acquisition, authority: anytype) !scopes.ledger.Receipt {
    try request.validate();
    if (request.effect.action != .acquire) return error.InvalidLockEffect;
    try request.admission.binding.requireIdentity(receiving);
    try authority.requireDataResource(txn, request.admission, request.physical_key);
    try requireOrigin(backing, txn, request.admission);
    const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = request.admission, .authority = authority };
    return scopes.ledger.cancel(backing, txn, request.admission.binding.namespace, request.effect, context);
}

/// Cancel only an independently observed waiter of this live native command.
/// Recovery need not retain or reconstruct the original row key to cancel it;
/// this entry can never acquire a resource or change an absent effect.
pub fn cancelWaiting(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, admission: Admission, effect: scopes.ledger.Effect, authority: anytype) !void {
    try admission.validate();
    try effect.validate();
    try admission.binding.requireIdentity(receiving);
    try authority.requireDataAdmission(txn, admission);
    try requireOrigin(backing, txn, admission);
    const backend = (try admission.registration()).lease.backend;
    if (!effect.backend.eql(backend) or effect.action != .acquire) return error.InvalidLockEffect;
    const owner = try scopes.ledger.readOwner(backing, txn, admission.binding.namespace, backend) orelse return error.LockOwnerNotRegistered;
    if (owner.waiting) |waiting| {
        if (!std.meta.eql(waiting, effect)) return error.LockEffectChanged;
        const context: Context(@TypeOf(authority)) = .{ .backing = backing, .admission = admission, .authority = authority };
        _ = try scopes.ledger.cancel(backing, txn, admission.binding.namespace, effect, context);
    } else if (owner.next_effect <= effect.sequence) return error.LockEffectChanged;
}

/// Fencing an absent participant installs its permanently fenced identity in
/// the same transaction. A delayed first BEGIN then cannot resurrect execution.
/// This requires a durable metadata enrollment, even when no data RPC arrived.
pub fn fence(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, admission: Admission, authority: anytype) !void {
    try admission.validate();
    try admission.binding.requireIdentity(receiving);
    try authority.requireDataFence(txn, admission);
    try admission.binding.install(txn, receiving);
    const registration = try admission.registration();
    const context: FenceContext(@TypeOf(authority)) = .{ .admission = admission, .authority = authority };
    if (try scopes.read(backing, txn, admission.binding.namespace, registration.lease.backend)) |previous| {
        var normalized = registration;
        normalized.lease.expires_at_ms = previous.registration.lease.expires_at_ms;
        if (!std.meta.eql(normalized, previous.registration)) return error.LockOwnerChanged;
    } else try scopes.register(backing, txn, admission.binding.namespace, registration, context);
    try scopes.fence(backing, txn, admission.binding.namespace, registration.lease.backend, .close, context);
}
fn FenceContext(comptime Authority: type) type {
    return struct {
        admission: Admission,
        authority: Authority,
        pub fn requireRegistration(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Registration) !void {
            if (!std.meta.eql(ns, self.admission.binding.namespace)) return error.InvalidLockScope;
            var normalized = try self.admission.registration();
            normalized.lease.expires_at_ms = value.lease.expires_at_ms;
            if (!std.meta.eql(normalized, value)) return error.LockOwnerChanged;
            try self.authority.requireDataFence(txn, self.admission);
        }
        pub fn requireFence(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, _: scopes.Fence) !void {
            if (!std.meta.eql(ns, self.admission.binding.namespace) or !value.registration.lease.backend.eql((try self.admission.registration()).lease.backend)) return error.InvalidLockScope;
            try self.authority.requireDataFence(txn, self.admission);
        }
    };
}

/// One bounded custody quantum under the existing native 2PC owner. Recovery
/// may finish this after source fencing; it may never evaluate SQL or acquire
/// another resource using this resolution-only descriptor.
pub fn prepareStep(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, admission: Admission, decision: scopes.DecisionRef, authority: anytype) !bool {
    try admission.validate();
    if (admission.purpose != .resolution) return error.InvalidLockEffect;
    try admission.binding.requireIdentity(receiving);
    const registration = try admission.registration();
    const expected: scopes.Transaction = .{ .id = admission.transaction, .decision = decision };
    const context: PreparedContext(@TypeOf(authority)) = .{ .admission = admission, .decision = decision, .authority = authority };
    try scopes.adoptPreparedAdmission(backing, txn, admission.binding.namespace, registration.lease.backend, try admission.command(), expected, context);
    const destination = try scopes.preparedLease(admission.binding.namespace, admission.source.lease.database, admission.source.lease.node, decision);
    return scopes.prepareStep(backing, txn, admission.binding.namespace, registration.lease.backend, try admission.command(), expected, destination, context);
}

fn PreparedContext(comptime Authority: type) type {
    return struct {
        admission: Admission,
        decision: scopes.DecisionRef,
        authority: Authority,
        pub fn requirePreparedAdmission(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, command_value: scopes.Command, expected: scopes.Transaction) !void {
            if (!std.meta.eql(ns, self.admission.binding.namespace) or !std.meta.eql(command_value, try self.admission.command()) or
                !std.meta.eql(expected, scopes.Transaction{ .id = self.admission.transaction, .decision = self.decision })) return error.InvalidLockScope;
            var registration = try self.admission.registration();
            registration.lease.expires_at_ms = value.registration.lease.expires_at_ms;
            if (!std.meta.eql(registration, value.registration)) return error.LockOwnerChanged;
            try self.authority.requireDataPreparedAdmission(txn, self.admission, self.decision, value);
        }
        pub fn requirePreparedHandoff(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, from: scopes.ledger.Lease, to: scopes.ledger.Lease, transaction: [16]u8) !void {
            try self.requirePreparedAdmission(txn, ns, value, try self.admission.command(), .{ .id = transaction, .decision = self.decision });
            if (!std.meta.eql(from, value.registration.lease) or !std.meta.eql(to, try scopes.preparedLease(ns, from.database, from.node, self.decision))) return error.InvalidLockOwner;
        }
    };
}

/// Resolution depends on the native decision owner, never a surviving frontend
/// connection. The source record is deliberately absent from this authority.
pub fn resolvePreparedStep(backing: std.mem.Allocator, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, binding: Binding, decision: scopes.DecisionRef, transaction: [16]u8, authority: anytype) !bool {
    try binding.requireIdentity(receiving);
    const backend = try scopes.preparedBackend(binding.namespace, decision);
    const owner = try scopes.ledger.readOwner(backing, txn, binding.namespace, backend) orelse return error.LockOwnerNotRegistered;
    const context: ResolutionContext(@TypeOf(authority)) = .{ .binding = binding, .decision = decision, .transaction = transaction, .authority = authority };
    return scopes.releasePreparedStep(backing, txn, binding.namespace, owner.lease, transaction, context);
}
fn ResolutionContext(comptime Authority: type) type {
    return struct {
        binding: Binding,
        decision: scopes.DecisionRef,
        transaction: [16]u8,
        authority: Authority,
        pub fn requirePreparedResolution(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, lease: scopes.ledger.Lease, transaction: [16]u8) !void {
            if (!std.meta.eql(ns, self.binding.namespace) or !std.mem.eql(u8, &transaction, &self.transaction) or
                !lease.backend.eql(try scopes.preparedBackend(ns, self.decision)) or !std.mem.eql(u8, &lease.principal, &self.decision.admission)) return error.InvalidLockScope;
            try self.authority.requirePreparedDataResolution(txn, self.binding, self.decision, transaction, lease);
        }
    };
}

fn originKey(buffer: []u8, admission: Admission) ![]const u8 {
    const ns = admission.binding.namespace;
    const backend = (try admission.registration()).lease.backend;
    return std.fmt.bufPrint(buffer, "\x00\x00__lock_origin__:{x:0>16}:{s}:{s}:{x:0>16}", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence });
}
fn requireOrigin(backing: std.mem.Allocator, txn: anytype, admission: Admission) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    var buffer: [160]u8 = undefined;
    const bytes = txn.get(try originKey(&buffer, admission)) catch |err| switch (err) {
        error.NotFound => return error.LockOwnerNotRegistered,
        else => return err,
    };
    if (bytes.len > 4096) return error.InvalidLockRecord;
    const previous = try std.json.parseFromSliceLeaky(Admission, arena.allocator(), bytes, .{});
    try previous.validate();
    var normalized = admission;
    // Heartbeats may advance the authoritative native lease without changing
    // the command identity or permitting replay of an earlier execution.
    normalized.source.lease.expires_at_ms = previous.source.lease.expires_at_ms;
    if (!std.meta.eql(previous, normalized)) return error.LockEffectChanged;
}
fn Context(comptime Authority: type) type {
    return struct {
        backing: std.mem.Allocator,
        admission: Admission,
        authority: Authority,
        failed_root: bool = false,
        fn registration(self: @This(), ns: scopes.ledger.Namespace, observed: scopes.Registration) !void {
            if (!std.meta.eql(ns, self.admission.binding.namespace)) return error.InvalidLockScope;
            var expected = try self.admission.registration();
            expected.lease.expires_at_ms = observed.lease.expires_at_ms;
            if (!std.meta.eql(expected, observed)) return error.LockOwnerChanged;
        }
        pub fn requireRegistration(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Registration) !void {
            try self.registration(ns, value);
            try self.authority.requireDataAdmission(txn, self.admission);
        }
        pub fn requireCommand(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, command_value: scopes.Command) !void {
            try self.registration(ns, value.registration);
            if (!std.meta.eql(command_value, try self.admission.command())) return error.LockEffectChanged;
            try self.authority.requireDataCommand(txn, self.admission, value);
        }
        pub fn requireTransaction(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, id: [16]u8) !void {
            if (!std.mem.eql(u8, &id, &self.admission.transaction)) return error.InvalidLockScope;
            try self.requireCommand(txn, ns, value, try self.admission.command());
        }
        pub fn requireRenewal(self: @This(), txn: anytype, ns: scopes.ledger.Namespace, value: scopes.Record, _: u64, expires: u64) !void {
            try self.registration(ns, value.registration);
            if (expires != self.admission.source.lease.expires_at_ms) return error.InvalidLockOwner;
            try self.authority.requireDataAdmission(txn, self.admission);
        }
        pub fn requireEffect(self: @This(), txn: anytype, lease: scopes.ledger.Lease, effect: scopes.ledger.Effect) !void {
            const ns = self.admission.binding.namespace;
            const value = try scopes.guard(self.backing, txn, ns, lease.backend, try self.admission.command(), self);
            if (!std.meta.eql(lease, value.registration.lease) or !effect.backend.eql(lease.backend)) return error.LockOwnerChanged;
            if (effect.action == .control) {
                const request: SavepointControl = .{ .admission = self.admission, .effect = effect, .failed_root = self.failed_root };
                try request.validate();
                try self.authority.requireDataControl(txn, self.admission, effect);
            } else {
                if (effect.action != .acquire and effect.action != .try_acquire) return error.InvalidLockEffect;
                if (effect.action.class() == .advisory) return error.InvalidLockClass;
                try self.authority.requireDataEffect(txn, self.admission, effect);
            }
        }
        pub fn requireObservation(self: @This(), txn: anytype, lease: scopes.ledger.Lease, effect: scopes.ledger.Effect) !void {
            const ns = self.admission.binding.namespace;
            const value = try scopes.read(self.backing, txn, ns, lease.backend) orelse return error.LockOwnerNotRegistered;
            try self.registration(ns, value.registration);
            if (!std.meta.eql(lease, value.registration.lease) or !effect.backend.eql(lease.backend)) return error.LockOwnerChanged;
            try self.authority.requireDataObservation(txn, self.admission, effect);
        }
        pub fn requirePrepare(_: @This(), _: anytype, _: scopes.ledger.Lease, _: scopes.ledger.Lease, _: [16]u8) !void {
            // Prepared handoff requires the existing native decision owner;
            // an acquisition envelope cannot turn itself into that authority.
            return error.InvalidLockEffect;
        }
    };
}

test "native data participation keeps identity through renewal and command gaps but separates transactions routes and source realms" {
    const admission: Admission = .{
        .purpose = .execution,
        .binding = .{ .namespace = .{ .home = .data, .group = 901, .incarnation = @splat(1) }, .identity = .{ .table_id = 7, .shard_id = 901, .range_id = 903 } },
        .source = .{ .group = 2188, .lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(3), .fence = 1 }, .database = 1, .node = 9, .principal = @splat(4), .expires_at_ms = 1000 }, .connection = @splat('a'), .nonce = @splat(5), .standalone = false },
        .transaction = @splat(6),
        .generation = 1,
        .parent_command = .{ .sequence = 42, .nonce = @splat(7), .executor = @splat(8) },
        .serial = 1,
    };
    const registration = try admission.registration();
    try std.testing.expectEqual(@as(u64, 901), registration.group);
    try std.testing.expectEqual(@as(u64, 2188), (try admission.sourceFence()).group);
    try std.testing.expectEqual(@as(u64, 1), (try admission.command()).sequence);
    try std.testing.expect(!registration.lease.backend.eql(admission.source.lease.backend));
    try std.testing.expect(!std.mem.eql(u8, &registration.connection, &admission.source.connection));
    var next = admission;
    next.parent_command.sequence = 57;
    next.parent_command.nonce = @splat(9);
    next.parent_command.executor = @splat(10);
    next.serial = 2;
    try std.testing.expectEqualDeep(registration, try next.registration());
    try std.testing.expectEqual(@as(u64, 2), (try next.command()).sequence);
    try std.testing.expectEqual(next.parent_command.executor, (try next.command()).executor);
    next.source.lease.expires_at_ms += 100;
    const renewed = try next.registration();
    try std.testing.expect(registration.lease.backend.eql(renewed.lease.backend));
    try std.testing.expectEqual(registration.connection, renewed.connection);
    try std.testing.expectEqual(registration.nonce, renewed.nonce);
    try std.testing.expectError(error.LockOwnerChanged, next.requireRegistration(registration));
    next = admission;
    next.purpose = .resolution;
    try std.testing.expectEqualDeep(registration, try next.registration());
    next = admission;
    next.transaction[0] += 1;
    try std.testing.expect(!registration.lease.backend.eql((try next.registration()).lease.backend));
    next = admission;
    next.binding.namespace.group += 1;
    next.binding.identity.shard_id += 1;
    try std.testing.expect(!registration.lease.backend.eql((try next.registration()).lease.backend));
    next = admission;
    next.generation += 1;
    try std.testing.expect(!registration.lease.backend.eql((try next.registration()).lease.backend));
    next = admission;
    next.source.group += 1;
    try std.testing.expect(!registration.lease.backend.eql((try next.registration()).lease.backend));
    next = admission;
    next.source.lease.backend.fence += 1;
    try std.testing.expect(!registration.lease.backend.eql((try next.registration()).lease.backend));
    next = admission;
    next.binding.namespace.incarnation[0] += 1;
    try std.testing.expectError(error.InvalidLockOwner, next.registration());
    next = admission;
    next.serial = 0;
    try std.testing.expectError(error.InvalidLockScope, next.command());
}
