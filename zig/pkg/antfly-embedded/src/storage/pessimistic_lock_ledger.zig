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

//! Transaction-local lock effects, aggregate counts and bounded exact receipts.
//! The caller owns the native writer transaction and aborts it on ANY error.
//! Context must independently authorize both registration and every effect;
//! this ledger neither trusts a SQL name nor grants connection authority.
const std = @import("std");
const locks = @import("pessimistic_lock_contract.zig");
const A = std.mem.Allocator;
pub const receipt_window = 64;
pub const max_resources_per_backend = 1024;
pub const max_waiters_per_resource = 128;
pub const max_backends_per_resource = 256;
const max_record_bytes = 8192;
pub const Namespace = struct {
    group: u64,
    incarnation: [16]u8,
    pub fn validate(self: Namespace) !void {
        if (self.group == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidLockResource;
    }
};
pub const Lease = struct {
    backend: locks.Backend,
    database: u64,
    /// Zero is the existing standalone-local SQL connection owner.
    node: u64,
    principal: [32]u8,
    expires_at_ms: u64,
    pub fn validate(self: Lease, ns: Namespace) !void {
        try ns.validate();
        try self.backend.validate();
        if (!std.mem.eql(u8, &self.backend.incarnation, &ns.incarnation) or self.database == 0 or self.expires_at_ms == 0 or std.mem.allEqual(u8, &self.principal, 0)) return error.InvalidLockOwner;
    }
};
pub const Control = union(enum) {
    unlock_all,
    rollback: struct { transaction: [16]u8, first_aborted: u64 },
    release_savepoint: struct { transaction: [16]u8, first_child: u64, parent: u64 },
    close,
    prepare: struct { transaction: [16]u8, destination: Lease },
};
pub const Job = struct { effect: Effect, after: ?[32]u8 = null, visited: u32 = 0, total: u32 };
pub const Incoming = struct { source: locks.Backend, sequence: u64, digest: [32]u8 };
pub const Owner = struct {
    lease: Lease,
    next_effect: u64 = 1,
    resources: u32 = 0,
    mixed_resources: u32 = 0,
    transaction_resources: u32 = 0,
    state: enum { active, incoming, prepared, retired } = .active,
    job: ?Job = null,
    waiting: ?Effect = null,
    transaction: ?[16]u8 = null,
    incoming: ?Incoming = null,
    pub fn validate(self: Owner, ns: Namespace) !void {
        try self.lease.validate(ns);
        if (self.next_effect == 0 or self.resources > max_resources_per_backend or self.transaction_resources > self.resources or self.mixed_resources > self.transaction_resources) return error.InvalidLockRecord;
        if ((self.state == .incoming) != (self.incoming != null) or (self.state == .retired and (self.resources != 0 or self.job != null))) return error.InvalidLockRecord;
        if (self.job) |job| {
            try job.effect.validate();
            if (job.effect.sequence != self.next_effect or !job.effect.backend.eql(self.lease.backend) or job.effect.action != .control or job.total > max_resources_per_backend or job.visited > job.total or (job.after == null) != (job.visited == 0)) return error.InvalidLockRecord;
        }
        if (self.waiting) |waiting| {
            try waiting.validate();
            if (self.job != null or self.state != .active or waiting.action != .acquire or waiting.sequence != self.next_effect or !waiting.backend.eql(self.lease.backend)) return error.InvalidLockRecord;
        }
        if (self.transaction) |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidLockRecord;
        if (self.transaction_resources != 0 and self.transaction == null) return error.InvalidLockRecord;
        if (self.state == .retired and self.transaction != null) return error.InvalidLockRecord;
        if (self.incoming) |incoming| {
            try incoming.source.validate();
            if (incoming.sequence == 0 or std.mem.allEqual(u8, &incoming.digest, 0)) return error.InvalidLockRecord;
        }
    }
};
pub const Resource = struct {
    digest: [32]u8,
    class: locks.Class,
    generation: [32]u8,
    revision: u64 = 1,
    backends: u32 = 0,
    counts: [locks.max_modes]u32 = @splat(0),
    first: ?locks.Backend = null,
    last: ?locks.Backend = null,
    waiters: u32 = 0,
    wait_counts: [locks.max_modes]u32 = @splat(0),
    pub fn validate(self: Resource) !void {
        if (self.revision == 0 or std.mem.allEqual(u8, &self.digest, 0) or std.mem.allEqual(u8, &self.generation, 0)) return error.InvalidLockRecord;
        if (self.backends > max_backends_per_resource or self.waiters > max_waiters_per_resource or (self.first == null) != (self.last == null) or (self.waiters == 0) != (self.first == null)) return error.InvalidLockRecord;
        var total: u32 = 0;
        for (self.wait_counts, 0..) |count, index| {
            if (index >= locks.modeCount(self.class) and count != 0) return error.InvalidLockRecord;
            total = std.math.add(u32, total, count) catch return error.InvalidLockRecord;
        }
        if (total != self.waiters) return error.InvalidLockRecord;
        var occupied = false;
        for (self.counts, 0..) |count, index| {
            if ((index >= locks.modeCount(self.class) and count != 0) or count > self.backends) return error.InvalidLockRecord;
            occupied = occupied or count != 0;
        }
        if (occupied != (self.backends != 0)) return error.InvalidLockRecord;
    }
};
pub const Acquisition = struct { resource: [32]u8, mode: locks.Mode, scope: locks.Scope };
pub const Waiter = struct { effect: Effect, previous: ?locks.Backend = null, next: ?locks.Backend = null };
pub const Action = union(enum) {
    control: Control,
    acquire: Acquisition,
    try_acquire: Acquisition,
    unlock: struct { resource: [32]u8, mode: locks.AdvisoryMode },
    pub fn resource(self: Action) [32]u8 {
        return switch (self) {
            .control => unreachable,
            inline else => |value| value.resource,
        };
    }
    pub fn class(self: Action) locks.Class {
        return switch (self) {
            .try_acquire, .acquire => |value| value.mode.class(),
            .unlock => .advisory,
            .control => unreachable,
        };
    }
};
pub const Effect = struct {
    backend: locks.Backend,
    sequence: u64,
    nonce: [16]u8,
    action: Action,
    pub fn validate(self: Effect) !void {
        try self.backend.validate();
        if (self.sequence == 0 or self.sequence == std.math.maxInt(u64) or std.mem.allEqual(u8, &self.nonce, 0) or (self.action != .control and std.mem.allEqual(u8, &self.action.resource(), 0))) return error.InvalidLockEffect;
        if (self.action == .control) switch (self.action.control) {
            .rollback => |value| if (std.mem.allEqual(u8, &value.transaction, 0)) {
                return error.InvalidLockScope;
            },
            .release_savepoint => |value| if (std.mem.allEqual(u8, &value.transaction, 0) or value.parent >= value.first_child) {
                return error.InvalidLockScope;
            },
            .prepare => |value| {
                if (std.mem.allEqual(u8, &value.transaction, 0) or std.mem.eql(u8, &self.backend.id, &value.destination.backend.id)) return error.InvalidLockOwner;
            },
            else => {},
        };
    }
    pub fn digest(self: Effect, ns: Namespace) ![32]u8 {
        try self.validate();
        try ns.validate();
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update("antfly/pessimistic-lock-effect/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hasher, &.{});
        try std.json.Stringify.value(.{ .namespace = ns, .effect = self }, .{}, &writer.writer);
        var result: [32]u8 = undefined;
        writer.hasher.final(&result);
        return result;
    }
};
pub const Outcome = enum { granted, contended, unlocked, not_held, pending, completed, cancelled, deadlocked };
pub const Receipt = struct {
    sequence: u64,
    nonce: [16]u8,
    digest: [32]u8,
    outcome: Outcome,
    pub fn requireFor(self: Receipt, ns: Namespace, effect: Effect) !void {
        if (self.sequence != effect.sequence or !std.mem.eql(u8, &self.nonce, &effect.nonce) or !std.mem.eql(u8, &self.digest, &try effect.digest(ns))) return error.LockEffectChanged;
    }
};

const Lane = enum { owner, resource, holder, member, receipt, waiter };
fn key(buffer: []u8, ns: Namespace, lane: Lane, backend: locks.Backend, resource: [32]u8, sequence: u64) ![]const u8 {
    try ns.validate();
    var base: [128]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&base, "\x00\x00__locks__:{x:0>16}:{s}:", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower) });
    return switch (lane) {
        .resource => std.fmt.bufPrint(buffer, "{s}resource:{s}:head", .{ prefix, std.fmt.bytesToHex(resource, .lower) }),
        .waiter => std.fmt.bufPrint(buffer, "{s}resource:{s}:wait:{s}:{x:0>16}", .{ prefix, std.fmt.bytesToHex(resource, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence }),
        .holder => std.fmt.bufPrint(buffer, "{s}resource:{s}:backend:{s}:{x:0>16}", .{ prefix, std.fmt.bytesToHex(resource, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence }),
        .owner => std.fmt.bufPrint(buffer, "{s}backend:{s}:{x:0>16}:head", .{ prefix, std.fmt.bytesToHex(backend.id, .lower), backend.fence }),
        .member => std.fmt.bufPrint(buffer, "{s}backend:{s}:{x:0>16}:member:{s}", .{ prefix, std.fmt.bytesToHex(backend.id, .lower), backend.fence, std.fmt.bytesToHex(resource, .lower) }),
        .receipt => std.fmt.bufPrint(buffer, "{s}backend:{s}:{x:0>16}:receipt:{x:0>16}", .{ prefix, std.fmt.bytesToHex(backend.id, .lower), backend.fence, sequence }),
    };
}
fn memberKey(buffer: []u8, ns: Namespace, backend: locks.Backend, transaction: ?[16]u8, resource: [32]u8) ![]const u8 {
    const id = transaction orelse return key(buffer, ns, .member, backend, resource, 0);
    if (std.mem.allEqual(u8, &id, 0)) return error.InvalidLockScope;
    var base: [256]u8 = undefined;
    const global = try key(&base, ns, .member, backend, resource, 0);
    // The global suffix is "member:" followed by a fixed 64-digit hash.
    return std.fmt.bufPrint(buffer, "{s}transaction:{s}:member:{s}", .{ global[0 .. global.len - 71], std.fmt.bytesToHex(id, .lower), std.fmt.bytesToHex(resource, .lower) });
}
fn requireMember(txn: anytype, ns: Namespace, backend: locks.Backend, transaction: ?[16]u8, resource: [32]u8, class: locks.Class) !void {
    var buffer: [256]u8 = undefined;
    const value = (try optional(txn, try memberKey(&buffer, ns, backend, transaction, resource))) orelse return error.InvalidLockRecord;
    if (value.len != 1 or value[0] != @backingInt(class)) return error.InvalidLockRecord;
}
fn controlTransaction(control: Control) ?[16]u8 {
    return switch (control) {
        .prepare => |value| value.transaction,
        .rollback => |value| value.transaction,
        .release_savepoint => |value| value.transaction,
        else => null,
    };
}
fn optional(txn: anytype, address: []const u8) !?[]const u8 {
    return txn.get(address) catch |err| if (err == error.NotFound) null else err;
}
fn decode(comptime T: type, a: A, bytes: []const u8) !T {
    if (bytes.len <= 32 or bytes.len > max_record_bytes) return error.InvalidLockRecord;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidLockRecord;
    const value = std.json.parseFromSliceLeaky(T, a, bytes[0 .. bytes.len - 32], .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidLockRecord;
    const canonical = try std.json.Stringify.valueAlloc(a, value, .{});
    if (!std.mem.eql(u8, canonical, bytes[0 .. bytes.len - 32])) return error.InvalidLockRecord;
    return value;
}
fn encode(a: A, value: anytype) ![]u8 {
    const body = try std.json.Stringify.valueAlloc(a, value, .{});
    if (body.len > max_record_bytes - 32) return error.LockLimitExceeded;
    const result = try a.alloc(u8, body.len + 32);
    @memcpy(result[0..body.len], body);
    var hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(body, &hash, .{});
    @memcpy(result[body.len..], &hash);
    return result;
}
pub fn readOwner(backing: A, txn: anytype, ns: Namespace, backend: locks.Backend) !?Owner {
    try ns.validate();
    try backend.validate();
    if (!std.mem.eql(u8, &backend.incarnation, &ns.incarnation)) return error.InvalidLockOwner;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(txn, try key(&buffer, ns, .owner, backend, @splat(0), 0))) orelse return null;
    const value = try decode(Owner, a, bytes);
    try value.validate(ns);
    if (!value.lease.backend.eql(backend)) return error.InvalidLockRecord;
    return value;
}
pub fn register(backing: A, txn: anytype, ns: Namespace, lease: Lease, context: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try lease.validate(ns);
    try context.requireLease(txn, lease);
    if (try readOwner(a, txn, ns, lease.backend)) |value| {
        if (!std.meta.eql(value.lease, lease)) return error.LockOwnerChanged;
        if (value.state != .active or value.job != null or value.waiting != null) return error.LockOwnerBusy;
        return;
    }
    const owner: Owner = .{ .lease = lease };
    const encoded = try encode(a, owner);
    var buffer: [256]u8 = undefined;
    try txn.put(try key(&buffer, ns, .owner, lease.backend, @splat(0), 0), encoded);
}
pub fn observe(backing: A, txn: anytype, ns: Namespace, effect: Effect, context: anytype) !?Receipt {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try effect.validate();
    const owner = (try readOwner(a, txn, ns, effect.backend)) orelse return null;
    try context.requireObservation(txn, owner.lease, effect);
    if (owner.waiting) |waiting| if (effect.sequence == waiting.sequence) {
        if (!std.mem.eql(u8, &try effect.digest(ns), &try waiting.digest(ns))) return error.LockEffectChanged;
        return .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = .pending };
    };
    if (owner.job) |job| if (effect.sequence == job.effect.sequence) {
        if (!std.mem.eql(u8, &try effect.digest(ns), &try job.effect.digest(ns))) return error.LockEffectChanged;
        return .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = .pending };
    };
    var buffer: [256]u8 = undefined;
    const raw = (try optional(txn, try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence))) orelse {
        if (effect.sequence < owner.next_effect and owner.next_effect - effect.sequence <= receipt_window) return error.InvalidLockRecord;
        return null;
    };
    const receipt = try decode(Receipt, a, raw);
    try receipt.requireFor(ns, effect);
    return receipt;
}
fn mixed(value: locks.Grant) bool {
    if (value.transaction == null) return false;
    for (value.session) |count| if (count != 0) return true;
    return false;
}

/// A false try-acquire is also a durable outcome. Losing that reply cannot
/// turn its replay into a later successful acquisition or another refcount.
pub fn apply(backing: A, txn: anytype, ns: Namespace, effect: Effect, context: anytype) !Receipt {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try effect.validate();
    var owner = (try readOwner(a, txn, ns, effect.backend)) orelse return error.LockOwnerNotRegistered;
    try context.requireEffect(txn, owner.lease, effect);
    if (try observe(a, txn, ns, effect, context)) |receipt| {
        if (receipt.outcome != .pending or owner.job != null) return receipt;
    }
    if (effect.sequence < owner.next_effect) return error.LockEffectExpired;
    if (owner.job != null or (owner.waiting != null and effect.action != .acquire)) return error.LockOwnerBusy;
    if (effect.sequence != owner.next_effect) return error.LockEffectOutOfOrder;
    if (owner.state == .incoming or owner.state == .retired) return error.LockOwnerBusy;
    if (owner.state == .prepared and (effect.action != .control or effect.action.control != .close)) return error.LockOwnerBusy;
    if (effect.action == .control) return beginControl(a, txn, ns, &owner, effect, context);
    const resource_id = effect.action.resource();
    var buffer: [256]u8 = undefined;
    const raw_head = try optional(txn, try key(&buffer, ns, .resource, effect.backend, resource_id, 0));
    var head: Resource = if (raw_head) |bytes| try decode(Resource, a, bytes) else .{ .digest = resource_id, .class = effect.action.class(), .generation = try effect.digest(ns) };
    try head.validate();
    if (!std.mem.eql(u8, &head.digest, &resource_id) or head.class != effect.action.class()) return error.InvalidLockRecord;
    const raw_grant = try optional(txn, try key(&buffer, ns, .holder, effect.backend, resource_id, 0));
    var grant: locks.Grant = if (raw_grant) |bytes| try locks.Grant.decode(bytes) else .{ .backend = effect.backend, .resource = resource_id, .class = head.class };
    if (!grant.backend.eql(effect.backend) or !std.mem.eql(u8, &grant.resource, &resource_id) or grant.class != head.class) return error.InvalidLockRecord;
    const before = grant.mask();
    if (raw_head != null and head.backends == 0 and head.waiters == 0) return error.InvalidLockRecord;
    const member = try optional(txn, try key(&buffer, ns, .member, effect.backend, resource_id, 0));
    if (raw_grant != null) {
        if (before == 0 or raw_head == null or owner.resources == 0 or member == null or member.?.len != 1 or member.?[0] != @backingInt(head.class)) return error.InvalidLockRecord;
        for (head.counts, 0..) |count, index| if (before & (@as(u8, 1) << @as(u3, @intCast(index))) != 0 and count == 0) return error.InvalidLockRecord;
    } else if (member != null) return error.InvalidLockRecord;
    const was_mixed = mixed(grant);
    const was_transaction = grant.transaction;
    if (was_transaction) |id| {
        if (owner.transaction_resources == 0 or !std.meta.eql(owner.transaction, was_transaction)) return error.InvalidLockRecord;
        try requireMember(txn, ns, effect.backend, id, resource_id, head.class);
    }
    var aggregate: locks.Aggregate = .{ .class = head.class, .owners = head.counts };
    var changed = false;
    var queue_changed = false;
    const outcome: Outcome = switch (effect.action) {
        .control => unreachable,
        .try_acquire, .acquire => |request| blk: {
            if (request.scope == .transaction) if (owner.transaction) |current| if (!std.mem.eql(u8, &current, &request.scope.transaction.id)) return error.LockOwnerChanged;
            if (raw_grant == null and owner.resources == max_resources_per_backend) return error.LockLimitExceeded;
            const admission = try queueAdmission(a, txn, ns, head, effect, if (raw_grant != null) grant else null);
            if (try aggregate.blocked(request.mode, .{ .backend = effect.backend, .resource = resource_id }, if (raw_grant != null) grant else null) or admission.blocked) {
                if (effect.action == .try_acquire) break :blk .contended;
                if (owner.waiting == null) {
                    try enqueue(a, txn, ns, &head, effect, admission.before);
                    owner.waiting = effect;
                    try owner.validate(ns);
                    const bytes = try encode(a, owner);
                    try txn.put(try key(&buffer, ns, .owner, effect.backend, @splat(0), 0), bytes);
                    const deadlocked = try resolveDeadlock(a, txn, ns, effect.backend);
                    if (deadlocked) return cancelWithOutcome(a, txn, ns, effect, context, .deadlocked);
                }
                return .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = .pending };
            }
            if (owner.waiting != null) {
                try dequeue(a, txn, ns, &head, effect);
                owner.waiting = null;
                queue_changed = true;
            }
            if (request.scope == .transaction) {
                const id = request.scope.transaction.id;
                if (owner.transaction) |current| if (!std.mem.eql(u8, &current, &id)) return error.LockOwnerChanged;
                owner.transaction = id;
            }
            changed = try grant.acquire(request.mode, request.scope);
            break :blk .granted;
        },
        .unlock => |request| blk: {
            changed = try grant.unlock(request.mode);
            break :blk if (changed) .unlocked else .not_held;
        },
    };
    const after = grant.mask();
    if (was_transaction == null and grant.transaction != null) owner.transaction_resources = std.math.add(u32, owner.transaction_resources, 1) catch return error.LockLimitExceeded;
    if (changed) {
        try aggregate.transition(before, after);
        head.counts = aggregate.owners;
        head.revision = std.math.add(u64, head.revision, 1) catch return error.LockLimitExceeded;
        if (before == 0 and after != 0) {
            if (owner.resources == max_resources_per_backend) return error.LockLimitExceeded;
            owner.resources += 1;
            if (head.backends == max_backends_per_resource) return error.LockLimitExceeded;
            head.backends += 1;
        }
        if (before != 0 and after == 0) {
            owner.resources = std.math.sub(u32, owner.resources, 1) catch return error.InvalidLockRecord;
            head.backends = std.math.sub(u32, head.backends, 1) catch return error.InvalidLockRecord;
        }
        const is_mixed = mixed(grant);
        if (was_mixed != is_mixed) {
            if (is_mixed) owner.mixed_resources = std.math.add(u32, owner.mixed_resources, 1) catch return error.LockLimitExceeded else owner.mixed_resources = std.math.sub(u32, owner.mixed_resources, 1) catch return error.InvalidLockRecord;
        }
        try head.validate();
    }
    owner.next_effect += 1;
    try owner.validate(ns);
    const receipt: Receipt = .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = outcome };
    // Queue edits and grant/receipt edits share this accepting transaction.
    // Any later failure must abort it, including allocator/encoder failures.
    const owner_bytes = try encode(a, owner);
    const receipt_bytes = try encode(a, receipt);
    const head_bytes = if ((changed or queue_changed) and (head.backends != 0 or head.waiters != 0)) try encode(a, head) else null;
    const grant_bytes = if (changed and after != 0) try grant.encode() else null;
    if (changed) {
        if (grant_bytes) |*bytes| {
            try txn.put(try key(&buffer, ns, .holder, effect.backend, resource_id, 0), bytes);
            try txn.put(try key(&buffer, ns, .member, effect.backend, resource_id, 0), &.{@backingInt(head.class)});
            if (was_transaction == null) if (grant.transaction) |id| try txn.put(try memberKey(&buffer, ns, effect.backend, id, resource_id), &.{@backingInt(head.class)});
        } else {
            try txn.delete(try key(&buffer, ns, .holder, effect.backend, resource_id, 0));
            try txn.delete(try key(&buffer, ns, .member, effect.backend, resource_id, 0));
        }
    }
    if (changed or queue_changed) {
        if (head_bytes) |bytes| try txn.put(try key(&buffer, ns, .resource, effect.backend, resource_id, 0), bytes) else try txn.delete(try key(&buffer, ns, .resource, effect.backend, resource_id, 0));
    }
    try txn.put(try key(&buffer, ns, .owner, effect.backend, @splat(0), 0), owner_bytes);
    try txn.put(try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence), receipt_bytes);
    if (effect.sequence > receipt_window) try txn.delete(try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence - receipt_window));
    return receipt;
}

const Admission = struct { blocked: bool, before: ?locks.Backend = null };
fn readWaiter(a: A, txn: anytype, ns: Namespace, head: Resource, backend: locks.Backend) !Waiter {
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(txn, try key(&buffer, ns, .waiter, backend, head.digest, 0))) orelse return error.InvalidLockRecord;
    const waiter = try decode(Waiter, a, bytes);
    try waiter.effect.validate();
    if (waiter.effect.action != .acquire or !waiter.effect.backend.eql(backend) or !std.mem.eql(u8, &waiter.effect.action.resource(), &head.digest) or waiter.effect.action.class() != head.class) return error.InvalidLockRecord;
    return waiter;
}
fn queueAdmission(a: A, txn: anytype, ns: Namespace, head: Resource, effect: Effect, own: ?locks.Grant) !Admission {
    const requested = switch (effect.action) {
        .acquire, .try_acquire => |value| value.mode,
        else => return error.InvalidLockEffect,
    };
    const own_mask = if (own) |grant| grant.mask() else 0;
    // Reentrance of an already held mode cannot wait behind another backend.
    if (own_mask & (@as(u8, 1) << requested.index()) != 0) return .{ .blocked = false };
    var queued_conflict = false;
    for (head.wait_counts, 0..) |count, index| if (count != 0 and requested.conflicts() & (@as(u8, 1) << @as(u3, @intCast(index))) != 0) {
        queued_conflict = true;
    };
    if (!queued_conflict) return .{ .blocked = false };
    var blocked = false;
    var current = head.first;
    var previous: ?locks.Backend = null;
    var visited: u32 = 0;
    while (current) |backend| {
        if (visited >= head.waiters) return error.InvalidLockRecord;
        const waiter = try readWaiter(a, txn, ns, head, backend);
        if (!std.meta.eql(previous, waiter.previous)) return error.InvalidLockRecord;
        if (backend.eql(effect.backend)) {
            if (!std.mem.eql(u8, &try effect.digest(ns), &try waiter.effect.digest(ns))) return error.LockEffectChanged;
            return .{ .blocked = blocked, .before = backend };
        }
        const mode = waiter.effect.action.acquire.mode;
        // An upgrade precedes waiters blocked by its existing grant. Earlier
        // waiters which that grant does not block retain their queue priority.
        if (own_mask & mode.conflicts() != 0) return .{ .blocked = blocked, .before = backend };
        blocked = blocked or try requested.conflictsWith(mode);
        visited += 1;
        previous = backend;
        current = waiter.next;
    }
    if (visited != head.waiters or !std.meta.eql(previous, head.last)) return error.InvalidLockRecord;
    return .{ .blocked = blocked };
}
fn enqueue(a: A, txn: anytype, ns: Namespace, head: *Resource, effect: Effect, before: ?locks.Backend) !void {
    if (head.waiters == max_waiters_per_resource) return error.LockLimitExceeded;
    var buffer: [256]u8 = undefined;
    if (try optional(txn, try key(&buffer, ns, .waiter, effect.backend, head.digest, 0)) != null) return error.InvalidLockRecord;
    var next: ?Waiter = if (before) |backend| try readWaiter(a, txn, ns, head.*, backend) else null;
    const previous_backend = if (next) |value| value.previous else head.last;
    var previous: ?Waiter = if (previous_backend) |backend| try readWaiter(a, txn, ns, head.*, backend) else null;
    if (previous) |value| {
        if (!std.meta.eql(value.next, before)) return error.InvalidLockRecord;
    } else if (!std.meta.eql(head.first, before)) return error.InvalidLockRecord;
    const waiter: Waiter = .{ .effect = effect, .previous = previous_backend, .next = before };
    if (previous) |*value| value.next = effect.backend else head.first = effect.backend;
    if (next) |*value| value.previous = effect.backend else head.last = effect.backend;
    head.waiters += 1;
    head.wait_counts[effect.action.acquire.mode.index()] += 1;
    head.revision = std.math.add(u64, head.revision, 1) catch return error.LockLimitExceeded;
    try head.validate();
    const waiter_bytes = try encode(a, waiter);
    const previous_bytes = if (previous) |value| try encode(a, value) else null;
    const next_bytes = if (next) |value| try encode(a, value) else null;
    const head_bytes = try encode(a, head.*);
    if (previous_bytes) |bytes| try txn.put(try key(&buffer, ns, .waiter, previous_backend.?, head.digest, 0), bytes);
    if (next_bytes) |bytes| try txn.put(try key(&buffer, ns, .waiter, before.?, head.digest, 0), bytes);
    try txn.put(try key(&buffer, ns, .waiter, effect.backend, head.digest, 0), waiter_bytes);
    try txn.put(try key(&buffer, ns, .resource, effect.backend, head.digest, 0), head_bytes);
}
fn dequeue(a: A, txn: anytype, ns: Namespace, head: *Resource, effect: Effect) !void {
    const waiter = try readWaiter(a, txn, ns, head.*, effect.backend);
    if (!std.mem.eql(u8, &try effect.digest(ns), &try waiter.effect.digest(ns))) return error.LockEffectChanged;
    var previous: ?Waiter = if (waiter.previous) |backend| try readWaiter(a, txn, ns, head.*, backend) else null;
    var next: ?Waiter = if (waiter.next) |backend| try readWaiter(a, txn, ns, head.*, backend) else null;
    if (previous) |*value| {
        if (!std.meta.eql(value.next, @as(?locks.Backend, effect.backend))) return error.InvalidLockRecord;
        value.next = waiter.next;
    } else {
        if (!std.meta.eql(head.first, @as(?locks.Backend, effect.backend))) return error.InvalidLockRecord;
        head.first = waiter.next;
    }
    if (next) |*value| {
        if (!std.meta.eql(value.previous, @as(?locks.Backend, effect.backend))) return error.InvalidLockRecord;
        value.previous = waiter.previous;
    } else {
        if (!std.meta.eql(head.last, @as(?locks.Backend, effect.backend))) return error.InvalidLockRecord;
        head.last = waiter.previous;
    }
    head.waiters = std.math.sub(u32, head.waiters, 1) catch return error.InvalidLockRecord;
    const slot = &head.wait_counts[effect.action.acquire.mode.index()];
    slot.* = std.math.sub(u32, slot.*, 1) catch return error.InvalidLockRecord;
    head.revision = std.math.add(u64, head.revision, 1) catch return error.LockLimitExceeded;
    try head.validate();
    const previous_bytes = if (previous) |value| try encode(a, value) else null;
    const next_bytes = if (next) |value| try encode(a, value) else null;
    var buffer: [256]u8 = undefined;
    if (previous_bytes) |bytes| try txn.put(try key(&buffer, ns, .waiter, waiter.previous.?, head.digest, 0), bytes);
    if (next_bytes) |bytes| try txn.put(try key(&buffer, ns, .waiter, waiter.next.?, head.digest, 0), bytes);
    try txn.delete(try key(&buffer, ns, .waiter, effect.backend, head.digest, 0));
}

/// Cancellation is an exact effect observation, never a compensating unlock.
/// If admission won before cancellation, return the grant's original receipt.
pub fn cancel(backing: A, txn: anytype, ns: Namespace, effect: Effect, context: anytype) !Receipt {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    return cancelWithOutcome(arena.allocator(), txn, ns, effect, context, .cancelled);
}
fn cancelWithOutcome(a: A, txn: anytype, ns: Namespace, effect: Effect, context: anytype, outcome: Outcome) !Receipt {
    try effect.validate();
    if (effect.action != .acquire) return error.InvalidLockEffect;
    var owner = (try readOwner(a, txn, ns, effect.backend)) orelse return error.LockOwnerNotRegistered;
    try context.requireEffect(txn, owner.lease, effect);
    if (try observe(a, txn, ns, effect, context)) |receipt| if (receipt.outcome != .pending) return receipt;
    if (effect.sequence < owner.next_effect) return error.LockEffectExpired;
    if (effect.sequence != owner.next_effect) return error.LockEffectOutOfOrder;
    if (owner.job != null or owner.state != .active) return error.LockOwnerBusy;
    var buffer: [256]u8 = undefined;
    const resource = effect.action.resource();
    var head: ?Resource = null;
    if (owner.waiting) |waiting| {
        if (!std.mem.eql(u8, &try effect.digest(ns), &try waiting.digest(ns))) return error.LockEffectChanged;
        const bytes = (try optional(txn, try key(&buffer, ns, .resource, effect.backend, resource, 0))) orelse return error.InvalidLockRecord;
        head = try decode(Resource, a, bytes);
        try head.?.validate();
        if (!std.mem.eql(u8, &head.?.digest, &resource) or head.?.class != effect.action.class()) return error.InvalidLockRecord;
        try dequeue(a, txn, ns, &head.?, effect);
    } else if (outcome != .cancelled) return error.InvalidLockEffect;
    // Cancellation may win before the original admission RPC arrives. Seal
    // that exact serial/body now, so a delayed request cannot acquire later.
    owner.waiting = null;
    owner.next_effect += 1;
    try owner.validate(ns);
    const receipt: Receipt = .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = outcome };
    const owner_bytes = try encode(a, owner);
    const receipt_bytes = try encode(a, receipt);
    const head_bytes = if (head != null and (head.?.backends != 0 or head.?.waiters != 0)) try encode(a, head.?) else null;
    if (head != null) {
        if (head_bytes) |value| try txn.put(try key(&buffer, ns, .resource, effect.backend, resource, 0), value) else try txn.delete(try key(&buffer, ns, .resource, effect.backend, resource, 0));
    }
    try txn.put(try key(&buffer, ns, .owner, effect.backend, @splat(0), 0), owner_bytes);
    try txn.put(try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence), receipt_bytes);
    if (effect.sequence > receipt_window) try txn.delete(try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence - receipt_window));
    return receipt;
}

// Advisory resources share one native metadata home. The wait graph below is
// read and changed in that same writer transaction, so a stale sampled edge
// cannot authorize a deadlock abort. Distributed row/table graphs must use
// their participant proof protocol instead of calling this local detector.
const SoftEdge = struct { from: locks.Backend, before: locks.Backend, resource: [32]u8 };
const Blocker = struct { backend: locks.Backend, soft: ?SoftEdge = null };
const Cycle = struct { soft: ?SoftEdge };
pub const max_deadlock_vertices = 64;
pub const max_queue_reorders = 32;
// A cumulative budget, not the product of the vertex and reorder limits.
// Exhaustion aborts the whole attempted admission, including any queue edits.
pub const max_deadlock_probes = 2048;
const PlanBudget = struct {
    used: usize = 0,
    fn take(self: *PlanBudget, count: usize) !void {
        if (count > max_deadlock_probes - self.used) return error.LockPlanLimitExceeded;
        self.used += count;
    }
};
fn blockers(a: A, txn: anytype, ns: Namespace, effect: Effect, budget: *PlanBudget) ![]Blocker {
    try budget.take(1);
    var buffer: [256]u8 = undefined;
    const resource = effect.action.resource();
    const bytes = (try optional(txn, try key(&buffer, ns, .resource, effect.backend, resource, 0))) orelse return error.InvalidLockRecord;
    const head = try decode(Resource, a, bytes);
    try head.validate();
    if (!std.mem.eql(u8, &head.digest, &resource) or head.class != effect.action.class()) return error.InvalidLockRecord;
    var result: std.ArrayList(Blocker) = .empty;
    const requested = effect.action.acquire.mode;
    // Holder-key probes are bounded by a separate per-resource backend cap.
    // Fast uncontended/try paths never build this graph.
    const address = try key(&buffer, ns, .holder, effect.backend, resource, 0);
    const prefix = address[0 .. address.len - 49];
    var cursor = try txn.openCursor();
    defer cursor.close();
    var pair = try cursor.seekAtOrAfter(prefix);
    var found: u32 = 0;
    while (pair) |item| : (pair = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (found >= head.backends) return error.InvalidLockRecord;
        try budget.take(1);
        const grant = try locks.Grant.decode(item.value);
        var expected: [256]u8 = undefined;
        if (grant.class != head.class or grant.mask() == 0 or !std.mem.eql(u8, &grant.resource, &resource) or !std.mem.eql(u8, &grant.backend.incarnation, &ns.incarnation) or !std.mem.eql(u8, item.key, try key(&expected, ns, .holder, grant.backend, resource, 0))) return error.InvalidLockRecord;
        found += 1;
        if (!grant.backend.eql(effect.backend) and requested.conflicts() & grant.mask() != 0) try result.append(a, .{ .backend = grant.backend });
    }
    if (found != head.backends) return error.InvalidLockRecord;
    var current = head.first;
    var previous: ?locks.Backend = null;
    var visited: u32 = 0;
    while (current) |backend| {
        if (visited >= head.waiters) return error.InvalidLockRecord;
        try budget.take(1);
        const waiter = try readWaiter(a, txn, ns, head, backend);
        if (!std.meta.eql(waiter.previous, previous)) return error.InvalidLockRecord;
        if (backend.eql(effect.backend)) {
            if (!std.mem.eql(u8, &try waiter.effect.digest(ns), &try effect.digest(ns))) return error.LockEffectChanged;
            return result.items;
        }
        if (try requested.conflictsWith(waiter.effect.action.acquire.mode)) try result.append(a, .{ .backend = backend, .soft = .{ .from = effect.backend, .before = backend, .resource = resource } });
        previous = backend;
        current = waiter.next;
        visited += 1;
    }
    return error.InvalidLockRecord;
}
const Search = struct {
    origin: locks.Backend,
    budget: *PlanBudget,
    visited: [max_deadlock_vertices]locks.Backend = undefined,
    count: usize = 0,
    fn visit(self: *Search, a: A, txn: anytype, ns: Namespace, backend: locks.Backend, soft: ?SoftEdge) anyerror!?Cycle {
        if (self.count != 0 and backend.eql(self.origin)) return .{ .soft = soft };
        for (self.visited[0..self.count]) |old| if (old.eql(backend)) return null;
        if (self.count == max_deadlock_vertices) return error.LockPlanLimitExceeded;
        self.visited[self.count] = backend;
        self.count += 1;
        try self.budget.take(1);
        const owner = (try readOwner(a, txn, ns, backend)) orelse return error.InvalidLockRecord;
        const waiting = owner.waiting orelse return null;
        for (try blockers(a, txn, ns, waiting, self.budget)) |blocker| {
            if (try self.visit(a, txn, ns, blocker.backend, soft orelse blocker.soft)) |cycle| return cycle;
        }
        return null;
    }
};
fn resolveDeadlock(a: A, txn: anytype, ns: Namespace, origin: locks.Backend) !bool {
    var budget: PlanBudget = .{};
    for (0..max_queue_reorders) |_| {
        var search: Search = .{ .origin = origin, .budget = &budget };
        const cycle = (try search.visit(a, txn, ns, origin, null)) orelse return false;
        const edge = cycle.soft orelse return true;
        try budget.take(16);
        var buffer: [256]u8 = undefined;
        const bytes = (try optional(txn, try key(&buffer, ns, .resource, edge.from, edge.resource, 0))) orelse return error.InvalidLockRecord;
        var head = try decode(Resource, a, bytes);
        try head.validate();
        const waiter = try readWaiter(a, txn, ns, head, edge.from);
        // The chosen edge is queue priority, not a conflicting held grant.
        // Move this waiter ahead of that predecessor and search the actual
        // graph again. An all-hard cycle produces a durable deadlock outcome.
        try dequeue(a, txn, ns, &head, waiter.effect);
        try enqueue(a, txn, ns, &head, waiter.effect, edge.before);
    }
    // Resource exhaustion is not proof of a deadlock. Abort this admission's
    // writer transaction; never emit SQLSTATE 40P01 from a truncated graph.
    return error.LockPlanLimitExceeded;
}

// Control jobs fence their owner before touching its membership list. Each
// step reads one indexed member and updates at most two grants, one resource
// aggregate and the accepting owner's progress. No writer transaction waits
// for another backend, scans the catalog or allocates a cohort-sized frame.
fn beginControl(a: A, txn: anytype, ns: Namespace, owner: *Owner, effect: Effect, context: anytype) !Receipt {
    const control = effect.action.control;
    const transaction = controlTransaction(control);
    if (transaction) |id| if (owner.transaction) |current| if (!std.mem.eql(u8, &current, &id)) return error.LockOwnerChanged;
    var destination: ?Owner = null;
    if (control == .prepare) {
        const request = control.prepare;
        try request.destination.validate(ns);
        try context.requirePrepare(txn, owner.lease, request.destination, request.transaction);
        if (owner.mixed_resources != 0) return error.MixedAdvisoryPrepareUnsupported;
        if (try readOwner(a, txn, ns, request.destination.backend) != null) return error.LockOwnerChanged;
        destination = .{ .lease = request.destination, .state = .incoming, .transaction = request.transaction, .incoming = .{ .source = effect.backend, .sequence = effect.sequence, .digest = try effect.digest(ns) } };
    }
    owner.job = .{ .effect = effect, .total = if (transaction != null) owner.transaction_resources else owner.resources };
    try owner.validate(ns);
    const owner_bytes = try encode(a, owner.*);
    const destination_bytes = if (destination) |value| try encode(a, value) else null;
    var buffer: [256]u8 = undefined;
    if (destination_bytes) |bytes| try txn.put(try key(&buffer, ns, .owner, destination.?.lease.backend, @splat(0), 0), bytes);
    try txn.put(try key(&buffer, ns, .owner, effect.backend, @splat(0), 0), owner_bytes);
    return .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = .pending };
}
const Member = struct { resource: [32]u8, class: locks.Class };
fn nextMember(txn: anytype, ns: Namespace, backend: locks.Backend, transaction: ?[16]u8, after: ?[32]u8) !?Member {
    var buffer: [256]u8 = undefined;
    const address = try memberKey(&buffer, ns, backend, transaction, after orelse @splat(0));
    const prefix = address[0 .. address.len - 64];
    var cursor = try txn.openCursor();
    defer cursor.close();
    var found = try cursor.seekAtOrAfter(if (after != null) address else prefix);
    if (after != null) if (found) |value| if (std.mem.eql(u8, value.key, address)) {
        found = try cursor.next();
    };
    const value = found orelse return null;
    if (!std.mem.startsWith(u8, value.key, prefix)) return null;
    if (value.key.len != prefix.len + 64 or value.value.len != 1) return error.InvalidLockRecord;
    var result: Member = .{ .resource = undefined, .class = std.enums.fromInt(locks.Class, value.value[0]) orelse return error.InvalidLockRecord };
    _ = std.fmt.hexToBytes(&result.resource, value.key[prefix.len..]) catch return error.InvalidLockRecord;
    return result;
}
fn incomingOwner(a: A, txn: anytype, ns: Namespace, effect: Effect) !Owner {
    const request = effect.action.control.prepare;
    const owner = (try readOwner(a, txn, ns, request.destination.backend)) orelse return error.InvalidLockRecord;
    const expected: Incoming = .{ .source = effect.backend, .sequence = effect.sequence, .digest = try effect.digest(ns) };
    if (!std.meta.eql(owner.lease, request.destination) or owner.state != .incoming or !std.meta.eql(owner.incoming, @as(?Incoming, expected)) or owner.job != null) return error.InvalidLockRecord;
    return owner;
}

/// The same exact control frame is supplied on every quantum. A caller must
/// commit the writer transaction before acknowledging pending or completed.
/// All authority checks run again, including when the previous reply was lost.
pub fn step(backing: A, txn: anytype, ns: Namespace, effect: Effect, context: anytype) !Receipt {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try effect.validate();
    if (effect.action != .control) return error.InvalidLockEffect;
    var owner = (try readOwner(a, txn, ns, effect.backend)) orelse return error.LockOwnerNotRegistered;
    try context.requireEffect(txn, owner.lease, effect);
    if (try observe(a, txn, ns, effect, context)) |receipt| if (receipt.outcome != .pending) return receipt;
    var job = owner.job orelse return error.LockEffectOutOfOrder;
    if (!std.mem.eql(u8, &try job.effect.digest(ns), &try effect.digest(ns))) return error.LockEffectChanged;
    const control = effect.action.control;
    var destination: ?Owner = if (control == .prepare) try incomingOwner(a, txn, ns, effect) else null;
    if (destination) |value| try context.requirePrepare(txn, owner.lease, value.lease, control.prepare.transaction);
    const member = try nextMember(txn, ns, effect.backend, controlTransaction(control), job.after);
    var buffer: [256]u8 = undefined;
    if (member) |item| {
        if (job.visited >= job.total) return error.InvalidLockRecord;
        const raw_head = (try optional(txn, try key(&buffer, ns, .resource, effect.backend, item.resource, 0))) orelse return error.InvalidLockRecord;
        var head = try decode(Resource, a, raw_head);
        try head.validate();
        if (head.backends == 0 or head.class != item.class or !std.mem.eql(u8, &head.digest, &item.resource)) return error.InvalidLockRecord;
        const raw_grant = (try optional(txn, try key(&buffer, ns, .holder, effect.backend, item.resource, 0))) orelse return error.InvalidLockRecord;
        var grant = try locks.Grant.decode(raw_grant);
        if (!grant.backend.eql(effect.backend) or grant.class != item.class or !std.mem.eql(u8, &grant.resource, &item.resource) or grant.mask() == 0) return error.InvalidLockRecord;
        try requireMember(txn, ns, effect.backend, null, item.resource, item.class);
        const was_transaction = grant.transaction;
        if (was_transaction) |id| {
            if (!std.meta.eql(owner.transaction, was_transaction) or owner.transaction_resources == 0) return error.InvalidLockRecord;
            try requireMember(txn, ns, effect.backend, id, item.resource, item.class);
        }
        if (controlTransaction(control) != null and was_transaction == null) return error.InvalidLockRecord;
        const before = grant.mask();
        const was_mixed = mixed(grant);
        var detached: ?locks.Grant = null;
        const changed = switch (control) {
            .unlock_all => if (grant.class == .advisory) try grant.unlockAllSession() else false,
            .rollback => |value| try grant.rollback(value.transaction, value.first_aborted),
            .release_savepoint => |value| try grant.releaseSavepoint(value.transaction, value.first_child, value.parent),
            .close => blk: {
                grant.session = @splat(0);
                grant.acquired = @splat(null);
                grant.transaction = null;
                break :blk true;
            },
            .prepare => |value| blk: {
                if (grant.transaction) |id| if (!std.mem.eql(u8, &id, &value.transaction)) return error.LockOwnerChanged;
                detached = try grant.detachTransaction(value.destination.backend);
                break :blk detached != null;
            },
        };
        const after = grant.mask();
        if (was_transaction != null and grant.transaction == null) owner.transaction_resources = std.math.sub(u32, owner.transaction_resources, 1) catch return error.InvalidLockRecord;
        if (changed) {
            var aggregate: locks.Aggregate = .{ .class = head.class, .owners = head.counts };
            // Validate own counts before subtraction, including modes that
            // the requested control would otherwise leave unchanged.
            for (head.counts, 0..) |count, index| if (before & (@as(u8, 1) << @as(u3, @intCast(index))) != 0 and count == 0) return error.InvalidLockRecord;
            try aggregate.transition(before, after);
            if (before != 0 and after == 0) {
                owner.resources = std.math.sub(u32, owner.resources, 1) catch return error.InvalidLockRecord;
                head.backends = std.math.sub(u32, head.backends, 1) catch return error.InvalidLockRecord;
            }
            if (was_mixed != mixed(grant)) owner.mixed_resources = std.math.sub(u32, owner.mixed_resources, 1) catch return error.InvalidLockRecord;
            if (detached) |value| {
                if (try optional(txn, try key(&buffer, ns, .holder, value.backend, item.resource, 0)) != null or try optional(txn, try key(&buffer, ns, .member, value.backend, item.resource, 0)) != null) return error.InvalidLockRecord;
                try aggregate.transition(0, value.mask());
                head.backends = std.math.add(u32, head.backends, 1) catch return error.LockLimitExceeded;
                destination.?.resources = std.math.add(u32, destination.?.resources, 1) catch return error.LockLimitExceeded;
                destination.?.transaction_resources = std.math.add(u32, destination.?.transaction_resources, 1) catch return error.LockLimitExceeded;
                try destination.?.validate(ns);
            }
            head.counts = aggregate.owners;
            head.revision = std.math.add(u64, head.revision, 1) catch return error.LockLimitExceeded;
            try head.validate();
        }
        job.after = item.resource;
        job.visited += 1;
        owner.job = job;
        try owner.validate(ns);
        const owner_bytes = try encode(a, owner);
        const head_bytes = if (changed and (head.backends != 0 or head.waiters != 0)) try encode(a, head) else null;
        const grant_bytes = if (changed and after != 0) try grant.encode() else null;
        const detached_bytes = if (detached) |value| try value.encode() else null;
        const destination_bytes = if (destination) |value| try encode(a, value) else null;
        if (changed) {
            if (grant_bytes) |*bytes| {
                try txn.put(try key(&buffer, ns, .holder, effect.backend, item.resource, 0), bytes);
            } else {
                try txn.delete(try key(&buffer, ns, .holder, effect.backend, item.resource, 0));
                try txn.delete(try key(&buffer, ns, .member, effect.backend, item.resource, 0));
            }
            if (was_transaction) |id| if (grant.transaction == null) try txn.delete(try memberKey(&buffer, ns, effect.backend, id, item.resource));
            if (detached_bytes) |*bytes| {
                const backend = detached.?.backend;
                try txn.put(try key(&buffer, ns, .holder, backend, item.resource, 0), bytes);
                try txn.put(try key(&buffer, ns, .member, backend, item.resource, 0), &.{@backingInt(item.class)});
                try txn.put(try memberKey(&buffer, ns, backend, detached.?.transaction.?, item.resource), &.{@backingInt(item.class)});
            }
            if (head_bytes) |bytes| try txn.put(try key(&buffer, ns, .resource, effect.backend, item.resource, 0), bytes) else try txn.delete(try key(&buffer, ns, .resource, effect.backend, item.resource, 0));
        }
        if (destination_bytes) |bytes| try txn.put(try key(&buffer, ns, .owner, destination.?.lease.backend, @splat(0), 0), bytes);
        try txn.put(try key(&buffer, ns, .owner, effect.backend, @splat(0), 0), owner_bytes);
        return .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = .pending };
    }
    if (job.visited != job.total) return error.InvalidLockRecord;
    owner.job = null;
    owner.next_effect += 1;
    switch (control) {
        .close, .prepare => {
            if (owner.transaction_resources != 0) return error.InvalidLockRecord;
            owner.transaction = null;
        },
        .rollback => |value| if (value.first_aborted == 0) {
            if (owner.transaction_resources != 0) return error.InvalidLockRecord;
            owner.transaction = null;
        },
        else => {},
    }
    if (control == .close) {
        if (owner.resources != 0 or owner.mixed_resources != 0) return error.InvalidLockRecord;
        owner.state = .retired;
    }
    if (destination) |*value| {
        value.state = .prepared;
        value.incoming = null;
        try value.validate(ns);
    }
    try owner.validate(ns);
    const receipt: Receipt = .{ .sequence = effect.sequence, .nonce = effect.nonce, .digest = try effect.digest(ns), .outcome = .completed };
    const owner_bytes = try encode(a, owner);
    const receipt_bytes = try encode(a, receipt);
    const destination_bytes = if (destination) |value| try encode(a, value) else null;
    if (destination_bytes) |bytes| try txn.put(try key(&buffer, ns, .owner, destination.?.lease.backend, @splat(0), 0), bytes);
    try txn.put(try key(&buffer, ns, .owner, effect.backend, @splat(0), 0), owner_bytes);
    try txn.put(try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence), receipt_bytes);
    if (effect.sequence > receipt_window) try txn.delete(try key(&buffer, ns, .receipt, effect.backend, @splat(0), effect.sequence - receipt_window));
    return receipt;
}

// In-memory transaction adapter qualifies ledger invariants and rollback on a
// failed staged write. Production native writer/replication gates separately
// qualify concrete storage atomicity and durable owner authorization.
const TestStore = struct {
    a: A,
    values: std.StringHashMapUnmanaged([]u8) = .empty,
    fail_after: ?usize = null,
    writes: usize = 0,
    fn deinit(self: *TestStore) void {
        var iterator = self.values.iterator();
        while (iterator.next()) |entry| {
            self.a.free(entry.key_ptr.*);
            self.a.free(entry.value_ptr.*);
        }
        self.values.deinit(self.a);
    }
    const Cursor = struct {
        store: *TestStore,
        position: ?[]const u8 = null,
        const KV = struct { key: []const u8, value: []const u8 };
        fn close(_: *Cursor) void {}
        fn find(self: *Cursor, lower: []const u8, strict: bool) !?KV {
            var selected: ?KV = null;
            var it = self.store.values.iterator();
            while (it.next()) |entry| {
                const order = std.mem.order(u8, entry.key_ptr.*, lower);
                if (order == .lt or (strict and order == .eq)) continue;
                if (selected == null or std.mem.order(u8, entry.key_ptr.*, selected.?.key) == .lt) selected = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
            }
            if (selected) |value| self.position = value.key;
            return selected;
        }
        fn seekAtOrAfter(self: *Cursor, lower: []const u8) !?KV {
            return self.find(lower, false);
        }
        fn next(self: *Cursor) !?KV {
            return self.find(self.position orelse return null, true);
        }
    };
    fn openCursor(self: *TestStore) !Cursor {
        return .{ .store = self };
    }
    fn get(self: *TestStore, address: []const u8) ![]const u8 {
        return self.values.get(address) orelse error.NotFound;
    }
    fn put(self: *TestStore, address: []const u8, value: []const u8) !void {
        if (self.fail_after) |limit| if (self.writes >= limit) return error.InjectedLockWriteFailure;
        const body = try self.a.dupe(u8, value);
        errdefer self.a.free(body);
        if (self.values.getPtr(address)) |existing| {
            self.a.free(existing.*);
            existing.* = body;
        } else {
            const owned_key = try self.a.dupe(u8, address);
            errdefer self.a.free(owned_key);
            try self.values.put(self.a, owned_key, body);
        }
        self.writes += 1;
    }
    fn delete(self: *TestStore, address: []const u8) !void {
        if (self.fail_after) |limit| if (self.writes >= limit) return error.InjectedLockWriteFailure;
        if (self.values.fetchRemove(address)) |entry| {
            self.a.free(entry.key);
            self.a.free(entry.value);
        }
        self.writes += 1;
    }
    fn clone(self: *TestStore) !TestStore {
        var copy: TestStore = .{ .a = self.a };
        errdefer copy.deinit();
        var iterator = self.values.iterator();
        while (iterator.next()) |entry| try copy.put(entry.key_ptr.*, entry.value_ptr.*);
        copy.writes = 0;
        return copy;
    }
    fn cancellation(self: *TestStore, ns: Namespace, request: Effect, context: TestContext, fail_after: ?usize) !Receipt {
        var pending = try self.clone();
        errdefer pending.deinit();
        pending.fail_after = fail_after;
        const result = try cancel(self.a, &pending, ns, request, context);
        self.deinit();
        self.* = pending;
        self.fail_after = null;
        return result;
    }
    fn complete(self: *TestStore, ns: Namespace, request: Effect, context: TestContext) !void {
        const accepted = try self.effect(ns, request, context, null);
        if (accepted.outcome == .completed) return;
        var steps: usize = 0;
        while ((try self.quantum(ns, request, context, null)).outcome == .pending) {
            steps += 1;
            if (steps > max_resources_per_backend) return error.UnboundedLockCleanup;
        }
    }
    fn quantum(self: *TestStore, ns: Namespace, request: Effect, context: TestContext, fail_after: ?usize) !Receipt {
        var pending = try self.clone();
        errdefer pending.deinit();
        pending.fail_after = fail_after;
        const result = try step(self.a, &pending, ns, request, context);
        self.deinit();
        self.* = pending;
        self.fail_after = null;
        return result;
    }
    fn effect(self: *TestStore, ns: Namespace, request: Effect, context: TestContext, fail_after: ?usize) !Receipt {
        var pending = try self.clone();
        errdefer pending.deinit();
        pending.fail_after = fail_after;
        const result = try apply(self.a, &pending, ns, request, context);
        self.deinit();
        self.* = pending;
        self.fail_after = null;
        return result;
    }
};
const TestContext = struct {
    expected: Lease,
    writable: bool = true,
    fn requireIdentity(self: TestContext, lease: Lease) !void {
        if (!std.meta.eql(self.expected, lease)) return error.Forbidden;
    }
    fn requireLease(self: TestContext, _: anytype, lease: Lease) !void {
        try self.requireIdentity(lease);
        if (!self.writable) return error.Forbidden;
    }
    fn requireEffect(self: TestContext, txn: anytype, lease: Lease, request: Effect) !void {
        try self.requireLease(txn, lease);
        if (!request.backend.eql(lease.backend)) return error.Forbidden;
    }
    fn requirePrepare(self: TestContext, _: anytype, source: Lease, destination: Lease, transaction: [16]u8) !void {
        try self.requireIdentity(source);
        if (!self.writable or source.backend.eql(destination.backend) or std.mem.allEqual(u8, &transaction, 0)) return error.Forbidden;
    }
    fn requireObservation(self: TestContext, _: anytype, lease: Lease, request: Effect) !void {
        try self.requireIdentity(lease);
        if (!request.backend.eql(lease.backend)) return error.Forbidden;
    }
};
const TestFixture = struct {
    const ns: Namespace = .{ .group = 21, .incarnation = @splat(1) };
    const a: Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .database = 1, .node = 7, .principal = @splat(3), .expires_at_ms = 1000 };
    const b: Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(4), .fence = 1 }, .database = 1, .node = 8, .principal = @splat(5), .expires_at_ms = 1000 };
    fn request(lease: Lease, serial: u64) Effect {
        return .{ .backend = lease.backend, .sequence = serial, .nonce = @splat(@as(u8, @intCast(serial))), .action = .{ .try_acquire = .{ .resource = @splat(6), .mode = .{ .advisory = .exclusive }, .scope = .session } } };
    }
    fn grant(store: *TestStore, lease: Lease) !?locks.Grant {
        var buffer: [256]u8 = undefined;
        const raw = (try optional(store, try key(&buffer, ns, .holder, lease.backend, @splat(6), 0))) orelse return null;
        return try locks.Grant.decode(raw);
    }
};

test "pessimistic lock ledger exact effects survive a copied ledger without repeating refs or turning an old false try into success" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    const first = f.request(f.a, 1);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, first, ca, null)).outcome);
    _ = try store.effect(f.ns, first, ca, null);
    try std.testing.expectEqual(@as(u32, 1), (try f.grant(&store, f.a)).?.session[1]);
    const refused = f.request(f.b, 1);
    try std.testing.expectEqual(Outcome.contended, (try store.effect(f.ns, refused, cb, null)).outcome);
    _ = try store.effect(f.ns, f.request(f.a, 2), ca, null);
    var release = f.request(f.a, 3);
    release.action = .{ .unlock = .{ .resource = @splat(6), .mode = .exclusive } };
    try std.testing.expectEqual(Outcome.unlocked, (try store.effect(f.ns, release, ca, null)).outcome);
    try std.testing.expectEqual(@as(u32, 1), (try f.grant(&store, f.a)).?.session[1]);
    release.sequence = 4;
    release.nonce = @splat(4);
    _ = try store.effect(f.ns, release, ca, null);
    try std.testing.expect((try f.grant(&store, f.a)) == null);
    var cold = try store.clone();
    defer cold.deinit();
    try std.testing.expectEqual(Outcome.contended, (try cold.effect(f.ns, refused, cb, null)).outcome);
    try std.testing.expect((try f.grant(&cold, f.b)) == null);
    try std.testing.expectEqual(Outcome.granted, (try cold.effect(f.ns, f.request(f.b, 2), cb, null)).outcome);
    var substituted = first;
    substituted.action.try_acquire.mode = .{ .advisory = .shared };
    try std.testing.expectError(error.LockEffectChanged, cold.effect(f.ns, substituted, ca, null));
    try std.testing.expectEqual(@as(u64, 5), (try readOwner(a, &cold, f.ns, f.a.backend)).?.next_effect);
}

test "pessimistic lock ledger failed staged writes abort all membership and receipt progress and reject read authority as writes" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    const request = f.request(f.a, 1);
    try std.testing.expectError(error.InjectedLockWriteFailure, store.effect(f.ns, request, context, 2));
    try std.testing.expectEqual(@as(u64, 1), (try readOwner(a, &store, f.ns, f.a.backend)).?.next_effect);
    try std.testing.expectEqual(@as(u32, 0), (try readOwner(a, &store, f.ns, f.a.backend)).?.resources);
    try std.testing.expect((try f.grant(&store, f.a)) == null);
    try std.testing.expect((try observe(a, &store, f.ns, request, context)) == null);
    const readonly: TestContext = .{ .expected = f.a, .writable = false };
    try std.testing.expectError(error.Forbidden, store.effect(f.ns, request, readonly, null));
    _ = try store.effect(f.ns, request, context, null);
    try std.testing.expectEqual(Outcome.granted, (try observe(a, &store, f.ns, request, readonly)).?.outcome);
    try std.testing.expectError(error.Forbidden, store.effect(f.ns, f.request(f.a, 2), .{ .expected = f.b }, null));
}

test "pessimistic lock ledger receipt retention is bounded and old effects cannot become new acquisitions" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    for (1..101) |serial| _ = try store.effect(f.ns, f.request(f.a, @intCast(serial)), context, null);
    try std.testing.expectEqual(@as(usize, receipt_window + 4), store.values.count());
    try std.testing.expect((try observe(a, &store, f.ns, f.request(f.a, 1), context)) == null);
    try std.testing.expectError(error.LockEffectExpired, store.effect(f.ns, f.request(f.a, 1), context, null));
    try std.testing.expect((try observe(a, &store, f.ns, f.request(f.a, 37), context)) != null);
    try std.testing.expectEqual(@as(u32, 100), (try f.grant(&store, f.a)).?.session[1]);
    try std.testing.expectError(error.LockEffectOutOfOrder, store.effect(f.ns, f.request(f.a, 102), context, null));
}

test "pessimistic lock lifecycle jobs resume bounded membership walks and keep session rollback independent" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    const transaction: [16]u8 = @splat(9);
    var request = f.request(f.a, 1);
    _ = try store.effect(f.ns, request, ca, null);
    request.sequence = 2;
    request.action.try_acquire.mode = .{ .advisory = .shared };
    request.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 4 } };
    _ = try store.effect(f.ns, request, ca, null);
    request.sequence = 3;
    request.action.try_acquire.resource = @splat(7);
    request.action.try_acquire.mode = .{ .row = .update };
    _ = try store.effect(f.ns, request, ca, null);
    try std.testing.expectEqual(@as(u32, 1), (try readOwner(a, &store, f.ns, f.a.backend)).?.mixed_resources);
    var rollback = f.request(f.a, 4);
    rollback.action = .{ .control = .{ .rollback = .{ .transaction = transaction, .first_aborted = 4 } } };
    _ = try store.effect(f.ns, rollback, ca, null);
    try std.testing.expectError(error.LockOwnerBusy, store.effect(f.ns, f.request(f.a, 5), ca, null));
    try std.testing.expectEqual(Outcome.pending, (try store.quantum(f.ns, rollback, ca, null)).outcome);
    const before_copy = (try readOwner(a, &store, f.ns, f.a.backend)).?;
    try std.testing.expectEqual(@as(u32, 1), before_copy.job.?.visited);
    var copy = try store.clone();
    defer copy.deinit();
    _ = try copy.quantum(f.ns, rollback, ca, null);
    try std.testing.expectEqual(Outcome.completed, (try copy.quantum(f.ns, rollback, ca, null)).outcome);
    try std.testing.expectEqual(Outcome.completed, (try copy.quantum(f.ns, rollback, ca, null)).outcome);
    const owner = (try readOwner(a, &copy, f.ns, f.a.backend)).?;
    try std.testing.expectEqual(@as(u32, 1), owner.resources);
    try std.testing.expectEqual(@as(u32, 0), owner.mixed_resources);
    const grant = (try f.grant(&copy, f.a)).?;
    try std.testing.expect(grant.transaction == null);
    try std.testing.expectEqual(@as(u32, 1), grant.session[1]);
    try std.testing.expectEqual(Outcome.contended, (try copy.effect(f.ns, f.request(f.b, 1), cb, null)).outcome);
    var unlock_all = f.request(f.a, 5);
    unlock_all.action = .{ .control = .unlock_all };
    _ = try copy.effect(f.ns, unlock_all, ca, null);
    _ = try copy.quantum(f.ns, unlock_all, ca, null);
    try std.testing.expectEqual(Outcome.completed, (try copy.quantum(f.ns, unlock_all, ca, null)).outcome);
    try std.testing.expect((try f.grant(&copy, f.a)) == null);
    try std.testing.expectEqual(Outcome.granted, (try copy.effect(f.ns, f.request(f.b, 2), cb, null)).outcome);
}

test "pessimistic lock savepoint release reparents transaction grants and unlock all preserves them" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    const transaction: [16]u8 = @splat(9);
    var acquire = f.request(f.a, 1);
    acquire.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 4 } };
    _ = try store.effect(f.ns, acquire, context, null);
    var command = f.request(f.a, 2);
    command.action = .{ .control = .{ .release_savepoint = .{ .transaction = transaction, .first_child = 4, .parent = 2 } } };
    try store.complete(f.ns, command, context);
    try std.testing.expectEqual(@as(?u64, 2), (try f.grant(&store, f.a)).?.acquired[1]);
    command.sequence = 3;
    command.action = .{ .control = .unlock_all };
    try store.complete(f.ns, command, context);
    try std.testing.expectEqual(@as(?u64, 2), (try f.grant(&store, f.a)).?.acquired[1]);
    command.sequence = 4;
    command.action = .{ .control = .{ .rollback = .{ .transaction = transaction, .first_aborted = 4 } } };
    try store.complete(f.ns, command, context);
    try std.testing.expect((try f.grant(&store, f.a)) != null);
    command.sequence = 5;
    command.action.control.rollback.first_aborted = 2;
    try store.complete(f.ns, command, context);
    try std.testing.expect((try f.grant(&store, f.a)) == null);
}

test "pessimistic lock prepared handoff never opens a gap or makes prepared locks self owned by the old connection" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    const transaction: [16]u8 = @splat(9);
    var request = f.request(f.a, 1);
    request.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 4 } };
    _ = try store.effect(f.ns, request, context, null);
    // A different resource retains session ownership on the live backend.
    request.sequence = 2;
    request.action.try_acquire.resource = @splat(7);
    request.action.try_acquire.scope = .session;
    _ = try store.effect(f.ns, request, context, null);
    var prepare = f.request(f.a, 3);
    prepare.action = .{ .control = .{ .prepare = .{ .transaction = transaction, .destination = f.b } } };
    _ = try store.effect(f.ns, prepare, context, null);
    try std.testing.expectError(error.LockOwnerBusy, store.effect(f.ns, f.request(f.b, 1), .{ .expected = f.b }, null));
    // Losing a quantum reply and retrying cannot duplicate a transferred grant.
    _ = try store.quantum(f.ns, prepare, context, null);
    try std.testing.expect((try f.grant(&store, f.a)) == null);
    try std.testing.expect((try f.grant(&store, f.b)) != null);
    var copy = try store.clone();
    defer copy.deinit();
    try store.complete(f.ns, prepare, context);
    try copy.complete(f.ns, prepare, context);
    try std.testing.expectEqual(@as(u32, 1), (try readOwner(a, &copy, f.ns, f.a.backend)).?.resources);
    try std.testing.expectEqual(.prepared, (try readOwner(a, &copy, f.ns, f.b.backend)).?.state);
    try std.testing.expectEqual(@as(?u64, 0), (try f.grant(&copy, f.b)).?.acquired[1]);
    try std.testing.expectEqual(Outcome.contended, (try copy.effect(f.ns, f.request(f.a, 4), context, null)).outcome);
    var close = f.request(f.b, 1);
    close.action = .{ .control = .close };
    try copy.complete(f.ns, close, .{ .expected = f.b });
    try std.testing.expectEqual(Outcome.granted, (try copy.effect(f.ns, f.request(f.a, 5), context, null)).outcome);
    try std.testing.expectError(error.LockOwnerBusy, register(a, &copy, f.ns, f.b, TestContext{ .expected = f.b }));
    try std.testing.expectError(error.LockOwnerBusy, copy.effect(f.ns, f.request(f.b, 2), .{ .expected = f.b }, null));
}

test "pessimistic lock prepared mixed scope preflight and staged transfer failures leave every owner unchanged" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    _ = try store.effect(f.ns, f.request(f.a, 1), context, null);
    var request = f.request(f.a, 2);
    request.action.try_acquire.scope = .{ .transaction = .{ .id = @splat(9), .subtransaction = 0 } };
    _ = try store.effect(f.ns, request, context, null);
    var prepare = f.request(f.a, 3);
    prepare.action = .{ .control = .{ .prepare = .{ .transaction = @splat(9), .destination = f.b } } };
    try std.testing.expectError(error.MixedAdvisoryPrepareUnsupported, store.effect(f.ns, prepare, context, null));
    try std.testing.expect((try readOwner(a, &store, f.ns, f.b.backend)) == null);
    try std.testing.expect((try readOwner(a, &store, f.ns, f.a.backend)).?.job == null);
    request.sequence = 3;
    request.action = .{ .unlock = .{ .resource = @splat(6), .mode = .exclusive } };
    _ = try store.effect(f.ns, request, context, null);
    prepare.sequence = 4;
    _ = try store.effect(f.ns, prepare, context, null);
    const count = store.values.count();
    for (0..5) |fail_after| {
        try std.testing.expectError(error.InjectedLockWriteFailure, store.quantum(f.ns, prepare, context, fail_after));
        try std.testing.expectEqual(count, store.values.count());
        try std.testing.expect((try f.grant(&store, f.a)) != null);
        try std.testing.expect((try f.grant(&store, f.b)) == null);
        try std.testing.expectEqual(@as(u32, 0), (try readOwner(a, &store, f.ns, f.a.backend)).?.job.?.visited);
    }
    var substitution = prepare;
    substitution.action.control.prepare.transaction = @splat(10);
    try std.testing.expectError(error.LockEffectChanged, store.quantum(f.ns, substitution, context, null));
    try store.complete(f.ns, prepare, context);
}

test "pessimistic lock queues prevent bypass starvation while allowing held-mode reentrance and upgrades" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    const c: Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(8), .fence = 1 }, .database = 1, .node = 9, .principal = @splat(8), .expires_at_ms = 1000 };
    const cc: TestContext = .{ .expected = c };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    try register(a, &store, f.ns, c, cc);
    var shared = f.request(f.a, 1);
    shared.action.try_acquire.mode = .{ .advisory = .shared };
    _ = try store.effect(f.ns, shared, ca, null);
    var waiting = f.request(f.b, 1);
    const waiting_acquisition = waiting.action.try_acquire;
    waiting.action = .{ .acquire = waiting_acquisition };
    try std.testing.expectEqual(Outcome.pending, (try store.effect(f.ns, waiting, cb, null)).outcome);
    shared.sequence = 2;
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, shared, ca, null)).outcome);
    // Exclusive upgrade must not wait behind the backend blocked by our SH.
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, f.request(f.a, 3), ca, null)).outcome);
    var close = f.request(f.a, 4);
    close.action = .{ .control = .close };
    try store.complete(f.ns, close, ca);
    // No grant remains, but a new try may not overtake the queued writer.
    try std.testing.expectEqual(Outcome.contended, (try store.effect(f.ns, f.request(c, 1), cc, null)).outcome);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, waiting, cb, null)).outcome);
    try std.testing.expectEqual(@as(u32, 1), (try f.grant(&store, f.b)).?.session[1]);
    _ = try store.effect(f.ns, waiting, cb, null);
    try std.testing.expectEqual(@as(u32, 1), (try f.grant(&store, f.b)).?.session[1]);
}

test "pessimistic lock cancellation seals unadmitted and queued effects but never compensates an admitted grant" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    var request = f.request(f.a, 1);
    const request_acquisition = request.action.try_acquire;
    request.action = .{ .acquire = request_acquisition };
    try std.testing.expectEqual(Outcome.cancelled, (try store.cancellation(f.ns, request, ca, null)).outcome);
    try std.testing.expectEqual(Outcome.cancelled, (try store.effect(f.ns, request, ca, null)).outcome);
    try std.testing.expect((try f.grant(&store, f.a)) == null);
    request.sequence = 2;
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, request, ca, null)).outcome);
    try std.testing.expectEqual(Outcome.granted, (try store.cancellation(f.ns, request, ca, null)).outcome);
    try std.testing.expect((try f.grant(&store, f.a)) != null);
    var waiting = f.request(f.b, 1);
    const waiting_acquisition = waiting.action.try_acquire;
    waiting.action = .{ .acquire = waiting_acquisition };
    _ = try store.effect(f.ns, waiting, cb, null);
    for (0..4) |failure| try std.testing.expectError(error.InjectedLockWriteFailure, store.cancellation(f.ns, waiting, cb, failure));
    try std.testing.expectEqual(Outcome.cancelled, (try store.cancellation(f.ns, waiting, cb, null)).outcome);
    var unlock = f.request(f.a, 3);
    unlock.action = .{ .unlock = .{ .resource = @splat(6), .mode = .exclusive } };
    _ = try store.effect(f.ns, unlock, ca, null);
    try std.testing.expectEqual(Outcome.cancelled, (try store.effect(f.ns, waiting, cb, null)).outcome);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, f.request(f.b, 2), cb, null)).outcome);
}

test "pessimistic lock hard deadlocks produce exact terminal outcomes without releasing the victim's existing locks" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    _ = try store.effect(f.ns, f.request(f.a, 1), ca, null);
    var other = f.request(f.b, 1);
    other.action.try_acquire.resource = @splat(7);
    _ = try store.effect(f.ns, other, cb, null);
    var a_wait = f.request(f.a, 2);
    const a_wait_acquisition = other.action.try_acquire;
    a_wait.action = .{ .acquire = a_wait_acquisition };
    _ = try store.effect(f.ns, a_wait, ca, null);
    var b_wait = f.request(f.b, 2);
    const b_wait_acquisition = b_wait.action.try_acquire;
    b_wait.action = .{ .acquire = b_wait_acquisition };
    try std.testing.expectEqual(Outcome.deadlocked, (try store.effect(f.ns, b_wait, cb, null)).outcome);
    try std.testing.expectEqual(@as(u32, 1), (try readOwner(a, &store, f.ns, f.b.backend)).?.resources);
    try std.testing.expect((try readOwner(a, &store, f.ns, f.b.backend)).?.waiting == null);
    try std.testing.expectEqual(Outcome.pending, (try store.effect(f.ns, a_wait, ca, null)).outcome);
    var close = f.request(f.b, 3);
    close.action = .{ .control = .close };
    try store.complete(f.ns, close, cb);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, a_wait, ca, null)).outcome);
    try std.testing.expectEqual(Outcome.deadlocked, (try store.effect(f.ns, b_wait, cb, null)).outcome);
}

test "pessimistic lock soft queue cycles reorder priority rather than reporting a false hard deadlock" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    const c: Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(8), .fence = 1 }, .database = 1, .node = 9, .principal = @splat(8), .expires_at_ms = 1000 };
    const cc: TestContext = .{ .expected = c };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    try register(a, &store, f.ns, c, cc);
    var a_shared = f.request(f.a, 1);
    a_shared.action.try_acquire.mode = .{ .advisory = .shared };
    _ = try store.effect(f.ns, a_shared, ca, null);
    var c_other = f.request(c, 1);
    c_other.action.try_acquire.resource = @splat(7);
    _ = try store.effect(f.ns, c_other, cc, null);
    var b_wait = f.request(f.b, 1);
    const b_wait_acquisition = b_wait.action.try_acquire;
    b_wait.action = .{ .acquire = b_wait_acquisition };
    _ = try store.effect(f.ns, b_wait, cb, null);
    var c_wait = f.request(c, 2);
    const c_wait_acquisition = a_shared.action.try_acquire;
    c_wait.action = .{ .acquire = c_wait_acquisition };
    try std.testing.expectEqual(Outcome.pending, (try store.effect(f.ns, c_wait, cc, null)).outcome);
    var a_wait = f.request(f.a, 2);
    const a_wait_acquisition = c_other.action.try_acquire;
    a_wait.action = .{ .acquire = a_wait_acquisition };
    // A -> C is a held-lock edge; C -> B is queue-only; B -> A is held.
    try std.testing.expectEqual(Outcome.pending, (try store.effect(f.ns, a_wait, ca, null)).outcome);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, c_wait, cc, null)).outcome);
    var close = f.request(c, 3);
    close.action = .{ .control = .close };
    try store.complete(f.ns, close, cc);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, a_wait, ca, null)).outcome);
    close = f.request(f.a, 3);
    close.action = .{ .control = .close };
    try store.complete(f.ns, close, ca);
    try std.testing.expectEqual(Outcome.granted, (try store.effect(f.ns, b_wait, cb, null)).outcome);
}

test "pessimistic lock planning limits abort admission without inventing a deadlock or losing earlier queued requests" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    var owners: [max_deadlock_vertices + 1]Lease = undefined;
    for (&owners, 0..) |*lease, index| {
        lease.* = f.a;
        lease.backend.id = @splat(0);
        std.mem.writeInt(u64, lease.backend.id[8..16], @intCast(index + 1), .big);
        lease.node = 0; // The normal standalone-local ownership sentinel.
        const context: TestContext = .{ .expected = lease.* };
        try register(a, &store, f.ns, lease.*, context);
        var request = f.request(lease.*, 1);
        request.action.try_acquire.resource = @splat(0);
        std.mem.writeInt(u64, request.action.try_acquire.resource[24..32], @intCast(index + 1), .big);
        _ = try store.effect(f.ns, request, context, null);
    }
    for (owners[1..], 1..) |lease, index| {
        const context: TestContext = .{ .expected = lease };
        var request = f.request(lease, 2);
        var acquisition = request.action.try_acquire;
        acquisition.resource = @splat(0);
        std.mem.writeInt(u64, acquisition.resource[24..32], @intCast(index), .big);
        request.action = .{ .acquire = acquisition };
        if (index == max_deadlock_vertices) {
            try std.testing.expectError(error.LockPlanLimitExceeded, store.effect(f.ns, request, context, null));
            const owner = (try readOwner(a, &store, f.ns, lease.backend)).?;
            try std.testing.expect(owner.waiting == null);
            try std.testing.expectEqual(@as(u64, 2), owner.next_effect);
            try std.testing.expectEqual(@as(u32, 1), owner.resources);
            try std.testing.expect((try observe(a, &store, f.ns, request, context)) == null);
        } else try std.testing.expectEqual(Outcome.pending, (try store.effect(f.ns, request, context, null)).outcome);
    }
    const prior = (try readOwner(a, &store, f.ns, owners[max_deadlock_vertices - 1].backend)).?;
    try std.testing.expect(prior.waiting != null);
    try std.testing.expectEqual(@as(u64, 2), prior.next_effect);
}

test "pessimistic lock owners reject cross-incarnation reads and transaction reuse across resources before creating cleanup debt" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    var wrong = f.a.backend;
    wrong.incarnation = @splat(9);
    try std.testing.expectError(error.InvalidLockOwner, readOwner(a, &store, f.ns, wrong));
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    var request = f.request(f.a, 1);
    request.action.try_acquire.scope = .{ .transaction = .{ .id = @splat(9), .subtransaction = 0 } };
    _ = try store.effect(f.ns, request, context, null);
    request.sequence = 2;
    request.action.try_acquire.resource = @splat(7);
    request.action.try_acquire.scope.transaction.id = @splat(10);
    try std.testing.expectError(error.LockOwnerChanged, store.effect(f.ns, request, context, null));
    try std.testing.expectEqual(@as(u32, 1), (try readOwner(a, &store, f.ns, f.a.backend)).?.resources);
    try std.testing.expectEqual(@as(u64, 2), (try readOwner(a, &store, f.ns, f.a.backend)).?.next_effect);
    var invalid_control = f.request(f.a, 2);
    invalid_control.action = .{ .control = .{ .rollback = .{ .transaction = @splat(10), .first_aborted = 0 } } };
    try std.testing.expectError(error.LockOwnerChanged, store.effect(f.ns, invalid_control, context, null));
    try std.testing.expect((try readOwner(a, &store, f.ns, f.a.backend)).?.job == null);
}

test "pessimistic lock compatible queue admission uses fixed point probes instead of loading queued backend records" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, ca);
    _ = try store.effect(f.ns, f.request(f.a, 1), ca, null);
    var last: Lease = undefined;
    for (1..9) |number| {
        var lease = f.b;
        lease.backend.id = @splat(@as(u8, @intCast(number + 10)));
        const context: TestContext = .{ .expected = lease };
        try register(a, &store, f.ns, lease, context);
        var request = f.request(lease, 1);
        request.action = .{ .acquire = .{ .resource = @splat(6), .mode = .{ .advisory = .shared }, .scope = .session } };
        _ = try store.effect(f.ns, request, context, null);
        last = lease;
    }
    const ProbeTxn = struct {
        store: *TestStore,
        gets: usize = 0,
        cursors: usize = 0,
        fn get(self: *@This(), address: []const u8) ![]const u8 {
            self.gets += 1;
            return self.store.get(address);
        }
        fn put(self: *@This(), address: []const u8, value: []const u8) !void {
            return self.store.put(address, value);
        }
        fn delete(self: *@This(), address: []const u8) !void {
            return self.store.delete(address);
        }
        fn openCursor(self: *@This()) !TestStore.Cursor {
            self.cursors += 1;
            return self.store.openCursor();
        }
    };
    var fresh = f.b;
    fresh.backend.id = @splat(99);
    const context: TestContext = .{ .expected = fresh };
    try register(a, &store, f.ns, fresh, context);
    var probe: ProbeTxn = .{ .store = &store };
    var request = f.request(fresh, 1);
    request.action.try_acquire.mode = .{ .advisory = .shared };
    try std.testing.expectEqual(Outcome.contended, (try apply(a, &probe, f.ns, request, context)).outcome);
    try std.testing.expectEqual(@as(usize, 6), probe.gets);
    try std.testing.expectEqual(@as(usize, 0), probe.cursors);
    // Releasing the exclusive holder leaves only compatible queued SH modes.
    // A fresh SH request may join them without fetching those eight waiters.
    var close = f.request(f.a, 2);
    close.action = .{ .control = .close };
    try store.complete(f.ns, close, ca);
    probe.gets = 0;
    request.sequence = 2;
    try std.testing.expectEqual(Outcome.granted, (try apply(a, &probe, f.ns, request, context)).outcome);
    try std.testing.expectEqual(@as(usize, 6), probe.gets);
    try std.testing.expectEqual(@as(usize, 0), probe.cursors);
    try std.testing.expect((try readOwner(a, &store, f.ns, last.backend)).?.waiting != null);
}

test "pessimistic lock transaction cleanup skips session-only resources and preserves exact per-transaction membership" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const context: TestContext = .{ .expected = f.a };
    try register(a, &store, f.ns, f.a, context);
    for (1..65) |number| {
        var request = f.request(f.a, @intCast(number));
        request.action.try_acquire.resource = @splat(0);
        std.mem.writeInt(u64, request.action.try_acquire.resource[24..32], @intCast(number), .big);
        _ = try store.effect(f.ns, request, context, null);
    }
    // An explicit transaction with no xact locks should not visit the 64
    // session resources just to end its transaction scope.
    var rollback = f.request(f.a, 65);
    rollback.action = .{ .control = .{ .rollback = .{ .transaction = @splat(9), .first_aborted = 0 } } };
    _ = try store.effect(f.ns, rollback, context, null);
    try std.testing.expectEqual(@as(u32, 0), (try readOwner(a, &store, f.ns, f.a.backend)).?.job.?.total);
    try std.testing.expectEqual(Outcome.completed, (try store.quantum(f.ns, rollback, context, null)).outcome);
    try std.testing.expectEqual(@as(u32, 64), (try readOwner(a, &store, f.ns, f.a.backend)).?.resources);
    // A mixed grant creates one transaction membership alongside its global
    // membership. Another session-only grant does not enter that cohort.
    var acquire = f.request(f.a, 66);
    acquire.action.try_acquire.resource = @splat(0);
    std.mem.writeInt(u64, acquire.action.try_acquire.resource[24..32], 1, .big);
    acquire.action.try_acquire.scope = .{ .transaction = .{ .id = @splat(9), .subtransaction = 4 } };
    _ = try store.effect(f.ns, acquire, context, null);
    rollback.sequence = 67;
    _ = try store.effect(f.ns, rollback, context, null);
    const pending = (try readOwner(a, &store, f.ns, f.a.backend)).?;
    try std.testing.expectEqual(@as(u32, 1), pending.job.?.total);
    try std.testing.expectEqual(@as(u32, 1), pending.transaction_resources);
    _ = try store.quantum(f.ns, rollback, context, null);
    try std.testing.expectEqual(Outcome.completed, (try store.quantum(f.ns, rollback, context, null)).outcome);
    const owner = (try readOwner(a, &store, f.ns, f.a.backend)).?;
    try std.testing.expectEqual(@as(u32, 64), owner.resources);
    try std.testing.expectEqual(@as(u32, 0), owner.transaction_resources);
    try std.testing.expectEqual(@as(u32, 0), owner.mixed_resources);
    try std.testing.expect(owner.transaction == null);
}

test "pessimistic lock deadlock proof rejects a checksummed grant copied from another resource before admitting a waiter" {
    const a = std.testing.allocator;
    const f = TestFixture;
    var store: TestStore = .{ .a = a };
    defer store.deinit();
    const ca: TestContext = .{ .expected = f.a };
    const cb: TestContext = .{ .expected = f.b };
    try register(a, &store, f.ns, f.a, ca);
    try register(a, &store, f.ns, f.b, cb);
    _ = try store.effect(f.ns, f.request(f.a, 1), ca, null);
    var copied = (try f.grant(&store, f.a)).?;
    copied.resource = @splat(7);
    const valid_checksum = try copied.encode();
    var buffer: [256]u8 = undefined;
    try store.put(try key(&buffer, f.ns, .holder, f.a.backend, @splat(6), 0), &valid_checksum);
    var request = f.request(f.b, 1);
    request.action = .{ .acquire = .{ .resource = @splat(6), .mode = .{ .advisory = .exclusive }, .scope = .session } };
    try std.testing.expectError(error.InvalidLockRecord, store.effect(f.ns, request, cb, null));
    const owner = (try readOwner(a, &store, f.ns, f.b.backend)).?;
    try std.testing.expectEqual(@as(u64, 1), owner.next_effect);
    try std.testing.expect(owner.waiting == null);
    try std.testing.expect((try observe(a, &store, f.ns, request, cb)) == null);
}
