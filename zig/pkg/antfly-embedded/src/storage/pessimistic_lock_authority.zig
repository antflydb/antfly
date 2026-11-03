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

//! Native command authority for the lock journal. Every operation runs inside
//! the accepting writer transaction, which the caller MUST abort on any error.
//! Expiry fences execution first; only independent decision authority permits
//! lock retirement. This module does not sample clocks or decide transactions.
const std = @import("std");
pub const ledger = @import("pessimistic_lock_ledger.zig");
const locks = @import("pessimistic_lock_contract.zig");
const A = std.mem.Allocator;
pub const max_active = 4096;
const max_record_bytes = 4096;

pub const Registration = struct {
    lease: ledger.Lease,
    connection: [32]u8,
    nonce: [16]u8,
    standalone: bool,
    pub fn validate(self: Registration, ns: ledger.Namespace) !void {
        try self.lease.validate(ns);
        if ((self.lease.node == 0) != self.standalone or std.mem.allEqual(u8, &self.nonce, 0) or std.mem.allEqual(u8, &self.connection, '0')) return error.InvalidLockOwner;
        for (self.connection) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidLockOwner;
    }
};
pub const Command = struct {
    sequence: u64,
    nonce: [16]u8,
    /// Immutable source execution identity; a command is never reassigned.
    executor: [16]u8,
    pub fn validate(self: Command) !void {
        if (self.sequence == 0 or self.sequence == std.math.maxInt(u64) or std.mem.allEqual(u8, &self.nonce, 0) or std.mem.allEqual(u8, &self.executor, 0)) return error.InvalidLockEffect;
    }
};
/// Restrictive metadata-effect fence. This grants no SQL/catalog privileges;
/// the accepting writer independently joins it to the live command scope.
pub const CommandFence = struct {
    backend: locks.Backend,
    command: Command,
    database: u64,
    principal: [32]u8,
    standalone: bool,

    pub fn validate(self: CommandFence) !void {
        try self.backend.validate();
        try self.command.validate();
        if (self.database == 0 or std.mem.allEqual(u8, &self.principal, 0)) return error.InvalidLockScope;
    }
    pub fn requireCommand(self: CommandFence, _: anytype, _: ledger.Namespace, value: Record, command: Command) !void {
        if (!self.backend.eql(value.registration.lease.backend) or !std.meta.eql(self.command, command) or
            self.database != value.registration.lease.database or self.standalone != value.registration.standalone or
            !std.mem.eql(u8, &self.principal, &value.registration.lease.principal)) return error.Forbidden;
    }
    /// Extend existing fixed-width request hashes without changing legacy
    /// requests. Never hash struct padding or native-endian integer bytes.
    pub fn hashInto(self: CommandFence, hash: anytype) void {
        hash.update("antfly/sql-command-fence/v1");
        hash.update(&self.backend.incarnation);
        hash.update(&self.backend.id);
        inline for (.{ self.backend.fence, self.command.sequence, self.database }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .little);
            hash.update(&bytes);
        }
        hash.update(&self.command.nonce);
        hash.update(&self.command.executor);
        hash.update(&self.principal);
        hash.update(&.{@intFromBool(self.standalone)});
    }
};
/// This is a reference to the existing external decision owner, not a second
/// decision. The receiver must join it to its native registry in requireSeal
/// and requireResolution, including the complete immutable participant cohort.
pub const DecisionRef = struct {
    operation: [16]u8,
    admission: [32]u8,
    pub fn validate(self: DecisionRef) !void {
        if (std.mem.allEqual(u8, &self.operation, 0) or std.mem.allEqual(u8, &self.admission, 0)) return error.InvalidLockScope;
    }
};
pub const Transaction = struct { id: [16]u8, decision: ?DecisionRef = null };
pub const Record = struct {
    registration: Registration,
    next_command: u64 = 1,
    command: ?Command = null,
    completed: ?Command = null,
    transaction: ?Transaction = null,
    released_transaction: ?Transaction = null,
    releasing: bool = false,
    state: enum { active, fenced, retired } = .active,
    pub fn validate(self: Record, ns: ledger.Namespace) !void {
        try self.registration.validate(ns);
        if (self.next_command == 0) return error.InvalidLockRecord;
        if (self.command) |command| {
            try command.validate();
            if (command.sequence != self.next_command or self.state != .active) return error.InvalidLockRecord;
        }
        if (self.completed) |command| {
            try command.validate();
            if (command.sequence + 1 != self.next_command) return error.InvalidLockRecord;
        } else if (self.next_command != 1) return error.InvalidLockRecord;
        for ([_]?Transaction{ self.transaction, self.released_transaction }) |entry| if (entry) |transaction| {
            if (std.mem.allEqual(u8, &transaction.id, 0)) return error.InvalidLockRecord;
            if (transaction.decision) |decision| try decision.validate();
        };
        if (self.releasing and self.transaction == null) return error.InvalidLockRecord;
        if (self.state == .retired and self.transaction != null) return error.InvalidLockRecord;
    }
};
pub const Begin = enum { admitted, active, completed };
pub const Fence = union(enum) { close, expired: struct { expected_expiry: u64, observed_at_ms: u64 } };

fn key(buffer: []u8, ns: ledger.Namespace, backend: ?locks.Backend) ![]const u8 {
    try ns.validate();
    if (ns.home != .metadata) return error.InvalidLockScope;
    if (backend) |owner| {
        try owner.validate();
        if (!std.mem.eql(u8, &owner.incarnation, &ns.incarnation)) return error.InvalidLockOwner;
        return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_scope:{x:0>16}:{s}:{s}:{x:0>16}", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), std.fmt.bytesToHex(owner.id, .lower), owner.fence });
    }
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_scope:{x:0>16}:{s}:count", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower) });
}
pub const Work = struct { at_ms: u64, backend: locks.Backend };
pub const WorkItem = struct { work: Work, scope: Record };
fn workKey(buffer: []u8, ns: ledger.Namespace, work: Work) ![]const u8 {
    try ns.validate();
    if (ns.home != .metadata) return error.InvalidLockScope;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_work:{x:0>16}:{s}:{x:0>16}:{s}:{x:0>16}", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), work.at_ms, std.fmt.bytesToHex(work.backend.id, .lower), work.backend.fence });
}
fn workFor(value: Record) Work {
    return .{ .at_ms = if (value.state == .active and !value.releasing) value.registration.lease.expires_at_ms else 0, .backend = value.registration.lease.backend };
}
fn writeWork(txn: anytype, ns: ledger.Namespace, value: Record) !void {
    var buffer: [192]u8 = undefined;
    try txn.put(try workKey(&buffer, ns, workFor(value)), &value.registration.nonce);
}
fn eraseWork(txn: anytype, ns: ledger.Namespace, value: Record) !void {
    var buffer: [192]u8 = undefined;
    try txn.delete(try workKey(&buffer, ns, workFor(value)));
}
/// One seek and one scope read. Workers persist/reuse the returned key as a
/// cursor; busy decisions cannot starve later expired backends. A new pass
/// starts at null so renewals and newly fenced owners are eventually visited.
pub fn readWork(backing: A, txn: anytype, ns: ledger.Namespace, observed_at_ms: u64, after: ?Work) !?WorkItem {
    var buffer: [192]u8 = undefined;
    const zero: Work = .{ .at_ms = 0, .backend = .{ .incarnation = ns.incarnation, .id = @splat(0), .fence = 0 } };
    const address = try workKey(&buffer, ns, after orelse zero);
    const suffix_len = 16 + 1 + 32 + 1 + 16;
    const prefix = address[0 .. address.len - suffix_len];
    var cursor = try txn.openCursor();
    defer cursor.close();
    var pair = try cursor.seekAtOrAfter(if (after != null) address else prefix);
    if (after != null) if (pair) |item| if (std.mem.eql(u8, item.key, address)) {
        pair = try cursor.next();
    };
    const item = pair orelse return null;
    if (!std.mem.startsWith(u8, item.key, prefix)) return null;
    if (item.key.len != address.len or item.value.len != 16) return error.InvalidLockRecord;
    const suffix = item.key[prefix.len..];
    if (suffix[16] != ':' or suffix[49] != ':') return error.InvalidLockRecord;
    var work: Work = .{
        .at_ms = std.fmt.parseInt(u64, suffix[0..16], 16) catch return error.InvalidLockRecord,
        .backend = .{ .incarnation = ns.incarnation, .id = undefined, .fence = std.fmt.parseInt(u64, suffix[50..66], 16) catch return error.InvalidLockRecord },
    };
    _ = std.fmt.hexToBytes(&work.backend.id, suffix[17..49]) catch return error.InvalidLockRecord;
    if (work.at_ms > observed_at_ms) return null;
    const nonce = item.value[0..16].*;
    var canonical: [192]u8 = undefined;
    if (!std.mem.eql(u8, try workKey(&canonical, ns, work), item.key)) return error.InvalidLockRecord;
    const value = try read(backing, txn, ns, work.backend) orelse return error.InvalidLockRecord;
    if (value.state == .retired or !std.meta.eql(workFor(value), work) or !std.mem.eql(u8, &nonce, &value.registration.nonce)) return error.InvalidLockRecord;
    return .{ .work = work, .scope = value };
}
pub fn nextWork(backing: A, txn: anytype, ns: ledger.Namespace, observed_at_ms: u64, after: ?Work) !?Work {
    return if (try readWork(backing, txn, ns, observed_at_ms, after)) |item| item.work else null;
}
fn optional(txn: anytype, address: []const u8) !?[]const u8 {
    return txn.get(address) catch |err| if (err == error.NotFound) null else err;
}
fn encode(a: A, value: Record) ![]const u8 {
    const body = try std.json.Stringify.valueAlloc(a, value, .{});
    if (body.len > max_record_bytes - 32) return error.LockLimitExceeded;
    const bytes = try a.alloc(u8, body.len + 32);
    @memcpy(bytes[0..body.len], body);
    std.crypto.hash.Blake3.hash(body, bytes[body.len..][0..32], .{});
    return bytes;
}
fn readWith(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend) !?Record {
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(txn, try key(&buffer, ns, backend))) orelse return null;
    if (bytes.len <= 32 or bytes.len > max_record_bytes) return error.InvalidLockRecord;
    var checksum: [32]u8 = undefined;
    const body = bytes[0 .. bytes.len - 32];
    std.crypto.hash.Blake3.hash(body, &checksum, .{});
    if (!std.mem.eql(u8, &checksum, bytes[body.len..])) return error.InvalidLockRecord;
    const value = std.json.parseFromSliceLeaky(Record, a, body, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidLockRecord;
    try value.validate(ns);
    if (!value.registration.lease.backend.eql(backend) or !std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, value, .{}), body)) return error.InvalidLockRecord;
    return value;
}
pub fn read(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend) !?Record {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    return readWith(arena.allocator(), txn, ns, backend);
}
fn write(a: A, txn: anytype, ns: ledger.Namespace, value: Record) !void {
    try value.validate(ns);
    var buffer: [192]u8 = undefined;
    try txn.put(try key(&buffer, ns, value.registration.lease.backend), try encode(a, value));
}
fn count(txn: anytype, ns: ledger.Namespace) !u32 {
    var buffer: [192]u8 = undefined;
    const raw = (try optional(txn, try key(&buffer, ns, null))) orelse return 0;
    if (raw.len != 4) return error.InvalidLockRecord;
    const value = std.mem.readInt(u32, raw[0..4], .little);
    if (value > max_active) return error.InvalidLockRecord;
    return value;
}
fn writeCount(txn: anytype, ns: ledger.Namespace, value: u32) !void {
    var buffer: [192]u8 = undefined;
    var raw: [4]u8 = undefined;
    std.mem.writeInt(u32, &raw, value, .little);
    try txn.put(try key(&buffer, ns, null), &raw);
}
fn connectionKey(buffer: []u8, ns: ledger.Namespace, node: u64, connection_id: [32]u8) ![]const u8 {
    try ns.validate();
    if (ns.home != .metadata) return error.InvalidLockScope;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_lock_connection:{x:0>16}:{s}:{x:0>16}:{s}", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), node, connection_id });
}
/// Retained bindings prevent an old connection registration from adopting a
/// new backend after its original owner was fenced or retired.
pub fn connection(backing: A, txn: anytype, ns: ledger.Namespace, node: u64, id: [32]u8) !?Record {
    var buffer: [192]u8 = undefined;
    const raw = (try optional(txn, try connectionKey(&buffer, ns, node, id))) orelse return null;
    if (raw.len != 24) return error.InvalidLockRecord;
    const backend: locks.Backend = .{ .incarnation = ns.incarnation, .id = raw[0..16].*, .fence = std.mem.readInt(u64, raw[16..24], .little) };
    const value = try read(backing, txn, ns, backend) orelse return error.InvalidLockRecord;
    if (value.registration.lease.node != node or !std.mem.eql(u8, &value.registration.connection, &id)) return error.InvalidLockRecord;
    return value;
}
pub fn register(backing: A, txn: anytype, ns: ledger.Namespace, registration: Registration, authority: anytype) !void {
    try registration.validate(ns);
    try authority.requireRegistration(txn, ns, registration);
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    if (try readWith(a, txn, ns, registration.lease.backend)) |prior| {
        if (!std.meta.eql(prior.registration, registration)) return error.LockOwnerChanged;
        if (prior.state != .active) return error.LockOwnerBusy;
        return;
    }
    if (try connection(a, txn, ns, registration.lease.node, registration.connection) != null) return error.LockOwnerChanged;
    const active = try count(txn, ns);
    if (active == max_active) return error.LockLimitExceeded;
    const value: Record = .{ .registration = registration };
    try ledger.register(a, txn, ns, registration.lease, RegistrationContext{});
    try write(a, txn, ns, value);
    try writeWork(txn, ns, value);
    try writeCount(txn, ns, active + 1);
    var buffer: [192]u8 = undefined;
    var binding: [24]u8 = undefined;
    @memcpy(binding[0..16], &registration.lease.backend.id);
    std.mem.writeInt(u64, binding[16..24], registration.lease.backend.fence, .little);
    try txn.put(try connectionKey(&buffer, ns, registration.lease.node, registration.connection), &binding);
}
const RegistrationContext = struct {
    pub fn requireLease(_: @This(), _: anytype, _: ledger.Lease) !void {}
};
fn load(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend) !Record {
    return (try readWith(a, txn, ns, backend)) orelse error.LockOwnerNotRegistered;
}
fn requireFrame(value: Record, frame: Command) !void {
    try frame.validate();
    if (value.state != .active) return error.LockOwnerChanged;
    const current = value.command orelse return error.LockEffectExpired;
    if (!std.meta.eql(current, frame)) return error.LockEffectChanged;
}
/// The caller must not execute a completed replay. An active replay identifies
/// the original command; it does not create a new source execution or nonce.
pub fn begin(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, authority: anytype) !Begin {
    try command.validate();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    try authority.requireCommand(txn, ns, value, command);
    if (value.state != .active) return error.LockOwnerBusy;
    if (command.sequence < value.next_command) {
        if (value.completed) |completed| if (completed.sequence == command.sequence) {
            if (!std.meta.eql(completed, command)) return error.LockEffectChanged;
            return .completed;
        };
        return error.LockEffectExpired;
    }
    if (command.sequence != value.next_command) return error.LockEffectOutOfOrder;
    if (value.command) |current| {
        if (!std.meta.eql(current, command)) return error.LockEffectChanged;
        return .active;
    }
    value.command = command;
    try write(a, txn, ns, value);
    return .admitted;
}
/// Must run in the SAME native transaction as the protected metadata effect.
/// A preflight read is not a substitute for this accepting-writer check.
pub fn guard(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, authority: anytype) !Record {
    const value = try read(backing, txn, ns, backend) orelse return error.LockOwnerNotRegistered;
    try authority.requireCommand(txn, ns, value, command);
    try requireFrame(value, command);
    if (value.releasing) return error.LockOwnerBusy;
    if (value.transaction) |transaction| if (transaction.decision != null) return error.LockOwnerBusy;
    return value;
}
pub fn finish(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, authority: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    try authority.requireCommand(txn, ns, value, command);
    if (value.completed) |completed| if (std.meta.eql(completed, command)) return;
    try requireFrame(value, command);
    if (value.releasing) return error.LockOwnerBusy;
    if (value.transaction) |transaction| if (transaction.decision != null) return error.LockOwnerBusy;
    const owner = try ledger.readOwner(a, txn, ns, backend) orelse return error.InvalidLockRecord;
    if (owner.waiting != null or owner.job != null) return error.LockOwnerBusy;
    value.command = null;
    value.completed = command;
    value.next_command += 1;
    try write(a, txn, ns, value);
}
pub fn bindTransaction(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, id: [16]u8, authority: anytype) !void {
    if (std.mem.allEqual(u8, &id, 0)) return error.InvalidLockScope;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try guard(a, txn, ns, backend, command, authority);
    try authority.requireTransaction(txn, ns, value, id);
    if (value.transaction) |current| {
        if (!std.mem.eql(u8, &current.id, &id)) return error.LockOwnerChanged;
        return;
    }
    if (value.released_transaction) |released| if (std.mem.eql(u8, &released.id, &id)) return error.LockEffectExpired;
    value.transaction = .{ .id = id };
    try write(a, txn, ns, value);
}
/// The external admission must be installed in this same writer transaction.
/// requireSeal independently reads that admission; a source's claimed proof or
/// a successful prepare vote alone cannot authorize releasing any lock.
pub fn seal(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, decision: DecisionRef, authority: anytype) !void {
    try decision.validate();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    try authority.requireCommand(txn, ns, value, command);
    try requireFrame(value, command);
    var transaction = value.transaction orelse return error.InvalidLockScope;
    const owner = try ledger.readOwner(a, txn, ns, backend) orelse return error.InvalidLockRecord;
    if (owner.waiting != null or owner.job != null) return error.LockOwnerBusy;
    try authority.requireSeal(txn, ns, value, decision);
    if (transaction.decision) |prior| {
        if (!std.meta.eql(prior, decision)) return error.LockOwnerChanged;
        return;
    }
    transaction.decision = decision;
    value.transaction = transaction;
    try write(a, txn, ns, value);
}
/// Resolution authority must prove complete existing-2PC resolution, or an
/// unsealed local abort that could never have installed native participants.
pub fn releaseTransaction(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, expected: Transaction, authority: anytype) !bool {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    try authority.requireResolution(txn, ns, value, expected);
    if (value.transaction) |current| {
        if (!std.meta.eql(current, expected)) return error.LockOwnerChanged;
        if (!value.releasing) {
            const prior_work = workFor(value);
            value.releasing = true;
            if (!std.meta.eql(prior_work, workFor(value))) {
                var buffer: [192]u8 = undefined;
                try txn.delete(try workKey(&buffer, ns, prior_work));
                try writeWork(txn, ns, value);
            }
            try write(a, txn, ns, value);
        }
        const owner = try ledger.readOwner(a, txn, ns, backend) orelse return error.InvalidLockRecord;
        const context: ResolutionContext(@TypeOf(authority)) = .{ .authority = authority, .value = value, .ns = ns, .transaction = expected };
        if (owner.waiting) |waiting| {
            _ = try ledger.cancel(a, txn, ns, waiting, context);
            return false;
        }
        if (owner.job) |job| {
            if (job.effect.action.control == .prepare or job.effect.action.control == .close) return error.LockOwnerBusy;
            _ = try ledger.step(a, txn, ns, job.effect, context);
            return false;
        }
        if (owner.transaction) |id| {
            if (!std.mem.eql(u8, &id, &expected.id)) return error.InvalidLockRecord;
            var nonce: [32]u8 = undefined;
            var h = std.crypto.hash.Blake3.init(.{});
            h.update("antfly/lock-transaction-release/v1");
            h.update(&value.registration.nonce);
            h.update(&expected.id);
            h.final(&nonce);
            _ = try ledger.apply(a, txn, ns, .{ .backend = backend, .sequence = owner.next_effect, .nonce = nonce[0..16].*, .action = .{ .control = .{ .rollback = .{ .transaction = expected.id, .first_aborted = 0 } } } }, context);
            return false;
        }
        if (owner.transaction_resources != 0) return error.InvalidLockRecord;
        const prior_work = workFor(value);
        value.releasing = false;
        if (!std.meta.eql(prior_work, workFor(value))) {
            var buffer: [192]u8 = undefined;
            try txn.delete(try workKey(&buffer, ns, prior_work));
            try writeWork(txn, ns, value);
        }
        value.transaction = null;
        value.released_transaction = expected;
        if (expected.decision != null) if (value.command) |command| {
            value.command = null;
            value.completed = command;
            value.next_command += 1;
        };
        try write(a, txn, ns, value);
    } else if (!std.meta.eql(value.released_transaction, @as(?Transaction, expected))) return error.LockOwnerChanged;
    return true;
}
fn ResolutionContext(comptime Authority: type) type {
    return struct {
        authority: Authority,
        value: Record,
        ns: ledger.Namespace,
        transaction: Transaction,
        pub fn requireEffect(self: @This(), txn: anytype, lease: ledger.Lease, request: ledger.Effect) !void {
            if (!std.meta.eql(self.value.registration.lease, lease) or !request.backend.eql(lease.backend)) return error.LockOwnerChanged;
            if (request.action == .control) switch (request.action.control) {
                .rollback => |rollback| if (!std.mem.eql(u8, &rollback.transaction, &self.transaction.id)) return error.InvalidLockScope,
                .release_savepoint => |release| if (!std.mem.eql(u8, &release.transaction, &self.transaction.id)) return error.InvalidLockScope,
                .unlock_all => {},
                .prepare, .close => return error.InvalidLockEffect,
            };
            try self.authority.requireResolution(txn, self.ns, self.value, self.transaction);
        }
        pub fn requireObservation(self: @This(), txn: anytype, lease: ledger.Lease, request: ledger.Effect) !void {
            try self.requireEffect(txn, lease, request);
        }
        pub fn requirePrepare(_: @This(), _: anytype, _: ledger.Lease, _: ledger.Lease, _: [16]u8) !void {
            return error.InvalidLockEffect;
        }
    };
}
/// A renewal never changes backend identity, command serial or queued effects.
/// expected_expiry prevents a delayed renewal from overwriting a later lease.
pub fn renew(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, expected_expiry: u64, expires: u64, authority: anytype) !void {
    if (expires <= expected_expiry) return error.InvalidLockOwner;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    try authority.requireRenewal(txn, ns, value, expected_expiry, expires);
    if (value.state != .active) return error.LockOwnerBusy;
    if (value.registration.lease.expires_at_ms == expires) return;
    if (value.registration.lease.expires_at_ms != expected_expiry) return error.LockOwnerChanged;
    var next = value.registration.lease;
    next.expires_at_ms = expires;
    try ledger.renew(a, txn, ns, value.registration.lease, next, RenewalContext{});
    try eraseWork(txn, ns, value);
    value.registration.lease = next;
    try writeWork(txn, ns, value);
    try write(a, txn, ns, value);
}
const RenewalContext = struct {
    pub fn requireRenewal(_: @This(), _: anytype, _: ledger.Lease, _: ledger.Lease) !void {}
};
pub fn fence(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, reason: Fence, authority: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    try authority.requireFence(txn, ns, value, reason);
    if (value.state != .active) return;
    if (reason == .expired and (value.registration.lease.expires_at_ms != reason.expired.expected_expiry or reason.expired.observed_at_ms < reason.expired.expected_expiry)) return error.LockOwnerChanged;
    try eraseWork(txn, ns, value);
    value.state = .fenced;
    value.command = null;
    try writeWork(txn, ns, value);
    try write(a, txn, ns, value);
}
const EffectContext = struct {
    registration: Registration,
    transaction: ?Transaction,
    fn requireLeaseValue(self: @This(), value: ledger.Lease) !void {
        if (!std.meta.eql(self.registration.lease, value)) return error.LockOwnerChanged;
    }
    pub fn requireEffect(self: @This(), _: anytype, value: ledger.Lease, effect: ledger.Effect) !void {
        try self.requireLeaseValue(value);
        if (!effect.backend.eql(value.backend)) return error.LockOwnerChanged;
        if (effect.action == .acquire or effect.action == .try_acquire) {
            const acquisition = if (effect.action == .acquire) effect.action.acquire else effect.action.try_acquire;
            if (acquisition.scope == .transaction) {
                const transaction = self.transaction orelse return error.InvalidLockScope;
                if (!std.mem.eql(u8, &transaction.id, &acquisition.scope.transaction.id)) return error.InvalidLockScope;
            }
        }
        if (effect.action == .control) switch (effect.action.control) {
            .unlock_all => {},
            .rollback => |rollback| {
                const transaction = self.transaction orelse return error.InvalidLockScope;
                if (!std.mem.eql(u8, &transaction.id, &rollback.transaction)) return error.InvalidLockScope;
            },
            .release_savepoint => |release| {
                const transaction = self.transaction orelse return error.InvalidLockScope;
                if (!std.mem.eql(u8, &transaction.id, &release.transaction)) return error.InvalidLockScope;
            },
            .prepare, .close => return error.InvalidLockEffect,
        };
    }
    pub fn requireObservation(self: @This(), txn: anytype, value: ledger.Lease, effect: ledger.Effect) !void {
        try self.requireEffect(txn, value, effect);
    }
    pub fn requirePrepare(_: @This(), _: anytype, _: ledger.Lease, _: ledger.Lease, _: [16]u8) !void {
        return error.InvalidLockEffect;
    }
};
pub fn applyEffect(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, request: ledger.Effect, authority: anytype) !ledger.Receipt {
    const value = try guard(backing, txn, ns, backend, command, authority);
    if (!request.backend.eql(backend)) return error.LockOwnerChanged;
    return ledger.apply(backing, txn, ns, request, EffectContext{ .registration = value.registration, .transaction = value.transaction });
}
pub fn cancelEffect(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, request: ledger.Effect, authority: anytype) !ledger.Receipt {
    const value = try guard(backing, txn, ns, backend, command, authority);
    if (!request.backend.eql(backend)) return error.LockOwnerChanged;
    return ledger.cancel(backing, txn, ns, request, EffectContext{ .registration = value.registration, .transaction = value.transaction });
}
pub fn advanceEffect(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, request: ledger.Effect, authority: anytype) !ledger.Receipt {
    const value = try guard(backing, txn, ns, backend, command, authority);
    if (!request.backend.eql(backend)) return error.LockOwnerChanged;
    return ledger.step(backing, txn, ns, request, EffectContext{ .registration = value.registration, .transaction = value.transaction });
}
/// One cancellation, existing job quantum, or close quantum per call. No owner
/// cohort is materialized and no loop runs under the native apply lock.
pub fn closeStep(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, authority: anytype) !bool {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    if (value.state == .active) return error.LockOwnerBusy;
    try authority.requireRetirement(txn, ns, value);
    if (value.state == .retired) return true;
    if (value.transaction != null) return error.LockOwnerBusy;
    const owner = try ledger.readOwner(a, txn, ns, backend) orelse return error.InvalidLockRecord;
    if (!std.meta.eql(owner.lease, value.registration.lease)) return error.InvalidLockRecord;
    const context: CloseContext(@TypeOf(authority)) = .{ .authority = authority, .value = value, .ns = ns };
    if (owner.waiting) |waiting| {
        _ = try ledger.cancel(a, txn, ns, waiting, context);
        return false;
    }
    if (owner.job) |job| {
        _ = try ledger.step(a, txn, ns, job.effect, context);
        return false;
    }
    if (owner.state != .retired) {
        if (owner.state != .active) return error.LockOwnerBusy;
        var nonce: [32]u8 = undefined;
        var h = std.crypto.hash.Blake3.init(.{});
        h.update("antfly/lock-scope-close/v1");
        h.update(&value.registration.nonce);
        h.final(&nonce);
        _ = try ledger.apply(a, txn, ns, .{ .backend = backend, .sequence = owner.next_effect, .nonce = nonce[0..16].*, .action = .{ .control = .close } }, context);
        return false;
    }
    const active = try count(txn, ns);
    if (active == 0) return error.InvalidLockRecord;
    try eraseWork(txn, ns, value);
    value.state = .retired;
    try write(a, txn, ns, value);
    try writeCount(txn, ns, active - 1);
    return true;
}
fn CloseContext(comptime Authority: type) type {
    return struct {
        authority: Authority,
        value: Record,
        ns: ledger.Namespace,
        pub fn requireEffect(self: @This(), txn: anytype, lease: ledger.Lease, _: ledger.Effect) !void {
            if (!std.meta.eql(self.value.registration.lease, lease)) return error.LockOwnerChanged;
            try self.authority.requireRetirement(txn, self.ns, self.value);
        }
        pub fn requireObservation(self: @This(), txn: anytype, lease: ledger.Lease, request: ledger.Effect) !void {
            try self.requireEffect(txn, lease, request);
        }
        pub fn requirePrepare(self: @This(), txn: anytype, from: ledger.Lease, to: ledger.Lease, transaction: [16]u8) !void {
            try self.authority.requirePreparedHandoff(txn, self.ns, self.value, from, to, transaction);
        }
    };
}
