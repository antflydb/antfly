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
    group: u64 = @import("../common/group_ids.zig").main_metadata_group_id,
    lease: ledger.Lease,
    connection: [32]u8,
    nonce: [16]u8,
    standalone: bool,
    pub fn validate(self: Registration, ns: ledger.Namespace) !void {
        try self.lease.validate(ns);
        if (self.group != ns.group) return error.InvalidLockOwner;
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
    group: u64 = @import("../common/group_ids.zig").main_metadata_group_id,
    backend: locks.Backend,
    command: Command,
    database: u64,
    principal: [32]u8,
    standalone: bool,

    pub fn validate(self: CommandFence) !void {
        try self.backend.validate();
        try self.command.validate();
        if (self.group == 0 or self.database == 0 or std.mem.allEqual(u8, &self.principal, 0)) return error.InvalidLockScope;
    }
    pub fn requireCommand(self: CommandFence, _: anytype, ns: ledger.Namespace, value: Record, command: Command) !void {
        if (ns.home != .metadata or self.group != ns.group or self.group != value.registration.group or !self.backend.eql(value.registration.lease.backend) or !std.meta.eql(self.command, command) or
            self.database != value.registration.lease.database or self.standalone != value.registration.standalone or
            !std.mem.eql(u8, &self.principal, &value.registration.lease.principal)) return error.Forbidden;
    }
    /// Extend existing fixed-width request hashes without changing legacy
    /// requests. Never hash struct padding or native-endian integer bytes.
    pub fn hashInto(self: CommandFence, hash: anytype) void {
        hash.update("antfly/sql-command-fence/v2");
        hash.update(&self.backend.incarnation);
        hash.update(&self.backend.id);
        inline for (.{ self.group, self.backend.fence, self.command.sequence, self.database }) |number| {
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
    data: ?Data.Header = null,
    next_command: u64 = 1,
    command: ?Command = null,
    completed: ?Command = null,
    transaction: ?Transaction = null,
    released_transaction: ?Transaction = null,
    releasing: bool = false,
    state: enum { active, fenced, retired } = .active,
    pub fn validate(self: Record, ns: ledger.Namespace) !void {
        try self.registration.validate(ns);
        if (self.data) |directory| {
            try directory.validate();
            if (ns.home != .metadata or (self.state == .retired and directory.count() != 0)) return error.InvalidLockRecord;
        }
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
/// Durable participant directory owned by the existing metadata command.
/// Enrollment precedes the first data RPC; FINISH and retirement cannot pass
/// outstanding native acknowledgements. Every operation is bounded by the
/// existing 64-member external transaction cohort, never by table row count.
pub const Data = struct {
    pub const capacity = 64;
    pub const Header = struct {
        next_generation: u64 = 1,
        slots: [capacity]u64 = @splat(0),
        open: u8 = 0,
        pub fn count(self: Header) usize {
            var total: usize = 0;
            for (self.slots) |generation| if (generation != 0) {
                total += 1;
            };
            return total;
        }
        pub fn validate(self: Header) !void {
            if (self.next_generation == 0 or self.open > self.count()) return error.InvalidLockRecord;
            for (self.slots, 0..) |generation, i| {
                if (generation >= self.next_generation) return error.InvalidLockRecord;
                if (generation != 0) for (self.slots[0..i]) |previous| if (generation == previous) return error.InvalidLockRecord;
            }
        }
    };
    pub const Spec = struct {
        table_name: []const u8,
        route: @import("../metadata/catalog_route_contract.zig").CatalogRouteFence,
        schema_version: ?u32 = null,
        pub fn validate(self: Spec, ns: ledger.Namespace) !void {
            try self.route.validate();
            const incarnation = self.route.metadata_incarnation orelse return error.InvalidLockResource;
            if (ns.home != .metadata or self.route.metadata_group_id != ns.group or !std.mem.eql(u8, &incarnation, &std.fmt.bytesToHex(ns.incarnation, .lower)) or
                self.table_name.len == 0 or self.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(self.table_name) or std.mem.indexOfScalar(u8, self.table_name, 0) != null or self.route.topology_epoch == 0) return error.InvalidLockResource;
        }
        pub fn binding(self: Spec, ns: ledger.Namespace) !@import("pessimistic_lock_resource.zig").Binding {
            try self.validate(ns);
            const identity = self.route.route.identity_namespace;
            return .{ .namespace = .{ .home = .data, .group = self.route.route.group_id, .incarnation = ns.incarnation }, .identity = .{ .table_id = identity.table_id, .shard_id = identity.shard_id, .range_id = identity.range_id } };
        }
    };
    pub const Participant = struct {
        spec: Spec,
        admission: @import("pessimistic_lock_participant.zig").Admission,
        command_open: bool = true,
        pub fn validate(self: Participant, ns: ledger.Namespace, backend: locks.Backend) !void {
            try self.spec.validate(ns);
            try self.admission.validate();
            if (!self.admission.source.lease.backend.eql(backend) or !self.admission.binding.eql(try self.spec.binding(ns)) or self.admission.purpose != .execution) return error.InvalidLockRecord;
        }
    };
    fn participantKey(buffer: []u8, ns: ledger.Namespace, backend: locks.Backend, generation: u64) ![]const u8 {
        var root_buffer: [192]u8 = undefined;
        const root = try key(&root_buffer, ns, backend);
        return std.fmt.bufPrint(buffer, "{s}:participant:{x:0>16}", .{ root, generation });
    }
    fn lookupKey(buffer: []u8, ns: ledger.Namespace, backend: locks.Backend, spec: Spec, transaction: [16]u8) ![]const u8 {
        var root_buffer: [192]u8 = undefined;
        const root = try key(&root_buffer, ns, backend);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/data-lock-enrollment/v1");
        hash.update(&try (try spec.binding(ns)).encode());
        hash.update(&transaction);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return std.fmt.bufPrint(buffer, "{s}:route:{s}", .{ root, std.fmt.bytesToHex(digest, .lower) });
    }
    fn tableCountKey(buffer: []u8, ns: ledger.Namespace, table: u64) ![]const u8 {
        return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:table:{x:0>16}:participants", .{ homePrefix(ns, .scope), ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), table });
    }
    /// Unreclaimed participant debt blocks topology/restore/schema retirement
    /// in O(1), including enrollment whose first data BEGIN was never delivered.
    pub fn tableCount(txn: anytype, ns: ledger.Namespace, table: u64) !u32 {
        var buffer: [256]u8 = undefined;
        const raw = (try optional(txn, try tableCountKey(&buffer, ns, table))) orelse return 0;
        if (raw.len != 4) return error.InvalidLockRecord;
        const total = std.mem.readInt(u32, raw[0..4], .little);
        if (total == 0 or total > max_active * capacity) return error.InvalidLockRecord;
        return total;
    }
    fn writeTableCount(txn: anytype, ns: ledger.Namespace, table: u64, total: u32) !void {
        var buffer: [256]u8 = undefined;
        const address = try tableCountKey(&buffer, ns, table);
        if (total == 0) return txn.delete(address);
        var raw: [4]u8 = undefined;
        std.mem.writeInt(u32, &raw, total, .little);
        try txn.put(address, &raw);
    }
    pub fn readParticipant(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, generation: u64) !?Participant {
        var buffer: [256]u8 = undefined;
        const raw = (try optional(txn, try participantKey(&buffer, ns, backend, generation))) orelse return null;
        if (raw.len > 8192 or raw.len <= 32) return error.InvalidLockRecord;
        const body = raw[0 .. raw.len - 32];
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(body, &checksum, .{});
        if (!std.mem.eql(u8, raw[body.len..], &checksum)) return error.InvalidLockRecord;
        const result = try std.json.parseFromSliceLeaky(Participant, a, body, .{ .allocate = .alloc_always });
        try result.validate(ns, backend);
        if (result.admission.generation != generation or !std.mem.eql(u8, body, try std.json.Stringify.valueAlloc(a, result, .{}))) return error.InvalidLockRecord;
        return result;
    }
    pub fn find(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, spec: Spec, transaction: [16]u8) !?Participant {
        var buffer: [256]u8 = undefined;
        const raw = (try optional(txn, try lookupKey(&buffer, ns, backend, spec, transaction))) orelse return null;
        if (raw.len != 8) return error.InvalidLockRecord;
        const generation = std.mem.readInt(u64, raw[0..8], .little);
        const value = try load(a, txn, ns, backend);
        const directory = value.data orelse return error.InvalidLockRecord;
        if (std.mem.indexOfScalar(u64, &directory.slots, generation) == null) return error.InvalidLockRecord;
        return try readParticipant(a, txn, ns, backend, generation) orelse error.InvalidLockRecord;
    }
    fn writeParticipant(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, value: Participant) !void {
        try value.validate(ns, backend);
        const body = try std.json.Stringify.valueAlloc(a, value, .{});
        if (body.len + 32 > 8192) return error.InvalidLockRecord;
        const bytes = try a.alloc(u8, body.len + 32);
        @memcpy(bytes[0..body.len], body);
        std.crypto.hash.Blake3.hash(body, bytes[body.len..][0..32], .{});
        var buffer: [256]u8 = undefined;
        try txn.put(try participantKey(&buffer, ns, backend, value.admission.generation), bytes);
    }
    /// Returns a caller-allocator-owned descriptor. An exact completed replay
    /// stays completed; it cannot reopen the same native command.
    pub fn enroll(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, transaction: [16]u8, spec: Spec, authority: anytype) !Participant {
        try spec.validate(ns);
        var value = try guard(a, txn, ns, backend, command, authority);
        try authority.requireTransaction(txn, ns, value, transaction);
        try authority.requireDataEnrollment(txn, ns, value, spec);
        if (std.mem.allEqual(u8, &transaction, 0)) return error.InvalidLockScope;
        if (value.transaction) |current| {
            if (!std.mem.eql(u8, &current.id, &transaction) or current.decision != null) return error.LockOwnerChanged;
        } else {
            if (value.released_transaction) |released| if (std.mem.eql(u8, &released.id, &transaction)) return error.LockEffectExpired;
            value.transaction = .{ .id = transaction };
        }
        var directory = value.data orelse Header{};
        var lookup_buffer: [256]u8 = undefined;
        const lookup_address = try lookupKey(&lookup_buffer, ns, backend, spec, transaction);
        if (try optional(txn, lookup_address)) |raw| {
            if (raw.len != 8) return error.InvalidLockRecord;
            const generation = std.mem.readInt(u64, raw[0..8], .little);
            const slot = std.mem.indexOfScalar(u64, &directory.slots, generation) orelse return error.LockOwnerChanged;
            _ = slot;
            var prior = try readParticipant(a, txn, ns, backend, generation) orelse return error.InvalidLockRecord;
            // An unrelated catalog write may advance the revision without
            // changing this participant's physical or schema identity. The
            // accepting metadata writer independently validates the new cut.
            var previous_spec = prior.spec;
            previous_spec.route.catalog_revision = spec.route.catalog_revision;
            if (!std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, previous_spec, .{}), try std.json.Stringify.valueAlloc(a, spec, .{}))) return error.LockOwnerChanged;
            prior.spec = spec;
            if (std.meta.eql(prior.admission.parent_command, command)) {
                try writeParticipant(a, txn, ns, backend, prior);
                return prior;
            }
            if (prior.command_open or prior.admission.parent_command.sequence >= command.sequence) return error.LockOwnerBusy;
            prior.admission.parent_command = command;
            prior.admission.serial = std.math.add(u64, prior.admission.serial, 1) catch return error.LockLimitExceeded;
            prior.admission.source = value.registration;
            prior.command_open = true;
            directory.open += 1;
            value.data = directory;
            try writeParticipant(a, txn, ns, backend, prior);
            try write(a, txn, ns, value);
            return prior;
        }
        const slot = std.mem.indexOfScalar(u64, &directory.slots, 0) orelse return error.LockLimitExceeded;
        if (directory.next_generation == std.math.maxInt(u64)) return error.LockLimitExceeded;
        const result: Participant = .{ .spec = spec, .admission = .{ .purpose = .execution, .binding = try spec.binding(ns), .source = value.registration, .transaction = transaction, .generation = directory.next_generation, .parent_command = command, .serial = 1 } };
        directory.slots[slot] = directory.next_generation;
        directory.next_generation += 1;
        directory.open += 1;
        value.data = directory;
        const total = try tableCount(txn, ns, spec.route.table_id);
        if (total == max_active * capacity) return error.LockLimitExceeded;
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, result.admission.generation, .little);
        try writeParticipant(a, txn, ns, backend, result);
        try txn.put(lookup_address, &raw);
        try writeTableCount(txn, ns, spec.route.table_id, total + 1);
        try write(a, txn, ns, value);
        return result;
    }
    /// Acknowledgement is accepted only after independent native observation.
    /// A late ACK may finish its exact frame, never a newer participant command.
    pub fn acknowledge(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, generation: u64, command: Command, authority: anytype) !void {
        var value = try load(a, txn, ns, backend);
        var directory = value.data orelse return error.InvalidLockScope;
        if (std.mem.indexOfScalar(u64, &directory.slots, generation) == null) return error.LockOwnerChanged;
        var participant = try readParticipant(a, txn, ns, backend, generation) orelse return error.InvalidLockRecord;
        if (!std.meta.eql(participant.admission.parent_command, command)) return error.LockEffectChanged;
        try authority.requireDataCompletion(txn, ns, value, participant);
        if (!participant.command_open) return;
        if (directory.open == 0) return error.InvalidLockRecord;
        directory.open -= 1;
        participant.command_open = false;
        value.data = directory;
        try writeParticipant(a, txn, ns, backend, participant);
        try write(a, txn, ns, value);
    }
    /// Retained enrollment bodies fence delayed data RPCs after slot reuse.
    /// Only independent native retirement releases catalog/topology debt.
    pub fn retire(a: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, generation: u64, authority: anytype) !void {
        var value = try load(a, txn, ns, backend);
        var directory = value.data orelse return error.InvalidLockScope;
        const participant = try readParticipant(a, txn, ns, backend, generation) orelse return error.InvalidLockRecord;
        try authority.requireDataRetirement(txn, ns, value, participant);
        const slot = std.mem.indexOfScalar(u64, &directory.slots, generation) orelse return;
        if (participant.command_open) {
            if (directory.open == 0) return error.InvalidLockRecord;
            directory.open -= 1;
        }
        directory.slots[slot] = 0;
        value.data = directory;
        const total = try tableCount(txn, ns, participant.spec.route.table_id);
        if (total == 0) return error.InvalidLockRecord;
        var lookup_buffer: [256]u8 = undefined;
        try txn.delete(try lookupKey(&lookup_buffer, ns, backend, participant.spec, participant.admission.transaction));
        try writeTableCount(txn, ns, participant.spec.route.table_id, total - 1);
        try write(a, txn, ns, value);
    }
};

pub const Begin = enum { admitted, active, completed };
pub const Fence = union(enum) { close, expired: struct { expected_expiry: u64, observed_at_ms: u64 } };

const Lane = enum { scope, work, connection };
fn homePrefix(ns: ledger.Namespace, lane: Lane) []const u8 {
    return switch (lane) {
        .scope => if (ns.home == .metadata) "\x00\x00__metadata__:sql_lock_scope:" else "\x00\x00__lock_scope__:",
        .work => if (ns.home == .metadata) "\x00\x00__metadata__:sql_lock_work:" else "\x00\x00__lock_work__:",
        .connection => if (ns.home == .metadata) "\x00\x00__metadata__:sql_lock_connection:" else "\x00\x00__lock_connection__:",
    };
}
fn key(buffer: []u8, ns: ledger.Namespace, backend: ?locks.Backend) ![]const u8 {
    try ns.validate();
    if (backend) |owner| {
        try owner.validate();
        if (!std.mem.eql(u8, &owner.incarnation, &ns.incarnation)) return error.InvalidLockOwner;
        return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:{s}:{x:0>16}", .{ homePrefix(ns, .scope), ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), std.fmt.bytesToHex(owner.id, .lower), owner.fence });
    }
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:count", .{ homePrefix(ns, .scope), ns.group, std.fmt.bytesToHex(ns.incarnation, .lower) });
}
pub const Work = struct { at_ms: u64, backend: locks.Backend };
pub const WorkItem = struct { work: Work, scope: Record };
fn workKey(buffer: []u8, ns: ledger.Namespace, work: Work) ![]const u8 {
    try ns.validate();
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:{x:0>16}:{s}:{x:0>16}", .{ homePrefix(ns, .work), ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), work.at_ms, std.fmt.bytesToHex(work.backend.id, .lower), work.backend.fence });
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
pub fn activeCount(txn: anytype, ns: ledger.Namespace) !u32 {
    return count(txn, ns);
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
fn databaseCountKey(buffer: []u8, ns: ledger.Namespace, database: u64) ![]const u8 {
    try ns.validate();
    if (database == 0) return error.InvalidLockScope;
    // Keep the counter in the existing scope snapshot projection. It counts
    // unreclaimed owners, including fenced owners whose cleanup is unfinished.
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:database:{x:0>16}", .{ homePrefix(ns, .scope), ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), database });
}
pub fn databaseCount(txn: anytype, ns: ledger.Namespace, database: u64) !u32 {
    var buffer: [192]u8 = undefined;
    const raw = (try optional(txn, try databaseCountKey(&buffer, ns, database))) orelse return 0;
    if (raw.len != 4) return error.InvalidLockRecord;
    const value = std.mem.readInt(u32, raw[0..4], .little);
    if (value == 0 or value > max_active) return error.InvalidLockRecord;
    return value;
}
fn writeDatabaseCount(txn: anytype, ns: ledger.Namespace, database: u64, value: u32) !void {
    var buffer: [192]u8 = undefined;
    const address = try databaseCountKey(&buffer, ns, database);
    if (value == 0) return txn.delete(address);
    var raw: [4]u8 = undefined;
    std.mem.writeInt(u32, &raw, value, .little);
    try txn.put(address, &raw);
}
fn connectionKey(buffer: []u8, ns: ledger.Namespace, node: u64, connection_id: [32]u8) ![]const u8 {
    try ns.validate();
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:{x:0>16}:{s}", .{ homePrefix(ns, .connection), ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), node, connection_id });
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
    const database_active = try databaseCount(txn, ns, registration.lease.database);
    if (database_active >= max_active) return error.InvalidLockRecord;
    const value: Record = .{ .registration = registration };
    try ledger.register(a, txn, ns, registration.lease, RegistrationContext{});
    try write(a, txn, ns, value);
    try writeWork(txn, ns, value);
    try writeCount(txn, ns, active + 1);
    try writeDatabaseCount(txn, ns, registration.lease.database, database_active + 1);
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
    if (value.releasing or (if (value.data) |directory| directory.open != 0 else false)) return error.LockOwnerBusy;
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
        // The checked terminal decision first publishes retirement intent.
        // Data owners can then drain independently without a frontend, while
        // the source transaction remains attached until every ACK is durable.
        if (value.data) |directory| if (directory.count() != 0) return false;
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
/// Stable non-expiring metadata participant. Its identity is derived solely
/// from the immutable external owner, so source retirement cannot orphan it.
pub fn preparedBackend(ns: ledger.Namespace, decision: DecisionRef) !locks.Backend {
    try ns.validate();
    try decision.validate();
    var digest: [32]u8 = undefined;
    var h = std.crypto.hash.Blake3.init(.{});
    h.update("antfly/prepared-lock-owner/v1");
    h.update(&decision.operation);
    h.update(&decision.admission);
    h.final(&digest);
    const backend: locks.Backend = .{ .incarnation = ns.incarnation, .id = digest[0..16].*, .fence = 1 };
    try backend.validate();
    return backend;
}
pub fn preparedLease(ns: ledger.Namespace, database: u64, node: u64, decision: DecisionRef) !ledger.Lease {
    const value: ledger.Lease = .{ .backend = try preparedBackend(ns, decision), .database = database, .node = node, .principal = decision.admission, .expires_at_ms = std.math.maxInt(u64) };
    try value.validate(ns);
    return value;
}

/// Attach an already admitted native decision to a data participant even if
/// its frontend expired before delivering SEAL. This creates no execution
/// permit and never reactivates a fenced scope. The independent authority must
/// join the native external owner and its frozen participant reference.
pub fn adoptPreparedAdmission(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, expected: Transaction, authority: anytype) !void {
    if (ns.home != .data or expected.decision == null or std.mem.allEqual(u8, &expected.id, 0)) return error.InvalidLockScope;
    try command.validate();
    try expected.decision.?.validate();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    // Observation of a completed transfer still needs the same native owner.
    try authority.requirePreparedAdmission(txn, ns, value, command, expected);
    if (std.meta.eql(value.released_transaction, @as(?Transaction, expected))) return;
    if (value.state == .retired or value.releasing or value.next_command != command.sequence) return error.LockOwnerChanged;
    const current = value.transaction orelse return error.InvalidLockScope;
    if (!std.mem.eql(u8, &current.id, &expected.id)) return error.LockOwnerChanged;
    if (current.decision) |decision| {
        if (!std.meta.eql(decision, expected.decision.?)) return error.LockOwnerChanged;
        return;
    }
    const owner = try ledger.readOwner(a, txn, ns, backend) orelse return error.InvalidLockRecord;
    if (owner.state != .active or owner.waiting != null or owner.job != null) return error.LockOwnerBusy;
    value.transaction = expected;
    try write(a, txn, ns, value);
}

/// Transfer at most one indexed transaction resource. A sealed native decision
/// owns the job even if its source command has expired or its reply was lost.
pub fn prepareStep(backing: A, txn: anytype, ns: ledger.Namespace, backend: locks.Backend, command: Command, expected: Transaction, destination: ledger.Lease, authority: anytype) !bool {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var value = try load(a, txn, ns, backend);
    if (!std.meta.eql(value.transaction, @as(?Transaction, expected))) {
        if (std.meta.eql(value.released_transaction, @as(?Transaction, expected))) {
            const prepared = try ledger.readOwner(a, txn, ns, destination.backend) orelse return error.InvalidLockRecord;
            if (!std.meta.eql(prepared.lease, destination) or prepared.state != .prepared or !std.meta.eql(prepared.transaction, @as(?[16]u8, expected.id))) return error.InvalidLockRecord;
            return true;
        }
        return error.LockOwnerChanged;
    }
    if (expected.decision == null or value.releasing or value.next_command != command.sequence) return error.InvalidLockScope;
    try authority.requirePreparedHandoff(txn, ns, value, value.registration.lease, destination, expected.id);
    const owner = try ledger.readOwner(a, txn, ns, backend) orelse return error.InvalidLockRecord;
    if (owner.waiting != null) return error.LockOwnerBusy;
    const context: PrepareContext(@TypeOf(authority)) = .{ .authority = authority, .value = value, .ns = ns, .destination = destination, .transaction = expected.id };
    if (owner.job) |job| {
        if (job.effect.action != .control or job.effect.action.control != .prepare or !std.meta.eql(job.effect.action.control.prepare.destination, destination) or !std.mem.eql(u8, &job.effect.action.control.prepare.transaction, &expected.id)) return error.LockOwnerBusy;
        const receipt = try ledger.step(a, txn, ns, job.effect, context);
        if (receipt.outcome != .completed) return false;
    } else {
        var nonce: [32]u8 = undefined;
        var h = std.crypto.hash.Blake3.init(.{});
        h.update("antfly/lock-scope-prepare/v1");
        h.update(&value.registration.nonce);
        h.update(&expected.decision.?.admission);
        h.final(&nonce);
        _ = try ledger.apply(a, txn, ns, .{ .backend = backend, .sequence = owner.next_effect, .nonce = nonce[0..16].*, .action = .{ .control = .{ .prepare = .{ .transaction = expected.id, .destination = destination } } } }, context);
        return false;
    }
    value.transaction = null;
    value.released_transaction = expected;
    value.command = null;
    value.completed = command;
    value.next_command += 1;
    try write(a, txn, ns, value);
    return true;
}
fn PrepareContext(comptime Authority: type) type {
    return struct {
        authority: Authority,
        value: Record,
        ns: ledger.Namespace,
        destination: ledger.Lease,
        transaction: [16]u8,
        pub fn requireEffect(self: @This(), txn: anytype, lease: ledger.Lease, effect: ledger.Effect) !void {
            if (!std.meta.eql(lease, self.value.registration.lease) or effect.action != .control or effect.action.control != .prepare or !std.meta.eql(effect.action.control.prepare.destination, self.destination) or !std.mem.eql(u8, &effect.action.control.prepare.transaction, &self.transaction)) return error.InvalidLockEffect;
            try self.requirePrepare(txn, lease, self.destination, self.transaction);
        }
        pub fn requireObservation(self: @This(), txn: anytype, lease: ledger.Lease, effect: ledger.Effect) !void {
            try self.requireEffect(txn, lease, effect);
        }
        pub fn requirePrepare(self: @This(), txn: anytype, from: ledger.Lease, to: ledger.Lease, id: [16]u8) !void {
            try self.authority.requirePreparedHandoff(txn, self.ns, self.value, from, to, id);
        }
    };
}

/// Independently authorize every cleanup quantum against the external decision.
/// Never borrow a live source scope or release grants from an expired vote.
pub fn releasePreparedStep(backing: A, txn: anytype, ns: ledger.Namespace, lease: ledger.Lease, transaction: [16]u8, authority: anytype) !bool {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try authority.requirePreparedResolution(txn, ns, lease, transaction);
    const owner = try ledger.readOwner(a, txn, ns, lease.backend) orelse return error.InvalidLockRecord;
    if (!std.meta.eql(owner.lease, lease)) return error.LockOwnerChanged;
    if (owner.state == .retired) return true;
    if (owner.state != .prepared or !std.meta.eql(owner.transaction, @as(?[16]u8, transaction)) or owner.waiting != null) return error.InvalidLockRecord;
    const context: PreparedResolutionContext(@TypeOf(authority)) = .{ .authority = authority, .ns = ns, .lease = lease, .transaction = transaction };
    if (owner.job) |job| {
        if (job.effect.action != .control or job.effect.action.control != .close) return error.InvalidLockRecord;
        return (try ledger.step(a, txn, ns, job.effect, context)).outcome == .completed;
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(&transaction, &digest, .{});
    _ = try ledger.apply(a, txn, ns, .{ .backend = lease.backend, .sequence = owner.next_effect, .nonce = digest[0..16].*, .action = .{ .control = .close } }, context);
    return false;
}
fn PreparedResolutionContext(comptime Authority: type) type {
    return struct {
        authority: Authority,
        ns: ledger.Namespace,
        lease: ledger.Lease,
        transaction: [16]u8,
        pub fn requireEffect(self: @This(), txn: anytype, lease: ledger.Lease, effect: ledger.Effect) !void {
            if (!std.meta.eql(lease, self.lease) or effect.action != .control or effect.action.control != .close) return error.InvalidLockEffect;
            try self.authority.requirePreparedResolution(txn, self.ns, lease, self.transaction);
        }
        pub fn requireObservation(self: @This(), txn: anytype, lease: ledger.Lease, effect: ledger.Effect) !void {
            try self.requireEffect(txn, lease, effect);
        }
        pub fn requirePrepare(_: @This(), _: anytype, _: ledger.Lease, _: ledger.Lease, _: [16]u8) !void {
            return error.InvalidLockEffect;
        }
    };
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
    if (value.data) |directory| if (directory.count() != 0) return error.LockOwnerBusy;
    if (value.state == .retired) return true;
    if (value.transaction != null) return error.LockOwnerBusy;
    // A data participant's source debt remains visible until its transferred
    // grants are actually retired. Metadata session owners may retire before
    // their independent prepared home; data topology owners may not.
    if (ns.home == .data) if (value.released_transaction) |released| if (released.decision) |decision| {
        if (try ledger.readOwner(a, txn, ns, try preparedBackend(ns, decision))) |prepared| {
            if (prepared.state != .retired or prepared.resources != 0 or prepared.job != null or prepared.waiting != null) return error.LockOwnerBusy;
        }
    };
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
    const database_active = try databaseCount(txn, ns, value.registration.lease.database);
    if (active == 0 or database_active == 0) return error.InvalidLockRecord;
    try eraseWork(txn, ns, value);
    value.state = .retired;
    try write(a, txn, ns, value);
    try writeCount(txn, ns, active - 1);
    try writeDatabaseCount(txn, ns, value.registration.lease.database, database_active - 1);
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

test "pessimistic locks command fences and registrations bind the receiving metadata group" {
    const a = std.testing.allocator;
    const ns: ledger.Namespace = .{ .home = .metadata, .group = 2188, .incarnation = @splat(1) };
    const registration: Registration = .{ .group = ns.group, .connection = @splat('a'), .nonce = @splat(2), .standalone = false, .lease = .{
        .backend = .{ .incarnation = ns.incarnation, .id = @splat(3), .fence = 1 },
        .database = 1,
        .node = 7,
        .principal = @splat(4),
        .expires_at_ms = 1000,
    } };
    const command: Command = .{ .sequence = 1, .nonce = @splat(5), .executor = @splat(6) };
    const record: Record = .{ .registration = registration, .command = command };
    const guarded: CommandFence = .{ .group = ns.group, .backend = registration.lease.backend, .command = command, .database = 1, .principal = registration.lease.principal, .standalone = false };
    try registration.validate(ns);
    try guarded.requireCommand({}, ns, record, command);
    var other_ns = ns;
    other_ns.group += 1;
    try std.testing.expectError(error.InvalidLockOwner, registration.validate(other_ns));
    try std.testing.expectError(error.Forbidden, guarded.requireCommand({}, other_ns, record, command));
    var other = guarded;
    other.group += 1;
    try std.testing.expectError(error.Forbidden, other.requireCommand({}, ns, record, command));
    var first = std.crypto.hash.Blake3.init(.{});
    var second = std.crypto.hash.Blake3.init(.{});
    guarded.hashInto(&first);
    other.hashInto(&second);
    var x: [32]u8 = undefined;
    var y: [32]u8 = undefined;
    first.final(&x);
    second.final(&y);
    try std.testing.expect(!std.mem.eql(u8, &x, &y));
    const bytes = try std.json.Stringify.valueAlloc(a, guarded, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(CommandFence, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(ns.group, parsed.value.group);
}

test "pessimistic native data participant scopes cannot alias metadata authority or its cleanup indexes" {
    const metadata: ledger.Namespace = .{ .home = .metadata, .group = 2188, .incarnation = @splat(1) };
    var data = metadata;
    data.home = .data;
    const backend: locks.Backend = .{ .incarnation = metadata.incarnation, .id = @splat(3), .fence = 1 };
    var first: [192]u8 = undefined;
    var second: [192]u8 = undefined;
    try std.testing.expect(!std.mem.eql(u8, try key(&first, metadata, backend), try key(&second, data, backend)));
    try std.testing.expect(!std.mem.eql(u8, try key(&first, metadata, null), try key(&second, data, null)));
    try std.testing.expect(!std.mem.eql(u8, try databaseCountKey(&first, metadata, 1), try databaseCountKey(&second, data, 1)));
    const work: Work = .{ .at_ms = 1000, .backend = backend };
    try std.testing.expect(!std.mem.eql(u8, try workKey(&first, metadata, work), try workKey(&second, data, work)));
    try std.testing.expect(!std.mem.eql(u8, try connectionKey(&first, metadata, 7, @splat('a')), try connectionKey(&second, data, 7, @splat('a'))));
    const command: Command = .{ .sequence = 1, .nonce = @splat(5), .executor = @splat(6) };
    const registration: Registration = .{ .group = data.group, .connection = @splat('a'), .nonce = @splat(2), .standalone = false, .lease = .{ .backend = backend, .database = 1, .node = 7, .principal = @splat(4), .expires_at_ms = 1000 } };
    try registration.validate(data);
    const fence_value: CommandFence = .{ .group = metadata.group, .backend = backend, .command = command, .database = 1, .principal = registration.lease.principal, .standalone = false };
    const record: Record = .{ .registration = registration, .command = command };
    try fence_value.requireCommand({}, metadata, record, command);
    try std.testing.expectError(error.Forbidden, fence_value.requireCommand({}, data, record, command));
    const decision: DecisionRef = .{ .operation = @splat(7), .admission = @splat(8) };
    const prepared = try preparedLease(data, 1, 7, decision);
    try std.testing.expect(prepared.backend.eql(try preparedBackend(data, decision)));
    var other = decision;
    other.admission[0] += 1;
    try std.testing.expect(!prepared.backend.eql(try preparedBackend(data, other)));
}
