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

//! A reference to the existing native 2PC participant, not another decision
//! protocol. BEGIN persists the frozen lock reference in its own atomic batch;
//! cleanup cannot erase that owner before its native grants are retired.
const std = @import("std");
const scopes = @import("pessimistic_lock_authority.zig");
const participant = @import("pessimistic_lock_participant.zig");
const transactions = @import("transactions.zig");
const A = std.mem.Allocator;
pub const prefix = "\x00\x00__txn_sql_locks__:";
pub const Ref = struct {
    admission: participant.Admission,
    decision: scopes.DecisionRef,
    pub fn validate(self: Ref, id: transactions.TxnId, owner: transactions.ExternalDecisionOwner) !void {
        try self.admission.validate();
        try self.decision.validate();
        if (self.admission.purpose != .resolution or !std.mem.eql(u8, &self.admission.transaction, &id) or
            !std.mem.eql(u8, &self.decision.operation, &owner.operation_id) or !std.mem.eql(u8, &self.decision.admission, &owner.admission_digest) or
            !std.mem.eql(u8, &std.fmt.bytesToHex(self.admission.binding.namespace.incarnation, .lower), &owner.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
    }
};
pub const Record = struct { ref: Ref, retired: bool = false };
pub fn key(id: transactions.TxnId) [prefix.len + 16]u8 {
    var result: [prefix.len + 16]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &id);
    return result;
}
pub fn encode(a: A, value: Record) ![]u8 {
    const body = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(body);
    if (body.len + 32 > 4096) return error.InvalidLockRecord;
    const result = try a.alloc(u8, body.len + 32);
    @memcpy(result[0..body.len], body);
    std.crypto.hash.Blake3.hash(body, result[body.len..][0..32], .{});
    return result;
}
pub fn read(a: A, txn: anytype, id: transactions.TxnId) !?Record {
    const bytes = txn.get(&key(id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len > 4096 or bytes.len <= 32) return error.InvalidLockRecord;
    const body = bytes[0 .. bytes.len - 32];
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(body, &checksum, .{});
    if (!std.mem.eql(u8, &checksum, bytes[body.len..])) return error.InvalidLockRecord;
    const record = try std.json.parseFromSliceLeaky(Record, a, body, .{});
    try record.ref.admission.validate();
    try record.ref.decision.validate();
    if (!std.mem.eql(u8, &record.ref.admission.transaction, &id) or record.ref.admission.purpose != .resolution) return error.InvalidLockRecord;
    return record;
}
/// Called under the receiving apply mutex before adding these exact bytes to
/// native BEGIN's MutationExtraBatch. A fenced source may complete an admitted
/// decision; a retired or absent source may never resurrect a delayed BEGIN.
pub fn requireBegin(a: A, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, id: transactions.TxnId, owner: transactions.ExternalDecisionOwner, ref: Ref) !void {
    try ref.validate(id, owner);
    try ref.admission.binding.requireIdentity(receiving);
    const ns = ref.admission.binding.namespace;
    const registration = try ref.admission.registration();
    if (try read(a, txn, id)) |previous| {
        if (!std.meta.eql(previous.ref, ref) or previous.retired) return error.ExternalTransactionDecisionChanged;
        return;
    }
    const source = try scopes.read(a, txn, ns, registration.lease.backend) orelse return error.LockOwnerChanged;
    var normalized = registration;
    normalized.lease.expires_at_ms = source.registration.lease.expires_at_ms;
    if (!std.meta.eql(normalized, source.registration) or source.state == .retired or source.releasing or source.next_command != (try ref.admission.command()).sequence) return error.LockOwnerChanged;
    const transaction = source.transaction orelse return error.InvalidLockScope;
    if (!std.mem.eql(u8, &transaction.id, &id)) return error.InvalidLockScope;
    if (transaction.decision) |decision| if (!std.meta.eql(decision, ref.decision)) return error.ExternalTransactionDecisionChanged;
}
/// Phase-two ABORT can win before BEGIN reaches this group. The existing
/// native abort tombstone is then the authority for retiring source custody;
/// it never authorizes a new vote or any row mutation.
pub fn adoptAbortedIfMissing(a: A, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, ref: Ref) !void {
    if (try read(a, txn, ref.admission.transaction)) |existing| {
        if (!std.meta.eql(existing.ref, ref)) return error.ExternalTransactionDecisionChanged;
        return;
    }
    return adoptAborted(a, txn, receiving, ref);
}
pub fn adoptAborted(a: A, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, ref: Ref) !void {
    const id = ref.admission.transaction;
    if (try read(a, txn, id)) |previous| {
        if (!std.meta.eql(previous.ref, ref)) return error.ExternalTransactionDecisionChanged;
        return;
    }
    try ref.admission.binding.requireIdentity(receiving);
    const native = try transactions.externalParticipant(txn, id) orelse return error.ExternalTransactionDecisionChanged;
    const owner = native.owner orelse return error.ExternalTransactionDecisionChanged;
    try ref.validate(id, owner);
    if (native.status != .aborted) return error.ExternalTransactionDecisionChanged;
    const registration = try ref.admission.registration();
    const ns = ref.admission.binding.namespace;
    const scope = try scopes.read(a, txn, ns, registration.lease.backend) orelse {
        // Actual ABORT plus indexed absence is a terminal physical fact, not
        // permission to manufacture a source or native execution grant.
        if (native.prepared) return error.InvalidLockRecord;
        try scopes.ledger.requireAbsent(txn, ns, registration.lease.backend);
        try scopes.ledger.requireAbsent(txn, ns, try scopes.preparedBackend(ns, ref.decision));
        try participant.requireUnadmitted(txn, ref.admission);
        const encoded = try encode(a, .{ .ref = ref, .retired = true });
        defer a.free(encoded);
        try txn.put(&key(id), encoded);
        return;
    };
    var expected = registration;
    expected.lease.expires_at_ms = scope.registration.lease.expires_at_ms;
    if (!std.meta.eql(expected, scope.registration)) return error.LockOwnerChanged;
    if (scope.state == .retired) {
        const held = try scopes.ledger.readOwner(a, txn, ns, registration.lease.backend) orelse return error.InvalidLockRecord;
        if (scope.transaction != null or held.state != .retired or held.resources != 0 or held.job != null or held.waiting != null or held.transaction != null or held.incoming != null) return error.InvalidLockRecord;
        if (scope.released_transaction) |released| if (!std.mem.eql(u8, &released.id, &id) or
            (released.decision != null and !std.meta.eql(released.decision.?, ref.decision))) return error.InvalidLockScope;
        try scopes.ledger.requireNoMembers(txn, ns, registration.lease.backend);
        const prepared_backend = try scopes.preparedBackend(ns, ref.decision);
        if (try scopes.ledger.readOwner(a, txn, ns, prepared_backend)) |prepared| {
            if (prepared.state != .retired or prepared.resources != 0 or prepared.job != null or prepared.waiting != null or prepared.transaction != null or prepared.incoming != null) return error.InvalidLockRecord;
            try scopes.ledger.requireNoMembers(txn, ns, prepared_backend);
        } else try scopes.ledger.requireAbsent(txn, ns, prepared_backend);
        const encoded = try encode(a, .{ .ref = ref, .retired = true });
        defer a.free(encoded);
        try txn.put(&key(id), encoded);
        return;
    }
    if (scope.transaction) |value| {
        if (!std.mem.eql(u8, &value.id, &id) or (value.decision != null and !std.meta.eql(value.decision.?, ref.decision))) return error.InvalidLockScope;
    } else if (scope.released_transaction) |value| {
        if (!std.mem.eql(u8, &value.id, &id)) return error.InvalidLockScope;
    } else {
        const held = try scopes.ledger.readOwner(a, txn, ref.admission.binding.namespace, expected.lease.backend) orelse return error.InvalidLockRecord;
        if (held.resources != 0 or held.transaction != null) return error.InvalidLockScope;
    }
    const encoded = try encode(a, .{ .ref = ref });
    defer a.free(encoded);
    try txn.put(&key(id), encoded);
}

/// The caller supplies the actual native writer used for lock retirement.
/// Completion follows the real native terminal decision and source retirement.
pub fn complete(a: A, txn: anytype, id: transactions.TxnId, owner: transactions.ExternalDecisionOwner) !void {
    var record = try read(a, txn, id) orelse return error.InvalidLockScope;
    try record.ref.validate(id, owner);
    const native = try transactions.externalParticipant(txn, id) orelse return error.ExternalTransactionDecisionChanged;
    if (native.status == .pending or native.owner == null or !native.owner.?.eql(owner)) return error.LockOwnerBusy;
    const admission = record.ref.admission;
    const source = try scopes.read(a, txn, admission.binding.namespace, (try admission.registration()).lease.backend) orelse return error.InvalidLockRecord;
    if (source.state != .retired or source.transaction != null) return error.LockOwnerBusy;
    if (source.released_transaction) |released| {
        if (!std.mem.eql(u8, &released.id, &id) or (!std.meta.eql(released.decision, @as(?scopes.DecisionRef, record.ref.decision)) and !(native.status == .aborted and released.decision == null))) return error.InvalidLockRecord;
    } else if (native.status != .aborted) return error.InvalidLockRecord;
    if (try scopes.ledger.readOwner(a, txn, admission.binding.namespace, try scopes.preparedBackend(admission.binding.namespace, record.ref.decision))) |prepared| {
        if (prepared.state != .retired or prepared.resources != 0 or prepared.job != null or prepared.waiting != null) return error.LockOwnerBusy;
    }
    if (record.retired) return;
    record.retired = true;
    const encoded = try encode(a, record);
    defer a.free(encoded);
    try txn.put(&key(id), encoded);
}
pub fn cleanupReady(a: A, txn: anytype, id: transactions.TxnId) !bool {
    const record = try read(a, txn, id) orelse return true;
    return record.retired;
}
