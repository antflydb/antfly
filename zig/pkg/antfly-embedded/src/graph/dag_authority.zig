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

//! Value-level serialization protocol for a DAG constraint domain. The storage
//! owner must durably apply each transition with compare-and-set/ordered apply.
//! This module alone does not install a domain authority or admit public DAGs.
//!
//! Reservations have NO timeout. Recovery resolves the retained transaction's
//! durable decision before releasing its domain. A new leader or a validation
//! deadline cannot discard a reservation whose participant may still commit.
const std = @import("std");
pub const Digest = [32]u8;
pub const TransactionId = [16]u8;

/// A fence applies to one physical table root. The activation coordinator
/// must acquire it on every source root before taking a shared snapshot.
pub const SourceFence = struct {
    pub const key = "\x00\x00__metadata__:dag-source-fence";
    activation: Digest,
    snapshot: Digest,

    pub fn read(txn: anytype) !?SourceFence {
        const raw = txn.get(key) catch |err| {
            if (err == error.NotFound) return null;
            return err;
        };
        if (raw.len != 96) return error.InvalidDagAuthority;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..64], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[64..96])) return error.InvalidDagAuthority;
        return .{ .activation = raw[0..32].*, .snapshot = raw[32..64].* };
    }

    pub fn acquire(self: SourceFence, txn: anytype) !void {
        if (try read(txn)) |current| {
            if (!std.meta.eql(current, self)) return error.GraphConstraintBusy;
            return;
        }
        var raw: [96]u8 = undefined;
        raw[0..32].* = self.activation;
        raw[32..64].* = self.snapshot;
        std.crypto.hash.sha2.Sha256.hash(raw[0..64], raw[64..96], .{});
        try txn.put(key, &raw);
    }

    pub fn release(self: SourceFence, txn: anytype) !void {
        const current = (try read(txn)) orelse return;
        if (!std.meta.eql(current, self)) return error.GraphConstraintAttemptConflict;
        try txn.delete(key);
    }

    pub fn requireWritable(txn: anytype) !void {
        if (try read(txn) != null) return error.GraphConstraintBusy;
    }

    fn requireSnapshot(txn: anytype, snapshot: Digest) !void {
        const fence = (try read(txn)) orelse return error.GraphConstraintNotReady;
        if (!std.mem.eql(u8, &fence.snapshot, &snapshot)) return error.GraphConstraintRevisionChanged;
    }
};

/// Hash the immutable table identity, index incarnation, and declared edge type.
/// Shard IDs are deliberately absent: splitting cannot create weaker domains.
pub fn domain(table: [16]u8, index_incarnation: u64, index: []const u8, edge_type: []const u8) !Digest {
    if (index_incarnation == 0 or index.len == 0 or edge_type.len == 0) return error.InvalidDagDomain;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/dag-domain/v1");
    hash.update(&table);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, index_incarnation, .little);
    hash.update(&number);
    for ([_][]const u8{ index, edge_type }) |name| {
        std.mem.writeInt(u64, &number, name.len, .little);
        hash.update(&number);
        hash.update(name);
    }
    return hash.finalResult();
}

/// Attempt identity must bind source/config revisions and complete candidate
/// membership. An ID may never be reused with different bytes or transaction.
pub const Attempt = struct {
    id: Digest,
    transaction: TransactionId,
    candidate: Digest,
};
pub const Reservation = struct {
    attempt: Attempt,
    validated: bool = false,
};
pub const Token = struct {
    domain: Digest,
    owner_epoch: u64,
    revision: u64,
    attempt: Attempt,
};
pub const Outcome = enum { accepted, semantic_rejection, aborted };
pub const Receipt = struct {
    domain: Digest,
    attempt: Attempt,
    outcome: Outcome,
    revision: u64,
};

pub const State = struct {
    domain: Digest,
    owner_epoch: u64,
    revision: u64 = 0,
    phase: enum(u8) { validating, active, retiring } = .validating,
    pending: ?Reservation = null,

    /// An activation certificate is intentionally not synthesized here. Only
    /// the future bounded activation owner can publish phase=active after its
    /// complete snapshot and concurrently admitted mutations are proven covered.
    pub fn begin(self: *State, epoch: u64, revision: u64, attempt: Attempt) !Token {
        if (epoch != self.owner_epoch) return error.GraphConstraintOwnerChanged;
        if (self.phase != .active) return error.GraphConstraintNotReady;
        if (revision != self.revision) return error.GraphConstraintRevisionChanged;
        // Reserve revision space BEFORE any distributed commit can be decided.
        if (revision == std.math.maxInt(u64)) return error.GraphConstraintCapacity;
        if (self.pending) |pending| {
            if (std.mem.eql(u8, &pending.attempt.id, &attempt.id)) {
                if (!std.meta.eql(pending.attempt, attempt)) return error.GraphConstraintAttemptConflict;
            } else return error.GraphConstraintBusy;
        } else self.pending = .{ .attempt = attempt };
        return .{ .domain = self.domain, .owner_epoch = epoch, .revision = revision, .attempt = attempt };
    }

    /// The caller may invoke this ONLY after dag_validation proves this exact
    /// candidate against the reserved accepted revision. Persist before voting
    /// yes. A bare successful traversal never authorizes participant publication.
    pub fn validated(self: *State, token: Token) !void {
        try self.require(token);
        self.pending.?.validated = true;
    }

    /// Apply only a terminal authoritative transaction decision. The returned
    /// receipt, state, accepted graph generation and accepted artifact references
    /// must share a recoverably atomic logical decision. Physical projections
    /// may catch up later; they cannot define the accepted revision.
    /// Persist the receipt per attempt: the state is NOT an idempotency history.
    pub fn resolve(self: *State, token: Token, outcome: Outcome) !Receipt {
        try self.require(token);
        if (outcome == .accepted and !self.pending.?.validated) return error.GraphConstraintNotValidated;
        if (outcome == .accepted) self.revision += 1;
        self.pending = null;
        return .{ .domain = self.domain, .attempt = token.attempt, .outcome = outcome, .revision = self.revision };
    }

    /// An ordered owner epoch fence invalidates old tokens while retaining all
    /// unresolved work. New ownership must recover the SAME transaction; it must
    /// not release a lock on a timer or reinterpret an unknown decision as abort.
    pub fn takeover(self: *State, expected_epoch: u64, new_epoch: u64) !void {
        if (self.owner_epoch != expected_epoch) return error.GraphConstraintOwnerChanged;
        if (new_epoch <= expected_epoch) return error.InvalidDagOwnerEpoch;
        self.owner_epoch = new_epoch;
    }
    pub fn recoveryToken(self: *const State, epoch: u64) !?Token {
        if (epoch != self.owner_epoch) return error.GraphConstraintOwnerChanged;
        const pending = self.pending orelse return null;
        return .{ .domain = self.domain, .owner_epoch = epoch, .revision = self.revision, .attempt = pending.attempt };
    }
    pub fn retire(self: *State, epoch: u64) !void {
        if (epoch != self.owner_epoch) return error.GraphConstraintOwnerChanged;
        if (self.pending != null) return error.GraphConstraintBusy;
        self.phase = .retiring;
    }
    fn require(self: *const State, token: Token) !void {
        if (self.owner_epoch != token.owner_epoch) return error.GraphConstraintOwnerChanged;
        if (!std.mem.eql(u8, &self.domain, &token.domain)) return error.InvalidDagDomain;
        if (self.phase != .active) return error.GraphConstraintNotReady;
        if (self.revision != token.revision) return error.GraphConstraintRevisionChanged;
        const pending = self.pending orelse return error.GraphConstraintAttemptConflict;
        if (!std.meta.eql(pending.attempt, token.attempt)) return error.GraphConstraintAttemptConflict;
    }

    pub const encoded_len = 167;
    pub fn encode(self: State) ![encoded_len]u8 {
        if (self.owner_epoch == 0 or (self.pending != null and (self.phase != .active or self.revision == std.math.maxInt(u64)))) return error.InvalidDagAuthority;
        var raw: [encoded_len]u8 = @splat(0);
        @memcpy(raw[0..4], "DAG1");
        @memcpy(raw[4..36], &self.domain);
        std.mem.writeInt(u64, raw[36..44], self.owner_epoch, .little);
        std.mem.writeInt(u64, raw[44..52], self.revision, .little);
        raw[52] = @backingInt(self.phase);
        if (self.pending) |pending| {
            raw[53] = 1;
            @memcpy(raw[54..86], &pending.attempt.id);
            @memcpy(raw[86..102], &pending.attempt.transaction);
            @memcpy(raw[102..134], &pending.attempt.candidate);
            raw[134] = @intFromBool(pending.validated);
        }
        std.crypto.hash.sha2.Sha256.hash(raw[0..135], raw[135..167], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "DAG1") or raw[53] > 1 or raw[134] > 1) return error.InvalidDagAuthority;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..135], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[135..167])) return error.InvalidDagAuthority;
        const state: State = .{
            .domain = raw[4..36].*,
            .owner_epoch = std.mem.readInt(u64, raw[36..44], .little),
            .revision = std.mem.readInt(u64, raw[44..52], .little),
            .phase = std.enums.fromInt(@FieldType(State, "phase"), raw[52]) orelse return error.InvalidDagAuthority,
            .pending = if (raw[53] == 0) null else .{ .attempt = .{ .id = raw[54..86].*, .transaction = raw[86..102].*, .candidate = raw[102..134].* }, .validated = raw[134] == 1 },
        };
        // Canonical re-encoding checks reserved bytes and impossible states.
        const expected = try state.encode();
        if (!std.mem.eql(u8, raw, &expected)) return error.InvalidDagAuthority;
        return state;
    }
};

fn testAttempt(byte: u8) Attempt {
    return .{ .id = @splat(byte), .transaction = @splat(byte), .candidate = @splat(byte) };
}
fn testState() State {
    return .{ .domain = @splat(1), .owner_epoch = 1, .phase = .active };
}

test "DAG authority domains bind incarnation and names without ambiguous concatenation" {
    const table: [16]u8 = @splat(1);
    const first = try domain(table, 1, "ab", "c");
    try std.testing.expect(!std.mem.eql(u8, &first, &try domain(table, 1, "a", "bc")));
    try std.testing.expect(!std.mem.eql(u8, &first, &try domain(table, 2, "ab", "c")));
    try std.testing.expect(!std.mem.eql(u8, &first, &try domain(@splat(2), 1, "ab", "c")));
    try std.testing.expectError(error.InvalidDagDomain, domain(table, 0, "ab", "c"));
}
test "DAG authority serializes explicit and generated publishers with stable attempt identities" {
    var state = testState();
    const first = try state.begin(1, 0, testAttempt(1));
    try std.testing.expectEqualDeep(first, try state.begin(1, 0, testAttempt(1)));
    var changed = testAttempt(1);
    changed.candidate = @splat(3);
    try std.testing.expectError(error.GraphConstraintAttemptConflict, state.begin(1, 0, changed));
    changed = testAttempt(1);
    changed.transaction = @splat(3);
    try std.testing.expectError(error.GraphConstraintAttemptConflict, state.begin(1, 0, changed));
    try std.testing.expectError(error.GraphConstraintBusy, state.begin(1, 0, testAttempt(2)));
    try std.testing.expectError(error.GraphConstraintNotValidated, state.resolve(first, .accepted));
    try state.validated(first);
    const receipt = try state.resolve(first, .accepted);
    try std.testing.expectEqual(@as(u64, 1), receipt.revision);
    try std.testing.expectEqual(Outcome.accepted, receipt.outcome);
    try std.testing.expectError(error.GraphConstraintRevisionChanged, state.begin(1, 0, testAttempt(2)));
    _ = try state.begin(1, 1, testAttempt(2));
}
test "DAG authority restart and leader transfer retain undecided reservations" {
    inline for (.{ false, true }) |validated| {
        var original = testState();
        const old = try original.begin(1, 0, testAttempt(1));
        if (validated) try original.validated(old);
        var recovered = try State.decode(&try original.encode());
        try recovered.takeover(1, 2);
        try std.testing.expectError(error.GraphConstraintOwnerChanged, recovered.resolve(old, .accepted));
        try std.testing.expectError(error.GraphConstraintOwnerChanged, recovered.validated(old));
        try std.testing.expectError(error.GraphConstraintBusy, recovered.begin(2, 0, testAttempt(2)));
        try std.testing.expectError(error.GraphConstraintBusy, recovered.retire(2));
        const token = (try recovered.recoveryToken(2)).?;
        try std.testing.expectEqualDeep(old.attempt, token.attempt);
        if (validated) {
            _ = try recovered.resolve(token, .accepted);
            try std.testing.expectEqual(@as(u64, 1), recovered.revision);
        } else {
            _ = try recovered.resolve(token, .aborted);
            try std.testing.expectEqual(@as(u64, 0), recovered.revision);
        }
        try recovered.retire(2);
        try std.testing.expectError(error.GraphConstraintNotReady, recovered.begin(2, recovered.revision, testAttempt(2)));
    }
}
test "DAG authority semantic rejection preserves accepted revision and invalid tokens cannot release work" {
    var state = testState();
    const token = try state.begin(1, 0, testAttempt(1));
    var wrong = token;
    wrong.attempt = testAttempt(2);
    try std.testing.expectError(error.GraphConstraintAttemptConflict, state.resolve(wrong, .aborted));
    wrong = token;
    wrong.domain = @splat(2);
    try std.testing.expectError(error.InvalidDagDomain, state.validated(wrong));
    const receipt = try state.resolve(token, .semantic_rejection);
    try std.testing.expectEqual(Outcome.semantic_rejection, receipt.outcome);
    try std.testing.expectEqual(@as(u64, 0), state.revision);
    try std.testing.expect(state.pending == null);
}
test "DAG authority codec rejects every single byte corruption and invalid states" {
    var state = testState();
    _ = try state.begin(1, 0, testAttempt(1));
    const raw = try state.encode();
    try std.testing.expectEqualDeep(state, try State.decode(&raw));
    for (0..raw.len) |i| {
        var corrupt = raw;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.InvalidDagAuthority, State.decode(&corrupt));
    }
    try std.testing.expectError(error.InvalidDagAuthority, State.decode(raw[0..10]));
    state.owner_epoch = 0;
    try std.testing.expectError(error.InvalidDagAuthority, state.encode());
    state = testState();
    state.revision = std.math.maxInt(u64);
    try std.testing.expectError(error.GraphConstraintCapacity, state.begin(1, state.revision, testAttempt(1)));
    state = testState();
    state.phase = .validating;
    try std.testing.expectError(error.GraphConstraintNotReady, state.begin(1, 0, testAttempt(1)));
}

/// Storage operations for the ordered domain owner. The caller supplies a
/// serialized write transaction and aborts it on any error. Acceptance effects
/// and resolution must be staged in that same transaction.
pub const Storage = struct {
    const state_prefix = "\x00\x00__metadata__:dag-state:";
    const receipt_prefix = "\x00\x00__metadata__:dag-receipt:";

    const validation = @import("dag_validation.zig");
    const proof_prefix = "\x00\x00__metadata__:dag-proof:";

    fn proofKey(id: Digest) [proof_prefix.len + 64]u8 {
        return proof_prefix.* ++ std.fmt.bytesToHex(id, .lower);
    }

    fn proof(token: Token, changes: []const validation.Change, membership: []const Membership.Update) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/dag-adjacency-proof/v1");
        hash.update(&token.attempt.id);
        hash.update(&token.attempt.transaction);
        hash.update(&token.attempt.candidate);
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, token.revision, .little);
        hash.update(&size);
        std.mem.writeInt(u64, &size, changes.len, .little);
        hash.update(&size);
        for (changes) |change| hashChange(&hash, change);
        std.mem.writeInt(u64, &size, membership.len, .little);
        hash.update(&size);
        for (membership) |update| {
            hashChange(&hash, update.edge);
            std.mem.writeInt(u64, &size, update.owners, .little);
            hash.update(&size);
        }
        return hash.finalResult();
    }

    fn hashChange(hash: *std.crypto.hash.sha2.Sha256, change: validation.Change) void {
        var size: [8]u8 = undefined;
        for ([_][]const u8{ change.source, change.target }) |key| {
            std.mem.writeInt(u64, &size, key.len, .little);
            hash.update(&size);
            hash.update(key);
        }
        hash.update(&.{@intFromBool(change.present)});
    }

    fn stateKey(id: Digest) [state_prefix.len + 64]u8 {
        return state_prefix.* ++ std.fmt.bytesToHex(id, .lower);
    }

    fn receiptKey(id: Digest, attempt_id: Digest) [receipt_prefix.len + 128]u8 {
        return receipt_prefix.* ++ std.fmt.bytesToHex(id, .lower) ++ std.fmt.bytesToHex(attempt_id, .lower);
    }

    pub fn load(txn: anytype, id: Digest) !State {
        const state = try State.decode(try txn.get(&stateKey(id)));
        if (!std.mem.eql(u8, &state.domain, &id)) return error.InvalidDagAuthority;
        return state;
    }

    /// Catalog publication may reference only an activated incarnation whose
    /// activation certificate is visible in the same storage snapshot.
    pub fn requireActive(txn: anytype, id: Digest) !State {
        const state = load(txn, id) catch |err| {
            if (err == error.NotFound) return error.GraphConstraintNotReady;
            return err;
        };
        if (state.phase != .active) return error.GraphConstraintNotReady;
        _ = try Contributions.readActivationCertificate(txn, id);
        return state;
    }

    pub fn create(txn: anytype, id: Digest, epoch: u64) !void {
        const key = stateKey(id);
        if (txn.get(&key)) |_| {
            return error.GraphConstraintAlreadyExists;
        } else |err| if (err != error.NotFound) return err;
        try save(txn, .{ .domain = id, .owner_epoch = epoch });
    }

    /// Activate a newly allocated graph incarnation. The catalog participant
    /// must prove that the logical graph itself is empty under its creation
    /// fence. The ordered store checks for leaked integrity rows separately.
    pub fn activateNewIncarnation(txn: anytype, id: Digest, epoch: u64) !void {
        var state = try load(txn, id);
        if (state.owner_epoch != epoch) return error.GraphConstraintOwnerChanged;
        if (state.phase == .active) {
            try Contributions.requireActivationCertificate(txn, id, null);
            return;
        }
        if (state.phase != .validating or state.revision != 0) return error.GraphConstraintNotReady;
        try requireEmptyLedger(txn, id);
        try Contributions.saveActivationCertificate(txn, id, null);
        state.phase = .active;
        try save(txn, state);
    }

    fn requireEmptyLedger(txn: anytype, id: Digest) !void {
        const Handle = switch (@typeInfo(@TypeOf(txn))) {
            .pointer => |pointer| pointer.child,
            else => @TypeOf(txn),
        };
        var cursor = if (comptime @hasDecl(Handle, "openPhysicalCursorAdapter"))
            try txn.openPhysicalCursorAdapter()
        else
            try txn.openCursor();
        defer cursor.close();
        const suffix = std.fmt.bytesToHex(id, .lower);
        try requireEmptyPrefix(&cursor, &(Adjacency.prefix.* ++ id));
        try requireEmptyPrefix(&cursor, &(Membership.prefix.* ++ id));
        try requireEmptyPrefix(&cursor, &(Contributions.prefix.* ++ id));
        try requireEmptyPrefix(&cursor, &(receipt_prefix.* ++ suffix));
        try requireEmptyPrefix(&cursor, &proofKey(id));
        try requireEmptyPrefix(&cursor, &Contributions.activationKey(id));
        try requireEmptyPrefix(&cursor, &Contributions.activationCertificateKey(id));
        try requireEmptyPrefix(&cursor, &TransactionParticipant.key(id));
    }

    fn requireEmptyPrefix(cursor: anytype, prefix: []const u8) !void {
        if (try cursor.seekAtOrAfter(prefix)) |entry| {
            if (std.mem.startsWith(u8, entry.key, prefix)) return error.GraphConstraintNotReady;
        }
    }

    pub fn retire(txn: anytype, id: Digest, epoch: u64) !void {
        var state = try load(txn, id);
        try state.retire(epoch);
        try save(txn, state);
    }

    fn save(txn: anytype, state: State) !void {
        try txn.put(&stateKey(state.domain), &try state.encode());
    }

    pub const Begin = union(enum) { reserved: Token, terminal: Receipt };

    pub fn begin(txn: anytype, id: Digest, epoch: u64, revision: u64, attempt: Attempt) !Begin {
        return beginChecked(txn, id, epoch, revision, attempt, false);
    }

    /// A co-located publisher must never resolve a reservation owned by a
    /// distributed coordinator, even if it carries the same attempt bytes.
    pub fn beginUnreserved(txn: anytype, id: Digest, epoch: u64, revision: u64, attempt: Attempt) !Begin {
        return beginChecked(txn, id, epoch, revision, attempt, true);
    }

    fn beginChecked(txn: anytype, id: Digest, epoch: u64, revision: u64, attempt: Attempt, require_unreserved: bool) !Begin {
        var state = load(txn, id) catch |err| {
            if (err == error.NotFound) return error.GraphConstraintNotReady;
            return err;
        };
        if (state.owner_epoch != epoch) return error.GraphConstraintOwnerChanged;
        if (try readReceipt(txn, id, attempt)) |receipt| return .{ .terminal = receipt };
        if (require_unreserved and state.pending != null) return error.GraphConstraintBusy;
        _ = try Contributions.readActivationCertificate(txn, id);
        const token = try state.begin(epoch, revision, attempt);
        try save(txn, state);
        return .{ .reserved = token };
    }

    /// Validate against the accepted adjacency in the same snapshot as the
    /// reservation. The digest must bind the reconciled changes and all source
    /// versions used to produce them.
    pub fn validate(
        alloc: std.mem.Allocator,
        txn: anytype,
        token: Token,
        changes: []const validation.Change,
        membership: []const Membership.Update,
        budget: *validation.Budget,
        witness: *validation.Witness,
    ) !void {
        var state = try load(txn, token.domain);
        try state.require(token);
        const reader: Adjacency.Reader(@TypeOf(txn)) = .{ .txn = txn, .domain_id = token.domain };
        try validation.validate(alloc, reader, changes, budget, witness);
        const digest = proof(token, changes, membership);
        if (state.pending.?.validated) {
            try requireProof(txn, token.domain, digest);
            return;
        }
        try txn.put(&proofKey(token.domain), &digest);
        try state.validated(token);
        try save(txn, state);
    }

    fn requireProof(txn: anytype, id: Digest, digest: Digest) !void {
        const saved = txn.get(&proofKey(id)) catch |err| {
            if (err == error.NotFound) return error.GraphConstraintNotValidated;
            return err;
        };
        if (!std.mem.eql(u8, saved, &digest)) return error.GraphConstraintAttemptConflict;
    }

    pub fn takeover(txn: anytype, id: Digest, expected_epoch: u64, new_epoch: u64) !void {
        var state = try load(txn, id);
        try state.takeover(expected_epoch, new_epoch);
        try save(txn, state);
    }

    pub fn reject(txn: anytype, token: Token, outcome: enum { semantic_rejection, aborted }) !Receipt {
        return resolve(txn, token, switch (outcome) {
            .semantic_rejection => .semantic_rejection,
            .aborted => .aborted,
        });
    }

    fn resolve(txn: anytype, token: Token, outcome: Outcome) !Receipt {
        var state = try load(txn, token.domain);
        if (state.owner_epoch != token.owner_epoch) return error.GraphConstraintOwnerChanged;
        if (try readReceipt(txn, token.domain, token.attempt)) |receipt| {
            if (receipt.outcome != outcome) return error.GraphConstraintAttemptConflict;
            return receipt;
        }
        const receipt = try state.resolve(token, outcome);
        var raw: [89]u8 = undefined;
        @memcpy(raw[0..16], &token.attempt.transaction);
        @memcpy(raw[16..48], &token.attempt.candidate);
        raw[48] = switch (outcome) {
            .accepted => 1,
            .semantic_rejection => 2,
            .aborted => 3,
        };
        std.mem.writeInt(u64, raw[49..57], receipt.revision, .little);
        const key = receiptKey(token.domain, token.attempt.id);
        receiptChecksum(&key, raw[0..57], raw[57..89]);
        try txn.put(&key, &raw);
        try save(txn, state);
        return receipt;
    }

    /// Stage accepted adjacency with the terminal receipt. The transaction
    /// must also stage the source/artifact changes, and abort on any error.
    pub fn accept(
        alloc: std.mem.Allocator,
        txn: anytype,
        token: Token,
        changes: []const validation.Change,
        membership: []const Membership.Update,
    ) !Receipt {
        if (try readReceipt(txn, token.domain, token.attempt)) |_| {
            return resolve(txn, token, .accepted);
        }
        const state = try load(txn, token.domain);
        try state.require(token);
        if (!state.pending.?.validated) return error.GraphConstraintNotValidated;
        try requireProof(txn, token.domain, proof(token, changes, membership));
        const receipt = try resolve(txn, token, .accepted);
        try Adjacency.stage(alloc, txn, token.domain, changes);
        try Membership.stage(alloc, txn, token.domain, membership);
        return receipt;
    }

    fn receiptChecksum(key: []const u8, value: []const u8, out: *Digest) void {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(key);
        hash.update(value);
        out.* = hash.finalResult();
    }

    /// Check the exact vote against a durable authority snapshot. Owner
    /// handoff and later accepted revisions leave this decision intact.
    /// Callers must separately fence the destination index incarnation.
    pub fn requireAccepted(txn: anytype, id: Digest, base_revision: u64, attempt: Attempt) !Receipt {
        const receipt = (try readReceipt(txn, id, attempt)) orelse return error.GraphConstraintNotValidated;
        switch (receipt.outcome) {
            .semantic_rejection => {
                if (receipt.revision != base_revision) return error.GraphConstraintRevisionChanged;
                return error.GraphCycle;
            },
            .aborted => return error.GraphConstraintAttemptConflict,
            .accepted => {
                const expected = std.math.add(u64, base_revision, 1) catch return error.GraphConstraintRevisionChanged;
                if (receipt.revision != expected) return error.GraphConstraintRevisionChanged;
            },
        }
        return receipt;
    }

    fn readReceipt(txn: anytype, id: Digest, attempt: Attempt) !?Receipt {
        const key = receiptKey(id, attempt.id);
        const raw = txn.get(&key) catch |err| {
            if (err == error.NotFound) return null;
            return err;
        };
        if (raw.len != 89) return error.InvalidDagAuthority;
        var checksum: Digest = undefined;
        receiptChecksum(&key, raw[0..57], &checksum);
        if (!std.mem.eql(u8, &checksum, raw[57..89])) return error.InvalidDagAuthority;
        if (!std.mem.eql(u8, raw[0..16], &attempt.transaction) or
            !std.mem.eql(u8, raw[16..48], &attempt.candidate)) return error.GraphConstraintAttemptConflict;
        return .{
            .domain = id,
            .attempt = attempt,
            .revision = std.mem.readInt(u64, raw[49..57], .little),
            .outcome = switch (raw[48]) {
                1 => .accepted,
                2 => .semantic_rejection,
                3 => .aborted,
                else => return error.InvalidDagAuthority,
            },
        };
    }
};

test "DAG storage terminal receipts fence replay and changed payloads" {
    const Txn = struct {
        state: ?[State.encoded_len]u8 = null,
        receipt: ?[89]u8 = null,
        proof_value: ?Digest = null,
        certificate: ?[33]u8 = null,
        edges: usize = 0,
        owners: u64 = 0,

        const Cursor = struct {
            const Entry = struct { key: []const u8 };
            fn close(_: *@This()) void {}
            fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
                return null;
            }
            fn next(_: *@This()) !?Entry {
                return null;
            }
        };

        fn openCursor(_: *@This()) !Cursor {
            return .{};
        }
        fn delete(_: *@This(), _: []const u8) !void {}
        fn now(_: *anyopaque) u64 {
            return 0;
        }

        fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (std.mem.startsWith(u8, key, Storage.state_prefix)) {
                if (self.state) |*raw| return raw;
            } else if (std.mem.startsWith(u8, key, Storage.proof_prefix)) {
                if (self.proof_value) |*raw| return raw;
            } else if (std.mem.startsWith(u8, key, Contributions.certificate_prefix)) {
                if (self.certificate) |*raw| return raw;
            } else if (self.receipt) |*raw| return raw;
            return error.NotFound;
        }

        fn put(self: *@This(), key: []const u8, raw: []const u8) !void {
            if (std.mem.startsWith(u8, key, Storage.state_prefix)) {
                self.state = raw[0..State.encoded_len].*;
            } else if (std.mem.startsWith(u8, key, Storage.proof_prefix)) {
                self.proof_value = raw[0..32].*;
            } else if (std.mem.startsWith(u8, key, Contributions.certificate_prefix)) {
                self.certificate = raw[0..33].*;
            } else if (std.mem.startsWith(u8, key, Adjacency.prefix)) {
                self.edges += 1;
            } else if (std.mem.startsWith(u8, key, Membership.prefix)) {
                self.owners = std.mem.readInt(u64, raw[0..8], .little);
            } else {
                self.receipt = raw[0..89].*;
            }
        }
    };
    var txn: Txn = .{};
    const state = testState();
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.begin(&txn, state.domain, 1, 0, testAttempt(1)));
    try Storage.create(&txn, state.domain, 1);
    try std.testing.expectError(error.GraphConstraintAlreadyExists, Storage.create(&txn, state.domain, 1));
    try Storage.save(&txn, state);
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.begin(&txn, state.domain, 1, 0, testAttempt(1)));
    try std.testing.expect((try Storage.load(&txn, state.domain)).pending == null);
    try Contributions.saveActivationCertificate(&txn, state.domain, null);
    const token = (try Storage.begin(&txn, state.domain, 1, 0, testAttempt(1))).reserved;
    const alloc = std.testing.allocator;
    const changes: []const Storage.validation.Change = &.{.{ .source = "a", .target = "b", .present = true }};
    const membership: []const Membership.Update = &.{.{ .edge = changes[0], .owners = 2 }};
    var budget: Storage.validation.Budget = .{ .clock = .{ .context = &txn, .now = Txn.now } };
    var witness: Storage.validation.Witness = .{};
    try std.testing.expectError(error.GraphConstraintNotValidated, Storage.accept(alloc, &txn, token, changes, membership));
    try Storage.validate(alloc, &txn, token, changes, membership, &budget, &witness);
    try std.testing.expectError(error.GraphConstraintNotValidated, Storage.requireAccepted(&txn, state.domain, 0, token.attempt));
    try std.testing.expectError(error.GraphConstraintAttemptConflict, Storage.accept(alloc, &txn, token, changes, &.{}));
    try std.testing.expectError(error.GraphConstraintAttemptConflict, Storage.accept(alloc, &txn, token, &.{}, &.{}));
    try std.testing.expectError(error.GraphConstraintAttemptConflict, Storage.validate(alloc, &txn, token, &.{}, &.{}, &budget, &witness));
    try Storage.takeover(&txn, state.domain, 1, 2);
    try std.testing.expectError(error.GraphConstraintOwnerChanged, Storage.accept(alloc, &txn, token, changes, membership));
    const recovered = try Storage.load(&txn, state.domain);
    const current = (try recovered.recoveryToken(2)).?;
    const receipt = try Storage.accept(alloc, &txn, current, changes, membership);
    try std.testing.expectEqualDeep(receipt, try Storage.accept(alloc, &txn, current, changes, membership));
    try std.testing.expectEqualDeep(receipt, try Storage.requireAccepted(&txn, state.domain, 0, token.attempt));
    try std.testing.expectError(error.GraphConstraintRevisionChanged, Storage.requireAccepted(&txn, state.domain, 1, token.attempt));
    var another_transaction = token.attempt;
    another_transaction.transaction[0] ^= 1;
    try std.testing.expectError(error.GraphConstraintAttemptConflict, Storage.requireAccepted(&txn, state.domain, 0, another_transaction));
    try std.testing.expectEqual(@as(usize, 1), txn.edges);
    try std.testing.expectEqual(@as(u64, 2), txn.owners);
    try std.testing.expectEqualDeep(receipt, (try Storage.begin(&txn, state.domain, 2, 0, testAttempt(1))).terminal);
    var conflicting = testAttempt(1);
    conflicting.candidate = @splat(2);
    try std.testing.expectError(error.GraphConstraintAttemptConflict, Storage.begin(&txn, state.domain, 2, 1, conflicting));
    txn.receipt.?[0] ^= 1;
    try std.testing.expectError(error.InvalidDagAuthority, Storage.begin(&txn, state.domain, 2, 1, testAttempt(1)));
}

/// Accepted endpoint membership in the authority's ordered store. The caller
/// reconciles contributor identities before producing these endpoint changes.
pub const Adjacency = struct {
    const validation = @import("dag_validation.zig");
    const prefix = "\x00\x00__metadata__:dag-adjacency:";
    const A = std.mem.Allocator;

    fn sourcePrefix(alloc: A, id: Digest, source: []const u8) ![]u8 {
        if (source.len > std.math.maxInt(u32)) return error.GraphConstraintCapacity;
        var length: [4]u8 = undefined;
        std.mem.writeInt(u32, &length, @intCast(source.len), .big);
        return std.mem.concat(alloc, u8, &.{ prefix, &id, &length, source });
    }

    pub fn stage(alloc: A, txn: anytype, id: Digest, changes: []const validation.Change) !void {
        for (changes) |change| {
            const source = try sourcePrefix(alloc, id, change.source);
            defer alloc.free(source);
            const key = try std.mem.concat(alloc, u8, &.{ source, change.target });
            defer alloc.free(key);
            if (change.present) {
                try txn.put(key, "");
            } else {
                txn.delete(key) catch |err| if (err != error.NotFound) return err;
            }
        }
    }

    pub fn Reader(comptime Txn: type) type {
        return struct {
            txn: Txn,
            domain_id: Digest,

            pub fn next(self: @This(), alloc: A, source: []const u8, after: ?[]const u8, budget: *validation.Budget) !?validation.Row {
                try budget.consume(.requests, 1);
                const source_prefix = try sourcePrefix(alloc, self.domain_id, source);
                var cursor = try self.txn.openCursor();
                defer cursor.close();
                var row = try cursor.seekAtOrAfter(after orelse source_prefix);
                if (after) |previous| {
                    if (row) |entry| {
                        if (std.mem.eql(u8, previous, entry.key)) row = try cursor.next();
                    }
                }
                const entry = row orelse return null;
                if (!std.mem.startsWith(u8, entry.key, source_prefix)) return null;
                const owned = try alloc.dupe(u8, entry.key);
                return .{ .key = owned, .target = owned[source_prefix.len..] };
            }
        };
    }
};

/// Reduces effective contributor changes to boolean adjacency. Source
/// precedence must already have been applied within each logical contribution.
/// Input is ordered by endpoint, with all changes for an endpoint contiguous.
pub const Membership = struct {
    const validation = @import("dag_validation.zig");
    const prefix = "\x00\x00__metadata__:dag-members:";
    const A = std.mem.Allocator;

    pub const Update = struct {
        edge: validation.Change,
        owners: u64,
    };

    fn key(alloc: A, id: Digest, edge: validation.Change) ![]u8 {
        const encoded = try Adjacency.sourcePrefix(alloc, id, edge.source);
        defer alloc.free(encoded);
        return std.mem.concat(alloc, u8, &.{ prefix, encoded[Adjacency.prefix.len..], edge.target });
    }

    fn read(txn: anytype, storage_key: []const u8) !u64 {
        const raw = txn.get(storage_key) catch |err| {
            if (err == error.NotFound) return 0;
            return err;
        };
        if (raw.len != 8) return error.InvalidDagAuthority;
        const count = std.mem.readInt(u64, raw[0..8], .little);
        if (count == 0) return error.InvalidDagAuthority;
        return count;
    }

    fn order(a: validation.Change, b: validation.Change) std.math.Order {
        const source_order = std.mem.order(u8, a.source, b.source);
        if (source_order != .eq) return source_order;
        return std.mem.order(u8, a.target, b.target);
    }

    /// Caller owns both arrays and may reuse them across retries. The capacity
    /// needed for each is at most the number of contributor changes.
    pub fn reconcile(
        alloc: A,
        txn: anytype,
        id: Digest,
        input: []const validation.Change,
        updates: []Update,
        adjacency: []validation.Change,
        budget: *validation.Budget,
    ) !struct { updates: usize, adjacency: usize } {
        if (updates.len < input.len or adjacency.len < input.len) return error.GraphConstraintCapacity;
        var update_count: usize = 0;
        var adjacency_count: usize = 0;
        var index: usize = 0;
        while (index < input.len) {
            const edge = input[index];
            var added: u64 = 0;
            var removed: u64 = 0;
            while (index < input.len and order(edge, input[index]) == .eq) : (index += 1) {
                try budget.consume(.edges, 1);
                try budget.consume(.bytes, edge.source.len);
                try budget.consume(.bytes, edge.target.len);
                if (input[index].present) added += 1 else removed += 1;
            }
            if (index < input.len and order(edge, input[index]) != .lt) return error.InvalidDagMutation;
            try budget.consume(.requests, 1);
            const storage_key = try key(alloc, id, edge);
            defer alloc.free(storage_key);
            const before = try read(txn, storage_key);
            try budget.checkpoint();
            if (removed > before) return error.InvalidDagMutation;
            const after = std.math.add(u64, before - removed, added) catch return error.GraphConstraintCapacity;
            if (before == after) continue;
            const change: validation.Change = .{ .source = edge.source, .target = edge.target, .present = after != 0 };
            updates[update_count] = .{ .edge = change, .owners = after };
            update_count += 1;
            if ((before != 0) != change.present) {
                adjacency[adjacency_count] = change;
                adjacency_count += 1;
            }
        }
        return .{ .updates = update_count, .adjacency = adjacency_count };
    }

    /// Stage alongside the validated adjacency and the new owner manifests.
    pub fn stage(alloc: A, txn: anytype, id: Digest, updates: []const Update) !void {
        for (updates) |update| {
            const storage_key = try key(alloc, id, update.edge);
            defer alloc.free(storage_key);
            if (update.owners == 0) {
                try txn.delete(storage_key);
            } else {
                var raw: [8]u8 = undefined;
                std.mem.writeInt(u64, &raw, update.owners, .little);
                try txn.put(storage_key, &raw);
            }
        }
    }
};

/// Complete replacements for effective logical contributions. The producer
/// chooses a stable owner identity after applying source precedence.
pub const Contributions = struct {
    const validation = @import("dag_validation.zig");
    const A = std.mem.Allocator;
    const prefix = "\x00\x00__metadata__:dag-contribution:";
    const activation_prefix = "\x00\x00__metadata__:dag-activation:";

    pub const Replacement = struct {
        owner: Digest,
        source: []const u8,
        targets: []const []const u8,
    };

    fn key(alloc: A, id: Digest, replacement: Replacement) ![]u8 {
        return std.mem.concat(alloc, u8, &.{ prefix, &id, &replacement.owner, replacement.source });
    }

    /// Bind complete replacements and the caller's pinned producer versions
    /// to the attempt. Replacements are ordered by owner and then source.
    pub fn candidate(versions: Digest, replacements: []const Replacement) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/dag-contribution-replacements/v1");
        hash.update(&versions);
        for (replacements) |replacement| {
            hash.update(&replacement.owner);
            hashBytes(&hash, replacement.source);
            var count: [8]u8 = undefined;
            std.mem.writeInt(u64, &count, replacement.targets.len, .little);
            hash.update(&count);
            for (replacement.targets) |target| hashBytes(&hash, target);
        }
        return hash.finalResult();
    }

    fn hashBytes(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, bytes.len, .little);
        hash.update(&length);
        hash.update(bytes);
    }

    const Plan = struct {
        adjacency: []const validation.Change,
        membership: []const Membership.Update,
    };

    fn plan(alloc: A, txn: anytype, id: Digest, replacements: []const Replacement, budget: *validation.Budget) !Plan {
        var changes: std.ArrayList(validation.Change) = .empty;
        var previous: ?Replacement = null;
        for (replacements) |replacement| {
            if (previous) |prior| {
                const owner_order = std.mem.order(u8, &prior.owner, &replacement.owner);
                if (owner_order == .gt or (owner_order == .eq and
                    std.mem.order(u8, prior.source, replacement.source) != .lt)) return error.InvalidDagMutation;
            }
            previous = replacement;
            try budget.consume(.requests, 1);
            try budget.consume(.bytes, replacement.source.len);
            var last: ?[]const u8 = null;
            for (replacement.targets) |target| {
                if (last) |prior| if (std.mem.order(u8, prior, target) != .lt) return error.InvalidDagMutation;
                last = target;
                try budget.consume(.edges, 1);
                try budget.consume(.bytes, target.len);
            }
            const storage_key = try key(alloc, id, replacement);
            defer alloc.free(storage_key);
            const stored = txn.get(storage_key) catch |err| blk: {
                if (err != error.NotFound) return err;
                break :blk "";
            };
            try budget.consume(.bytes, stored.len);
            // Storage values may be invalidated by subsequent reads.
            var remaining: []const u8 = try alloc.dupe(u8, stored);
            var old = try takeTarget(&remaining);
            var next: usize = 0;
            while (old != null or next < replacement.targets.len) {
                try budget.checkpoint();
                const order: std.math.Order = if (old == null) .gt else if (next == replacement.targets.len) .lt else std.mem.order(u8, old.?, replacement.targets[next]);
                if (order != .eq) {
                    try budget.consume(.edges, 1);
                    try changes.append(alloc, .{
                        .source = replacement.source,
                        .target = if (order == .lt) old.? else replacement.targets[next],
                        .present = order == .gt,
                    });
                }
                if (order != .gt) {
                    const prior = old.?;
                    old = try takeTarget(&remaining);
                    if (old) |target| if (std.mem.order(u8, prior, target) != .lt) return error.InvalidDagAuthority;
                }
                if (order != .lt) next += 1;
            }
        }

        std.mem.sort(validation.Change, changes.items, {}, struct {
            fn lessThan(_: void, a: validation.Change, b: validation.Change) bool {
                return Membership.order(a, b) == .lt;
            }
        }.lessThan);
        const membership = try alloc.alloc(Membership.Update, changes.items.len);
        const adjacency = try alloc.alloc(validation.Change, changes.items.len);
        const counts = try Membership.reconcile(alloc, txn, id, changes.items, membership, adjacency, budget);
        return .{ .adjacency = adjacency[0..counts.adjacency], .membership = membership[0..counts.updates] };
    }

    fn takeTarget(remaining: *[]const u8) !?[]const u8 {
        if (remaining.*.len == 0) return null;
        if (remaining.*.len < 4) return error.InvalidDagAuthority;
        const len = std.mem.readInt(u32, remaining.*[0..4], .big);
        remaining.* = remaining.*[4..];
        if (len > remaining.*.len) return error.InvalidDagAuthority;
        const target = remaining.*[0..len];
        remaining.* = remaining.*[len..];
        return target;
    }

    fn activationKey(id: Digest) [activation_prefix.len + @sizeOf(Digest)]u8 {
        return activation_prefix.* ++ id;
    }

    fn activationState(txn: anytype, id: Digest, epoch: u64) !State {
        const state = try Storage.load(txn, id);
        if (state.owner_epoch != epoch) return error.GraphConstraintOwnerChanged;
        if (state.phase != .validating or state.revision != 0) return error.GraphConstraintNotReady;
        return state;
    }

    /// Persist the scanner's source snapshot before fetching its first page.
    /// The catalog must hold the corresponding source-write fence.
    pub fn beginActivation(txn: anytype, id: Digest, epoch: u64, snapshot: Digest) !void {
        _ = try activationState(txn, id, epoch);
        try SourceFence.requireSnapshot(txn, snapshot);
        if (try readActivationProgress(txn, id)) |progress| {
            if (!std.mem.eql(u8, &progress.snapshot, &snapshot)) return error.GraphConstraintRevisionChanged;
            return;
        }
        try Storage.requireEmptyLedger(txn, id);
        try txn.put(&activationKey(id), &snapshot);
    }

    pub fn activationSnapshot(txn: anytype, id: Digest, epoch: u64) !?Digest {
        _ = try activationState(txn, id, epoch);
        const progress = (try readActivationProgress(txn, id)) orelse return null;
        return progress.snapshot;
    }

    /// The catalog must fence writes and pin the authoritative contribution
    /// snapshot before invoking this function. The cursor is the concatenation
    /// of the last owner digest and source. Each page commits atomically with
    /// its new cursor and is checked against every previous accepted page.
    pub fn seedActivationPage(
        alloc: A,
        txn: anytype,
        id: Digest,
        epoch: u64,
        snapshot: Digest,
        expected_after: ?[]const u8,
        replacements: []const Replacement,
        budget: *validation.Budget,
        witness: *validation.Witness,
    ) !void {
        _ = try activationState(txn, id, epoch);
        try SourceFence.requireSnapshot(txn, snapshot);
        try requireActivationCursor(txn, id, snapshot, expected_after);
        if (replacements.len == 0) return error.InvalidDagMutation;
        const first = replacements[0];
        if (expected_after) |after| {
            const owner_order = std.mem.order(u8, after[0..@sizeOf(Digest)], &first.owner);
            if (owner_order == .gt or (owner_order == .eq and
                std.mem.order(u8, after[@sizeOf(Digest)..], first.source) != .lt)) return error.InvalidDagMutation;
        }
        var memory: validation.Memory = .{ .parent = alloc, .budget = budget };
        seedPage(memory.allocator(), txn, id, snapshot, replacements, budget, witness) catch |err| {
            if (err == error.OutOfMemory and memory.denied) {
                budget.exhausted = .memory;
                return error.GraphConstraintCapacity;
            }
            return err;
        };
    }

    fn seedPage(alloc: A, txn: anytype, id: Digest, snapshot: Digest, replacements: []const Replacement, budget: *validation.Budget, witness: *validation.Witness) !void {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const delta = try plan(arena.allocator(), txn, id, replacements, budget);
        const reader: Adjacency.Reader(@TypeOf(txn)) = .{ .txn = txn, .domain_id = id };
        try validation.validate(alloc, reader, delta.adjacency, budget, witness);
        try Adjacency.stage(alloc, txn, id, delta.adjacency);
        try Membership.stage(alloc, txn, id, delta.membership);
        try stageManifests(alloc, txn, id, replacements);
        const last = replacements[replacements.len - 1];
        const progress = try std.mem.concat(alloc, u8, &.{ &snapshot, &last.owner, last.source });
        defer alloc.free(progress);
        try txn.put(&activationKey(id), progress);
    }

    /// Load the persisted cursor for the pinned snapshot when resuming work.
    pub fn activationCursor(alloc: A, txn: anytype, id: Digest, epoch: u64, snapshot: Digest) !?[]u8 {
        _ = try activationState(txn, id, epoch);
        const progress = (try readActivationProgress(txn, id)) orelse return null;
        if (!std.mem.eql(u8, &progress.snapshot, &snapshot)) return error.GraphConstraintRevisionChanged;
        const after = progress.after orelse return null;
        return try alloc.dupe(u8, after);
    }

    const ActivationProgress = struct { snapshot: Digest, after: ?[]const u8 };

    fn readActivationProgress(txn: anytype, id: Digest) !?ActivationProgress {
        const raw = txn.get(&activationKey(id)) catch |err| {
            if (err == error.NotFound) return null;
            return err;
        };
        const width = @sizeOf(Digest);
        if (raw.len != width and raw.len < 2 * width) return error.InvalidDagAuthority;
        return .{ .snapshot = raw[0..width].*, .after = if (raw.len == width) null else raw[width..] };
    }

    fn requireActivationCursor(txn: anytype, id: Digest, snapshot: Digest, expected_after: ?[]const u8) !void {
        const progress = (try readActivationProgress(txn, id)) orelse return error.GraphConstraintNotReady;
        if (!std.mem.eql(u8, &progress.snapshot, &snapshot)) return error.GraphConstraintRevisionChanged;
        if ((progress.after == null) != (expected_after == null)) return error.GraphConstraintRevisionChanged;
        if (progress.after) |after| {
            if (!std.mem.eql(u8, after, expected_after.?)) return error.GraphConstraintRevisionChanged;
        }
    }

    const ActivationProof = struct { snapshot: Digest, after: []const u8 };
    const certificate_prefix = "\x00\x00__metadata__:dag-activation-certificate:";

    fn activationCertificateKey(id: Digest) [certificate_prefix.len + @sizeOf(Digest)]u8 {
        return certificate_prefix.* ++ id;
    }

    fn activationCertificate(id: Digest, proof: ?ActivationProof) [33]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/dag-activation-certificate/v1");
        hash.update(&id);
        const kind: u8 = if (proof != null) 1 else 0;
        hash.update(&.{kind});
        if (proof) |value| {
            hash.update(&value.snapshot);
            hashBytes(&hash, value.after);
        }
        return [1]u8{kind} ++ hash.finalResult();
    }

    fn saveActivationCertificate(txn: anytype, id: Digest, proof: ?ActivationProof) !void {
        try txn.put(&activationCertificateKey(id), &activationCertificate(id, proof));
    }

    fn readActivationCertificate(txn: anytype, id: Digest) ![33]u8 {
        const raw = txn.get(&activationCertificateKey(id)) catch |err| {
            if (err == error.NotFound) return error.GraphConstraintNotReady;
            return err;
        };
        if (raw.len != 33 or raw[0] > 1) return error.InvalidDagAuthority;
        return raw[0..33].*;
    }

    fn requireActivationCertificate(txn: anytype, id: Digest, proof: ?ActivationProof) !void {
        const raw = try readActivationCertificate(txn, id);
        if (!std.mem.eql(u8, &raw, &activationCertificate(id, proof))) return error.GraphConstraintRevisionChanged;
    }

    /// Call only after the fenced snapshot reader has certified end of input
    /// at this exact cursor. An empty cursor certifies a pinned empty scan.
    /// Publish the catalog's enforcing state in the same
    /// transaction or through its authoritative terminal decision.
    pub fn finishActivation(txn: anytype, id: Digest, epoch: u64, snapshot: Digest, after: []const u8) !void {
        var state = try Storage.load(txn, id);
        if (state.owner_epoch != epoch) return error.GraphConstraintOwnerChanged;
        const proof: ActivationProof = .{ .snapshot = snapshot, .after = after };
        if (state.phase == .active) return requireActivationCertificate(txn, id, proof);
        if (state.phase != .validating or state.revision != 0) return error.GraphConstraintNotReady;
        try SourceFence.requireSnapshot(txn, snapshot);
        try requireActivationCursor(txn, id, snapshot, if (after.len == 0) null else after);
        try saveActivationCertificate(txn, id, proof);
        try txn.delete(&activationKey(id));
        state.phase = .active;
        try Storage.save(txn, state);
    }

    /// Stage the participant's vote in the reserved snapshot.
    pub fn prepare(alloc: A, txn: anytype, token: Token, versions: Digest, replacements: []const Replacement, budget: *validation.Budget, witness: *validation.Witness) !void {
        var memory: validation.Memory = .{ .parent = alloc, .budget = budget };
        const bounded = memory.allocator();
        var arena = std.heap.ArenaAllocator.init(bounded);
        defer arena.deinit();
        const delta = plan(arena.allocator(), txn, token.domain, replacements, budget) catch |err| {
            if (err == error.OutOfMemory and memory.denied) {
                budget.exhausted = .memory;
                return error.GraphConstraintCapacity;
            }
            return err;
        };
        try requireCandidate(token, versions, replacements);
        Storage.validate(bounded, txn, token, delta.adjacency, delta.membership, budget, witness) catch |err| {
            if (err == error.OutOfMemory and memory.denied) {
                budget.exhausted = .memory;
                return error.GraphConstraintCapacity;
            }
            return err;
        };
    }

    /// Stage the committed decision, effective manifests and accepted adjacency
    /// in one write transaction. The transaction owns rollback on any error.
    pub fn accept(alloc: A, txn: anytype, token: Token, versions: Digest, replacements: []const Replacement) !Receipt {
        try requireCandidate(token, versions, replacements);
        if (try Storage.readReceipt(txn, token.domain, token.attempt)) |_| {
            return Storage.resolve(txn, token, .accepted);
        }
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        // A durable commit decision must not be subject to a fresh admission
        // deadline or to limits changed since the participant voted.
        const state = try Storage.load(txn, token.domain);
        try state.require(token);
        if (!state.pending.?.validated) return error.GraphConstraintNotValidated;
        var replay_budget: validation.Budget = .{
            .limits = .{ .edges = std.math.maxInt(usize), .bytes = std.math.maxInt(usize), .requests = std.math.maxInt(usize) },
            .clock = .{ .context = &arena, .now = replayTime },
        };
        const delta = try plan(arena.allocator(), txn, token.domain, replacements, &replay_budget);
        const receipt = try Storage.accept(alloc, txn, token, delta.adjacency, delta.membership);
        try stageManifests(alloc, txn, token.domain, replacements);
        return receipt;
    }

    fn stageManifests(alloc: A, txn: anytype, id: Digest, replacements: []const Replacement) !void {
        for (replacements) |replacement| {
            const storage_key = try key(alloc, id, replacement);
            defer alloc.free(storage_key);
            if (replacement.targets.len == 0) {
                txn.delete(storage_key) catch |err| if (err != error.NotFound) return err;
                continue;
            }
            var encoded: std.ArrayList(u8) = .empty;
            defer encoded.deinit(alloc);
            for (replacement.targets) |target| {
                const len = std.math.cast(u32, target.len) orelse return error.GraphConstraintCapacity;
                var header: [4]u8 = undefined;
                std.mem.writeInt(u32, &header, len, .big);
                try encoded.appendSlice(alloc, &header);
                try encoded.appendSlice(alloc, target);
            }
            try txn.put(storage_key, encoded.items);
        }
    }

    fn replayTime(_: *anyopaque) u64 {
        return 0;
    }

    fn requireCandidate(token: Token, versions: Digest, replacements: []const Replacement) !void {
        const digest = candidate(versions, replacements);
        if (!std.mem.eql(u8, &digest, &token.attempt.candidate)) return error.GraphConstraintAttemptConflict;
    }
};

/// Durable prepare payload for an authority participant. Resolution reads
/// this packet after failover and uses the current owner's recovery token.
pub const TransactionParticipant = struct {
    const validation = @import("dag_validation.zig");
    const A = std.mem.Allocator;
    const prefix = "\x00\x00__metadata__:dag-prepared:";
    const magic = "DAGP\x01";
    const max_packet_bytes = 64 * 1024 * 1024;

    fn key(domain_id: Digest) [prefix.len + 32]u8 {
        return prefix.* ++ domain_id;
    }

    pub fn prepare(
        alloc: A,
        txn: anytype,
        token: Token,
        versions: Digest,
        replacements: []const Contributions.Replacement,
        budget: *validation.Budget,
        witness: *validation.Witness,
    ) !void {
        witness.* = .{};
        try Contributions.prepare(alloc, txn, token, versions, replacements, budget, witness);
        var memory: validation.Memory = .{ .parent = alloc, .budget = budget };
        const bounded = memory.allocator();
        const packet = encode(bounded, versions, replacements, budget) catch |err| {
            if (err == error.OutOfMemory and memory.denied) {
                budget.exhausted = .memory;
                return error.GraphConstraintCapacity;
            }
            return err;
        };
        defer bounded.free(packet);
        try txn.put(&key(token.domain), packet);
    }

    /// The caller authenticates the durable coordinator decision and attaches
    /// its acknowledgement to the same write transaction. An unknown decision
    /// must leave the reservation untouched.
    pub fn resolve(alloc: A, txn: anytype, domain_id: Digest, epoch: u64, attempt: Attempt, outcome: Outcome) !Receipt {
        const state = try Storage.load(txn, domain_id);
        if (state.owner_epoch != epoch) return error.GraphConstraintOwnerChanged;
        if (try Storage.readReceipt(txn, domain_id, attempt)) |receipt| {
            if (receipt.outcome != outcome) return error.GraphConstraintAttemptConflict;
            return receipt;
        }
        const token = (try state.recoveryToken(epoch)) orelse return error.GraphConstraintAttemptConflict;
        if (!std.meta.eql(token.attempt, attempt)) return error.GraphConstraintAttemptConflict;
        const receipt = switch (outcome) {
            .accepted => blk: {
                if (!state.pending.?.validated) return error.GraphConstraintNotValidated;
                const raw = txn.get(&key(domain_id)) catch |err| {
                    if (err == error.NotFound) return error.GraphConstraintNotValidated;
                    return err;
                };
                if (raw.len > max_packet_bytes) return error.InvalidDagAuthority;
                var arena = std.heap.ArenaAllocator.init(alloc);
                defer arena.deinit();
                const scratch = arena.allocator();
                var packet: Packet = .{ .remaining = try scratch.dupe(u8, raw) };
                if (!std.mem.eql(u8, try packet.take(magic.len), magic)) return error.InvalidDagAuthority;
                const versions = (try packet.take(32))[0..32].*;
                const replacements = try packet.decode(scratch);
                break :blk try Contributions.accept(scratch, txn, token, versions, replacements);
            },
            .aborted => try Storage.reject(txn, token, .aborted),
            .semantic_rejection => try Storage.reject(txn, token, .semantic_rejection),
        };
        txn.delete(&key(domain_id)) catch |err| if (err != error.NotFound) return err;
        return receipt;
    }

    fn append(packet: *std.ArrayList(u8), alloc: A, bytes: []const u8) !void {
        if (bytes.len > max_packet_bytes -| packet.items.len) return error.GraphConstraintCapacity;
        try packet.appendSlice(alloc, bytes);
    }

    fn appendCount(packet: *std.ArrayList(u8), alloc: A, count: usize) !void {
        var raw: [4]u8 = undefined;
        std.mem.writeInt(u32, &raw, std.math.cast(u32, count) orelse return error.GraphConstraintCapacity, .big);
        try append(packet, alloc, &raw);
    }

    fn appendBytes(packet: *std.ArrayList(u8), alloc: A, bytes: []const u8) !void {
        try appendCount(packet, alloc, bytes.len);
        try append(packet, alloc, bytes);
    }

    fn encode(alloc: A, versions: Digest, replacements: []const Contributions.Replacement, budget: *validation.Budget) ![]u8 {
        var packet: std.ArrayList(u8) = .empty;
        errdefer packet.deinit(alloc);
        try append(&packet, alloc, magic);
        try append(&packet, alloc, &versions);
        try appendCount(&packet, alloc, replacements.len);
        for (replacements) |replacement| {
            try budget.checkpoint();
            try append(&packet, alloc, &replacement.owner);
            try appendBytes(&packet, alloc, replacement.source);
            try appendCount(&packet, alloc, replacement.targets.len);
            for (replacement.targets) |target| {
                try budget.checkpoint();
                try appendBytes(&packet, alloc, target);
            }
        }
        return packet.toOwnedSlice(alloc);
    }

    const Packet = struct {
        remaining: []const u8,

        fn take(self: *Packet, len: usize) ![]const u8 {
            if (len > self.remaining.len) return error.InvalidDagAuthority;
            const raw = self.remaining[0..len];
            self.remaining = self.remaining[len..];
            return raw;
        }

        fn count(self: *Packet) !usize {
            return std.mem.readInt(u32, (try self.take(4))[0..4], .big);
        }

        fn bytes(self: *Packet) ![]const u8 {
            const size = try self.count();
            return self.take(size);
        }

        fn decode(self: *Packet, alloc: A) ![]Contributions.Replacement {
            const len = try self.count();
            if (len > self.remaining.len / 40) return error.InvalidDagAuthority;
            const replacements = try alloc.alloc(Contributions.Replacement, len);
            for (replacements) |*replacement| {
                const owner = (try self.take(32))[0..32].*;
                const source = try self.bytes();
                const target_count = try self.count();
                if (target_count > self.remaining.len / 4) return error.InvalidDagAuthority;
                const targets = try alloc.alloc([]const u8, target_count);
                for (targets) |*target| target.* = try self.bytes();
                replacement.* = .{ .owner = owner, .source = source, .targets = targets };
            }
            if (self.remaining.len != 0) return error.InvalidDagAuthority;
            return replacements;
        }
    };
};

test "DAG adjacency source prefixes distinguish arbitrary binary identifiers" {
    const alloc = std.testing.allocator;
    const first = try Adjacency.sourcePrefix(alloc, @splat(7), "a");
    defer alloc.free(first);
    const second = try Adjacency.sourcePrefix(alloc, @splat(7), "a\x00b");
    defer alloc.free(second);
    const other_domain = try Adjacency.sourcePrefix(alloc, @splat(8), "a");
    defer alloc.free(other_domain);
    try std.testing.expect(!std.mem.startsWith(u8, second, first));
    try std.testing.expect(!std.mem.eql(u8, first, other_domain));
}

test "DAG adjacency cursor stays inside its source and resumes strictly after a row" {
    const Txn = struct {
        const Entry = struct { key: []const u8 };
        keys: [3][]const u8,
        const Cursor = struct {
            keys: [3][]const u8,
            offset: usize = 0,
            fn close(_: *@This()) void {}
            fn next(self: *@This()) !?Entry {
                if (self.offset == self.keys.len) return null;
                const key = self.keys[self.offset];
                self.offset += 1;
                return .{ .key = key };
            }
            fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
                while (self.offset < self.keys.len and std.mem.order(u8, self.keys[self.offset], key) == .lt) self.offset += 1;
                return self.next();
            }
        };
        fn openCursor(self: *@This()) !Cursor {
            return .{ .keys = self.keys };
        }
    };
    const Clock = struct {
        fn now(_: *anyopaque) u64 {
            return 0;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const id: Digest = @splat(1);
    const a = try Adjacency.sourcePrefix(alloc, id, "a");
    const b = try Adjacency.sourcePrefix(alloc, id, "b");
    var txn: Txn = .{ .keys = .{
        try std.mem.concat(alloc, u8, &.{ a, "first" }),
        try std.mem.concat(alloc, u8, &.{ a, "second" }),
        try std.mem.concat(alloc, u8, &.{ b, "third" }),
    } };
    const reader: Adjacency.Reader(*Txn) = .{ .txn = &txn, .domain_id = id };
    var budget: Adjacency.validation.Budget = .{ .clock = .{ .context = &txn, .now = Clock.now } };
    const first = (try reader.next(alloc, "a", null, &budget)).?;
    const second = (try reader.next(alloc, "a", first.key, &budget)).?;
    try std.testing.expectEqualStrings("first", first.target);
    try std.testing.expectEqualStrings("second", second.target);
    try std.testing.expect(try reader.next(alloc, "a", second.key, &budget) == null);
    try std.testing.expectEqual(@as(usize, 3), budget.stats.requests);
}

test "DAG membership emits removal only after the final effective owner leaves" {
    const Txn = struct {
        count: ?[8]u8 = null,
        fn get(self: *@This(), _: []const u8) ![]const u8 {
            if (self.count) |*count| return count;
            return error.NotFound;
        }
        fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.count = value[0..8].*;
        }
        fn delete(self: *@This(), _: []const u8) !void {
            self.count = null;
        }
        fn now(_: *anyopaque) u64 {
            return 0;
        }
    };
    const alloc = std.testing.allocator;
    const id: Digest = @splat(1);
    var txn: Txn = .{};
    var budget: Membership.validation.Budget = .{ .clock = .{ .now = Txn.now, .context = &txn } };
    const add: Membership.validation.Change = .{ .source = "a", .target = "b", .present = true };
    var remove = add;
    remove.present = false;
    var updates: [2]Membership.Update = undefined;
    var adjacency: [2]Membership.validation.Change = undefined;
    const first = try Membership.reconcile(alloc, &txn, id, &.{ add, add }, &updates, &adjacency, &budget);
    try std.testing.expectEqual(@as(usize, 1), first.adjacency);
    try std.testing.expect(adjacency[0].present);
    try Membership.stage(alloc, &txn, id, updates[0..first.updates]);
    const transfer = try Membership.reconcile(alloc, &txn, id, &.{ remove, add }, &updates, &adjacency, &budget);
    try std.testing.expectEqual(@as(usize, 0), transfer.updates);
    const second = try Membership.reconcile(alloc, &txn, id, &.{remove}, &updates, &adjacency, &budget);
    try std.testing.expectEqual(@as(usize, 0), second.adjacency);
    try Membership.stage(alloc, &txn, id, updates[0..second.updates]);
    const third = try Membership.reconcile(alloc, &txn, id, &.{remove}, &updates, &adjacency, &budget);
    try std.testing.expectEqual(@as(usize, 1), third.adjacency);
    try std.testing.expect(!adjacency[0].present);
    try Membership.stage(alloc, &txn, id, updates[0..third.updates]);
    try std.testing.expectError(error.InvalidDagMutation, Membership.reconcile(alloc, &txn, id, &.{remove}, &updates, &adjacency, &budget));
}

test "DAG contribution acceptance persists full manifests and fences owner swaps" {
    const Txn = struct {
        const alloc = std.testing.allocator;
        rows: std.StringHashMap([]const u8),

        fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const stored = try alloc.dupe(u8, value);
            errdefer alloc.free(stored);
            const slot = try self.rows.getOrPut(key);
            if (slot.found_existing) {
                alloc.free(slot.value_ptr.*);
            } else {
                slot.key_ptr.* = alloc.dupe(u8, key) catch |err| {
                    _ = self.rows.remove(key);
                    return err;
                };
            }
            slot.value_ptr.* = stored;
        }
        fn delete(self: *@This(), key: []const u8) !void {
            const row = self.rows.fetchRemove(key) orelse return error.NotFound;
            alloc.free(row.key);
            alloc.free(row.value);
        }
        const Cursor = struct {
            const Entry = struct { key: []const u8 };
            rows: *const std.StringHashMap([]const u8),
            current: ?[]const u8 = null,

            fn close(_: *@This()) void {}
            fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
                return self.seek(key, false);
            }
            fn next(self: *@This()) !?Entry {
                return self.seek(self.current orelse return null, true);
            }
            fn seek(self: *@This(), key: []const u8, exclusive: bool) ?Entry {
                var keys = self.rows.keyIterator();
                var best: ?[]const u8 = null;
                while (keys.next()) |item| {
                    const order = std.mem.order(u8, item.*, key);
                    if (order == .lt or (exclusive and order == .eq)) continue;
                    if (best) |prior| if (std.mem.order(u8, prior, item.*) != .gt) continue;
                    best = item.*;
                }
                self.current = best;
                return .{ .key = best orelse return null };
            }
        };
        fn openCursor(self: *@This()) !Cursor {
            return .{ .rows = &self.rows };
        }
        fn now(_: *anyopaque) u64 {
            return 0;
        }
    };
    const alloc = std.testing.allocator;
    var txn: Txn = .{ .rows = .init(alloc) };
    defer {
        var rows = txn.rows.iterator();
        while (rows.next()) |row| {
            alloc.free(row.key_ptr.*);
            alloc.free(row.value_ptr.*);
        }
        txn.rows.deinit();
    }
    const id: Digest = @splat(1);
    const versions: Digest = @splat(7);
    const initial = [_]Contributions.Replacement{
        .{ .owner = @splat(1), .source = "a", .targets = &.{"b"} },
    };
    const transfer = [_]Contributions.Replacement{
        .{ .owner = @splat(1), .source = "a", .targets = &.{} },
        .{ .owner = @splat(2), .source = "a", .targets = &.{"b"} },
    };
    const remove = [_]Contributions.Replacement{
        .{ .owner = @splat(2), .source = "a", .targets = &.{} },
    };
    try Storage.create(&txn, id, 1);
    const edge = try std.mem.concat(alloc, u8, &.{ Adjacency.prefix, &id, "\x00\x00\x00\x01ab" });
    defer alloc.free(edge);
    try txn.put(edge, "");
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.activateNewIncarnation(&txn, id, 1));
    try txn.delete(edge);
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.requireActive(&txn, id));
    try Storage.activateNewIncarnation(&txn, id, 1);
    _ = try Storage.requireActive(&txn, id);
    var epoch: u64 = 1;
    for ([_][]const Contributions.Replacement{ &initial, &transfer, &remove }, 0..) |replacements, revision| {
        var attempt = testAttempt(@intCast(revision + 1));
        attempt.candidate = Contributions.candidate(versions, replacements);
        const token = (try Storage.begin(&txn, id, epoch, revision, attempt)).reserved;
        var budget: Contributions.validation.Budget = .{ .clock = .{ .now = Txn.now, .context = &txn } };
        var witness: Contributions.validation.Witness = .{};
        try TransactionParticipant.prepare(alloc, &txn, token, versions, replacements, &budget, &witness);
        try std.testing.expectError(error.GraphConstraintAttemptConflict, Contributions.accept(alloc, &txn, token, @splat(9), replacements));
        if (revision == 0) {
            try Storage.takeover(&txn, id, epoch, epoch + 1);
            try std.testing.expectError(error.GraphConstraintOwnerChanged, TransactionParticipant.resolve(alloc, &txn, id, epoch, attempt, .accepted));
            epoch += 1;
            const persisted = txn.rows.get(&TransactionParticipant.key(id)).?;
            const writable: []u8 = @constCast(persisted);
            writable[0] ^= 1;
            try std.testing.expectError(error.InvalidDagAuthority, TransactionParticipant.resolve(alloc, &txn, id, epoch, attempt, .accepted));
            writable[0] ^= 1;
        }
        const receipt = try TransactionParticipant.resolve(alloc, &txn, id, epoch, attempt, .accepted);
        try std.testing.expectEqual(revision + 1, receipt.revision);
        try std.testing.expectEqualDeep(receipt, try TransactionParticipant.resolve(alloc, &txn, id, epoch, attempt, .accepted));
        try std.testing.expect(!txn.rows.contains(&TransactionParticipant.key(id)));
        try std.testing.expectEqual(revision < 2, txn.rows.contains(edge));
        if (revision == 1) {
            const reverse: []const Contributions.Replacement = &.{
                .{ .owner = @splat(3), .source = "b", .targets = &.{"a"} },
            };
            var rejected = testAttempt(9);
            rejected.candidate = Contributions.candidate(versions, reverse);
            const reverse_token = (try Storage.begin(&txn, id, epoch, 2, rejected)).reserved;
            try std.testing.expectError(error.GraphConstraintBusy, Storage.retire(&txn, id, epoch));
            try std.testing.expectError(error.GraphCycle, Contributions.prepare(alloc, &txn, reverse_token, versions, reverse, &budget, &witness));
            try std.testing.expectError(error.GraphConstraintNotValidated, Contributions.accept(alloc, &txn, reverse_token, versions, reverse));
            _ = try TransactionParticipant.resolve(alloc, &txn, id, epoch, rejected, .semantic_rejection);
            try std.testing.expectError(error.GraphCycle, Storage.requireAccepted(&txn, id, 2, rejected));
            try std.testing.expectError(error.GraphConstraintRevisionChanged, Storage.requireAccepted(&txn, id, 1, rejected));
        }
    }
    const existing_id: Digest = @splat(2);
    try Storage.create(&txn, existing_id, 1);
    const fence: SourceFence = .{ .activation = @splat(9), .snapshot = versions };
    try std.testing.expectError(error.GraphConstraintNotReady, Contributions.beginActivation(&txn, existing_id, 1, versions));
    try fence.acquire(&txn);
    var budget: Contributions.validation.Budget = .{ .clock = .{ .now = Txn.now, .context = &txn } };
    var witness: Contributions.validation.Witness = .{};
    try std.testing.expectError(error.GraphConstraintNotReady, Contributions.seedActivationPage(alloc, &txn, existing_id, 1, versions, null, &initial, &budget, &witness));
    try Contributions.beginActivation(&txn, existing_id, 1, versions);
    try Contributions.beginActivation(&txn, existing_id, 1, versions);
    try std.testing.expectError(error.GraphConstraintRevisionChanged, Contributions.beginActivation(&txn, existing_id, 1, @splat(9)));
    try std.testing.expectEqualDeep(versions, (try Contributions.activationSnapshot(&txn, existing_id, 1)).?);
    try std.testing.expect((try Contributions.activationCursor(alloc, &txn, existing_id, 1, versions)) == null);
    try Contributions.seedActivationPage(alloc, &txn, existing_id, 1, versions, null, &initial, &budget, &witness);
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.activateNewIncarnation(&txn, existing_id, 1));
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.begin(&txn, existing_id, 1, 0, testAttempt(7)));
    try Storage.takeover(&txn, existing_id, 1, 2);
    const first_cursor = (try Contributions.activationCursor(alloc, &txn, existing_id, 2, versions)).?;
    defer alloc.free(first_cursor);
    const cycle: []const Contributions.Replacement = &.{
        .{ .owner = @splat(1), .source = "b", .targets = &.{"a"} },
    };
    try std.testing.expectError(error.GraphConstraintOwnerChanged, Contributions.seedActivationPage(alloc, &txn, existing_id, 1, versions, first_cursor, cycle, &budget, &witness));
    try std.testing.expectError(error.GraphCycle, Contributions.seedActivationPage(alloc, &txn, existing_id, 2, versions, first_cursor, cycle, &budget, &witness));
    const valid_page: []const Contributions.Replacement = &.{
        .{ .owner = @splat(1), .source = "b", .targets = &.{"c"} },
    };
    try Contributions.seedActivationPage(alloc, &txn, existing_id, 2, versions, first_cursor, valid_page, &budget, &witness);
    try std.testing.expectError(error.GraphConstraintRevisionChanged, Contributions.finishActivation(&txn, existing_id, 2, versions, first_cursor));
    const last_cursor = (try Contributions.activationCursor(alloc, &txn, existing_id, 2, versions)).?;
    defer alloc.free(last_cursor);
    try Contributions.finishActivation(&txn, existing_id, 2, versions, last_cursor);
    try Contributions.finishActivation(&txn, existing_id, 2, versions, last_cursor);
    try std.testing.expectError(error.GraphConstraintRevisionChanged, Contributions.finishActivation(&txn, existing_id, 2, versions, first_cursor));
    try std.testing.expectError(error.GraphConstraintRevisionChanged, Storage.activateNewIncarnation(&txn, existing_id, 2));
    try Storage.activateNewIncarnation(&txn, id, epoch);
    _ = try Storage.begin(&txn, existing_id, 2, 0, testAttempt(7));

    const empty_id: Digest = @splat(3);
    try Storage.create(&txn, empty_id, 1);
    try std.testing.expectError(error.GraphConstraintNotReady, Contributions.finishActivation(&txn, empty_id, 1, versions, ""));
    try Contributions.beginActivation(&txn, empty_id, 1, versions);
    try Storage.takeover(&txn, empty_id, 1, 2);
    try std.testing.expectEqualDeep(versions, (try Contributions.activationSnapshot(&txn, empty_id, 2)).?);
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.activateNewIncarnation(&txn, empty_id, 2));
    try Contributions.finishActivation(&txn, empty_id, 2, versions, "");
    try Contributions.finishActivation(&txn, empty_id, 2, versions, "");
    try fence.release(&txn);

    try Storage.retire(&txn, id, epoch);
    try std.testing.expectError(error.GraphConstraintNotReady, Storage.activateNewIncarnation(&txn, id, epoch));
}
