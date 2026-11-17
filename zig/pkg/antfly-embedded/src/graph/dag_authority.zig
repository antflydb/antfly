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
