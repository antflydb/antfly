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

//! Exact, immutable proof for retiring one hosted hidden initial-FK replica.
//! A ticket is metadata-owned discovery, not permission to unlink. The data
//! owner must separately prove its cold AICH and local replica-catalog state,
//! persist an intent, drain readers/writers/Raft, and durably unlink before it
//! may attest completion. Metadata accepts that attestation only from the
//! currently registered physical store root.
const std = @import("std");
const incarnation = @import("incarnation.zig");
const retirement = @import("fk_initial_retirement.zig");

pub const ticket_encoded_len = 256;
pub const intent_encoded_len = 296;
pub const receipt_encoded_len = 336;
pub const Digest = [32]u8;

/// The obsolete replica's original process incarnation is intentionally part
/// of the immutable historical identity. It is *not* the reporter fence: a
/// restarted process on the same physical root may complete old work.
pub const Ticket = struct {
    metadata_incarnation: incarnation.MetadataClusterIncarnation,
    /// Terminal publication revision (canceled or published-obsolete). The
    /// legacy field name is retained on the wire; Replica binds the reason.
    cancel_revision: u64,
    replica: retirement.Replica,

    pub fn validate(self: @This()) !void {
        if (!incarnation.isValid(self.metadata_incarnation) or self.cancel_revision == 0 or
            self.replica.store_id == 0 or self.replica.store_incarnation == 0 or
            self.replica.store_root_incarnation == 0 or !self.replica.canceled or self.replica.acked)
            return error.InvalidInitialFkRetirementTicket;
        self.replica.validate() catch return error.InvalidInitialFkRetirementTicket;
    }

    pub fn encode(self: @This()) ![ticket_encoded_len]u8 {
        try self.validate();
        var bytes: [ticket_encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "IFRT");
        bytes[4] = 1;
        @memcpy(bytes[8..40], &self.metadata_incarnation);
        std.mem.writeInt(u64, bytes[40..48], self.cancel_revision, .little);
        const replica_bytes = try self.replica.encode();
        @memcpy(bytes[48..224], &replica_bytes);
        std.crypto.hash.Blake3.hash(bytes[0..224], bytes[224..256], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != ticket_encoded_len or !std.mem.eql(u8, bytes[0..4], "IFRT") or
            bytes[4] != 1 or !std.mem.allEqual(u8, bytes[5..8], 0))
            return error.InvalidInitialFkRetirementTicket;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..224], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[224..256])) return error.InvalidInitialFkRetirementTicket;
        const result: @This() = .{
            .metadata_incarnation = bytes[8..40].*,
            .cancel_revision = std.mem.readInt(u64, bytes[40..48], .little),
            .replica = retirement.Replica.decode(bytes[48..224]) catch return error.InvalidInitialFkRetirementTicket,
        };
        try result.validate();
        return result;
    }

    pub fn digest(self: @This()) !Digest {
        const encoded = try self.encode();
        return encoded[224..256].*;
    }
};

/// The same file is rewritten and fsynced at the logical unlink boundary.
/// `unlinked` means the exact group root has been renamed to the durable trash
/// namespace and its parent directory synced; background recursive deletion
/// may continue after the ACK.
pub const Intent = struct {
    ticket: Ticket,
    phase: Phase,

    pub const Phase = enum(u8) { prepared = 1, unlinked = 2 };

    pub fn encode(self: @This()) ![intent_encoded_len]u8 {
        var bytes: [intent_encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "IFRI");
        bytes[4] = 1;
        bytes[5] = @intFromEnum(self.phase);
        const ticket_bytes = try self.ticket.encode();
        @memcpy(bytes[8..264], &ticket_bytes);
        std.crypto.hash.Blake3.hash(bytes[0..264], bytes[264..296], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != intent_encoded_len or !std.mem.eql(u8, bytes[0..4], "IFRI") or
            bytes[4] != 1 or !std.mem.allEqual(u8, bytes[6..8], 0))
            return error.InvalidInitialFkRetirementIntent;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..264], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[264..296])) return error.InvalidInitialFkRetirementIntent;
        return .{
            .ticket = Ticket.decode(bytes[8..264]) catch return error.InvalidInitialFkRetirementIntent,
            .phase = std.enums.fromInt(Phase, bytes[5]) orelse return error.InvalidInitialFkRetirementIntent,
        };
    }

    pub fn digest(self: @This()) !Digest {
        const encoded = try self.encode();
        return encoded[264..296].*;
    }
};

/// Authenticated current reporter attestation. `reporter_incarnation` is
/// checked against the current store registration, not against the ticket's
/// historical store incarnation. A duplicate ACK for the same ticket is safe
/// after a reporter restart on the same physical root.
pub const Receipt = struct {
    ticket: Ticket,
    reporter_incarnation: u64,
    unlinked_intent_digest: Digest,

    pub fn validate(self: @This()) !void {
        try self.ticket.validate();
        const expected = try (Intent{ .ticket = self.ticket, .phase = .unlinked }).digest();
        if (self.reporter_incarnation == 0 or !std.mem.eql(u8, &self.unlinked_intent_digest, &expected))
            return error.InvalidInitialFkRetirementReceipt;
    }

    pub fn fromUnlinkedIntent(intent: Intent, reporter_incarnation: u64) !@This() {
        if (intent.phase != .unlinked) return error.InitialFkRetirementNotUnlinked;
        const result: @This() = .{
            .ticket = intent.ticket,
            .reporter_incarnation = reporter_incarnation,
            .unlinked_intent_digest = try intent.digest(),
        };
        try result.validate();
        return result;
    }

    pub fn encode(self: @This()) ![receipt_encoded_len]u8 {
        try self.validate();
        var bytes: [receipt_encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "IFRA");
        bytes[4] = 1;
        const ticket_bytes = try self.ticket.encode();
        @memcpy(bytes[8..264], &ticket_bytes);
        std.mem.writeInt(u64, bytes[264..272], self.reporter_incarnation, .little);
        @memcpy(bytes[272..304], &self.unlinked_intent_digest);
        std.crypto.hash.Blake3.hash(bytes[0..304], bytes[304..336], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != receipt_encoded_len or !std.mem.eql(u8, bytes[0..4], "IFRA") or
            bytes[4] != 1 or !std.mem.allEqual(u8, bytes[5..8], 0))
            return error.InvalidInitialFkRetirementReceipt;
        var digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..304], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[304..336])) return error.InvalidInitialFkRetirementReceipt;
        const result: @This() = .{
            .ticket = Ticket.decode(bytes[8..264]) catch return error.InvalidInitialFkRetirementReceipt,
            .reporter_incarnation = std.mem.readInt(u64, bytes[264..272], .little),
            .unlinked_intent_digest = bytes[272..304].*,
        };
        try result.validate();
        return result;
    }
};

/// Pure comparison used by the Raft ACK transaction after it has authenticated
/// the reporter and looked up the exact historical work item. A root swap,
/// reused group/replica ID, or another cancellation epoch cannot acknowledge
/// this work, even when other numeric IDs happen to match.
pub fn requireExactWork(ticket: Ticket, stored: retirement.Replica, metadata_cluster: incarnation.MetadataClusterIncarnation, cancel_revision: u64) !void {
    try ticket.validate();
    if (!incarnation.isValid(metadata_cluster) or cancel_revision != ticket.cancel_revision or
        !std.mem.eql(u8, &metadata_cluster, &ticket.metadata_incarnation))
        return error.InitialFkRetirementWorkChanged;
    var canonical = stored;
    canonical.acked = false;
    if (!std.meta.eql(canonical, ticket.replica)) return error.InitialFkRetirementWorkChanged;
}

pub const CurrentReporter = struct {
    node_id: u64,
    store_id: u64,
    reporter_incarnation: u64,
    store_root_incarnation: u128,
    live: bool,
};

/// The transport authenticates the store principal separately. The Raft ACK
/// transaction then compares that principal's current registration with the
/// receipt, rather than mistaking the historical placement's process
/// incarnation for the current reporter's incarnation.
pub fn requireCurrentReporter(receipt: Receipt, current: CurrentReporter) !void {
    try receipt.validate();
    if (!current.live or current.node_id != receipt.ticket.replica.node_id or
        current.store_id != receipt.ticket.replica.store_id or
        current.reporter_incarnation != receipt.reporter_incarnation or
        current.store_root_incarnation != receipt.ticket.replica.store_root_incarnation)
        return error.InitialFkRetirementReporterChanged;
}

test "hosted initial FK retirement ticket, intent, receipt bind exact root and cancellation" {
    const base: Ticket = .{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .cancel_revision = 29,
        .replica = .{
            .plan_id = @splat(1),
            .plan_digest = @splat(2),
            .child_table_id = 3,
            .group_id = 5,
            .range_id = 7,
            .node_id = 11,
            .store_id = 13,
            .store_incarnation = 17,
            .store_root_incarnation = 19,
            .replica_id = 23,
            .root_generation = 27,
            .canceled = true,
        },
    };
    const encoded = try base.encode();
    try std.testing.expectEqualDeep(base, try Ticket.decode(&encoded));
    var corrupt = encoded;
    corrupt[120] ^= 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementTicket, Ticket.decode(&corrupt));
    var wrong_root = base;
    wrong_root.replica.store_root_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, requireExactWork(wrong_root, base.replica, base.metadata_incarnation, base.cancel_revision));
    var wrong_generation = base;
    wrong_generation.replica.root_generation += 1;
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, requireExactWork(wrong_generation, base.replica, base.metadata_incarnation, base.cancel_revision));
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, requireExactWork(base, base.replica, base.metadata_incarnation, base.cancel_revision + 1));
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, requireExactWork(base, base.replica, "ffffffffffffffffffffffffffffffff".*, base.cancel_revision));
    try requireExactWork(base, base.replica, base.metadata_incarnation, base.cancel_revision);
    var already_acked = base.replica;
    already_acked.acked = true;
    try requireExactWork(base, already_acked, base.metadata_incarnation, base.cancel_revision);

    const prepared: Intent = .{ .ticket = base, .phase = .prepared };
    try std.testing.expectError(error.InitialFkRetirementNotUnlinked, Receipt.fromUnlinkedIntent(prepared, 31));
    const unlinked: Intent = .{ .ticket = base, .phase = .unlinked };
    const local_bytes = try unlinked.encode();
    try std.testing.expectEqualDeep(unlinked, try Intent.decode(&local_bytes));
    var bad_local = local_bytes;
    bad_local[36] ^= 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementIntent, Intent.decode(&bad_local));
    const receipt = try Receipt.fromUnlinkedIntent(unlinked, 31);
    const current: CurrentReporter = .{
        .node_id = base.replica.node_id,
        .store_id = base.replica.store_id,
        .reporter_incarnation = 31,
        .store_root_incarnation = base.replica.store_root_incarnation,
        .live = true,
    };
    try requireCurrentReporter(receipt, current);
    var replaced = current;
    replaced.store_root_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, requireCurrentReporter(receipt, replaced));
    replaced = current;
    replaced.reporter_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, requireCurrentReporter(receipt, replaced));
    replaced = current;
    replaced.live = false;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, requireCurrentReporter(receipt, replaced));
    const receipt_bytes = try receipt.encode();
    try std.testing.expectEqualDeep(receipt, try Receipt.decode(&receipt_bytes));
    var corrupt_receipt = receipt_bytes;
    corrupt_receipt[265] ^= 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementReceipt, Receipt.decode(&corrupt_receipt));
    var forged_receipt = receipt;
    forged_receipt.unlinked_intent_digest = try prepared.digest();
    try std.testing.expectError(error.InvalidInitialFkRetirementReceipt, forged_receipt.encode());
}
