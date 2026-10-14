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

//! One bounded cancellation-intent slot per physical owner. An intent closes
//! delayed normal admission/control, but is NOT a canceled publication receipt:
//! fence/drain, allocation burns and cleanup must still complete independently.
//! Callers authenticate metadata and stage the durable outbox in the SAME
//! transaction. Every error requires abort, including a replay-clock error.
const std = @import("std");
const topology = @import("relational_integrity_topology.zig");
const authority = @import("../source_authority.zig");
const positions = @import("receipt_position.zig");
const identity = @import("doc_identity.zig");
const catalog = @import("relational_integrity_catalog.zig");
const scope = @import("online_source_contract.zig");

pub const key = "\x00\x00__metadata__:relational_index_owner_job:v1:cancel";

/// Same immutable metadata header as admission; no physical allocation or
/// receipt digest is accepted from the coordinator.
pub const Intent = struct {
    id: [16]u8,
    plan_digest: [32]u8,
    owner_digest: [32]u8,
    ordinal: u16,
    fence: topology.Fence,
    candidate_digest: [32]u8,
    reservation_digest: [32]u8,

    pub fn validate(self: Intent) !void {
        _ = try self.fence.encode();
        if (self.fence.role != .index_build or self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0 or
            self.ordinal >= @import("relational_index_publication_progress.zig").max_owners or
            std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.owner_digest, 0) or std.mem.allEqual(u8, &self.candidate_digest, 0) or
            std.mem.allEqual(u8, &self.reservation_digest, 0)) return error.InvalidPrivateIndexCancellation;
    }

    pub fn fromBinding(binding: anytype) Intent {
        return .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .fence = binding.fence, .candidate_digest = binding.integrity_candidate, .reservation_digest = binding.integrity_reserved };
    }

    pub fn eql(self: Intent, other: Intent) bool {
        return std.mem.eql(u8, &self.id, &other.id) and std.mem.eql(u8, &self.plan_digest, &other.plan_digest) and
            std.mem.eql(u8, &self.owner_digest, &other.owner_digest) and self.ordinal == other.ordinal and
            self.fence.eql(other.fence) and std.mem.eql(u8, &self.candidate_digest, &other.candidate_digest) and
            std.mem.eql(u8, &self.reservation_digest, &other.reservation_digest);
    }
};

fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub const Record = struct {
    pub const encoded_len = 368;
    intent: Intent,
    position: positions.Native,

    pub fn encode(self: Record) ![encoded_len]u8 {
        try self.intent.validate();
        try self.position.validate();
        if (!self.position.namespace.eql(self.intent.fence.namespace)) return error.IdentityNamespaceMismatch;
        var out: [encoded_len]u8 = @splat(0);
        @memcpy(out[0..4], "AICI");
        std.mem.writeInt(u32, out[4..8], 1, .little);
        @memcpy(out[8..24], &self.intent.id);
        @memcpy(out[24..56], &self.intent.plan_digest);
        @memcpy(out[56..88], &self.intent.owner_digest);
        std.mem.writeInt(u16, out[88..90], self.intent.ordinal, .little);
        @memcpy(out[96..232], &try self.intent.fence.encode());
        @memcpy(out[232..264], &self.intent.candidate_digest);
        @memcpy(out[264..296], &self.intent.reservation_digest);
        @memcpy(out[296..329], &try (positions.Position{ .native = self.position }).encode());
        @memcpy(out[336..368], &digest(out[0..336]));
        return out;
    }

    pub fn decode(bytes: []const u8) !Record {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "AICI") or
            std.mem.readInt(u32, bytes[4..8], .little) != 1 or !std.mem.allEqual(u8, bytes[90..96], 0) or
            !std.mem.allEqual(u8, bytes[329..336], 0) or !std.mem.eql(u8, bytes[336..368], &digest(bytes[0..336])))
            return error.InvalidPrivateIndexCancellation;
        const position = positions.Position.decode(bytes[296..329]) catch return error.InvalidPrivateIndexCancellation;
        if (position != .native) return error.InvalidPrivateIndexCancellation;
        const result: Record = .{
            .intent = .{ .id = bytes[8..24].*, .plan_digest = bytes[24..56].*, .owner_digest = bytes[56..88].*, .ordinal = std.mem.readInt(u16, bytes[88..90], .little), .fence = try topology.Fence.decode(bytes[96..232]), .candidate_digest = bytes[232..264].*, .reservation_digest = bytes[264..296].* },
            .position = position.native,
        };
        _ = try result.encode();
        return result;
    }
};

fn optional(txn: anytype, item: []const u8) !?[]const u8 {
    return txn.get(item) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

fn requireScope(txn: anytype, namespace: identity.Namespace) !authority.State {
    const actual = (try identity.loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
    if (!actual.eql(namespace)) return error.IdentityNamespaceMismatch;
    return authority.require(txn, .native, scope.namespaceBytes(namespace));
}

/// Decode borrowed bytes before any further read; this remains safe on stores
/// that reuse one scratch buffer for every point lookup.
pub fn load(txn: anytype) !?Record {
    const record = if (try optional(txn, key)) |bytes| try Record.decode(bytes) else return null;
    const state = try requireScope(txn, record.intent.fence.namespace);
    if (record.position.sequence > state.sequence) return error.InvalidPrivateIndexCancellation;
    return record;
}

/// A retained completed intent is a tombstone, not pending work for a later
/// owner. Only the exact durable completed-epoch proof can make it inert.
pub fn pending(txn: anytype) !?Record {
    const record = (try load(txn)) orelse return null;
    if (try topology.completed(txn)) |completed| {
        if (completed.admission_epoch >= record.intent.fence.admission_epoch) {
            try requireCompleted(txn, record);
            return null;
        }
    }
    return record;
}

fn requireCompleted(txn: anytype, record: Record) !void {
    const completed = (try topology.completed(txn)) orelse return error.PrivateIndexCancellationPending;
    if (!completed.namespace.eql(record.intent.fence.namespace) or
        completed.admission_epoch < record.intent.fence.admission_epoch or
        (completed.admission_epoch == record.intent.fence.admission_epoch and !completed.eql(record.intent.fence)))
        return error.PrivateIndexCancellationPending;
}

/// An older slot may be replaced only once its epoch has been durably consumed.
/// Otherwise forgetting it would reopen delayed admission for the old plan.
pub const Stage = struct { record: Record, duplicate: bool };

pub fn stage(txn: anytype, intent: Intent, binding: anytype, replay: ?positions.Native) !Stage {
    try intent.validate();
    _ = try requireScope(txn, intent.fence.namespace);
    if (replay) |position| {
        try position.validate();
        if (!position.namespace.eql(intent.fence.namespace)) return error.IdentityNamespaceMismatch;
    }
    if (binding) |current| {
        if (current.authority != .native or !intent.eql(Intent.fromBinding(current))) return error.PrivateIndexOwnerJobChanged;
    }
    if (try load(txn)) |previous| {
        if (previous.intent.eql(intent)) {
            try verifyRecovery(txn, binding);
            if (replay) |position| if (!std.meta.eql(position, previous.position)) return error.InvalidControlReceiptPosition;
            return .{ .record = previous, .duplicate = true };
        }
        if (intent.fence.admission_epoch <= previous.intent.fence.admission_epoch) return error.PrivateIndexOwnerJobChanged;
        try requireCompleted(txn, previous);
    }
    if (binding == null) {
        if (try topology.nextEpoch(txn) != intent.fence.admission_epoch) return error.IntegrityTopologyChanged;
        const before = (try optional(txn, catalog.key)) orelse return error.IntegrityCatalogChanged;
        if (!std.mem.eql(u8, &digest(before), &intent.fence.catalog_digest)) return error.IntegrityCatalogChanged;
    }
    const record: Record = .{ .intent = intent, .position = .{ .namespace = intent.fence.namespace, .sequence = try authority.advance(txn, scope.namespaceBytes(intent.fence.namespace), if (replay) |value| value.sequence else null) } };
    try txn.put(key, &try record.encode());
    try topology.stagePrivateIndexCancellationIntent(txn, intent.fence);
    return .{ .record = record, .duplicate = false };
}

pub fn requireFence(txn: anytype, fence: topology.Fence) !void {
    const record = (try load(txn)) orelse return error.PrivateIndexCancellationRequired;
    if (!record.intent.fence.eql(fence)) return error.PrivateIndexOwnerJobChanged;
}

pub fn requirePendingFence(txn: anytype, fence: topology.Fence) !void {
    try requireFence(txn, fence);
    if (try topology.completed(txn)) |completed| {
        if (completed.admission_epoch >= fence.admission_epoch) return error.PrivateIndexPublicationRequired;
    }
}

/// Normal admission and mutation paths call this in their committing snapshot.
/// Cancellation workers use requireCanceled instead, never a caller bypass bit.
pub fn requireOpen(txn: anytype, binding: anytype) !void {
    const record = (try load(txn)) orelse return;
    if (record.intent.fence.admission_epoch < binding.fence.admission_epoch) return requireCompleted(txn, record);
    if (record.intent.fence.admission_epoch == binding.fence.admission_epoch and
        !record.intent.eql(Intent.fromBinding(binding))) return error.PrivateIndexOwnerJobChanged;
    return error.PrivateIndexPublicationCanceled;
}

pub fn requireCanceled(txn: anytype, binding: anytype) !void {
    const record = (try load(txn)) orelse return error.PrivateIndexCancellationRequired;
    if (binding.authority != .native or !record.intent.eql(Intent.fromBinding(binding))) return error.PrivateIndexOwnerJobChanged;
}

/// Structural recovery must validate even an intent for a never-visited job.
/// Retained completed epochs are inert; pending intent identity cannot drift.
pub fn verifyRecovery(txn: anytype, binding: anytype) !void {
    const record = (try load(txn)) orelse return;
    if (binding) |current| {
        if (current.fence.admission_epoch > record.intent.fence.admission_epoch) return requireCompleted(txn, record);
        return requireCanceled(txn, current);
    }
    if (try topology.completed(txn)) |completed| {
        if (completed.admission_epoch >= record.intent.fence.admission_epoch) return requireCompleted(txn, record);
    }
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(record.intent.fence)) return error.IntegrityTopologyChanged;
    const before = (try optional(txn, catalog.key)) orelse return error.IntegrityCatalogChanged;
    if (!std.mem.eql(u8, &digest(before), &record.intent.fence.catalog_digest)) return error.IntegrityCatalogChanged;
}

test "relational index candidate cancellation intent framing preserves canonical native identity" {
    const intent: Intent = .{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .catalog_digest = @splat(4) },
        .candidate_digest = @splat(5),
        .reservation_digest = @splat(6),
    };
    const record: Record = .{ .intent = intent, .position = .{ .namespace = intent.fence.namespace, .sequence = std.math.maxInt(u64) } };
    const bytes = try record.encode();
    try std.testing.expectEqualDeep(record, try Record.decode(&bytes));
    for (0..bytes.len) |length| try std.testing.expectError(error.InvalidPrivateIndexCancellation, Record.decode(bytes[0..length]));
    for ([_]usize{ 0, 4, 90, 329, 336 }) |offset| {
        var corrupt = bytes;
        corrupt[offset] ^= 1;
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, Record.decode(&corrupt));
    }
    for ([_]usize{ 90, 329 }) |offset| {
        var corrupt = bytes;
        corrupt[offset] = 1;
        @memcpy(corrupt[336..368], &digest(corrupt[0..336]));
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, Record.decode(&corrupt));
    }
    var raft = bytes;
    @memcpy(raft[296..329], &try (positions.Position{ .raft = .{ .term = 1, .index = 1 } }).encode());
    @memcpy(raft[336..368], &digest(raft[0..336]));
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, Record.decode(&raft));
    var foreign = record;
    foreign.position.namespace.range_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, foreign.encode());
    foreign = record;
    foreign.intent.plan_digest = @splat(0);
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, foreign.encode());
}
