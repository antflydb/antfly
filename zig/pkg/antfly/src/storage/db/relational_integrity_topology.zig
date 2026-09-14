// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Durable quiescing admission for coordinated range ownership changes.
//! Existing participants finish under their original owner; new participants
//! cannot enter once the fence commits. No transaction lock is transferred.
const std = @import("std");
const identity = @import("doc_identity.zig");
const transactions = @import("../transactions.zig");
const integrity = @import("relational_integrity.zig");
const catalog = @import("relational_integrity_catalog.zig");
const Allocator = std.mem.Allocator;

pub const fence_key = "\x00\x00__metadata__:relational_integrity_topology_fence";
pub const receipt_key = "\x00\x00__metadata__:relational_integrity_topology_receipt";
pub const abort_prefix = "\x00\x00__metadata__:relational_integrity_topology_aborted:";
pub const Role = enum(u8) { split_source = 1, split_destination = 2, merge_source = 3, merge_destination = 4, backup_snapshot = 5 };

pub const Fence = struct {
    admission_epoch: u64 = 1,
    transition_id: u64,
    attempt: u64,
    peer_group_id: u64,
    owner_group_id: u64,
    role: Role,
    namespace: identity.Namespace,
    catalog_digest: integrity.Digest,

    pub fn eql(a: Fence, b: Fence) bool {
        return a.admission_epoch == b.admission_epoch and a.transition_id == b.transition_id and a.attempt == b.attempt and
            a.peer_group_id == b.peer_group_id and a.owner_group_id == b.owner_group_id and a.role == b.role and
            a.namespace.eql(b.namespace) and std.mem.eql(u8, &a.catalog_digest, &b.catalog_digest);
    }

    pub fn encode(self: Fence) ![136]u8 {
        if (self.admission_epoch == 0 or self.transition_id == 0 or self.attempt == 0 or self.peer_group_id == 0 or self.owner_group_id == 0 or self.namespace.table_id == 0)
            return error.InvalidIntegrityTopologyFence;
        var bytes: [136]u8 = @splat(0);
        @memcpy(bytes[0..4], "AIT1");
        bytes[4] = @intFromEnum(self.role);
        std.mem.writeInt(u64, bytes[8..16], self.transition_id, .little);
        std.mem.writeInt(u64, bytes[16..24], self.attempt, .little);
        std.mem.writeInt(u64, bytes[24..32], self.peer_group_id, .little);
        std.mem.writeInt(u64, bytes[32..40], self.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[40..48], self.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[48..56], self.namespace.range_id, .little);
        @memcpy(bytes[56..88], &self.catalog_digest);
        std.mem.writeInt(u64, bytes[88..96], self.owner_group_id, .little);
        std.mem.writeInt(u64, bytes[96..104], self.admission_epoch, .little);
        std.crypto.hash.Blake3.hash(bytes[0..104], bytes[104..136], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Fence {
        if (bytes.len != 136 or !std.mem.eql(u8, bytes[0..4], "AIT1") or
            !std.mem.allEqual(u8, bytes[5..8], 0)) return error.InvalidIntegrityTopologyFence;
        var checksum: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..104], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[104..136])) return error.InvalidIntegrityTopologyFence;
        const result: Fence = .{
            .admission_epoch = std.mem.readInt(u64, bytes[96..104], .little),
            .transition_id = std.mem.readInt(u64, bytes[8..16], .little),
            .attempt = std.mem.readInt(u64, bytes[16..24], .little),
            .peer_group_id = std.mem.readInt(u64, bytes[24..32], .little),
            .owner_group_id = std.mem.readInt(u64, bytes[88..96], .little),
            .role = std.enums.fromInt(Role, bytes[4]) orelse return error.InvalidIntegrityTopologyFence,
            .namespace = .{
                .table_id = std.mem.readInt(u64, bytes[32..40], .little),
                .shard_id = std.mem.readInt(u64, bytes[40..48], .little),
                .range_id = std.mem.readInt(u64, bytes[48..56], .little),
            },
            .catalog_digest = bytes[56..88].*,
        };
        _ = try result.encode();
        return result;
    }
};

pub const Command = struct {
    fence: Fence,
    action: enum { begin, release, cancel, abort_transition, transfer, prune },
    transfer: ?@import("relational_integrity_handoff.zig").Command = null,
    pub fn jsonStringify(self: Command, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn current(txn: anytype) !?Fence {
    const bytes = (try optional(txn, fence_key)) orelse return null;
    const fence = try Fence.decode(bytes);
    // Range publication can atomically close admission by writing the exact
    // completion receipt in its existing metadata batch. The stale physical
    // fence is harmless and is overwritten by a later higher-epoch begin.
    if (try optional(txn, receipt_key)) |receipt| if ((try Fence.decode(receipt)).eql(fence)) return null;
    return fence;
}

pub fn active(txn: anytype) !bool {
    return try current(txn) != null;
}

pub fn completed(txn: anytype) !?Fence {
    return if (try optional(txn, receipt_key)) |bytes| try Fence.decode(bytes) else null;
}

/// Owner-local durable clock. A coordinator persists this value in its plan
/// before delivering a begin; concurrent plans at the same epoch cannot both
/// be admitted. Completed/cancelled epochs are never reusable.
pub fn nextEpoch(txn: anytype) !u64 {
    var epoch: u64 = 0;
    if (try current(txn)) |fence| epoch = fence.admission_epoch;
    if (try optional(txn, receipt_key)) |bytes| epoch = @max(epoch, (try Fence.decode(bytes)).admission_epoch);
    return std.math.add(u64, epoch, 1) catch error.IntegrityTopologyEpochExhausted;
}

fn stageReceipt(txn: anytype, fence: Fence) !void {
    if (try optional(txn, receipt_key)) |bytes| {
        const previous = try Fence.decode(bytes);
        if (previous.admission_epoch > fence.admission_epoch) return;
        if (previous.admission_epoch == fence.admission_epoch) {
            if (!previous.eql(fence)) return error.IntegrityTopologyChanged;
            return;
        }
    }
    const bytes = try fence.encode();
    try txn.put(receipt_key, &bytes);
}

/// The caller stages this in the source/destination Raft control transaction.
/// A PREPARE is allowed to fence an owner that still has old participants:
/// rejecting the Raft entry would prevent later resolution entries from ever
/// draining those participants. Snapshot/cutover independently require drained.
pub fn stageBegin(txn: anytype, fence: Fence) !void {
    _ = try fence.encode();
    const abort_key = abortedKey(fence);
    if (try optional(txn, &abort_key)) |attempt| {
        if (attempt.len != 8) return error.InvalidIntegrityTopologyFence;
        if (std.mem.readInt(u64, attempt[0..8], .little) >= fence.attempt) return error.IntegrityTopologyCompleted;
    }
    if (try @import("relational_integrity_retirement.zig").active(txn)) return error.ConstraintRetirementInProgress;
    if (try current(txn)) |existing| {
        if (!existing.eql(fence)) return error.IntegrityTopologyBusy;
        return;
    }
    if (try optional(txn, receipt_key)) |bytes| {
        const previous = try Fence.decode(bytes);
        if (previous.admission_epoch >= fence.admission_epoch)
            return error.IntegrityTopologyCompleted;
    }
    const raw_catalog = (try optional(txn, catalog.key)) orelse if (fence.role == .backup_snapshot) "" else return error.IntegrityCatalogChanged;
    var digest: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(raw_catalog, &digest, .{});
    if (!std.mem.eql(u8, &digest, &fence.catalog_digest)) return error.IntegrityCatalogChanged;
    const bytes = try fence.encode();
    try txn.put(fence_key, &bytes);
}

fn abortedKey(fence: Fence) [abort_prefix.len + 9]u8 {
    var out: [abort_prefix.len + 9]u8 = undefined;
    @memcpy(out[0..abort_prefix.len], abort_prefix);
    out[abort_prefix.len] = @intFromEnum(fence.role);
    std.mem.writeInt(u64, out[abort_prefix.len + 1 ..][0..8], fence.transition_id, .little);
    return out;
}

/// A split rollback knows its durable transition/attempt even when a begin's
/// reply (or the begin itself) was lost. Keep an exact transition tombstone,
/// independent of its unknown admission epoch, so delayed begins stay dead.
/// These small structural receipts require an authoritative metadata history
/// horizon before collection; elapsed wall time is not a correctness proof.
pub fn stageAbortTransition(txn: anytype, expected: Fence) !void {
    _ = try expected.encode();
    if (expected.role != .split_source and expected.role != .split_destination and expected.role != .merge_source and expected.role != .merge_destination) return error.InvalidIntegrityTopologyFence;
    const key = abortedKey(expected);
    const previous = if (try optional(txn, &key)) |bytes| blk: {
        if (bytes.len != 8) return error.InvalidIntegrityTopologyFence;
        break :blk std.mem.readInt(u64, bytes[0..8], .little);
    } else 0;
    if (try current(txn)) |fence| {
        if (fence.role == expected.role and fence.transition_id == expected.transition_id and fence.attempt <= expected.attempt) {
            try stageReceipt(txn, fence);
            try txn.delete(fence_key);
        }
    }
    var attempt: [8]u8 = undefined;
    std.mem.writeInt(u64, &attempt, @max(previous, expected.attempt), .little);
    try txn.put(&key, &attempt);
}

/// Call under the DB apply fence immediately before acquiring participant
/// locks. Repeated preparation of an already bound transaction may finish;
/// activation, repair and retirement are new participants and remain fenced.
pub fn admitPrepare(txn: anytype, manager: *transactions.TxnManager, alloc: Allocator, txn_id: transactions.TxnId) !void {
    if (try current(txn) == null) return;
    if (try manager.loadSchemaBinding(alloc, txn_id) == null) return error.IntegrityTopologyBusy;
}

pub fn requireUnfenced(txn: anytype) !void {
    if (try current(txn) != null) return error.IntegrityTopologyBusy;
}

/// Runtime reconciliation of an already-published identical producer catalog
/// is harmless while frozen; delayed *changes* may not create new callbacks.
pub fn requireUnfencedOrUnchanged(txn: anytype, key: []const u8, candidate: []const u8) !void {
    if (try current(txn) == null) return;
    const existing = (try optional(txn, key)) orelse return error.IntegrityTopologyBusy;
    if (!std.mem.eql(u8, existing, candidate)) return error.IntegrityTopologyBusy;
}

/// A frozen source snapshot is meaningful only once decisions, intents,
/// participant acknowledgements and durable recovery outboxes have drained.
pub fn requireDrained(txn: anytype, manager: *transactions.TxnManager, expected: Fence) !void {
    const actual = (try current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!actual.eql(expected)) return error.IntegrityTopologyChanged;
    if (try manager.hasTopologySensitiveTransactions()) return error.TransactionTopologyBusy;
}

/// Final release must share the ownership/cutover transaction. Persist a
/// checksummed receipt so delayed control commands cannot resurrect a fence.
pub fn stageRelease(txn: anytype, expected: Fence) !void {
    if (try current(txn)) |actual| {
        if (!actual.eql(expected)) return error.IntegrityTopologyChanged;
        try stageReceipt(txn, expected);
        try txn.delete(fence_key);
    } else {
        const receipt = (try optional(txn, receipt_key)) orelse return error.IntegrityTopologyFenceMissing;
        const previous = try Fence.decode(receipt);
        if (previous.admission_epoch < expected.admission_epoch or
            (previous.admission_epoch == expected.admission_epoch and !previous.eql(expected))) return error.IntegrityTopologyChanged;
    }
}

/// Cancelling an ambiguously delivered begin consumes its epoch even if the
/// begin has not arrived yet. Never releases another live lifecycle owner.
pub fn stageCancel(txn: anytype, expected: Fence) !void {
    _ = try expected.encode();
    if (try current(txn)) |actual| {
        if (actual.admission_epoch == expected.admission_epoch and !actual.eql(expected)) return error.IntegrityTopologyChanged;
        if (actual.eql(expected)) try txn.delete(fence_key);
    }
    try stageReceipt(txn, expected);
}

test "relational integrity topology fence codec detects corrupt ownership identity" {
    const fence: Fence = .{
        .transition_id = 11,
        .attempt = 2,
        .peer_group_id = 12,
        .owner_group_id = 11,
        .role = .split_source,
        .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .catalog_digest = @splat(4),
    };
    var bytes = try fence.encode();
    try std.testing.expect((try Fence.decode(&bytes)).eql(fence));
    bytes[40] ^= 1;
    try std.testing.expectError(error.InvalidIntegrityTopologyFence, Fence.decode(&bytes));
}
