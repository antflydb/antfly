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

pub const fence_key = @import("relational_integrity_topology_contract.zig").fence_key;
pub const receipt_key = @import("relational_integrity_topology_contract.zig").receipt_key;
pub const abort_prefix = @import("relational_integrity_topology_contract.zig").abort_prefix;
pub const Role = @import("relational_integrity_topology_contract.zig").Role;

pub const Fence = @import("relational_integrity_topology_contract.zig").Fence;

pub const Command = @import("relational_integrity_topology_contract.zig").Command;

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
    try @import("relational_integrity_generation_retirement.zig").requireClear(txn);
    if (fence.role == .split_source or fence.role == .split_destination or
        fence.role == .merge_source or fence.role == .merge_destination or
        fence.role == .rewrite_source)
        try @import("relational_integrity_generation_retirement.zig").requireNoActive(txn);
    if (try optional(txn, receipt_key)) |bytes| {
        const previous = try Fence.decode(bytes);
        if (previous.admission_epoch >= fence.admission_epoch)
            return error.IntegrityTopologyCompleted;
    }
    const raw_catalog = try catalogForFence(txn, fence);
    var digest: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(raw_catalog, &digest, .{});
    if (!std.mem.eql(u8, &digest, &fence.catalog_digest)) return error.IntegrityCatalogChanged;
    const bytes = try fence.encode();
    try txn.put(fence_key, &bytes);
}

/// Ordinary document owners need the same source fence for online transfer,
/// but legitimately have no relational integrity catalog. The durable storage
/// mode, not catalog absence alone, authorizes this case. A relational owner
/// with missing integrity metadata remains corruption, never an empty catalog.
pub fn catalogForFence(txn: anytype, fence: Fence) ![]const u8 {
    if (try optional(txn, catalog.key)) |bytes| return bytes;
    if (fence.role == .backup_snapshot) return "";
    if (fence.role == .merge_source or fence.role == .rewrite_source) {
        const table = @import("table_catalog.zig");
        const raw = try optional(txn, table.key) orelse return error.IntegrityCatalogChanged;
        const facts = try table.Catalog.decode(raw);
        if (facts.mode_initialized and facts.storage_mode == .document) return "";
    }
    return error.IntegrityCatalogChanged;
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
    if (expected.role != .split_source and expected.role != .split_destination and expected.role != .merge_source and expected.role != .merge_destination and expected.role != .rewrite_source and expected.role != .truncate_parent) return error.InvalidIntegrityTopologyFence;
    const key = abortedKey(expected);
    const previous = if (try optional(txn, &key)) |bytes| blk: {
        if (bytes.len != 8) return error.InvalidIntegrityTopologyFence;
        break :blk std.mem.readInt(u64, bytes[0..8], .little);
    } else 0;
    if (try current(txn)) |fence| {
        if (fence.role == expected.role and fence.transition_id == expected.transition_id and fence.attempt <= expected.attempt) {
            if (fence.role == .truncate_parent) try @import("relational_integrity_generation_retirement.zig").stageCancel(txn, fence) else try @import("relational_integrity_generation_retirement.zig").requireClear(txn);
            try stageReceipt(txn, fence);
            try txn.delete(fence_key);
        }
    } else try @import("relational_integrity_generation_retirement.zig").requireClear(txn);
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
    if (try current(txn) == null and try @import("online_integrity_shadow.zig").rawRange(txn) == null) return;
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
    // Publication must activate the exact pending parent generations before
    // lifting the write fence. Until that path exists, release fails closed.
    try @import("relational_integrity_generation_retirement.zig").requireClear(txn);
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
        if (actual.eql(expected)) {
            if (expected.role == .truncate_parent) try @import("relational_integrity_generation_retirement.zig").stageCancel(txn, expected) else try @import("relational_integrity_generation_retirement.zig").requireClear(txn);
            try txn.delete(fence_key);
        }
    } else {
        try @import("relational_integrity_generation_retirement.zig").requireClear(txn);
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

test "relational integrity topology pending inverse generation is owner bound and corruptions fail closed" {
    const retirement = @import("relational_integrity_generation_retirement.zig");
    const alloc = std.testing.allocator;
    const fence: Fence = .{ .role = .truncate_parent, .transition_id = 11, .attempt = 1, .peer_group_id = 21, .owner_group_id = 31, .namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 }, .catalog_digest = @splat(2) };
    const entries = [_]retirement.Entry{ .{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk1", .generation = @splat(3) }, .{ .child_table_id = 52, .child_table_name = "other_children", .constraint_name = "fk2", .generation = @splat(4) } };
    const bytes = try retirement.encodePending(alloc, fence, @splat(5), &entries);
    defer alloc.free(bytes);
    const pending = try retirement.Pending.decode(bytes);
    try std.testing.expect(pending.fence.eql(fence));
    try std.testing.expect(pending.contains(51, @splat(3)));
    try std.testing.expect(!pending.contains(51, @splat(4)));
    const reference: @import("relational_integrity_contract.zig").Reference = .{ .child_table = "children", .child_key = "row", .constraint_name = "fk1", .constraint_generation = @splat(3) };
    try std.testing.expect(pending.matchesReference(reference));
    var wrong_reference = reference;
    wrong_reference.child_table = "same_generation_wrong_child";
    try std.testing.expect(!pending.matchesReference(wrong_reference));
    wrong_reference = reference;
    wrong_reference.constraint_name = "same_generation_wrong_fk";
    try std.testing.expect(!pending.matchesReference(wrong_reference));
    try std.testing.expectError(error.InvalidGenerationRetirement, retirement.encodePending(alloc, fence, @splat(5), &.{ entries[0], entries[0] }));
    bytes[185] ^= 1;
    try std.testing.expectError(error.InvalidGenerationRetirement, retirement.Pending.decode(bytes));
}

test "relational integrity topology cancels exact pending parent generation before fence release" {
    const retirement = @import("relational_integrity_generation_retirement.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const Mock = struct {
        alloc: Allocator,
        fence: ?[]const u8 = null,
        pending: ?[]const u8 = null,
        receipt: ?[]const u8 = null,

        pub fn get(self: *@This(), physical_key: []const u8) ![]const u8 {
            const value = if (std.mem.eql(u8, physical_key, fence_key)) self.fence else if (std.mem.eql(u8, physical_key, retirement.key)) self.pending else if (std.mem.eql(u8, physical_key, receipt_key)) self.receipt else null;
            return value orelse error.NotFound;
        }
        pub fn put(self: *@This(), physical_key: []const u8, value: []const u8) !void {
            const copied = try self.alloc.dupe(u8, value);
            if (std.mem.eql(u8, physical_key, fence_key)) self.fence = copied else if (std.mem.eql(u8, physical_key, retirement.key)) self.pending = copied else if (std.mem.eql(u8, physical_key, receipt_key)) self.receipt = copied else return error.InvalidTestKey;
        }
        pub fn delete(self: *@This(), physical_key: []const u8) !void {
            if (std.mem.eql(u8, physical_key, fence_key)) self.fence = null else if (std.mem.eql(u8, physical_key, retirement.key)) self.pending = null else return error.InvalidTestKey;
        }
    };
    const fence: Fence = .{ .role = .truncate_parent, .transition_id = 11, .attempt = 1, .peer_group_id = 21, .owner_group_id = 31, .namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 }, .catalog_digest = @splat(2) };
    const encoded_fence = try fence.encode();
    var txn: Mock = .{ .alloc = alloc, .fence = &encoded_fence, .pending = try retirement.encodePending(alloc, fence, @splat(5), &.{.{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(3) }}) };
    try std.testing.expectError(error.GenerationRetirementPending, stageRelease(&txn, fence));
    var other = fence;
    other.transition_id += 1;
    try std.testing.expectError(error.IntegrityTopologyChanged, retirement.stageCancel(&txn, other));
    try stageCancel(&txn, fence);
    try std.testing.expect(txn.fence == null and txn.pending == null and txn.receipt != null);
    try std.testing.expect((try completed(&txn)).?.eql(fence));
    try stageCancel(&txn, fence);
    try std.testing.expectEqual(@as(u64, 2), try nextEpoch(&txn));
}
