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

//! Durable replay receipts shared by local storage and replication adapters.
//! The caller owns apply admission and commits these KV writes atomically with
//! mutations. Raft entry identity and HA LSN remain separate progress domains.
const std = @import("std");
const Allocator = std.mem.Allocator;
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
pub const OrderedApplyReceipt = @import("types.zig").OrderedApplyReceipt;

pub const ordered_apply_receipt_value_len = 2 * @sizeOf(u64);

pub fn orderedApplyReceiptWrite(
    identity: OrderedApplyReceipt,
    value_buf: *[ordered_apply_receipt_value_len]u8,
) docstore_mod.KVPair {
    std.mem.writeInt(u64, value_buf[0..8], identity.term, .little);
    std.mem.writeInt(u64, value_buf[8..16], identity.index, .little);
    return .{
        .key = internal_keys.raft_document_applied_entry_key[0..],
        .value = value_buf[0..],
    };
}

pub fn readOrderedApplyReceipt(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
) !?OrderedApplyReceipt {
    const raw = store.get(alloc, internal_keys.raft_document_applied_entry_key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != ordered_apply_receipt_value_len) return error.CorruptRaftAppliedEntry;
    const identity: OrderedApplyReceipt = .{
        .term = std.mem.readInt(u64, raw[0..8], .little),
        .index = std.mem.readInt(u64, raw[8..16], .little),
    };
    if (identity.term == 0 or identity.index == 0) return error.CorruptRaftAppliedEntry;
    return identity;
}

pub const OrderedApplyDisposition = enum { apply, already_applied };

pub fn orderedApplyDisposition(
    persisted: ?OrderedApplyReceipt,
    incoming: OrderedApplyReceipt,
) !OrderedApplyDisposition {
    if (incoming.term == 0 or incoming.index == 0) return error.InvalidRaftAppliedEntry;
    const current = persisted orelse return .apply;
    if (current.index > incoming.index) return .already_applied;
    if (current.index < incoming.index) return .apply;
    if (current.term != incoming.term) return error.ConflictingRaftAppliedEntry;
    return .already_applied;
}

pub const replication_applied_lsn_value_len: usize = @sizeOf(u64);

pub fn replicationAppliedSequenceWrite(lsn: u64, value_buf: *[replication_applied_lsn_value_len]u8) docstore_mod.KVPair {
    std.mem.writeInt(u64, value_buf, lsn, .little);
    return .{
        .key = internal_keys.replication_applied_lsn_key[0..],
        .value = value_buf[0..],
    };
}

pub fn readReplicationAppliedSequence(alloc: Allocator, store: *docstore_mod.DocStore) !u64 {
    const raw = store.get(alloc, internal_keys.replication_applied_lsn_key[0..]) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != replication_applied_lsn_value_len) return error.CorruptHAAppliedReplicationLsn;
    return std.mem.readInt(u64, raw[0..replication_applied_lsn_value_len], .little);
}

test "storage.hot_standby apply receipts preserve independent Raft and HA encodings" {
    var raft_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    var replication_buf: [replication_applied_lsn_value_len]u8 = undefined;
    const raft = orderedApplyReceiptWrite(.{ .term = 0x0102030405060708, .index = 9 }, &raft_buf);
    const ha = replicationAppliedSequenceWrite(11, &replication_buf);
    try std.testing.expectEqualSlices(u8, &.{ 8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0 }, raft.value);
    try std.testing.expectEqualSlices(u8, &.{ 11, 0, 0, 0, 0, 0, 0, 0 }, ha.value);
    try std.testing.expect(!std.mem.eql(u8, raft.key, ha.key));
}

test "storage.hot_standby apply receipts reject conflicting identities without advancing progress" {
    const current: OrderedApplyReceipt = .{ .term = 3, .index = 11 };
    try std.testing.expectEqual(OrderedApplyDisposition.apply, try orderedApplyDisposition(null, current));
    try std.testing.expectEqual(OrderedApplyDisposition.already_applied, try orderedApplyDisposition(current, current));
    try std.testing.expectEqual(OrderedApplyDisposition.already_applied, try orderedApplyDisposition(current, .{ .term = 2, .index = 10 }));
    try std.testing.expectEqual(OrderedApplyDisposition.apply, try orderedApplyDisposition(current, .{ .term = 4, .index = 12 }));
    try std.testing.expectError(error.ConflictingRaftAppliedEntry, orderedApplyDisposition(current, .{ .term = 4, .index = 11 }));
    try std.testing.expectError(error.InvalidRaftAppliedEntry, orderedApplyDisposition(current, .{ .term = 0, .index = 12 }));
    try std.testing.expectError(error.InvalidRaftAppliedEntry, orderedApplyDisposition(null, .{ .term = 3, .index = 0 }));
}

// Server source compatibility; storage keys, bytes and errors stay unchanged.
pub const RaftAppliedEntryIdentity = OrderedApplyReceipt;
pub const raft_applied_entry_value_len = ordered_apply_receipt_value_len;
pub const raftAppliedEntryWrite = orderedApplyReceiptWrite;
pub const readRaftAppliedEntry = readOrderedApplyReceipt;
pub const RaftAppliedEntryDisposition = OrderedApplyDisposition;
pub const raftAppliedEntryDisposition = orderedApplyDisposition;
