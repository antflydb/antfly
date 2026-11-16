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
        .key = internal_keys.ordered_document_applied_entry_key[0..],
        .value = value_buf[0..],
    };
}

pub fn readOrderedApplyReceipt(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
) !?OrderedApplyReceipt {
    const raw = store.get(alloc, internal_keys.ordered_document_applied_entry_key[0..]) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != ordered_apply_receipt_value_len) return error.CorruptOrderedApplyReceipt;
    const identity: OrderedApplyReceipt = .{
        .term = std.mem.readInt(u64, raw[0..8], .little),
        .index = std.mem.readInt(u64, raw[8..16], .little),
    };
    if (identity.term == 0 or identity.index == 0) return error.CorruptOrderedApplyReceipt;
    return identity;
}

pub const OrderedApplyDisposition = enum { apply, already_applied };

pub fn orderedApplyDisposition(
    persisted: ?OrderedApplyReceipt,
    incoming: OrderedApplyReceipt,
) !OrderedApplyDisposition {
    if (incoming.term == 0 or incoming.index == 0) return error.InvalidOrderedApplyReceipt;
    const current = persisted orelse return .apply;
    if (current.index > incoming.index) return .already_applied;
    if (current.index < incoming.index) return .apply;
    if (current.term != incoming.term) return error.ConflictingOrderedApplyReceipt;
    return .already_applied;
}

/// Outcome receipts for exported writes (autocommit SQL and batch exports).
///
/// Each writer (one per public handle, rotated after an unknown outcome)
/// overwrites one marker with the sequence of its latest call, atomically with
/// that call's primary write. A call that ends with an unknown outcome hands the
/// caller `(writer, sequence)`, and the writer is retired so its marker never
/// moves again. Once the store has reconciled the uncertain commit, the marker
/// answers exactly: equal to the sequence means applied, lower or absent means
/// not applied.
pub const OutcomeMarker = struct {
    writer: [outcome_writer_len]u8,
    sequence: u64,
};

pub const outcome_writer_len = 16;
pub const outcome_marker_key_len = internal_keys.outcome_receipt_prefix.len + outcome_writer_len;
pub const outcome_marker_value_len = @sizeOf(u64);

pub fn outcomeMarkerKey(writer: [outcome_writer_len]u8) [outcome_marker_key_len]u8 {
    var key: [outcome_marker_key_len]u8 = undefined;
    @memcpy(key[0..internal_keys.outcome_receipt_prefix.len], &internal_keys.outcome_receipt_prefix);
    @memcpy(key[internal_keys.outcome_receipt_prefix.len..], &writer);
    return key;
}

pub fn outcomeMarkerWrite(
    marker: OutcomeMarker,
    key_buf: *[outcome_marker_key_len]u8,
    value_buf: *[outcome_marker_value_len]u8,
) docstore_mod.KVPair {
    key_buf.* = outcomeMarkerKey(marker.writer);
    std.mem.writeInt(u64, value_buf, marker.sequence, .little);
    return .{ .key = key_buf[0..], .value = value_buf[0..] };
}

/// The writer's last applied sequence, or null if none of its calls applied.
pub fn readOutcomeMarker(alloc: Allocator, store: *docstore_mod.DocStore, writer: [outcome_writer_len]u8) !?u64 {
    const key = outcomeMarkerKey(writer);
    const raw = store.get(alloc, &key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != outcome_marker_value_len) return error.CorruptOutcomeMarker;
    return std.mem.readInt(u64, raw[0..outcome_marker_value_len], .little);
}

pub const OutcomeDisposition = enum { applied, not_applied };

pub fn outcomeDisposition(persisted: ?u64, sequence: u64) !OutcomeDisposition {
    if (sequence == 0) return error.InvalidOutcomeReceipt;
    const last = persisted orelse return .not_applied;
    if (last == sequence) return .applied;
    if (last < sequence) return .not_applied;
    // A retired writer never writes again, so a later sequence means the
    // receipt does not belong to this marker.
    return error.InvalidOutcomeReceipt;
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
    if (raw.len != replication_applied_lsn_value_len) return error.CorruptReplicationAppliedSequence;
    return std.mem.readInt(u64, raw[0..replication_applied_lsn_value_len], .little);
}

test "storage.hot_standby apply receipts preserve independent Raft and HA encodings" {
    var ordered_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    var replication_buf: [replication_applied_lsn_value_len]u8 = undefined;
    const ordered = orderedApplyReceiptWrite(.{ .term = 0x0102030405060708, .index = 9 }, &ordered_buf);
    const replication = replicationAppliedSequenceWrite(11, &replication_buf);
    // These released keys are independent of the implementation names. A
    // rename must not strand persisted progress or replay a committed write.
    try std.testing.expectEqualSlices(u8, &.{ 0x02, 0xff, 0x05 }, ordered.key);
    try std.testing.expectEqualSlices(u8, &.{ 0x02, 0xff, 0x04 }, replication.key);
    try std.testing.expectEqualSlices(u8, &.{ 8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0 }, ordered.value);
    try std.testing.expectEqualSlices(u8, &.{ 11, 0, 0, 0, 0, 0, 0, 0 }, replication.value);
    try std.testing.expect(!std.mem.eql(u8, ordered.key, replication.key));
}

test "outcome receipt markers use a released key family and answer exactly" {
    var key_buf: [outcome_marker_key_len]u8 = undefined;
    var value_buf: [outcome_marker_value_len]u8 = undefined;
    const writer: [outcome_writer_len]u8 = @splat(0xab);
    const write = outcomeMarkerWrite(.{ .writer = writer, .sequence = 7 }, &key_buf, &value_buf);
    // The key family is persisted: a rename must not strand receipts.
    try std.testing.expectEqualSlices(u8, &.{ 0x02, 0xff, 0x55 }, write.key[0..3]);
    try std.testing.expectEqualSlices(u8, &writer, write.key[3..]);
    try std.testing.expectEqualSlices(u8, &.{ 7, 0, 0, 0, 0, 0, 0, 0 }, write.value);
    try std.testing.expectEqual(OutcomeDisposition.applied, try outcomeDisposition(7, 7));
    try std.testing.expectEqual(OutcomeDisposition.not_applied, try outcomeDisposition(6, 7));
    try std.testing.expectEqual(OutcomeDisposition.not_applied, try outcomeDisposition(null, 7));
    try std.testing.expectError(error.InvalidOutcomeReceipt, outcomeDisposition(8, 7));
    try std.testing.expectError(error.InvalidOutcomeReceipt, outcomeDisposition(null, 0));
}

test "storage.hot_standby apply receipts reject conflicting identities without advancing progress" {
    const current: OrderedApplyReceipt = .{ .term = 3, .index = 11 };
    try std.testing.expectEqual(OrderedApplyDisposition.apply, try orderedApplyDisposition(null, current));
    try std.testing.expectEqual(OrderedApplyDisposition.already_applied, try orderedApplyDisposition(current, current));
    try std.testing.expectEqual(OrderedApplyDisposition.already_applied, try orderedApplyDisposition(current, .{ .term = 2, .index = 10 }));
    try std.testing.expectEqual(OrderedApplyDisposition.apply, try orderedApplyDisposition(current, .{ .term = 4, .index = 12 }));
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, orderedApplyDisposition(current, .{ .term = 4, .index = 11 }));
    try std.testing.expectError(error.InvalidOrderedApplyReceipt, orderedApplyDisposition(current, .{ .term = 0, .index = 12 }));
    try std.testing.expectError(error.InvalidOrderedApplyReceipt, orderedApplyDisposition(null, .{ .term = 3, .index = 0 }));
}
