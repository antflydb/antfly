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

//! Replay-journal format detection and the in-place upgrade from the base
//! revision's layout. That layout has an initialized journal with no format
//! marker, keeps `next_sequence` under global replay kind 0x02 (journal
//! sharing moved it), and has no credit ledger. Its lane entries are kept as
//! they are; only the metadata is rewritten.

const std = @import("std");
const internal_keys = @import("internal_keys.zig");

/// `next_sequence`'s key in the base layout.
pub const legacy_next_sequence_key = [3]u8{ internal_keys.replay_namespace, 0xff, 0x02 };

pub const State = enum {
    /// No journal yet: the first append initializes and stamps it.
    absent,
    current,
    /// Initialized by the base revision: `upgrade` rewrites it.
    legacy,
};

pub fn state(txn: anytype) !State {
    _ = txn.get(internal_keys.replay_meta_init_key[0..]) catch |err| switch (err) {
        error.NotFound => return .absent,
        else => return err,
    };
    const raw = txn.get(internal_keys.replay_journal_format_version_key[0..]) catch |err| switch (err) {
        error.NotFound => return .legacy,
        else => return err,
    };
    const version = internal_keys.decodeReplayJournalFormatVersion(raw) orelse return error.UnsupportedReplayJournalFormat;
    if (version != internal_keys.replay_journal_format_v2) return error.UnsupportedReplayJournalFormat;
    return .current;
}

/// The stored `next_sequence` bytes. A journal still in the base layout (a
/// read-only open never upgrades it) keeps them under the legacy key; read
/// there only while the format marker is absent, so a populated database
/// never reports an empty journal.
pub fn getNextSequence(txn: anytype) ![]const u8 {
    return txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch |err| switch (err) {
        error.NotFound => {
            _ = txn.get(internal_keys.replay_journal_format_version_key[0..]) catch |marker_err| switch (marker_err) {
                error.NotFound => return txn.get(legacy_next_sequence_key[0..]),
                else => return marker_err,
            };
            return error.NotFound;
        },
        else => return err,
    };
}

/// What the credit ledger charges for one lane entry.
pub fn entryCharge(key_len: usize, value_len: usize) u64 {
    return @as(u64, key_len) + value_len;
}

/// Rewrites a legacy journal's metadata in the caller's write transaction:
/// moves `next_sequence` to its current key, stores `ledger` (the caller's
/// scan of every lane entry, charged as `entryCharge`), and stamps the
/// current format.
pub fn upgrade(txn: anytype, ledger: internal_keys.JournalCreditLedger) !void {
    const legacy_next: ?[8]u8 = blk: {
        const raw = txn.get(legacy_next_sequence_key[0..]) catch |err| switch (err) {
            error.NotFound => break :blk null,
            else => return err,
        };
        if (raw.len != 8) return error.CorruptReplayMetadata;
        break :blk raw[0..8].*;
    };
    if (legacy_next) |raw| {
        const current_next: ?u64 = blk: {
            const current = txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch |err| switch (err) {
                error.NotFound => break :blk null,
                else => return err,
            };
            if (current.len != 8) return error.CorruptReplayMetadata;
            break :blk std.mem.readInt(u64, current[0..8], .little);
        };
        const next = @max(std.mem.readInt(u64, &raw, .little), current_next orelse 0);
        var next_raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_raw, next, .little);
        try txn.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
        try txn.delete(legacy_next_sequence_key[0..]);
    }
    const ledger_raw = ledger.encode();
    try txn.put(internal_keys.journal_credit_ledger_key[0..], ledger_raw[0..]);
    const version_raw = internal_keys.encodeReplayJournalFormatVersion(internal_keys.replay_journal_format_v2);
    try txn.put(internal_keys.replay_journal_format_version_key[0..], version_raw[0..]);
}
