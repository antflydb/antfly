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

//! Replay-journal format detection and the in-place upgrade from earlier
//! layouts. The base revision's layout has an initialized journal with no
//! format marker, keeps `next_sequence` under global replay kind 0x02
//! (journal sharing moved it), and has no credit ledger. v2 kept per-hint
//! latest-sequence markers only for active consumers. Lane entries are kept
//! as they are; only the metadata is rewritten.
//!
//! A store whose readers use per-hint lanes (`keep_base_layout`, every
//! backend but Lite) stays readable by the base revision: its appends keep
//! writing every hint lane and `next_sequence` under the base key too, so
//! the upgrade keeps that key. A base binary that wrote since leaves the base
//! key ahead; `baseWroteSince` sends the next open through the upgrade
//! again. Lite files carry a native header the base revision refuses.

const std = @import("std");
const internal_keys = @import("internal_keys.zig");

/// `next_sequence`'s key in the base layout.
pub const legacy_next_sequence_key = [3]u8{ internal_keys.replay_namespace, 0xff, 0x02 };

pub const State = enum {
    /// No journal yet: the first append initializes and stamps it.
    absent,
    current,
    protected_credit,
    /// Initialized by the base revision or as v2: `upgrade` rewrites it.
    legacy,

    pub fn isCurrent(value: State) bool {
        return value == .current or value == .protected_credit;
    }
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
    if (version == internal_keys.replay_journal_format_v2) return .legacy;
    if (version == internal_keys.replay_journal_format_protected_credit) return .protected_credit;
    if (version != internal_keys.replay_journal_format_current) return error.UnsupportedReplayJournalFormat;
    return .current;
}

/// Whether a base binary appended after the upgrade: it advances only the
/// base `next_sequence` key, leaving it ahead of the current one.
pub fn baseWroteSince(txn: anytype) !bool {
    const base = try readSequence(txn, legacy_next_sequence_key[0..]) orelse return false;
    const current = try readSequence(txn, internal_keys.replay_meta_next_sequence_key[0..]) orelse return true;
    return base > current;
}

fn readSequence(txn: anytype, key: []const u8) !?u64 {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 8) return error.CorruptReplayMetadata;
    return std.mem.readInt(u64, raw[0..8], .little);
}

/// The stored `next_sequence`, as its 8 bytes: the later of the current
/// key and the base key, read in the caller's one snapshot. A journal still
/// in the base layout (a read-only open never upgrades it) has only the base
/// key; a base binary rolled back onto an upgraded store advances only the
/// base key, and a read-only reopen cannot reconcile that, so both count.
pub fn getNextSequence(txn: anytype) ![8]u8 {
    const current = try readSequence(txn, internal_keys.replay_meta_next_sequence_key[0..]);
    const base = try readSequence(txn, legacy_next_sequence_key[0..]);
    const next = if (current) |c| @max(c, base orelse 0) else base orelse return error.NotFound;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, next, .little);
    return raw;
}

/// What the credit ledger charges for one lane entry.
pub fn entryCharge(key_len: usize, value_len: usize) u64 {
    return @as(u64, key_len) + value_len;
}

/// Rewrites an older journal's metadata in the caller's write transaction:
/// `next_sequence` under its current key (and, with `keep_base_layout`,
/// under the base key too), `ledger` (the caller's scan of every lane entry,
/// charged as `entryCharge`), each of `hint_ordinals`' latest-sequence
/// markers raised to the journal head (an older layout may have skipped a
/// hint's appends, so only the head bounds them), and the current format.
pub fn upgrade(txn: anytype, ledger: internal_keys.JournalCreditLedger, hint_ordinals: []const u8, keep_base_layout: bool) !void {
    const base_next = try readSequence(txn, legacy_next_sequence_key[0..]);
    const current_next = try readSequence(txn, internal_keys.replay_meta_next_sequence_key[0..]);
    const next = @max(base_next orelse 0, current_next orelse 0);
    if (base_next != null or current_next != null) {
        var next_raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_raw, next, .little);
        try txn.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
        if (keep_base_layout) {
            try txn.put(legacy_next_sequence_key[0..], next_raw[0..]);
        } else if (base_next != null) {
            try txn.delete(legacy_next_sequence_key[0..]);
        }
    }
    if (next > 0) {
        var head_raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &head_raw, next - 1, .little);
        for (hint_ordinals) |ordinal| {
            const key = internal_keys.replayLatestSequenceKey(ordinal);
            if ((try readSequence(txn, key[0..]) orelse 0) < next - 1) try txn.put(key[0..], head_raw[0..]);
        }
        // Records before `next` may lack hint-lane entries.
        if ((try readSequence(txn, internal_keys.replay_hint_index_floor_key[0..]) orelse 0) < next) {
            var floor_raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &floor_raw, next, .little);
            try txn.put(internal_keys.replay_hint_index_floor_key[0..], floor_raw[0..]);
        }
    }
    const ledger_raw = ledger.encode();
    try txn.put(internal_keys.journal_credit_ledger_key[0..], ledger_raw[0..]);
    const version_raw = internal_keys.encodeReplayJournalFormatVersion(internal_keys.replay_journal_format_current);
    try txn.put(internal_keys.replay_journal_format_version_key[0..], version_raw[0..]);
}
