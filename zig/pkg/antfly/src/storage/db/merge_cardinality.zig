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

//! Receiver counts separate live base ownership from stale donor rows retained
//! outside that ownership. The bounded record and row effects commit together;
//! cancellation restores the continuously maintained base count in O(1).
const std = @import("std");
const Allocator = std.mem.Allocator;
const Store = @import("../docstore.zig").DocStore;
const KV = @import("../docstore.zig").KVPair;
const types = @import("types.zig");
const merge = @import("merge_state.zig");
const counts = @import("range_cardinality.zig");
const identity = @import("doc_identity.zig");
const keys = @import("../internal_keys.zig");
pub const key = "\x00\x00__metadata__:raftmerge:cardinality";

fn append(alloc: Allocator, out: *std.ArrayListUnmanaged(KV), name: []const u8, value: []const u8) !void {
    const owned_key = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try out.append(alloc, .{ .key = owned_key, .value = owned_value });
}

fn changed(current: u64, delta: i128) !u64 {
    const result = @as(i128, current) + delta;
    if (result < 0) return error.InvalidRangeDocumentCount;
    return std.math.cast(u64, result) orelse error.RangeDocumentCountOverflow;
}

/// Returns true when this transition supplied the authoritative range count.
/// Work is O(touched keys), with no scans, row decoding, or payload reads.
pub fn prepare(alloc: Allocator, store: *Store, req: types.BatchRequest, checkpoint_applied: bool, upserts: []const []const u8, deletes: []const []const u8, before: u64, after: u64, out: *std.ArrayListUnmanaged(KV)) !bool {
    const admitted_checkpoint = if (checkpoint_applied) req.merge_checkpoint else null;
    var txn = try store.beginProbeTxn();
    defer txn.abort();
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (raw == null and admitted_checkpoint == null) return false;
    if (raw) |value| {
        if (value.len != 17 or value[16] > 2) return error.InvalidRangeDocumentCount;
        if (value[16] == 2 and admitted_checkpoint == null) return false;
    }
    const state_raw = txn.get(merge.key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var state = if (state_raw) |value| try merge.decodeAlloc(alloc, value) else null;
    defer if (state) |*value| value.deinit(alloc);
    if (admitted_checkpoint) |checkpoint| if (checkpoint.kind == .accept and
        (state == null or state.?.transition_id != checkpoint.transition_id))
    {
        const count = (try counts.loadOrProveEmpty(alloc, store)) orelse return error.InvalidRangeDocumentCount;
        var record: [17]u8 = @splat(0);
        std.mem.writeInt(u64, record[0..8], checkpoint.transition_id, .little);
        std.mem.writeInt(u64, record[8..16], count, .little);
        try append(alloc, out, key, &record);
        try append(alloc, out, &keys.range_document_count_key, record[8..16]);
        return false;
    };
    const current = state orelse return false;
    if (current.phase != .accepting and current.phase != .rolling_back) return false;
    const value = raw orelse return false; // Pre-existing non-page merge path.
    if (value.len != 17 or value[16] > 1 or std.mem.readInt(u64, value[0..8], .little) != current.transition_id) return error.InvalidRangeDocumentCount;
    var record: [17]u8 = value[0..17].*;
    const old_base = std.mem.readInt(u64, record[8..16], .little);
    if (admitted_checkpoint) |checkpoint| {
        // A shared copy retry may bind a new immutable attempt. Its old donor
        // rows become uncounted staging data again; reset to the continuously
        // maintained live base count before bounded cleanup restarts.
        if (checkpoint.kind == .begin_copy and checkpoint.page_source != null and current.copy_attempt.sequence != 0 and
            !std.meta.eql(current.copy_attempt, checkpoint.copy_attempt))
        {
            record[16] = 0;
            try append(alloc, out, key, &record);
            try append(alloc, out, &keys.range_document_count_key, record[8..16]);
            return true;
        }
        if (checkpoint.kind == .rollback) {
            record[16] = 2;
            try append(alloc, out, key, &record);
            var count: [8]u8 = undefined;
            std.mem.writeInt(u64, &count, old_base, .little);
            try append(alloc, out, &keys.range_document_count_key, &count);
            return true;
        }
        if (checkpoint.kind == .finalize) {
            record[16] = 2;
            try append(alloc, out, key, &record);
        }
    }
    if (upserts.len == 0 and deletes.len == 0) {
        if (req.merge_page) |page| if (page.phase == .cleanup and page.exhausted and record[16] == 0) {
            record[16] = 1;
            try append(alloc, out, key, &record);
        };
        return false;
    }
    var final = std.StringHashMapUnmanaged(bool).empty;
    defer final.deinit(alloc);
    var donor_upsert = false;
    for (upserts) |doc| {
        if (current.receiver_base_range.contains(doc)) try final.put(alloc, doc, true) else donor_upsert = true;
    }
    for (deletes) |doc| if (current.receiver_base_range.contains(doc)) {
        try final.put(alloc, doc, false);
    };
    var base_delta: i128 = 0;
    var iter = final.iterator();
    while (iter.next()) |entry| {
        const ordinal = try identity.lookupOrdinalTxn(alloc, &txn, entry.key_ptr.*);
        const prior = if (ordinal) |id| try identity.lookupStateTxn(&txn, id) else null;
        const was_live = if (prior) |item| item.isLive() else false;
        base_delta += @as(i128, @intFromBool(entry.value_ptr.*)) - @as(i128, @intFromBool(was_live));
    }
    const base = try changed(old_base, base_delta);
    const old_total = (try counts.loadFromTxn(&txn)) orelse return error.InvalidRangeDocumentCount;
    // Until cleanup ends, stale donor identities were never in the range
    // count. The first copied upsert also starts the legacy non-page path.
    const total = try changed(old_total, if (record[16] == 1 or donor_upsert) @as(i128, after) - @as(i128, before) else base_delta);
    std.mem.writeInt(u64, record[8..16], base, .little);
    if (donor_upsert) record[16] = 1;
    if (req.merge_page) |page| if (page.phase == .cleanup and page.exhausted) {
        record[16] = 1;
    };
    if (!std.mem.eql(u8, value, &record)) try append(alloc, out, key, &record);
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, total, .little);
    try append(alloc, out, &keys.range_document_count_key, &count);
    return true;
}
