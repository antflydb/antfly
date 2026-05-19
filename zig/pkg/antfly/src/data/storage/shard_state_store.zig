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

const std = @import("std");
const docstore = @import("../../storage/docstore.zig");
const internal_keys = @import("../../storage/internal_keys.zig");
const range_state = @import("../../storage/db/range_state.zig");
const shard_mod = @import("../../storage/shard.zig");

pub const AppliedDataKV = struct {
    key: []const u8,
    value: []const u8,
};

pub const AppliedDataRange = docstore.ByteRange;
pub const SplitPhase = shard_mod.SplitPhase;
pub const SplitDelta = shard_mod.SplitDelta;

pub const AppliedSplitState = struct {
    phase: SplitPhase,
    split_key: []const u8,
    new_shard_id: u64,
    original_range_end: []const u8,
};

pub const SplitHandoff = struct {
    byte_range: AppliedDataRange,
    split_state: AppliedSplitState,
    base_delta_sequence: u64,
    entries: []AppliedDataKV,
};

pub const DataOperation = union(enum) {
    put: struct {
        key: []u8,
        value: []u8,
    },
    delete: []u8,
    set_range: struct {
        start: []u8,
        end: []u8,
    },
    prepare_split: []u8,
    start_split: struct {
        new_shard_id: u64,
        split_key: []u8,
    },
    finalize_split,
    rollback_split,
};

pub fn currentRange(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !AppliedDataRange {
    const key = try groupRangeKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    return try range_state.loadRangeAtKey(alloc, store, key);
}

pub fn groupState(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
    const logical_prefix = try groupDocumentPrefixAlloc(alloc, group_id);
    defer alloc.free(logical_prefix);
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, logical_prefix);
    defer alloc.free(lower);
    const upper = (try internal_keys.documentRangeUpperAlloc(alloc, logical_prefix)) orelse return &.{};
    defer alloc.free(upper);

    const kvs = try store.scanRange(alloc, lower, upper);
    errdefer {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }

    const state = try alloc.alloc(AppliedDataKV, kvs.len);
    errdefer {
        for (state[0..kvs.len]) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        alloc.free(state);
    }
    for (kvs, 0..) |kv, i| {
        const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, kv.key)) orelse return error.InvalidAppliedDataDocumentKey;
        defer alloc.free(logical_key);
        state[i] = .{
            .key = try stripGroupDocumentPrefixAlloc(alloc, logical_key, group_id),
            .value = kv.value,
        };
        alloc.free(kv.key);
    }
    alloc.free(kvs);
    return state;
}

pub fn currentSplitState(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !?AppliedSplitState {
    const key = try groupSplitStateKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    const decoded = try decodeSplitStateAlloc(alloc, raw);
    if (decoded.phase == .none) {
        freeSplitState(alloc, decoded);
        return null;
    }
    return decoded;
}

pub fn freeSplitState(alloc: std.mem.Allocator, state: AppliedSplitState) void {
    if (state.split_key.len > 0) alloc.free(@constCast(state.split_key));
    if (state.original_range_end.len > 0) alloc.free(@constCast(state.original_range_end));
}

pub fn freeGroupStateEntries(alloc: std.mem.Allocator, entries: []AppliedDataKV) void {
    for (entries) |entry| {
        alloc.free(@constCast(entry.key));
        alloc.free(@constCast(entry.value));
    }
    alloc.free(entries);
}

pub fn freeHandoff(alloc: std.mem.Allocator, handoff: SplitHandoff) void {
    range_state.freeRange(alloc, handoff.byte_range);
    freeSplitState(alloc, handoff.split_state);
    freeGroupStateEntries(alloc, handoff.entries);
}

pub fn currentSplitDeltaSequence(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !u64 {
    const key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != 8) return error.InvalidSplitDeltaSequence;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn captureSplitHandoff(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !SplitHandoff {
    const split_state = (try currentSplitState(store, alloc, group_id)) orelse return error.SplitInProgress;
    errdefer freeSplitState(alloc, split_state);

    try shard_mod.validateFinalizeSplit(.{
        .phase = split_state.phase,
        .split_key = split_state.split_key,
        .new_shard_id = split_state.new_shard_id,
        .started_at = 0,
        .original_range_end = split_state.original_range_end,
    });

    const byte_range: AppliedDataRange = .{
        .start = try alloc.dupe(u8, split_state.split_key),
        .end = try alloc.dupe(u8, split_state.original_range_end),
    };
    errdefer range_state.freeRange(alloc, byte_range);

    const entries = try groupStateInRange(store, alloc, group_id, byte_range);
    errdefer freeGroupStateEntries(alloc, entries);

    return .{
        .byte_range = byte_range,
        .split_state = split_state,
        .base_delta_sequence = try currentSplitDeltaSequence(store, alloc, group_id),
        .entries = entries,
    };
}

pub fn listDeltasAfter(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64, after_seq: u64) ![]SplitDelta {
    const prefix = try groupSplitDeltaPrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    const all = try store.scanPrefix(alloc, prefix);
    defer {
        for (all) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(all);
    }

    var results = std.ArrayListUnmanaged(SplitDelta).empty;
    errdefer {
        for (results.items) |*delta| shard_mod.freeDelta(alloc, delta);
        results.deinit(alloc);
    }

    for (all) |kv| {
        const seq = parseSplitDeltaSeq(group_id, kv.key) orelse continue;
        if (seq <= after_seq) continue;
        try results.append(alloc, try shard_mod.decodeSplitDeltaAlloc(alloc, seq, kv.value));
    }
    return try results.toOwnedSlice(alloc);
}

pub fn applyHandoff(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64, handoff: SplitHandoff) !void {
    var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
    defer {
        for (writes.items) |write| alloc.free(@constCast(write.key));
        writes.deinit(alloc);
    }

    const range_key = try groupRangeKeyAlloc(alloc, group_id);
    var range_buf: [1024]u8 = undefined;
    const encoded_range = try range_state.encodeRange(handoff.byte_range, &range_buf);
    try writes.append(alloc, .{ .key = range_key, .value = encoded_range });

    for (handoff.entries) |entry| {
        const key = try groupDocumentStoreKeyAlloc(alloc, group_id, entry.key);
        try writes.append(alloc, .{ .key = key, .value = entry.value });
    }

    const deletes: []const []const u8 = &.{};
    try store.putBatch(writes.items, deletes);
}

pub fn applyDeltas(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64, deltas: []const SplitDelta) !void {
    const target_range = try currentRange(store, alloc, group_id);
    defer range_state.freeRange(alloc, target_range);

    for (deltas) |delta| {
        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer {
            for (writes.items) |write| alloc.free(@constCast(write.key));
            writes.deinit(alloc);
        }
        for (delta.writes) |write| {
            const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, write.key)) orelse continue;
            defer alloc.free(logical_key);
            const doc_key = stripAnyGroupDocumentPrefixAlloc(alloc, logical_key) catch continue;
            defer alloc.free(doc_key);
            if (!target_range.contains(doc_key)) continue;
            const remapped_key = try groupDocumentStoreKeyAlloc(alloc, group_id, doc_key);
            try writes.append(alloc, .{ .key = remapped_key, .value = write.value });
        }

        var del_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (del_keys.items) |key| alloc.free(@constCast(key));
            del_keys.deinit(alloc);
        }
        for (delta.deletes) |key| {
            const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, key)) orelse continue;
            defer alloc.free(logical_key);
            const doc_key = stripAnyGroupDocumentPrefixAlloc(alloc, logical_key) catch continue;
            defer alloc.free(doc_key);
            if (!target_range.contains(doc_key)) continue;
            try del_keys.append(alloc, try groupDocumentStoreKeyAlloc(alloc, group_id, doc_key));
        }

        try store.putBatch(writes.items, del_keys.items);
    }
}

pub fn buildSnapshot(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    const byte_range = try currentRange(store, alloc, group_id);
    defer range_state.freeRange(alloc, byte_range);
    const split_state = try currentSplitState(store, alloc, group_id);
    defer if (split_state) |state| freeSplitState(alloc, state);
    const entries = try groupState(store, alloc, group_id);
    defer {
        for (entries) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        alloc.free(entries);
    }
    return try encodeGroupStateSnapshot(alloc, byte_range, split_state, entries);
}

fn groupStateInRange(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    byte_range: AppliedDataRange,
) ![]AppliedDataKV {
    const lower = try groupDocumentLowerBoundAlloc(alloc, group_id, byte_range.start);
    defer alloc.free(lower);
    const upper = try groupDocumentUpperBoundAlloc(alloc, group_id, byte_range.end);
    defer if (upper) |bound| alloc.free(bound);

    const kvs = try store.scanRange(alloc, lower, if (upper) |bound| bound else "");
    errdefer {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }

    const state = try alloc.alloc(AppliedDataKV, kvs.len);
    errdefer {
        for (state[0..kvs.len]) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        alloc.free(state);
    }
    for (kvs, 0..) |kv, i| {
        const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, kv.key)) orelse return error.InvalidAppliedDataDocumentKey;
        defer alloc.free(logical_key);
        state[i] = .{
            .key = try stripGroupDocumentPrefixAlloc(alloc, logical_key, group_id),
            .value = kv.value,
        };
        alloc.free(kv.key);
    }
    alloc.free(kvs);
    return state;
}

pub fn appendOperationEffects(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    operations: []const DataOperation,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    var byte_range = try currentRange(store, alloc, group_id);
    defer range_state.freeRange(alloc, byte_range);
    var split_state = try currentSplitState(store, alloc, group_id);
    defer if (split_state) |state| freeSplitState(alloc, state);
    var delta_writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer {
        for (delta_writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        delta_writes.deinit(alloc);
    }
    var delta_deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (delta_deletes.items) |key| alloc.free(key);
        delta_deletes.deinit(alloc);
    }

    for (operations) |op| switch (op) {
        .put => |put| {
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validateSplitAwareOwnership(byte_range, shard_split_state, put.key);
            const state_key = try groupDocumentStoreKeyAlloc(alloc, group_id, put.key);
            errdefer alloc.free(state_key);
            removeOwnedWriteByKey(alloc, writes, state_key);
            removeDeleteByKey(alloc, deletes, state_key);
            const state_value = try alloc.dupe(u8, put.value);
            errdefer alloc.free(state_value);
            try writes.append(alloc, .{ .key = state_key, .value = state_value });
            if (split_state != null and split_state.?.phase == .splitting) {
                removeOwnedWriteByKey(alloc, &delta_writes, state_key);
                removeDeleteByKey(alloc, &delta_deletes, state_key);
                try delta_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, state_key),
                    .value = try alloc.dupe(u8, put.value),
                });
            }
        },
        .delete => |key_to_delete| {
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validateSplitAwareOwnership(byte_range, shard_split_state, key_to_delete);
            const state_key = try groupDocumentStoreKeyAlloc(alloc, group_id, key_to_delete);
            errdefer alloc.free(state_key);
            removeOwnedWriteByKey(alloc, writes, state_key);
            removeDeleteByKey(alloc, deletes, state_key);
            try deletes.append(alloc, state_key);
            if (split_state != null and split_state.?.phase == .splitting) {
                removeOwnedWriteByKey(alloc, &delta_writes, state_key);
                removeDeleteByKey(alloc, &delta_deletes, state_key);
                try delta_deletes.append(alloc, try alloc.dupe(u8, state_key));
            }
        },
        .set_range => |range| {
            range_state.freeRange(alloc, byte_range);
            byte_range = .{
                .start = try alloc.dupe(u8, range.start),
                .end = try alloc.dupe(u8, range.end),
            };
            const range_key = try groupRangeKeyAlloc(alloc, group_id);
            errdefer alloc.free(range_key);
            removeOwnedWriteByKey(alloc, writes, range_key);
            removeDeleteByKey(alloc, deletes, range_key);
            var range_buf: [1024]u8 = undefined;
            const encoded_range = try range_state.encodeRange(byte_range, &range_buf);
            const range_value = try alloc.dupe(u8, encoded_range);
            errdefer alloc.free(range_value);
            try writes.append(alloc, .{ .key = range_key, .value = range_value });
        },
        .prepare_split => |split_key| {
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validatePrepareSplit(byte_range, shard_split_state, split_key);

            if (split_state) |state| {
                freeSplitState(alloc, state);
                split_state = null;
            }

            const owned_split_key = try alloc.dupe(u8, split_key);
            errdefer alloc.free(owned_split_key);
            const owned_original_end = try alloc.dupe(u8, byte_range.end);
            errdefer alloc.free(owned_original_end);
            split_state = .{
                .phase = .prepare,
                .split_key = owned_split_key,
                .new_shard_id = 0,
                .original_range_end = owned_original_end,
            };
            const split_state_key = try groupSplitStateKeyAlloc(alloc, group_id);
            errdefer alloc.free(split_state_key);
            removeOwnedWriteByKey(alloc, writes, split_state_key);
            removeDeleteByKey(alloc, deletes, split_state_key);
            var split_buf: [1024]u8 = undefined;
            const encoded_split_state = try encodeSplitState(split_state.?, &split_buf);
            const split_state_value = try alloc.dupe(u8, encoded_split_state);
            errdefer alloc.free(split_state_value);
            try writes.append(alloc, .{ .key = split_state_key, .value = split_state_value });
        },
        .start_split => |start| {
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validateStartSplit(shard_split_state, start.split_key);

            split_state.?.phase = .splitting;
            split_state.?.new_shard_id = start.new_shard_id;

            const original_start = try alloc.dupe(u8, byte_range.start);
            errdefer alloc.free(original_start);
            range_state.freeRange(alloc, byte_range);
            byte_range = .{
                .start = original_start,
                .end = try alloc.dupe(u8, start.split_key),
            };

            const range_key = try groupRangeKeyAlloc(alloc, group_id);
            errdefer alloc.free(range_key);
            removeOwnedWriteByKey(alloc, writes, range_key);
            removeDeleteByKey(alloc, deletes, range_key);
            var range_buf: [1024]u8 = undefined;
            const encoded_range = try range_state.encodeRange(byte_range, &range_buf);
            const range_value = try alloc.dupe(u8, encoded_range);
            errdefer alloc.free(range_value);
            try writes.append(alloc, .{ .key = range_key, .value = range_value });

            const split_state_key = try groupSplitStateKeyAlloc(alloc, group_id);
            errdefer alloc.free(split_state_key);
            removeOwnedWriteByKey(alloc, writes, split_state_key);
            removeDeleteByKey(alloc, deletes, split_state_key);
            var split_buf: [1024]u8 = undefined;
            const encoded_split_state = try encodeSplitState(split_state.?, &split_buf);
            const split_state_value = try alloc.dupe(u8, encoded_split_state);
            errdefer alloc.free(split_state_value);
            try writes.append(alloc, .{ .key = split_state_key, .value = split_state_value });

            const split_delta_seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
            errdefer alloc.free(split_delta_seq_key);
            removeOwnedWriteByKey(alloc, writes, split_delta_seq_key);
            removeDeleteByKey(alloc, deletes, split_delta_seq_key);
            var zero_seq: [8]u8 = undefined;
            std.mem.writeInt(u64, &zero_seq, 0, .little);
            const zero_seq_value = try alloc.dupe(u8, &zero_seq);
            errdefer alloc.free(zero_seq_value);
            try writes.append(alloc, .{ .key = split_delta_seq_key, .value = zero_seq_value });
        },
        .finalize_split => {
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validateFinalizeSplit(shard_split_state);
            try appendFinalizeSplitDeletes(store, alloc, group_id, split_state.?, deletes);
            try appendSplitDeltaClears(store, alloc, group_id, deletes);
            const split_state_key = try groupSplitStateKeyAlloc(alloc, group_id);
            defer alloc.free(split_state_key);
            removeOwnedWriteByKey(alloc, writes, split_state_key);
            removeDeleteByKey(alloc, deletes, split_state_key);
            try deletes.append(alloc, try alloc.dupe(u8, split_state_key));
            removeOwnedWritesWithPrefix(alloc, writes, "\x00\x00__metadata__:data_group_split_delta:");
            removeDeletesWithPrefix(alloc, deletes, "\x00\x00__metadata__:data_group_split_delta:");
            const split_delta_seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
            defer alloc.free(split_delta_seq_key);
            removeOwnedWriteByKey(alloc, writes, split_delta_seq_key);
            removeDeleteByKey(alloc, deletes, split_delta_seq_key);
            freeSplitState(alloc, split_state.?);
            split_state = null;
        },
        .rollback_split => {
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validateRollbackSplit(shard_split_state);
            const original_start = try alloc.dupe(u8, byte_range.start);
            errdefer alloc.free(original_start);
            range_state.freeRange(alloc, byte_range);
            byte_range = .{
                .start = original_start,
                .end = try alloc.dupe(u8, split_state.?.original_range_end),
            };
            const range_key = try groupRangeKeyAlloc(alloc, group_id);
            errdefer alloc.free(range_key);
            removeOwnedWriteByKey(alloc, writes, range_key);
            removeDeleteByKey(alloc, deletes, range_key);
            var range_buf: [1024]u8 = undefined;
            const encoded_range = try range_state.encodeRange(byte_range, &range_buf);
            const range_value = try alloc.dupe(u8, encoded_range);
            errdefer alloc.free(range_value);
            try writes.append(alloc, .{ .key = range_key, .value = range_value });

            try appendSplitDeltaClears(store, alloc, group_id, deletes);
            const split_state_key = try groupSplitStateKeyAlloc(alloc, group_id);
            defer alloc.free(split_state_key);
            removeOwnedWriteByKey(alloc, writes, split_state_key);
            removeDeleteByKey(alloc, deletes, split_state_key);
            try deletes.append(alloc, try alloc.dupe(u8, split_state_key));
            removeOwnedWritesWithPrefix(alloc, writes, "\x00\x00__metadata__:data_group_split_delta:");
            removeDeletesWithPrefix(alloc, deletes, "\x00\x00__metadata__:data_group_split_delta:");
            const split_delta_seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
            defer alloc.free(split_delta_seq_key);
            removeOwnedWriteByKey(alloc, writes, split_delta_seq_key);
            removeDeleteByKey(alloc, deletes, split_delta_seq_key);
            freeSplitState(alloc, split_state.?);
            split_state = null;
        },
    };

    if (split_state != null and split_state.?.phase == .splitting and (delta_writes.items.len > 0 or delta_deletes.items.len > 0)) {
        const next_seq = (try currentSplitDeltaSequence(store, alloc, group_id)) + 1;
        const delta_writes_view = try alloc.alloc(docstore.KVPair, delta_writes.items.len);
        defer alloc.free(delta_writes_view);
        for (delta_writes.items, 0..) |write, i| {
            delta_writes_view[i] = .{ .key = write.key, .value = write.value };
        }
        const delta_deletes_view = try alloc.alloc([]const u8, delta_deletes.items.len);
        defer alloc.free(delta_deletes_view);
        for (delta_deletes.items, 0..) |key, i| delta_deletes_view[i] = key;

        const encoded_delta = try shard_mod.encodeSplitDeltaAlloc(alloc, 0, delta_writes_view, delta_deletes_view);
        defer alloc.free(encoded_delta);
        const delta_key = try groupSplitDeltaKeyAlloc(alloc, group_id, next_seq);
        errdefer alloc.free(delta_key);
        const delta_value = try alloc.dupe(u8, encoded_delta);
        errdefer alloc.free(delta_value);
        try writes.append(alloc, .{ .key = delta_key, .value = delta_value });

        const delta_seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
        errdefer alloc.free(delta_seq_key);
        var seq_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_buf, next_seq, .little);
        const seq_value = try alloc.dupe(u8, &seq_buf);
        errdefer alloc.free(seq_value);
        try writes.append(alloc, .{ .key = delta_seq_key, .value = seq_value });
    }
}

pub fn putOwnedBatch(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    writes: []const docstore.OwnedKVPair,
    deletes: []const []const u8,
) !void {
    const borrowed = try alloc.alloc(docstore.KVPair, writes.len);
    defer alloc.free(borrowed);
    for (writes, 0..) |write, i| {
        borrowed[i] = .{
            .key = write.key,
            .value = write.value,
        };
    }
    try store.putBatch(borrowed, deletes);
}

pub fn freeOwnedWrites(
    alloc: std.mem.Allocator,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
) void {
    for (writes.items) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    }
    writes.deinit(alloc);
}

fn groupDocumentPrefixAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "g:{d}:", .{group_id});
}

fn removeOwnedWriteByKey(
    alloc: std.mem.Allocator,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    key: []const u8,
) void {
    var i: usize = 0;
    while (i < writes.items.len) {
        if (std.mem.eql(u8, writes.items[i].key, key)) {
            const removed = writes.swapRemove(i);
            alloc.free(removed.key);
            alloc.free(removed.value);
            continue;
        }
        i += 1;
    }
}

fn removeDeleteByKey(
    alloc: std.mem.Allocator,
    deletes: *std.ArrayListUnmanaged([]u8),
    key: []const u8,
) void {
    var i: usize = 0;
    while (i < deletes.items.len) {
        if (std.mem.eql(u8, deletes.items[i], key)) {
            const removed = deletes.swapRemove(i);
            alloc.free(removed);
            continue;
        }
        i += 1;
    }
}

fn removeOwnedWritesWithPrefix(
    alloc: std.mem.Allocator,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    prefix: []const u8,
) void {
    var i: usize = 0;
    while (i < writes.items.len) {
        if (std.mem.startsWith(u8, writes.items[i].key, prefix)) {
            const removed = writes.swapRemove(i);
            alloc.free(removed.key);
            alloc.free(removed.value);
            continue;
        }
        i += 1;
    }
}

fn removeDeletesWithPrefix(
    alloc: std.mem.Allocator,
    deletes: *std.ArrayListUnmanaged([]u8),
    prefix: []const u8,
) void {
    var i: usize = 0;
    while (i < deletes.items.len) {
        if (std.mem.startsWith(u8, deletes.items[i], prefix)) {
            const removed = deletes.swapRemove(i);
            alloc.free(removed);
            continue;
        }
        i += 1;
    }
}

fn groupDocumentLogicalKeyAlloc(alloc: std.mem.Allocator, group_id: u64, key: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "g:{d}:{s}", .{ group_id, key });
}

fn groupDocumentStoreKeyAlloc(alloc: std.mem.Allocator, group_id: u64, key: []const u8) ![]u8 {
    const logical_key = try groupDocumentLogicalKeyAlloc(alloc, group_id, key);
    defer alloc.free(logical_key);
    return try internal_keys.documentKeyAlloc(alloc, logical_key);
}

fn groupRangeKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_range:{d}", .{group_id});
}

fn groupSplitStateKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_split_state:{d}", .{group_id});
}

fn groupSplitDeltaSeqKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_split_delta_seq:{d}", .{group_id});
}

fn groupSplitDeltaPrefixAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_split_delta:{d}:", .{group_id});
}

fn groupSplitDeltaKeyAlloc(alloc: std.mem.Allocator, group_id: u64, seq: u64) ![]u8 {
    const prefix = try groupSplitDeltaPrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    const key = try alloc.alloc(u8, prefix.len + 8);
    @memcpy(key[0..prefix.len], prefix);
    std.mem.writeInt(u64, key[prefix.len..][0..8], seq, .big);
    return key;
}

fn parseSplitDeltaSeq(group_id: u64, key: []const u8) ?u64 {
    var buf: [128]u8 = undefined;
    const prefix = std.fmt.bufPrint(&buf, "\x00\x00__metadata__:data_group_split_delta:{d}:", .{group_id}) catch return null;
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    if (key.len != prefix.len + 8) return null;
    return std.mem.readInt(u64, key[prefix.len..][0..8], .big);
}

fn groupDocumentLowerBoundAlloc(alloc: std.mem.Allocator, group_id: u64, key: []const u8) ![]u8 {
    const logical = try groupDocumentLogicalKeyAlloc(alloc, group_id, key);
    defer alloc.free(logical);
    return try internal_keys.documentRangeLowerAlloc(alloc, logical);
}

fn groupDocumentUpperBoundAlloc(alloc: std.mem.Allocator, group_id: u64, key: []const u8) !?[]u8 {
    if (key.len == 0) return null;
    const logical = try groupDocumentLogicalKeyAlloc(alloc, group_id, key);
    defer alloc.free(logical);
    return try internal_keys.documentRangeUpperAlloc(alloc, logical);
}

fn stripGroupDocumentPrefixAlloc(alloc: std.mem.Allocator, logical_key: []const u8, group_id: u64) ![]u8 {
    const prefix = try groupDocumentPrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    if (!std.mem.startsWith(u8, logical_key, prefix)) return error.InvalidAppliedDataDocumentKey;
    return try alloc.dupe(u8, logical_key[prefix.len..]);
}

fn stripAnyGroupDocumentPrefixAlloc(alloc: std.mem.Allocator, logical_key: []const u8) ![]u8 {
    if (!std.mem.startsWith(u8, logical_key, "g:")) return error.InvalidAppliedDataDocumentKey;
    const sep = std.mem.indexOfScalarPos(u8, logical_key, 2, ':') orelse return error.InvalidAppliedDataDocumentKey;
    return try alloc.dupe(u8, logical_key[sep + 1 ..]);
}

fn encodeGroupStateSnapshot(alloc: std.mem.Allocator, byte_range: AppliedDataRange, split_state: ?AppliedSplitState, entries: []const AppliedDataKV) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var start_len_bytes: [4]u8 = undefined;
    var end_len_bytes: [4]u8 = undefined;
    var split_present: [1]u8 = .{if (split_state == null) 0 else 1};
    var count_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &start_len_bytes, @intCast(byte_range.start.len), .little);
    std.mem.writeInt(u32, &end_len_bytes, @intCast(byte_range.end.len), .little);
    std.mem.writeInt(u32, &count_bytes, @intCast(entries.len), .little);
    try out.appendSlice(alloc, &start_len_bytes);
    try out.appendSlice(alloc, byte_range.start);
    try out.appendSlice(alloc, &end_len_bytes);
    try out.appendSlice(alloc, byte_range.end);
    try out.appendSlice(alloc, &split_present);
    if (split_state) |state| {
        var split_buf: [1024]u8 = undefined;
        const encoded_split = try encodeSplitState(state, &split_buf);
        var split_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &split_len_bytes, @intCast(encoded_split.len), .little);
        try out.appendSlice(alloc, &split_len_bytes);
        try out.appendSlice(alloc, encoded_split);
    }
    try out.appendSlice(alloc, &count_bytes);
    for (entries) |entry| {
        var key_len_bytes: [4]u8 = undefined;
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &key_len_bytes, @intCast(entry.key.len), .little);
        std.mem.writeInt(u32, &len_bytes, @intCast(entry.value.len), .little);
        try out.appendSlice(alloc, &key_len_bytes);
        try out.appendSlice(alloc, entry.key);
        try out.appendSlice(alloc, &len_bytes);
        try out.appendSlice(alloc, entry.value);
    }
    return try out.toOwnedSlice(alloc);
}

fn encodeSplitState(state: AppliedSplitState, buf: []u8) ![]const u8 {
    const total_len = 1 + 8 + 4 + state.split_key.len + 4 + state.original_range_end.len;
    if (total_len > buf.len) return error.SplitStateTooLarge;
    var pos: usize = 0;
    buf[pos] = @intFromEnum(state.phase);
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], state.new_shard_id, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(state.split_key.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..state.split_key.len], state.split_key);
    pos += state.split_key.len;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(state.original_range_end.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..state.original_range_end.len], state.original_range_end);
    pos += state.original_range_end.len;
    return buf[0..pos];
}

fn decodeSplitStateAlloc(alloc: std.mem.Allocator, encoded: []const u8) !AppliedSplitState {
    if (encoded.len < 1 + 8 + 4 + 4) return error.InvalidSplitState;
    var pos: usize = 0;
    const phase: SplitPhase = @enumFromInt(encoded[pos]);
    pos += 1;
    const new_shard_id = std.mem.readInt(u64, encoded[pos..][0..8], .little);
    pos += 8;
    const split_key_len = std.mem.readInt(u32, encoded[pos..][0..4], .little);
    pos += 4;
    if (pos + split_key_len + 4 > encoded.len) return error.InvalidSplitState;
    const split_key = try alloc.dupe(u8, encoded[pos .. pos + split_key_len]);
    pos += split_key_len;
    errdefer alloc.free(split_key);
    const original_range_end_len = std.mem.readInt(u32, encoded[pos..][0..4], .little);
    pos += 4;
    if (pos + original_range_end_len != encoded.len) return error.InvalidSplitState;
    const original_range_end = try alloc.dupe(u8, encoded[pos .. pos + original_range_end_len]);
    return .{
        .phase = phase,
        .split_key = split_key,
        .new_shard_id = new_shard_id,
        .original_range_end = original_range_end,
    };
}

fn appendFinalizeSplitDeletes(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    split_state: AppliedSplitState,
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    const lower = try groupDocumentLowerBoundAlloc(alloc, group_id, split_state.split_key);
    defer alloc.free(lower);
    const upper = try groupDocumentUpperBoundAlloc(alloc, group_id, split_state.original_range_end);
    defer if (upper) |bound| alloc.free(bound);

    const to_delete = try store.scanRange(alloc, lower, if (upper) |bound| bound else "");
    defer {
        for (to_delete) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(to_delete);
    }

    for (to_delete) |kv| {
        try deletes.append(alloc, try alloc.dupe(u8, kv.key));
    }
}

fn appendSplitDeltaClears(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    const prefix = try groupSplitDeltaPrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    const deltas = try store.scanPrefix(alloc, prefix);
    defer {
        for (deltas) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(deltas);
    }
    for (deltas) |kv| {
        try deletes.append(alloc, try alloc.dupe(u8, kv.key));
    }

    const seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
    defer alloc.free(seq_key);
    try deletes.append(alloc, try alloc.dupe(u8, seq_key));
}

test "shard state store persists ranges and document state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore.DocStore.open(std.testing.allocator, path_z.ptr, .{});
    defer store.close();

    var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer freeOwnedWrites(std.testing.allocator, &writes);
    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| std.testing.allocator.free(key);
        deletes.deinit(std.testing.allocator);
    }

    const ops = [_]DataOperation{
        .{ .set_range = .{ .start = @constCast("doc:a"), .end = @constCast("doc:z") } },
        .{ .put = .{ .key = @constCast("doc:c"), .value = @constCast("value-c") } },
        .{ .put = .{ .key = @constCast("doc:m"), .value = @constCast("value-m") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 17, &ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);

    const byte_range = try currentRange(&store, std.testing.allocator, 17);
    defer range_state.freeRange(std.testing.allocator, byte_range);
    try std.testing.expectEqualStrings("doc:a", byte_range.start);
    try std.testing.expectEqualStrings("doc:z", byte_range.end);

    const state = try groupState(&store, std.testing.allocator, 17);
    defer {
        for (state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(state);
    }
    try std.testing.expectEqual(@as(usize, 2), state.len);
    try std.testing.expectEqualStrings("doc:c", state[0].key);
    try std.testing.expectEqualStrings("value-c", state[0].value);
    try std.testing.expectEqualStrings("doc:m", state[1].key);
    try std.testing.expectEqualStrings("value-m", state[1].value);

    const snapshot = try buildSnapshot(&store, std.testing.allocator, 17);
    defer std.testing.allocator.free(snapshot);
    try std.testing.expect(snapshot.len > 0);
}

test "shard state store persists split lifecycle and ownership" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-split", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore.DocStore.open(std.testing.allocator, path_z.ptr, .{});
    defer store.close();

    var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer freeOwnedWrites(std.testing.allocator, &writes);
    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| std.testing.allocator.free(key);
        deletes.deinit(std.testing.allocator);
    }

    const set_range_ops = [_]DataOperation{
        .{ .set_range = .{ .start = @constCast("doc:a"), .end = @constCast("doc:z") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &set_range_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const prepare_ops = [_]DataOperation{
        .{ .prepare_split = @constCast("doc:m") },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &prepare_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const prepared = (try currentSplitState(&store, std.testing.allocator, 23)).?;
    defer freeSplitState(std.testing.allocator, prepared);
    try std.testing.expectEqual(SplitPhase.prepare, prepared.phase);
    try std.testing.expectEqualStrings("doc:m", prepared.split_key);

    const start_ops = [_]DataOperation{
        .{ .start_split = .{ .new_shard_id = 42, .split_key = @constCast("doc:m") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &start_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const narrowed_range = try currentRange(&store, std.testing.allocator, 23);
    defer range_state.freeRange(std.testing.allocator, narrowed_range);
    try std.testing.expectEqualStrings("doc:a", narrowed_range.start);
    try std.testing.expectEqualStrings("doc:m", narrowed_range.end);

    const out_of_range_put = [_]DataOperation{
        .{ .put = .{ .key = @constCast("doc:zz"), .value = @constCast("blocked") } },
    };
    try std.testing.expectError(error.KeyOutOfRange, appendOperationEffects(&store, std.testing.allocator, 23, &out_of_range_put, &writes, &deletes));

    const rollback_ops = [_]DataOperation{
        .rollback_split,
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &rollback_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const restored_range = try currentRange(&store, std.testing.allocator, 23);
    defer range_state.freeRange(std.testing.allocator, restored_range);
    try std.testing.expectEqualStrings("doc:a", restored_range.start);
    try std.testing.expectEqualStrings("doc:z", restored_range.end);
    try std.testing.expect((try currentSplitState(&store, std.testing.allocator, 23)) == null);
}

test "shard state store finalize split reclaims right-hand document range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-finalize", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore.DocStore.open(std.testing.allocator, path_z.ptr, .{});
    defer store.close();

    var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer freeOwnedWrites(std.testing.allocator, &writes);
    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| std.testing.allocator.free(key);
        deletes.deinit(std.testing.allocator);
    }

    const initial_ops = [_]DataOperation{
        .{ .set_range = .{ .start = @constCast("doc:a"), .end = @constCast("doc:z") } },
        .{ .put = .{ .key = @constCast("doc:c"), .value = @constCast("left") } },
        .{ .put = .{ .key = @constCast("doc:t"), .value = @constCast("right") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 31, &initial_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const split_ops = [_]DataOperation{
        .{ .prepare_split = @constCast("doc:m") },
        .{ .start_split = .{ .new_shard_id = 77, .split_key = @constCast("doc:m") } },
        .finalize_split,
    };
    try appendOperationEffects(&store, std.testing.allocator, 31, &split_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);

    const state = try groupState(&store, std.testing.allocator, 31);
    defer {
        for (state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(state);
    }
    try std.testing.expectEqual(@as(usize, 1), state.len);
    try std.testing.expectEqualStrings("doc:c", state[0].key);
    try std.testing.expectEqualStrings("left", state[0].value);
    try std.testing.expect((try currentSplitState(&store, std.testing.allocator, 31)) == null);
}

test "shard state store records and replays split deltas" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-deltas-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_path);
    const src_path_z = try std.testing.allocator.dupeZ(u8, src_path);
    defer std.testing.allocator.free(src_path_z);
    var src = try docstore.DocStore.open(std.testing.allocator, src_path_z.ptr, .{});
    defer src.close();

    const dst_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-deltas-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_path);
    const dst_path_z = try std.testing.allocator.dupeZ(u8, dst_path);
    defer std.testing.allocator.free(dst_path_z);
    var dst = try docstore.DocStore.open(std.testing.allocator, dst_path_z.ptr, .{});
    defer dst.close();

    var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer freeOwnedWrites(std.testing.allocator, &writes);
    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| std.testing.allocator.free(key);
        deletes.deinit(std.testing.allocator);
    }

    const setup_ops = [_]DataOperation{
        .{ .set_range = .{ .start = @constCast("doc:a"), .end = @constCast("doc:z") } },
        .{ .prepare_split = @constCast("doc:m") },
        .{ .start_split = .{ .new_shard_id = 88, .split_key = @constCast("doc:m") } },
    };
    try appendOperationEffects(&src, std.testing.allocator, 41, &setup_ops, &writes, &deletes);
    try putOwnedBatch(&src, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const delta_ops = [_]DataOperation{
        .{ .put = .{ .key = @constCast("doc:b"), .value = @constCast("left-1") } },
        .{ .delete = @constCast("doc:b") },
        .{ .put = .{ .key = @constCast("doc:c"), .value = @constCast("left-2") } },
    };
    try appendOperationEffects(&src, std.testing.allocator, 41, &delta_ops, &writes, &deletes);
    try putOwnedBatch(&src, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    try std.testing.expectEqual(@as(u64, 1), try currentSplitDeltaSequence(&src, std.testing.allocator, 41));

    const deltas = try listDeltasAfter(&src, std.testing.allocator, 41, 0);
    defer shard_mod.freeDeltas(std.testing.allocator, deltas);
    try std.testing.expectEqual(@as(usize, 1), deltas.len);
    try std.testing.expectEqual(@as(usize, 1), deltas[0].writes.len);
    try std.testing.expectEqual(@as(usize, 1), deltas[0].deletes.len);

    try applyDeltas(&dst, std.testing.allocator, 41, deltas);

    const copied_state = try groupState(&dst, std.testing.allocator, 41);
    defer {
        for (copied_state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(copied_state);
    }
    try std.testing.expectEqual(@as(usize, 1), copied_state.len);
    try std.testing.expectEqualStrings("doc:c", copied_state[0].key);
    try std.testing.expectEqualStrings("left-2", copied_state[0].value);
}

test "shard state store captures right-hand split handoff and filters delta catch-up" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-handoff-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_path);
    const src_path_z = try std.testing.allocator.dupeZ(u8, src_path);
    defer std.testing.allocator.free(src_path_z);
    var src = try docstore.DocStore.open(std.testing.allocator, src_path_z.ptr, .{});
    defer src.close();

    const dst_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/shard-state-handoff-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_path);
    const dst_path_z = try std.testing.allocator.dupeZ(u8, dst_path);
    defer std.testing.allocator.free(dst_path_z);
    var dst = try docstore.DocStore.open(std.testing.allocator, dst_path_z.ptr, .{});
    defer dst.close();

    var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer freeOwnedWrites(std.testing.allocator, &writes);
    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| std.testing.allocator.free(key);
        deletes.deinit(std.testing.allocator);
    }

    const setup_ops = [_]DataOperation{
        .{ .set_range = .{ .start = @constCast("doc:a"), .end = @constCast("doc:z") } },
        .{ .put = .{ .key = @constCast("doc:b"), .value = @constCast("left-0") } },
        .{ .put = .{ .key = @constCast("doc:t"), .value = @constCast("right-0") } },
        .{ .prepare_split = @constCast("doc:m") },
        .{ .start_split = .{ .new_shard_id = 89, .split_key = @constCast("doc:m") } },
        .{ .put = .{ .key = @constCast("doc:u"), .value = @constCast("right-1") } },
    };
    try appendOperationEffects(&src, std.testing.allocator, 51, &setup_ops, &writes, &deletes);
    try putOwnedBatch(&src, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const handoff = try captureSplitHandoff(&src, std.testing.allocator, 51);
    defer freeHandoff(std.testing.allocator, handoff);
    try std.testing.expectEqualStrings("doc:m", handoff.byte_range.start);
    try std.testing.expectEqualStrings("doc:z", handoff.byte_range.end);
    try std.testing.expectEqual(@as(u64, 1), handoff.base_delta_sequence);
    try std.testing.expectEqual(@as(usize, 2), handoff.entries.len);
    try applyHandoff(&dst, std.testing.allocator, 52, handoff);

    const initial_range = try currentRange(&dst, std.testing.allocator, 52);
    defer range_state.freeRange(std.testing.allocator, initial_range);
    try std.testing.expectEqualStrings("doc:m", initial_range.start);
    try std.testing.expectEqualStrings("doc:z", initial_range.end);

    const post_capture_ops = [_]DataOperation{
        .{ .put = .{ .key = @constCast("doc:c"), .value = @constCast("left-1") } },
        .{ .put = .{ .key = @constCast("doc:x"), .value = @constCast("right-2") } },
        .{ .delete = @constCast("doc:t") },
    };
    try appendOperationEffects(&src, std.testing.allocator, 51, &post_capture_ops, &writes, &deletes);
    try putOwnedBatch(&src, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const catchup = try listDeltasAfter(&src, std.testing.allocator, 51, handoff.base_delta_sequence);
    defer shard_mod.freeDeltas(std.testing.allocator, catchup);
    try std.testing.expectEqual(@as(usize, 1), catchup.len);
    try applyDeltas(&dst, std.testing.allocator, 52, catchup);

    const dst_state = try groupState(&dst, std.testing.allocator, 52);
    defer freeGroupStateEntries(std.testing.allocator, dst_state);
    try std.testing.expectEqual(@as(usize, 2), dst_state.len);
    try std.testing.expectEqualStrings("doc:u", dst_state[0].key);
    try std.testing.expectEqualStrings("right-1", dst_state[0].value);
    try std.testing.expectEqualStrings("doc:x", dst_state[1].key);
    try std.testing.expectEqualStrings("right-2", dst_state[1].value);
}
