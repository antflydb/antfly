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
    transition_id: u64,
    attempt_epoch: u64,
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
    prepare_split: SplitTransition,
    start_split: SplitTransition,
    acknowledge_split: SplitAcknowledgement,
    finalize_split: SplitTransition,
    rollback_split: SplitTransition,
};

pub const SplitTransition = struct {
    transition_id: u64,
    attempt_epoch: u64,
    new_shard_id: u64,
    split_key: []u8,
};

pub const SplitAcknowledgement = struct {
    transition_id: u64,
    attempt_epoch: u64,
    destination_group_id: u64,
    delta_sequence: u64,
};

pub const SplitTerminalOutcome = enum(u8) {
    finalized = 1,
    rolled_back = 2,
};

/// Bounded durable idempotency fence for a completed source split. The
/// source-local epoch orders attempts; transition IDs remain opaque identities.
pub const AppliedSplitTerminal = struct {
    transition_id: u64,
    attempt_epoch: u64,
    destination_group_id: u64,
    split_key: []const u8,
    outcome: SplitTerminalOutcome,
};

pub fn validateSplitIdentity(state: AppliedSplitState, transition_id: u64, attempt_epoch: u64, destination_group_id: u64, split_key: ?[]const u8) !void {
    if (state.transition_id != transition_id or state.attempt_epoch != attempt_epoch or state.new_shard_id != destination_group_id)
        return error.ConflictingSplitTransition;
    if (split_key) |expected| {
        if (!std.mem.eql(u8, state.split_key, expected)) return error.ConflictingSplitTransition;
    }
}

pub fn validateSplitTerminalIdentity(terminal: AppliedSplitTerminal, transition_id: u64, attempt_epoch: u64, destination_group_id: u64, split_key: ?[]const u8) !void {
    if (terminal.transition_id != transition_id or terminal.attempt_epoch != attempt_epoch or terminal.destination_group_id != destination_group_id)
        return error.ConflictingSplitTransition;
    if (split_key) |expected| {
        if (!std.mem.eql(u8, terminal.split_key, expected)) return error.ConflictingSplitTransition;
    }
}

pub fn currentRange(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !AppliedDataRange {
    var txn = try store.beginReadTxn();
    defer txn.abort();
    return try currentRangeTxn(&txn, alloc, group_id);
}

fn currentRangeTxn(txn: *docstore.DocStore.Txn, alloc: std.mem.Allocator, group_id: u64) !AppliedDataRange {
    const key = try groupRangeKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{ .start = "", .end = "" },
        else => return err,
    };
    return try range_state.decodeRangeAlloc(alloc, raw);
}

pub fn groupState(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
    var txn = try store.beginReadTxn();
    defer txn.abort();
    return try groupStateTxn(&txn, alloc, group_id);
}

fn groupStateTxn(txn: *docstore.DocStore.Txn, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
    const logical_prefix = try groupDocumentPrefixAlloc(alloc, group_id);
    defer alloc.free(logical_prefix);
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, logical_prefix);
    defer alloc.free(lower);
    const upper = (try internal_keys.documentRangeUpperAlloc(alloc, logical_prefix)) orelse return &.{};
    defer alloc.free(upper);

    return try collectGroupDocumentsInPhysicalRangeTxn(txn, alloc, group_id, lower, upper);
}

pub fn replaceGroupSnapshot(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    byte_range: AppliedDataRange,
    entries: []const AppliedDataKV,
) !void {
    try replaceGroupSnapshotWithMetadata(store, alloc, group_id, byte_range, entries, &.{});
}

pub fn replaceGroupSnapshotWithMetadata(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    byte_range: AppliedDataRange,
    entries: []const AppliedDataKV,
    metadata_writes: []const docstore.KVPair,
) !void {
    try replaceGroupSnapshotWithMetadataAndDeletes(store, alloc, group_id, byte_range, entries, metadata_writes, &.{});
}

fn replaceGroupSnapshotWithMetadataAndDeletes(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    byte_range: AppliedDataRange,
    entries: []const AppliedDataKV,
    metadata_writes: []const docstore.KVPair,
    metadata_deletes: []const []const u8,
) !void {
    const logical_prefix = try groupDocumentPrefixAlloc(alloc, group_id);
    defer alloc.free(logical_prefix);
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, logical_prefix);
    defer alloc.free(lower);
    const upper = (try internal_keys.documentRangeUpperAlloc(alloc, logical_prefix)) orelse return error.InvalidAppliedDataRange;
    defer alloc.free(upper);

    const existing = try store.scanRangeKeys(alloc, lower, upper);
    defer {
        for (existing) |key| alloc.free(key);
        alloc.free(existing);
    }
    var deletes = try alloc.alloc([]const u8, existing.len + metadata_deletes.len);
    defer alloc.free(deletes);
    for (existing, 0..) |key, i| deletes[i] = key;
    for (metadata_deletes, existing.len..) |key, i| deletes[i] = key;

    var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
    defer {
        for (writes.items) |write| alloc.free(@constCast(write.key));
        writes.deinit(alloc);
    }
    var range_buf: [1024]u8 = undefined;
    {
        const range_key = try groupRangeKeyAlloc(alloc, group_id);
        errdefer alloc.free(range_key);
        try writes.append(alloc, .{
            .key = range_key,
            .value = try range_state.encodeRange(byte_range, &range_buf),
        });
    }
    for (entries) |entry| {
        const document_key = try groupDocumentStoreKeyAlloc(alloc, group_id, entry.key);
        errdefer alloc.free(document_key);
        try writes.append(alloc, .{
            .key = document_key,
            .value = entry.value,
        });
    }
    for (metadata_writes) |write| {
        const metadata_key = try alloc.dupe(u8, write.key);
        errdefer alloc.free(metadata_key);
        try writes.append(alloc, .{
            .key = metadata_key,
            .value = write.value,
        });
    }
    try store.putBatch(writes.items, deletes);
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

pub fn currentSplitAcknowledgement(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !?SplitAcknowledgement {
    const key = try groupSplitAcknowledgementKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != 32) return error.InvalidSplitAcknowledgement;
    return .{
        .transition_id = std.mem.readInt(u64, raw[0..8], .little),
        .attempt_epoch = std.mem.readInt(u64, raw[8..16], .little),
        .destination_group_id = std.mem.readInt(u64, raw[16..24], .little),
        .delta_sequence = std.mem.readInt(u64, raw[24..32], .little),
    };
}

pub fn currentSplitTerminal(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) !?AppliedSplitTerminal {
    const key = try groupSplitTerminalKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try decodeSplitTerminalAlloc(alloc, raw);
}

pub fn freeSplitTerminal(alloc: std.mem.Allocator, terminal: AppliedSplitTerminal) void {
    if (terminal.split_key.len > 0) alloc.free(@constCast(terminal.split_key));
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
    var txn = try store.beginReadTxn();
    defer txn.abort();
    return try buildSnapshotTxn(&txn, alloc, group_id);
}

pub fn buildSnapshotTxn(txn: *docstore.DocStore.Txn, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try writeSnapshotTxn(txn, alloc, group_id, &out.writer, null);
    return try out.toOwnedSlice();
}

pub fn writeSnapshotTxn(
    txn: *docstore.DocStore.Txn,
    alloc: std.mem.Allocator,
    group_id: u64,
    writer: *std.Io.Writer,
    cancelled: ?*const std.atomic.Value(bool),
) !void {
    const byte_range = try currentRangeTxn(txn, alloc, group_id);
    defer range_state.freeRange(alloc, byte_range);
    const controls = try groupControlStateTxn(txn, alloc, group_id);
    defer freeGroupStateEntries(alloc, controls);

    try writer.writeAll(group_snapshot_magic);
    try writer.writeByte(group_snapshot_version);
    try writeSnapshotBytes(writer, byte_range.start);
    try writeSnapshotBytes(writer, byte_range.end);
    try writeGroupDocumentsTxn(txn, alloc, group_id, writer, cancelled);
    try writeSnapshotTerminatedEntries(writer, controls);
}

fn writeGroupDocumentsTxn(
    txn: *docstore.DocStore.Txn,
    alloc: std.mem.Allocator,
    group_id: u64,
    writer: *std.Io.Writer,
    cancelled: ?*const std.atomic.Value(bool),
) !void {
    const logical_prefix = try groupDocumentPrefixAlloc(alloc, group_id);
    defer alloc.free(logical_prefix);
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, logical_prefix);
    defer alloc.free(lower);
    const upper = (try internal_keys.documentRangeUpperAlloc(alloc, logical_prefix)) orelse {
        try writer.writeByte(0);
        return;
    };
    defer alloc.free(upper);

    var cur = try txn.openCursor();
    defer cur.close();
    cur.setUpperBound(upper);
    var entry = try cur.seekAtOrAfter(lower);
    while (entry) |kv| : (entry = try cur.next()) {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (std.mem.order(u8, kv.key, upper) != .lt) break;
        const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, kv.key)) orelse continue;
        defer alloc.free(logical_key);
        if (!std.mem.startsWith(u8, logical_key, logical_prefix)) continue;
        try writer.writeByte(1);
        try writeSnapshotBytes(writer, logical_key[logical_prefix.len..]);
        try writeSnapshotBytes(writer, kv.value);
    }
    try writer.writeByte(0);
}

fn writeSnapshotTerminatedEntries(writer: *std.Io.Writer, entries: []const AppliedDataKV) !void {
    for (entries) |entry| {
        try writer.writeByte(1);
        try writeSnapshotBytes(writer, entry.key);
        try writeSnapshotBytes(writer, entry.value);
    }
    try writer.writeByte(0);
}

fn writeSnapshotBytes(writer: *std.Io.Writer, bytes: []const u8) !void {
    try writeSnapshotU32(writer, try snapshotLength(bytes.len));
    try writer.writeAll(bytes);
}

fn writeSnapshotU32(writer: *std.Io.Writer, value: u32) !void {
    var encoded: [4]u8 = undefined;
    std.mem.writeInt(u32, &encoded, value, .little);
    try writer.writeAll(&encoded);
}

pub const GroupStateSnapshot = struct {
    byte_range: AppliedDataRange,
    entries: []AppliedDataKV,
    controls: []AppliedDataKV,

    pub fn deinit(self: *GroupStateSnapshot, alloc: std.mem.Allocator) void {
        range_state.freeRange(alloc, self.byte_range);
        freeGroupStateEntries(alloc, self.entries);
        freeGroupStateEntries(alloc, self.controls);
        self.* = undefined;
    }
};

pub const GroupStateSnapshotView = struct {
    byte_range: AppliedDataRange,
    entries: []AppliedDataKV,
    controls: []AppliedDataKV,

    pub fn deinit(self: *GroupStateSnapshotView, alloc: std.mem.Allocator) void {
        alloc.free(self.entries);
        alloc.free(self.controls);
        self.* = undefined;
    }
};

pub const GroupStateSnapshotStream = struct {
    encoded: []const u8,
    byte_range: AppliedDataRange,
    entries_pos: usize,
    controls_pos: usize,
    first_entry_key: ?[]const u8,
    last_entry_key: ?[]const u8,

    pub const Iterator = struct {
        encoded: []const u8,
        pos: usize,
        finished: bool = false,

        pub fn next(self: *Iterator) !?AppliedDataKV {
            if (self.finished) return null;
            if (self.pos >= self.encoded.len) return error.InvalidGroupStateSnapshot;
            const tag = self.encoded[self.pos];
            self.pos += 1;
            if (tag == 0) {
                self.finished = true;
                return null;
            }
            if (tag != 1) return error.InvalidGroupStateSnapshot;
            return .{
                .key = try readSnapshotBytesView(self.encoded, &self.pos),
                .value = try readSnapshotBytesView(self.encoded, &self.pos),
            };
        }
    };

    pub fn init(encoded: []const u8) !GroupStateSnapshotStream {
        if (encoded.len < group_snapshot_magic.len + 1 or
            !std.mem.eql(u8, encoded[0..group_snapshot_magic.len], group_snapshot_magic) or
            encoded[group_snapshot_magic.len] != group_snapshot_version)
        {
            return error.InvalidGroupStateSnapshot;
        }
        var pos: usize = group_snapshot_magic.len + 1;
        const start = try readSnapshotBytesView(encoded, &pos);
        const end = try readSnapshotBytesView(encoded, &pos);
        const entries_pos = pos;
        var entry_iterator = Iterator{ .encoded = encoded, .pos = entries_pos };
        var first_entry_key: ?[]const u8 = null;
        var last_entry_key: ?[]const u8 = null;
        while (try entry_iterator.next()) |entry| {
            if (last_entry_key) |previous| {
                if (std.mem.order(u8, previous, entry.key) != .lt) return error.InvalidGroupStateSnapshot;
            } else {
                first_entry_key = entry.key;
            }
            last_entry_key = entry.key;
        }
        pos = entry_iterator.pos;
        const controls_pos = pos;
        try skipSnapshotTerminatedEntries(encoded, &pos);
        if (pos != encoded.len) return error.InvalidGroupStateSnapshot;
        return .{
            .encoded = encoded,
            .byte_range = .{ .start = start, .end = end },
            .entries_pos = entries_pos,
            .controls_pos = controls_pos,
            .first_entry_key = first_entry_key,
            .last_entry_key = last_entry_key,
        };
    }

    pub fn entries(self: GroupStateSnapshotStream) Iterator {
        return .{ .encoded = self.encoded, .pos = self.entries_pos };
    }

    pub fn controls(self: GroupStateSnapshotStream) Iterator {
        return .{ .encoded = self.encoded, .pos = self.controls_pos };
    }
};

const SliceSnapshotIterator = struct {
    values: []const AppliedDataKV,
    index: usize = 0,

    fn next(self: *SliceSnapshotIterator) !?AppliedDataKV {
        if (self.index >= self.values.len) return null;
        defer self.index += 1;
        return self.values[self.index];
    }
};

pub fn decodeGroupStateSnapshotAlloc(alloc: std.mem.Allocator, encoded: []const u8) !GroupStateSnapshot {
    if (encoded.len < group_snapshot_magic.len + 1 or
        !std.mem.eql(u8, encoded[0..group_snapshot_magic.len], group_snapshot_magic))
    {
        return error.InvalidGroupStateSnapshot;
    }
    const snapshot_version = encoded[group_snapshot_magic.len];
    if (snapshot_version != group_snapshot_version) return error.InvalidGroupStateSnapshot;
    var pos: usize = group_snapshot_magic.len + 1;
    const start = try readSnapshotBytesAlloc(alloc, encoded, &pos);
    errdefer alloc.free(start);
    const end = try readSnapshotBytesAlloc(alloc, encoded, &pos);
    errdefer alloc.free(end);
    const entries = try readSnapshotTerminatedEntriesAlloc(alloc, encoded, &pos);
    errdefer freeGroupStateEntries(alloc, entries);
    const controls = try readSnapshotTerminatedEntriesAlloc(alloc, encoded, &pos);
    errdefer freeGroupStateEntries(alloc, controls);
    if (pos != encoded.len) return error.InvalidGroupStateSnapshot;
    return .{
        .byte_range = .{ .start = start, .end = end },
        .entries = entries,
        .controls = controls,
    };
}

pub fn decodeGroupStateSnapshotViewAlloc(alloc: std.mem.Allocator, encoded: []const u8) !GroupStateSnapshotView {
    if (encoded.len < group_snapshot_magic.len + 1 or
        !std.mem.eql(u8, encoded[0..group_snapshot_magic.len], group_snapshot_magic) or
        encoded[group_snapshot_magic.len] != group_snapshot_version)
    {
        return error.InvalidGroupStateSnapshot;
    }
    var pos: usize = group_snapshot_magic.len + 1;
    const start = try readSnapshotBytesView(encoded, &pos);
    const end = try readSnapshotBytesView(encoded, &pos);
    const entries = try readSnapshotTerminatedEntryViewsAlloc(alloc, encoded, &pos);
    errdefer alloc.free(entries);
    const controls = try readSnapshotTerminatedEntryViewsAlloc(alloc, encoded, &pos);
    errdefer alloc.free(controls);
    if (pos != encoded.len) return error.InvalidGroupStateSnapshot;
    return .{
        .byte_range = .{ .start = start, .end = end },
        .entries = entries,
        .controls = controls,
    };
}

pub fn validateGroupStateSnapshotStream(alloc: std.mem.Allocator, group_id: u64, snapshot: GroupStateSnapshotStream) !void {
    try validateGroupStateSnapshotEntryBounds(
        alloc,
        group_id,
        snapshot.byte_range,
        snapshot.first_entry_key,
        snapshot.last_entry_key,
        snapshot.controls(),
    );
}

pub fn installSnapshot(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64, encoded: []const u8) !void {
    try installSnapshotWithMetadata(store, alloc, group_id, encoded, &.{});
}

/// Imports a validated snapshot into a private, empty generation. Publication
/// is owned by the caller, so independently committed chunks cannot become
/// visible. Memory is bounded by one chunk of transformed keys and descriptors.
pub fn installSnapshotStreamIntoEmptyStore(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    snapshot: GroupStateSnapshotStream,
    external_metadata_writes: []const docstore.KVPair,
) !void {
    try validateGroupStateSnapshotStream(alloc, group_id, snapshot);

    const max_chunk_entries = 512;
    var writes: [max_chunk_entries]docstore.KVPair = undefined;
    var owned_keys: [max_chunk_entries][]u8 = undefined;
    var entries = snapshot.entries();
    var exhausted = false;
    while (!exhausted) {
        var len: usize = 0;
        defer for (owned_keys[0..len]) |key| alloc.free(key);
        while (len < writes.len) {
            const entry = (try entries.next()) orelse {
                exhausted = true;
                break;
            };
            const key = try groupDocumentStoreKeyAlloc(alloc, group_id, entry.key);
            owned_keys[len] = key;
            writes[len] = .{ .key = key, .value = entry.value };
            len += 1;
        }
        if (len > 0) try store.putBatch(writes[0..len], &.{});
    }

    var controls = snapshot.controls();
    exhausted = false;
    while (!exhausted) {
        var len: usize = 0;
        while (len < writes.len) {
            const control = (try controls.next()) orelse {
                exhausted = true;
                break;
            };
            writes[len] = .{ .key = control.key, .value = control.value };
            len += 1;
        }
        if (len > 0) try store.putBatch(writes[0..len], &.{});
    }

    const range_key = try groupRangeKeyAlloc(alloc, group_id);
    defer alloc.free(range_key);
    var range_buf: [1024]u8 = undefined;
    const range_value = try range_state.encodeRange(snapshot.byte_range, &range_buf);
    if (external_metadata_writes.len > max_chunk_entries - 1) return error.SnapshotMetadataTooLarge;
    writes[0] = .{ .key = range_key, .value = range_value };
    @memcpy(writes[1..][0..external_metadata_writes.len], external_metadata_writes);
    try store.putBatch(writes[0 .. external_metadata_writes.len + 1], &.{});
    try store.sync(true);
}

pub fn installSnapshotWithMetadata(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    encoded: []const u8,
    external_metadata_writes: []const docstore.KVPair,
) !void {
    var snapshot = try decodeGroupStateSnapshotViewAlloc(alloc, encoded);
    defer snapshot.deinit(alloc);
    try validateGroupStateSnapshot(alloc, group_id, snapshot);

    const existing_controls = try groupControlState(store, alloc, group_id);
    defer freeGroupStateEntries(alloc, existing_controls);
    const deletes = try alloc.alloc([]const u8, existing_controls.len);
    defer alloc.free(deletes);
    for (existing_controls, 0..) |control, i| deletes[i] = control.key;
    const writes = try alloc.alloc(docstore.KVPair, snapshot.controls.len + external_metadata_writes.len);
    defer alloc.free(writes);
    for (snapshot.controls, 0..) |control, i| writes[i] = .{ .key = control.key, .value = control.value };
    @memcpy(writes[snapshot.controls.len..], external_metadata_writes);
    try replaceGroupSnapshotWithMetadataAndDeletes(
        store,
        alloc,
        group_id,
        snapshot.byte_range,
        snapshot.entries,
        writes,
        deletes,
    );
}

pub fn validateGroupStateSnapshot(alloc: std.mem.Allocator, group_id: u64, snapshot: anytype) !void {
    var first_entry_key: ?[]const u8 = null;
    var previous_entry_key: ?[]const u8 = null;
    for (snapshot.entries) |entry| {
        if (previous_entry_key) |previous| {
            if (std.mem.order(u8, previous, entry.key) != .lt) return error.InvalidGroupStateSnapshot;
        } else {
            first_entry_key = entry.key;
        }
        previous_entry_key = entry.key;
    }
    try validateGroupStateSnapshotEntryBounds(
        alloc,
        group_id,
        .{ .start = snapshot.byte_range.start, .end = snapshot.byte_range.end },
        first_entry_key,
        previous_entry_key,
        SliceSnapshotIterator{ .values = snapshot.controls },
    );
}

fn validateGroupStateSnapshotEntryBounds(
    alloc: std.mem.Allocator,
    group_id: u64,
    byte_range: AppliedDataRange,
    first_entry_key: ?[]const u8,
    last_entry_key: ?[]const u8,
    controls_iterator: anytype,
) !void {
    if (byte_range.start.len > 0 and byte_range.end.len > 0 and
        std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
    {
        return error.InvalidGroupStateSnapshot;
    }

    var state_entry: ?AppliedDataKV = null;
    var sequence: ?u64 = null;
    var acknowledgement: ?SplitAcknowledgement = null;
    var terminal_entry: ?AppliedDataKV = null;
    var delta_count: u64 = 0;
    var max_delta_sequence: u64 = 0;
    var previous_control_order: ?u8 = null;
    var previous_delta_sequence: u64 = 0;
    var controls = controls_iterator;
    while (try controls.next()) |control| {
        try validateGroupControlEntry(alloc, group_id, control);
        const kind = groupControlKind(group_id, control.key) orelse unreachable;
        const control_order: u8 = switch (kind) {
            .split_state => 0,
            .delta_sequence => 1,
            .acknowledgement => 2,
            .terminal => 3,
            .delta => 4,
        };
        if (previous_control_order) |previous| {
            if (control_order < previous or (control_order == previous and control_order != 4))
                return error.InvalidGroupStateSnapshot;
        }
        previous_control_order = control_order;
        switch (kind) {
            .split_state => state_entry = control,
            .delta_sequence => sequence = std.mem.readInt(u64, control.value[0..8], .little),
            .acknowledgement => acknowledgement = .{
                .transition_id = std.mem.readInt(u64, control.value[0..8], .little),
                .attempt_epoch = std.mem.readInt(u64, control.value[8..16], .little),
                .destination_group_id = std.mem.readInt(u64, control.value[16..24], .little),
                .delta_sequence = std.mem.readInt(u64, control.value[24..32], .little),
            },
            .terminal => terminal_entry = control,
            .delta => |delta_sequence| {
                if (delta_sequence == 0 or delta_sequence <= previous_delta_sequence)
                    return error.InvalidGroupStateSnapshot;
                previous_delta_sequence = delta_sequence;
                delta_count += 1;
                max_delta_sequence = @max(max_delta_sequence, delta_sequence);
            },
        }
    }

    const source_sequence = sequence orelse 0;
    if ((delta_count > 0 and sequence == null) or
        max_delta_sequence > source_sequence or
        (delta_count > 0 and (delta_count != max_delta_sequence or max_delta_sequence != source_sequence)))
    {
        return error.InvalidGroupStateSnapshot;
    }

    var state: ?AppliedSplitState = null;
    defer if (state) |value| freeSplitState(alloc, value);
    if (state_entry) |entry| state = decodeSplitStateAlloc(alloc, entry.value) catch return error.InvalidGroupStateSnapshot;
    var terminal: ?AppliedSplitTerminal = null;
    defer if (terminal) |value| freeSplitTerminal(alloc, value);
    if (terminal_entry) |entry| terminal = decodeSplitTerminalAlloc(alloc, entry.value) catch return error.InvalidGroupStateSnapshot;

    if (state) |active| {
        if (sequence == null) return error.InvalidGroupStateSnapshot;
        if (terminal) |completed| {
            if (active.attempt_epoch <= completed.attempt_epoch) return error.InvalidGroupStateSnapshot;
        }
    } else if (delta_count != 0 or sequence != null) {
        return error.InvalidGroupStateSnapshot;
    }

    const snapshot_range = AppliedDataRange{
        .start = byte_range.start,
        .end = byte_range.end,
    };
    const document_range = if (state) |active|
        AppliedDataRange{
            .start = snapshot_range.start,
            .end = if (active.phase == .splitting or active.phase == .finalizing)
                active.original_range_end
            else
                snapshot_range.end,
        }
    else
        snapshot_range;
    if (document_range.start.len > 0 and document_range.end.len > 0 and
        std.mem.order(u8, document_range.start, document_range.end) != .lt)
    {
        return error.InvalidGroupStateSnapshot;
    }
    if (first_entry_key) |key| if (!document_range.contains(key)) return error.InvalidGroupStateSnapshot;
    if (last_entry_key) |key| if (!document_range.contains(key)) return error.InvalidGroupStateSnapshot;

    if (acknowledgement) |ack| {
        if (state) |active| {
            if (ack.delta_sequence > source_sequence or
                active.transition_id != ack.transition_id or
                active.attempt_epoch != ack.attempt_epoch or
                active.new_shard_id != ack.destination_group_id)
            {
                return error.InvalidGroupStateSnapshot;
            }
        } else if (terminal) |completed| {
            if (completed.outcome != .finalized or
                completed.transition_id != ack.transition_id or
                completed.attempt_epoch != ack.attempt_epoch or
                completed.destination_group_id != ack.destination_group_id)
            {
                return error.InvalidGroupStateSnapshot;
            }
        } else {
            return error.InvalidGroupStateSnapshot;
        }
    }
}

const GroupControlKind = union(enum) {
    split_state,
    delta_sequence,
    acknowledgement,
    terminal,
    delta: u64,
};

fn groupControlKind(group_id: u64, key: []const u8) ?GroupControlKind {
    var buf: [160]u8 = undefined;
    const state_key = std.fmt.bufPrint(&buf, "\x00\x00__metadata__:data_group_split_state:{d}", .{group_id}) catch return null;
    if (std.mem.eql(u8, state_key, key)) return .split_state;
    const sequence_key = std.fmt.bufPrint(&buf, "\x00\x00__metadata__:data_group_split_delta_seq:{d}", .{group_id}) catch return null;
    if (std.mem.eql(u8, sequence_key, key)) return .delta_sequence;
    const acknowledgement_key = std.fmt.bufPrint(&buf, "\x00\x00__metadata__:data_group_split_ack:{d}", .{group_id}) catch return null;
    if (std.mem.eql(u8, acknowledgement_key, key)) return .acknowledgement;
    const terminal_key = std.fmt.bufPrint(&buf, "\x00\x00__metadata__:data_group_split_terminal:{d}", .{group_id}) catch return null;
    if (std.mem.eql(u8, terminal_key, key)) return .terminal;
    return if (parseSplitDeltaSeq(group_id, key)) |sequence| .{ .delta = sequence } else null;
}

fn validateGroupControlEntry(alloc: std.mem.Allocator, group_id: u64, entry: AppliedDataKV) !void {
    const kind = groupControlKind(group_id, entry.key) orelse return error.InvalidGroupStateSnapshot;
    switch (kind) {
        .split_state => {
            const state = decodeSplitStateAlloc(alloc, entry.value) catch return error.InvalidGroupStateSnapshot;
            defer freeSplitState(alloc, state);
            if (state.phase == .none or state.transition_id == 0 or state.attempt_epoch == 0 or
                state.new_shard_id == 0 or state.split_key.len == 0)
            {
                return error.InvalidGroupStateSnapshot;
            }
        },
        .delta_sequence => if (entry.value.len != 8) return error.InvalidGroupStateSnapshot,
        .acknowledgement => {
            if (entry.value.len != 32 or
                std.mem.readInt(u64, entry.value[0..8], .little) == 0 or
                std.mem.readInt(u64, entry.value[8..16], .little) == 0 or
                std.mem.readInt(u64, entry.value[16..24], .little) == 0)
            {
                return error.InvalidGroupStateSnapshot;
            }
        },
        .terminal => {
            const terminal = decodeSplitTerminalAlloc(alloc, entry.value) catch return error.InvalidGroupStateSnapshot;
            defer freeSplitTerminal(alloc, terminal);
            if (terminal.transition_id == 0 or terminal.attempt_epoch == 0 or
                terminal.destination_group_id == 0 or terminal.split_key.len == 0)
            {
                return error.InvalidGroupStateSnapshot;
            }
        },
        .delta => |sequence| {
            var delta = shard_mod.decodeSplitDeltaAlloc(alloc, sequence, entry.value) catch return error.InvalidGroupStateSnapshot;
            defer shard_mod.freeDelta(alloc, &delta);
        },
    }
}

fn readSnapshotTerminatedEntriesAlloc(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) ![]AppliedDataKV {
    var entries = std.ArrayListUnmanaged(AppliedDataKV).empty;
    errdefer {
        for (entries.items) |entry| {
            alloc.free(@constCast(entry.key));
            alloc.free(@constCast(entry.value));
        }
        entries.deinit(alloc);
    }
    while (true) {
        if (pos.* >= encoded.len) return error.InvalidGroupStateSnapshot;
        const tag = encoded[pos.*];
        pos.* += 1;
        if (tag == 0) break;
        if (tag != 1) return error.InvalidGroupStateSnapshot;
        const key = try readSnapshotBytesAlloc(alloc, encoded, pos);
        const value = readSnapshotBytesAlloc(alloc, encoded, pos) catch |err| {
            alloc.free(key);
            return err;
        };
        entries.append(alloc, .{ .key = key, .value = value }) catch |err| {
            alloc.free(key);
            alloc.free(value);
            return err;
        };
    }
    return try entries.toOwnedSlice(alloc);
}

fn readSnapshotTerminatedEntryViewsAlloc(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) ![]AppliedDataKV {
    var entries = std.ArrayListUnmanaged(AppliedDataKV).empty;
    errdefer entries.deinit(alloc);
    while (true) {
        if (pos.* >= encoded.len) return error.InvalidGroupStateSnapshot;
        const tag = encoded[pos.*];
        pos.* += 1;
        if (tag == 0) break;
        if (tag != 1) return error.InvalidGroupStateSnapshot;
        try entries.append(alloc, .{
            .key = try readSnapshotBytesView(encoded, pos),
            .value = try readSnapshotBytesView(encoded, pos),
        });
    }
    return try entries.toOwnedSlice(alloc);
}

fn skipSnapshotTerminatedEntries(encoded: []const u8, pos: *usize) !void {
    while (true) {
        if (pos.* >= encoded.len) return error.InvalidGroupStateSnapshot;
        const tag = encoded[pos.*];
        pos.* += 1;
        if (tag == 0) return;
        if (tag != 1) return error.InvalidGroupStateSnapshot;
        _ = try readSnapshotBytesView(encoded, pos);
        _ = try readSnapshotBytesView(encoded, pos);
    }
}

fn readSnapshotBytesAlloc(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) ![]u8 {
    const len = try readSnapshotU32(encoded, pos);
    if (pos.* > encoded.len or len > encoded.len - pos.*) return error.InvalidGroupStateSnapshot;
    const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + len]);
    pos.* += len;
    return value;
}

fn readSnapshotBytesView(encoded: []const u8, pos: *usize) ![]const u8 {
    const len = try readSnapshotU32(encoded, pos);
    if (pos.* > encoded.len or len > encoded.len - pos.*) return error.InvalidGroupStateSnapshot;
    defer pos.* += len;
    return encoded[pos.* .. pos.* + len];
}

fn readSnapshotU32(encoded: []const u8, pos: *usize) !u32 {
    if (pos.* > encoded.len or encoded.len - pos.* < 4) return error.InvalidGroupStateSnapshot;
    const value = std.mem.readInt(u32, encoded[pos.*..][0..4], .little);
    pos.* += 4;
    return value;
}

fn groupControlState(store: *docstore.DocStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
    var txn = try store.beginReadTxn();
    defer txn.abort();
    return try groupControlStateTxn(&txn, alloc, group_id);
}

fn groupControlStateTxn(txn: *docstore.DocStore.Txn, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
    var out = std.ArrayListUnmanaged(AppliedDataKV).empty;
    errdefer {
        for (out.items) |entry| {
            alloc.free(@constCast(entry.key));
            alloc.free(@constCast(entry.value));
        }
        out.deinit(alloc);
    }

    var point_keys: [4][]u8 = undefined;
    var initialized: usize = 0;
    defer for (point_keys[0..initialized]) |key| alloc.free(key);
    point_keys[initialized] = try groupSplitStateKeyAlloc(alloc, group_id);
    initialized += 1;
    point_keys[initialized] = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
    initialized += 1;
    point_keys[initialized] = try groupSplitAcknowledgementKeyAlloc(alloc, group_id);
    initialized += 1;
    point_keys[initialized] = try groupSplitTerminalKeyAlloc(alloc, group_id);
    initialized += 1;
    for (point_keys) |key| {
        const borrowed = txn.get(key) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        const value = try alloc.dupe(u8, borrowed);
        errdefer alloc.free(value);
        try out.append(alloc, .{ .key = owned_key, .value = value });
    }

    const delta_prefix = try groupSplitDeltaPrefixAlloc(alloc, group_id);
    defer alloc.free(delta_prefix);
    const deltas = try docstore.DocStore.scanPrefixTxn(alloc, txn, delta_prefix);
    defer alloc.free(deltas);
    for (deltas) |delta| {
        errdefer {
            alloc.free(delta.key);
            alloc.free(delta.value);
        }
        try out.append(alloc, .{ .key = delta.key, .value = delta.value });
    }
    return try out.toOwnedSlice(alloc);
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

    return try collectGroupDocumentsInPhysicalRange(store, alloc, group_id, lower, if (upper) |bound| bound else "");
}

fn collectGroupDocumentsInPhysicalRange(
    store: *docstore.DocStore,
    alloc: std.mem.Allocator,
    group_id: u64,
    lower: []const u8,
    upper: []const u8,
) ![]AppliedDataKV {
    var txn = try store.beginReadTxn();
    defer txn.abort();
    return try collectGroupDocumentsInPhysicalRangeTxn(&txn, alloc, group_id, lower, upper);
}

fn collectGroupDocumentsInPhysicalRangeTxn(
    txn: *docstore.DocStore.Txn,
    alloc: std.mem.Allocator,
    group_id: u64,
    lower: []const u8,
    upper: []const u8,
) ![]AppliedDataKV {
    const kvs = try docstore.DocStore.scanRangeTxn(alloc, txn, lower, upper);
    var processed: usize = 0;
    defer {
        for (kvs[processed..]) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }

    var state = std.ArrayListUnmanaged(AppliedDataKV).empty;
    errdefer {
        for (state.items) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        state.deinit(alloc);
    }

    const logical_prefix = try groupDocumentPrefixAlloc(alloc, group_id);
    defer alloc.free(logical_prefix);

    for (kvs) |kv| {
        const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, kv.key)) orelse {
            alloc.free(kv.key);
            alloc.free(kv.value);
            processed += 1;
            continue;
        };
        defer alloc.free(logical_key);
        if (!std.mem.startsWith(u8, logical_key, logical_prefix)) {
            alloc.free(kv.key);
            alloc.free(kv.value);
            processed += 1;
            continue;
        }
        const key = try stripGroupDocumentPrefixAlloc(alloc, logical_key, group_id);
        state.append(alloc, .{ .key = key, .value = kv.value }) catch |err| {
            alloc.free(key);
            return err;
        };
        alloc.free(kv.key);
        processed += 1;
    }
    return try state.toOwnedSlice(alloc);
}

test "group state range scan is allocation-failure safe" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/group-state-range-oom", .{tmp.sub_path});
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
    const operations = [_]DataOperation{
        .{ .set_range = .{ .start = @constCast("doc:a"), .end = @constCast("") } },
        .{ .put = .{ .key = @constCast("doc:n"), .value = @constCast("one") } },
        .{ .put = .{ .key = @constCast("doc:t"), .value = @constCast("two") } },
        .{ .put = .{ .key = @constCast("doc:z"), .value = @constCast("exclusive-end") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 61, &operations, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    const ttl_key = try internal_keys.ttlKeyAlloc(std.testing.allocator, "g:61:doc:t");
    defer std.testing.allocator.free(ttl_key);
    try store.put(ttl_key, "1234");

    const Runner = struct {
        fn run(alloc: std.mem.Allocator, source: *docstore.DocStore) !void {
            const state = try groupStateInRange(source, alloc, 61, .{ .start = "doc:m", .end = "doc:z" });
            defer freeGroupStateEntries(alloc, state);
            try std.testing.expectEqual(@as(usize, 2), state.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{&store});
}

fn allocPreparedSplitState(
    alloc: std.mem.Allocator,
    original_range_end: []const u8,
    transition: SplitTransition,
) !AppliedSplitState {
    const split_key = try alloc.dupe(u8, transition.split_key);
    errdefer alloc.free(split_key);
    const range_end = try alloc.dupe(u8, original_range_end);
    errdefer alloc.free(range_end);
    return .{
        .phase = .prepare,
        .transition_id = transition.transition_id,
        .attempt_epoch = transition.attempt_epoch,
        .split_key = split_key,
        .new_shard_id = transition.new_shard_id,
        .original_range_end = range_end,
    };
}

fn appendSplitStateWrite(
    alloc: std.mem.Allocator,
    group_id: u64,
    state: AppliedSplitState,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    const key = try groupSplitStateKeyAlloc(alloc, group_id);
    errdefer alloc.free(key);
    var split_buf: [1024]u8 = undefined;
    const encoded = try encodeSplitState(state, &split_buf);
    const value = try alloc.dupe(u8, encoded);
    errdefer alloc.free(value);
    removeOwnedWriteByKey(alloc, writes, key);
    removeDeleteByKey(alloc, deletes, key);
    try writes.append(alloc, .{ .key = key, .value = value });
}

fn appendSplitAcknowledgementClear(
    alloc: std.mem.Allocator,
    group_id: u64,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    const key = try groupSplitAcknowledgementKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    removeOwnedWriteByKey(alloc, writes, key);
    removeDeleteByKey(alloc, deletes, key);
    try deletes.append(alloc, try alloc.dupe(u8, key));
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
    var needs_split_ack = false;
    for (operations) |op| switch (op) {
        .acknowledge_split => needs_split_ack = true,
        else => {},
    };
    var split_acknowledgement = if (needs_split_ack)
        try currentSplitAcknowledgement(store, alloc, group_id)
    else
        null;
    const source_delta_sequence = if (needs_split_ack)
        try currentSplitDeltaSequence(store, alloc, group_id)
    else
        0;
    var split_terminal = try currentSplitTerminal(store, alloc, group_id);
    defer if (split_terminal) |terminal| freeSplitTerminal(alloc, terminal);
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
        .prepare_split => |prepare| {
            if (prepare.attempt_epoch == 0) return error.InvalidSplitAttemptEpoch;
            if (split_terminal) |terminal| {
                if (prepare.attempt_epoch < terminal.attempt_epoch) continue;
                if (prepare.attempt_epoch == terminal.attempt_epoch) {
                    try validateSplitTerminalIdentity(terminal, prepare.transition_id, prepare.attempt_epoch, prepare.new_shard_id, prepare.split_key);
                    continue;
                }
            }
            if (split_state) |state| {
                const already_prepared = switch (state.phase) {
                    .prepare, .splitting, .finalizing => state.transition_id == prepare.transition_id and
                        state.attempt_epoch == prepare.attempt_epoch and
                        state.new_shard_id == prepare.new_shard_id and
                        std.mem.eql(u8, state.split_key, prepare.split_key),
                    .none, .rolling_back => false,
                };
                if (already_prepared) continue;
                if (state.phase == .prepare or state.phase == .splitting or state.phase == .finalizing)
                    return error.ConflictingSplitTransition;
            }
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validatePrepareSplit(byte_range, shard_split_state, prepare.split_key);

            if (split_state) |state| {
                freeSplitState(alloc, state);
                split_state = null;
            }

            split_state = try allocPreparedSplitState(alloc, byte_range.end, prepare);
            try appendSplitStateWrite(alloc, group_id, split_state.?, writes, deletes);

            try appendSplitAcknowledgementClear(alloc, group_id, writes, deletes);
            split_acknowledgement = null;
        },
        .start_split => |start| {
            if (start.attempt_epoch == 0) return error.InvalidSplitAttemptEpoch;
            if (split_terminal) |terminal| {
                if (start.attempt_epoch < terminal.attempt_epoch) continue;
                if (start.attempt_epoch == terminal.attempt_epoch) {
                    try validateSplitTerminalIdentity(terminal, start.transition_id, start.attempt_epoch, start.new_shard_id, start.split_key);
                    continue;
                }
            }
            if (split_state) |state| {
                const already_started = switch (state.phase) {
                    .splitting, .finalizing => state.transition_id == start.transition_id and
                        state.attempt_epoch == start.attempt_epoch and
                        state.new_shard_id == start.new_shard_id and
                        std.mem.eql(u8, state.split_key, start.split_key),
                    .none, .prepare, .rolling_back => false,
                };
                if (already_started) continue;
                if (state.phase == .splitting or state.phase == .finalizing)
                    return error.ConflictingSplitTransition;
            }
            if (split_state == null) {
                // A replacement generation can inherit a Raft applied index
                // just ahead of its projected state. Start carries the full
                // transition identity, so recover the missing prepare only
                // while the unsplit range still validates it. Conflicting or
                // already-narrowed generations remain rejected below.
                try shard_mod.validatePrepareSplit(byte_range, null, start.split_key);
                split_state = try allocPreparedSplitState(alloc, byte_range.end, start);
                try appendSplitAcknowledgementClear(alloc, group_id, writes, deletes);
                split_acknowledgement = null;
            }
            const shard_split_state: ?shard_mod.SplitState = if (split_state) |state| .{
                .phase = state.phase,
                .split_key = state.split_key,
                .new_shard_id = state.new_shard_id,
                .started_at = 0,
                .original_range_end = state.original_range_end,
            } else null;
            try shard_mod.validateStartSplit(shard_split_state, start.split_key);
            try validateSplitIdentity(split_state.?, start.transition_id, start.attempt_epoch, start.new_shard_id, start.split_key);

            split_state.?.phase = .splitting;

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

            try appendSplitStateWrite(alloc, group_id, split_state.?, writes, deletes);

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
        .acknowledge_split => |acknowledgement| {
            const state = split_state orelse return error.SplitInProgress;
            try validateSplitIdentity(state, acknowledgement.transition_id, acknowledgement.attempt_epoch, acknowledgement.destination_group_id, null);
            if (state.phase != .splitting and state.phase != .finalizing) return error.SplitInProgress;
            if (split_acknowledgement) |current| {
                if (current.transition_id != acknowledgement.transition_id or
                    current.attempt_epoch != acknowledgement.attempt_epoch or
                    current.destination_group_id != acknowledgement.destination_group_id)
                    return error.ConflictingSplitTransition;
                if (acknowledgement.delta_sequence <= current.delta_sequence) continue;
            }
            if (acknowledgement.delta_sequence > source_delta_sequence)
                return error.SplitAcknowledgementAheadOfSource;
            const acknowledgement_key = try groupSplitAcknowledgementKeyAlloc(alloc, group_id);
            errdefer alloc.free(acknowledgement_key);
            removeOwnedWriteByKey(alloc, writes, acknowledgement_key);
            removeDeleteByKey(alloc, deletes, acknowledgement_key);
            const value = try alloc.alloc(u8, 32);
            errdefer alloc.free(value);
            std.mem.writeInt(u64, value[0..8], acknowledgement.transition_id, .little);
            std.mem.writeInt(u64, value[8..16], acknowledgement.attempt_epoch, .little);
            std.mem.writeInt(u64, value[16..24], acknowledgement.destination_group_id, .little);
            std.mem.writeInt(u64, value[24..32], acknowledgement.delta_sequence, .little);
            try writes.append(alloc, .{ .key = acknowledgement_key, .value = value });
            split_acknowledgement = acknowledgement;
        },
        .finalize_split => |finalize| {
            if (finalize.attempt_epoch == 0) return error.InvalidSplitAttemptEpoch;
            if (split_terminal) |terminal| {
                if (finalize.attempt_epoch < terminal.attempt_epoch) continue;
                if (finalize.attempt_epoch == terminal.attempt_epoch) {
                    try validateSplitTerminalIdentity(terminal, finalize.transition_id, finalize.attempt_epoch, finalize.new_shard_id, finalize.split_key);
                    if (terminal.outcome != .finalized) return error.ConflictingSplitTransition;
                    continue;
                }
            }
            if (split_state) |state| try validateSplitIdentity(state, finalize.transition_id, finalize.attempt_epoch, finalize.new_shard_id, finalize.split_key);
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
            const split_delta_seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
            defer alloc.free(split_delta_seq_key);
            removeOwnedWriteByKey(alloc, writes, split_delta_seq_key);
            try appendSplitTerminal(
                alloc,
                group_id,
                .{
                    .transition_id = finalize.transition_id,
                    .attempt_epoch = finalize.attempt_epoch,
                    .destination_group_id = finalize.new_shard_id,
                    .split_key = finalize.split_key,
                    .outcome = .finalized,
                },
                &split_terminal,
                writes,
                deletes,
            );
            freeSplitState(alloc, split_state.?);
            split_state = null;
        },
        .rollback_split => |rollback| {
            if (rollback.attempt_epoch == 0) return error.InvalidSplitAttemptEpoch;
            if (split_terminal) |terminal| {
                if (rollback.attempt_epoch < terminal.attempt_epoch) continue;
                if (rollback.attempt_epoch == terminal.attempt_epoch) {
                    try validateSplitTerminalIdentity(terminal, rollback.transition_id, rollback.attempt_epoch, rollback.new_shard_id, rollback.split_key);
                    if (terminal.outcome != .rolled_back) return error.ConflictingSplitTransition;
                    continue;
                }
            }
            if (split_state) |state| try validateSplitIdentity(state, rollback.transition_id, rollback.attempt_epoch, rollback.new_shard_id, rollback.split_key);
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
            const split_delta_seq_key = try groupSplitDeltaSeqKeyAlloc(alloc, group_id);
            defer alloc.free(split_delta_seq_key);
            removeOwnedWriteByKey(alloc, writes, split_delta_seq_key);
            const acknowledgement_key = try groupSplitAcknowledgementKeyAlloc(alloc, group_id);
            defer alloc.free(acknowledgement_key);
            removeOwnedWriteByKey(alloc, writes, acknowledgement_key);
            removeDeleteByKey(alloc, deletes, acknowledgement_key);
            try deletes.append(alloc, try alloc.dupe(u8, acknowledgement_key));
            split_acknowledgement = null;
            try appendSplitTerminal(
                alloc,
                group_id,
                .{
                    .transition_id = rollback.transition_id,
                    .attempt_epoch = rollback.attempt_epoch,
                    .destination_group_id = rollback.new_shard_id,
                    .split_key = rollback.split_key,
                    .outcome = .rolled_back,
                },
                &split_terminal,
                writes,
                deletes,
            );
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

fn appendSplitTerminal(
    alloc: std.mem.Allocator,
    group_id: u64,
    terminal: AppliedSplitTerminal,
    current: *?AppliedSplitTerminal,
    writes: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    const key = try groupSplitTerminalKeyAlloc(alloc, group_id);
    defer alloc.free(key);
    removeOwnedWriteByKey(alloc, writes, key);
    removeDeleteByKey(alloc, deletes, key);
    {
        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        const value = try encodeSplitTerminalAlloc(alloc, terminal);
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = owned_key, .value = value });
    }
    const owned_split_key = try alloc.dupe(u8, terminal.split_key);
    errdefer alloc.free(owned_split_key);
    if (current.*) |previous| freeSplitTerminal(alloc, previous);
    current.* = .{
        .transition_id = terminal.transition_id,
        .attempt_epoch = terminal.attempt_epoch,
        .destination_group_id = terminal.destination_group_id,
        .split_key = owned_split_key,
        .outcome = terminal.outcome,
    };
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

fn groupSplitAcknowledgementKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_split_ack:{d}", .{group_id});
}

fn groupSplitTerminalKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_group_split_terminal:{d}", .{group_id});
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
    // Byte ranges are [start, end), while documentRangeUpperAlloc is a
    // prefix upper bound and would include the document exactly at `end`.
    return try internal_keys.documentRangeLowerAlloc(alloc, logical);
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

const group_snapshot_magic = "AFDS";
const group_snapshot_version: u8 = 3;

pub fn encodeGroupStateSnapshot(
    alloc: std.mem.Allocator,
    byte_range: AppliedDataRange,
    entries: []const AppliedDataKV,
    controls: []const AppliedDataKV,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, group_snapshot_magic);
    try out.append(alloc, group_snapshot_version);
    try appendSnapshotU32(alloc, &out, try snapshotLength(byte_range.start.len));
    try out.appendSlice(alloc, byte_range.start);
    try appendSnapshotU32(alloc, &out, try snapshotLength(byte_range.end.len));
    try out.appendSlice(alloc, byte_range.end);
    try appendSnapshotTerminatedEntries(alloc, &out, entries);
    try appendSnapshotTerminatedEntries(alloc, &out, controls);
    return try out.toOwnedSlice(alloc);
}

fn appendSnapshotTerminatedEntries(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), entries: []const AppliedDataKV) !void {
    for (entries) |entry| {
        try out.append(alloc, 1);
        try appendSnapshotU32(alloc, out, try snapshotLength(entry.key.len));
        try out.appendSlice(alloc, entry.key);
        try appendSnapshotU32(alloc, out, try snapshotLength(entry.value.len));
        try out.appendSlice(alloc, entry.value);
    }
    try out.append(alloc, 0);
}

fn snapshotLength(value: usize) !u32 {
    return std.math.cast(u32, value) orelse error.GroupStateSnapshotTooLarge;
}

fn appendSnapshotU32(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var encoded: [4]u8 = undefined;
    std.mem.writeInt(u32, &encoded, value, .little);
    try out.appendSlice(alloc, &encoded);
}

fn encodeSplitState(state: AppliedSplitState, buf: []u8) ![]const u8 {
    const total_len = 1 + 8 + 8 + 8 + 4 + state.split_key.len + 4 + state.original_range_end.len;
    if (total_len > buf.len) return error.SplitStateTooLarge;
    var pos: usize = 0;
    buf[pos] = @intFromEnum(state.phase);
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], state.transition_id, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], state.attempt_epoch, .little);
    pos += 8;
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
    if (encoded.len < 1 + 8 + 8 + 8 + 4 + 4) return error.InvalidSplitState;
    var pos: usize = 0;
    const phase = std.enums.fromInt(SplitPhase, encoded[pos]) orelse return error.InvalidSplitState;
    pos += 1;
    const transition_id = std.mem.readInt(u64, encoded[pos..][0..8], .little);
    pos += 8;
    const attempt_epoch = std.mem.readInt(u64, encoded[pos..][0..8], .little);
    pos += 8;
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
        .transition_id = transition_id,
        .attempt_epoch = attempt_epoch,
        .split_key = split_key,
        .new_shard_id = new_shard_id,
        .original_range_end = original_range_end,
    };
}

const split_terminal_format_version: u8 = 2;

fn encodeSplitTerminalAlloc(alloc: std.mem.Allocator, terminal: AppliedSplitTerminal) ![]u8 {
    const header_len = 1 + 1 + 8 + 8 + 8 + 4;
    const encoded = try alloc.alloc(u8, header_len + terminal.split_key.len);
    encoded[0] = split_terminal_format_version;
    encoded[1] = @intFromEnum(terminal.outcome);
    std.mem.writeInt(u64, encoded[2..10], terminal.transition_id, .little);
    std.mem.writeInt(u64, encoded[10..18], terminal.attempt_epoch, .little);
    std.mem.writeInt(u64, encoded[18..26], terminal.destination_group_id, .little);
    std.mem.writeInt(u32, encoded[26..30], @intCast(terminal.split_key.len), .little);
    @memcpy(encoded[30..], terminal.split_key);
    return encoded;
}

fn decodeSplitTerminalAlloc(alloc: std.mem.Allocator, encoded: []const u8) !AppliedSplitTerminal {
    if (encoded.len < 30 or encoded[0] != split_terminal_format_version) return error.InvalidSplitTerminal;
    const outcome = std.enums.fromInt(SplitTerminalOutcome, encoded[1]) orelse return error.InvalidSplitTerminal;
    const split_key_len = std.mem.readInt(u32, encoded[26..30], .little);
    if (encoded.len != 30 + split_key_len) return error.InvalidSplitTerminal;
    return .{
        .transition_id = std.mem.readInt(u64, encoded[2..10], .little),
        .attempt_epoch = std.mem.readInt(u64, encoded[10..18], .little),
        .destination_group_id = std.mem.readInt(u64, encoded[18..26], .little),
        .split_key = try alloc.dupe(u8, encoded[30..]),
        .outcome = outcome,
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
        .{ .prepare_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 42, .split_key = @constCast("doc:m") } },
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
    try std.testing.expectEqual(@as(u64, 42), prepared.new_shard_id);
    try std.testing.expectEqualStrings("doc:m", prepared.split_key);

    try appendOperationEffects(&store, std.testing.allocator, 23, &prepare_ops, &writes, &deletes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    const conflicting_prepare = [_]DataOperation{
        .{ .prepare_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 43, .split_key = @constCast("doc:m") } },
    };
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &conflicting_prepare, &writes, &deletes),
    );
    const conflicting_start = [_]DataOperation{
        .{ .start_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 43, .split_key = @constCast("doc:m") } },
    };
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &conflicting_start, &writes, &deletes),
    );

    const start_ops = [_]DataOperation{
        .{ .start_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 42, .split_key = @constCast("doc:m") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &start_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    try appendOperationEffects(&store, std.testing.allocator, 23, &start_ops, &writes, &deletes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &conflicting_start, &writes, &deletes),
    );
    const conflicting_finalize = [_]DataOperation{
        .{ .finalize_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 43, .split_key = @constCast("doc:m") } },
    };
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &conflicting_finalize, &writes, &deletes),
    );
    const conflicting_rollback = [_]DataOperation{
        .{ .rollback_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 43, .split_key = @constCast("doc:m") } },
    };
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &conflicting_rollback, &writes, &deletes),
    );
    const conflicting_acknowledgement = [_]DataOperation{
        .{ .acknowledge_split = .{ .transition_id = 41, .attempt_epoch = 1, .destination_group_id = 43, .delta_sequence = 0 } },
    };
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &conflicting_acknowledgement, &writes, &deletes),
    );
    const acknowledgement_ops = [_]DataOperation{
        .{ .acknowledge_split = .{ .transition_id = 41, .attempt_epoch = 1, .destination_group_id = 42, .delta_sequence = 0 } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &acknowledgement_ops, &writes, &deletes);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();
    const acknowledgement = (try currentSplitAcknowledgement(&store, std.testing.allocator, 23)) orelse
        return error.MissingSplitAcknowledgement;
    try std.testing.expectEqual(@as(u64, 41), acknowledgement.transition_id);
    try std.testing.expectEqual(@as(u64, 42), acknowledgement.destination_group_id);

    const delta_sequence_key = try groupSplitDeltaSeqKeyAlloc(std.testing.allocator, 23);
    defer std.testing.allocator.free(delta_sequence_key);
    var delta_sequence_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &delta_sequence_bytes, 2, .little);
    try store.put(delta_sequence_key, &delta_sequence_bytes);

    const advanced_acknowledgement = [_]DataOperation{
        .{ .acknowledge_split = .{ .transition_id = 41, .attempt_epoch = 1, .destination_group_id = 42, .delta_sequence = 2 } },
        .{ .acknowledge_split = .{ .transition_id = 41, .attempt_epoch = 1, .destination_group_id = 42, .delta_sequence = 1 } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &advanced_acknowledgement, &writes, &deletes);
    try std.testing.expectEqual(@as(usize, 1), writes.items.len);
    try putOwnedBatch(&store, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const stale_acknowledgement = [_]DataOperation{
        .{ .acknowledge_split = .{ .transition_id = 41, .attempt_epoch = 1, .destination_group_id = 42, .delta_sequence = 1 } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &stale_acknowledgement, &writes, &deletes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    const retained_acknowledgement = (try currentSplitAcknowledgement(&store, std.testing.allocator, 23)) orelse
        return error.MissingSplitAcknowledgement;
    try std.testing.expectEqual(@as(u64, 2), retained_acknowledgement.delta_sequence);

    const future_acknowledgement = [_]DataOperation{
        .{ .acknowledge_split = .{ .transition_id = 41, .attempt_epoch = 1, .destination_group_id = 42, .delta_sequence = 3 } },
    };
    try std.testing.expectError(
        error.SplitAcknowledgementAheadOfSource,
        appendOperationEffects(&store, std.testing.allocator, 23, &future_acknowledgement, &writes, &deletes),
    );
    const stale_incarnation_acknowledgement = [_]DataOperation{
        .{ .acknowledge_split = .{ .transition_id = 40, .attempt_epoch = 1, .destination_group_id = 42, .delta_sequence = 2 } },
    };
    try std.testing.expectError(
        error.ConflictingSplitTransition,
        appendOperationEffects(&store, std.testing.allocator, 23, &stale_incarnation_acknowledgement, &writes, &deletes),
    );

    const narrowed_range = try currentRange(&store, std.testing.allocator, 23);
    defer range_state.freeRange(std.testing.allocator, narrowed_range);
    try std.testing.expectEqualStrings("doc:a", narrowed_range.start);
    try std.testing.expectEqualStrings("doc:m", narrowed_range.end);

    const out_of_range_put = [_]DataOperation{
        .{ .put = .{ .key = @constCast("doc:zz"), .value = @constCast("blocked") } },
    };
    try std.testing.expectError(error.KeyOutOfRange, appendOperationEffects(&store, std.testing.allocator, 23, &out_of_range_put, &writes, &deletes));

    const rollback_ops = [_]DataOperation{
        .{ .rollback_split = .{ .transition_id = 41, .attempt_epoch = 1, .new_shard_id = 42, .split_key = @constCast("doc:m") } },
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
    try std.testing.expect((try currentSplitAcknowledgement(&store, std.testing.allocator, 23)) == null);
    const rolled_back = (try currentSplitTerminal(&store, std.testing.allocator, 23)) orelse
        return error.MissingSplitTerminal;
    defer freeSplitTerminal(std.testing.allocator, rolled_back);
    try std.testing.expectEqual(SplitTerminalOutcome.rolled_back, rolled_back.outcome);

    try appendOperationEffects(&store, std.testing.allocator, 23, &prepare_ops, &writes, &deletes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    const next_prepare = [_]DataOperation{
        .{ .prepare_split = .{ .transition_id = 41, .attempt_epoch = 2, .new_shard_id = 43, .split_key = @constCast("doc:n") } },
    };
    try appendOperationEffects(&store, std.testing.allocator, 23, &next_prepare, &writes, &deletes);
    try std.testing.expect(writes.items.len > 0);
}

test "shard state snapshot round trips split control state" {
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    const source_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-source", .{source_tmp.sub_path});
    defer std.testing.allocator.free(source_path);
    const source_path_z = try std.testing.allocator.dupeZ(u8, source_path);
    defer std.testing.allocator.free(source_path_z);
    var source = try docstore.DocStore.open(std.testing.allocator, source_path_z.ptr, .{});
    defer source.close();
    try replaceGroupSnapshot(&source, std.testing.allocator, 91, .{ .start = "doc:a", .end = "doc:z" }, &.{});

    var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
    defer freeOwnedWrites(std.testing.allocator, &writes);
    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| std.testing.allocator.free(key);
        deletes.deinit(std.testing.allocator);
    }
    const prepare_start = [_]DataOperation{
        .{ .prepare_split = .{ .transition_id = 7001, .attempt_epoch = 1, .new_shard_id = 92, .split_key = @constCast("doc:m") } },
        .{ .start_split = .{ .transition_id = 7001, .attempt_epoch = 1, .new_shard_id = 92, .split_key = @constCast("doc:m") } },
        .{ .acknowledge_split = .{ .transition_id = 7001, .attempt_epoch = 1, .destination_group_id = 92, .delta_sequence = 0 } },
    };
    try appendOperationEffects(&source, std.testing.allocator, 91, &prepare_start, &writes, &deletes);
    try putOwnedBatch(&source, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const split_write = [_]DataOperation{
        .{ .put = .{ .key = @constCast("doc:t"), .value = @constCast("right-1") } },
    };
    try appendOperationEffects(&source, std.testing.allocator, 91, &split_write, &writes, &deletes);
    try putOwnedBatch(&source, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();
    try std.testing.expectEqual(@as(u64, 1), try currentSplitDeltaSequence(&source, std.testing.allocator, 91));

    const active_snapshot = try buildSnapshot(&source, std.testing.allocator, 91);
    defer std.testing.allocator.free(active_snapshot);
    var decoded = try decodeGroupStateSnapshotAlloc(std.testing.allocator, active_snapshot);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("doc:m", decoded.byte_range.end);
    try std.testing.expect(decoded.controls.len >= 3);

    var target_tmp = std.testing.tmpDir(.{});
    defer target_tmp.cleanup();
    const target_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-target", .{target_tmp.sub_path});
    defer std.testing.allocator.free(target_path);
    const target_path_z = try std.testing.allocator.dupeZ(u8, target_path);
    defer std.testing.allocator.free(target_path_z);
    var target = try docstore.DocStore.open(std.testing.allocator, target_path_z.ptr, .{});
    defer target.close();
    try installSnapshot(&target, std.testing.allocator, 91, active_snapshot);
    const active = (try currentSplitState(&target, std.testing.allocator, 91)) orelse return error.MissingSplitState;
    defer freeSplitState(std.testing.allocator, active);
    try std.testing.expectEqual(@as(u64, 1), active.attempt_epoch);
    const ack = (try currentSplitAcknowledgement(&target, std.testing.allocator, 91)) orelse return error.MissingSplitAcknowledgement;
    try std.testing.expectEqual(@as(u64, 1), ack.attempt_epoch);
    const restored_deltas = try listDeltasAfter(&target, std.testing.allocator, 91, 0);
    defer shard_mod.freeDeltas(std.testing.allocator, restored_deltas);
    try std.testing.expectEqual(@as(usize, 1), restored_deltas.len);

    const rollback = [_]DataOperation{
        .{ .rollback_split = .{ .transition_id = 7001, .attempt_epoch = 1, .new_shard_id = 92, .split_key = @constCast("doc:m") } },
    };
    try appendOperationEffects(&source, std.testing.allocator, 91, &rollback, &writes, &deletes);
    try putOwnedBatch(&source, std.testing.allocator, writes.items, deletes.items);
    freeOwnedWrites(std.testing.allocator, &writes);
    writes = .empty;
    for (deletes.items) |key| std.testing.allocator.free(key);
    deletes.clearRetainingCapacity();

    const terminal_snapshot = try buildSnapshot(&source, std.testing.allocator, 91);
    defer std.testing.allocator.free(terminal_snapshot);
    try installSnapshot(&target, std.testing.allocator, 91, terminal_snapshot);
    try std.testing.expect((try currentSplitState(&target, std.testing.allocator, 91)) == null);
    try std.testing.expect((try currentSplitAcknowledgement(&target, std.testing.allocator, 91)) == null);
    const terminal = (try currentSplitTerminal(&target, std.testing.allocator, 91)) orelse return error.MissingSplitTerminal;
    defer freeSplitTerminal(std.testing.allocator, terminal);
    try std.testing.expectEqual(@as(u64, 1), terminal.attempt_epoch);
    try std.testing.expectEqual(SplitTerminalOutcome.rolled_back, terminal.outcome);
}

test "shard state snapshot rejects duplicate and out-of-range documents" {
    var duplicate_entries = [_]AppliedDataKV{
        .{ .key = "doc:b", .value = "one" },
        .{ .key = "doc:b", .value = "two" },
    };
    try std.testing.expectError(error.InvalidGroupStateSnapshot, validateGroupStateSnapshot(
        std.testing.allocator,
        91,
        .{
            .byte_range = .{ .start = "doc:a", .end = "doc:m" },
            .entries = &duplicate_entries,
            .controls = &.{},
        },
    ));

    const encoded = try encodeGroupStateSnapshot(
        std.testing.allocator,
        .{ .start = "doc:a", .end = "doc:m" },
        &duplicate_entries,
        &.{},
    );
    defer std.testing.allocator.free(encoded);
    try std.testing.expectError(
        error.InvalidGroupStateSnapshot,
        GroupStateSnapshotStream.init(encoded),
    );

    var out_of_range_entries = [_]AppliedDataKV{
        .{ .key = "doc:z", .value = "outside" },
    };
    try std.testing.expectError(error.InvalidGroupStateSnapshot, validateGroupStateSnapshot(
        std.testing.allocator,
        91,
        .{
            .byte_range = .{ .start = "doc:a", .end = "doc:m" },
            .entries = &out_of_range_entries,
            .controls = &.{},
        },
    ));
    const encoded_out_of_range = try encodeGroupStateSnapshot(
        std.testing.allocator,
        .{ .start = "doc:a", .end = "doc:m" },
        &out_of_range_entries,
        &.{},
    );
    defer std.testing.allocator.free(encoded_out_of_range);
    const streamed_out_of_range = try GroupStateSnapshotStream.init(encoded_out_of_range);
    try std.testing.expectError(
        error.InvalidGroupStateSnapshot,
        validateGroupStateSnapshotStream(std.testing.allocator, 91, streamed_out_of_range),
    );
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
        .{ .prepare_split = .{ .transition_id = 76, .attempt_epoch = 1, .new_shard_id = 77, .split_key = @constCast("doc:m") } },
        .{ .start_split = .{ .transition_id = 76, .attempt_epoch = 1, .new_shard_id = 77, .split_key = @constCast("doc:m") } },
        .{ .finalize_split = .{ .transition_id = 76, .attempt_epoch = 1, .new_shard_id = 77, .split_key = @constCast("doc:m") } },
        .{ .prepare_split = .{ .transition_id = 76, .attempt_epoch = 1, .new_shard_id = 77, .split_key = @constCast("doc:m") } },
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
    const finalized = (try currentSplitTerminal(&store, std.testing.allocator, 31)) orelse
        return error.MissingSplitTerminal;
    defer freeSplitTerminal(std.testing.allocator, finalized);
    try std.testing.expectEqual(SplitTerminalOutcome.finalized, finalized.outcome);
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
        .{ .prepare_split = .{ .transition_id = 87, .attempt_epoch = 1, .new_shard_id = 88, .split_key = @constCast("doc:m") } },
        .{ .start_split = .{ .transition_id = 87, .attempt_epoch = 1, .new_shard_id = 88, .split_key = @constCast("doc:m") } },
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
        .{ .prepare_split = .{ .transition_id = 88, .attempt_epoch = 1, .new_shard_id = 89, .split_key = @constCast("doc:m") } },
        .{ .start_split = .{ .transition_id = 88, .attempt_epoch = 1, .new_shard_id = 89, .split_key = @constCast("doc:m") } },
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
