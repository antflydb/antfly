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

//! Range-based shard splitting with a 5-phase state machine and delta tracking.
//!
//! Matches Go antfly's PrepareSplit/Split/FinalizeSplit lifecycle:
//!   - Split state persisted in LMDB under `splitstate:current`
//!   - Split deltas stored under `splitdelta:<seq>` (u64 big-endian for sorted iteration)
//!   - streamRange for copying data between DocStores
//!   - validateKeyOwnership considering active split state

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_time = @import("../platform/time.zig");
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const lsm_backend = @import("lsm_backend.zig");
const lmdb = @import("lmdb.zig");
const mem_backend = @import("mem_backend.zig");
const docstore = @import("docstore.zig");
const internal_keys = @import("internal_keys.zig");
const DocStore = docstore.DocStore;
const KVPair = docstore.KVPair;
const OwnedKVPair = docstore.OwnedKVPair;
const ByteRange = docstore.ByteRange;

// ============================================================================
// Split state machine
// ============================================================================

pub const SplitPhase = enum(u8) {
    none = 0,
    prepare = 1,
    splitting = 2,
    finalizing = 3,
    rolling_back = 4,
};

pub const SplitState = struct {
    phase: SplitPhase,
    split_key: []const u8,
    new_shard_id: u64,
    started_at: u64,
    original_range_end: []const u8,
};

pub const SplitDelta = struct {
    sequence: u64,
    timestamp: u64,
    writes: []OwnedKVPair,
    deletes: [][]u8,
};

// ============================================================================
// Binary encoding for SplitState
// ============================================================================

/// Encode SplitState to binary:
/// [phase:u8][split_key_len:u32 LE][split_key][new_shard_id:u64 LE]
/// [started_at:u64 LE][orig_end_len:u32 LE][orig_end]
fn encodeSplitState(buf: []u8, state: *const SplitState) []const u8 {
    var pos: usize = 0;
    buf[pos] = @intFromEnum(state.phase);
    pos += 1;

    const sk_len: u32 = @intCast(state.split_key.len);
    @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, sk_len)));
    pos += 4;
    @memcpy(buf[pos..][0..state.split_key.len], state.split_key);
    pos += state.split_key.len;

    @memcpy(buf[pos..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, state.new_shard_id)));
    pos += 8;
    @memcpy(buf[pos..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, state.started_at)));
    pos += 8;

    const oe_len: u32 = @intCast(state.original_range_end.len);
    @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, oe_len)));
    pos += 4;
    @memcpy(buf[pos..][0..state.original_range_end.len], state.original_range_end);
    pos += state.original_range_end.len;

    return buf[0..pos];
}

/// Decode SplitState from binary. Returned slices point into `data`.
fn decodeSplitState(data: []const u8) ?SplitState {
    if (data.len < 1 + 4 + 8 + 8 + 4) return null;
    var pos: usize = 0;

    const phase: SplitPhase = @enumFromInt(data[pos]);
    pos += 1;

    const sk_len = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
    pos += 4;
    if (pos + sk_len > data.len) return null;
    const split_key = data[pos..][0..sk_len];
    pos += sk_len;

    if (pos + 8 + 8 + 4 > data.len) return null;
    const new_shard_id = std.mem.littleToNative(u64, @as(*align(1) const u64, @ptrCast(data[pos..][0..8])).*);
    pos += 8;
    const started_at = std.mem.littleToNative(u64, @as(*align(1) const u64, @ptrCast(data[pos..][0..8])).*);
    pos += 8;

    const oe_len = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
    pos += 4;
    if (pos + oe_len > data.len) return null;
    const original_range_end = data[pos..][0..oe_len];

    return .{
        .phase = phase,
        .split_key = split_key,
        .new_shard_id = new_shard_id,
        .started_at = started_at,
        .original_range_end = original_range_end,
    };
}

// ============================================================================
// Delta encoding
// ============================================================================

/// Encode a delta: [timestamp:u64 LE][num_writes:u32 LE][num_deletes:u32 LE]
///   writes: [key_len:u32 LE][key][val_len:u32 LE][val] ...
///   deletes: [key_len:u32 LE][key] ...
pub fn encodeSplitDeltaAlloc(alloc: Allocator, timestamp: u64, writes: []const KVPair, deletes: []const []const u8) ![]u8 {
    // Calculate total size
    var size: usize = 16; // timestamp + num_writes + num_deletes
    for (writes) |kv| {
        size += 4 + kv.key.len + 4 + kv.value.len;
    }
    for (deletes) |key| {
        size += 4 + key.len;
    }

    const buf = try alloc.alloc(u8, size);
    errdefer alloc.free(buf);
    var pos: usize = 0;

    std.mem.writeInt(u64, buf[pos..][0..8], timestamp, .little);
    pos += 8;

    const nw: u32 = @intCast(writes.len);
    @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, nw)));
    pos += 4;
    const nd: u32 = @intCast(deletes.len);
    @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, nd)));
    pos += 4;

    for (writes) |kv| {
        const kl: u32 = @intCast(kv.key.len);
        @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, kl)));
        pos += 4;
        @memcpy(buf[pos..][0..kv.key.len], kv.key);
        pos += kv.key.len;
        const vl: u32 = @intCast(kv.value.len);
        @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, vl)));
        pos += 4;
        @memcpy(buf[pos..][0..kv.value.len], kv.value);
        pos += kv.value.len;
    }
    for (deletes) |key| {
        const kl: u32 = @intCast(key.len);
        @memcpy(buf[pos..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, kl)));
        pos += 4;
        @memcpy(buf[pos..][0..key.len], key);
        pos += key.len;
    }

    return buf;
}

/// Decode a delta from binary. Caller owns returned SplitDelta memory.
pub fn decodeSplitDeltaAlloc(alloc: Allocator, seq: u64, data: []const u8) !SplitDelta {
    if (data.len < 16) return error.InvalidDelta;
    var pos: usize = 0;

    const timestamp = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;

    const nw = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
    pos += 4;
    const nd = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
    pos += 4;

    var writes = try alloc.alloc(OwnedKVPair, nw);
    errdefer {
        for (writes[0..nw]) |w| {
            alloc.free(w.key);
            alloc.free(w.value);
        }
        alloc.free(writes);
    }

    for (0..nw) |idx| {
        const kl = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
        pos += 4;
        const key = try alloc.dupe(u8, data[pos..][0..kl]);
        pos += kl;
        const vl = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
        pos += 4;
        const val = try alloc.dupe(u8, data[pos..][0..vl]);
        pos += vl;
        writes[idx] = .{ .key = key, .value = val };
    }

    var del_list = try alloc.alloc([]u8, nd);
    errdefer {
        for (del_list[0..nd]) |d| alloc.free(d);
        alloc.free(del_list);
    }

    for (0..nd) |idx| {
        const kl = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data[pos..][0..4])).*);
        pos += 4;
        del_list[idx] = try alloc.dupe(u8, data[pos..][0..kl]);
        pos += kl;
    }

    return .{
        .sequence = seq,
        .timestamp = timestamp,
        .writes = writes,
        .deletes = del_list,
    };
}

/// Free a SplitDelta.
pub fn freeDelta(alloc: Allocator, delta: *const SplitDelta) void {
    for (delta.writes) |w| {
        alloc.free(w.key);
        alloc.free(w.value);
    }
    alloc.free(delta.writes);
    for (delta.deletes) |d| alloc.free(d);
    alloc.free(delta.deletes);
}

/// Free a slice of SplitDeltas returned from listDeltasAfter.
pub fn freeDeltas(alloc: Allocator, deltas: []SplitDelta) void {
    for (deltas) |*d| freeDelta(alloc, d);
    alloc.free(deltas);
}

// ============================================================================
// LMDB key helpers
// ============================================================================

const split_state_key = "splitstate:current";
const delta_prefix = "splitdelta:";

/// Build delta key: "splitdelta:<seq as u64 big-endian>"
fn deltaKey(buf: *[19]u8, seq: u64) []const u8 {
    @memcpy(buf[0..11], delta_prefix);
    const be = std.mem.nativeToBig(u64, seq);
    @memcpy(buf[11..19], std.mem.asBytes(&be));
    return buf[0..19];
}

/// Parse sequence number from a delta key.
fn parseDeltaSeq(key: []const u8) ?u64 {
    if (key.len != 19) return null;
    if (!std.mem.startsWith(u8, key, delta_prefix)) return null;
    return std.mem.bigToNative(u64, @as(*align(1) const u64, @ptrCast(key[11..19])).*);
}

fn isSplitMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, split_state_key) or std.mem.startsWith(u8, key, delta_prefix);
}

// ============================================================================
// ShardManager
// ============================================================================

pub const KeyOwnershipError = error{
    KeyOutOfRange,
    SplitInProgress,
};

pub fn validateRangeOwnership(byte_range: ByteRange, key: []const u8) KeyOwnershipError!void {
    if (!byte_range.contains(key)) return error.KeyOutOfRange;
}

pub fn validateSplitAwareOwnership(byte_range: ByteRange, split_state: ?SplitState, key: []const u8) KeyOwnershipError!void {
    validateRangeOwnership(byte_range, key) catch |err| switch (err) {
        error.KeyOutOfRange => {
            const state = split_state orelse return error.KeyOutOfRange;
            if (state.phase == .splitting) {
                const original_range = ByteRange{
                    .start = byte_range.start,
                    .end = state.original_range_end,
                };
                if (original_range.contains(key)) return;
            }
            return error.KeyOutOfRange;
        },
        error.SplitInProgress => return error.SplitInProgress,
    };
}

pub fn validatePrepareSplit(byte_range: ByteRange, split_state: ?SplitState, split_key: []const u8) KeyOwnershipError!void {
    if (split_state) |state| {
        if (state.phase != .none) return error.SplitInProgress;
    }
    try validateRangeOwnership(byte_range, split_key);
    if (byte_range.start.len > 0 and std.mem.eql(u8, split_key, byte_range.start))
        return error.KeyOutOfRange;
}

pub fn validateStartSplit(split_state: ?SplitState, split_key: []const u8) KeyOwnershipError!void {
    if (split_state == null) return error.SplitInProgress;
    const state = split_state.?;
    if (state.phase != .prepare) return error.SplitInProgress;
    if (!std.mem.eql(u8, state.split_key, split_key)) return error.KeyOutOfRange;
}

pub fn validateFinalizeSplit(split_state: ?SplitState) KeyOwnershipError!void {
    if (split_state == null) return error.SplitInProgress;
    const state = split_state.?;
    if (state.phase != .splitting and state.phase != .finalizing) return error.SplitInProgress;
}

pub fn validateRollbackSplit(split_state: ?SplitState) KeyOwnershipError!void {
    if (split_state == null) return error.SplitInProgress;
}

pub const ShardManager = struct {
    alloc: Allocator,
    store: backend_erased.Store,
    owns_store: bool,
    byte_range: ByteRange,
    split_state: ?SplitState,
    delta_seq: u64,
    // Owned copies of split state strings
    owned_split_key: []u8,
    owned_orig_end: []u8,
    owned_range_start: []u8,
    owned_range_end: []u8,

    pub fn init(alloc: Allocator, store: anytype, byte_range: ByteRange) !ShardManager {
        const runtime_store = try initRuntimeStore(alloc, store);
        var mgr = ShardManager{
            .alloc = alloc,
            .store = runtime_store.store,
            .owns_store = runtime_store.owned,
            .byte_range = byte_range,
            .split_state = null,
            .delta_seq = 0,
            .owned_split_key = &.{},
            .owned_orig_end = &.{},
            .owned_range_start = try alloc.dupe(u8, byte_range.start),
            .owned_range_end = try alloc.dupe(u8, byte_range.end),
        };
        mgr.byte_range.start = mgr.owned_range_start;
        mgr.byte_range.end = mgr.owned_range_end;

        // Try to load persisted split state
        mgr.loadSplitState() catch {};
        mgr.loadDeltaSeq() catch {};
        return mgr;
    }

    pub fn deinit(self: *ShardManager) void {
        if (self.owned_split_key.len > 0) self.alloc.free(self.owned_split_key);
        if (self.owned_orig_end.len > 0) self.alloc.free(self.owned_orig_end);
        self.alloc.free(self.owned_range_start);
        self.alloc.free(self.owned_range_end);
        if (self.owns_store) self.store.deinit();
        self.* = undefined;
    }

    fn loadSplitState(self: *ShardManager) !void {
        const data = storeGetAlloc(self, split_state_key) catch return;
        defer self.alloc.free(data);

        const state = decodeSplitState(data) orelse return;
        if (state.phase == .none) return;

        self.owned_split_key = try self.alloc.dupe(u8, state.split_key);
        self.owned_orig_end = try self.alloc.dupe(u8, state.original_range_end);
        self.split_state = .{
            .phase = state.phase,
            .split_key = self.owned_split_key,
            .new_shard_id = state.new_shard_id,
            .started_at = state.started_at,
            .original_range_end = self.owned_orig_end,
        };

        // If we were in splitting phase, range was already narrowed
        if (state.phase == .splitting or state.phase == .finalizing) {
            self.alloc.free(self.owned_range_end);
            self.owned_range_end = try self.alloc.dupe(u8, state.split_key);
            self.byte_range.end = self.owned_range_end;
        }
    }

    fn loadDeltaSeq(self: *ShardManager) !void {
        const all = try self.storeScanPrefix(self.alloc, delta_prefix);
        defer backend_scan.freeResults(self.alloc, all);

        var max_seq: u64 = 0;
        for (all) |kv| {
            const seq = parseDeltaSeq(kv.key) orelse continue;
            max_seq = @max(max_seq, seq);
        }
        self.delta_seq = max_seq;
    }

    fn persistSplitState(self: *ShardManager) !void {
        if (self.split_state) |*state| {
            var buf: [1024]u8 = undefined;
            const encoded = encodeSplitState(&buf, state);
            try storePut(self, split_state_key, encoded);
        }
    }

    fn clearSplitState(self: *ShardManager) !void {
        storeDelete(self, split_state_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        if (self.owned_split_key.len > 0) {
            self.alloc.free(self.owned_split_key);
            self.owned_split_key = &.{};
        }
        if (self.owned_orig_end.len > 0) {
            self.alloc.free(self.owned_orig_end);
            self.owned_orig_end = &.{};
        }
        self.split_state = null;
    }

    pub fn getByteRange(self: *const ShardManager) ByteRange {
        return self.byte_range;
    }

    pub fn setByteRange(self: *ShardManager, byte_range: ByteRange) !void {
        const start = try self.alloc.dupe(u8, byte_range.start);
        errdefer self.alloc.free(start);
        const end = try self.alloc.dupe(u8, byte_range.end);
        errdefer self.alloc.free(end);

        self.alloc.free(self.owned_range_start);
        self.alloc.free(self.owned_range_end);
        self.owned_range_start = start;
        self.owned_range_end = end;
        self.byte_range = .{
            .start = self.owned_range_start,
            .end = self.owned_range_end,
        };
    }

    pub fn getSplitState(self: *const ShardManager) ?SplitState {
        return self.split_state;
    }

    pub fn setSplitState(self: *ShardManager, state: ?SplitState) !void {
        if (state == null) {
            try self.clearSplitState();
            return;
        }

        if (self.owned_split_key.len > 0) {
            self.alloc.free(self.owned_split_key);
            self.owned_split_key = &.{};
        }
        if (self.owned_orig_end.len > 0) {
            self.alloc.free(self.owned_orig_end);
            self.owned_orig_end = &.{};
        }

        self.owned_split_key = try self.alloc.dupe(u8, state.?.split_key);
        errdefer {
            self.alloc.free(self.owned_split_key);
            self.owned_split_key = &.{};
        }
        self.owned_orig_end = try self.alloc.dupe(u8, state.?.original_range_end);
        errdefer {
            self.alloc.free(self.owned_orig_end);
            self.owned_orig_end = &.{};
        }

        self.split_state = .{
            .phase = state.?.phase,
            .split_key = self.owned_split_key,
            .new_shard_id = state.?.new_shard_id,
            .started_at = state.?.started_at,
            .original_range_end = self.owned_orig_end,
        };

        if (state.?.phase == .splitting or state.?.phase == .finalizing) {
            const new_end = try self.alloc.dupe(u8, state.?.split_key);
            self.alloc.free(self.owned_range_end);
            self.owned_range_end = new_end;
            self.byte_range.end = self.owned_range_end;
        }

        try self.persistSplitState();
    }

    pub fn getDeltaSequence(self: *const ShardManager) u64 {
        return self.delta_seq;
    }

    /// Phase 1: Validate split key is in range, set PREPARE phase.
    pub fn prepareSplit(self: *ShardManager, split_key: []const u8) !void {
        try validatePrepareSplit(self.byte_range, self.split_state, split_key);

        self.owned_split_key = try self.alloc.dupe(u8, split_key);
        self.owned_orig_end = try self.alloc.dupe(u8, self.byte_range.end);

        self.split_state = .{
            .phase = .prepare,
            .split_key = self.owned_split_key,
            .new_shard_id = 0,
            .started_at = monotonicNs(),
            .original_range_end = self.owned_orig_end,
        };

        try self.persistSplitState();
    }

    /// Phase 2: Narrow range to [start, splitKey), begin tracking deltas.
    pub fn split(self: *ShardManager, new_shard_id: u64, split_key: []const u8) !void {
        try validateStartSplit(self.split_state, split_key);
        const state = &self.split_state.?;

        state.phase = .splitting;
        state.new_shard_id = new_shard_id;

        // Narrow range: end = split_key
        self.alloc.free(self.owned_range_end);
        self.owned_range_end = try self.alloc.dupe(u8, split_key);
        self.byte_range.end = self.owned_range_end;

        self.delta_seq = 0;

        try self.persistSplitState();
    }

    /// Phase 3: Physically reclaim the right-hand range, then clear state.
    pub fn finalizeSplit(self: *ShardManager) !void {
        try validateFinalizeSplit(self.split_state);
        const state = &self.split_state.?;

        state.phase = .finalizing;
        try self.persistSplitState();

        const split_key = state.split_key;
        const physical_split_key = try self.physicalRangeLowerBoundAlloc(split_key);
        defer self.alloc.free(physical_split_key);

        const orig_end = state.original_range_end;
        const physical_orig_end = try self.physicalRangeUpperBoundAlloc(orig_end);
        defer if (physical_orig_end) |bound| self.alloc.free(bound);
        const internal_user_keys = try self.storeUsesInternalUserKeys();

        const to_delete = try self.storeScanRange(
            self.alloc,
            physical_split_key,
            if (physical_orig_end) |bound| bound else "",
        );
        defer backend_scan.freeResults(self.alloc, to_delete);

        if (to_delete.len > 0) {
            var del_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer del_keys.deinit(self.alloc);

            for (to_delete) |kv| {
                if (isSplitMetadataKey(kv.key)) continue;
                if (internal_user_keys and !internal_keys.isInternalPhysicalTableDataKey(kv.key)) continue;
                try del_keys.append(self.alloc, kv.key);
            }

            const no_writes: []const KVPair = &.{};
            try storePutBatch(self, no_writes, del_keys.items);
        }

        // Clear deltas
        try self.clearDeltas();

        // Clear split state
        try self.clearSplitState();
    }

    fn storeUsesInternalUserKeys(self: *ShardManager) !bool {
        const prefix = [_]u8{internal_keys.user_namespace};
        const keys = try self.storeScanPrefix(self.alloc, prefix[0..]);
        defer backend_scan.freeResults(self.alloc, keys);
        if (keys.len == 0) return false;
        return keys[0].key.len > 0 and keys[0].key[0] == internal_keys.user_namespace;
    }

    fn physicalRangeLowerBoundAlloc(self: *ShardManager, key: []const u8) ![]u8 {
        if (!(try self.storeUsesInternalUserKeys())) return try self.alloc.dupe(u8, key);
        return try internal_keys.documentRangeLowerAlloc(self.alloc, key);
    }

    fn physicalRangeUpperBoundAlloc(self: *ShardManager, key: []const u8) !?[]u8 {
        if (key.len == 0) return null;
        if (!(try self.storeUsesInternalUserKeys())) return try self.alloc.dupe(u8, key);
        return try internal_keys.documentRangeUpperAlloc(self.alloc, key);
    }

    /// Rollback: restore original range, clear state + deltas.
    pub fn rollbackSplit(self: *ShardManager) !void {
        try validateRollbackSplit(self.split_state);
        const state = &self.split_state.?;

        // Restore original range end
        const orig_end = try self.alloc.dupe(u8, state.original_range_end);
        self.alloc.free(self.owned_range_end);
        self.owned_range_end = orig_end;
        self.byte_range.end = self.owned_range_end;

        // Clear deltas
        try self.clearDeltas();

        // Clear state
        try self.clearSplitState();
    }

    /// Append a split delta (changes that happened during split).
    pub fn appendSplitDelta(self: *ShardManager, timestamp: u64, writes: []const KVPair, deletes: []const []const u8) !void {
        if (self.split_state == null) return error.SplitInProgress;
        if (self.split_state.?.phase != .splitting) return error.SplitInProgress;

        self.delta_seq += 1;
        const encoded = try encodeSplitDeltaAlloc(self.alloc, timestamp, writes, deletes);
        defer self.alloc.free(encoded);

        var key_buf: [19]u8 = undefined;
        const key = deltaKey(&key_buf, self.delta_seq);
        try storePut(self, key, encoded);
    }

    /// List all deltas with sequence > after_seq. Caller owns returned memory.
    pub fn listDeltasAfter(self: *ShardManager, alloc: Allocator, after_seq: u64) ![]SplitDelta {
        const all = try self.storeScanPrefix(alloc, delta_prefix);
        defer backend_scan.freeResults(alloc, all);

        var results = std.ArrayListUnmanaged(SplitDelta).empty;
        errdefer {
            for (results.items) |*d| freeDelta(alloc, d);
            results.deinit(alloc);
        }

        for (all) |kv| {
            const seq = parseDeltaSeq(kv.key) orelse continue;
            if (seq <= after_seq) continue;
            const delta = try decodeSplitDeltaAlloc(alloc, seq, kv.value);
            try results.append(alloc, delta);
        }

        const owned = try alloc.dupe(SplitDelta, results.items);
        results.deinit(alloc);
        return owned;
    }

    /// Apply deltas to this store (for the new shard to catch up).
    pub fn applyDeltas(self: *ShardManager, deltas: []const SplitDelta) !void {
        for (deltas) |delta| {
            // Convert OwnedKVPair to KVPair for putBatch
            const writes = try self.alloc.alloc(KVPair, delta.writes.len);
            defer self.alloc.free(writes);
            for (delta.writes, 0..) |w, idx| {
                writes[idx] = .{ .key = w.key, .value = w.value };
            }

            // Convert [][]u8 to [][]const u8
            const del_list = try self.alloc.alloc([]const u8, delta.deletes.len);
            defer self.alloc.free(del_list);
            for (delta.deletes, 0..) |d, idx| {
                del_list[idx] = d;
            }

            try storePutBatch(self, writes, del_list);
        }
    }

    pub fn clearSplitDeltas(self: *ShardManager) !void {
        try self.clearDeltas();
        self.delta_seq = 0;
    }

    /// Copy all keys in [lower, upper) from this store to dest, in batches.
    pub fn streamRange(self: *ShardManager, lower: []const u8, upper: []const u8, dest: anytype) !void {
        const batch_size = 8192;
        var dest_store = try initRuntimeStore(self.alloc, dest);
        defer dest_store.deinit();
        const scanned = try self.storeScanRange(self.alloc, lower, upper);
        defer backend_scan.freeResults(self.alloc, scanned);

        var writes = std.ArrayListUnmanaged(KVPair).empty;
        defer writes.deinit(self.alloc);

        for (scanned) |entry| {
            if (isSplitMetadataKey(entry.key)) continue;

            try writes.append(self.alloc, .{
                .key = entry.key,
                .value = entry.value,
            });
            if (writes.items.len == batch_size) {
                try putWriteBatch(&dest_store.store, writes.items);
                writes.clearRetainingCapacity();
            }
        }

        if (writes.items.len > 0) try putWriteBatch(&dest_store.store, writes.items);
    }

    /// Validate that a key belongs to this shard's current range.
    pub fn validateKeyOwnership(self: *ShardManager, key: []const u8) KeyOwnershipError!void {
        try validateRangeOwnership(self.byte_range, key);
    }

    fn clearDeltas(self: *ShardManager) !void {
        const all = try self.storeScanPrefix(self.alloc, delta_prefix);
        defer backend_scan.freeResults(self.alloc, all);

        if (all.len == 0) return;

        var del_keys = try self.alloc.alloc([]const u8, all.len);
        defer self.alloc.free(del_keys);
        for (all, 0..) |kv, idx| {
            del_keys[idx] = kv.key;
        }

        const no_writes: []const KVPair = &.{};
        try storePutBatch(self, no_writes, del_keys);
    }

    fn storeScanPrefix(self: *ShardManager, alloc: Allocator, prefix: []const u8) ![]backend_scan.OwnedKVPair {
        return try backend_scan.scanPrefix(alloc, &self.store, prefix);
    }

    fn storeScanRange(self: *ShardManager, alloc: Allocator, lower: []const u8, upper: []const u8) ![]backend_scan.OwnedKVPair {
        return try backend_scan.scanRange(alloc, &self.store, lower, upper);
    }
};

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = true };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn storeGetAlloc(self: *ShardManager, key: []const u8) ![]u8 {
    var txn = try self.store.beginRead();
    defer txn.abort();
    const raw = try txn.get(key);
    return try self.alloc.dupe(u8, raw);
}

fn storePut(self: *ShardManager, key: []const u8, value: []const u8) !void {
    var txn = try self.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, value);
    try txn.commit();
}

fn storeDelete(self: *ShardManager, key: []const u8) !void {
    var txn = try self.store.beginWrite();
    errdefer txn.abort();
    try txn.delete(key);
    try txn.commit();
}

fn storePutBatch(self: *ShardManager, writes: []const KVPair, deletes: []const []const u8) !void {
    var batch = try self.store.beginBatch();
    errdefer batch.abort();
    for (writes) |kv| try batch.put(kv.key, kv.value);
    for (deletes) |key| {
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    try batch.commit();
}

fn putWriteBatch(dest: *backend_erased.Store, writes: []const KVPair) !void {
    var batch = try dest.beginBatch();
    errdefer batch.abort();
    for (writes) |kv| try batch.put(kv.key, kv.value);
    try batch.commit();
}

// ============================================================================
// Tests
// ============================================================================

var tmp_path_nonce: u64 = 0;

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ts = monotonicNs();
    const nonce = @atomicRmw(u64, &tmp_path_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-shard-{s}-{d}-{d}\x00", .{ label, ts, nonce }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn monotonicNs() u64 {
    return platform_time.monotonicNs();
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn testStorePut(store: *backend_erased.Store, key: []const u8, value: []const u8) !void {
    var txn = try store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, value);
    try txn.commit();
}

fn testStoreGetAlloc(alloc: Allocator, store: *backend_erased.Store, key: []const u8) ![]u8 {
    var txn = try store.beginRead();
    defer txn.abort();
    const raw = try txn.get(key);
    return try alloc.dupe(u8, raw);
}

test "shard split basic lifecycle" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "sl1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // Populate: keys a through f
    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");
    try store.put("e", "5");

    // Shard owns ["", "") = everything
    var mgr = try ShardManager.init(alloc, &store, .{ .start = "", .end = "" });
    defer mgr.deinit();

    // Phase 1: prepare
    try mgr.prepareSplit("c");
    try std.testing.expectEqual(SplitPhase.prepare, mgr.split_state.?.phase);

    // Phase 2: split — range narrows to ["", "c")
    try mgr.split(42, "c");
    try std.testing.expectEqual(SplitPhase.splitting, mgr.split_state.?.phase);
    try std.testing.expectEqualStrings("c", mgr.byte_range.end);

    // Keys c, d, e are now outside our range
    try std.testing.expect(!mgr.byte_range.contains("c"));
    try std.testing.expect(!mgr.byte_range.contains("d"));
    try std.testing.expect(mgr.byte_range.contains("a"));
    try std.testing.expect(mgr.byte_range.contains("b"));

    // Phase 3: finalize — deletes keys in [c, original_end)
    try mgr.finalizeSplit();
    try std.testing.expect(mgr.split_state == null);

    // Keys c, d, e should be deleted from store
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "c"));
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "d"));
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "e"));

    // Keys a, b still exist
    const va = try store.get(alloc, "a");
    defer alloc.free(va);
    try std.testing.expectEqualStrings("1", va);
}

test "shard split delta tracking" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "sd1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("d", "4");

    var mgr = try ShardManager.init(alloc, &store, .{ .start = "", .end = "" });
    defer mgr.deinit();

    try mgr.prepareSplit("c");
    try mgr.split(99, "c");

    // Append deltas during split
    const w1 = [_]KVPair{.{ .key = "a", .value = "updated" }};
    const d1 = [_][]const u8{"d"};
    try mgr.appendSplitDelta(1_000, &w1, &d1);

    const w2 = [_]KVPair{.{ .key = "b", .value = "new" }};
    const no_del: []const []const u8 = &.{};
    try mgr.appendSplitDelta(2_000, &w2, no_del);

    // List deltas after seq 0
    const deltas = try mgr.listDeltasAfter(alloc, 0);
    defer freeDeltas(alloc, deltas);

    try std.testing.expectEqual(@as(usize, 2), deltas.len);
    try std.testing.expectEqual(@as(u64, 1), deltas[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), deltas[1].sequence);
    try std.testing.expectEqual(@as(u64, 1_000), deltas[0].timestamp);
    try std.testing.expectEqual(@as(u64, 2_000), deltas[1].timestamp);
    try std.testing.expectEqual(@as(usize, 1), deltas[0].writes.len);
    try std.testing.expectEqualStrings("a", deltas[0].writes[0].key);
    try std.testing.expectEqualStrings("updated", deltas[0].writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), deltas[0].deletes.len);

    // List after seq 1 should only return delta 2
    const deltas2 = try mgr.listDeltasAfter(alloc, 1);
    defer freeDeltas(alloc, deltas2);
    try std.testing.expectEqual(@as(usize, 1), deltas2.len);
    try std.testing.expectEqual(@as(u64, 2), deltas2[0].sequence);
}

test "shard split delta tracking works with memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try testStorePut(&runtime, "a", "1");
    try testStorePut(&runtime, "d", "4");

    var mgr = try ShardManager.init(alloc, &runtime, .{ .start = "", .end = "" });
    defer mgr.deinit();

    try mgr.prepareSplit("c");
    try mgr.split(99, "c");

    const w1 = [_]KVPair{.{ .key = "a", .value = "updated" }};
    const d1 = [_][]const u8{"d"};
    try mgr.appendSplitDelta(1_000, &w1, &d1);

    const deltas = try mgr.listDeltasAfter(alloc, 0);
    defer freeDeltas(alloc, deltas);

    try std.testing.expectEqual(@as(usize, 1), deltas.len);
    try std.testing.expectEqualStrings("a", deltas[0].writes[0].key);
    try std.testing.expectEqualStrings("updated", deltas[0].writes[0].value);
    try std.testing.expectEqualStrings("d", deltas[0].deletes[0]);
}

test "shard streamRange copies data" {
    const alloc = std.testing.allocator;
    var pb1: [256]u8 = undefined;
    const sp1 = tmpPath(&pb1, "sr1");
    defer cleanupTmp(sp1);
    var pb2: [256]u8 = undefined;
    const sp2 = tmpPath(&pb2, "sr2");
    defer cleanupTmp(sp2);

    var src_store = try DocStore.open(alloc, sp1, .{});
    defer src_store.close();
    var dst_store = try DocStore.open(alloc, sp2, .{});
    defer dst_store.close();

    try src_store.put("a", "1");
    try src_store.put("b", "2");
    try src_store.put("c", "3");
    try src_store.put("d", "4");
    try src_store.put("e", "5");

    var mgr = try ShardManager.init(alloc, &src_store, .{ .start = "", .end = "" });
    defer mgr.deinit();

    // Stream range [c, e) to dest
    try mgr.streamRange("c", "e", &dst_store);

    // Dest should have c, d but not a, b, e
    const vc = try dst_store.get(alloc, "c");
    defer alloc.free(vc);
    try std.testing.expectEqualStrings("3", vc);

    const vd = try dst_store.get(alloc, "d");
    defer alloc.free(vd);
    try std.testing.expectEqualStrings("4", vd);

    try std.testing.expectError(lmdb.Error.NotFound, dst_store.get(alloc, "a"));
    try std.testing.expectError(lmdb.Error.NotFound, dst_store.get(alloc, "e"));
}

test "shard streamRange copies data between lsm backend stores" {
    const alloc = std.testing.allocator;
    var src_backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer src_backend.close();
    var dst_backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer dst_backend.close();

    var src = try src_backend.runtimeStore(alloc, .{ .name = "docs" });
    defer src.deinit();
    var dst = try dst_backend.runtimeStore(alloc, .{ .name = "docs" });
    defer dst.deinit();

    try testStorePut(&src, "a", "1");
    try testStorePut(&src, "b", "2");
    try testStorePut(&src, "c", "3");
    try testStorePut(&src, "d", "4");
    try testStorePut(&src, "e", "5");

    var mgr = try ShardManager.init(alloc, &src, .{ .start = "", .end = "" });
    defer mgr.deinit();

    try mgr.streamRange("c", "e", &dst);

    const vc = try testStoreGetAlloc(alloc, &dst, "c");
    defer alloc.free(vc);
    try std.testing.expectEqualStrings("3", vc);

    const vd = try testStoreGetAlloc(alloc, &dst, "d");
    defer alloc.free(vd);
    try std.testing.expectEqualStrings("4", vd);

    try std.testing.expectError(error.NotFound, testStoreGetAlloc(alloc, &dst, "a"));
    try std.testing.expectError(error.NotFound, testStoreGetAlloc(alloc, &dst, "e"));
}

test "shard rollback restores range" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "rb1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("c", "3");

    var mgr = try ShardManager.init(alloc, &store, .{ .start = "", .end = "" });
    defer mgr.deinit();

    try mgr.prepareSplit("c");
    try mgr.split(42, "c");

    // Range narrowed
    try std.testing.expectEqualStrings("c", mgr.byte_range.end);

    // Rollback
    try mgr.rollbackSplit();

    // Range restored to original
    try std.testing.expectEqualStrings("", mgr.byte_range.end);
    try std.testing.expect(mgr.split_state == null);

    // Data unchanged
    const va = try store.get(alloc, "a");
    defer alloc.free(va);
    try std.testing.expectEqualStrings("1", va);
    const vc = try store.get(alloc, "c");
    defer alloc.free(vc);
    try std.testing.expectEqualStrings("3", vc);
}

test "shard validateKeyOwnership" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "vo1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // Shard owns [b, e)
    var mgr = try ShardManager.init(alloc, &store, .{ .start = "b", .end = "e" });
    defer mgr.deinit();

    try mgr.validateKeyOwnership("b"); // start inclusive — ok
    try mgr.validateKeyOwnership("c"); // in range — ok
    try mgr.validateKeyOwnership("d"); // in range — ok

    try std.testing.expectError(error.KeyOutOfRange, mgr.validateKeyOwnership("a")); // below
    try std.testing.expectError(error.KeyOutOfRange, mgr.validateKeyOwnership("e")); // at end (exclusive)
    try std.testing.expectError(error.KeyOutOfRange, mgr.validateKeyOwnership("f")); // above
}

test "shard finalize deletes split-off data" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "fd1");
    defer cleanupTmp(sp);

    var store = try DocStore.open(alloc, sp, .{});
    defer store.close();

    // Dense data to ensure multi-key deletion
    for ([_][]const u8{ "aa", "ab", "ac", "ba", "bb", "ca", "cb", "da" }) |key| {
        try store.put(key, "val");
    }

    // Split at "ca": left shard keeps ["", "ca"), right gets ["ca", "")
    var mgr = try ShardManager.init(alloc, &store, .{ .start = "", .end = "" });
    defer mgr.deinit();

    try mgr.prepareSplit("ca");
    try mgr.split(1, "ca");
    try mgr.finalizeSplit();

    // Keys aa, ab, ac, ba, bb should remain
    for ([_][]const u8{ "aa", "ab", "ac", "ba", "bb" }) |key| {
        const val = try store.get(alloc, key);
        alloc.free(val);
    }

    // Keys ca, cb, da should be deleted
    for ([_][]const u8{ "ca", "cb", "da" }) |key| {
        try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, key));
    }
}

test "shard state persistence across restart" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const sp = tmpPath(&pb, "sp1");
    defer cleanupTmp(sp);

    // Start split, close, reopen
    {
        var store = try DocStore.open(alloc, sp, .{});
        defer store.close();

        try store.put("a", "1");
        try store.put("d", "4");

        var mgr = try ShardManager.init(alloc, &store, .{ .start = "", .end = "" });
        defer mgr.deinit();

        try mgr.prepareSplit("c");
        try mgr.split(77, "c");
    }

    // Reopen — split state should be restored
    {
        var store = try DocStore.open(alloc, sp, .{});
        defer store.close();

        var mgr = try ShardManager.init(alloc, &store, .{ .start = "", .end = "" });
        defer mgr.deinit();

        // State should be restored
        try std.testing.expect(mgr.split_state != null);
        try std.testing.expectEqual(SplitPhase.splitting, mgr.split_state.?.phase);
        try std.testing.expectEqualStrings("c", mgr.split_state.?.split_key);
        try std.testing.expectEqual(@as(u64, 77), mgr.split_state.?.new_shard_id);

        // Range should be narrowed
        try std.testing.expectEqualStrings("c", mgr.byte_range.end);

        // Can finalize from restored state
        try mgr.finalizeSplit();
        try std.testing.expect(mgr.split_state == null);

        // d should be deleted (was in split-off range)
        try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "d"));
    }
}
