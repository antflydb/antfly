// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const block = @import("block.zig");
const block_table = @import("block_table.zig");
const pool_mod = @import("pool.zig");
const storage_mod = @import("storage.zig");

pub const SequenceId = u32;

pub const SequenceState = struct {
    id: SequenceId,
    pool_id: block.KvPoolId,
    block_table: block_table.SequenceBlockTable = .{},
    /// When true, this sequence holds compacted KV cache and sliding window
    /// trimming is skipped (compaction replaces eviction).
    compacted: bool = false,

    pub fn deinit(self: *SequenceState, allocator: std.mem.Allocator) void {
        self.block_table.deinit(allocator);
    }
};

pub const KvManager = struct {
    allocator: std.mem.Allocator,
    pools: std.ArrayListUnmanaged(storage_mod.KvStorage) = .empty,
    sequences: std.ArrayListUnmanaged(SequenceState) = .empty,

    pub fn init(allocator: std.mem.Allocator) KvManager {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *KvManager) void {
        for (self.sequences.items, 0..) |_, idx| {
            self.releaseSequenceByIndex(idx) catch {};
        }
        for (self.sequences.items) |*seq_state| seq_state.deinit(self.allocator);
        self.sequences.deinit(self.allocator);
        for (self.pools.items) |*storage| storage.deinit(self.allocator);
        self.pools.deinit(self.allocator);
    }

    pub fn addPool(self: *KvManager, config: pool_mod.KvPoolConfig) !block.KvPoolId {
        const id: block.KvPoolId = @intCast(self.pools.items.len);
        var storage = try storage_mod.initHost(self.allocator, config);
        errdefer storage.deinit(self.allocator);
        storage.setPoolId(id);
        try self.pools.append(self.allocator, storage);
        return id;
    }

    /// Insert a caller-constructed `KvStorage` into the pool list. Used by
    /// backends that want to provision a non-host storage (e.g. an
    /// MTLBuffer-backed pool) while still routing sequences through this
    /// manager. The manager takes ownership — `deinit` tears the storage
    /// down via its vtable.
    pub fn addStorage(self: *KvManager, storage: storage_mod.KvStorage) !block.KvPoolId {
        const id: block.KvPoolId = @intCast(self.pools.items.len);
        var owned = storage;
        owned.setPoolId(id);
        try self.pools.append(self.allocator, owned);
        return id;
    }

    pub fn attachSequence(self: *KvManager, pool_id: block.KvPoolId) !SequenceId {
        if (pool_id >= self.pools.items.len) return error.InvalidPoolId;
        const id: SequenceId = @intCast(self.sequences.items.len + 1);
        try self.sequences.append(self.allocator, .{
            .id = id,
            .pool_id = pool_id,
        });
        return id;
    }

    pub fn attachSequenceWithSharedPrefix(self: *KvManager, pool_id: block.KvPoolId, source_sequence_id: SequenceId, shared_token_count: usize) !SequenceId {
        const source_state = try self.sequenceState(source_sequence_id);
        if (source_state.pool_id != pool_id) return error.InvalidPoolId;
        const storage = self.getPoolMut(pool_id) orelse return error.InvalidPoolId;
        const page_size = storage.config.page_size_tokens;
        const shareable_blocks = source_state.block_table.fullBlockCount(page_size);
        const requested_blocks = shared_token_count / page_size;
        const shared_blocks = @min(shareable_blocks, requested_blocks);

        const sequence_id = try self.attachSequence(pool_id);
        const sequence_state = try self.sequenceMut(sequence_id);

        for (source_state.block_table.blocks.items[0..shared_blocks]) |block_id| {
            try storage.retain(block_id);
            try sequence_state.block_table.appendExisting(self.allocator, block_id);
        }
        sequence_state.block_table.markSharedPrefix(@intCast(shared_blocks));
        if (shared_blocks > 0) sequence_state.block_table.tail_tokens = page_size;
        return sequence_id;
    }

    pub fn reserveTailBlock(self: *KvManager, sequence_id: SequenceId) !block.KvBlockId {
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = &self.pools.items[sequence_state.pool_id];
        if (sequence_state.block_table.last()) |last_id| {
            if (sequence_state.block_table.tail_tokens < storage.config.page_size_tokens) return last_id;
        }
        const id = try storage.acquire(self.allocator);
        try sequence_state.block_table.append(self.allocator, id);
        return id;
    }

    pub fn appendTokens(self: *KvManager, sequence_id: SequenceId, count: u16) !void {
        var remaining = count;
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = &self.pools.items[sequence_state.pool_id];

        while (remaining > 0) {
            _ = try self.reserveTailBlock(sequence_id);
            const space = storage.config.page_size_tokens - sequence_state.block_table.tail_tokens;
            const consumed = @min(space, remaining);
            sequence_state.block_table.tail_tokens += consumed;
            remaining -= consumed;
        }
    }

    pub fn releaseSequence(self: *KvManager, sequence_id: SequenceId) !void {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        try self.releaseSequenceByIndex(idx);
    }

    pub fn tokenCount(self: *const KvManager, sequence_id: SequenceId) ?usize {
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        const seq_state = &self.sequences.items[idx];
        const storage = self.getPool(seq_state.pool_id) orelse return null;
        return seq_state.block_table.tokenCount(storage.config.page_size_tokens);
    }

    /// Number of additional blocks the sequence will need to hold an
    /// `additional_tokens` extension beyond its current tail. This is the
    /// per-item KV-block cost the scheduler consults to admit pending work
    /// against pool headroom.
    ///
    /// Returns 0 when no additional blocks are required (the existing tail
    /// has enough free slots) and accounts for partial-block tails so a
    /// single-token decode at a block boundary correctly costs one block.
    pub fn estimateBlocksFor(
        self: *const KvManager,
        sequence_id: SequenceId,
        additional_tokens: usize,
    ) ?usize {
        if (additional_tokens == 0) return 0;
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        const seq_state = &self.sequences.items[idx];
        const storage = self.getPool(seq_state.pool_id) orelse return null;
        const page_size: usize = storage.config.page_size_tokens;
        if (page_size == 0) return null;

        const tail_tokens: usize = seq_state.block_table.tail_tokens;
        const has_open_tail = seq_state.block_table.blocks.items.len > 0 and tail_tokens < page_size;
        const slack_in_tail: usize = if (has_open_tail) page_size - tail_tokens else 0;

        if (additional_tokens <= slack_in_tail) return 0;
        const overflow = additional_tokens - slack_in_tail;
        return (overflow + page_size - 1) / page_size;
    }

    /// Headroom in additional block acquires the pool can serve before
    /// exceeding its soft cap. Forwards to the storage vtable; returns null
    /// when the pool has no cap configured (treat as unbounded).
    pub fn poolAvailableBlocks(self: *const KvManager, pool_id: block.KvPoolId) ?usize {
        const storage = self.getPool(pool_id) orelse return null;
        return storage.availableBlocks();
    }

    /// Sets the soft block cap on the named pool. See
    /// `KvPool.setTargetMaxBlocks` for semantics.
    pub fn setPoolTargetMaxBlocks(
        self: *KvManager,
        pool_id: block.KvPoolId,
        target: ?usize,
    ) void {
        const storage = self.getPoolMut(pool_id) orelse return;
        storage.setTargetMaxBlocks(target);
    }

    pub fn writeFullLayerKv(
        self: *KvManager,
        sequence_id: SequenceId,
        layer_index: usize,
        token_count: usize,
        k_rows: []const f32,
        v_rows: []const f32,
    ) !void {
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = self.getPoolMut(sequence_state.pool_id) orelse return error.InvalidPoolId;
        const key_width = storage.keyValuesPerToken();
        const value_width = storage.valueValuesPerToken();
        if (k_rows.len != token_count * key_width or v_rows.len != token_count * value_width) return error.InvalidKvShape;
        if (token_count > sequence_state.block_table.tokenCount(storage.config.page_size_tokens)) return error.KvCapacityTooSmall;

        for (0..token_count) |token_idx| {
            const block_idx = token_idx / storage.config.page_size_tokens;
            const token_offset = token_idx % storage.config.page_size_tokens;
            const block_id = sequence_state.block_table.blocks.items[block_idx];
            const key_start = token_idx * key_width;
            const value_start = token_idx * value_width;
            try storage.writeToken(block_id, layer_index, token_offset, k_rows[key_start .. key_start + key_width], v_rows[value_start .. value_start + value_width]);
        }
    }

    pub fn writeLayerKvSuffix(
        self: *KvManager,
        sequence_id: SequenceId,
        layer_index: usize,
        total_token_count: usize,
        suffix_token_count: usize,
        k_rows: []const f32,
        v_rows: []const f32,
    ) !void {
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = self.getPoolMut(sequence_state.pool_id) orelse return error.InvalidPoolId;
        const key_width = storage.keyValuesPerToken();
        const value_width = storage.valueValuesPerToken();
        if (suffix_token_count > total_token_count) return error.InvalidKvShape;
        if (suffix_token_count == 0) return;
        const actual_key_width = k_rows.len / suffix_token_count;
        const actual_value_width = v_rows.len / suffix_token_count;
        if (k_rows.len != suffix_token_count * actual_key_width or v_rows.len != suffix_token_count * actual_value_width) return error.InvalidKvShape;
        if (actual_key_width > key_width or actual_value_width > value_width) return error.InvalidKvShape;
        if (total_token_count > sequence_state.block_table.tokenCount(storage.config.page_size_tokens)) return error.KvCapacityTooSmall;

        const start_token = total_token_count - suffix_token_count;

        if (actual_key_width == key_width and actual_value_width == value_width) {
            // Fast path: layer width matches pool width, no padding needed.
            for (0..suffix_token_count) |suffix_idx| {
                const token_idx = start_token + suffix_idx;
                const block_idx = token_idx / storage.config.page_size_tokens;
                const token_offset = token_idx % storage.config.page_size_tokens;
                const block_id = sequence_state.block_table.blocks.items[block_idx];
                const key_start = suffix_idx * key_width;
                const value_start = suffix_idx * value_width;
                try storage.writeToken(block_id, layer_index, token_offset, k_rows[key_start .. key_start + key_width], v_rows[value_start .. value_start + value_width]);
            }
        } else {
            // Per-layer GQA: layer K/V width is narrower than pool max. Zero-pad each token.
            const allocator = std.heap.page_allocator;
            const k_padded = try allocator.alloc(f32, key_width);
            defer allocator.free(k_padded);
            const v_padded = try allocator.alloc(f32, value_width);
            defer allocator.free(v_padded);
            @memset(k_padded, 0);
            @memset(v_padded, 0);

            for (0..suffix_token_count) |suffix_idx| {
                const token_idx = start_token + suffix_idx;
                const block_idx = token_idx / storage.config.page_size_tokens;
                const token_offset = token_idx % storage.config.page_size_tokens;
                const block_id = sequence_state.block_table.blocks.items[block_idx];
                const key_start = suffix_idx * actual_key_width;
                const value_start = suffix_idx * actual_value_width;
                @memcpy(k_padded[0..actual_key_width], k_rows[key_start .. key_start + actual_key_width]);
                @memcpy(v_padded[0..actual_value_width], v_rows[value_start .. value_start + actual_value_width]);
                try storage.writeToken(block_id, layer_index, token_offset, k_padded, v_padded);
                // Re-zero the padding region for next iteration (actual data region will be overwritten).
                @memset(k_padded[actual_key_width..], 0);
                @memset(v_padded[actual_value_width..], 0);
            }
        }
    }

    pub fn gatherLayerKv(
        self: *KvManager,
        allocator: std.mem.Allocator,
        sequence_id: SequenceId,
        layer_index: usize,
        token_count: usize,
    ) !struct { k: []f32, v: []f32 } {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        const seq_state = &self.sequences.items[idx];
        const storage = self.getPoolMut(seq_state.pool_id) orelse return error.InvalidPoolId;
        const key_width = storage.keyValuesPerToken();
        const value_width = storage.valueValuesPerToken();
        if (token_count > seq_state.block_table.tokenCount(storage.config.page_size_tokens)) return error.KvCapacityTooSmall;

        const k = try allocator.alloc(f32, token_count * key_width);
        errdefer allocator.free(k);
        const v = try allocator.alloc(f32, token_count * value_width);
        errdefer allocator.free(v);

        for (0..token_count) |token_idx| {
            const block_idx = token_idx / storage.config.page_size_tokens;
            const token_offset = token_idx % storage.config.page_size_tokens;
            const block_id = seq_state.block_table.blocks.items[block_idx];
            const row = try storage.readToken(block_id, layer_index, token_offset);
            const key_start = token_idx * key_width;
            const value_start = token_idx * value_width;
            @memcpy(k[key_start .. key_start + key_width], row.k);
            @memcpy(v[value_start .. value_start + value_width], row.v);
        }

        return .{ .k = k, .v = v };
    }

    /// Remove `count` tokens from the tail of a sequence's KV cache, releasing
    /// any fully emptied trailing blocks back to the pool.  Returns the number
    /// of tokens actually removed.
    pub fn truncateSequence(self: *KvManager, sequence_id: SequenceId, count: usize) !usize {
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = self.getPoolMut(sequence_state.pool_id) orelse return error.InvalidPoolId;
        const page_size = storage.config.page_size_tokens;
        const before = sequence_state.block_table.tokenCount(page_size);
        if (count == 0 or before == 0) return 0;

        const old_len = sequence_state.block_table.blocks.items.len;
        const excess_blocks: usize = blk: {
            if (count >= before) break :blk old_len;
            const target = before - count;
            const needed_blocks = (target + page_size - 1) / page_size;
            break :blk if (old_len > needed_blocks) old_len - needed_blocks else 0;
        };

        const dropped_block_ids: []const block.KvBlockId = if (excess_blocks > 0)
            try self.allocator.dupe(
                block.KvBlockId,
                sequence_state.block_table.blocks.items[old_len - excess_blocks .. old_len],
            )
        else
            &[_]block.KvBlockId{};
        defer if (excess_blocks > 0) self.allocator.free(dropped_block_ids);

        _ = sequence_state.block_table.dropTailTokens(page_size, count);
        for (dropped_block_ids) |block_id| {
            _ = try storage.releaseRef(self.allocator, block_id);
        }
        const after = sequence_state.block_table.tokenCount(page_size);
        return before - after;
    }

    pub fn trimSequenceToWindow(self: *KvManager, sequence_id: SequenceId, keep_tokens: usize) !usize {
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = self.getPoolMut(sequence_state.pool_id) orelse return error.InvalidPoolId;
        const page_size = storage.config.page_size_tokens;
        const current_tokens = sequence_state.block_table.tokenCount(page_size);
        if (keep_tokens >= current_tokens) return 0;

        const excess_tokens = current_tokens - keep_tokens;
        const droppable_blocks = @min(excess_tokens / page_size, sequence_state.block_table.fullBlockCount(page_size));
        if (droppable_blocks == 0) return 0;

        for (sequence_state.block_table.blocks.items[0..droppable_blocks]) |block_id| {
            _ = try storage.releaseRef(self.allocator, block_id);
        }
        sequence_state.block_table.dropFrontBlocks(droppable_blocks);
        return droppable_blocks * page_size;
    }

    pub fn trimSequenceToSlidingWindow(self: *KvManager, sequence_id: SequenceId) !usize {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        const seq_state = &self.sequences.items[idx];
        // Compacted sequences skip sliding window trimming — compaction replaces eviction.
        if (seq_state.compacted) return 0;
        const storage = self.getPool(seq_state.pool_id) orelse return error.InvalidPoolId;
        const keep_tokens = storage.config.sliding_window_size orelse return 0;
        return self.trimSequenceToWindow(sequence_id, keep_tokens);
    }

    pub fn blockTable(self: *KvManager, sequence_id: SequenceId) ?*const block_table.SequenceBlockTable {
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        return &self.sequences.items[idx].block_table;
    }

    pub fn getPool(self: *const KvManager, pool_id: block.KvPoolId) ?*const storage_mod.KvStorage {
        if (pool_id >= self.pools.items.len) return null;
        return &self.pools.items[pool_id];
    }

    pub fn getPoolMut(self: *KvManager, pool_id: block.KvPoolId) ?*storage_mod.KvStorage {
        if (pool_id >= self.pools.items.len) return null;
        return &self.pools.items[pool_id];
    }

    pub fn sequenceMut(self: *KvManager, sequence_id: SequenceId) !*SequenceState {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        return &self.sequences.items[idx];
    }

    fn sequenceState(self: *const KvManager, sequence_id: SequenceId) !*const SequenceState {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        return &self.sequences.items[idx];
    }

    fn releaseSequenceByIndex(self: *KvManager, idx: usize) !void {
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        const seq_state = &self.sequences.items[idx];
        if (seq_state.block_table.blocks.items.len == 0) {
            seq_state.block_table.reset();
            return;
        }

        const storage = self.getPoolMut(seq_state.pool_id) orelse return error.InvalidPoolId;
        for (seq_state.block_table.blocks.items) |block_id| {
            _ = try storage.releaseRef(self.allocator, block_id);
        }
        seq_state.block_table.reset();
    }
};

fn sequenceIndex(sequence_id: SequenceId) ?usize {
    if (sequence_id == 0) return null;
    return sequence_id - 1;
}

test "manager allocates paged tail blocks" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .metal,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    const sequence_id = try manager.attachSequence(pool_id);

    try manager.appendTokens(sequence_id, 3);
    {
        const table = manager.blockTable(sequence_id).?;
        try std.testing.expectEqual(@as(usize, 1), table.len());
        try std.testing.expectEqual(@as(u16, 3), table.tail_tokens);
    }

    try manager.appendTokens(sequence_id, 3);
    {
        const table = manager.blockTable(sequence_id).?;
        try std.testing.expectEqual(@as(usize, 2), table.len());
        try std.testing.expectEqual(@as(u16, 2), table.tail_tokens);
    }
}

test "manager truncateSequence releases dropped tail blocks without relying on backing storage" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_kv_heads = 2,
        .head_dim = 4,
    });
    const sequence_id = try manager.attachSequence(pool_id);

    try manager.appendTokens(sequence_id, 9);
    try std.testing.expectEqual(@as(?usize, 9), manager.tokenCount(sequence_id));
    const pool0 = manager.getPoolMut(pool_id).?.hostPool().?;
    try std.testing.expectEqual(@as(usize, 0), pool0.free_list.items.len);

    const removed = try manager.truncateSequence(sequence_id, 5);
    try std.testing.expectEqual(@as(usize, 5), removed);
    try std.testing.expectEqual(@as(?usize, 4), manager.tokenCount(sequence_id));
    const pool1 = manager.getPoolMut(pool_id).?.hostPool().?;
    try std.testing.expectEqual(@as(usize, 2), pool1.free_list.items.len);
}

test "manager stores and gathers kv rows" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const sequence_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(sequence_id, 3);

    try manager.writeFullLayerKv(
        sequence_id,
        1,
        3,
        &.{ 1, 2, 3, 4, 11, 12, 13, 14, 21, 22, 23, 24 },
        &.{ 5, 6, 7, 8, 15, 16, 17, 18, 25, 26, 27, 28 },
    );
    const gathered = try manager.gatherLayerKv(allocator, sequence_id, 1, 3);
    defer allocator.free(gathered.k);
    defer allocator.free(gathered.v);

    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 11, 12, 13, 14, 21, 22, 23, 24 }, gathered.k);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8, 15, 16, 17, 18, 25, 26, 27, 28 }, gathered.v);
}

test "manager pads and gathers asymmetric kv row widths" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 8,
        .key_values_per_token = 6,
        .value_values_per_token = 4,
    });
    const sequence_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(sequence_id, 2);

    try manager.writeLayerKvSuffix(
        sequence_id,
        0,
        2,
        2,
        &.{ 1, 2, 3, 11, 12, 13 },
        &.{ 4, 5, 14, 15 },
    );

    const gathered = try manager.gatherLayerKv(allocator, sequence_id, 0, 2);
    defer allocator.free(gathered.k);
    defer allocator.free(gathered.v);

    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 0, 0, 0, 11, 12, 13, 0, 0, 0 }, gathered.k);
    try std.testing.expectEqualSlices(f32, &.{ 4, 5, 0, 0, 14, 15, 0, 0 }, gathered.v);
}

test "manager can attach sequence with shared prefix blocks" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .metal,
        .dtype = .f16,
        .page_size_tokens = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const source_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(source_id, 5);

    const derived_id = try manager.attachSequenceWithSharedPrefix(pool_id, source_id, 4);
    const derived = manager.blockTable(derived_id).?;
    try std.testing.expectEqual(@as(usize, 2), derived.len());
    try std.testing.expectEqual(@as(u32, 2), derived.shared_prefix_blocks);
    try std.testing.expectEqual(@as(u16, 2), derived.tail_tokens);

    const pool = manager.getPool(pool_id).?;
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(1).?.refcount);
}

test "manager releases shared prefix blocks by refcount" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const source_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(source_id, 4);
    const derived_id = try manager.attachSequenceWithSharedPrefix(pool_id, source_id, 4);

    try manager.releaseSequence(derived_id);
    const pool = manager.getPool(pool_id).?;
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(1).?.refcount);

    try manager.releaseSequence(source_id);
    try std.testing.expectEqual(@as(u32, 0), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 0), pool.blockInfo(1).?.refcount);
}

test "manager trims sequence to sliding window in whole blocks" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .metal,
        .dtype = .f16,
        .page_size_tokens = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .sliding_window_size = 3,
    });
    const sequence_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(sequence_id, 6);

    const dropped = try manager.trimSequenceToSlidingWindow(sequence_id);
    try std.testing.expectEqual(@as(usize, 2), dropped);

    const table = manager.blockTable(sequence_id).?;
    try std.testing.expectEqual(@as(usize, 2), table.len());
    try std.testing.expectEqual(@as(u16, 2), table.tail_tokens);
    try std.testing.expectEqual(@as(usize, 4), manager.tokenCount(sequence_id).?);
}

test "estimateBlocksFor returns 0 when tail has slack" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 8,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const seq = try manager.attachSequence(pool_id);
    try manager.appendTokens(seq, 3);

    try std.testing.expectEqual(@as(usize, 0), manager.estimateBlocksFor(seq, 0).?);
    try std.testing.expectEqual(@as(usize, 0), manager.estimateBlocksFor(seq, 1).?);
    try std.testing.expectEqual(@as(usize, 0), manager.estimateBlocksFor(seq, 5).?);
}

test "estimateBlocksFor counts whole and partial overflow blocks" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 8,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const seq = try manager.attachSequence(pool_id);
    try manager.appendTokens(seq, 6);

    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(seq, 3).?);
    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(seq, 10).?);
    try std.testing.expectEqual(@as(usize, 2), manager.estimateBlocksFor(seq, 11).?);
    try std.testing.expectEqual(@as(usize, 2), manager.estimateBlocksFor(seq, 18).?);
    try std.testing.expectEqual(@as(usize, 3), manager.estimateBlocksFor(seq, 19).?);
}

test "estimateBlocksFor handles empty sequences and full tails" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const seq_empty = try manager.attachSequence(pool_id);
    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(seq_empty, 1).?);
    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(seq_empty, 4).?);
    try std.testing.expectEqual(@as(usize, 2), manager.estimateBlocksFor(seq_empty, 5).?);

    const seq_full = try manager.attachSequence(pool_id);
    try manager.appendTokens(seq_full, 4);
    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(seq_full, 1).?);
    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(seq_full, 4).?);
    try std.testing.expectEqual(@as(usize, 2), manager.estimateBlocksFor(seq_full, 5).?);
}

test "poolAvailableBlocks reflects target_max_blocks cap" {
    const allocator = std.testing.allocator;
    var manager = KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });

    try std.testing.expectEqual(@as(?usize, null), manager.poolAvailableBlocks(pool_id));

    manager.setPoolTargetMaxBlocks(pool_id, 8);
    try std.testing.expectEqual(@as(?usize, 8), manager.poolAvailableBlocks(pool_id));

    const seq = try manager.attachSequence(pool_id);
    try manager.appendTokens(seq, 12);
    // 12 tokens / 4 page_size = 3 blocks live; 8 - 3 = 5 left
    try std.testing.expectEqual(@as(?usize, 5), manager.poolAvailableBlocks(pool_id));

    manager.setPoolTargetMaxBlocks(pool_id, null);
    try std.testing.expectEqual(@as(?usize, null), manager.poolAvailableBlocks(pool_id));
}
