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

const std = @import("std");
const block = @import("block.zig");
const block_table = @import("block_table.zig");
const writable = @import("writable_blocks.zig");
const pool_mod = @import("pool.zig");
const storage_mod = @import("storage.zig");

pub const SequenceId = u32;

pub const SequenceState = struct {
    id: SequenceId,
    pool_id: block.KvPoolId,
    active: bool = true,
    block_table: block_table.SequenceBlockTable = .{},
    reserved_tail_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty,
    /// When true, this sequence holds compacted KV cache and sliding window
    /// trimming is skipped (compaction replaces eviction).
    compacted: bool = false,

    pub fn deinit(self: *SequenceState, allocator: std.mem.Allocator) void {
        self.block_table.deinit(allocator);
        self.reserved_tail_blocks.deinit(allocator);
    }
};

pub const KvManager = struct {
    allocator: std.mem.Allocator,
    pools: std.ArrayListUnmanaged(storage_mod.KvStorage) = .empty,
    sequences: std.ArrayListUnmanaged(SequenceState) = .empty,
    free_sequence_indices: std.ArrayListUnmanaged(usize) = .empty,

    pub fn init(allocator: std.mem.Allocator) KvManager {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *KvManager) void {
        for (self.sequences.items, 0..) |_, idx| {
            self.releaseSequenceByIndex(idx) catch {};
        }
        for (self.sequences.items) |*seq_state| seq_state.deinit(self.allocator);
        self.sequences.deinit(self.allocator);
        self.free_sequence_indices.deinit(self.allocator);
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
        if (self.recycledSequenceIndex()) |idx| {
            const id: SequenceId = @intCast(idx + 1);
            const seq_state = &self.sequences.items[idx];
            seq_state.id = id;
            seq_state.pool_id = pool_id;
            seq_state.active = true;
            seq_state.compacted = false;
            seq_state.block_table.reset();
            seq_state.reserved_tail_blocks.clearRetainingCapacity();
            return id;
        }
        const next = try std.math.add(usize, self.sequences.items.len, 1);
        const id = std.math.cast(SequenceId, next) orelse return error.KvSequenceLimitExceeded;
        // Reserve every future retirement slot before publishing this ID.
        // Valid retirement/reuse never allocates, including destructor cleanup.
        try self.free_sequence_indices.ensureTotalCapacity(self.allocator, next);
        try self.sequences.ensureUnusedCapacity(self.allocator, 1);
        self.sequences.appendAssumeCapacity(.{
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
        errdefer self.releaseSequence(sequence_id) catch {};
        const sequence_state = try self.sequenceMut(sequence_id);
        try sequence_state.block_table.blocks.ensureTotalCapacity(self.allocator, shared_blocks);
        const refreshed_source_state = try self.sequenceState(source_sequence_id);

        for (refreshed_source_state.block_table.blocks.items[0..shared_blocks]) |block_id| {
            try storage.retain(block_id);
            sequence_state.block_table.blocks.appendAssumeCapacity(block_id);
        }
        sequence_state.block_table.markSharedPrefix(@intCast(shared_blocks));
        if (shared_blocks > 0) sequence_state.block_table.tail_tokens = page_size;
        return sequence_id;
    }

    pub fn attachSequenceWithRetainedBlocks(self: *KvManager, pool_id: block.KvPoolId, block_ids: []const block.KvBlockId, token_count: usize) !SequenceId {
        const storage = self.getPoolMut(pool_id) orelse return error.InvalidPoolId;
        const page_size = storage.config.page_size_tokens;
        if (page_size == 0) return error.InvalidPoolId;
        const retained_token_count = std.math.mul(usize, block_ids.len, @as(usize, page_size)) catch return error.InvalidKvShape;
        if (token_count != retained_token_count) return error.InvalidKvShape;

        const sequence_id = try self.attachSequence(pool_id);
        errdefer self.releaseSequence(sequence_id) catch {};
        const sequence_state = try self.sequenceMut(sequence_id);
        try sequence_state.block_table.blocks.ensureTotalCapacity(self.allocator, block_ids.len);

        for (block_ids) |block_id| {
            try storage.retain(block_id);
            sequence_state.block_table.blocks.appendAssumeCapacity(block_id);
        }
        sequence_state.block_table.markSharedPrefix(@intCast(block_ids.len));
        if (block_ids.len > 0) sequence_state.block_table.tail_tokens = page_size;
        return sequence_id;
    }

    pub fn retainSequencePrefixBlocks(self: *KvManager, sequence_id: SequenceId, token_count: usize, out: *std.ArrayListUnmanaged(block.KvBlockId)) !void {
        const sequence_state = try self.sequenceState(sequence_id);
        const storage = self.getPoolMut(sequence_state.pool_id) orelse return error.InvalidPoolId;
        const page_size = storage.config.page_size_tokens;
        if (page_size == 0) return error.InvalidPoolId;
        if (token_count % page_size != 0) return error.InvalidKvShape;
        if (token_count > sequence_state.block_table.tokenCount(page_size)) return error.KvCapacityTooSmall;
        const block_count = token_count / page_size;

        out.clearRetainingCapacity();
        errdefer {
            for (out.items) |block_id| _ = storage.releaseRef(self.allocator, block_id) catch {};
            out.clearRetainingCapacity();
        }
        try out.ensureTotalCapacity(self.allocator, block_count);
        for (sequence_state.block_table.blocks.items[0..block_count]) |block_id| {
            try storage.retain(block_id);
            out.appendAssumeCapacity(block_id);
        }
    }

    pub fn releaseRetainedBlocks(self: *KvManager, pool_id: block.KvPoolId, block_ids: []const block.KvBlockId) void {
        const storage = self.getPoolMut(pool_id) orelse return;
        for (block_ids) |block_id| _ = storage.releaseRef(self.allocator, block_id) catch {};
    }

    pub fn reserveTailBlock(self: *KvManager, sequence_id: SequenceId) !block.KvBlockId {
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = &self.pools.items[sequence_state.pool_id];
        if (sequence_state.block_table.last() != null) {
            if (sequence_state.block_table.tail_tokens < storage.config.page_size_tokens) {
                try writable.ensureRange(storage, &sequence_state.block_table, &sequence_state.reserved_tail_blocks, self.allocator, (sequence_state.block_table.blocks.items.len - 1) * storage.config.page_size_tokens, 1, true);
                return sequence_state.block_table.last().?;
            }
        }
        if (sequence_state.reserved_tail_blocks.items.len > 0) {
            const id = sequence_state.reserved_tail_blocks.items[0];
            try sequence_state.block_table.append(self.allocator, id);
            std.mem.copyForwards(
                block.KvBlockId,
                sequence_state.reserved_tail_blocks.items[0 .. sequence_state.reserved_tail_blocks.items.len - 1],
                sequence_state.reserved_tail_blocks.items[1..],
            );
            sequence_state.reserved_tail_blocks.items.len -= 1;
            return id;
        }
        return writable.appendAcquired(storage, &sequence_state.block_table, self.allocator);
    }

    pub fn reserveTokenCapacity(self: *KvManager, sequence_id: SequenceId, token_capacity: usize) !void {
        if (token_capacity == 0) return;
        const sequence_state = try self.sequenceMut(sequence_id);
        const storage = &self.pools.items[sequence_state.pool_id];
        const page_size: usize = storage.config.page_size_tokens;
        if (page_size == 0) return error.InvalidPoolId;
        if (token_capacity <= sequence_state.block_table.tokenCount(@intCast(page_size))) return;
        const logical_blocks = std.math.divCeil(usize, token_capacity, page_size) catch return error.InvalidPoolId;
        const copies = writable.tailCopies(storage, &sequence_state.block_table) orelse return error.InvalidBlockId;
        const target_blocks = try std.math.add(usize, logical_blocks, copies);
        var available_blocks = sequence_state.block_table.blocks.items.len + sequence_state.reserved_tail_blocks.items.len;
        while (available_blocks < target_blocks) : (available_blocks += 1) {
            {
                try sequence_state.reserved_tail_blocks.ensureUnusedCapacity(self.allocator, 1);
                const id = try storage.acquire(self.allocator);
                sequence_state.reserved_tail_blocks.appendAssumeCapacity(id);
            }
        }
    }

    /// Metadata-only capacity already owned by this sequence, including its
    /// reserved tail. This grants no allocation/quota credit and never grows.
    pub fn reservedTokenCapacity(self: *const KvManager, sequence_id: SequenceId) !usize {
        const state = try self.sequenceState(sequence_id);
        const storage = self.getPool(state.pool_id) orelse return error.InvalidPoolId;
        const count = try std.math.add(usize, state.block_table.blocks.items.len, state.reserved_tail_blocks.items.len);
        return std.math.mul(usize, count, storage.config.page_size_tokens);
    }

    pub fn logicalBlocksWithReservations(
        self: *const KvManager,
        sequence_id: SequenceId,
        out: *std.ArrayListUnmanaged(block.KvBlockId),
    ) ![]const block.KvBlockId {
        return self.logicalBlocksWithReservationsWithAllocator(sequence_id, out, self.allocator);
    }
    /// The caller owns the destination's allocator and its eventual free.
    /// Manager storage ownership grants no allocation authority for that list.
    pub fn logicalBlocksWithReservationsWithAllocator(self: *const KvManager, sequence_id: SequenceId, out: *std.ArrayListUnmanaged(block.KvBlockId), destination_allocator: std.mem.Allocator) ![]const block.KvBlockId {
        const sequence_state = try self.sequenceState(sequence_id);
        out.clearRetainingCapacity();
        try out.appendSlice(destination_allocator, sequence_state.block_table.blocks.items);
        try out.appendSlice(destination_allocator, sequence_state.reserved_tail_blocks.items);
        return out.items;
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
        if (!seq_state.active) return null;
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

        const copies = writable.tailCopies(storage, &seq_state.block_table) orelse return null;
        if (additional_tokens <= slack_in_tail) return copies -| seq_state.reserved_tail_blocks.items.len;
        const overflow = additional_tokens - slack_in_tail;
        const required = std.math.add(usize, (std.math.divCeil(usize, overflow, page_size) catch return null), copies) catch return null;
        return required -| seq_state.reserved_tail_blocks.items.len;
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

        try writable.ensureRange(storage, &sequence_state.block_table, &sequence_state.reserved_tail_blocks, self.allocator, 0, token_count, true);

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
        try writable.ensureRange(storage, &sequence_state.block_table, &sequence_state.reserved_tail_blocks, self.allocator, start_token, suffix_token_count, true);

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

        const dropped_block_ids = sequence_state.block_table.dropTailTokens(page_size, count);
        // Own this table through the loop; storage callbacks must not reenter
        // and mutate it. Custom storage release errors still propagate.
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
        if (!self.sequences.items[idx].active) return error.InvalidSequenceId;
        return &self.sequences.items[idx];
    }

    fn sequenceState(self: *const KvManager, sequence_id: SequenceId) !*const SequenceState {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        if (!self.sequences.items[idx].active) return error.InvalidSequenceId;
        return &self.sequences.items[idx];
    }

    fn releaseSequenceByIndex(self: *KvManager, idx: usize) !void {
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        const seq_state = &self.sequences.items[idx];
        if (!seq_state.active) return;
        const storage = self.getPoolMut(seq_state.pool_id) orelse return error.InvalidPoolId;
        for (seq_state.block_table.blocks.items) |block_id| {
            _ = try storage.releaseRef(self.allocator, block_id);
        }
        for (seq_state.reserved_tail_blocks.items) |block_id| {
            _ = try storage.releaseRef(self.allocator, block_id);
        }
        seq_state.block_table.reset();
        seq_state.reserved_tail_blocks.clearRetainingCapacity();
        seq_state.compacted = false;
        seq_state.active = false;
        self.free_sequence_indices.appendAssumeCapacity(idx);
    }

    fn recycledSequenceIndex(self: *KvManager) ?usize {
        const idx = self.free_sequence_indices.pop() orelse return null;
        std.debug.assert(idx < self.sequences.items.len and !self.sequences.items[idx].active);
        return idx;
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

test "manager reserves future token capacity without advancing token count" {
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
    const sequence_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(sequence_id, 2);
    try manager.reserveTokenCapacity(sequence_id, 10);

    try std.testing.expectEqual(@as(?usize, 2), manager.tokenCount(sequence_id));
    try std.testing.expectEqual(@as(usize, 1), manager.blockTable(sequence_id).?.len());

    var logical_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer logical_blocks.deinit(allocator);
    _ = try manager.logicalBlocksWithReservations(sequence_id, &logical_blocks);
    try std.testing.expectEqual(@as(usize, 3), logical_blocks.items.len);

    try manager.appendTokens(sequence_id, 3);
    try std.testing.expectEqual(@as(?usize, 5), manager.tokenCount(sequence_id));
    try std.testing.expectEqual(@as(usize, 2), manager.blockTable(sequence_id).?.len());
    _ = try manager.logicalBlocksWithReservations(sequence_id, &logical_blocks);
    try std.testing.expectEqual(@as(usize, 3), logical_blocks.items.len);
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

test "manager can attach sequence with retained blocks" {
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

    var blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer blocks.deinit(allocator);
    try manager.retainSequencePrefixBlocks(source_id, 4, &blocks);
    defer manager.releaseRetainedBlocks(pool_id, blocks.items);

    try std.testing.expectError(
        error.InvalidKvShape,
        manager.attachSequenceWithRetainedBlocks(pool_id, blocks.items, 2),
    );

    const derived_id = try manager.attachSequenceWithRetainedBlocks(pool_id, blocks.items, 4);
    const pool = manager.getPool(pool_id).?;
    try std.testing.expectEqual(@as(u32, 3), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 3), pool.blockInfo(1).?.refcount);

    try manager.releaseSequence(derived_id);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(1).?.refcount);
}

test "manager retain paths do not leak block refs on allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var manager = KvManager.init(allocator);
    defer {
        failing.fail_index = std.math.maxInt(usize);
        failing.resize_fail_index = std.math.maxInt(usize);
        manager.deinit();
    }

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    const source_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(source_id, 4);

    // Recycle an empty sequence so the failure below is specifically the
    // destination block-table allocation, after attachSequence succeeds.
    const empty_id = try manager.attachSequence(pool_id);
    try manager.releaseSequence(empty_id);

    const pool = manager.getPool(pool_id).?;
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(1).?.refcount);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try std.testing.expectError(
        error.OutOfMemory,
        manager.attachSequenceWithSharedPrefix(pool_id, source_id, 4),
    );
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(?usize, null), manager.tokenCount(empty_id));
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(1).?.refcount);

    const source_blocks = manager.blockTable(source_id).?.blocks.items;
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try std.testing.expectError(
        error.OutOfMemory,
        manager.attachSequenceWithRetainedBlocks(pool_id, source_blocks, 4),
    );
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(?usize, null), manager.tokenCount(empty_id));
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(1).?.refcount);

    var retained: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer retained.deinit(allocator);
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try std.testing.expectError(
        error.OutOfMemory,
        manager.retainSequencePrefixBlocks(source_id, 4, &retained),
    );
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 0), retained.items.len);
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), pool.blockInfo(1).?.refcount);
}

test "manager reuses released sequence slots" {
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
    const first = try manager.attachSequence(pool_id);
    try manager.appendTokens(first, 2);
    try manager.releaseSequence(first);

    const second = try manager.attachSequence(pool_id);
    try std.testing.expectEqual(first, second);
    try std.testing.expectEqual(@as(usize, 1), manager.sequences.items.len);
    try std.testing.expectEqual(@as(?usize, 0), manager.tokenCount(second));
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

test "manager retires and reuses the reserved sequence recycler under forced allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var manager = KvManager.init(failing.allocator());
    defer manager.deinit();
    const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 4 });
    const first = try manager.attachSequence(pool_id);
    try std.testing.expect(manager.free_sequence_indices.capacity >= manager.sequences.items.len);
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try manager.releaseSequence(first);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), manager.free_sequence_indices.items.len);
    const next = try manager.attachSequence(pool_id);
    try std.testing.expectEqual(first, next);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), manager.sequences.items.len);
    try std.testing.expectEqual(@as(?usize, 0), manager.tokenCount(next));
}

test "manager shared prefix truncation and full rollback retire grown blocks under forced allocation failure" {
    const a = std.testing.allocator;
    var manager = KvManager.init(a);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 2 });
    const source = try manager.attachSequence(pool_id);
    try manager.appendTokens(source, 9);
    const shared = try manager.attachSequenceWithSharedPrefix(pool_id, source, 8);
    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0, .resize_fail_index = 0 });
    manager.allocator = failure.allocator();
    defer manager.allocator = a;
    try std.testing.expectEqual(@as(usize, 3), try manager.truncateSequence(shared, 3));
    try std.testing.expectEqual(@as(u32, 3), manager.blockTable(shared).?.shared_prefix_blocks);
    try std.testing.expectEqual(@as(usize, 5), try manager.truncateSequence(shared, 99));
    try std.testing.expectEqual(@as(usize, 9), try manager.truncateSequence(source, 99));
    try std.testing.expect(!failure.has_induced_failure);
    const storage = manager.getPoolMut(pool_id).?.hostPool().?;
    try std.testing.expectEqual(@as(usize, 5), storage.free_list.items.len);
    try std.testing.expectEqual(@as(?usize, 0), manager.tokenCount(source));
}

test "shared prefix truncate then supported suffix append preserves source KV values" {
    const a = std.testing.allocator;
    var manager = KvManager.init(a);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    const source = try manager.attachSequence(pool_id);
    try manager.appendTokens(source, 6);
    try manager.writeFullLayerKv(source, 0, 6, &.{ 10, 20, 30, 40, 50, 60 }, &.{ 11, 21, 31, 41, 51, 61 });
    const derived = try manager.attachSequenceWithSharedPrefix(pool_id, source, 6);
    try std.testing.expectEqual(@as(usize, 1), try manager.truncateSequence(derived, 1));
    try manager.appendTokens(derived, 1);
    try manager.writeLayerKvSuffix(derived, 0, 6, 1, &.{999}, &.{1000});
    const source_values = try manager.gatherLayerKv(a, source, 0, 6);
    defer a.free(source_values.k);
    defer a.free(source_values.v);
    try std.testing.expectEqualSlices(f32, &.{ 10, 20, 30, 40, 50, 60 }, source_values.k);
    try std.testing.expectEqualSlices(f32, &.{ 11, 21, 31, 41, 51, 61 }, source_values.v);
    const derived_values = try manager.gatherLayerKv(a, derived, 0, 6);
    defer a.free(derived_values.k);
    defer a.free(derived_values.v);
    try std.testing.expectEqualSlices(f32, &.{ 10, 20, 30, 40, 50, 999 }, derived_values.k);
    try std.testing.expectEqualSlices(f32, &.{ 11, 21, 31, 41, 51, 1000 }, derived_values.v);
}

test "manager failed tail metadata append leaves no orphaned acquired KV ref" {
    const a = std.testing.allocator;
    var failures: usize = 0;
    var success = false;
    for (0..32) |failure_index| {
        var manager = KvManager.init(a);
        defer manager.deinit();
        const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
        const sequence = try manager.attachSequence(pool_id);
        var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = failure_index, .resize_fail_index = 0 });
        manager.allocator = failure.allocator();
        defer manager.allocator = a;
        manager.appendTokens(sequence, 1) catch |err| {
            if (err != error.OutOfMemory) return err;
            failures += 1;
        };
        const sequence_state = try manager.sequenceMut(sequence);
        const storage = manager.getPoolMut(pool_id).?.hostPool().?;
        var live_refs: usize = 0;
        for (storage.blocks.items) |entry| live_refs += entry.meta.refcount;
        try std.testing.expectEqual(sequence_state.block_table.blocks.items.len + sequence_state.reserved_tail_blocks.items.len, live_refs);
        if (!failure.has_induced_failure) {
            success = true;
            break;
        }
    }
    try std.testing.expect(success and failures != 0);
}

fn exerciseSharedKvWrites(a: std.mem.Allocator) !void {
    var manager = KvManager.init(a);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 2, .num_kv_heads = 1, .head_dim = 1 });
    const source = try manager.attachSequence(pool_id);
    try manager.appendTokens(source, 6);
    try manager.writeFullLayerKv(source, 0, 6, &.{ 10, 20, 30, 40, 50, 60 }, &.{ 11, 21, 31, 41, 51, 61 });
    try manager.writeFullLayerKv(source, 1, 6, &.{ 110, 120, 130, 140, 150, 160 }, &.{ 111, 121, 131, 141, 151, 161 });
    var cached: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer cached.deinit(a);
    try manager.retainSequencePrefixBlocks(source, 6, &cached);
    defer manager.releaseRetainedBlocks(pool_id, cached.items);
    // The original sequence has no shared-prefix marker, yet its cache holds
    // real physical refs. Writing that source must protect the cached prefix.
    try std.testing.expectEqual(@as(u32, 0), manager.blockTable(source).?.shared_prefix_blocks);
    try manager.writeLayerKvSuffix(source, 0, 6, 1, &.{99}, &.{100});
    const storage = manager.getPoolMut(pool_id).?;
    const cached_row = try storage.readToken(cached.items[2], 0, 1);
    try std.testing.expectEqualSlices(f32, &.{60}, cached_row.k);
    try std.testing.expectEqualSlices(f32, &.{61}, cached_row.v);
    const derived = try manager.attachSequenceWithRetainedBlocks(pool_id, cached.items, 6);
    try std.testing.expectEqual(@as(usize, 1), try manager.truncateSequence(derived, 1));
    try std.testing.expectEqual(@as(usize, 1), manager.estimateBlocksFor(derived, 1).?);
    {
        var no_op = std.testing.FailingAllocator.init(a, .{ .fail_index = 0, .resize_fail_index = 0 });
        manager.allocator = no_op.allocator();
        defer manager.allocator = a;
        try manager.reserveTokenCapacity(derived, 5);
        try std.testing.expect(!no_op.has_induced_failure);
        try std.testing.expectEqual(@as(usize, 0), (try manager.sequenceMut(derived)).reserved_tail_blocks.items.len);
    }
    try manager.reserveTokenCapacity(derived, 6);
    try std.testing.expectEqual(@as(usize, 0), manager.estimateBlocksFor(derived, 1).?);
    try std.testing.expectEqual(@as(usize, 1), (try manager.sequenceMut(derived)).reserved_tail_blocks.items.len);
    storage.setTargetMaxBlocks(storage.liveBlocks());
    try std.testing.expectEqual(@as(?usize, 0), storage.availableBlocks());
    const physical_before = storage.liveBlocks();
    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0, .resize_fail_index = 0 });
    manager.allocator = failure.allocator();
    defer manager.allocator = a;
    // The exact pre-reserved unique page covers COW even at the physical cap;
    // source/derived/cache ownership does not require any new allocation.
    try manager.appendTokens(derived, 1);
    try manager.writeLayerKvSuffix(derived, 0, 6, 1, &.{999}, &.{1000});
    try std.testing.expect(!failure.has_induced_failure);
    try std.testing.expectEqual(physical_before, storage.liveBlocks());
    const source_values = try manager.gatherLayerKv(a, source, 0, 6);
    defer a.free(source_values.k);
    defer a.free(source_values.v);
    try std.testing.expectEqualSlices(f32, &.{ 10, 20, 30, 40, 50, 99 }, source_values.k);
    const derived_values = try manager.gatherLayerKv(a, derived, 0, 6);
    defer a.free(derived_values.k);
    defer a.free(derived_values.v);
    try std.testing.expectEqualSlices(f32, &.{ 10, 20, 30, 40, 50, 999 }, derived_values.k);
    const other_layer = try manager.gatherLayerKv(a, derived, 1, 6);
    defer a.free(other_layer.k);
    defer a.free(other_layer.v);
    // COW preserved every existing encoded layer, not only the one written.
    try std.testing.expectEqualSlices(f32, &.{ 110, 120, 130, 140, 150 }, other_layer.k[0..5]);
    const cached_again = try storage.readToken(cached.items[2], 1, 1);
    try std.testing.expectEqualSlices(f32, &.{160}, cached_again.k);
}

test "manager shared KV source and retained prefix writes preserve all layers with reserved COW capacity" {
    try exerciseSharedKvWrites(std.testing.allocator);
}
test "manager shared KV COW and retained owners unwind every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseSharedKvWrites, .{});
}

test "manager shared partial tail allocation failure leaves source and logical derived state unchanged" {
    const a = std.testing.allocator;
    var failures: usize = 0;
    var success = false;
    for (0..32) |index| {
        var manager = KvManager.init(a);
        defer manager.deinit();
        const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
        const source = try manager.attachSequence(pool_id);
        try manager.appendTokens(source, 6);
        try manager.writeFullLayerKv(source, 0, 6, &.{ 1, 2, 3, 4, 5, 6 }, &.{ 7, 8, 9, 10, 11, 12 });
        const derived = try manager.attachSequenceWithSharedPrefix(pool_id, source, 6);
        _ = try manager.truncateSequence(derived, 1);
        const original_tail = manager.blockTable(derived).?.last().?;
        var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = index });
        manager.allocator = failure.allocator();
        defer manager.allocator = a;
        manager.appendTokens(derived, 1) catch |err| {
            if (err != error.OutOfMemory) return err;
            failures += 1;
            try std.testing.expectEqual(@as(?usize, 5), manager.tokenCount(derived));
            try std.testing.expectEqual(original_tail, manager.blockTable(derived).?.last().?);
        };
        const source_values = try manager.gatherLayerKv(a, source, 0, 6);
        defer a.free(source_values.k);
        defer a.free(source_values.v);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6 }, source_values.k);
        var refs: usize = 0;
        for (manager.getPoolMut(pool_id).?.hostPool().?.blocks.items) |entry| refs += entry.meta.refcount;
        try std.testing.expectEqual(manager.blockTable(source).?.blocks.items.len + manager.blockTable(derived).?.blocks.items.len, refs);
        if (!failure.has_induced_failure) {
            success = true;
            break;
        }
    }
    try std.testing.expect(success and failures != 0);
}

fn exerciseRetiredSequenceMetadata(a: std.mem.Allocator) !void {
    var manager = KvManager.init(a);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    var ids: [17]SequenceId = undefined;
    for (&ids) |*id| {
        id.* = try manager.attachSequence(pool_id);
        try manager.appendTokens(id.*, 1);
    }
    try std.testing.expect(manager.free_sequence_indices.capacity >= manager.sequences.items.len);
    const selected = [_]usize{ 2, 15, 7, 0 };
    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0, .resize_fail_index = 0 });
    manager.allocator = failure.allocator();
    defer manager.allocator = a;
    for (0..3) |_| {
        for (selected) |index| {
            try manager.releaseSequence(ids[index]);
            try manager.releaseSequence(ids[index]); // idempotent, no duplicate ID
        }
        try std.testing.expectEqual(selected.len, manager.free_sequence_indices.items.len);
        for (0..selected.len) |index| {
            const selected_index = selected[selected.len - index - 1];
            const reused = try manager.attachSequence(pool_id);
            try std.testing.expectEqual(ids[selected_index], reused);
            try std.testing.expectEqual(@as(?usize, 0), manager.tokenCount(reused));
        }
        try std.testing.expectEqual(@as(usize, 0), manager.free_sequence_indices.items.len);
        try std.testing.expectEqual(@as(usize, 17), manager.sequences.items.len);
    }
    try std.testing.expect(!failure.has_induced_failure);
    // Reusing a retired slot preserves every unrelated live sequence.
    try std.testing.expectEqual(@as(?usize, 1), manager.tokenCount(ids[1]));
}

test "manager noncontiguous sequence retirement and reuse require no allocator calls" {
    try exerciseRetiredSequenceMetadata(std.testing.allocator);
}
test "manager sequence and recycler publication unwind every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseRetiredSequenceMetadata, .{});
}

test "manager cold recycler metadata failure publishes no sequence or ID gap" {
    const a = std.testing.allocator;
    for (0..2) |index| {
        var manager = KvManager.init(a);
        defer manager.deinit();
        const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
        var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = index });
        manager.allocator = failure.allocator();
        defer manager.allocator = a;
        try std.testing.expectError(error.OutOfMemory, manager.attachSequence(pool_id));
        try std.testing.expectEqual(@as(usize, 0), manager.sequences.items.len);
        try std.testing.expectEqual(@as(usize, 0), manager.free_sequence_indices.items.len);
        failure.fail_index = std.math.maxInt(usize);
        const id = try manager.attachSequence(pool_id);
        try std.testing.expectEqual(@as(SequenceId, 1), id);
        try std.testing.expect(manager.free_sequence_indices.capacity >= manager.sequences.items.len);
    }
}

test "manager custom retirement failure preserves the active sequence and is never recycled" {
    const a = std.testing.allocator;
    var manager = KvManager.init(a);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    const id = try manager.attachSequence(pool_id);
    try manager.appendTokens(id, 1);
    const storage = manager.getPoolMut(pool_id).?;
    const original = storage.vtable;
    var altered = original.*;
    const Refuse = struct {
        fn release(_: *anyopaque, _: std.mem.Allocator, _: block.KvBlockId) !bool {
            return error.CustomRetirementFailure;
        }
    };
    altered.releaseRef = Refuse.release;
    storage.vtable = &altered;
    defer storage.vtable = original;
    try std.testing.expectError(error.CustomRetirementFailure, manager.releaseSequence(id));
    try std.testing.expect((try manager.sequenceMut(id)).active);
    try std.testing.expectEqual(@as(usize, 0), manager.free_sequence_indices.items.len);
    storage.vtable = original;
    try manager.releaseSequence(id);
    try std.testing.expectEqual(@as(usize, 1), manager.free_sequence_indices.items.len);
}

test "manager reserved capacity query keeps visible count unchanged and rejects retired identity" {
    var manager = KvManager.init(std.testing.allocator);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    const sequence = try manager.attachSequence(pool);
    try std.testing.expectEqual(@as(usize, 0), try manager.reservedTokenCapacity(sequence));
    try manager.reserveTokenCapacity(sequence, 5);
    const visible = manager.tokenCount(sequence);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const original = manager.allocator;
    manager.allocator = failing.allocator();
    const capacity = try manager.reservedTokenCapacity(sequence);
    manager.allocator = original;
    try std.testing.expect(capacity >= 5);
    try std.testing.expectEqual(visible, manager.tokenCount(sequence));
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    try manager.releaseSequence(sequence);
    try std.testing.expectError(error.InvalidSequenceId, manager.reservedTokenCapacity(sequence));
}
