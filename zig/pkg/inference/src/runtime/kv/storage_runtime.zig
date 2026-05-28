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
const manager_mod = @import("manager.zig");
const pool_mod = @import("pool.zig");
const storage_mod = @import("storage.zig");

pub const SequenceId = manager_mod.SequenceId;

pub const SequenceState = struct {
    id: SequenceId,
    block_table: block_table.SequenceBlockTable = .{},
    compacted: bool = false,

    pub fn deinit(self: *SequenceState, allocator: std.mem.Allocator) void {
        self.block_table.deinit(allocator);
    }
};

/// Opaque device reference threaded through to backend-specific device-write
/// hooks. Backends interpret `handle` (e.g. Metal: MTLBuffer handle). `byte_len`
/// is a defensive cap — the impl verifies it matches the suffix bytes implied
/// by the KvSuffixWrite context.
pub const DeviceKvRef = struct {
    handle: *anyopaque,
    byte_offset: usize,
    byte_len: usize,
};

pub const KvSuffixWrite = struct {
    sequence_id: SequenceId,
    layer_index: usize,
    total_token_count: usize,
    suffix_token_count: usize,
    /// Absolute position of the first suffix token in the sequence — passed
    /// through to the kernel so it can tag the span with position_offset.
    position_offset: usize,
    num_kv_heads: u32,
    head_dim: u32,
    logical_blocks: ?[]const block.KvBlockId = null,
    page_size_tokens: u16 = 0,
};

pub const KvLayerGather = struct {
    sequence_id: SequenceId,
    layer_index: usize,
    token_count: usize,
    num_kv_heads: u32,
    head_dim: u32,
};

pub const DeviceKvLayerGather = KvLayerGather;

pub const DeviceKvLayerReserve = struct {
    sequence_id: SequenceId,
    layer_index: usize,
    token_capacity: usize,
    position_offset: usize,
    num_kv_heads: u32,
    head_dim: u32,
    logical_blocks: ?[]const block.KvBlockId = null,
    page_size_tokens: u16 = 0,
};

pub const DeviceKvLayer = struct {
    runtime: ?*anyopaque = null,
    k: DeviceKvRef,
    v: DeviceKvRef,
    token_count: usize,
    row_width: usize,
    position_offset: usize,
    value_element_bytes: usize,
};

pub const DevicePagedKvLayer = struct {
    runtime: ?*anyopaque = null,
    slot: usize,
    format: u32,
    token_count: usize,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    v_row_stride: usize,
    page_size_tokens: u16 = 0,
    position_offset: usize,
};

/// Backend-provided hook that services device-resident KV reads and writes
/// without going through the host KvPool. Installed by Metal provisioning;
/// absent on native paths.
///
/// - `writeLayerKvSuffix`: encode device-resident k/v into backend storage.
/// - `gatherLayerKv`: materialize host f32 rows from backend storage. Null
///   when the backend has no device-side read path, forcing callers to use
///   the default storage-backed gather. Returning `error.DeviceReadFallback`
///   tells callers to retry via the host path for this specific read.
pub const DeviceWriteHook = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        writeLayerKvSuffix: *const fn (
            ctx: *anyopaque,
            write: KvSuffixWrite,
            k: DeviceKvRef,
            v: DeviceKvRef,
        ) anyerror!void,
        /// Optional device-side gather. When null, `KvStorageRuntime.gatherLayerKv`
        /// falls back to the default readToken loop over the host KvPool.
        gatherLayerKv: ?*const fn (
            ctx: *anyopaque,
            gather: KvLayerGather,
            k_out: []f32,
            v_out: []f32,
        ) anyerror!void = null,
        /// Optional device-side raw f32 gather. This exposes backend-owned
        /// device buffers for decode kernels that can consume KV without
        /// materializing host rows. Backends return `DeviceReadFallback` when
        /// the layer is not represented as raw f32 device rows.
        gatherLayerKvDevice: ?*const fn (
            ctx: *anyopaque,
            gather: DeviceKvLayerGather,
        ) anyerror!DeviceKvLayer = null,
        /// Optional device-side paged/encoded gather. This exposes the
        /// backend-owned encoded KV slot and format metadata for attention
        /// kernels that can consume compressed or dtype-specific KV directly.
        pagedLayerKvDevice: ?*const fn (
            ctx: *anyopaque,
            gather: DeviceKvLayerGather,
        ) anyerror!DevicePagedKvLayer = null,
        /// Optional device-side capacity reservation. This lets runtimes grow
        /// persistent device KV buffers at sequence-mutation boundaries instead
        /// of reallocating from the attention hot path.
        reserveLayerKvDevice: ?*const fn (
            ctx: *anyopaque,
            reserve: DeviceKvLayerReserve,
        ) anyerror!void = null,
        /// Optional notification that a sequence has been released — the hook
        /// can reclaim any per-sequence resources (device slot reservations,
        /// cache entries, etc.). Called from `releaseSequence` before the
        /// sequence state is cleared. Null = no-op.
        releaseSequence: ?*const fn (ctx: *anyopaque, sequence_id: SequenceId) void = null,
        deinit: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) void,
    };

    pub fn writeLayerKvSuffix(
        self: DeviceWriteHook,
        write: KvSuffixWrite,
        k: DeviceKvRef,
        v: DeviceKvRef,
    ) !void {
        return self.vtable.writeLayerKvSuffix(self.ctx, write, k, v);
    }

    pub fn gatherLayerKv(
        self: DeviceWriteHook,
        gather: KvLayerGather,
        k_out: []f32,
        v_out: []f32,
    ) !void {
        const gather_fn = self.vtable.gatherLayerKv orelse return error.DeviceReadUnsupported;
        return gather_fn(self.ctx, gather, k_out, v_out);
    }

    pub fn gatherLayerKvDevice(self: DeviceWriteHook, gather: DeviceKvLayerGather) !DeviceKvLayer {
        const gather_fn = self.vtable.gatherLayerKvDevice orelse return error.DeviceReadUnsupported;
        return gather_fn(self.ctx, gather);
    }

    pub fn pagedLayerKvDevice(self: DeviceWriteHook, gather: DeviceKvLayerGather) !DevicePagedKvLayer {
        const gather_fn = self.vtable.pagedLayerKvDevice orelse return error.DeviceReadUnsupported;
        return gather_fn(self.ctx, gather);
    }

    pub fn reserveLayerKvDevice(self: DeviceWriteHook, reserve: DeviceKvLayerReserve) !void {
        const reserve_fn = self.vtable.reserveLayerKvDevice orelse return error.DeviceWriteUnsupported;
        return reserve_fn(self.ctx, reserve);
    }

    pub fn releaseSequence(self: DeviceWriteHook, sequence_id: SequenceId) void {
        const release_fn = self.vtable.releaseSequence orelse return;
        release_fn(self.ctx, sequence_id);
    }

    pub fn deinit(self: DeviceWriteHook, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ctx, allocator);
    }
};

pub const KvStorageRuntime = struct {
    allocator: std.mem.Allocator,
    storage: storage_mod.KvStorage,
    sequences: std.ArrayListUnmanaged(SequenceState) = .empty,
    /// Optional backend-provided fast path for device-resident suffix writes.
    /// When set, `writeLayerKvSuffixDevice` goes through the hook; otherwise
    /// callers must materialize host slices and use `writeLayerKvSuffix`.
    device_write_hook: ?DeviceWriteHook = null,

    pub fn init(allocator: std.mem.Allocator, config: pool_mod.KvPoolConfig) !KvStorageRuntime {
        var storage = try storage_mod.initHost(allocator, config);
        storage.setPoolId(0);
        return .{
            .allocator = allocator,
            .storage = storage,
        };
    }

    /// Construct a runtime over a caller-provided storage. The runtime takes
    /// ownership of the storage — deinit will tear it down.
    pub fn initWithStorage(allocator: std.mem.Allocator, storage: storage_mod.KvStorage) KvStorageRuntime {
        var owned = storage;
        owned.setPoolId(0);
        return .{
            .allocator = allocator,
            .storage = owned,
        };
    }

    pub fn deinit(self: *KvStorageRuntime) void {
        for (self.sequences.items, 0..) |_, idx| {
            self.releaseSequenceByIndex(idx) catch {};
        }
        for (self.sequences.items) |*seq_state| seq_state.deinit(self.allocator);
        self.sequences.deinit(self.allocator);
        if (self.device_write_hook) |hook| hook.deinit(self.allocator);
        self.storage.deinit(self.allocator);
    }

    /// Install a backend-specific device-write hook. The runtime takes
    /// ownership — `deinit` tears the hook down. Passing null clears the hook.
    pub fn setDeviceWriteHook(self: *KvStorageRuntime, hook: ?DeviceWriteHook) void {
        if (self.device_write_hook) |existing| existing.deinit(self.allocator);
        self.device_write_hook = hook;
    }

    /// Device-resident suffix write. Requires a device-write hook installed.
    /// Returns `error.DeviceWriteUnsupported` when no hook is present so
    /// callers can fall back to the host-slice path.
    pub fn writeLayerKvSuffixDevice(
        self: *KvStorageRuntime,
        write: KvSuffixWrite,
        k: DeviceKvRef,
        v: DeviceKvRef,
    ) !void {
        const hook = self.device_write_hook orelse return error.DeviceWriteUnsupported;
        const sequence_state = try self.sequenceMut(write.sequence_id);
        if (write.suffix_token_count > write.total_token_count) return error.InvalidKvShape;
        if (write.total_token_count > sequence_state.block_table.tokenCount(self.storage.config.page_size_tokens)) return error.KvCapacityTooSmall;
        var enriched = write;
        enriched.logical_blocks = sequence_state.block_table.blocks.items;
        enriched.page_size_tokens = self.storage.config.page_size_tokens;
        try hook.writeLayerKvSuffix(enriched, k, v);
    }

    pub fn reserveLayerKvDeviceCapacity(
        self: *KvStorageRuntime,
        sequence_id: SequenceId,
        layer_index: usize,
        token_capacity: usize,
        position_offset: usize,
        num_kv_heads: u32,
        head_dim: u32,
    ) !void {
        const hook = self.device_write_hook orelse return error.DeviceWriteUnsupported;
        if (token_capacity == 0) return;
        const sequence_state = try self.sequenceMut(sequence_id);
        if (token_capacity > sequence_state.block_table.tokenCount(self.storage.config.page_size_tokens)) return error.KvCapacityTooSmall;
        try hook.reserveLayerKvDevice(.{
            .sequence_id = sequence_id,
            .layer_index = layer_index,
            .token_capacity = token_capacity,
            .position_offset = position_offset,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .logical_blocks = sequence_state.block_table.blocks.items,
            .page_size_tokens = self.storage.config.page_size_tokens,
        });
    }

    pub fn poolId(self: *const KvStorageRuntime) block.KvPoolId {
        return self.storage.pool_id;
    }

    pub fn getPool(self: *const KvStorageRuntime, pool_id: block.KvPoolId) ?*const storage_mod.KvStorage {
        if (pool_id != self.storage.pool_id) return null;
        return &self.storage;
    }

    pub fn getPoolMut(self: *KvStorageRuntime, pool_id: block.KvPoolId) ?*storage_mod.KvStorage {
        if (pool_id != self.storage.pool_id) return null;
        return &self.storage;
    }

    pub fn attachSequence(self: *KvStorageRuntime, pool_id: block.KvPoolId) !SequenceId {
        if (pool_id != self.storage.pool_id) return error.InvalidPoolId;
        const id: SequenceId = @intCast(self.sequences.items.len + 1);
        try self.sequences.append(self.allocator, .{ .id = id });
        return id;
    }

    pub fn releaseSequence(self: *KvStorageRuntime, sequence_id: SequenceId) !void {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        try self.releaseSequenceByIndex(idx);
    }

    pub fn tokenCount(self: *const KvStorageRuntime, sequence_id: SequenceId) ?usize {
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        return self.sequences.items[idx].block_table.tokenCount(self.storage.config.page_size_tokens);
    }

    pub fn blockTable(self: *KvStorageRuntime, sequence_id: SequenceId) ?*const block_table.SequenceBlockTable {
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        return &self.sequences.items[idx].block_table;
    }

    pub fn sequenceMut(self: *KvStorageRuntime, sequence_id: SequenceId) !*SequenceState {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        return &self.sequences.items[idx];
    }

    pub fn reserveTailBlock(self: *KvStorageRuntime, sequence_id: SequenceId) !block.KvBlockId {
        const sequence_state = try self.sequenceMut(sequence_id);
        if (sequence_state.block_table.last()) |last_id| {
            if (sequence_state.block_table.tail_tokens < self.storage.config.page_size_tokens) return last_id;
        }
        const id = try self.storage.acquire(self.allocator);
        try sequence_state.block_table.append(self.allocator, id);
        return id;
    }

    pub fn appendTokens(self: *KvStorageRuntime, sequence_id: SequenceId, count: u16) !void {
        var remaining = count;
        const sequence_state = try self.sequenceMut(sequence_id);
        while (remaining > 0) {
            _ = try self.reserveTailBlock(sequence_id);
            const space = self.storage.config.page_size_tokens - sequence_state.block_table.tail_tokens;
            const consumed = @min(space, remaining);
            sequence_state.block_table.tail_tokens += consumed;
            remaining -= consumed;
        }
    }

    pub fn writeFullLayerKv(
        self: *KvStorageRuntime,
        sequence_id: SequenceId,
        layer_index: usize,
        token_count: usize,
        k_rows: []const f32,
        v_rows: []const f32,
    ) !void {
        const sequence_state = try self.sequenceMut(sequence_id);
        const key_width = self.storage.keyValuesPerToken();
        const value_width = self.storage.valueValuesPerToken();
        if (k_rows.len != token_count * key_width or v_rows.len != token_count * value_width) return error.InvalidKvShape;
        if (token_count > sequence_state.block_table.tokenCount(self.storage.config.page_size_tokens)) return error.KvCapacityTooSmall;

        for (0..token_count) |token_idx| {
            const block_idx = token_idx / self.storage.config.page_size_tokens;
            const token_offset = token_idx % self.storage.config.page_size_tokens;
            const block_id = sequence_state.block_table.blocks.items[block_idx];
            const key_start = token_idx * key_width;
            const value_start = token_idx * value_width;
            try self.storage.writeToken(block_id, layer_index, token_offset, k_rows[key_start .. key_start + key_width], v_rows[value_start .. value_start + value_width]);
        }
    }

    pub fn writeLayerKvSuffix(
        self: *KvStorageRuntime,
        sequence_id: SequenceId,
        layer_index: usize,
        total_token_count: usize,
        suffix_token_count: usize,
        k_rows: []const f32,
        v_rows: []const f32,
    ) !void {
        const sequence_state = try self.sequenceMut(sequence_id);
        const key_width = self.storage.keyValuesPerToken();
        const value_width = self.storage.valueValuesPerToken();
        if (suffix_token_count > total_token_count) return error.InvalidKvShape;
        if (suffix_token_count == 0) return;
        const actual_key_width = k_rows.len / suffix_token_count;
        const actual_value_width = v_rows.len / suffix_token_count;
        if (k_rows.len != suffix_token_count * actual_key_width or v_rows.len != suffix_token_count * actual_value_width) return error.InvalidKvShape;
        if (actual_key_width > key_width or actual_value_width > value_width) return error.InvalidKvShape;
        if (total_token_count > sequence_state.block_table.tokenCount(self.storage.config.page_size_tokens)) return error.KvCapacityTooSmall;

        const start_token = total_token_count - suffix_token_count;
        if (actual_key_width == key_width and actual_value_width == value_width) {
            for (0..suffix_token_count) |suffix_idx| {
                const token_idx = start_token + suffix_idx;
                const block_idx = token_idx / self.storage.config.page_size_tokens;
                const token_offset = token_idx % self.storage.config.page_size_tokens;
                const block_id = sequence_state.block_table.blocks.items[block_idx];
                const key_start = suffix_idx * key_width;
                const value_start = suffix_idx * value_width;
                try self.storage.writeToken(block_id, layer_index, token_offset, k_rows[key_start .. key_start + key_width], v_rows[value_start .. value_start + value_width]);
            }
        } else {
            const allocator = std.heap.page_allocator;
            const k_padded = try allocator.alloc(f32, key_width);
            defer allocator.free(k_padded);
            const v_padded = try allocator.alloc(f32, value_width);
            defer allocator.free(v_padded);
            @memset(k_padded, 0);
            @memset(v_padded, 0);

            for (0..suffix_token_count) |suffix_idx| {
                const token_idx = start_token + suffix_idx;
                const block_idx = token_idx / self.storage.config.page_size_tokens;
                const token_offset = token_idx % self.storage.config.page_size_tokens;
                const block_id = sequence_state.block_table.blocks.items[block_idx];
                const key_start = suffix_idx * actual_key_width;
                const value_start = suffix_idx * actual_value_width;
                @memcpy(k_padded[0..actual_key_width], k_rows[key_start .. key_start + actual_key_width]);
                @memcpy(v_padded[0..actual_value_width], v_rows[value_start .. value_start + actual_value_width]);
                try self.storage.writeToken(block_id, layer_index, token_offset, k_padded, v_padded);
                @memset(k_padded[actual_key_width..], 0);
                @memset(v_padded[actual_value_width..], 0);
            }
        }
    }

    pub fn gatherLayerKv(
        self: *KvStorageRuntime,
        allocator: std.mem.Allocator,
        sequence_id: SequenceId,
        layer_index: usize,
        token_count: usize,
    ) !struct { k: []f32, v: []f32 } {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        const seq_state = &self.sequences.items[idx];
        const key_width = self.storage.keyValuesPerToken();
        const value_width = self.storage.valueValuesPerToken();
        if (token_count > seq_state.block_table.tokenCount(self.storage.config.page_size_tokens)) return error.KvCapacityTooSmall;

        const k = try allocator.alloc(f32, token_count * key_width);
        errdefer allocator.free(k);
        const v = try allocator.alloc(f32, token_count * value_width);
        errdefer allocator.free(v);

        // Phase 6 fast path: if a device hook provides gatherLayerKv, use it
        // so the device-resident span buffer is the source of truth. Falls
        // back to the per-token readToken loop on `error.DeviceReadUnsupported`
        // or `error.DeviceReadFallback` from the hook.
        if (self.device_write_hook) |hook| blk: {
            hook.gatherLayerKv(.{
                .sequence_id = sequence_id,
                .layer_index = layer_index,
                .token_count = token_count,
                .num_kv_heads = self.storage.config.num_kv_heads,
                .head_dim = self.storage.config.head_dim,
            }, k, v) catch |err| switch (err) {
                error.DeviceReadUnsupported, error.DeviceReadFallback => break :blk,
                else => return err,
            };
            return .{ .k = k, .v = v };
        }

        for (0..token_count) |token_idx| {
            const block_idx = token_idx / self.storage.config.page_size_tokens;
            const token_offset = token_idx % self.storage.config.page_size_tokens;
            const block_id = seq_state.block_table.blocks.items[block_idx];
            const row = try self.storage.readToken(block_id, layer_index, token_offset);
            const key_start = token_idx * key_width;
            const value_start = token_idx * value_width;
            @memcpy(k[key_start .. key_start + key_width], row.k);
            @memcpy(v[value_start .. value_start + value_width], row.v);
        }

        return .{ .k = k, .v = v };
    }

    pub fn truncateSequence(self: *KvStorageRuntime, sequence_id: SequenceId, count: usize) !usize {
        const sequence_state = try self.sequenceMut(sequence_id);
        const page_size = self.storage.config.page_size_tokens;
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
            try self.allocator.dupe(block.KvBlockId, sequence_state.block_table.blocks.items[old_len - excess_blocks .. old_len])
        else
            &[_]block.KvBlockId{};
        defer if (excess_blocks > 0) self.allocator.free(dropped_block_ids);

        _ = sequence_state.block_table.dropTailTokens(page_size, count);
        for (dropped_block_ids) |block_id| {
            _ = try self.storage.releaseRef(self.allocator, block_id);
        }
        const after = sequence_state.block_table.tokenCount(page_size);
        return before - after;
    }

    pub fn trimSequenceToWindow(self: *KvStorageRuntime, sequence_id: SequenceId, keep_tokens: usize) !usize {
        const sequence_state = try self.sequenceMut(sequence_id);
        const page_size = self.storage.config.page_size_tokens;
        const before = sequence_state.block_table.tokenCount(page_size);
        if (keep_tokens >= before) return 0;

        const drop_tokens = before - keep_tokens;
        const drop_full_blocks = @min(drop_tokens / page_size, sequence_state.block_table.fullBlockCount(page_size));
        if (drop_full_blocks == 0) return 0;

        const dropped = try self.allocator.dupe(block.KvBlockId, sequence_state.block_table.blocks.items[0..drop_full_blocks]);
        defer self.allocator.free(dropped);
        std.mem.copyForwards(block.KvBlockId, sequence_state.block_table.blocks.items[0 .. sequence_state.block_table.blocks.items.len - drop_full_blocks], sequence_state.block_table.blocks.items[drop_full_blocks..]);
        sequence_state.block_table.blocks.items.len -= drop_full_blocks;
        for (dropped) |block_id| {
            _ = try self.storage.releaseRef(self.allocator, block_id);
        }
        return drop_full_blocks * page_size;
    }

    pub fn trimSequenceToSlidingWindow(self: *KvStorageRuntime, sequence_id: SequenceId) !usize {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        if (self.sequences.items[idx].compacted) return 0;
        const keep_tokens = self.storage.config.sliding_window_size orelse return 0;
        return self.trimSequenceToWindow(sequence_id, keep_tokens);
    }

    fn releaseSequenceByIndex(self: *KvStorageRuntime, idx: usize) !void {
        if (idx >= self.sequences.items.len) return;
        const seq_state = &self.sequences.items[idx];
        if (self.device_write_hook) |hook| hook.releaseSequence(seq_state.id);
        for (seq_state.block_table.blocks.items) |block_id| {
            _ = try self.storage.releaseRef(self.allocator, block_id);
        }
        seq_state.block_table.reset();
        seq_state.compacted = false;
    }
};

fn sequenceIndex(sequence_id: SequenceId) ?usize {
    if (sequence_id == 0) return null;
    return sequence_id - 1;
}

test "storage runtime pads and gathers asymmetric kv row widths" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 8,
        .key_values_per_token = 6,
        .value_values_per_token = 4,
    });
    defer runtime.deinit();

    const sequence_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(sequence_id, 2);

    try runtime.writeLayerKvSuffix(
        sequence_id,
        0,
        2,
        2,
        &.{ 1, 2, 3, 11, 12, 13 },
        &.{ 4, 5, 14, 15 },
    );

    const gathered = try runtime.gatherLayerKv(allocator, sequence_id, 0, 2);
    defer allocator.free(gathered.k);
    defer allocator.free(gathered.v);

    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 0, 0, 0, 11, 12, 13, 0, 0, 0 }, gathered.k);
    try std.testing.expectEqualSlices(f32, &.{ 4, 5, 0, 0, 14, 15, 0, 0 }, gathered.v);
}
