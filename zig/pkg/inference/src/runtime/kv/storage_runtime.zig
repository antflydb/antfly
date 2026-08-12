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

pub fn swaRingPageCount(
    page_size_tokens: u16,
    sliding_window: usize,
    max_inflight_tokens: usize,
    allow_swa_ring: bool,
) !usize {
    if (!allow_swa_ring or sliding_window == 0 or max_inflight_tokens == 0 or page_size_tokens == 0) return 0;
    const live_span = (std.math.add(usize, sliding_window, max_inflight_tokens) catch return error.KvCapacityTooSmall) - 1;
    // The oldest and newest live tokens can occupy partial pages. Include the
    // worst-case alignment slack so those pages never alias in the ring.
    const padded_span = std.math.add(usize, live_span, @as(usize, page_size_tokens) - 1) catch return error.KvCapacityTooSmall;
    return std.math.divCeil(usize, padded_span, page_size_tokens) catch return error.KvCapacityTooSmall;
}

pub fn physicalPageForLogical(logical_page: usize, ring_page_count: usize) usize {
    return if (ring_page_count > 0) logical_page % ring_page_count else logical_page;
}

/// A page-table result can borrow a reusable cyclic cache or own a one-shot
/// table for arbitrary physical block ids. Callers must always call `deinit`.
pub const PageTokenOffsets = union(enum) {
    borrowed: []const u32,
    owned: []u32,

    pub fn values(self: PageTokenOffsets) []const u32 {
        return switch (self) {
            .borrowed => |offsets| offsets,
            .owned => |offsets| offsets,
        };
    }

    pub fn deinit(self: PageTokenOffsets, allocator: std.mem.Allocator) void {
        switch (self) {
            .borrowed => {},
            .owned => |offsets| allocator.free(offsets),
        }
    }
};

/// Reuses the monotonically-growing logical-to-ring offset prefix for SWA.
/// The owning backend is single-threaded; returned borrowed slices remain
/// valid until its next cache request.
pub const CyclicPageTableCache = struct {
    offsets: std.ArrayListUnmanaged(u32) = .empty,
    page_size_tokens: usize = 0,
    ring_page_count: usize = 0,

    pub fn deinit(self: *CyclicPageTableCache, allocator: std.mem.Allocator) void {
        self.offsets.deinit(allocator);
        self.* = .{};
    }

    pub fn get(
        self: *CyclicPageTableCache,
        allocator: std.mem.Allocator,
        page_size_tokens: usize,
        ring_page_count: usize,
        needed_blocks: usize,
        cache_enabled: bool,
    ) !PageTokenOffsets {
        if (page_size_tokens == 0 or ring_page_count == 0 or needed_blocks == 0) return error.KvCapacityTooSmall;
        const largest_offset = std.math.mul(usize, ring_page_count - 1, page_size_tokens) catch return error.KvCapacityTooSmall;
        if (largest_offset > std.math.maxInt(u32)) return error.KvCapacityTooSmall;

        if (!cache_enabled) {
            const owned = try allocator.alloc(u32, needed_blocks);
            fillCyclicPageTokenOffsets(owned, page_size_tokens, ring_page_count, 0);
            return .{ .owned = owned };
        }

        const same_geometry = self.page_size_tokens == page_size_tokens and self.ring_page_count == ring_page_count;
        if (!same_geometry) {
            var replacement: std.ArrayListUnmanaged(u32) = .empty;
            errdefer replacement.deinit(allocator);
            try replacement.resize(allocator, needed_blocks);
            fillCyclicPageTokenOffsets(replacement.items, page_size_tokens, ring_page_count, 0);
            var old = self.offsets;
            self.offsets = replacement;
            self.page_size_tokens = page_size_tokens;
            self.ring_page_count = ring_page_count;
            old.deinit(allocator);
        } else if (needed_blocks > self.offsets.items.len) {
            const first_to_fill = self.offsets.items.len;
            try self.offsets.resize(allocator, needed_blocks);
            fillCyclicPageTokenOffsets(self.offsets.items, page_size_tokens, ring_page_count, first_to_fill);
        }
        return .{ .borrowed = self.offsets.items[0..needed_blocks] };
    }
};

fn fillCyclicPageTokenOffsets(offsets: []u32, page_size_tokens: usize, ring_page_count: usize, start: usize) void {
    for (offsets[start..], start..) |*offset, logical_page| {
        offset.* = @intCast(physicalPageForLogical(logical_page, ring_page_count) * page_size_tokens);
    }
}

/// Grow global/full-history KV buffers by 1.5x after the exact first
/// allocation. Later growth is page-aligned so all callers reserve the same
/// physical capacity.
pub fn geometricKvTokenCapacity(current: usize, required: usize, page_size_tokens: usize) !usize {
    if (required == 0 or required <= current) return @max(current, required);
    if (current == 0) return required;
    const half = current / 2 + current % 2;
    const grown = std.math.add(usize, current, half) catch return error.KvCapacityTooSmall;
    var target = @max(required, grown);
    if (page_size_tokens > 1) {
        const pages = std.math.divCeil(usize, target, page_size_tokens) catch return error.KvCapacityTooSmall;
        target = std.math.mul(usize, pages, page_size_tokens) catch return error.KvCapacityTooSmall;
    }
    return target;
}

pub const SequenceState = struct {
    id: SequenceId,
    active: bool = true,
    block_table: block_table.SequenceBlockTable = .{},
    reserved_tail_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty,
    compacted: bool = false,

    pub fn deinit(self: *SequenceState, allocator: std.mem.Allocator) void {
        self.block_table.deinit(allocator);
        self.reserved_tail_blocks.deinit(allocator);
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
    sliding_window: usize = 0,
    max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
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

pub const DeviceKvLayerGather = struct {
    sequence_id: SequenceId,
    layer_index: usize,
    token_count: usize,
    num_kv_heads: u32,
    head_dim: u32,
    /// A fused backend operation may need the paged slot descriptor before it
    /// encodes the suffix that makes the full logical span readable. This is
    /// valid only while the backend owns an ordered active frame. Ordinary
    /// gathers leave this at zero and therefore require full written coverage.
    pending_suffix_token_count: usize = 0,
};

pub const DeviceKvLayerWriteCommit = struct {
    sequence_id: SequenceId,
    layer_index: usize,
    total_token_count: usize,
    position_offset: usize,
};

pub const DeviceKvLayerReserve = struct {
    sequence_id: SequenceId,
    layer_index: usize,
    token_capacity: usize,
    position_offset: usize,
    num_kv_heads: u32,
    head_dim: u32,
    sliding_window: usize = 0,
    max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
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
    /// Non-zero when logical pages wrap through a fixed physical SWA ring.
    ring_page_count: usize = 0,
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
        /// Commit metadata for a suffix write encoded by a fused backend
        /// operation. The fused operation owns the device write; this callback
        /// advances readable coverage only after encoding succeeded.
        commitLayerKvDeviceWrite: ?*const fn (
            ctx: *anyopaque,
            commit: DeviceKvLayerWriteCommit,
        ) anyerror!void = null,
        /// Optional notification that a sequence tail was truncated. Device
        /// coverage metadata must shrink with the logical block table so a
        /// subsequent correction/retry can rewrite the rolled-back positions.
        truncateSequence: ?*const fn (
            ctx: *anyopaque,
            sequence_id: SequenceId,
            retained_token_count: usize,
        ) void = null,
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

    pub fn commitLayerKvDeviceWrite(self: DeviceWriteHook, commit: DeviceKvLayerWriteCommit) !void {
        const commit_fn = self.vtable.commitLayerKvDeviceWrite orelse return error.DeviceWriteUnsupported;
        return commit_fn(self.ctx, commit);
    }

    pub fn truncateSequence(self: DeviceWriteHook, sequence_id: SequenceId, retained_token_count: usize) void {
        const truncate_fn = self.vtable.truncateSequence orelse return;
        truncate_fn(self.ctx, sequence_id, retained_token_count);
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
    free_sequence_indices: std.ArrayListUnmanaged(usize) = .empty,
    logical_blocks_with_reservations: std.ArrayListUnmanaged(block.KvBlockId) = .empty,
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
        self.free_sequence_indices.deinit(self.allocator);
        self.logical_blocks_with_reservations.deinit(self.allocator);
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
        const page_size = self.storage.config.page_size_tokens;
        const reserved_capacity = (sequence_state.block_table.blocks.items.len + sequence_state.reserved_tail_blocks.items.len) * page_size;
        if (write.total_token_count > reserved_capacity) return error.KvCapacityTooSmall;
        var enriched = write;
        enriched.allow_swa_ring = enriched.allow_swa_ring and sequence_state.block_table.shared_prefix_blocks == 0;
        enriched.logical_blocks = try self.logicalBlocksWithReservations(write.sequence_id);
        enriched.page_size_tokens = page_size;
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
        sliding_window: usize,
        max_inflight_tokens: usize,
        allow_swa_ring: bool,
    ) !void {
        const hook = self.device_write_hook orelse return error.DeviceWriteUnsupported;
        if (token_capacity == 0) return;
        try self.reserveTokenCapacity(sequence_id, token_capacity);
        const sequence_state = try self.sequenceMut(sequence_id);
        try hook.reserveLayerKvDevice(.{
            .sequence_id = sequence_id,
            .layer_index = layer_index,
            .token_capacity = token_capacity,
            .position_offset = position_offset,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .sliding_window = sliding_window,
            .max_inflight_tokens = max_inflight_tokens,
            .allow_swa_ring = allow_swa_ring and sequence_state.block_table.shared_prefix_blocks == 0,
            .logical_blocks = try self.logicalBlocksWithReservations(sequence_id),
            .page_size_tokens = self.storage.config.page_size_tokens,
        });
    }

    /// Advance backend-readable coverage after a fused device operation has
    /// successfully encoded its KV suffix into the reserved slot.
    pub fn commitLayerKvDeviceWrite(
        self: *KvStorageRuntime,
        sequence_id: SequenceId,
        layer_index: usize,
        total_token_count: usize,
        position_offset: usize,
    ) !void {
        const hook = self.device_write_hook orelse return error.DeviceWriteUnsupported;
        try hook.commitLayerKvDeviceWrite(.{
            .sequence_id = sequence_id,
            .layer_index = layer_index,
            .total_token_count = total_token_count,
            .position_offset = position_offset,
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
        if (self.recycledSequenceIndex()) |idx| {
            const id: SequenceId = @intCast(idx + 1);
            const seq_state = &self.sequences.items[idx];
            seq_state.id = id;
            seq_state.active = true;
            seq_state.compacted = false;
            seq_state.block_table.reset();
            seq_state.reserved_tail_blocks.clearRetainingCapacity();
            return id;
        }
        const id: SequenceId = @intCast(self.sequences.items.len + 1);
        try self.sequences.append(self.allocator, .{ .id = id });
        return id;
    }

    pub fn attachSequenceWithRetainedBlocks(self: *KvStorageRuntime, pool_id: block.KvPoolId, block_ids: []const block.KvBlockId, token_count: usize) !SequenceId {
        if (pool_id != self.storage.pool_id) return error.InvalidPoolId;
        const page_size = self.storage.config.page_size_tokens;
        if (page_size == 0) return error.InvalidPoolId;
        const retained_token_count = std.math.mul(usize, block_ids.len, @as(usize, page_size)) catch return error.InvalidKvShape;
        if (token_count != retained_token_count) return error.InvalidKvShape;

        const sequence_id = try self.attachSequence(pool_id);
        errdefer self.releaseSequence(sequence_id) catch {};
        const sequence_state = try self.sequenceMut(sequence_id);
        try sequence_state.block_table.blocks.ensureTotalCapacity(self.allocator, block_ids.len);

        for (block_ids) |block_id| {
            try self.storage.retain(block_id);
            sequence_state.block_table.blocks.appendAssumeCapacity(block_id);
        }
        sequence_state.block_table.markSharedPrefix(@intCast(block_ids.len));
        if (block_ids.len > 0) sequence_state.block_table.tail_tokens = page_size;
        return sequence_id;
    }

    pub fn releaseSequence(self: *KvStorageRuntime, sequence_id: SequenceId) !void {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        try self.releaseSequenceByIndex(idx);
    }

    pub fn tokenCount(self: *const KvStorageRuntime, sequence_id: SequenceId) ?usize {
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        if (!self.sequences.items[idx].active) return null;
        return self.sequences.items[idx].block_table.tokenCount(self.storage.config.page_size_tokens);
    }

    pub fn retainSequencePrefixBlocks(self: *KvStorageRuntime, sequence_id: SequenceId, token_count: usize, out: *std.ArrayListUnmanaged(block.KvBlockId)) !void {
        const sequence_state = try self.sequenceMut(sequence_id);
        const page_size = self.storage.config.page_size_tokens;
        if (page_size == 0) return error.InvalidPoolId;
        if (token_count % page_size != 0) return error.InvalidKvShape;
        if (token_count > sequence_state.block_table.tokenCount(page_size)) return error.KvCapacityTooSmall;
        const block_count = token_count / page_size;

        out.clearRetainingCapacity();
        errdefer {
            for (out.items) |block_id| _ = self.storage.releaseRef(self.allocator, block_id) catch {};
            out.clearRetainingCapacity();
        }
        try out.ensureTotalCapacity(self.allocator, block_count);
        for (sequence_state.block_table.blocks.items[0..block_count]) |block_id| {
            try self.storage.retain(block_id);
            out.appendAssumeCapacity(block_id);
        }
    }

    pub fn releaseRetainedBlocks(self: *KvStorageRuntime, block_ids: []const block.KvBlockId) void {
        for (block_ids) |block_id| _ = self.storage.releaseRef(self.allocator, block_id) catch {};
    }

    pub fn blockTable(self: *KvStorageRuntime, sequence_id: SequenceId) ?*const block_table.SequenceBlockTable {
        const idx = sequenceIndex(sequence_id) orelse return null;
        if (idx >= self.sequences.items.len) return null;
        if (!self.sequences.items[idx].active) return null;
        return &self.sequences.items[idx].block_table;
    }

    pub fn sequenceMut(self: *KvStorageRuntime, sequence_id: SequenceId) !*SequenceState {
        const idx = sequenceIndex(sequence_id) orelse return error.InvalidSequenceId;
        if (idx >= self.sequences.items.len) return error.InvalidSequenceId;
        if (!self.sequences.items[idx].active) return error.InvalidSequenceId;
        return &self.sequences.items[idx];
    }

    pub fn reserveTailBlock(self: *KvStorageRuntime, sequence_id: SequenceId) !block.KvBlockId {
        const sequence_state = try self.sequenceMut(sequence_id);
        if (sequence_state.block_table.last()) |last_id| {
            if (sequence_state.block_table.tail_tokens < self.storage.config.page_size_tokens) return last_id;
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
        const id = try self.storage.acquire(self.allocator);
        try sequence_state.block_table.append(self.allocator, id);
        return id;
    }

    pub fn reserveTokenCapacity(self: *KvStorageRuntime, sequence_id: SequenceId, token_capacity: usize) !void {
        if (token_capacity == 0) return;
        const sequence_state = try self.sequenceMut(sequence_id);
        const page_size: usize = self.storage.config.page_size_tokens;
        if (page_size == 0) return error.InvalidPoolId;
        const target_blocks = std.math.divCeil(usize, token_capacity, page_size) catch return error.InvalidPoolId;
        var available_blocks = sequence_state.block_table.blocks.items.len + sequence_state.reserved_tail_blocks.items.len;
        while (available_blocks < target_blocks) : (available_blocks += 1) {
            {
                const id = try self.storage.acquire(self.allocator);
                errdefer _ = self.storage.releaseRef(self.allocator, id) catch {};
                try sequence_state.reserved_tail_blocks.append(self.allocator, id);
            }
        }
    }

    pub fn logicalBlocksWithReservations(self: *KvStorageRuntime, sequence_id: SequenceId) ![]const block.KvBlockId {
        const sequence_state = try self.sequenceMut(sequence_id);
        self.logical_blocks_with_reservations.clearRetainingCapacity();
        try self.logical_blocks_with_reservations.appendSlice(self.allocator, sequence_state.block_table.blocks.items);
        try self.logical_blocks_with_reservations.appendSlice(self.allocator, sequence_state.reserved_tail_blocks.items);
        return self.logical_blocks_with_reservations.items;
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
        const after = sequence_state.block_table.tokenCount(page_size);
        if (self.device_write_hook) |hook| hook.truncateSequence(sequence_id, after);
        for (dropped_block_ids) |block_id| {
            _ = try self.storage.releaseRef(self.allocator, block_id);
        }
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
        if (!self.sequences.items[idx].active) return error.InvalidSequenceId;
        if (self.sequences.items[idx].compacted) return 0;
        const keep_tokens = self.storage.config.sliding_window_size orelse return 0;
        return self.trimSequenceToWindow(sequence_id, keep_tokens);
    }

    fn releaseSequenceByIndex(self: *KvStorageRuntime, idx: usize) !void {
        if (idx >= self.sequences.items.len) return;
        const seq_state = &self.sequences.items[idx];
        if (!seq_state.active) return;
        if (self.device_write_hook) |hook| hook.releaseSequence(seq_state.id);
        for (seq_state.block_table.blocks.items) |block_id| {
            _ = try self.storage.releaseRef(self.allocator, block_id);
        }
        for (seq_state.reserved_tail_blocks.items) |block_id| {
            _ = try self.storage.releaseRef(self.allocator, block_id);
        }
        seq_state.block_table.reset();
        seq_state.reserved_tail_blocks.clearRetainingCapacity();
        seq_state.compacted = false;
        seq_state.active = false;
        self.free_sequence_indices.append(self.allocator, idx) catch {};
    }

    fn recycledSequenceIndex(self: *KvStorageRuntime) ?usize {
        while (self.free_sequence_indices.pop()) |idx| {
            if (idx < self.sequences.items.len and !self.sequences.items[idx].active) return idx;
        }
        // ponytail: fallback scan covers OOM while recording a released slot; keep a real freelist if this ever profiles hot.
        for (self.sequences.items, 0..) |seq_state, idx| {
            if (!seq_state.active) return idx;
        }
        return null;
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

test "storage runtime reserves future token capacity without advancing token count" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer runtime.deinit();

    const sequence_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(sequence_id, 2);
    try runtime.reserveTokenCapacity(sequence_id, 10);

    try std.testing.expectEqual(@as(?usize, 2), runtime.tokenCount(sequence_id));
    try std.testing.expectEqual(@as(usize, 1), runtime.blockTable(sequence_id).?.len());

    const logical_blocks = try runtime.logicalBlocksWithReservations(sequence_id);
    try std.testing.expectEqual(@as(usize, 3), logical_blocks.len);

    try runtime.appendTokens(sequence_id, 3);
    try std.testing.expectEqual(@as(?usize, 5), runtime.tokenCount(sequence_id));
    try std.testing.expectEqual(@as(usize, 2), runtime.blockTable(sequence_id).?.len());

    const logical_blocks_after_append = try runtime.logicalBlocksWithReservations(sequence_id);
    try std.testing.expectEqual(@as(usize, 3), logical_blocks_after_append.len);
}

test "storage runtime can attach sequence with retained blocks" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .metal,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer runtime.deinit();

    const source_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(source_id, 4);

    var blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer blocks.deinit(allocator);
    try runtime.retainSequencePrefixBlocks(source_id, 4, &blocks);
    defer runtime.releaseRetainedBlocks(blocks.items);

    try std.testing.expectError(
        error.InvalidKvShape,
        runtime.attachSequenceWithRetainedBlocks(runtime.poolId(), blocks.items, 2),
    );

    const derived_id = try runtime.attachSequenceWithRetainedBlocks(runtime.poolId(), blocks.items, 4);
    const storage = runtime.getPool(runtime.poolId()).?;
    try std.testing.expectEqual(@as(u32, 3), storage.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 3), storage.blockInfo(1).?.refcount);

    try runtime.releaseSequence(derived_id);
    try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(1).?.refcount);
}

test "storage runtime retain paths do not leak block refs on allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer {
        failing.fail_index = std.math.maxInt(usize);
        failing.resize_fail_index = std.math.maxInt(usize);
        runtime.deinit();
    }

    const source_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(source_id, 4);
    const empty_id = try runtime.attachSequence(runtime.poolId());
    try runtime.releaseSequence(empty_id);

    const storage = runtime.getPool(runtime.poolId()).?;
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(1).?.refcount);

    const source_blocks = runtime.blockTable(source_id).?.blocks.items;
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try std.testing.expectError(
        error.OutOfMemory,
        runtime.attachSequenceWithRetainedBlocks(runtime.poolId(), source_blocks, 4),
    );
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(?usize, null), runtime.tokenCount(empty_id));
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(1).?.refcount);

    var retained: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer retained.deinit(allocator);
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try std.testing.expectError(
        error.OutOfMemory,
        runtime.retainSequencePrefixBlocks(source_id, 4, &retained),
    );
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 0), retained.items.len);
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(1).?.refcount);
}

test "storage runtime reuses released sequence slots" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer runtime.deinit();

    const first = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(first, 2);
    try runtime.releaseSequence(first);

    const second = try runtime.attachSequence(runtime.poolId());
    try std.testing.expectEqual(first, second);
    try std.testing.expectEqual(@as(usize, 1), runtime.sequences.items.len);
    try std.testing.expectEqual(@as(?usize, 0), runtime.tokenCount(second));
}

const TestDeviceWriteHookContext = struct {
    calls: usize = 0,
    last_total_token_count: usize = 0,
    last_logical_block_count: usize = 0,
    last_page_size_tokens: u16 = 0,
    last_allow_swa_ring: bool = false,
    truncate_calls: usize = 0,
    last_truncated_sequence_id: SequenceId = 0,
    last_retained_token_count: usize = 0,
};

fn testDeviceWriteLayerKvSuffix(ctx: *anyopaque, write: KvSuffixWrite, _: DeviceKvRef, _: DeviceKvRef) anyerror!void {
    const typed: *TestDeviceWriteHookContext = @ptrCast(@alignCast(ctx));
    typed.calls += 1;
    typed.last_total_token_count = write.total_token_count;
    typed.last_logical_block_count = if (write.logical_blocks) |blocks| blocks.len else 0;
    typed.last_page_size_tokens = write.page_size_tokens;
    typed.last_allow_swa_ring = write.allow_swa_ring;
}

fn testDeviceWriteHookDeinit(_: *anyopaque, _: std.mem.Allocator) void {}

fn testDeviceTruncateSequence(ctx: *anyopaque, sequence_id: SequenceId, retained_token_count: usize) void {
    const typed: *TestDeviceWriteHookContext = @ptrCast(@alignCast(ctx));
    typed.truncate_calls += 1;
    typed.last_truncated_sequence_id = sequence_id;
    typed.last_retained_token_count = retained_token_count;
}

const test_device_write_hook_vtable = DeviceWriteHook.VTable{
    .writeLayerKvSuffix = testDeviceWriteLayerKvSuffix,
    .truncateSequence = testDeviceTruncateSequence,
    .deinit = testDeviceWriteHookDeinit,
};

test "storage runtime notifies device hooks after tail truncation" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer runtime.deinit();

    var hook_context = TestDeviceWriteHookContext{};
    runtime.setDeviceWriteHook(.{
        .ctx = &hook_context,
        .vtable = &test_device_write_hook_vtable,
    });

    const sequence_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(sequence_id, 7);
    try std.testing.expectEqual(@as(usize, 3), try runtime.truncateSequence(sequence_id, 3));
    try std.testing.expectEqual(@as(usize, 1), hook_context.truncate_calls);
    try std.testing.expectEqual(sequence_id, hook_context.last_truncated_sequence_id);
    try std.testing.expectEqual(@as(usize, 4), hook_context.last_retained_token_count);
}

test "storage runtime device writes count reserved blocks as capacity" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer runtime.deinit();

    var hook_context = TestDeviceWriteHookContext{};
    runtime.setDeviceWriteHook(.{
        .ctx = &hook_context,
        .vtable = &test_device_write_hook_vtable,
    });

    const sequence_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(sequence_id, 2);
    try runtime.reserveTokenCapacity(sequence_id, 10);

    var k_value: f32 = 1.0;
    var v_value: f32 = 2.0;
    const ref_len = @sizeOf(f32);
    try runtime.writeLayerKvSuffixDevice(
        .{
            .sequence_id = sequence_id,
            .layer_index = 0,
            .total_token_count = 10,
            .suffix_token_count = 1,
            .position_offset = 0,
            .num_kv_heads = 1,
            .head_dim = 1,
            .allow_swa_ring = true,
        },
        .{ .handle = @ptrCast(&k_value), .byte_offset = 0, .byte_len = ref_len },
        .{ .handle = @ptrCast(&v_value), .byte_offset = 0, .byte_len = ref_len },
    );

    try std.testing.expectEqual(@as(usize, 1), hook_context.calls);
    try std.testing.expectEqual(@as(usize, 10), hook_context.last_total_token_count);
    try std.testing.expectEqual(@as(usize, 3), hook_context.last_logical_block_count);
    try std.testing.expectEqual(@as(u16, 4), hook_context.last_page_size_tokens);
    try std.testing.expect(hook_context.last_allow_swa_ring);
}

test "SWA ring geometry includes the largest in-flight suffix" {
    try std.testing.expectEqual(@as(usize, 0), try swaRingPageCount(128, 512, 128, false));
    try std.testing.expectEqual(@as(usize, 0), try swaRingPageCount(128, 0, 128, true));
    try std.testing.expectEqual(@as(usize, 6), try swaRingPageCount(128, 512, 128, true));
    try std.testing.expectEqual(@as(usize, 9), try swaRingPageCount(128, 512, 512, true));
    try std.testing.expectEqual(@as(usize, 41), try swaRingPageCount(16, 512, 128, true));

    try std.testing.expectEqual(@as(usize, 0), physicalPageForLogical(0, 5));
    try std.testing.expectEqual(@as(usize, 4), physicalPageForLogical(4, 5));
    try std.testing.expectEqual(@as(usize, 0), physicalPageForLogical(5, 5));
    try std.testing.expectEqual(@as(usize, 1), physicalPageForLogical(6, 5));
    try std.testing.expectEqual(@as(usize, 6), physicalPageForLogical(6, 0));
}

test "cyclic page table cache reuses and extends ring offsets" {
    const allocator = std.testing.allocator;
    var cache: CyclicPageTableCache = .{};
    defer cache.deinit(allocator);

    const first = try cache.get(allocator, 16, 3, 4, true);
    defer first.deinit(allocator);
    try std.testing.expect(first == .borrowed);
    try std.testing.expectEqualSlices(u32, &.{ 0, 16, 32, 0 }, first.values());

    const extended = try cache.get(allocator, 16, 3, 7, true);
    defer extended.deinit(allocator);
    try std.testing.expect(extended == .borrowed);
    try std.testing.expectEqualSlices(u32, &.{ 0, 16, 32, 0, 16, 32, 0 }, extended.values());

    const shorter = try cache.get(allocator, 16, 3, 2, true);
    defer shorter.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 7), cache.offsets.items.len);
    try std.testing.expectEqualSlices(u32, &.{ 0, 16 }, shorter.values());

    const changed = try cache.get(allocator, 8, 2, 5, true);
    defer changed.deinit(allocator);
    try std.testing.expectEqualSlices(u32, &.{ 0, 8, 0, 8, 0 }, changed.values());

    const uncached = try cache.get(allocator, 16, 3, 2, false);
    defer uncached.deinit(allocator);
    try std.testing.expect(uncached == .owned);
    try std.testing.expectEqualSlices(u32, &.{ 0, 16 }, uncached.values());
}

test "global KV capacity grows geometrically and page aligns" {
    try std.testing.expectEqual(@as(usize, 100), try geometricKvTokenCapacity(0, 100, 16));
    try std.testing.expectEqual(@as(usize, 160), try geometricKvTokenCapacity(100, 101, 16));
    try std.testing.expectEqual(@as(usize, 160), try geometricKvTokenCapacity(160, 150, 16));
    try std.testing.expectEqual(@as(usize, 240), try geometricKvTokenCapacity(160, 200, 16));
    try std.testing.expectError(error.KvCapacityTooSmall, geometricKvTokenCapacity(std.math.maxInt(usize) - 1, std.math.maxInt(usize), 16));
}

test "storage runtime disables SWA ring for retained prompt prefixes" {
    const allocator = std.testing.allocator;
    var runtime = try KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 1,
    });
    defer runtime.deinit();

    var hook_context = TestDeviceWriteHookContext{};
    runtime.setDeviceWriteHook(.{
        .ctx = &hook_context,
        .vtable = &test_device_write_hook_vtable,
    });

    const sequence_id = try runtime.attachSequence(runtime.poolId());
    try runtime.appendTokens(sequence_id, 1);
    (try runtime.sequenceMut(sequence_id)).block_table.markSharedPrefix(1);

    var k_value: f32 = 1.0;
    var v_value: f32 = 2.0;
    try runtime.writeLayerKvSuffixDevice(
        .{
            .sequence_id = sequence_id,
            .layer_index = 0,
            .total_token_count = 1,
            .suffix_token_count = 1,
            .position_offset = 0,
            .num_kv_heads = 1,
            .head_dim = 1,
            .sliding_window = 512,
            .max_inflight_tokens = 128,
            .allow_swa_ring = true,
        },
        .{ .handle = @ptrCast(&k_value), .byte_offset = 0, .byte_len = @sizeOf(f32) },
        .{ .handle = @ptrCast(&v_value), .byte_offset = 0, .byte_len = @sizeOf(f32) },
    );

    try std.testing.expectEqual(@as(usize, 1), hook_context.calls);
    try std.testing.expect(!hook_context.last_allow_swa_ring);
}
