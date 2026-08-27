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

//! Device-resident KV storage for the Metal backend (Phase 6 / option B).
//!
//! `MetalKvStorage` implements the `KvStorageRuntime.DeviceWriteHook` so that
//! `writeLayerKvSuffix` on device-resident k/v tensors goes straight to the
//! Metal encode kernel — no host materialization, no host KvPool shadow on the
//! hot path. Gather still works by downloading the encoded span bytes from
//! the device slot buffers and decoding them on the host using the existing
//! turboquant routines.
//!
//! **Current scope**:
//! - KV dtypes supported: polar4, turbo3 (compressed encode), f32 (raw
//!   device→device blit), f16 (device f32→half convert kernel), and int8
//!   per-head (device quantize with threadgroup tree reduction over
//!   head_dim). int4, fp8, bf16 still fall through to the host path.
//! - Full-history paged KV uses one device page pool per layer. Physical page
//!   offsets are the storage runtime's reference-counted block ids, so cached
//!   prefixes remain resident after the request that populated them exits.
//!   Sliding-window rings remain sequence-owned because their cyclic physical
//!   offsets intentionally alias across logical pages. Capacity-exhaustion
//!   returns `error.DeviceWriteFallback` so callers downgrade gracefully.
//! - Caller is responsible for keeping the host KvPool in sync if any path
//!   still reads from it. In the full Phase 6 rollout `ensurePagedKvSuffixWritten`
//!   calls the device path first and skips the host write when the hook
//!   accepts; gather-driven paths download from the slot buffer instead.

const std = @import("std");
const build_options = @import("build_options");
const block = @import("../runtime/kv/block.zig");
const storage_runtime = @import("../runtime/kv/storage_runtime.zig");
const pool_mod = @import("../runtime/kv/pool.zig");
const turboquant = @import("../runtime/kv/turboquant.zig");
const metal_runtime = @import("metal_runtime.zig");

/// Matches the `format` values accepted by
/// `termite_metal_decode_runtime_update_attention_span_from_f32_key_device_slot`.
/// Compressed formats run the polar4/turbo3 encode pipeline; `raw_f32` skips
/// the pipeline and copies device→device; `f16` dispatches the f32→half
/// converter kernel for both keys and values; `int8_per_head` quantizes keys
/// into a (f32_scale + int8[head_dim]) per-head layout via the threadgroup
/// reduction kernel while values stay raw f32 (parallel to polar4/turbo3).
/// Other KV dtypes (int4, bf16) still fall through to the host write path.
pub const KeyFormat = enum(u32) {
    polar4 = 0,
    turbo3 = 1,
    raw_f32 = 2,
    f16 = 3,
    int8_per_head = 4,

    pub fn fromKvDType(dtype: pool_mod.KvDType) ?KeyFormat {
        return switch (dtype) {
            .polar4 => .polar4,
            .turbo3 => .turbo3,
            .f32 => .raw_f32,
            .f16 => .f16,
            .int8 => .int8_per_head,
            else => null,
        };
    }

    pub fn isCompressed(self: KeyFormat) bool {
        return switch (self) {
            .polar4, .turbo3, .int8_per_head => true,
            .raw_f32, .f16 => false,
        };
    }
};

/// Retained for external callers that imported the old name.
pub const CompressedKeyFormat = KeyFormat;

/// Key for the (sequence, layer) → slot mapping. Layer index fits in u32 —
/// even 80-layer models (Llama-3 405B) stay far below u32.
const SlotKey = struct {
    sequence_id: storage_runtime.SequenceId,
    layer_index: u32,
};

/// Per-sequence view into a backing slot. Full-history bindings for the same
/// layer share a slot but keep sequence-local layout and position metadata.
/// Ring bindings own a dedicated slot and are reclaimed with the sequence.
const SlotBinding = struct {
    slot: usize,
    ring_page_count: usize = 0,
    sequence_owned: bool = false,
    logical_contiguous: bool = false,
    physical_base_tokens: usize = 0,
    written_tokens: usize = 0,
    position_offset: usize = 0,

    fn ownsSlot(self: SlotBinding) bool {
        return self.sequence_owned;
    }

    fn covers(self: SlotBinding, token_count: usize) bool {
        return self.written_tokens >= token_count;
    }

    fn coversBeforePendingSuffix(self: SlotBinding, token_count: usize, pending_suffix_token_count: usize) bool {
        if (pending_suffix_token_count > token_count) return false;
        return self.covers(token_count - pending_suffix_token_count);
    }

    fn commitWrite(self: *SlotBinding, total_token_count: usize, position_offset: usize) bool {
        if (total_token_count < self.written_tokens or position_offset < self.position_offset) return false;
        self.written_tokens = total_token_count;
        self.position_offset = position_offset;
        return true;
    }

    fn truncateTo(self: *SlotBinding, retained_token_count: usize) void {
        self.written_tokens = @min(self.written_tokens, retained_token_count);
    }
};

pub const MetalKvStorage = struct {
    allocator: std.mem.Allocator,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    format: KeyFormat,
    num_kv_heads: u32,
    head_dim: u32,
    page_size_tokens: u16,
    /// (seq, layer) → sequence-local view of a device slot.
    slot_map: std.AutoHashMapUnmanaged(SlotKey, SlotBinding) = .empty,
    /// Stable layer → slot mapping for full-history paged KV. These slots are
    /// never reset at a request boundary: retained block ids, rather than a
    /// transient sequence id, own the bytes stored at each physical page.
    global_layer_slots: std.AutoHashMapUnmanaged(u32, usize) = .empty,
    /// Physical slots are leased by the shared raw runtime rather than handed
    /// out independently by each hook. A prompt-cache hook can persist across
    /// requests while request-local hooks use the same runtime, so local bump
    /// allocators would alias and corrupt retained KV pages.
    leased_slots: [metal_runtime.attention_span_slot_capacity]bool = [_]bool{false} ** metal_runtime.attention_span_slot_capacity,
    slot_ring_page_count: [metal_runtime.attention_span_slot_capacity]usize = [_]usize{0} ** metal_runtime.attention_span_slot_capacity,
    slot_ring_policy_initialized: [metal_runtime.attention_span_slot_capacity]bool = [_]bool{false} ** metal_runtime.attention_span_slot_capacity,
    slot_buffer_capacity_tokens: [metal_runtime.attention_span_slot_capacity]usize = [_]usize{0} ** metal_runtime.attention_span_slot_capacity,
    cyclic_page_table_cache: storage_runtime.CyclicPageTableCache = .{},

    /// Allocate a MetalKvStorage keyed to `runtime`. The storage does not own
    /// the runtime — callers are responsible for its lifetime. `dtype` must
    /// resolve to a compressed key format; other formats are unsupported by
    /// this fast path (caller falls back to the host write path).
    pub fn create(
        allocator: std.mem.Allocator,
        runtime: *metal_runtime.RawMetalDecodeRuntime,
        dtype: pool_mod.KvDType,
        num_kv_heads: u32,
        head_dim: u32,
        page_size_tokens: u16,
    ) !*MetalKvStorage {
        const format = KeyFormat.fromKvDType(dtype) orelse return error.DeviceWriteFormatUnsupported;
        const self = try allocator.create(MetalKvStorage);
        self.* = .{
            .allocator = allocator,
            .runtime = runtime,
            .format = format,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .page_size_tokens = page_size_tokens,
        };
        return self;
    }

    fn allocateSlot(self: *MetalKvStorage) !usize {
        var slot: usize = undefined;
        if (metal_runtime.termite_metal_decode_runtime_acquire_paged_kv_slot(self.runtime, &slot) != 0)
            return error.DeviceWriteFallback;
        if (slot >= self.leased_slots.len) {
            _ = metal_runtime.termite_metal_decode_runtime_release_paged_kv_slot(self.runtime, slot);
            return error.DeviceWriteFallback;
        }
        self.leased_slots[slot] = true;
        self.slot_ring_page_count[slot] = 0;
        self.slot_ring_policy_initialized[slot] = false;
        self.slot_buffer_capacity_tokens[slot] = 0;
        return slot;
    }

    fn reclaimSlot(self: *MetalKvStorage, slot: usize) void {
        if (slot >= self.leased_slots.len or !self.leased_slots[slot]) return;
        _ = metal_runtime.termite_metal_decode_runtime_release_paged_kv_slot(self.runtime, slot);
        self.leased_slots[slot] = false;
        self.slot_ring_page_count[slot] = 0;
        self.slot_ring_policy_initialized[slot] = false;
        self.slot_buffer_capacity_tokens[slot] = 0;
    }

    /// Bind a sequence/layer to either the layer-global full-history page pool
    /// or a dedicated cyclic SWA ring. `paged` must be true before global
    /// sharing is allowed; legacy contiguous writes remain sequence-owned.
    fn acquireBinding(
        self: *MetalKvStorage,
        key: SlotKey,
        ring_page_count: usize,
        paged: bool,
    ) !*SlotBinding {
        if (self.slot_map.getPtr(key)) |binding| {
            if (binding.ring_page_count != ring_page_count) return error.DeviceWriteFallback;
            return binding;
        }

        const owns_slot = ring_page_count > 0 or !paged;
        var global_created = false;
        var owned_created = false;
        var slot: usize = undefined;
        errdefer {
            if (global_created) _ = self.global_layer_slots.remove(key.layer_index);
            if (global_created or owned_created) self.reclaimSlot(slot);
        }
        if (!owns_slot) {
            if (self.global_layer_slots.get(key.layer_index)) |existing| {
                slot = existing;
            } else {
                slot = try self.allocateSlot();
                global_created = true;
                try self.global_layer_slots.put(self.allocator, key.layer_index, slot);
                self.slot_ring_page_count[slot] = 0;
                self.slot_ring_policy_initialized[slot] = true;
            }
        } else {
            slot = try self.allocateSlot();
            owned_created = true;
            _ = try self.configureSlotRingPolicy(slot, ring_page_count);
        }

        try self.slot_map.put(self.allocator, key, .{
            .slot = slot,
            .ring_page_count = ring_page_count,
            .sequence_owned = owns_slot,
        });
        return self.slot_map.getPtr(key).?;
    }

    fn flagValueEnabled(value: []const u8) bool {
        return value.len != 0 and
            !std.mem.eql(u8, value, "0") and
            !std.ascii.eqlIgnoreCase(value, "false") and
            !std.ascii.eqlIgnoreCase(value, "no") and
            !std.ascii.eqlIgnoreCase(value, "off");
    }

    fn envFlagEnabled(name: [*:0]const u8) bool {
        const value = std.c.getenv(name) orelse return false;
        return flagValueEnabled(std.mem.span(value));
    }

    pub fn splitSwaKvRingEnabled() bool {
        return !envFlagEnabled("TERMITE_METAL_DISABLE_SPLIT_SWA_KV_RING") and
            !envFlagEnabled("TERMITE_METAL_DISABLE_PAGED_SLOT_ATTENTION");
    }

    fn requestedRingPageCount(
        page_size_tokens: u16,
        sliding_window: usize,
        max_inflight_tokens: usize,
        allow_swa_ring: bool,
    ) !usize {
        if (!allow_swa_ring or sliding_window == 0 or max_inflight_tokens == 0 or page_size_tokens == 0) return 0;
        // Production requests opt in through the typed KV policy. Keep only a
        // hard rollback flag here.
        if (envFlagEnabled("TERMITE_METAL_DISABLE_SPLIT_SWA_KV_RING")) return 0;
        if (envFlagEnabled("TERMITE_METAL_DISABLE_PAGED_SLOT_ATTENTION")) return 0;
        return storage_runtime.swaRingPageCount(page_size_tokens, sliding_window, max_inflight_tokens, allow_swa_ring);
    }

    fn configureSlotRingPolicy(self: *MetalKvStorage, slot: usize, requested_page_count: usize) !usize {
        if (!self.slot_ring_policy_initialized[slot]) {
            self.slot_ring_page_count[slot] = requested_page_count;
            self.slot_ring_policy_initialized[slot] = true;
        } else if (self.slot_ring_page_count[slot] != requested_page_count) {
            return error.DeviceWriteFallback;
        }
        return self.slot_ring_page_count[slot];
    }

    fn pageTokenOffsets(
        self: *MetalKvStorage,
        logical_blocks: []const u32,
        needed_blocks: usize,
        page_size_tokens: usize,
        ring_page_count: usize,
    ) !storage_runtime.PageTokenOffsets {
        if (ring_page_count > 0) {
            return self.cyclic_page_table_cache.get(
                self.allocator,
                page_size_tokens,
                ring_page_count,
                needed_blocks,
                !envFlagEnabled("TERMITE_METAL_DISABLE_CYCLIC_PAGE_TABLE_CACHE"),
            );
        }
        if (logical_blocks.len < needed_blocks) return error.KvCapacityTooSmall;
        const offsets = try self.allocator.alloc(u32, needed_blocks);
        errdefer self.allocator.free(offsets);
        for (offsets, logical_blocks[0..needed_blocks]) |*offset, block_id| {
            const token_offset = std.math.mul(usize, @as(usize, block_id), page_size_tokens) catch return error.KvCapacityTooSmall;
            if (token_offset > std.math.maxInt(u32)) return error.KvCapacityTooSmall;
            offset.* = @intCast(token_offset);
        }
        return .{ .owned = offsets };
    }

    fn reserveSlotCapacity(
        self: *MetalKvStorage,
        slot: usize,
        required_tokens: usize,
        page_size_tokens: usize,
        ring_page_count: usize,
        key_row_bytes: usize,
        v_row_stride: usize,
    ) !void {
        const current = self.slot_buffer_capacity_tokens[slot];
        if (required_tokens <= current) return;
        const target = if (ring_page_count > 0 or envFlagEnabled("TERMITE_METAL_DISABLE_GEOMETRIC_KV_GROWTH"))
            required_tokens
        else
            try storage_runtime.geometricKvTokenCapacity(current, required_tokens, page_size_tokens);
        const rc = metal_runtime.termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
            self.runtime,
            slot,
            @intFromEnum(self.format),
            target,
            key_row_bytes,
            v_row_stride,
        );
        if (rc != 0) return error.DeviceWriteFallback;
        self.slot_buffer_capacity_tokens[slot] = target;
    }

    fn requiredTokenCapacity(block_offsets: []const u32, page_size_tokens: usize) !usize {
        var required: usize = 0;
        for (block_offsets) |offset| {
            const end = std.math.add(usize, @as(usize, offset), page_size_tokens) catch return error.KvCapacityTooSmall;
            required = @max(required, end);
        }
        return required;
    }

    /// Remove every binding for `sequence_id`. Full-history page-pool slots
    /// remain live because retained physical block ids may still reference
    /// their bytes; only sequence-owned ring/legacy slots are reset/reclaimed.
    fn releaseSequenceSlots(self: *MetalKvStorage, sequence_id: storage_runtime.SequenceId) void {
        var keys: [metal_runtime.attention_span_slot_capacity]SlotKey = undefined;
        while (true) {
            var it = self.slot_map.iterator();
            var key_count: usize = 0;
            while (it.next()) |entry| {
                if (entry.key_ptr.sequence_id != sequence_id) continue;
                keys[key_count] = entry.key_ptr.*;
                key_count += 1;
                if (key_count == keys.len) break;
            }
            if (key_count == 0) return;
            for (keys[0..key_count]) |key| {
                if (self.slot_map.fetchRemove(key)) |removed| {
                    if (removed.value.ownsSlot()) self.reclaimSlot(removed.value.slot);
                }
            }
        }
    }

    fn rowLayout(
        self: *const MetalKvStorage,
        num_kv_heads: u32,
        head_dim: u32,
    ) struct {
        token_values: usize,
        key_row_bytes: usize,
        base_key_row_bytes: usize,
        v_row_stride: usize,
    } {
        const token_values: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
        const int8_row_bytes: usize = @as(usize, num_kv_heads) * (@as(usize, head_dim) + @sizeOf(f32));
        const key_row_bytes: usize = switch (self.format) {
            .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
            .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim) + turboquant.turbo3ResidualBytes(num_kv_heads, head_dim),
            .raw_f32 => token_values * @sizeOf(f32),
            .f16 => token_values * @sizeOf(u16),
            .int8_per_head => int8_row_bytes,
        };
        const base_key_row_bytes: usize = switch (self.format) {
            .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
            .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim),
            .raw_f32 => token_values * @sizeOf(f32),
            .f16 => token_values * @sizeOf(u16),
            .int8_per_head => int8_row_bytes,
        };
        return .{
            .token_values = token_values,
            .key_row_bytes = key_row_bytes,
            .base_key_row_bytes = base_key_row_bytes,
            .v_row_stride = token_values,
        };
    }

    pub fn deviceWriteHook(self: *MetalKvStorage) storage_runtime.DeviceWriteHook {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &hook_vtable,
        };
    }

    fn traceKvGather() bool {
        // Cached: this runs per KV write/read in the decode inner loop.
        const S = struct {
            var cached: ?bool = null;
        };
        if (S.cached) |value| return value;
        const enabled = blk: {
            const value = std.c.getenv("TERMITE_METAL_TRACE_KV_GATHER") orelse break :blk false;
            const span = std.mem.span(value);
            break :blk span.len > 0 and !std.mem.eql(u8, span, "0");
        };
        S.cached = enabled;
        return enabled;
    }

    fn writeLayerKvSuffix(
        ctx: *anyopaque,
        write: storage_runtime.KvSuffixWrite,
        k: storage_runtime.DeviceKvRef,
        v: storage_runtime.DeviceKvRef,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const num_kv_heads: u32 = write.num_kv_heads;
        const head_dim: u32 = write.head_dim;

        const layout = self.rowLayout(num_kv_heads, head_dim);
        const key_row_bytes = layout.key_row_bytes;
        const base_key_row_bytes = layout.base_key_row_bytes;
        const v_row_stride = layout.v_row_stride;

        const expected_elems = write.suffix_token_count * v_row_stride;
        const expected_bytes = expected_elems * @sizeOf(f32);
        if (k.byte_len < expected_bytes or v.byte_len < expected_bytes) return error.InvalidKvShape;

        const requested_ring_pages = try requestedRingPageCount(
            write.page_size_tokens,
            write.sliding_window,
            write.max_inflight_tokens,
            write.allow_swa_ring,
        );
        const binding = try self.acquireBinding(
            .{
                .sequence_id = write.sequence_id,
                .layer_index = @intCast(write.layer_index),
            },
            requested_ring_pages,
            write.logical_blocks != null,
        );
        const slot = binding.slot;
        const ring_page_count = binding.ring_page_count;
        if (traceKvGather()) std.debug.print(
            "kv-write: seq={d} layer={d} suffix={d} total={d} slot={d} ring_pages={d}\n",
            .{ write.sequence_id, write.layer_index, write.suffix_token_count, write.total_token_count, slot, ring_page_count },
        );

        const rc = if (write.logical_blocks) |logical_blocks| paged: {
            if (logical_blocks.len == 0 or write.page_size_tokens == 0) break :paged -9999;
            const needed_blocks = std.math.divCeil(usize, write.total_token_count, write.page_size_tokens) catch break :paged -9999;
            if (logical_blocks.len < needed_blocks) break :paged -9999;
            const block_offsets_result = self.pageTokenOffsets(
                logical_blocks,
                needed_blocks,
                write.page_size_tokens,
                ring_page_count,
            ) catch break :paged -9999;
            defer block_offsets_result.deinit(self.allocator);
            const block_offsets = block_offsets_result.values();
            const required_tokens = if (ring_page_count > 0)
                std.math.mul(usize, ring_page_count, write.page_size_tokens) catch break :paged -9999
            else
                requiredTokenCapacity(block_offsets, write.page_size_tokens) catch break :paged -9999;
            self.reserveSlotCapacity(
                slot,
                required_tokens,
                write.page_size_tokens,
                ring_page_count,
                key_row_bytes,
                v_row_stride,
            ) catch break :paged -9999;
            binding.physical_base_tokens = if (ring_page_count > 0) 0 else block_offsets[0];
            binding.logical_contiguous = ring_page_count == 0;
            if (ring_page_count == 0) {
                for (block_offsets, 0..) |offset, block_idx| {
                    const expected = binding.physical_base_tokens + block_idx * @as(usize, write.page_size_tokens);
                    if (offset != expected) {
                        binding.logical_contiguous = false;
                        break;
                    }
                }
            }
            break :paged metal_runtime.termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
                self.runtime,
                slot,
                @intFromEnum(self.format),
                k.handle,
                k.byte_offset,
                v.handle,
                v.byte_offset,
                write.total_token_count,
                write.suffix_token_count,
                num_kv_heads,
                head_dim,
                key_row_bytes,
                base_key_row_bytes,
                v_row_stride,
                write.position_offset,
                block_offsets.ptr,
                block_offsets.len,
                write.page_size_tokens,
            );
        } else blk: {
            if (ring_page_count > 0) break :blk -9999;
            binding.logical_contiguous = true;
            binding.physical_base_tokens = 0;
            self.reserveSlotCapacity(
                slot,
                write.total_token_count,
                write.page_size_tokens,
                ring_page_count,
                key_row_bytes,
                v_row_stride,
            ) catch break :blk -9999;
            break :blk metal_runtime.termite_metal_decode_runtime_update_attention_span_from_f32_key_device_slot(
                self.runtime,
                slot,
                @intFromEnum(self.format),
                k.handle,
                k.byte_offset,
                v.handle,
                v.byte_offset,
                write.total_token_count,
                num_kv_heads,
                head_dim,
                key_row_bytes,
                base_key_row_bytes,
                v_row_stride,
                write.position_offset,
            );
        };
        if (rc != 0) return error.DeviceWriteFallback;
        binding.written_tokens = write.total_token_count;
        binding.position_offset = write.position_offset;
    }

    fn reserveLayerKvDevice(
        ctx: *anyopaque,
        reserve: storage_runtime.DeviceKvLayerReserve,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (reserve.token_capacity == 0) return;
        const layout = self.rowLayout(reserve.num_kv_heads, reserve.head_dim);
        if (layout.key_row_bytes == 0 or layout.v_row_stride == 0) return error.InvalidKvShape;

        const requested_ring_pages = try requestedRingPageCount(
            reserve.page_size_tokens,
            reserve.sliding_window,
            reserve.max_inflight_tokens,
            reserve.allow_swa_ring,
        );
        const binding = try self.acquireBinding(
            .{
                .sequence_id = reserve.sequence_id,
                .layer_index = @intCast(reserve.layer_index),
            },
            requested_ring_pages,
            reserve.logical_blocks != null,
        );
        const slot = binding.slot;
        const ring_page_count = binding.ring_page_count;
        if (traceKvGather()) std.debug.print(
            "kv-reserve: seq={d} layer={d} capacity={d} slot={d} ring_pages={d}\n",
            .{ reserve.sequence_id, reserve.layer_index, reserve.token_capacity, slot, ring_page_count },
        );
        if (ring_page_count > 0 and reserve.logical_blocks == null) return error.DeviceWriteFallback;

        const token_capacity = if (ring_page_count > 0)
            std.math.mul(usize, ring_page_count, reserve.page_size_tokens) catch return error.KvCapacityTooSmall
        else if (reserve.logical_blocks) |logical_blocks| blk: {
            if (logical_blocks.len == 0 or reserve.page_size_tokens == 0) break :blk reserve.token_capacity;
            const needed_blocks = std.math.divCeil(usize, reserve.token_capacity, reserve.page_size_tokens) catch break :blk reserve.token_capacity;
            if (logical_blocks.len < needed_blocks) break :blk reserve.token_capacity;
            var capacity_tokens: usize = 0;
            binding.physical_base_tokens = std.math.mul(
                usize,
                @as(usize, logical_blocks[0]),
                reserve.page_size_tokens,
            ) catch return error.KvCapacityTooSmall;
            binding.logical_contiguous = true;
            for (logical_blocks[0..needed_blocks], 0..) |block_id, block_idx| {
                const block_start = std.math.mul(
                    usize,
                    @as(usize, block_id),
                    reserve.page_size_tokens,
                ) catch return error.KvCapacityTooSmall;
                const expected_delta = std.math.mul(
                    usize,
                    block_idx,
                    @as(usize, reserve.page_size_tokens),
                ) catch return error.KvCapacityTooSmall;
                const expected = std.math.add(
                    usize,
                    binding.physical_base_tokens,
                    expected_delta,
                ) catch return error.KvCapacityTooSmall;
                if (block_start != expected) binding.logical_contiguous = false;
                const block_end = std.math.add(
                    usize,
                    block_start,
                    reserve.page_size_tokens,
                ) catch return error.KvCapacityTooSmall;
                if (block_end > capacity_tokens) capacity_tokens = block_end;
            }
            break :blk capacity_tokens;
        } else blk: {
            binding.logical_contiguous = true;
            binding.physical_base_tokens = 0;
            break :blk reserve.token_capacity;
        };
        if (ring_page_count > 0) {
            binding.logical_contiguous = false;
            binding.physical_base_tokens = 0;
        }
        binding.position_offset = reserve.position_offset;

        try self.reserveSlotCapacity(
            slot,
            token_capacity,
            reserve.page_size_tokens,
            ring_page_count,
            layout.key_row_bytes,
            layout.v_row_stride,
        );
    }

    fn commitLayerKvDeviceWrite(
        ctx: *anyopaque,
        commit: storage_runtime.DeviceKvLayerWriteCommit,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const binding = self.slot_map.getPtr(.{
            .sequence_id = commit.sequence_id,
            .layer_index = @intCast(commit.layer_index),
        }) orelse return error.DeviceWriteFallback;
        if (!binding.commitWrite(commit.total_token_count, commit.position_offset)) return error.DeviceWriteFallback;
    }

    fn hookDeinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        for (&self.leased_slots, 0..) |*leased, slot| {
            if (!leased.*) continue;
            _ = metal_runtime.termite_metal_decode_runtime_release_paged_kv_slot(self.runtime, slot);
            leased.* = false;
        }
        self.slot_map.deinit(allocator);
        self.global_layer_slots.deinit(allocator);
        self.cyclic_page_table_cache.deinit(allocator);
        allocator.destroy(self);
    }

    fn releaseSequenceOp(ctx: *anyopaque, sequence_id: storage_runtime.SequenceId) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        self.releaseSequenceSlots(sequence_id);
    }

    fn truncateSequenceOp(
        ctx: *anyopaque,
        sequence_id: storage_runtime.SequenceId,
        retained_token_count: usize,
    ) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        var it = self.slot_map.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.sequence_id != sequence_id) continue;
            entry.value_ptr.truncateTo(retained_token_count);
        }
    }

    fn gatherLayerKv(
        ctx: *anyopaque,
        gather: storage_runtime.KvLayerGather,
        k_out: []f32,
        v_out: []f32,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const num_kv_heads = gather.num_kv_heads;
        const head_dim = gather.head_dim;
        const token_width: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
        const expected_elems = gather.token_count * token_width;
        if (k_out.len < expected_elems or v_out.len < expected_elems) return error.InvalidKvShape;

        if (traceKvGather()) std.debug.print("kv-read: seq={d} layer={d} tokens={d}\n", .{ gather.sequence_id, gather.layer_index, gather.token_count });
        const binding = self.slot_map.get(.{
            .sequence_id = gather.sequence_id,
            .layer_index = @intCast(gather.layer_index),
        }) orelse return error.DeviceReadFallback;
        const slot = binding.slot;
        if (binding.ring_page_count > 0) return error.RingKvRequiresPagedAttention;
        if (!binding.logical_contiguous) return error.DeviceReadFallback;
        if (binding.written_tokens < gather.token_count) return error.DeviceReadFallback;
        const info = try self.slotInfo(slot);

        const k_handle = info.encoded_key_handle orelse return error.DeviceReadFallback;
        const v_handle = info.v_handle orelse return error.DeviceReadFallback;

        const key_row_bytes = info.key_row_bytes;
        const v_row_stride = info.v_row_stride;
        const expect_v_stride = token_width;
        if (v_row_stride != expect_v_stride) return error.DeviceReadFallback;

        // For f16 format values are stored as halfs (2 bytes each), for all
        // other formats values are stored as plain f32.
        const v_element_bytes: usize = if (self.format == .f16) @sizeOf(u16) else @sizeOf(f32);
        const key_bytes = gather.token_count * key_row_bytes;
        const v_byte_count = gather.token_count * v_row_stride * v_element_bytes;
        const physical_base = binding.physical_base_tokens;
        const key_byte_offset = physical_base * key_row_bytes;
        const v_byte_offset = physical_base * v_row_stride * v_element_bytes;
        if (key_byte_offset + key_bytes > info.encoded_key_capacity or v_byte_offset + v_byte_count > info.v_capacity) return error.DeviceReadFallback;

        const k_staging = try self.allocator.alloc(u8, key_bytes);
        defer self.allocator.free(k_staging);
        const v_staging = try self.allocator.alloc(u8, v_byte_count);
        defer self.allocator.free(v_staging);

        if (metal_runtime.termite_metal_buffer_download(self.runtime, k_handle, key_byte_offset, @ptrCast(k_staging.ptr), key_bytes) != 0) {
            return error.MetalDeviceReadFailed;
        }
        if (metal_runtime.termite_metal_buffer_download(self.runtime, v_handle, v_byte_offset, @ptrCast(v_staging.ptr), v_byte_count) != 0) {
            return error.MetalDeviceReadFailed;
        }

        for (0..gather.token_count) |tok_idx| {
            const row_bytes = k_staging[tok_idx * key_row_bytes ..][0..key_row_bytes];
            const row_dst = k_out[tok_idx * token_width ..][0..token_width];
            switch (self.format) {
                .polar4 => try turboquant.decodePolar4Key(row_bytes, row_dst, num_kv_heads, head_dim),
                .turbo3 => {
                    const base_bytes = turboquant.turbo3KeyBytes(num_kv_heads, head_dim);
                    try turboquant.decodeTurbo3Key(row_bytes[0..base_bytes], row_dst, num_kv_heads, head_dim);
                },
                .raw_f32 => {
                    const src_f32: [*]const f32 = @ptrCast(@alignCast(row_bytes.ptr));
                    @memcpy(row_dst, src_f32[0..token_width]);
                },
                .f16 => {
                    const src_f16: [*]const f16 = @ptrCast(@alignCast(row_bytes.ptr));
                    for (0..token_width) |i| row_dst[i] = @floatCast(src_f16[i]);
                },
                .int8_per_head => pool_mod.dequantizeInt8PerHeadToF32(row_bytes, row_dst, num_kv_heads, head_dim),
            }
            const v_row_src = v_staging[tok_idx * v_row_stride * v_element_bytes ..][0 .. v_row_stride * v_element_bytes];
            const v_row_dst = v_out[tok_idx * token_width ..][0..token_width];
            switch (self.format) {
                .f16 => {
                    const src_f16: [*]const f16 = @ptrCast(@alignCast(v_row_src.ptr));
                    for (0..token_width) |i| v_row_dst[i] = @floatCast(src_f16[i]);
                },
                .int8_per_head => {
                    const src_f32: [*]const f32 = @ptrCast(@alignCast(v_row_src.ptr));
                    @memcpy(v_row_dst, src_f32[0..token_width]);
                },
                else => {
                    const src_f32: [*]const f32 = @ptrCast(@alignCast(v_row_src.ptr));
                    @memcpy(v_row_dst, src_f32[0..token_width]);
                },
            }
        }
    }

    fn gatherLayerKvDevice(
        ctx: *anyopaque,
        gather: storage_runtime.DeviceKvLayerGather,
    ) anyerror!storage_runtime.DeviceKvLayer {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (self.format != .raw_f32) return error.DeviceReadFallback;
        const token_width: usize = @as(usize, gather.num_kv_heads) * @as(usize, gather.head_dim);
        const byte_len = gather.token_count * token_width * @sizeOf(f32);

        if (traceKvGather()) std.debug.print("kv-read: seq={d} layer={d} tokens={d}\n", .{ gather.sequence_id, gather.layer_index, gather.token_count });
        const binding = self.slot_map.get(.{
            .sequence_id = gather.sequence_id,
            .layer_index = @intCast(gather.layer_index),
        }) orelse return error.DeviceReadFallback;
        const slot = binding.slot;
        if (binding.ring_page_count > 0) return error.RingKvRequiresPagedAttention;
        if (!binding.logical_contiguous) return error.DeviceReadFallback;
        if (binding.written_tokens < gather.token_count) return error.DeviceReadFallback;
        const info = try self.slotInfo(slot);
        if (info.key_row_bytes != token_width * @sizeOf(f32)) return error.DeviceReadFallback;
        if (info.v_row_stride != token_width) return error.DeviceReadFallback;

        const k_handle = info.encoded_key_handle orelse return error.DeviceReadFallback;
        const v_handle = info.v_handle orelse return error.DeviceReadFallback;
        const byte_offset = binding.physical_base_tokens * token_width * @sizeOf(f32);
        if (byte_offset + byte_len > info.encoded_key_capacity or byte_offset + byte_len > info.v_capacity) return error.DeviceReadFallback;

        return .{
            .runtime = @ptrCast(self.runtime),
            .k = .{
                .handle = k_handle,
                .byte_offset = byte_offset,
                .byte_len = byte_len,
            },
            .v = .{
                .handle = v_handle,
                .byte_offset = byte_offset,
                .byte_len = byte_len,
            },
            .token_count = gather.token_count,
            .row_width = token_width,
            .position_offset = info.position_offset,
            .value_element_bytes = @sizeOf(f32),
        };
    }

    fn pagedLayerKvDevice(
        ctx: *anyopaque,
        gather: storage_runtime.DeviceKvLayerGather,
    ) anyerror!storage_runtime.DevicePagedKvLayer {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const num_kv_heads = gather.num_kv_heads;
        const head_dim = gather.head_dim;
        const token_width: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
        const int8_row_bytes: usize = @as(usize, num_kv_heads) * (@as(usize, head_dim) + @sizeOf(f32));
        const key_row_bytes: usize = switch (self.format) {
            .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
            .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim) + turboquant.turbo3ResidualBytes(num_kv_heads, head_dim),
            .raw_f32 => token_width * @sizeOf(f32),
            .f16 => token_width * @sizeOf(u16),
            .int8_per_head => int8_row_bytes,
        };
        const base_key_row_bytes: usize = switch (self.format) {
            .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
            .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim),
            .raw_f32 => token_width * @sizeOf(f32),
            .f16 => token_width * @sizeOf(u16),
            .int8_per_head => int8_row_bytes,
        };
        const key = SlotKey{
            .sequence_id = gather.sequence_id,
            .layer_index = @intCast(gather.layer_index),
        };
        const active_frame = metal_runtime.hasActiveFrame(self.runtime);
        const binding = self.slot_map.get(key) orelse return error.DeviceReadFallback;
        const slot = binding.slot;
        const ring_page_count = binding.ring_page_count;
        const info_opt = self.slotInfo(slot) catch |err| blk: {
            if (!active_frame) return err;
            break :blk null;
        };
        // A fused operation may expose a reserved slot before it encodes the
        // current suffix, but only while an ordered frame is active. Every
        // token before that explicitly declared suffix must already be
        // committed; otherwise the slot contains unreadable capacity.
        if (gather.pending_suffix_token_count != 0 and !active_frame) return error.DeviceReadFallback;
        if (!binding.coversBeforePendingSuffix(gather.token_count, gather.pending_suffix_token_count)) {
            return error.DeviceReadFallback;
        }
        if (info_opt) |info| {
            if (info.key_row_bytes != 0 and info.key_row_bytes != key_row_bytes) return error.DeviceReadFallback;
            if (info.v_row_stride != 0 and info.v_row_stride != token_width) return error.DeviceReadFallback;
        }
        return .{
            .runtime = @ptrCast(self.runtime),
            .slot = slot,
            .format = @intFromEnum(self.format),
            .token_count = gather.token_count,
            .key_row_bytes = key_row_bytes,
            .base_key_row_bytes = base_key_row_bytes,
            .v_row_stride = token_width,
            .page_size_tokens = self.page_size_tokens,
            .ring_page_count = ring_page_count,
            .position_offset = binding.position_offset,
        };
    }

    /// Fetch a slot's current state + device buffer handles so a caller can
    /// download the encoded bytes and decode them on the host. Used by the
    /// gather path — the read logic itself lives in the caller rather than
    /// this module so that the pool's existing decode routines can be reused.
    pub fn slotInfo(self: *const MetalKvStorage, slot: usize) !SlotInfo {
        var info: SlotInfo = .{};
        const rc = metal_runtime.termite_metal_decode_runtime_attention_span_slot_info(
            self.runtime,
            slot,
            &info.encoded_key_handle,
            &info.encoded_key_capacity,
            &info.v_handle,
            &info.v_capacity,
            &info.tokens,
            &info.key_row_bytes,
            &info.v_row_stride,
            &info.position_offset,
        );
        if (rc != 0) return error.InvalidSlot;
        return info;
    }

    /// Return the physical slot backing a sequence/layer view. Intended for
    /// diagnostics and invariant tests; callers must not treat slot identity
    /// as stable beyond the owning hook's lifetime.
    pub fn boundSlot(self: *const MetalKvStorage, sequence_id: storage_runtime.SequenceId, layer_index: usize) ?usize {
        const binding = self.slot_map.get(.{
            .sequence_id = sequence_id,
            .layer_index = std.math.cast(u32, layer_index) orelse return null,
        }) orelse return null;
        return binding.slot;
    }

    pub const SlotInfo = struct {
        encoded_key_handle: ?*anyopaque = null,
        encoded_key_capacity: usize = 0,
        v_handle: ?*anyopaque = null,
        v_capacity: usize = 0,
        tokens: usize = 0,
        key_row_bytes: usize = 0,
        v_row_stride: usize = 0,
        position_offset: usize = 0,
    };
};

test "Metal KV rollout flags recognize common false values" {
    try std.testing.expect(!MetalKvStorage.flagValueEnabled(""));
    try std.testing.expect(!MetalKvStorage.flagValueEnabled("0"));
    try std.testing.expect(!MetalKvStorage.flagValueEnabled("false"));
    try std.testing.expect(!MetalKvStorage.flagValueEnabled("NO"));
    try std.testing.expect(!MetalKvStorage.flagValueEnabled("Off"));
    try std.testing.expect(MetalKvStorage.flagValueEnabled("1"));
    try std.testing.expect(MetalKvStorage.flagValueEnabled("true"));
}

test "Metal KV binding only covers successfully encoded tokens" {
    var binding = SlotBinding{
        .slot = 0,
        .written_tokens = 32,
    };
    try std.testing.expect(binding.covers(32));
    try std.testing.expect(!binding.covers(33));
    try std.testing.expect(binding.coversBeforePendingSuffix(40, 8));
    try std.testing.expect(!binding.coversBeforePendingSuffix(40, 7));
    try std.testing.expect(!binding.coversBeforePendingSuffix(32, 33));
    try std.testing.expect(binding.commitWrite(40, 4));
    try std.testing.expectEqual(@as(usize, 40), binding.written_tokens);
    try std.testing.expectEqual(@as(usize, 4), binding.position_offset);
    // Expert-checkpoint replay rewrites the same token position after a miss.
    // Equal watermarks are intentionally idempotent and must remain valid.
    try std.testing.expect(binding.commitWrite(40, 4));
    try std.testing.expectEqual(@as(usize, 40), binding.written_tokens);
    try std.testing.expectEqual(@as(usize, 4), binding.position_offset);
    try std.testing.expect(!binding.commitWrite(39, 4));
    try std.testing.expect(!binding.commitWrite(40, 3));
    binding.truncateTo(32);
    try std.testing.expectEqual(@as(usize, 32), binding.written_tokens);
    try std.testing.expectEqual(@as(usize, 4), binding.position_offset);
    try std.testing.expect(binding.commitWrite(33, 4));
}

const hook_vtable: storage_runtime.DeviceWriteHook.VTable = .{
    .writeLayerKvSuffix = MetalKvStorage.writeLayerKvSuffix,
    .gatherLayerKv = MetalKvStorage.gatherLayerKv,
    .gatherLayerKvDevice = MetalKvStorage.gatherLayerKvDevice,
    .pagedLayerKvDevice = MetalKvStorage.pagedLayerKvDevice,
    .reserveLayerKvDevice = MetalKvStorage.reserveLayerKvDevice,
    .commitLayerKvDeviceWrite = MetalKvStorage.commitLayerKvDeviceWrite,
    .truncateSequence = MetalKvStorage.truncateSequenceOp,
    .releaseSequence = MetalKvStorage.releaseSequenceOp,
    .deinit = MetalKvStorage.hookDeinit,
};

test "MetalKvStorage.create rejects unsupported dtype" {
    if (!build_options.enable_metal) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    // RawMetalDecodeRuntime is `opaque {}` so it has no known alignment at
    // comptime; use a word-aligned pointer value since `create` returns before
    // ever dereferencing when the dtype check fails.
    const fake_runtime: *metal_runtime.RawMetalDecodeRuntime = @ptrFromInt(@alignOf(usize));
    // int4 and fp8 still have no device kernel — fall through to host.
    try std.testing.expectError(
        error.DeviceWriteFormatUnsupported,
        MetalKvStorage.create(allocator, fake_runtime, .int4, 8, 128, 256),
    );
    try std.testing.expectError(
        error.DeviceWriteFormatUnsupported,
        MetalKvStorage.create(allocator, fake_runtime, .fp8, 8, 128, 256),
    );
}

test "KeyFormat.fromKvDType covers supported dtypes" {
    try std.testing.expectEqual(KeyFormat.polar4, KeyFormat.fromKvDType(.polar4).?);
    try std.testing.expectEqual(KeyFormat.turbo3, KeyFormat.fromKvDType(.turbo3).?);
    try std.testing.expectEqual(KeyFormat.raw_f32, KeyFormat.fromKvDType(.f32).?);
    try std.testing.expectEqual(KeyFormat.f16, KeyFormat.fromKvDType(.f16).?);
    try std.testing.expectEqual(KeyFormat.int8_per_head, KeyFormat.fromKvDType(.int8).?);
    try std.testing.expect(KeyFormat.fromKvDType(.int4) == null);
    try std.testing.expect(KeyFormat.fromKvDType(.fp8) == null);
    try std.testing.expect(KeyFormat.polar4.isCompressed());
    try std.testing.expect(KeyFormat.turbo3.isCompressed());
    try std.testing.expect(!KeyFormat.raw_f32.isCompressed());
    try std.testing.expect(!KeyFormat.f16.isCompressed());
    try std.testing.expect(KeyFormat.int8_per_head.isCompressed());
}

test "Metal KV sequence release removes more bindings than one slot batch" {
    const allocator = std.testing.allocator;
    var storage: MetalKvStorage = undefined;
    storage.allocator = allocator;
    storage.slot_map = .empty;
    defer storage.slot_map.deinit(allocator);

    const target_sequence: storage_runtime.SequenceId = 7;
    for (0..metal_runtime.attention_span_slot_capacity + 1) |layer_index| {
        try storage.slot_map.put(allocator, .{
            .sequence_id = target_sequence,
            .layer_index = @intCast(layer_index),
        }, .{
            .slot = 0,
            .sequence_owned = false,
        });
    }
    try storage.slot_map.put(allocator, .{
        .sequence_id = 8,
        .layer_index = 0,
    }, .{
        .slot = 0,
        .sequence_owned = false,
    });

    storage.releaseSequenceSlots(target_sequence);
    try std.testing.expectEqual(@as(usize, 1), storage.slot_map.count());
    try std.testing.expect(storage.slot_map.contains(.{ .sequence_id = 8, .layer_index = 0 }));
}

test "Metal paged KV hooks sharing a runtime lease disjoint slots" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const runtime = metal_runtime.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_decode_runtime_destroy(runtime);
    if (metal_runtime.termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    const cached = try MetalKvStorage.create(allocator, runtime, .f16, 1, 2, 2);
    defer cached.deviceWriteHook().deinit(allocator);
    const cached_binding = try cached.acquireBinding(.{ .sequence_id = 1, .layer_index = 0 }, 0, true);
    const cached_slot = cached_binding.slot;

    const request_local = try MetalKvStorage.create(allocator, runtime, .f32, 1, 2, 2);
    const local_binding = try request_local.acquireBinding(.{ .sequence_id = 1, .layer_index = 0 }, 0, true);
    const local_slot = local_binding.slot;
    try std.testing.expect(cached_slot >= metal_runtime.paged_kv_slot_base);
    try std.testing.expect(local_slot >= metal_runtime.paged_kv_slot_base);
    try std.testing.expect(cached_slot != local_slot);

    // Returning a request-local hook publishes only its own lease. The cache
    // still owns its original slot, while its next layer can reuse the slot
    // that the completed request returned to the shared runtime.
    request_local.deviceWriteHook().deinit(allocator);
    const cached_layer_1 = try cached.acquireBinding(.{ .sequence_id = 1, .layer_index = 1 }, 0, true);
    try std.testing.expectEqual(local_slot, cached_layer_1.slot);
    try std.testing.expect(cached_layer_1.slot != cached_slot);
}

test "Metal paged KV keeps layer-global pages across source sequence release" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const runtime = metal_runtime.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_decode_runtime_destroy(runtime);
    if (metal_runtime.termite_metal_decode_runtime_ready(runtime) == 0) return error.SkipZigTest;

    var storage = try storage_runtime.KvStorageRuntime.init(allocator, .{
        .backend = .metal,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 2,
        .num_kv_heads = 1,
        .head_dim = 2,
    });
    defer storage.deinit();
    const metal_storage = try MetalKvStorage.create(allocator, runtime, .f32, 1, 2, 2);
    storage.setDeviceWriteHook(metal_storage.deviceWriteHook());

    const input_bytes = 4 * @sizeOf(f32);
    const k_handle = metal_runtime.termite_metal_buffer_alloc(runtime, input_bytes, 0) orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_buffer_release(k_handle);
    const v_handle = metal_runtime.termite_metal_buffer_alloc(runtime, input_bytes, 0) orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_buffer_release(v_handle);

    const source_id = try storage.attachSequence(storage.poolId());
    try storage.appendTokens(source_id, 2);
    const source_layers = [_]struct { k: [4]f32, v: [4]f32 }{
        .{ .k = .{ 1, 2, 3, 4 }, .v = .{ 11, 12, 13, 14 } },
        .{ .k = .{ 21, 22, 23, 24 }, .v = .{ 31, 32, 33, 34 } },
    };
    var original_slots: [2]usize = undefined;
    for (source_layers, 0..) |layer, layer_index| {
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(runtime, k_handle, 0, &layer.k, input_bytes));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(runtime, v_handle, 0, &layer.v, input_bytes));
        try storage.writeLayerKvSuffixDevice(.{
            .sequence_id = source_id,
            .layer_index = layer_index,
            .total_token_count = 2,
            .suffix_token_count = 2,
            .position_offset = 0,
            .num_kv_heads = 1,
            .head_dim = 2,
        }, .{ .handle = k_handle, .byte_offset = 0, .byte_len = input_bytes }, .{ .handle = v_handle, .byte_offset = 0, .byte_len = input_bytes });
        original_slots[layer_index] = metal_storage.slot_map.get(.{
            .sequence_id = source_id,
            .layer_index = @intCast(layer_index),
        }).?.slot;
        try std.testing.expectEqual(original_slots[layer_index], metal_storage.global_layer_slots.get(@intCast(layer_index)).?);
    }
    try std.testing.expect(original_slots[0] != original_slots[1]);

    var retained: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer retained.deinit(allocator);
    try storage.retainSequencePrefixBlocks(source_id, 2, &retained);
    try storage.releaseSequence(source_id);
    try std.testing.expect(metal_storage.slot_map.get(.{ .sequence_id = source_id, .layer_index = 0 }) == null);
    try std.testing.expectEqual(original_slots[0], metal_storage.global_layer_slots.get(0).?);
    try std.testing.expectEqual(original_slots[1], metal_storage.global_layer_slots.get(1).?);

    const derived_id = try storage.attachSequenceWithRetainedBlocks(storage.poolId(), retained.items, 2);
    storage.releaseRetainedBlocks(retained.items);
    try storage.appendTokens(derived_id, 1);
    const suffix_layers = [_]struct { k: [2]f32, v: [2]f32 }{
        .{ .k = .{ 5, 6 }, .v = .{ 15, 16 } },
        .{ .k = .{ 25, 26 }, .v = .{ 35, 36 } },
    };
    for (suffix_layers, 0..) |layer, layer_index| {
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(runtime, k_handle, 0, &layer.k, layer.k.len * @sizeOf(f32)));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(runtime, v_handle, 0, &layer.v, layer.v.len * @sizeOf(f32)));
        try storage.writeLayerKvSuffixDevice(.{
            .sequence_id = derived_id,
            .layer_index = layer_index,
            .total_token_count = 3,
            .suffix_token_count = 1,
            .position_offset = 0,
            .num_kv_heads = 1,
            .head_dim = 2,
        }, .{ .handle = k_handle, .byte_offset = 0, .byte_len = layer.k.len * @sizeOf(f32) }, .{ .handle = v_handle, .byte_offset = 0, .byte_len = layer.v.len * @sizeOf(f32) });
        try std.testing.expectEqual(original_slots[layer_index], metal_storage.slot_map.get(.{
            .sequence_id = derived_id,
            .layer_index = @intCast(layer_index),
        }).?.slot);
        const gathered = try storage.gatherLayerKv(allocator, derived_id, layer_index, 3);
        defer allocator.free(gathered.k);
        defer allocator.free(gathered.v);
        const expected_k = source_layers[layer_index].k ++ layer.k;
        const expected_v = source_layers[layer_index].v ++ layer.v;
        try std.testing.expectEqualSlices(f32, &expected_k, gathered.k);
        try std.testing.expectEqualSlices(f32, &expected_v, gathered.v);
    }
    try storage.releaseSequence(derived_id);
}
