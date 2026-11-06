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
const builtin = @import("builtin");
const build_options = @import("build_options");
const block = @import("../runtime/kv/block.zig");
const storage_runtime = @import("../runtime/kv/storage_runtime.zig");
const selection = @import("../runtime/kv/selection.zig");
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
pub const RowLayout = struct { token_values: usize, key_row_bytes: usize, base_key_row_bytes: usize, v_row_stride: usize };

pub const KeyFormat = enum(u32) {
    polar4 = 0,
    turbo3 = 1,
    raw_f32 = 2,
    f16 = 3,
    int8_per_head = 4,

    pub fn rowLayout(self: KeyFormat, num_kv_heads: u32, head_dim: u32) RowLayout {
        const token_values: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
        const int8_row_bytes: usize = @as(usize, num_kv_heads) * (@as(usize, head_dim) + @sizeOf(f32));
        const key_row_bytes: usize = switch (self) {
            .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
            .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim) + turboquant.turbo3ResidualBytes(num_kv_heads, head_dim),
            .raw_f32 => token_values * @sizeOf(f32),
            .f16 => token_values * @sizeOf(u16),
            .int8_per_head => int8_row_bytes,
        };
        const base_key_row_bytes: usize = switch (self) {
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
    compactable_global: bool = false,
    logical_contiguous: bool = false,
    physical_base_tokens: usize = 0,
    written_tokens: usize = 0,
    position_offset: usize = 0,
    selected_positions: ?[]u32 = null,

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
    leased_slots: [metal_runtime.attention_span_slot_capacity]bool = @as([metal_runtime.attention_span_slot_capacity]bool, @splat(false)),
    slot_ring_page_count: [metal_runtime.attention_span_slot_capacity]usize = @as([metal_runtime.attention_span_slot_capacity]usize, @splat(0)),
    slot_ring_policy_initialized: [metal_runtime.attention_span_slot_capacity]bool = @as([metal_runtime.attention_span_slot_capacity]bool, @splat(false)),
    slot_buffer_capacity_tokens: [metal_runtime.attention_span_slot_capacity]usize = @as([metal_runtime.attention_span_slot_capacity]usize, @splat(0)),
    cyclic_page_table_cache: storage_runtime.PageTokenOffsetCache = .{},
    capacity_epoch: u64 = 1,
    cold_bank: ?*ColdKvBank = null,
    capacity_owners: [metal_runtime.attention_span_slot_capacity][2]?*storage_runtime.device_capacity.BackingOwner = @splat(@splat(null)),

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

    fn discardPrivateSlot(self: *MetalKvStorage, slot: usize) void {
        if (slot >= self.leased_slots.len or !self.leased_slots[slot]) return;
        if (metal_runtime.termite_metal_decode_runtime_discard_paged_kv_slot(self.runtime, slot) != 0) {
            self.reclaimSlot(slot);
            return;
        }
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
        share_global: bool,
        compactable_global: bool,
    ) !*SlotBinding {
        var cold_sequence_slot: ?usize = null;
        if (self.cold_bank) |bank| {
            if (!bank.adopted or ring_page_count != 0 or !share_global or compactable_global or self.global_layer_slots.get(key.layer_index) == null) return error.DeviceWriteFallback;
            var found = false;
            for (bank.active_sequences, 0..) |sequence, index| {
                if (sequence == key.sequence_id) {
                    found = true;
                    break;
                }
                if (sequence == null and cold_sequence_slot == null) cold_sequence_slot = index;
            }
            if (found) cold_sequence_slot = null else if (cold_sequence_slot == null) return error.KvCapacityTooSmall;
        }
        if (self.slot_map.getPtr(key)) |binding| {
            if (binding.ring_page_count != ring_page_count) return error.DeviceWriteFallback;
            if (compactable_global and !binding.compactable_global) return error.DeviceWriteFallback;
            return binding;
        }

        const owns_slot = ring_page_count > 0 or !share_global;
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

        const value = SlotBinding{ .slot = slot, .ring_page_count = ring_page_count, .sequence_owned = owns_slot, .compactable_global = compactable_global };
        if (self.cold_bank) |bank| {
            self.slot_map.putAssumeCapacity(key, value);
            if (cold_sequence_slot) |index| bank.active_sequences[index] = key.sequence_id;
        } else try self.slot_map.put(self.allocator, key, value);
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
        return self.cyclic_page_table_cache.getMapped(self.allocator, logical_blocks, needed_blocks, page_size_tokens);
    }

    fn identityPageTokenOffsets(
        self: *MetalKvStorage,
        needed_blocks: usize,
        page_size_tokens: usize,
    ) !storage_runtime.PageTokenOffsets {
        return self.cyclic_page_table_cache.getIdentity(self.allocator, needed_blocks, page_size_tokens);
    }

    fn requirePreparedLayout(self: *const MetalKvStorage, index: usize, heads: u32, dimension: u32) !void {
        const bank = self.cold_bank orelse return;
        if (!bank.adopted) return error.PreparedKvBankConsumed;
        for (bank.rows[0..bank.count]) |row| {
            if (row.geometry.index != index) continue;
            if (row.geometry.num_kv_heads != heads or row.geometry.head_dim != dimension) return error.DeviceKvCapacityLayoutChanged;
            return;
        }
        return error.DeviceKvCapacityLayoutChanged;
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
        if (self.cold_bank) |bank| {
            if (!bank.adopted or ring_page_count != 0 or page_size_tokens != bank.window.page_size) return error.DeviceWriteFallback;
            var matches = false;
            for (bank.rows[0..bank.count]) |row| {
                if (self.global_layer_slots.get(row.geometry.index) != slot) continue;
                matches = row.key_row_bytes == key_row_bytes and row.value_row_bytes == v_row_stride * (if (self.format == .f16) @as(usize, 2) else 4);
                break;
            }
            if (!matches) return error.DeviceKvCapacityLayoutChanged;
        }
        const current = self.slot_buffer_capacity_tokens[slot];
        if (required_tokens <= current) return;
        if (self.capacity_owners[slot][0] != null or self.capacity_owners[slot][1] != null) return error.DeviceKvCapacityAdmissionRequired;
        self.capacity_epoch = try std.math.add(u64, self.capacity_epoch, 1);
        const target = if (ring_page_count > 0 or envFlagEnabled("TERMITE_METAL_DISABLE_GEOMETRIC_KV_GROWTH"))
            required_tokens
        else
            try storage_runtime.geometricKvTokenCapacity(current, required_tokens, page_size_tokens);
        const rc = metal_runtime.termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
            self.runtime,
            slot,
            @backingInt(self.format),
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
            if (key_count == 0) {
                if (self.cold_bank) |bank| for (&bank.active_sequences) |*id| {
                    if (id.* == sequence_id) id.* = null;
                };
                return;
            }
            for (keys[0..key_count]) |key| {
                if (self.slot_map.fetchRemove(key)) |removed| {
                    if (removed.value.selected_positions) |positions| self.allocator.free(positions);
                    if (removed.value.selected_positions != null) {
                        self.discardPrivateSlot(removed.value.slot);
                    } else if (removed.value.ownsSlot()) {
                        self.reclaimSlot(removed.value.slot);
                    }
                }
            }
        }
    }

    fn rowLayout(self: *const MetalKvStorage, num_kv_heads: u32, head_dim: u32) RowLayout {
        return self.format.rowLayout(num_kv_heads, head_dim);
    }

    pub fn deviceWriteHook(self: *MetalKvStorage) storage_runtime.DeviceWriteHook {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &hook_vtable,
        };
    }

    /// Borrowed synchronous recipe from this actual sequence/layer binding.
    /// All offset storage was reserved by ColdKvBank BEFORE numerical entry.
    /// Native setBytes copies <=4096 table bytes while encoding; no host slice
    /// survives this call, no per-token plan, table allocation or quota credit.
    pub const PreparedDecodeAttention = struct {
        layer: storage_runtime.DevicePagedKvLayer,
        offsets: []const u32,
        keys: *anyopaque,
        values: *anyopaque,
    };
    pub fn prepareDecodeAttention(storage: *storage_runtime.KvStorageRuntime, expected_runtime: *metal_runtime.RawMetalDecodeRuntime, attention: @import("../graph/backend_contracts.zig").AttentionContext, num_heads: usize, num_kv_heads: usize, head_dim: usize) !?PreparedDecodeAttention {
        const hook = storage.device_write_hook orelse return null;
        // Header attestation before opaque context. Custom/copied hooks keep
        // their original callback and never grant this owned recipe.
        if (hook.vtable != &hook_vtable) return null;
        const self: *MetalKvStorage = @ptrCast(@alignCast(hook.ctx));
        if (self.runtime != expected_runtime or self.format != .raw_f32 or self.page_size_tokens == 0 or num_heads == 0 or num_kv_heads == 0 or num_heads % num_kv_heads != 0) return null;
        const bank = self.cold_bank orelse return null;
        if (!bank.adopted or !bank.published or bank.store != self or bank.adopted_storage_ptr != storage.storage.ptr or bank.adopted_storage_vtable != @as(*const anyopaque, @ptrCast(storage.storage.vtable))) return null;
        if (attention.kv_storage != storage or attention.query_sequence_len != 1 or attention.mode != .paged_decode or attention.kv_batch != null or attention.attn_or_mask != null or attention.attention_sink.hasMetadata() or attention.skip_kv_write or attention.sliding_window != 0 or attention.kv_position_offset != 0 or attention.total_sequence_len == 0 or attention.kv_sequence_len != attention.total_sequence_len) return null;
        const kv = attention.kv_cache orelse return null;
        if (kv.kv_storage != storage or kv.allow_swa_ring or kv.position_offset != 0) return null;
        if (kv.pool_id != storage.poolId()) return error.InvalidPoolId;
        try self.requirePreparedLayout(attention.layer_index, std.math.cast(u32, num_kv_heads) orelse return null, std.math.cast(u32, head_dim) orelse return null);
        const binding = self.slot_map.get(.{ .sequence_id = kv.sequence_id, .layer_index = std.math.cast(u32, attention.layer_index) orelse return null }) orelse return null;
        if (binding.ring_page_count != 0 or binding.selected_positions != null or binding.position_offset != 0 or !binding.covers(attention.total_sequence_len)) return null;
        const table = storage.blockTable(kv.sequence_id) orelse return error.InvalidSequenceId;
        const page = self.page_size_tokens;
        if (storage.storage.config.page_size_tokens != page or table.tokenCount(page) != attention.total_sequence_len or kv.tail_tokens != table.tail_tokens) return error.InvalidPagedKvState;
        const blocks = try std.math.divCeil(usize, attention.total_sequence_len, page);
        if (blocks == 0 or blocks > 1024 or blocks > self.cyclic_page_table_cache.mapped_limit or blocks > self.cyclic_page_table_cache.offsets.capacity or table.blocks.items.len < blocks or kv.logical_block_count < blocks) return null;
        const info = try self.slotInfo(binding.slot);
        const layout = self.rowLayout(@intCast(num_kv_heads), @intCast(head_dim));
        if (info.key_row_bytes != layout.key_row_bytes or info.v_row_stride != layout.v_row_stride) return error.DeviceKvCapacityLayoutChanged;
        for (table.blocks.items[0..blocks]) |id| {
            const end = try std.math.mul(usize, try std.math.add(usize, id, 1), page);
            if (try std.math.mul(usize, end, layout.key_row_bytes) > info.encoded_key_capacity or try std.math.mul(usize, end, layout.v_row_stride * @sizeOf(f32)) > info.v_capacity) return error.KvCapacityTooSmall;
        }
        const layer = try pagedLayerKvDevice(self, .{ .sequence_id = kv.sequence_id, .layer_index = attention.layer_index, .token_count = attention.total_sequence_len, .num_kv_heads = @intCast(num_kv_heads), .head_dim = @intCast(head_dim) });
        const offsets = try self.pageTokenOffsets(table.blocks.items, blocks, page, 0);
        return .{ .layer = layer, .offsets = offsets.values(), .keys = info.encoded_key_handle orelse return error.InvalidSlot, .values = info.v_handle orelse return error.InvalidSlot };
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
        try self.requirePreparedLayout(write.layer_index, write.num_kv_heads, write.head_dim);
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
            write.logical_blocks != null and !write.sequence_owned_global,
            write.sequence_owned_global,
        );
        if (binding.compactable_global and write.total_token_count > try selection.defaults.transientRows())
            return error.SelectedKvCapacityExceeded;
        if (binding.selected_positions) |old_positions| {
            if (requested_ring_pages != 0) return error.SelectedKvRequiresPagedAttention;
            if (write.suffix_token_count == 0) return error.SelectedKvWriteFailed;
            if (write.total_token_count < write.suffix_token_count or
                write.total_token_count - write.suffix_token_count != binding.written_tokens) return error.StaleKvSelection;
            const compact_total = std.math.add(usize, old_positions.len, write.suffix_token_count) catch return error.KvCapacityTooSmall;
            if (compact_total > try selection.defaults.transientRows()) return error.SelectedKvCapacityExceeded;
            const positions = try self.allocator.alloc(u32, compact_total);
            errdefer self.allocator.free(positions);
            @memcpy(positions[0..old_positions.len], old_positions);
            const suffix_abs_start = std.math.add(
                usize,
                write.position_offset,
                write.total_token_count - write.suffix_token_count,
            ) catch return error.KvCapacityTooSmall;
            for (positions[old_positions.len..], 0..) |*position, index| {
                const absolute = std.math.add(usize, suffix_abs_start, index) catch return error.KvCapacityTooSmall;
                position.* = std.math.cast(u32, absolute) orelse return error.KvCapacityTooSmall;
                if (position.* <= positions[old_positions.len - 1]) return error.InvalidKvPositions;
            }
            const compact_page_size: usize = self.page_size_tokens;
            if (compact_page_size == 0) return error.SelectedKvWriteFailed;
            const block_count = std.math.divCeil(usize, compact_total, compact_page_size) catch return error.KvCapacityTooSmall;
            const block_offsets = try self.allocator.alloc(u32, block_count);
            defer self.allocator.free(block_offsets);
            for (block_offsets, 0..) |*offset, block_index| {
                const token_offset = std.math.mul(usize, block_index, compact_page_size) catch return error.KvCapacityTooSmall;
                offset.* = std.math.cast(u32, token_offset) orelse return error.KvCapacityTooSmall;
            }
            try self.reserveSlotCapacity(
                binding.slot,
                compact_total,
                compact_page_size,
                0,
                key_row_bytes,
                v_row_stride,
            );
            const selected_rc = metal_runtime.termite_metal_decode_runtime_update_attention_paged_from_f32_key_device_slot(
                self.runtime,
                binding.slot,
                @backingInt(self.format),
                k.handle,
                k.byte_offset,
                v.handle,
                v.byte_offset,
                compact_total,
                write.suffix_token_count,
                num_kv_heads,
                head_dim,
                key_row_bytes,
                base_key_row_bytes,
                v_row_stride,
                positions[0],
                block_offsets.ptr,
                block_offsets.len,
                compact_page_size,
            );
            if (selected_rc != 0) return error.SelectedKvWriteFailed;
            binding.selected_positions = positions;
            binding.written_tokens = write.total_token_count;
            binding.position_offset = positions[0];
            self.allocator.free(old_positions);
            return;
        }
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
            const block_offsets_result = if (binding.compactable_global)
                self.identityPageTokenOffsets(needed_blocks, write.page_size_tokens) catch break :paged -9999
            else
                self.pageTokenOffsets(
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
                @backingInt(self.format),
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
                @backingInt(self.format),
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
        try self.requirePreparedLayout(reserve.layer_index, reserve.num_kv_heads, reserve.head_dim);
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
            reserve.logical_blocks != null and !reserve.sequence_owned_global,
            reserve.sequence_owned_global,
        );
        // Selected donors grow by compact physical row count in the write
        // path; the caller's logical token capacity is not a valid buffer size.
        if (binding.selected_positions != null) return;
        const slot = binding.slot;
        const ring_page_count = binding.ring_page_count;
        if (traceKvGather()) std.debug.print(
            "kv-reserve: seq={d} layer={d} capacity={d} slot={d} ring_pages={d}\n",
            .{ reserve.sequence_id, reserve.layer_index, reserve.token_capacity, slot, ring_page_count },
        );
        if (ring_page_count > 0 and reserve.logical_blocks == null) return error.DeviceWriteFallback;

        const token_capacity = if (binding.compactable_global) blk: {
            binding.logical_contiguous = true;
            binding.physical_base_tokens = 0;
            break :blk reserve.token_capacity;
        } else if (ring_page_count > 0)
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
        if (binding.selected_positions != null) return error.DeviceWriteFallback;
        if (!binding.commitWrite(commit.total_token_count, commit.position_offset)) return error.DeviceWriteFallback;
    }

    fn copySharedBlockPrefixes(ctx: *anyopaque, implementation: *const anyopaque, request: storage_runtime.block_copy.Request) !void {
        if (implementation != @as(*const anyopaque, @ptrCast(&hook_vtable))) return error.SharedKvWriteUnsupported;
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        try request.validate();
        if (request.page_size != self.page_size_tokens or self.global_layer_slots.count() != request.layers) return error.SharedKvCopyLayoutChanged;
        if (metal_runtime.termite_metal_decode_runtime_has_active_frame(self.runtime) != 0 or metal_runtime.termite_metal_decode_runtime_has_submitted_frame(self.runtime) != 0) return error.SharedKvCopyDuringFrame;
        var planes: [256]metal_runtime.RawPagedCopyPlane = undefined;
        for (0..request.layers) |index| {
            const slot = self.global_layer_slots.get(@intCast(index)) orelse return error.SharedKvWriteUnsupported;
            if (slot >= self.leased_slots.len or !self.leased_slots[slot] or self.slot_ring_page_count[slot] != 0) return error.SharedKvCopyLayoutChanged;
            if (self.slot_map.get(.{ .sequence_id = request.sequence, .layer_index = @intCast(index) })) |binding| if (binding.slot != slot or binding.sequence_owned or binding.ring_page_count != 0 or binding.selected_positions != null or binding.compactable_global) return error.SharedKvWriteUnsupported;
            const info = try self.slotInfo(slot);
            if (info.encoded_key_handle == null or info.v_handle == null or info.key_row_bytes == 0 or info.v_row_stride == 0) return error.SharedKvCopyLayoutChanged;
            const value_bytes = try std.math.mul(usize, info.v_row_stride, if (self.format == .f16) @as(usize, 2) else @as(usize, 4));
            const key_capacity = try std.math.mul(usize, self.slot_buffer_capacity_tokens[slot], info.key_row_bytes);
            const value_capacity = try std.math.mul(usize, self.slot_buffer_capacity_tokens[slot], value_bytes);
            if (key_capacity == 0 or value_capacity == 0 or key_capacity > info.encoded_key_capacity or value_capacity > info.v_capacity) return error.SharedKvCopyLayoutChanged;
            for (request.items) |item| {
                const keys = try request.byteRange(item, info.key_row_bytes);
                const values = try request.byteRange(item, value_bytes);
                if (keys.source > key_capacity or keys.bytes > key_capacity - keys.source or keys.destination > key_capacity or keys.bytes > key_capacity - keys.destination or values.source > value_capacity or values.bytes > value_capacity - values.source or values.destination > value_capacity or values.bytes > value_capacity - values.destination) return error.SharedKvCopyCapacityRequired;
            }
            planes[index] = .{ .keys = info.encoded_key_handle.?, .values = info.v_handle.?, .key_row_bytes = info.key_row_bytes, .value_row_bytes = value_bytes, .key_capacity = key_capacity, .value_capacity = value_capacity };
        }
        const rc = metal_runtime.antfly_metal_copy_paged_rows_checked(self.runtime, &planes, request.layers, request.items.ptr, request.items.len, request.page_size);
        if (rc == -100) @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (rc != 0) return error.SharedKvCopyFailed;
    }

    fn planCapacity(ctx: *anyopaque, implementation: *const anyopaque, minimum_tokens: usize, page: u16, layers: []const storage_runtime.device_capacity.LayerGeometry, out: *storage_runtime.device_capacity.Plan) !void {
        if (implementation != @as(*const anyopaque, @ptrCast(&hook_vtable))) return error.DeviceKvCapacityUnsupported;
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (minimum_tokens == 0 or page != self.page_size_tokens or layers.len == 0 or layers.len > 256 or self.global_layer_slots.count() != layers.len) return error.DeviceKvCapacityUnsupported;
        if (metal_runtime.hasActiveFrame(self.runtime) or metal_runtime.hasSubmittedFrame(self.runtime)) return error.SharedKvCopyDuringFrame;
        var plan = storage_runtime.device_capacity.Plan{ .backend = .metal, .owner = @intFromPtr(self), .implementation = @intFromPtr(&hook_vtable), .epoch = self.capacity_epoch, .minimum_tokens = minimum_tokens, .page_size = page };
        for (layers) |layout| {
            const index = layout.index;
            const slot = self.global_layer_slots.get(@intCast(index)) orelse return error.DeviceKvCapacityUnsupported;
            if (slot >= self.leased_slots.len or !self.leased_slots[slot] or self.slot_ring_page_count[slot] != 0) return error.DeviceKvCapacityLayoutChanged;
            var bindings = self.slot_map.valueIterator();
            while (bindings.next()) |binding| if (binding.slot == slot and (binding.sequence_owned or binding.ring_page_count != 0 or binding.selected_positions != null or binding.compactable_global)) return error.DeviceKvCapacityUnsupported;
            const info = try self.slotInfo(slot);
            const expected = self.rowLayout(layout.num_kv_heads, layout.head_dim);
            if (info.key_row_bytes != expected.key_row_bytes or info.v_row_stride != expected.v_row_stride) return error.DeviceKvCapacityLayoutChanged;
            const keys = info.encoded_key_handle orelse return error.DeviceKvCapacityLayoutChanged;
            const values = info.v_handle orelse return error.DeviceKvCapacityLayoutChanged;
            const current = self.slot_buffer_capacity_tokens[slot];
            const target = if (minimum_tokens <= current) current else if (envFlagEnabled("TERMITE_METAL_DISABLE_GEOMETRIC_KV_GROWTH")) minimum_tokens else try storage_runtime.geometricKvTokenCapacity(current, minimum_tokens, page);
            const value_row = try std.math.mul(usize, info.v_row_stride, if (self.format == .f16) @as(usize, 2) else @as(usize, 4));
            // Reused slots can retain unequal K/V spans. Admit and replace
            // only a plane whose actual visible capacity cannot cover target.
            const required_keys = try std.math.mul(usize, target, info.key_row_bytes);
            const required_values = try std.math.mul(usize, target, value_row);
            const new_keys = if (required_keys > info.encoded_key_capacity) required_keys else 0;
            const new_values = if (required_values > info.v_capacity) required_values else 0;
            const key_bound = if (new_keys != 0) metal_runtime.antfly_metal_physical_allocation_bytes(self.runtime, new_keys) else 0;
            const value_bound = if (new_values != 0) metal_runtime.antfly_metal_physical_allocation_bytes(self.runtime, new_values) else 0;
            try plan.append(.{ .index = @intCast(index), .geometry = layout, .slot = @intCast(slot), .current_tokens = current, .target_tokens = target, .keys = .{ .handle = @intFromPtr(keys), .length = info.encoded_key_capacity, .allocated = metal_runtime.termite_metal_buffer_allocated_size(keys), .row_bytes = info.key_row_bytes, .new_length = new_keys, .new_bound = key_bound }, .values = .{ .handle = @intFromPtr(values), .length = info.v_capacity, .allocated = metal_runtime.termite_metal_buffer_allocated_size(values), .row_bytes = value_row, .new_length = new_values, .new_bound = value_bound } });
        }
        out.* = plan;
    }

    fn growCapacity(ctx: *anyopaque, implementation: *const anyopaque, plan: *const storage_runtime.device_capacity.Plan, claim: *storage_runtime.device_capacity.Claim, cleanup: *const @import("../execution/retirement_sink.zig").Epoch) !void {
        const Allocation = struct {
            fn prepare(runtime: *metal_runtime.RawMetalDecodeRuntime, planes: [*]const metal_runtime.RawKvGrowthPlane, count: usize, stage: *?*anyopaque) c_int {
                return metal_runtime.antfly_metal_kv_growth_prepare(runtime, planes, count, stage);
            }
        };
        return growCapacityUsing(ctx, implementation, plan, claim, cleanup, Allocation);
    }
    fn growCapacityUsing(ctx: *anyopaque, implementation: *const anyopaque, plan: *const storage_runtime.device_capacity.Plan, claim: *storage_runtime.device_capacity.Claim, cleanup: *const @import("../execution/retirement_sink.zig").Epoch, comptime Allocation: type) !void {
        if (implementation != @as(*const anyopaque, @ptrCast(&hook_vtable))) return error.DeviceKvCapacityUnsupported;
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        var current: storage_runtime.device_capacity.Plan = undefined;
        const geometry = try plan.requiredGeometry();
        try planCapacity(ctx, implementation, plan.minimum_tokens, plan.page_size, geometry.items(), &current);
        if (!plan.eql(&current)) return error.DeviceKvCapacityPlanChanged;
        if (plan.new_bytes == 0) {
            var changed = false;
            for (plan.layers[0..plan.count]) |layer| changed = changed or layer.target_tokens != layer.current_tokens;
            if (!changed) return;
            if (try cleanup.isSealed()) return error.DeviceKvCapacityRequiresActiveCleanupEpoch;
            const next_epoch = try std.math.add(u64, self.capacity_epoch, 1);
            for (plan.layers[0..plan.count]) |layer| self.slot_buffer_capacity_tokens[layer.slot] = layer.target_tokens;
            self.capacity_epoch = next_epoch;
            return; // Actual buffers already cover every row: no SDK or quota work.
        }
        if (try cleanup.isSealed()) return error.DeviceKvCapacityRequiresActiveCleanupEpoch;
        try claim.require(plan);
        const next_epoch = try std.math.add(u64, self.capacity_epoch, 1);
        const retirement = @import("../execution/resource_retirement.zig");
        const initialized = metal_runtime.antfly_metal_set_retirement_poll(@import("../ops/metal_compute.zig").MetalCompute.pollRetirementChargesCPU);
        if (initialized == -1) return error.OutOfMemory;
        if (initialized != 0) return error.InvalidResourceRetirement;
        var planes: [256]metal_runtime.RawKvGrowthPlane = undefined;
        for (plan.layers[0..plan.count], 0..) |layer, index| planes[index] = .{
            .slot = layer.slot,
            .old_keys = @ptrFromInt(layer.keys.handle),
            .old_values = @ptrFromInt(layer.values.handle),
            .old_key_length = layer.keys.length,
            .old_value_length = layer.values.length,
            .old_key_allocated = layer.keys.allocated,
            .old_value_allocated = layer.values.allocated,
            .new_key_length = layer.keys.new_length,
            .new_value_length = layer.values.new_length,
            .key_bound = layer.keys.new_bound,
            .value_bound = layer.values.new_bound,
        };
        var stage: ?*anyopaque = null;
        claim.attempted = true;
        const prepared = Allocation.prepare(self.runtime, &planes, plan.count, &stage);
        if (prepared == -5) return error.OutOfMemory;
        if (prepared != 0) return error.DeviceKvCapacityPreparationFailed;
        defer metal_runtime.antfly_metal_kv_growth_discard(stage);

        const Attachment = struct {
            runtime: *metal_runtime.RawMetalDecodeRuntime,
            buffer: *anyopaque,
            fn beginScope(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT) !retirement.Scope.Begun {
                const value: *@This() = @ptrCast(@alignCast(raw));
                var origin: u64 = 0;
                const pending = metal_runtime.antfly_metal_output_retirement_begin(value.runtime, &origin, retirement.Queue.createIdentity);
                if (pending < 0) return error.InvalidResourceRetirement;
                metal_runtime.antfly_metal_output_retirement_end(value.runtime);
                // These freshly allocated buffers have not been encoded yet.
                // Unrelated frame/reuse-pool batches do not own this claim.
                return .{ .origin = .{ .identity = origin }, .pending_batches = 0 };
            }
            fn end(_: *anyopaque) void {}
            fn attach(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload) !void {
                const value: *@This() = @ptrCast(@alignCast(raw));
                if (nodes.len != 0) return error.InvalidResourceRetirement;
                const buffers = [_]*anyopaque{value.buffer};
                const witnesses = metal_runtime.antfly_metal_prepare_retirement_witnesses(&buffers, buffers.len, payload) orelse return error.OutOfMemory;
                defer metal_runtime.antfly_metal_discard_retirement_witnesses(witnesses);
                if (metal_runtime.antfly_metal_buffer_retirement_attach(witnesses) != 0) return error.InvalidResourceRetirement;
            }
            fn attachPrepared(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload, metadata: *retirement.BackendPreparation) !void {
                const value: *@This() = @ptrCast(@alignCast(raw));
                if (nodes.len != 0) return error.InvalidResourceRetirement;
                const buffers = [_]*anyopaque{value.buffer};
                const witnesses = try metadata.armMetal(null, &buffers, payload);
                defer metal_runtime.antfly_metal_discard_retirement_witnesses(witnesses);
                if (metal_runtime.antfly_metal_buffer_retirement_attach(witnesses) != 0) return error.InvalidResourceRetirement;
            }
            const table = retirement.Scope.VTable{ .begin = beginScope, .attach = attach, .attach_prepared = attachPrepared, .end = end, .notify = metal_runtime.antfly_metal_request_retirement_poll };
        };
        for (plan.layers[0..plan.count], 0..) |layer, index| {
            const planned_planes = [_]storage_runtime.device_capacity.Plane{ layer.keys, layer.values };
            for (planned_planes, 0..) |plane, which| if (plane.new_length != 0) {
                const buffer = metal_runtime.antfly_metal_kv_growth_buffer(stage.?, index, which) orelse return error.InvalidResourceRetirement;
                const owner = claim.ownerAt(index, which);
                owner.measure(cleanup, metal_runtime.termite_metal_buffer_allocated_size(buffer));
                var attachment = Attachment{ .runtime = self.runtime, .buffer = buffer };
                const begun = try Attachment.beginScope(&attachment, &.{});
                var scope = retirement.Scope{ .context = &attachment, .origin = begun.origin, .pending_batches = begun.pending_batches, .outputs = &.{}, .active = true, .vtable = &Attachment.table };
                defer scope.end();
                if (begun.pending_batches != 0) return error.SharedKvCopyDuringFrame;
                try owner.charge.attach(&scope);
                scope.end();
            };
        }
        const copied = metal_runtime.antfly_metal_kv_growth_copy_checked(stage);
        if (copied == -100) @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (copied != 0) return error.DeviceKvCapacityCopyFailed;
        metal_runtime.antfly_metal_kv_growth_publish(stage);
        for (plan.layers[0..plan.count], 0..) |layer, index| {
            const planned_planes = [_]storage_runtime.device_capacity.Plane{ layer.keys, layer.values };
            for (planned_planes, 0..) |plane, which| if (plane.new_length != 0) {
                if (self.capacity_owners[layer.slot][which]) |previous| previous.release(cleanup);
                self.capacity_owners[layer.slot][which] = claim.takeOwner(index, which);
            };
            self.slot_buffer_capacity_tokens[layer.slot] = layer.target_tokens;
        }
        self.capacity_epoch = next_epoch;
        claim.deinit(cleanup);
    }
    fn repackSelectedGlobalKv(
        ctx: *anyopaque,
        repack: storage_runtime.SelectedGlobalKvRepack,
    ) anyerror!storage_runtime.DevicePagedKvLayer {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (repack.key_positions.len == 0 or repack.key_positions.len != repack.source_token_offsets.len)
            return error.InvalidKvShape;
        if (repack.num_kv_heads != self.num_kv_heads or repack.head_dim != self.head_dim)
            return error.InvalidKvShape;
        for (repack.key_positions[1..], repack.key_positions[0 .. repack.key_positions.len - 1]) |position, previous| {
            if (previous >= position) return error.InvalidKvPositions;
        }
        const key = SlotKey{
            .sequence_id = repack.sequence_id,
            .layer_index = std.math.cast(u32, repack.layer_index) orelse return error.InvalidKvShape,
        };
        const binding = self.slot_map.getPtr(key) orelse return error.DeviceReadFallback;
        if (binding.slot != repack.expected_source_slot or binding.written_tokens != repack.expected_written_tokens)
            return error.StaleKvSelection;
        if (binding.ring_page_count != 0 or !binding.compactable_global or !binding.sequence_owned)
            return error.DeviceWriteFallback;
        const source_physical_rows = if (binding.selected_positions) |positions| positions.len else binding.written_tokens;
        try validateSelectedSourceRows(repack.source_token_offsets, source_physical_rows);

        const layout = self.rowLayout(repack.num_kv_heads, repack.head_dim);
        const positions = try self.allocator.dupe(u32, repack.key_positions);
        errdefer self.allocator.free(positions);
        const destination_slot = try self.allocateSlot();
        errdefer self.discardPrivateSlot(destination_slot);
        const destination_capacity = try selection.defaults.transientRows();
        if (positions.len > destination_capacity) return error.SelectedKvCapacityExceeded;
        const reserve_rc = metal_runtime.termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
            self.runtime,
            destination_slot,
            @backingInt(self.format),
            destination_capacity,
            layout.key_row_bytes,
            layout.v_row_stride,
        );
        if (reserve_rc != 0) return error.DeviceWriteFallback;
        self.slot_buffer_capacity_tokens[destination_slot] = destination_capacity;
        self.slot_ring_page_count[destination_slot] = 0;
        self.slot_ring_policy_initialized[destination_slot] = true;

        const compact_rc = metal_runtime.termite_metal_decode_runtime_compact_attention_span_slot(
            self.runtime,
            binding.slot,
            destination_slot,
            @backingInt(self.format),
            repack.source_token_offsets.ptr,
            repack.source_token_offsets.len,
            layout.key_row_bytes,
            layout.v_row_stride,
            positions[0],
        );
        if (compact_rc != 0) return error.DeviceWriteFallback;

        // No allocation or fallible operation follows this point. Publish the
        // fully copied staging slot, then release only a sequence-owned source;
        // shared global page pools remain available to other bindings/cache
        // owners.
        const source_slot = binding.slot;
        const source_owned = binding.sequence_owned;
        const old_positions = binding.selected_positions;
        binding.* = .{
            .slot = destination_slot,
            .sequence_owned = true,
            .compactable_global = true,
            .logical_contiguous = true,
            .physical_base_tokens = 0,
            .written_tokens = repack.expected_written_tokens,
            .position_offset = positions[0],
            .selected_positions = positions,
        };
        if (old_positions) |old| self.allocator.free(old);
        if (source_owned) self.discardPrivateSlot(source_slot);

        return .{
            .runtime = @ptrCast(self.runtime),
            .slot = destination_slot,
            .format = @backingInt(self.format),
            .token_count = positions.len,
            .key_row_bytes = layout.key_row_bytes,
            .base_key_row_bytes = layout.base_key_row_bytes,
            .v_row_stride = layout.v_row_stride,
            .page_size_tokens = self.page_size_tokens,
            .position_offset = positions[0],
            .key_positions = positions,
        };
    }

    fn scoreSelectedAttention(ctx: *anyopaque, request: storage_runtime.SelectedAttentionScore) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const binding = self.slot_map.get(.{ .sequence_id = request.sequence_id, .layer_index = @intCast(request.layer_index) }) orelse return error.DeviceReadFallback;
        if (request.key_positions.len == 0 or request.scores_out.len != request.key_positions.len) return error.InvalidKvShape;
        if (request.query_positions.len == 0 or request.queries.len != request.query_positions.len * request.num_heads * request.head_dim) return error.InvalidKvShape;
        if (request.query_positions.len > selection.defaults.query_horizon or
            request.key_positions.len > try selection.defaults.transientRows()) return error.InvalidKvShape;
        for (request.query_positions[1..], request.query_positions[0 .. request.query_positions.len - 1]) |position, previous| {
            if (position <= previous) return error.InvalidKvShape;
        }
        for (request.key_positions[1..], request.key_positions[0 .. request.key_positions.len - 1]) |position, previous| {
            if (position <= previous) return error.InvalidKvShape;
        }
        if (binding.selected_positions) |positions| {
            if (!std.mem.eql(u32, positions, request.key_positions)) return error.StaleKvSelection;
        } else if (request.key_positions.len != binding.written_tokens) return error.StaleKvSelection;
        const layout = self.rowLayout(@intCast(request.num_kv_heads), @intCast(request.head_dim));
        const rc = metal_runtime.termite_metal_decode_runtime_score_selected_attention(
            self.runtime,
            binding.slot,
            @backingInt(self.format),
            request.queries.ptr,
            request.query_positions.ptr,
            request.query_positions.len,
            request.key_positions.ptr,
            request.key_positions.len,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            layout.key_row_bytes,
            layout.base_key_row_bytes,
            request.score_scale,
            request.softcap,
            request.scores_out.ptr,
            request.check_ctx,
            request.check_fn,
        );
        if (rc == -12) return error.Cancelled;
        if (rc != 0) return error.DeviceReadFallback;
        var score_sum: f64 = 0;
        for (request.scores_out) |score| {
            if (!std.math.isFinite(score) or score < 0) return error.InvalidSelectionScore;
            score_sum += score;
        }
        // Every captured query has at least sink/recent causal keys, so a
        // normalized consumer distribution must sum to one. A shortfall also
        // surfaces any nonfinite encoded key rejected by pass one.
        if (@abs(score_sum - 1.0) > 2e-3) return error.InvalidSelectionScore;
    }

    fn validateSelectedSourceRows(source_rows: []const u32, committed_physical_rows: usize) !void {
        if (source_rows.len == 0 or committed_physical_rows == 0) return error.InvalidKvShape;
        var previous: ?u32 = null;
        for (source_rows) |row| {
            if (row >= committed_physical_rows) return error.InvalidKvShape;
            if (previous) |prior| if (row <= prior) return error.InvalidKvShape;
            previous = row;
        }
    }

    fn hookDeinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        hookDeinitWithBoundary(ctx, allocator, null);
    }
    fn hookDeinitWithBoundary(ctx: *anyopaque, _: std.mem.Allocator, cleanup: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const allocator = self.allocator;
        const cold_owner = self.cold_bank;
        defer if (cold_owner) |bank| {
            bank.store = null;
        };
        for (self.capacity_owners) |pair| for (pair) |owner| if (owner != null) {
            const epoch = cleanup orelse @import("antfly_platform").inference_process_supervisor.restartWorker();
            if (epoch.isSealed() catch true) @import("antfly_platform").inference_process_supervisor.restartWorker();
            if (metal_runtime.hasActiveFrame(self.runtime) or metal_runtime.hasSubmittedFrame(self.runtime)) @import("antfly_platform").inference_process_supervisor.restartWorker();
        };
        var binding_it = self.slot_map.valueIterator();
        while (binding_it.next()) |binding| {
            if (binding.selected_positions) |positions| allocator.free(positions);
            if (binding.selected_positions != null and binding.sequence_owned) self.discardPrivateSlot(binding.slot);
        }
        for (&self.leased_slots, 0..) |*leased, slot| {
            if (!leased.*) continue;
            // Independently charged backings must reach actual SDK death;
            // ordinary uncharged slots retain their existing capacity cache.
            const charged = self.capacity_owners[slot][0] != null or self.capacity_owners[slot][1] != null;
            const released = if (charged)
                metal_runtime.termite_metal_decode_runtime_discard_paged_kv_slot(self.runtime, slot)
            else
                metal_runtime.termite_metal_decode_runtime_release_paged_kv_slot(self.runtime, slot);
            for (&self.capacity_owners[slot]) |*owner| if (owner.*) |value| {
                if (released != 0) @import("antfly_platform").inference_process_supervisor.restartWorker();
                value.release(cleanup.?);
                owner.* = null;
            };
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

    /// The storage/runtime ABI can stage and atomically publish compacted K/V
    /// rows with an explicit absolute-position table. Production admission is
    /// a separate pipeline qualification decision; this bit only describes
    /// the implemented backend capability.
    fn supportsSelectedGlobalKv(_: *const anyopaque) bool {
        return true;
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
        if (binding.selected_positions != null) return error.SelectedKvRequiresPagedAttention;
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
                    try turboquant.decodeTurbo3KeyWithResidual(row_bytes[0..base_bytes], row_bytes[base_bytes..], row_dst, num_kv_heads, head_dim);
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

    fn gatherLayerKeys(
        ctx: *anyopaque,
        gather: storage_runtime.KvLayerGather,
        k_out: []f32,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const token_width: usize = @as(usize, gather.num_kv_heads) * @as(usize, gather.head_dim);
        if (k_out.len < gather.token_count * token_width) return error.InvalidKvShape;
        const binding = self.slot_map.get(.{
            .sequence_id = gather.sequence_id,
            .layer_index = @intCast(gather.layer_index),
        }) orelse return error.DeviceReadFallback;
        if (binding.ring_page_count > 0 or !binding.logical_contiguous or binding.written_tokens < gather.token_count)
            return error.DeviceReadFallback;
        const info = try self.slotInfo(binding.slot);
        const handle = info.encoded_key_handle orelse return error.DeviceReadFallback;
        const byte_count = gather.token_count * info.key_row_bytes;
        const byte_offset = binding.physical_base_tokens * info.key_row_bytes;
        if (byte_offset + byte_count > info.encoded_key_capacity) return error.DeviceReadFallback;
        const encoded = try self.allocator.alloc(u8, byte_count);
        defer self.allocator.free(encoded);
        if (metal_runtime.termite_metal_buffer_download(self.runtime, handle, byte_offset, @ptrCast(encoded.ptr), byte_count) != 0)
            return error.MetalDeviceReadFailed;
        for (0..gather.token_count) |row| {
            const src = encoded[row * info.key_row_bytes ..][0..info.key_row_bytes];
            const dst = k_out[row * token_width ..][0..token_width];
            switch (self.format) {
                .polar4 => try turboquant.decodePolar4Key(src, dst, gather.num_kv_heads, gather.head_dim),
                .turbo3 => {
                    const base_bytes = turboquant.turbo3KeyBytes(gather.num_kv_heads, gather.head_dim);
                    try turboquant.decodeTurbo3KeyWithResidual(src[0..base_bytes], src[base_bytes..], dst, gather.num_kv_heads, gather.head_dim);
                },
                .raw_f32 => {
                    const values: [*]const f32 = @ptrCast(@alignCast(src.ptr));
                    @memcpy(dst, values[0..token_width]);
                },
                .f16 => {
                    const values: [*]const f16 = @ptrCast(@alignCast(src.ptr));
                    for (0..token_width) |i| dst[i] = @floatCast(values[i]);
                },
                .int8_per_head => pool_mod.dequantizeInt8PerHeadToF32(src, dst, gather.num_kv_heads, gather.head_dim),
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
        // DeviceKvLayer has no absolute-position channel. Returning a compact
        // span would silently reinterpret selected rows as contiguous history.
        if (binding.selected_positions != null) return error.SelectedKvRequiresPagedAttention;
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
            .token_count = if (binding.selected_positions) |positions| positions.len else gather.token_count,
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
            .format = @backingInt(self.format),
            .token_count = if (binding.selected_positions) |positions| positions.len else gather.token_count,
            .key_row_bytes = key_row_bytes,
            .base_key_row_bytes = base_key_row_bytes,
            .v_row_stride = token_width,
            .page_size_tokens = self.page_size_tokens,
            .ring_page_count = ring_page_count,
            .position_offset = binding.position_offset,
            .key_positions = binding.selected_positions,
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

    /// Initial full-history bank. All numerical work retains the existing
    /// device hook and codec schedules. Preparation owns only atomically leased
    /// slots and an independent MTLDevice; publication uses the actual Gate.
    pub const ColdKvBank = struct {
        const capacity = storage_runtime.device_capacity;
        const banks = @import("../execution/prepared_kv_banks.zig");
        const cold = @import("cold_compute_target.zig");
        const sink = @import("../execution/retirement_sink.zig");
        const storage_admission = @import("../execution/storage_admission.zig");
        const retirement = @import("../execution/resource_retirement.zig");
        const platform = @import("antfly_platform");
        const Control = @import("../execution_control.zig").InferenceExecutionControl;
        allocator: std.mem.Allocator,
        retained: cold.Retained,
        ticket: @import("../execution/operation_gate.zig").Gate.Ticket,
        store: ?*MetalKvStorage = null,
        window: capacity.Window,
        dtype: pool_mod.KvDType,
        rows: [256]banks.EncodedLayer = undefined,
        count: usize = 0,
        adopted: bool = false,
        published: bool = false,
        active_sequences: [128]?storage_runtime.SequenceId = @splat(null),
        // Stable actual storage owner, not the movable KvStorageRuntime address.
        adopted_storage_ptr: ?*anyopaque = null,
        adopted_storage_vtable: ?*const anyopaque = null,

        const Budget = struct {
            reservation: ?storage_admission.Reservation,
            fn take(raw: *anyopaque, allocator: std.mem.Allocator, bytes: usize) !storage_admission.Reservation {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.reservation.?.partition(allocator, bytes);
            }
        };
        const Attachment = struct {
            buffer: *anyopaque,
            origin: retirement.Queue,
            fn begin(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT) !retirement.Scope.Begun {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return .{ .origin = self.origin, .pending_batches = 0 };
            }
            fn attach(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (nodes.len != 0) return error.InvalidResourceRetirement;
                const handles = [_]*anyopaque{self.buffer};
                const prepared = metal_runtime.antfly_metal_prepare_retirement_witnesses(&handles, 1, payload) orelse return error.OutOfMemory;
                defer metal_runtime.antfly_metal_discard_retirement_witnesses(prepared);
                if (metal_runtime.antfly_metal_buffer_retirement_attach(prepared) != 0) return error.InvalidResourceRetirement;
            }
            fn attachPrepared(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload, metadata: *retirement.BackendPreparation) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (nodes.len != 0) return error.InvalidResourceRetirement;
                const handles = [_]*anyopaque{self.buffer};
                const witnesses = try metadata.armMetal(null, &handles, payload);
                defer metal_runtime.antfly_metal_discard_retirement_witnesses(witnesses);
                if (metal_runtime.antfly_metal_buffer_retirement_attach(witnesses) != 0) return error.InvalidResourceRetirement;
            }
            fn end(_: *anyopaque) void {}
            const table = retirement.Scope.VTable{ .begin = begin, .attach = attach, .attach_prepared = attachPrepared, .end = end, .notify = metal_runtime.antfly_metal_request_retirement_poll };
            fn bind(self: *@This(), owner: *capacity.BackingOwner) !void {
                var scope = retirement.Scope{ .context = self, .origin = self.origin, .pending_batches = 0, .outputs = &.{}, .active = true, .vtable = &table };
                defer scope.end();
                try owner.charge.attach(&scope);
            }
        };

        /// Pure pre-join description: no SDK handles, allocation or lane access.
        pub fn describeCoverage(target: cold.Target, dtype: pool_mod.KvDType, window: capacity.Window, mode: banks.Mode, geometry: []const capacity.LayerGeometry) !?banks.Descriptor {
            switch (target) {
                .metal => {},
                else => return null,
            }
            if (mode != .shared_physical_pages) return null;
            const format = KeyFormat.fromKvDType(dtype) orelse return null;
            if (geometry.len == 0 or geometry.len > 256 or window.live_sequences > 128) return null;
            _ = try window.sharedTokens();
            var result = banks.Descriptor{ .target = target, .dtype = dtype, .window = window, .mode = .shared_physical_pages };
            for (geometry) |layer| {
                _ = try std.math.mul(usize, try layer.width(), @sizeOf(f32));
                const layout = format.rowLayout(layer.num_kv_heads, layer.head_dim);
                const values = try std.math.mul(usize, layout.v_row_stride, if (format == .f16) @as(usize, 2) else 4);
                try result.append(.{ .geometry = layer, .key_row_bytes = layout.key_row_bytes, .base_key_row_bytes = layout.base_key_row_bytes, .value_row_bytes = values, .codec_words = .{ @backingInt(format), if (format == .f16) 1 else 0, 0, 0 } });
            }
            return result;
        }

        pub fn prepare(retained: *cold.Retained, dtype: pool_mod.KvDType, window: capacity.Window, mode: banks.Mode, geometry: []const capacity.LayerGeometry, admission: capacity.Admission, control: Control, epoch: *const sink.Epoch) !?*ColdKvBank {
            const Physical = struct {
                fn alloc(device: *anyopaque, bytes: usize) ?*anyopaque {
                    return metal_runtime.antfly_metal_standalone_buffer_alloc(device, bytes);
                }
                fn free(buffer: *anyopaque) void {
                    metal_runtime.termite_metal_buffer_release(buffer);
                }
                fn releaseDevice(device: *anyopaque) void {
                    metal_runtime.antfly_metal_standalone_device_release(device);
                }
            };
            return prepareUsing(retained, dtype, window, mode, geometry, admission, control, epoch, Physical);
        }
        fn prepareUsing(retained: *cold.Retained, dtype: pool_mod.KvDType, window: capacity.Window, mode: banks.Mode, geometry: []const capacity.LayerGeometry, admission: capacity.Admission, control: Control, epoch: *const sink.Epoch, comptime Allocation: type) !?*ColdKvBank {
            const target = switch (retained.target) {
                .metal => |value| value,
                else => return null,
            };
            if (geometry.len == 0 or geometry.len > 256 or admission.domain == null or window.live_sequences > 128) return null;
            const descriptor = (try describeCoverage(retained.target, dtype, window, mode, geometry)) orelse return null;
            const model = retained.model orelse return error.PreparedKvBankConsumed;
            if (control.native_call_policy == .require_supervision and model.close_protection == null) return null;
            try control.check();
            if (try epoch.isSealed()) return error.DeviceKvCapacityRequiresActiveCleanupEpoch;
            const tokens = try window.sharedTokens();
            const a = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
            var ticket = try target.exclusion.prepare();
            var ticket_owned = true;
            errdefer if (ticket_owned) ticket.release() catch platform.inference_process_supervisor.restartWorker();
            const self = try a.create(ColdKvBank);
            self.* = .{ .allocator = a, .retained = retained.*, .ticket = ticket, .window = window, .dtype = dtype, .rows = descriptor.inline_rows, .count = descriptor.row_count };
            _ = model.retain(); // Independent construction pin; errors preserve caller ownership.
            ticket_owned = false;
            errdefer self.abortOutsideLanes(epoch);
            const runtime: *metal_runtime.RawMetalDecodeRuntime = @ptrCast(@alignCast(target.runtime));
            const store = try MetalKvStorage.create(a, runtime, dtype, geometry[0].num_kv_heads, geometry[0].head_dim, window.page_size);
            self.store = store;
            try store.global_layer_slots.ensureTotalCapacity(a, @intCast(geometry.len));
            try store.slot_map.ensureTotalCapacity(a, std.math.cast(u32, try std.math.mul(usize, geometry.len, window.live_sequences)) orelse return error.Overflow);
            try store.cyclic_page_table_cache.reserveMapped(a, try window.blocksPerSequence());
            var metadata_guard = try control.enterUninterruptible(.uninterruptible);
            defer metadata_guard.deinit();
            var plan = capacity.Plan{ .backend = .metal, .owner = @intFromPtr(store), .implementation = @intFromPtr(&hook_vtable), .epoch = store.capacity_epoch, .minimum_tokens = tokens, .page_size = window.page_size };
            var slots: [256]metal_runtime.RawInitialKvSlot = undefined;
            var fresh: [256][2]?*anyopaque = @splat(@splat(null));
            var old_spans: [512]usize = undefined;
            var old_handles: [512]*anyopaque = undefined;
            var old_count: usize = 0;
            var old_bytes: usize = 0;
            for (geometry, 0..) |layer, index| {
                try control.check();
                const slot = try store.allocateSlot();
                store.global_layer_slots.putAssumeCapacity(layer.index, slot);
                store.slot_ring_policy_initialized[slot] = true;
                const before = try store.slotInfo(slot);
                if (before.tokens != 0) return error.DeviceKvCapacityLayoutChanged;
                const layout = store.rowLayout(layer.num_kv_heads, layer.head_dim);
                const value_row = try std.math.mul(usize, layout.v_row_stride, if (store.format == .f16) @as(usize, 2) else 4);
                const required = [2]usize{ try std.math.mul(usize, tokens, layout.key_row_bytes), try std.math.mul(usize, tokens, value_row) };
                const old = [2]?*anyopaque{ before.encoded_key_handle, before.v_handle };
                var planes: [2]capacity.Plane = undefined;
                for (old, 0..) |handle, which| {
                    const bytes = if (handle) |h| metal_runtime.antfly_metal_standalone_buffer_length(h) else 0;
                    const allocated = if (handle) |h| metal_runtime.termite_metal_buffer_allocated_size(h) else 0;
                    const reuse = handle != null and bytes >= required[which];
                    const length = if (reuse) bytes else required[which];
                    const bound = if (reuse) allocated else metal_runtime.antfly_metal_physical_allocation_bytes(runtime, length);
                    if (bound < length or length == 0) return error.InvalidDeviceCapacityLayout;
                    planes[which] = .{ .handle = if (handle) |h| @intFromPtr(h) else 0, .length = 0, .allocated = 0, .row_bytes = if (which == 0) layout.key_row_bytes else value_row, .new_length = length, .new_bound = bound };
                    if (!reuse and handle != null) {
                        old_spans[old_count] = allocated;
                        old_handles[old_count] = handle.?;
                        old_count += 1;
                        old_bytes = try std.math.add(usize, old_bytes, allocated);
                    }
                }
                try plan.appendCold(.{ .index = layer.index, .geometry = layer, .slot = @intCast(slot), .current_tokens = 0, .target_tokens = tokens, .keys = planes[0], .values = planes[1] });
                slots[index] = .{ .slot = slot, .old_keys = old[0], .old_values = old[1], .old_key_capacity = before.encoded_key_capacity, .old_value_capacity = before.v_capacity, .old_key_allocated = if (old[0]) |h| metal_runtime.termite_metal_buffer_allocated_size(h) else 0, .old_value_allocated = if (old[1]) |h| metal_runtime.termite_metal_buffer_allocated_size(h) else 0, .keys = undefined, .values = undefined, .key_length = planes[0].new_length, .value_length = planes[1].new_length, .key_bound = planes[0].new_bound, .value_bound = planes[1].new_bound };
            }
            metadata_guard.deinit(); // External admission callbacks own no SDK interval.
            // Old cached buffers are unadmitted: grant ZERO old-capacity credit.
            // Reserve full new/reused ownership plus actual replacement overlap.
            const total = try std.math.add(usize, plan.new_bytes, old_bytes);
            var budget = Budget{ .reservation = try admission.reserve(admission.context, a, total) };
            defer if (budget.reservation) |*r| r.release();
            if (!budget.reservation.?.identity.eql(admission.identity) or !std.meta.eql(budget.reservation.?.amounts(), @import("../runtime/tier/memory.zig").AdmissionAmounts{ .backend_kv_bytes = total })) return error.InvalidDeviceCapacityAdmission;
            var claim = try capacity.Claim.prepare(&plan, .{ .identity = admission.identity, .context = &budget, .reserve = Budget.take, .domain = admission.domain });
            defer claim.deinit(epoch);
            var retiring: ?*ColdRetiringBackings = null;
            if (old_count != 0) {
                var old_part: ?storage_admission.Reservation = try budget.reservation.?.partition(a, old_bytes);
                defer if (old_part) |*r| r.release();
                retiring = try ColdRetiringBackings.create(a, &old_part, old_spans[0..old_count], admission.domain.?);
            }
            defer if (retiring) |r| r.finish(epoch);
            if (budget.reservation.?.reservedBytes() != 0) return error.InvalidDeviceCapacityAdmission;
            var sdk_guard = try control.enterUninterruptible(.uninterruptible);
            defer sdk_guard.deinit();
            const device = metal_runtime.antfly_metal_standalone_device_retain(runtime) orelse return error.RuntimeUnavailable;
            defer Allocation.releaseDevice(device);
            defer for (fresh[0..geometry.len]) |pair| for (pair) |handle| if (handle) |buffer| Allocation.free(buffer);
            const poll = metal_runtime.antfly_metal_set_retirement_poll(@import("../ops/metal_compute.zig").MetalCompute.pollRetirementChargesCPU);
            if (poll != 0) return error.InvalidResourceRetirement;
            // Independent SDK allocation does not touch mutable runtime/frame,
            // reuse pools, numerical gates or callbacks into the quota producer.
            for (plan.layers[0..plan.count], 0..) |layer, index| {
                const previous = [2]?*anyopaque{ slots[index].old_keys, slots[index].old_values };
                const planes = [2]capacity.Plane{ layer.keys, layer.values };
                var chosen: [2]*anyopaque = undefined;
                for (planes, previous, 0..) |plane, old, which| {
                    try control.check();
                    const reuse = old != null and metal_runtime.antfly_metal_standalone_buffer_length(old.?) >= plane.new_length;
                    const buffer = if (reuse) old.? else Allocation.alloc(device, plane.new_length) orelse return error.OutOfMemory;
                    if (!reuse) fresh[index][which] = buffer;
                    chosen[which] = buffer;
                    claim.ownerAt(index, which).measure(epoch, metal_runtime.termite_metal_buffer_allocated_size(buffer));
                }
                slots[index].keys = chosen[0];
                slots[index].values = chosen[1];
            }
            // Publication alone takes the actual provider Gate, before either
            // long-lived reader lane. Every failure before publication discards
            // the leased cache slots before any charge root can retire.
            try self.ticket.begin();
            defer self.ticket.end() catch platform.inference_process_supervisor.restartWorker();
            if (metal_runtime.hasActiveFrame(runtime) or metal_runtime.hasSubmittedFrame(runtime)) return error.SharedKvCopyDuringFrame;
            const origin = retirement.Queue.init();
            // Roots in claim/retiring remain live until after this Gate unwinds.
            if (retiring) |r| for (r.owners, old_handles[0..old_count]) |*owner, buffer| {
                var attachment = Attachment{ .buffer = buffer, .origin = origin };
                try attachment.bind(owner);
            };
            for (slots[0..self.count], 0..) |slot, index| {
                const buffers = [2]*anyopaque{ slot.keys, slot.values };
                for (buffers, 0..) |buffer, which| {
                    var attachment = Attachment{ .buffer = buffer, .origin = origin };
                    try attachment.bind(claim.ownerAt(index, which));
                }
            }
            if (metal_runtime.antfly_metal_initialize_leased_kv_slots(runtime, &slots, self.count) != 0) return error.DeviceKvCapacityPreparationFailed;
            self.published = true;
            for (plan.layers[0..plan.count], 0..) |layer, index| {
                store.capacity_owners[layer.slot] = .{ claim.takeOwner(index, 0), claim.takeOwner(index, 1) };
                store.slot_buffer_capacity_tokens[layer.slot] = tokens;
            }
            store.cold_bank = self;
            // The bank's separate pin makes this guaranteed nonfinal/atomic.
            // Consume the original only after every fallible step succeeded.
            retained.deinitWithBoundary(epoch);
            return self;
        }

        pub fn snapshotCoverage(self: *const ColdKvBank) banks.Coverage {
            return .{ .target = self.retained.target, .window = self.window, .dtype = self.dtype, .mode = .shared_physical_pages, .rows = self.rows[0..self.count] };
        }
        pub fn adopt(self: *ColdKvBank, backend: *const @import("../ops/ops.zig").ComputeBackend, runtime: *storage_runtime.KvStorageRuntime) !void {
            if (self.adopted or !self.published) return error.PreparedKvBankConsumed;
            const target = self.retained.target.metal;
            const MetalCompute = @import("../ops/metal_compute.zig").MetalCompute;
            const identity = MetalCompute.preparedBackendIdentity(backend) orelse return error.DeviceKvCapacityLayoutChanged;
            const compute: *MetalCompute = @ptrCast(@alignCast(identity.context));
            if (compute.provider_impl.raw_decode_runtime != @as(?*metal_runtime.RawMetalDecodeRuntime, @ptrCast(@alignCast(target.runtime))) or &compute.provider_impl.operations.gate != target.exclusion) return error.DeviceKvCapacityLayoutChanged;
            const operation = backend.operationToken() orelse return error.BackendOperationNotActive;
            try operation.validate(.{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = null, .mode = .managed });
            // Caller must install only into an empty, already-built runtime.
            if (runtime.device_write_hook != null or runtime.sequences.items.len != 0 or runtime.storage.config.dtype != self.dtype or runtime.storage.config.page_size_tokens != self.window.page_size) return error.PreparedKvBankTargetNotEmpty;
            runtime.setDeviceWriteHook(self.store.?.deviceWriteHook());
            self.adopted_storage_ptr = runtime.storage.ptr;
            self.adopted_storage_vtable = @ptrCast(runtime.storage.vtable);
            self.adopted = true;
        }
        pub fn abortOutsideLanes(self: *ColdKvBank, epoch: *const sink.Epoch) void {
            const model = self.retained.model orelse platform.inference_process_supervisor.restartWorker();
            var close = model.beginClose();
            defer close.deinit();
            if (self.adopted) platform.inference_process_supervisor.restartWorker();
            while (true) {
                self.ticket.begin() catch |err| switch (err) {
                    error.QueueFull => {
                        platform.time.sleepNs(100 * std.time.ns_per_us);
                        continue;
                    },
                    else => platform.inference_process_supervisor.restartWorker(),
                };
                break;
            }
            if (self.store) |store| {
                // Preparation failures may have attached witnesses to cached
                // uncharged planes. Force actual SDK death under exclusion.
                if (!self.published) for (store.leased_slots, 0..) |leased, slot| if (leased) {
                    if (metal_runtime.termite_metal_decode_runtime_discard_paged_kv_slot(store.runtime, slot) != 0) platform.inference_process_supervisor.restartWorker();
                    store.leased_slots[slot] = false;
                };
                hookDeinitWithBoundary(store, store.allocator, epoch);
                self.store = null;
            }
            self.ticket.end() catch platform.inference_process_supervisor.restartWorker();
            self.ticket.release() catch platform.inference_process_supervisor.restartWorker();
            self.retained.deinitWithBoundary(epoch);
            self.allocator.destroy(self);
        }
        pub fn finishAfterUnlock(self: *ColdKvBank, epoch: *const sink.Epoch) void {
            if (!self.adopted or self.store != null) platform.inference_process_supervisor.restartWorker();
            self.ticket.release() catch platform.inference_process_supervisor.restartWorker();
            self.retained.deinitWithBoundary(epoch);
            self.allocator.destroy(self);
        }
    };

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
    .gatherLayerKeys = MetalKvStorage.gatherLayerKeys,
    .gatherLayerKvDevice = MetalKvStorage.gatherLayerKvDevice,
    .pagedLayerKvDevice = MetalKvStorage.pagedLayerKvDevice,
    .reserveLayerKvDevice = MetalKvStorage.reserveLayerKvDevice,
    .commitLayerKvDeviceWrite = MetalKvStorage.commitLayerKvDeviceWrite,
    .repackSelectedGlobalKv = MetalKvStorage.repackSelectedGlobalKv,
    .copySharedBlockPrefixes = MetalKvStorage.copySharedBlockPrefixes,
    .planCapacity = MetalKvStorage.planCapacity,
    .growCapacity = MetalKvStorage.growCapacity,
    .deinitWithBoundary = MetalKvStorage.hookDeinitWithBoundary,
    .scoreSelectedAttention = MetalKvStorage.scoreSelectedAttention,
    .truncateSequence = MetalKvStorage.truncateSequenceOp,
    .releaseSequence = MetalKvStorage.releaseSequenceOp,
    .supportsSelectedGlobalKv = MetalKvStorage.supportsSelectedGlobalKv,
    .deinit = MetalKvStorage.hookDeinit,
};

test "Metal KV hook teardown uses its allocation owner instead of the request allocator" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const Bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
    var owner = Bounded{ .backing = std.testing.allocator, .limit = 128 * 1024 };
    var request = Bounded{ .backing = std.testing.allocator, .limit = 128 * 1024 };
    const runtime: *metal_runtime.RawMetalDecodeRuntime = @ptrFromInt(@alignOf(usize));
    const storage = try MetalKvStorage.create(owner.allocator(), runtime, .f16, 2, 256, 16);
    storage.deviceWriteHook().deinit(request.allocator());
    try std.testing.expectEqual(@as(usize, 0), owner.live);
    try std.testing.expectEqual(@as(usize, 0), request.live);
}

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

test "selected Metal repack validates committed physical source rows" {
    try MetalKvStorage.validateSelectedSourceRows(&.{ 0, 3, 7 }, 8);
    try std.testing.expectError(error.InvalidKvShape, MetalKvStorage.validateSelectedSourceRows(&.{}, 8));
    try std.testing.expectError(error.InvalidKvShape, MetalKvStorage.validateSelectedSourceRows(&.{ 0, 8 }, 8));
    try std.testing.expectError(error.InvalidKvShape, MetalKvStorage.validateSelectedSourceRows(&.{ 2, 2 }, 8));
    try std.testing.expectError(error.InvalidKvShape, MetalKvStorage.validateSelectedSourceRows(&.{ 3, 2 }, 8));
}

fn selectionTestNotCancelled(_: ?*anyopaque) callconv(.c) c_int {
    return 0;
}

test "selected Metal GPU scorer matches decoded-key oracle before and after repack" {
    if (!build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const metal = metal_runtime.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_decode_runtime_destroy(metal);
    if (metal_runtime.termite_metal_decode_runtime_ready(metal) == 0) return error.SkipZigTest;

    const rows: usize = 4;
    const kv_heads: u32 = 1;
    const query_heads: u32 = 2;
    const head_dim: u32 = 128;
    const width: usize = @as(usize, kv_heads) * @as(usize, head_dim);
    var keys: [rows * width]f32 = undefined;
    var values: [rows * width]f32 = undefined;
    for (&keys, &values, 0..) |*key, *value, i| {
        key.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 29)) - 14)) / 31.0;
        value.* = @as(f32, @floatFromInt(i % 17)) / 19.0;
    }
    var queries: [2 * query_heads * head_dim]f32 = undefined;
    for (&queries, 0..) |*query, i| query.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 23)) - 11)) / 27.0;
    const query_positions = [_]u32{ 2, 3 };
    const original_positions = [_]u32{ 0, 1, 2, 3 };

    const input_bytes = keys.len * @sizeOf(f32);
    const k_handle = metal_runtime.termite_metal_buffer_alloc(metal, input_bytes, 0) orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_buffer_release(k_handle);
    const v_handle = metal_runtime.termite_metal_buffer_alloc(metal, input_bytes, 0) orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_buffer_release(v_handle);
    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(metal, k_handle, 0, &keys, input_bytes));
    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(metal, v_handle, 0, &values, input_bytes));

    inline for ([_]pool_mod.KvDType{ .polar4, .turbo3, .f32, .f16, .int8 }) |dtype| {
        const owned = try MetalKvStorage.create(allocator, metal, dtype, kv_heads, head_dim, 4);
        defer owned.deviceWriteHook().deinit(allocator);
        const binding = try owned.acquireBinding(.{ .sequence_id = 91, .layer_index = 0 }, 0, false, true);
        try std.testing.expect(binding.slot >= metal_runtime.paged_kv_slot_base);
        const layout = owned.rowLayout(kv_heads, head_dim);
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_update_attention_span_from_f32_key_device_slot(
            metal,
            binding.slot,
            @backingInt(owned.format),
            k_handle,
            0,
            v_handle,
            0,
            rows,
            kv_heads,
            head_dim,
            layout.key_row_bytes,
            layout.base_key_row_bytes,
            layout.v_row_stride,
            0,
        ));
        binding.written_tokens = rows;
        // This fixture writes the native slot directly, bypassing the storage
        // writer that normally publishes contiguous physical-row metadata.
        binding.logical_contiguous = true;

        // This fixture writes the physical prefix directly, bypassing the
        // normal writer that establishes its logical row mapping.
        binding.logical_contiguous = true;

        var decoded_keys: [rows * width]f32 = undefined;
        var decoded_values: [rows * width]f32 = undefined;
        try MetalKvStorage.gatherLayerKv(@ptrCast(owned), .{
            .sequence_id = 91,
            .layer_index = 0,
            .token_count = rows,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
        }, &decoded_keys, &decoded_values);
        var gpu_scores: [rows]f32 = undefined;
        const score_memory_0 = @import("metal_telemetry.zig").memorySnapshot(true);
        var score_interval_0: @import("metal_telemetry.zig").BufferInterval = .{};
        score_interval_0.begin(true);
        defer _ = score_interval_0.finish(true);
        try MetalKvStorage.scoreSelectedAttention(@ptrCast(owned), .{
            .sequence_id = 91,
            .layer_index = 0,
            .queries = &queries,
            .query_positions = &query_positions,
            .key_positions = &original_positions,
            .num_heads = query_heads,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
            .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))),
            .scores_out = &gpu_scores,
            .check_ctx = null,
            .check_fn = selectionTestNotCancelled,
        });
        const score_owned_0 = score_interval_0.finish(true).?;
        try std.testing.expectEqual(@as(u64, 5), score_owned_0.owned_allocations);
        try std.testing.expectEqual(@as(u64, 5), score_owned_0.owned_frees);
        try std.testing.expectEqual(score_memory_0[0], score_owned_0.live_bytes[0]);
        try std.testing.expect(score_owned_0.interval_peak_bytes[0] > score_owned_0.live_bytes[0]);

        const oracle = try selection.scoreConsumerNormalizedAttention(allocator, .{
            .queries = &queries,
            .query_positions = &.{ 2, 3 },
            .keys = &decoded_keys,
            .key_positions = &.{ 0, 1, 2, 3 },
            .num_query_heads = query_heads,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
            .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))),
        });
        defer allocator.free(oracle);
        for (gpu_scores, oracle) |actual, expected| try std.testing.expectApproxEqAbs(expected, @as(f64, actual), 3e-4);

        const selected_rows = [_]u32{ 0, 2, 3 };
        const selected_positions = [_]u32{ 0, 2, 3 };
        const compact = try MetalKvStorage.repackSelectedGlobalKv(@ptrCast(owned), .{
            .sequence_id = 91,
            .layer_index = 0,
            .expected_source_slot = binding.slot,
            .expected_written_tokens = rows,
            .source_token_offsets = &selected_rows,
            .key_positions = &selected_positions,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
        });
        try std.testing.expectEqual(@as(usize, 3), compact.token_count);
        var compact_scores: [3]f32 = undefined;
        const score_memory_1 = @import("metal_telemetry.zig").memorySnapshot(true);
        var score_interval_1: @import("metal_telemetry.zig").BufferInterval = .{};
        score_interval_1.begin(true);
        defer _ = score_interval_1.finish(true);
        try MetalKvStorage.scoreSelectedAttention(@ptrCast(owned), .{
            .sequence_id = 91,
            .layer_index = 0,
            .queries = &queries,
            .query_positions = &query_positions,
            .key_positions = &selected_positions,
            .num_heads = query_heads,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
            .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))),
            .scores_out = &compact_scores,
            .check_ctx = null,
            .check_fn = selectionTestNotCancelled,
        });
        const score_owned_1 = score_interval_1.finish(true).?;
        try std.testing.expectEqual(@as(u64, 5), score_owned_1.owned_allocations);
        try std.testing.expectEqual(@as(u64, 5), score_owned_1.owned_frees);
        try std.testing.expectEqual(score_memory_1[0], score_owned_1.live_bytes[0]);
        try std.testing.expect(score_owned_1.interval_peak_bytes[0] > score_owned_1.live_bytes[0]);

        var compact_keys: [3 * width]f32 = undefined;
        try MetalKvStorage.gatherLayerKeys(@ptrCast(owned), .{
            .sequence_id = 91,
            .layer_index = 0,
            .token_count = 3,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
        }, &compact_keys);
        const compact_oracle = try selection.scoreConsumerNormalizedAttention(allocator, .{
            .queries = &queries,
            .query_positions = &.{ 2, 3 },
            .keys = &compact_keys,
            .key_positions = &.{ 0, 2, 3 },
            .num_query_heads = query_heads,
            .num_kv_heads = kv_heads,
            .head_dim = head_dim,
            .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))),
        });
        defer allocator.free(compact_oracle);
        for (compact_scores, compact_oracle) |actual, expected| try std.testing.expectApproxEqAbs(expected, @as(f64, actual), 3e-4);

        // Exercise the selected-position attention ABI itself. Query position
        // 2 may attend absolute positions 0 and 2, but must reject position 3
        // even though it is compact physical row 2. This detects a kernel that
        // accidentally substitutes compact row indices for absolute positions.
        const output_values = @as(usize, query_heads) * @as(usize, head_dim);
        const query_head_count: usize = query_heads;
        const head_width: usize = head_dim;
        const output_bytes = output_values * @sizeOf(f32);
        const q_offset_values: usize = 0;
        const q_device = metal_runtime.termite_metal_buffer_alloc(metal, output_bytes, 0) orelse return error.SkipZigTest;
        defer metal_runtime.termite_metal_buffer_release(q_device);
        const out_device = metal_runtime.termite_metal_buffer_alloc(metal, output_bytes, 0) orelse return error.SkipZigTest;
        defer metal_runtime.termite_metal_buffer_release(out_device);
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(
            metal,
            q_device,
            0,
            @ptrCast(queries[q_offset_values..].ptr),
            output_bytes,
        ));
        const compact_blocks = [_]u32{0};
        var positions_interval: @import("metal_telemetry.zig").BufferInterval = .{};
        positions_interval.begin(true);
        defer _ = positions_interval.finish(true);
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_attention_paged_slot_device_selected(
            metal,
            compact.slot,
            compact.format,
            q_device,
            0,
            &compact_blocks,
            compact_blocks.len,
            &selected_positions,
            selected_positions.len,
            compact.page_size_tokens,
            1,
            selected_positions.len,
            query_heads,
            kv_heads,
            head_dim,
            compact.key_row_bytes,
            compact.base_key_row_bytes,
            2,
            0,
            0,
            null,
            0,
            out_device,
            0,
        ));
        var gpu_attention: [output_values]f32 = undefined;
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(metal, out_device, 0, &gpu_attention, output_bytes));
        // This Shared download checks and flushes the actual active frame.
        // Selected positions are command-owned until that terminal boundary;
        // interval metadata must observe real SDK death, not encode return.
        const positions_owned = positions_interval.finish(true).?;
        // Small table/sinks use setBytes; this existing selected-position
        // route owns exactly one copied key-position MTLBuffer.
        try std.testing.expectEqual(@as(u64, 1), positions_owned.owned_allocations);
        try std.testing.expectEqual(@as(u64, 1), positions_owned.owned_frees);
        var cpu_attention: [output_values]f32 = @splat(0);
        const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
        for (0..query_head_count) |head| {
            var logits: [3]f32 = undefined;
            var best = -std.math.inf(f32);
            for (selected_rows[0..2], 0..) |source_row, retained_row| {
                var dot: f32 = 0;
                for (0..head_width) |d| {
                    dot += queries[q_offset_values + head * head_width + d] * decoded_keys[@as(usize, source_row) * width + d];
                }
                logits[retained_row] = dot * scale;
                best = @max(best, logits[retained_row]);
            }
            var denom: f32 = 0;
            for (logits[0..2]) |logit| denom += @exp(logit - best);
            for (selected_rows[0..2], 0..) |source_row, retained_row| {
                const probability = @exp(logits[retained_row] - best) / denom;
                for (0..head_width) |d| {
                    cpu_attention[head * head_width + d] += probability * decoded_values[@as(usize, source_row) * width + d];
                }
            }
        }
        for (gpu_attention, cpu_attention) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 8e-4);
    }
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
    const cached_binding = try cached.acquireBinding(.{ .sequence_id = 1, .layer_index = 0 }, 0, true, false);
    const cached_slot = cached_binding.slot;

    const request_local = try MetalKvStorage.create(allocator, runtime, .f32, 1, 2, 2);
    const local_binding = try request_local.acquireBinding(.{ .sequence_id = 1, .layer_index = 0 }, 0, true, false);
    const local_slot = local_binding.slot;
    try std.testing.expect(cached_slot >= metal_runtime.paged_kv_slot_base);
    try std.testing.expect(local_slot >= metal_runtime.paged_kv_slot_base);
    try std.testing.expect(cached_slot != local_slot);

    // Returning a request-local hook publishes only its own lease. The cache
    // still owns its original slot, while its next layer can reuse the slot
    // that the completed request returned to the shared runtime.
    request_local.deviceWriteHook().deinit(allocator);
    const cached_layer_1 = try cached.acquireBinding(.{ .sequence_id = 1, .layer_index = 1 }, 0, true, false);
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

test "Metal zero-growth encoded KV COW fences all planes once and preserves exact source pages" {
    if (!build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const raw = metal_runtime.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_decode_runtime_destroy(raw);
    if (metal_runtime.termite_metal_decode_runtime_ready(raw) == 0) return error.SkipZigTest;
    for ([_]pool_mod.KvDType{ .f32, .f16, .polar4, .turbo3, .int8 }) |dtype| {
        var storage = try storage_runtime.KvStorageRuntime.init(a, .{ .backend = .metal, .dtype = dtype, .page_size_tokens = 2, .num_layers_packed = 2, .num_kv_heads = 1, .head_dim = 64, .store_cpu_bytes = false });
        defer storage.deinit();
        const owner = try MetalKvStorage.create(a, raw, dtype, 1, 64, 2);
        storage.setDeviceWriteHook(owner.deviceWriteHook());
        const source = try storage.attachSequence(storage.poolId());
        try storage.appendTokens(source, 4);
        var values: [4 * 64]f32 = undefined;
        for (&values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index % 31)) * 0.125 - 1;
        const bytes = @sizeOf(@TypeOf(values));
        const keys = metal_runtime.termite_metal_buffer_alloc(raw, bytes, 0) orelse return error.MetalBufferAllocFailed;
        defer metal_runtime.termite_metal_buffer_release(keys);
        const vals = metal_runtime.termite_metal_buffer_alloc(raw, bytes, 0) orelse return error.MetalBufferAllocFailed;
        defer metal_runtime.termite_metal_buffer_release(vals);
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(raw, keys, 0, &values, bytes));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(raw, vals, 0, &values, bytes));
        const key_ref = storage_runtime.DeviceKvRef{ .handle = keys, .byte_offset = 0, .byte_len = bytes };
        const value_ref = storage_runtime.DeviceKvRef{ .handle = vals, .byte_offset = 0, .byte_len = bytes };
        for (0..2) |index| try storage.writeLayerKvSuffixDevice(.{ .sequence_id = source, .layer_index = index, .total_token_count = 4, .suffix_token_count = 4, .position_offset = 0, .num_kv_heads = 1, .head_dim = 64 }, key_ref, value_ref);
        var original_keys: [2]?[]u8 = @splat(null);
        defer for (original_keys) |data| if (data) |owned| a.free(owned);
        var original_values: [2]?[]u8 = @splat(null);
        defer for (original_values) |data| if (data) |owned| a.free(owned);
        for (0..2) |index| {
            const slot = owner.global_layer_slots.get(@intCast(index)).?;
            const info = try owner.slotInfo(slot);
            const value_bytes = info.v_row_stride * (if (owner.format == .f16) @as(usize, 2) else @as(usize, 4));
            original_keys[index] = try a.alloc(u8, 4 * info.key_row_bytes);
            original_values[index] = try a.alloc(u8, 4 * value_bytes);
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.encoded_key_handle, 0, original_keys[index].?.ptr, original_keys[index].?.len));
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.v_handle, 0, original_values[index].?.ptr, original_values[index].?.len));
        }
        const derived = try storage.attachSequenceWithRetainedBlocks(storage.poolId(), storage.blockTable(source).?.blocks.items, 4);
        _ = try storage.truncateSequence(derived, 1);
        try storage.reserveTokenCapacity(derived, 4);
        const old_tail = storage.blockTable(derived).?.last().?;
        const target = (try storage.sequenceMut(derived)).reserved_tail_blocks.items[0];
        const metadata = storage.storage.hostPool().?.storageMut(old_tail).?;
        metadata.meta.tokens_written = 2;
        metadata.meta.model_tag = 42;
        metadata.meta.priority = 7;
        metadata.meta.prefix_cacheable = true;
        // Initial exact slot capacity cannot authorize writes to an SDK tail.
        try std.testing.expectError(error.SharedKvCopyCapacityRequired, storage.appendTokens(derived, 1));
        try std.testing.expectEqual(old_tail, storage.blockTable(derived).?.last().?);
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_begin_frame(raw));
        try std.testing.expectError(error.SharedKvCopyDuringFrame, storage.appendTokens(derived, 1));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_cancel_frame(raw));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_begin_frame(raw));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_copy(raw, keys, 0, vals, 0, bytes));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_submit_frame(raw));
        try std.testing.expectError(error.SharedKvCopyDuringFrame, storage.appendTokens(derived, 1));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_wait_frame(raw));
        // Explicit preparation owns any growth. The COW callback itself must
        // use only these declared, preexisting plane capacities and handles.
        var handles: [2][2]*anyopaque = undefined;
        for (0..2) |index| {
            const slot = owner.global_layer_slots.get(@intCast(index)).?;
            const before = try owner.slotInfo(slot);
            try owner.reserveSlotCapacity(slot, 8, 2, 0, before.key_row_bytes, before.v_row_stride);
            const prepared = try owner.slotInfo(slot);
            handles[index] = .{ prepared.encoded_key_handle.?, prepared.v_handle.? };
        }
        const plane_bytes = metal_runtime.runtimeMemorySnapshot(raw).attention_span_bytes;
        var audit = @import("../execution/transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        try @import("../execution/transfer_audit.zig").begin(&audit);
        storage.appendTokens(derived, 1) catch |err| {
            @import("../execution/transfer_audit.zig").end();
            return err;
        };
        @import("../execution/transfer_audit.zig").end();
        try std.testing.expectEqual(@as(u64, 0), audit.upload_calls);
        try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
        try std.testing.expectEqual(plane_bytes, metal_runtime.runtimeMemorySnapshot(raw).attention_span_bytes);
        try std.testing.expectEqual(target, storage.blockTable(derived).?.last().?);
        const target_meta = storage.storage.hostPool().?.storage(target).?.meta;
        try std.testing.expectEqual(@as(u16, 1), target_meta.tokens_written);
        try std.testing.expectEqual(@as(u64, 42), target_meta.model_tag);
        try std.testing.expectEqual(@as(i32, 7), target_meta.priority);
        try std.testing.expect(!target_meta.prefix_cacheable);
        for (0..2) |index| {
            const info = try owner.slotInfo(owner.global_layer_slots.get(@intCast(index)).?);
            try std.testing.expect(handles[index][0] == info.encoded_key_handle.? and handles[index][1] == info.v_handle.?);
            const value_bytes = info.v_row_stride * (if (owner.format == .f16) @as(usize, 2) else @as(usize, 4));
            const copied_key = try a.alloc(u8, info.key_row_bytes);
            defer a.free(copied_key);
            const copied_value = try a.alloc(u8, value_bytes);
            defer a.free(copied_value);
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.encoded_key_handle, target * 2 * info.key_row_bytes, copied_key.ptr, copied_key.len));
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.v_handle, target * 2 * value_bytes, copied_value.ptr, copied_value.len));
            try std.testing.expectEqualSlices(u8, original_keys[index].?[old_tail * 2 * info.key_row_bytes ..][0..copied_key.len], copied_key);
            try std.testing.expectEqualSlices(u8, original_values[index].?[old_tail * 2 * value_bytes ..][0..copied_value.len], copied_value);
            const unchanged = try a.alloc(u8, original_keys[index].?.len);
            defer a.free(unchanged);
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.encoded_key_handle, 0, unchanged.ptr, unchanged.len));
            try std.testing.expectEqualSlices(u8, original_keys[index].?, unchanged);
        }
    }
}

test "Metal admitted KV growth is atomic across SDK allocation faults and preserves encoded held backings beyond epoch reuse" {
    if (comptime !build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const capacity = storage_runtime.device_capacity;
    const sink_mod = @import("../execution/retirement_sink.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const operation = @import("../execution/backend_operation.zig");
    var context = try @import("compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const compute: *@import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(cb.ptr));
    const raw = compute.provider_impl.raw_decode_runtime.?;
    var receiver = try sink_mod.Ref.create();
    defer receiver.release();
    const Scenario = enum { all_layers, heterogeneous, sparse };
    for ([_]Scenario{ .all_layers, .heterogeneous, .sparse }) |scenario| for ([_]pool_mod.KvDType{ .f32, .f16, .polar4, .turbo3, .int8 }) |dtype| {
        const sparse = scenario == .sparse;
        const layer_ids: [2]u16 = if (sparse) .{ 1, 3 } else .{ 0, 1 };
        var geometry = capacity.Geometry{};
        for (layer_ids) |index| try geometry.append(.{ .index = index, .num_kv_heads = 1, .head_dim = 64 });
        var runtime = try storage_runtime.KvStorageRuntime.init(a, .{ .backend = .metal, .dtype = dtype, .page_size_tokens = 2, .num_layers_packed = if (sparse) 4 else 2, .num_kv_heads = 1, .head_dim = 64, .store_cpu_bytes = false });
        var runtime_live = true;
        defer if (runtime_live) {
            var final = receiver.enter() catch @panic("cleanup epoch unavailable");
            var epoch = final.retainEpoch() catch @panic("cleanup epoch pin failed");
            runtime.deinitWithBoundary(&epoch);
            final.finish();
            epoch.release();
            _ = retirement.poll();
        };
        const owner = try MetalKvStorage.create(a, raw, dtype, 1, 64, 2);
        runtime.setDeviceWriteHook(owner.deviceWriteHook());
        const sequence = try runtime.attachSequence(runtime.poolId());
        try runtime.appendTokens(sequence, 16);
        var values: [16 * 64]f32 = undefined;
        for (&values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index % 29)) * 0.125 - 1;
        const input = metal_runtime.termite_metal_buffer_alloc(raw, @sizeOf(@TypeOf(values)), 0) orelse return error.MetalBufferAllocFailed;
        defer metal_runtime.termite_metal_buffer_release(input);
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(raw, input, 0, &values, @sizeOf(@TypeOf(values))));
        const input_ref = storage_runtime.DeviceKvRef{ .handle = input, .byte_offset = 0, .byte_len = @sizeOf(@TypeOf(values)) };
        for (layer_ids) |index| try runtime.writeLayerKvSuffixDevice(.{ .sequence_id = sequence, .layer_index = index, .total_token_count = 16, .suffix_token_count = 16, .position_offset = 0, .num_kv_heads = 1, .head_dim = 64 }, input_ref, input_ref);
        if (scenario == .heterogeneous) {
            // Real public reserve keeps layer1's independently larger bank.
            try runtime.reserveLayerKvDeviceCapacity(sequence, 1, 64, 0, 1, 64, 0, 0, false);
        }
        var requested: usize = 18;
        if (sparse) {
            const cached = try runtime.planDeviceCapacityFor(requested, &geometry);
            if (cached.new_bytes == 0) {
                // Reuse can cover both planes beyond the old token hint.
                // Publish only the validated hint, without SDK or quota work.
                var metadata_guard = try receiver.enter();
                defer metadata_guard.finish();
                var metadata_epoch = try metadata_guard.retainEpoch();
                defer metadata_epoch.release();
                var no_claim = capacity.Claim{ .batch = null };
                var metadata_scope = try operation.enter(cb, null, null, .managed);
                defer metadata_scope.deinitChecked() catch @panic("capacity metadata cleanup failed");
                var metadata_audit = @import("../execution/transfer_audit.zig").Audit{ .forbid_device_host_access = true };
                try @import("../execution/transfer_audit.zig").begin(&metadata_audit);
                runtime.growDeviceCapacity(&cached, &no_claim, &metadata_epoch) catch |err| {
                    @import("../execution/transfer_audit.zig").end();
                    return err;
                };
                @import("../execution/transfer_audit.zig").end();
                try metadata_scope.finish();
                metadata_guard.finish();
                try std.testing.expectEqual(@as(u64, 0), metadata_audit.upload_calls);
                try std.testing.expectEqual(@as(u64, 0), metadata_audit.download_calls);
                const reused = try runtime.planDeviceCapacityFor(requested, &geometry);
                for (cached.layers[0..cached.count], reused.layers[0..reused.count]) |before, after| {
                    try std.testing.expectEqual(before.keys.handle, after.keys.handle);
                    try std.testing.expectEqual(before.values.handle, after.values.handle);
                    try std.testing.expectEqual(before.target_tokens, after.current_tokens);
                }
            }
            // Independently exercise actual replacement even when the runtime
            // retained larger SDK buffers from a previous format or slot user.
            for (cached.layers[0..cached.count]) |layer| {
                requested = @max(requested, try std.math.add(usize, layer.keys.length / layer.keys.row_bytes, 2));
                requested = @max(requested, try std.math.add(usize, layer.values.length / layer.values.row_bytes, 2));
            }
        }
        const plan = (if (sparse) try runtime.planDeviceCapacityFor(requested, &geometry) else try runtime.planDeviceCapacity(requested));
        if (scenario == .heterogeneous) {
            try std.testing.expect(plan.layers[0].keys.new_length != 0);
            try std.testing.expectEqual(@as(usize, 0), plan.layers[1].keys.new_length);
            try std.testing.expectEqual(@as(usize, 0), plan.layers[1].values.new_length);
        }
        if (sparse) {
            try std.testing.expectEqual(@as(u16, 1), plan.layers[0].index);
            try std.testing.expectEqual(@as(u16, 3), plan.layers[1].index);
            var wrong = geometry;
            wrong.layers[0].head_dim = 32;
            try std.testing.expectError(error.DeviceKvCapacityLayoutChanged, runtime.planDeviceCapacityFor(plan.minimum_tokens, &wrong));
            var missing = capacity.Geometry{};
            try missing.append(geometry.layers[0]);
            try std.testing.expectError(error.DeviceKvCapacityUnsupported, runtime.planDeviceCapacityFor(plan.minimum_tokens, &missing));
            try std.testing.expectError(error.InvalidDeviceCapacityLayout, geometry.append(geometry.layers[1]));
        }
        const budget = try capacity.TestAdmission.create(a, plan.old_bytes, (try plan.peakBytes()) - 1);
        defer {
            if (runtime_live) {
                var final = receiver.enter() catch @panic("cleanup epoch unavailable");
                var epoch = final.retainEpoch() catch @panic("cleanup epoch pin failed");
                runtime.deinitWithBoundary(&epoch);
                runtime_live = false;
                final.finish();
                epoch.release();
            }
            _ = retirement.poll();
            budget.deinit();
        }
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, capacity.Claim.prepare(&plan, budget.admission()));
        budget.limit = try plan.peakBytes();
        var saved: [2][2]?[]u8 = @splat(@splat(null));
        defer for (saved) |layer| for (layer) |bytes| if (bytes) |owned| a.free(owned);
        for (plan.layers[0..plan.count], 0..) |layer, index| {
            const planes = [_]capacity.Plane{ layer.keys, layer.values };
            for (planes, 0..) |plane, which| {
                saved[index][which] = try a.alloc(u8, plane.length);
                try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, @ptrFromInt(plane.handle), 0, saved[index][which].?.ptr, plane.length));
            }
        }
        const Fault = struct {
            var calls: usize = 0;
            var fail_at: usize = 0;
            var hook: storage_runtime.DeviceWriteHook.VTable = undefined;
            fn allocate(_: ?*anyopaque, runtime_value: *metal_runtime.RawMetalDecodeRuntime, bytes: usize) callconv(.c) ?*anyopaque {
                calls += 1;
                if (calls == fail_at) return null;
                return metal_runtime.termite_metal_buffer_alloc_fresh(runtime_value, bytes, 1);
            }
            fn prepare(runtime_value: *metal_runtime.RawMetalDecodeRuntime, planes: [*]const metal_runtime.RawKvGrowthPlane, count: usize, stage: *?*anyopaque) c_int {
                return metal_runtime.antfly_metal_kv_growth_prepare_with_allocator(runtime_value, planes, count, stage, allocate, null);
            }
            fn grow(raw_value: *anyopaque, implementation: *const anyopaque, prepared: *const capacity.Plan, claim: *capacity.Claim, epoch: *const sink_mod.Epoch) !void {
                if (implementation != @as(*const anyopaque, @ptrCast(&hook))) return error.WrongKvFaultProvider;
                return MetalKvStorage.growCapacityUsing(raw_value, &hook_vtable, prepared, claim, epoch, @This());
            }
        };
        const failure_indices: [2]usize = if (scenario == .heterogeneous) .{ 1, 2 } else .{ 2, 3 };
        for (failure_indices) |failure| {
            var guard = try receiver.enter();
            defer guard.finish();
            var epoch = try guard.retainEpoch();
            defer epoch.release();
            var claim = try capacity.Claim.prepare(&plan, budget.admission());
            defer claim.deinit(&epoch);
            Fault.calls = 0;
            Fault.fail_at = failure;
            Fault.hook = hook_vtable;
            Fault.hook.growCapacity = Fault.grow;
            const original = runtime.device_write_hook;
            runtime.device_write_hook = .{ .ctx = owner, .vtable = &Fault.hook };
            defer runtime.device_write_hook = original;
            try std.testing.expect(budget.provider_gate.tryLock());
            var outer_held = true;
            defer if (outer_held) budget.provider_gate.unlock();
            var operation_scope = try operation.enter(cb, null, null, .managed);
            defer operation_scope.deinitChecked() catch @panic("capacity failure drain failed");
            try std.testing.expectError(error.OutOfMemory, runtime.growDeviceCapacity(&plan, &claim, &epoch));
            try operation_scope.finish();
            claim.deinit(&epoch);
            try std.testing.expectEqual(try plan.peakBytes(), budget.domain.controller.snapshot().backend_kv_bytes);
            runtime.device_write_hook = original;
            const unchanged = (if (sparse) try runtime.planDeviceCapacityFor(requested, &geometry) else try runtime.planDeviceCapacity(requested));
            try std.testing.expect(plan.eql(&unchanged));
            budget.provider_gate.unlock();
            outer_held = false;
            guard.finish();
            try std.testing.expectEqual(plan.old_bytes, budget.domain.controller.snapshot().backend_kv_bytes);
        }
        var held: ?*anyopaque = null;
        defer if (held) |buffer| metal_runtime.termite_metal_buffer_release(buffer);
        var first_actual: usize = 0;
        for (0..2) |round| {
            const current = if (round == 0) plan else (if (sparse) try runtime.planDeviceCapacityFor(owner.slot_buffer_capacity_tokens[plan.layers[0].slot] + 2, &geometry) else try runtime.planDeviceCapacity(owner.slot_buffer_capacity_tokens[plan.layers[0].slot] + 2));
            budget.limit = try std.math.add(usize, budget.domain.controller.snapshot().backend_kv_bytes, current.new_bytes);
            var guard = try receiver.enter();
            defer guard.finish();
            var epoch = try guard.retainEpoch();
            defer epoch.release();
            var claim = try capacity.Claim.prepare(&current, budget.admission());
            defer claim.deinit(&epoch);
            // Owned frames are rejected before any allocation/publication.
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_begin_frame(raw));
            try std.testing.expectError(error.SharedKvCopyDuringFrame, runtime.growDeviceCapacity(&current, &claim, &epoch));
            try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_decode_runtime_cancel_frame(raw));
            try std.testing.expect(budget.provider_gate.tryLock());
            var outer_held = true;
            defer if (outer_held) budget.provider_gate.unlock();
            var operation_scope = try operation.enter(cb, null, null, .managed);
            defer operation_scope.deinitChecked() catch @panic("capacity success drain failed");
            var audit = @import("../execution/transfer_audit.zig").Audit{ .forbid_device_host_access = true };
            try @import("../execution/transfer_audit.zig").begin(&audit);
            runtime.growDeviceCapacity(&current, &claim, &epoch) catch |err| {
                @import("../execution/transfer_audit.zig").end();
                return err;
            };
            @import("../execution/transfer_audit.zig").end();
            try operation_scope.finish();
            budget.provider_gate.unlock();
            outer_held = false;
            guard.finish();
            const retired_boundary = try epoch.boundary();
            epoch.release(); // Do not let the test itself pin the retired record.
            try std.testing.expectEqual(@as(u64, 0), audit.upload_calls);
            try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
            try std.testing.expect(claim.batch == null);
            const completed = (if (sparse) try runtime.planDeviceCapacityFor(current.minimum_tokens, &geometry) else try runtime.planDeviceCapacity(current.minimum_tokens));
            try std.testing.expectEqual(@as(usize, 0), completed.new_bytes);
            var completed_charged: usize = 0;
            for (completed.layers[0..completed.count]) |layer| {
                const planes = [_]capacity.Plane{ layer.keys, layer.values };
                for (planes, 0..) |plane, which| if (owner.capacity_owners[layer.slot][which] != null) {
                    completed_charged = try std.math.add(usize, completed_charged, plane.allocated);
                };
            }
            if (scenario == .heterogeneous) {
                try std.testing.expectEqual(plan.layers[1].keys.handle, completed.layers[1].keys.handle);
                try std.testing.expectEqual(plan.layers[1].values.handle, completed.layers[1].values.handle);
                try std.testing.expect(owner.capacity_owners[completed.layers[1].slot][0] == null);
                try std.testing.expect(owner.capacity_owners[completed.layers[1].slot][1] == null);
            }
            for (completed.layers[0..completed.count], 0..) |layer, index| {
                const planes = [_]capacity.Plane{ layer.keys, layer.values };
                for (planes, 0..) |plane, which| {
                    const actual = try a.alloc(u8, saved[index][which].?.len);
                    defer a.free(actual);
                    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, @ptrFromInt(plane.handle), 0, actual.ptr, actual.len));
                    try std.testing.expectEqualSlices(u8, saved[index][which].?, actual);
                }
            }
            if (round == 0) {
                first_actual = completed.layers[0].keys.allocated;
                const plane = completed.layers[0].keys;
                held = metal_runtime.termite_metal_buffer_retain_external(raw, @ptrFromInt(plane.handle), plane.length) orelse return error.ExpectedRetainedKvBacking;
            } else {
                // The original growth epoch is over. Reuse bounded records
                // repeatedly BEFORE actual held SDK backing finally dies.
                for (0..64) |reuse| {
                    var next = try receiver.enter();
                    defer next.finish();
                    const fresh = try next.boundary();
                    if (reuse == 0) try std.testing.expect(fresh.context == retired_boundary.context and fresh.nonce != retired_boundary.nonce);
                    next.finish();
                }
                for (0..1_000_000) |_| {
                    _ = retirement.poll();
                    if (budget.domain.controller.snapshot().backend_kv_bytes == plan.old_bytes + first_actual + completed_charged) break;
                    std.atomic.spinLoopHint();
                }
                try std.testing.expectEqual(plan.old_bytes + first_actual + completed_charged, budget.domain.controller.snapshot().backend_kv_bytes);
                metal_runtime.termite_metal_buffer_release(held.?);
                held = null;
                for (0..1_000_000) |_| {
                    _ = retirement.poll();
                    if (budget.domain.controller.snapshot().backend_kv_bytes == plan.old_bytes + completed_charged) break;
                    std.atomic.spinLoopHint();
                }
                try std.testing.expectEqual(plan.old_bytes + completed_charged, budget.domain.controller.snapshot().backend_kv_bytes);
            }
        }
        var final_guard = try receiver.enter();
        defer final_guard.finish();
        var final_epoch = try final_guard.retainEpoch();
        defer final_epoch.release();
        try std.testing.expect(budget.provider_gate.tryLock());
        var final_gate_held = true;
        defer if (final_gate_held) budget.provider_gate.unlock();
        runtime.deinitWithBoundary(&final_epoch);
        runtime_live = false;
        budget.provider_gate.unlock();
        final_gate_held = false;
        final_guard.finish();
        for (0..1_000_000) |_| {
            _ = retirement.poll();
            if (budget.domain.controller.snapshot().backend_kv_bytes == plan.old_bytes) break;
            std.atomic.spinLoopHint();
        }
        try std.testing.expectEqual(plan.old_bytes, budget.domain.controller.snapshot().backend_kv_bytes);
    };
}

// Pure quota metadata for cached unadmitted buffers that will be replaced.
// The SDK/runtime and caller model never enter these delayed owner records.
const ColdRetiringBackings = struct {
    const capacity = storage_runtime.device_capacity;
    const storage = @import("../execution/storage_admission.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const sink = @import("../execution/retirement_sink.zig");
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    owners: []capacity.BackingOwner,

    fn releaseMetadata(self: *ColdRetiringBackings) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        const allocator = self.allocator;
        allocator.free(self.owners);
        allocator.destroy(self);
    }
    fn releaseCell(raw: *anyopaque) void {
        const self: *ColdRetiringBackings = @ptrCast(@alignCast(raw));
        self.releaseMetadata();
    }
    /// Consumes the complete separately partitioned old-cache reservation.
    /// This synchronous cold constructor runs before any actual gate/SDK use.
    fn create(allocator: std.mem.Allocator, reservation: *?storage.Reservation, spans: []const usize, domain: @import("../runtime/tier/admission_domain.zig").Pin) !*ColdRetiringBackings {
        var source = reservation.* orelse return error.MissingResourceRetirementLease;
        reservation.* = null;
        defer source.release();
        var total: usize = 0;
        for (spans) |bytes| {
            if (bytes == 0) return error.InvalidDeviceCapacityAdmission;
            total = try std.math.add(usize, total, bytes);
        }
        if (spans.len == 0 or source.reservedBytes() != total or source.identity.controller != domain.controller()) return error.InvalidDeviceCapacityAdmission;
        const self = try allocator.create(ColdRetiringBackings);
        errdefer allocator.destroy(self);
        const owners = try allocator.alloc(capacity.BackingOwner, spans.len);
        errdefer allocator.free(owners);
        self.* = .{ .allocator = allocator, .owners = owners };
        var initialized: usize = 0;
        errdefer for (owners[0..initialized]) |*owner| owner.charge.release();
        for (owners, spans) |*owner, bytes| {
            var part: ?storage.Reservation = try source.partition(allocator, bytes);
            errdefer if (part) |*value| value.release();
            var held: ?@import("../runtime/tier/admission_domain.zig").Pin = domain.retain();
            errdefer if (held) |pin| pin.release();
            const charge = try retirement.Charge.createReservation(&part, &held);
            owner.init(charge, .{ .context = self, .release = releaseCell });
            _ = self.references.fetchAdd(1, .monotonic);
            initialized += 1;
        }
        return self;
    }
    fn finish(self: *ColdRetiringBackings, epoch: *const sink.Epoch) void {
        // The root reference keeps the owner slab valid if a sealed boundary
        // executes any of these metadata callbacks immediately.
        for (self.owners) |*owner| owner.release(epoch);
        self.releaseMetadata();
    }
};

test "Metal cold KV bank prefunds sparse planes failure atomically and reuses actual buffers through sixty four refills" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const platform = @import("antfly_platform");
    const path = platform.env.getenv("ANTFLY_UNIFIED_DECODER_FIXTURE") orelse return error.SkipZigTest;
    const factory = @import("../architectures/session_factory.zig");
    const cold = @import("cold_compute_target.zig");
    const capacity = storage_runtime.device_capacity;
    const retirement = @import("../execution/resource_retirement.zig");
    const a = std.testing.allocator;
    const Monitor = struct {
        active: std.atomic.Value(bool) = .init(false),
        arms: usize = 0,
        fn arm(raw: *anyopaque, _: @import("../execution_control.zig").MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.active.swap(true, .acq_rel)) return error.UnexpectedNestedSdkScope;
            self.arms += 1;
            return 1;
        }
        fn disarm(raw: *anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active.swap(false, .acq_rel));
        }
    };
    var monitor = Monitor{};
    const control = @import("../execution_control.zig").InferenceExecutionControl{ .native_call_policy = .in_process, .hard_cancellation = .{ .ptr = &monitor, .arm_fn = Monitor.arm, .disarm_fn = Monitor.disarm } };
    var session = try factory.createMetalSession(a, path);
    defer session.close();
    factory.attachIo(session, std.testing.io);
    try session.installOwnedExecutionGate(a);
    var provider_gate: *@import("../execution/operation_gate.zig").Gate = undefined;
    var raw: *metal_runtime.RawMetalDecodeRuntime = undefined;
    var input: ?*anyopaque = null;
    defer if (input) |handle| metal_runtime.termite_metal_buffer_release(handle);
    var values: [8 * 64]f32 = undefined;
    for (&values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index % 13)) * 0.125 - 0.5;
    {
        const backend = try factory.getComputeBackend(session, a);
        defer backend.deinit();
        const compute: *@import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(backend.ptr));
        raw = compute.provider_impl.raw_decode_runtime.?;
        provider_gate = &compute.provider_impl.operations.gate;
        input = metal_runtime.termite_metal_buffer_alloc(raw, @sizeOf(@TypeOf(values)), 0) orelse return error.OutOfMemory;
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(raw, input, 0, &values, @sizeOf(@TypeOf(values))));
    }
    const budget = try capacity.TestAdmission.create(a, 0, 1024 * 1024);
    budget.actual_provider_gate = &provider_gate.mutex;
    budget.actual_model_gate = session.execution_gate;
    budget.actual_sdk_active = &monitor.active;
    defer {
        const end = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
        while (budget.domain.controller.snapshot().backend_kv_bytes != 0) {
            _ = retirement.poll();
            if (platform.time.monotonicNs() >= end) @panic("cold KV backing failed to retire");
            std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
        }
        budget.deinit();
    }
    var receiver = try session.model_lifetime.?.retainRetirementSink();
    defer receiver.release();
    const geometry = [_]capacity.LayerGeometry{ .{ .index = 1, .num_kv_heads = 1, .head_dim = 64 }, .{ .index = 3, .num_kv_heads = 1, .head_dim = 64 } };
    const window = try capacity.Window.fromRequest(8, 0, 2, 2, 0);
    const Fault = struct {
        var calls: usize = 0;
        var fail_at: usize = 0;
        var sdk_monitor: *Monitor = undefined;
        fn free(buffer: *anyopaque) void {
            std.debug.assert(sdk_monitor.active.load(.acquire));
            metal_runtime.termite_metal_buffer_release(buffer);
            std.debug.assert(sdk_monitor.active.load(.acquire));
        }
        fn releaseDevice(device: *anyopaque) void {
            std.debug.assert(sdk_monitor.active.load(.acquire));
            metal_runtime.antfly_metal_standalone_device_release(device);
            std.debug.assert(sdk_monitor.active.load(.acquire));
        }
        fn alloc(device: *anyopaque, bytes: usize) ?*anyopaque {
            std.debug.assert(sdk_monitor.active.load(.acquire));
            calls += 1;
            if (calls == fail_at) return null;
            return metal_runtime.antfly_metal_standalone_buffer_alloc(device, bytes);
        }
    };
    Fault.sdk_monitor = &monitor;
    // Fresh slots have no SDK buffers. Every partial allocation must unwind
    // actual objects, quota and the retained model without a caller Arena.
    for (1..5) |failure| {
        var guard = try receiver.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        var target = (try cold.Retained.acquire(session, control)).?;
        defer target.deinitWithBoundary(&epoch);
        Fault.calls = 0;
        Fault.fail_at = failure;
        try std.testing.expectError(error.OutOfMemory, MetalKvStorage.ColdKvBank.prepareUsing(&target, .f32, window, .shared_physical_pages, &geometry, budget.admission(), control, &epoch, Fault));
        try std.testing.expectEqual(failure, Fault.calls);
        try std.testing.expect(target.model != null); // Failed construction never consumes caller pin.
        try std.testing.expectEqual(@as(usize, 2), session.model_lifetime.?.references.load(.acquire));
        try std.testing.expect(!monitor.active.load(.acquire));
        guard.finish();
        _ = retirement.poll();
        try std.testing.expectEqual(@as(usize, 0), budget.domain.controller.snapshot().backend_kv_bytes);
    }
    const config = pool_mod.KvPoolConfig{ .backend = .metal, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 4, .num_kv_heads = 1, .head_dim = 64, .store_cpu_bytes = false };
    var prepare_guard = try receiver.enter();
    defer prepare_guard.finish();
    var prepare_epoch = try prepare_guard.retainEpoch();
    defer prepare_epoch.release();
    var target = (try cold.Retained.acquire(session, control)).?;
    defer target.deinitWithBoundary(&prepare_epoch);
    for ([_]@import("../execution/prepared_kv_banks.zig").Mode{ .sequence_private, .ring, .fitted_match }) |mode| {
        try std.testing.expect((try MetalKvStorage.ColdKvBank.prepare(&target, .f32, window, mode, &geometry, budget.admission(), control, &prepare_epoch)) == null);
        try std.testing.expect(target.model != null);
    }
    Fault.calls = 0;
    Fault.fail_at = 0;
    const bank = (try MetalKvStorage.ColdKvBank.prepareUsing(&target, .f32, window, .shared_physical_pages, &geometry, budget.admission(), control, &prepare_epoch, Fault)).?;
    var adopted = false;
    var bank_live = true;
    defer if (bank_live) {
        // Error cleanup runs outside the nested real model/provider scopes.
        // It owns a fresh teardown boundary, never the old preparation A.
        var close_guard = receiver.enter() catch @panic("test cleanup owner unavailable");
        var close_epoch = close_guard.retainEpoch() catch @panic("test cleanup epoch unavailable");
        if (adopted) bank.finishAfterUnlock(&close_epoch) else bank.abortOutsideLanes(&close_epoch);
        close_guard.finish();
        close_epoch.release();
    };
    try std.testing.expect(target.model == null);
    try std.testing.expectEqual(@as(usize, 4), Fault.calls);
    const descriptor = (try MetalKvStorage.ColdKvBank.describeCoverage(bank.retained.target, .f32, window, .shared_physical_pages, &geometry)).?;
    try std.testing.expect(bank.snapshotCoverage().covers(descriptor.coverage()));
    prepare_guard.finish();
    // Cycle beyond bounded epoch capacity before real physical destruction.
    for (0..70) |_| {
        var unrelated = try receiver.enter();
        unrelated.finish();
    }
    const held = budget.domain.controller.snapshot().backend_kv_bytes;
    try std.testing.expect(held > 0);
    var current = try receiver.enter();
    defer current.finish();
    var epoch = try current.retainEpoch();
    defer epoch.release();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var runtime = try storage_runtime.KvStorageRuntime.init(arena.allocator(), config);
    {
        try control.lock(session.execution_gate.?);
        defer session.execution_gate.?.unlock();
        const backend = try factory.getComputeBackend(session, a);
        defer backend.deinit();
        try std.testing.expect(try backend.bindCleanupEpoch(&epoch));
        try bank.adopt(&backend, &runtime);
        adopted = true;
        var runtime_live = true;
        defer if (runtime_live) runtime.deinitWithBoundary(&epoch);
        var original: [2][2]*anyopaque = undefined;
        for (geometry, 0..) |layer, index| {
            const info = try bank.store.?.slotInfo(bank.store.?.global_layer_slots.get(layer.index).?);
            original[index] = .{ info.encoded_key_handle.?, info.v_handle.? };
        }
        // One long-lived page stays resident while a second slot refills.
        const longest = try runtime.attachSequence(runtime.poolId());
        try runtime.appendTokens(longest, 8);
        const retained_source = storage_runtime.DeviceKvRef{ .handle = input.?, .byte_offset = 0, .byte_len = @sizeOf(@TypeOf(values)) };
        for (geometry) |layer| try runtime.writeLayerKvSuffixDevice(.{ .sequence_id = longest, .layer_index = layer.index, .total_token_count = 8, .suffix_token_count = 8, .position_offset = 0, .num_kv_heads = 1, .head_dim = 64 }, retained_source, retained_source);
        {
            const attention = @import("../graph/backend_contracts.zig").AttentionContext{
                .mode = .paged_decode,
                .query_sequence_len = 1,
                .kv_sequence_len = 8,
                .total_sequence_len = 8,
                .layer_index = geometry[0].index,
                .kv_storage = &runtime,
                .kv_cache = .{ .sequence_id = longest, .pool_id = runtime.poolId(), .logical_block_count = runtime.blockTable(longest).?.blocks.items.len, .tail_tokens = runtime.blockTable(longest).?.tail_tokens, .kv_storage = &runtime },
            };
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
            const old_allocator = bank.store.?.allocator;
            bank.store.?.allocator = failing.allocator();
            defer bank.store.?.allocator = old_allocator;
            const prepared_attention = (try MetalKvStorage.prepareDecodeAttention(&runtime, raw, attention, 1, 1, 64)) orelse return error.MissingPreparedAttention;
            var foreign = runtime; // Non-owning metadata copy, never deinitialized.
            foreign.storage.ptr = @ptrFromInt(16);
            var foreign_attention = attention;
            foreign_attention.kv_storage = &foreign;
            foreign_attention.kv_cache.?.kv_storage = &foreign;
            // Genuine canonical hook + identical sequence IDs cannot attest an
            // unrelated host owner. Reject before table/host callback access.
            try std.testing.expect((try MetalKvStorage.prepareDecodeAttention(&foreign, raw, foreign_attention, 1, 1, 64)) == null);
            var wrong_pool = attention;
            wrong_pool.kv_cache.?.pool_id = runtime.poolId() + 1;
            try std.testing.expectError(error.InvalidPoolId, MetalKvStorage.prepareDecodeAttention(&runtime, raw, wrong_pool, 1, 1, 64));
            var moved = runtime; // Same stable host storage remains valid.
            var moved_attention = attention;
            moved_attention.kv_storage = &moved;
            moved_attention.kv_cache.?.kv_storage = &moved;
            try std.testing.expect((try MetalKvStorage.prepareDecodeAttention(&moved, raw, moved_attention, 1, 1, 64)) != null);
            try std.testing.expect(prepared_attention.offsets.ptr == bank.store.?.cyclic_page_table_cache.offsets.items.ptr);
            const ids = runtime.blockTable(longest).?.blocks.items;
            for (prepared_attention.offsets, ids) |offset, id| try std.testing.expectEqual(@as(u32, id * 2), offset);
            var malformed = attention;
            malformed.kv_sequence_len = 9;
            try std.testing.expect((try MetalKvStorage.prepareDecodeAttention(&runtime, raw, malformed, 1, 1, 64)) == null);
            var copied = hook_vtable;
            std.mem.doNotOptimizeAway(&copied);
            try std.testing.expect(@as(*const anyopaque, @ptrCast(&copied)) != @as(*const anyopaque, @ptrCast(&hook_vtable)));
            const real_hook = runtime.device_write_hook;
            runtime.device_write_hook = .{ .ctx = @ptrFromInt(16), .vtable = &copied };
            defer runtime.device_write_hook = real_hook;
            try std.testing.expect((try MetalKvStorage.prepareDecodeAttention(&runtime, raw, attention, 1, 1, 64)) == null);
            runtime.device_write_hook = real_hook;
            // The genuine public operation consumes the prepared layer. Zero
            // queries make the exact independent oracle the mean of eight rows.
            const q_values: [64]f32 = @splat(0);
            const q = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(&q_values, &.{ 1, 64 }));
            defer backend.free(q);
            const k = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(values[7 * 64 ..], &.{ 1, 64 }));
            defer backend.free(k);
            const v = try backend.ensureDeviceResidentOwned(try backend.fromFloat32Shape(values[7 * 64 ..], &.{ 1, 64 }));
            defer backend.free(v);
            const result = try backend.gqaPagedAttention(q, k, v, null, attention, 1, 1, 1, 64);
            defer backend.free(result);
            const actual = try backend.toFloat32(result, a);
            defer a.free(actual);
            for (actual, 0..) |got, column| {
                var expected: f64 = 0;
                for (0..8) |row| expected += values[row * 64 + column];
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected / 8)), got, 0.0001);
            }
            try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
        }
        for (0..64) |_| {
            const sequence = try runtime.attachSequence(runtime.poolId());
            try runtime.appendTokens(sequence, 8);
            const source = storage_runtime.DeviceKvRef{ .handle = input.?, .byte_offset = 0, .byte_len = @sizeOf(@TypeOf(values)) };
            for (geometry, 0..) |layer, index| {
                try runtime.writeLayerKvSuffixDevice(.{ .sequence_id = sequence, .layer_index = layer.index, .total_token_count = 8, .suffix_token_count = 8, .position_offset = 0, .num_kv_heads = 1, .head_dim = 64 }, source, source);
                const info = try bank.store.?.slotInfo(bank.store.?.global_layer_slots.get(layer.index).?);
                // Recycled global blocks need not be contiguous. Read their
                // actual SDK rows using the logical table as an independent
                // scalar oracle; ordinary gather intentionally declines that
                // layout when no CPU mirror is present.
                const ids = runtime.blockTable(sequence).?.blocks.items;
                var actual_k: [8 * 64]f32 = undefined;
                var actual_v: [8 * 64]f32 = undefined;
                for (0..8) |token| {
                    const physical = (@as(usize, ids[token / 2]) * 2 + token % 2) * 64 * @sizeOf(f32);
                    const first = token * 64;
                    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.encoded_key_handle, physical, @ptrCast(actual_k[first..].ptr), 64 * @sizeOf(f32)));
                    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_download(raw, info.v_handle, physical, @ptrCast(actual_v[first..].ptr), 64 * @sizeOf(f32)));
                }
                try std.testing.expectEqualSlices(f32, &values, &actual_k);
                try std.testing.expectEqualSlices(f32, &values, &actual_v);
                try std.testing.expectEqual(original[index][0], info.encoded_key_handle.?);
                try std.testing.expectEqual(original[index][1], info.v_handle.?);
            }
            try runtime.releaseSequence(sequence);
            try std.testing.expectEqual(held, budget.domain.controller.snapshot().backend_kv_bytes);
        }
        try runtime.releaseSequence(longest);
        try std.testing.expectEqual(@as(usize, 4), Fault.calls);
        runtime.deinitWithBoundary(&epoch);
        runtime_live = false;
        try std.testing.expect(bank.store == null);
        try std.testing.expect(budget.domain.controller.snapshot().backend_kv_bytes > 0);
    }
    arena.deinit();
    arena = .init(a);
    bank.finishAfterUnlock(&epoch);
    bank_live = false;
    current.finish();
    _ = retirement.poll();
}
