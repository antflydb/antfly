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
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");
const context_teacher_forced = @import("context_teacher_forced.zig");
const attention_match_storage = @import("../ops/metal_attention_match_storage.zig");
const AttentionMatchControl = @import("../runtime/kv/attention_match.zig").Control;

fn fingerprintU32(hash: *std.crypto.hash.sha2.Sha256, value: usize) !void {
    const narrowed = std.math.cast(u32, value) orelse return error.QualificationFingerprintOverflow;
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, narrowed, .little);
    hash.update(&bytes);
}

fn fingerprintQualificationCoverage(
    hash: *std.crypto.hash.sha2.Sha256,
    logical_start: usize,
    logical_end: usize,
    page_size: usize,
) !void {
    if (page_size == 0 or logical_start >= logical_end) return error.InvalidQualificationFingerprint;
    const first_page = logical_start / page_size;
    const last_page = (logical_end - 1) / page_size;
    try fingerprintU32(hash, last_page - first_page + 1);
    var logical_page = first_page;
    while (logical_page <= last_page) : (logical_page += 1) {
        const page_first = @max(logical_start, logical_page * page_size);
        const page_end = @min(logical_end, (logical_page + 1) * page_size);
        try fingerprintU32(hash, logical_page);
        try fingerprintU32(hash, page_first);
        try fingerprintU32(hash, page_end - page_first);
    }
}

const QualificationVisibleRange = struct { start: usize, count: usize };

fn validateQualificationOwnerLayers(owner_layers: []const u32, expected_count: usize) !void {
    if (owner_layers.len == 0 or owner_layers.len != expected_count)
        return error.InvalidQualificationFingerprint;
    var previous: ?u32 = null;
    for (owner_layers) |layer| {
        if (previous) |value| if (layer <= value)
            return error.InvalidQualificationFingerprint;
        previous = layer;
    }
}

fn qualificationVisibleRange(binding: SlotBinding, logical_tokens: usize, page_size: usize) !QualificationVisibleRange {
    if (logical_tokens == 0 or page_size == 0) return error.InvalidQualificationFingerprint;
    const count = if (binding.ring_page_count > 0) blk: {
        if (!binding.sequence_owned or binding.sliding_window_tokens == 0) return error.InvalidQualificationFingerprint;
        const ring_capacity = std.math.mul(usize, binding.ring_page_count, page_size) catch
            return error.QualificationFingerprintOverflow;
        if (binding.sliding_window_tokens > ring_capacity) return error.InvalidQualificationFingerprint;
        break :blk @min(logical_tokens, binding.sliding_window_tokens);
    } else blk: {
        if (binding.sliding_window_tokens != 0) return error.InvalidQualificationFingerprint;
        break :blk logical_tokens;
    };
    return .{ .start = logical_tokens - count, .count = count };
}

fn fingerprintQualificationLayer(
    hash: *std.crypto.hash.sha2.Sha256,
    layer: usize,
    binding: SlotBinding,
    logical_start: usize,
    visible_tokens: usize,
    logical_tokens: usize,
    page_size: usize,
    key_row_bytes: usize,
    value_row_stride: usize,
    value_element_bytes: usize,
) !void {
    // Deliberately omit slot id, physical pages, buffer capacities, and pool
    // capacity. Independent sibling contexts may choose different allocation
    // geometry while implementing identical logical context semantics.
    try fingerprintU32(hash, layer);
    try fingerprintU32(hash, if (binding.sequence_owned) 1 else 0);
    try fingerprintU32(hash, binding.ring_page_count);
    try fingerprintU32(hash, binding.sliding_window_tokens);
    try fingerprintU32(hash, binding.written_tokens);
    try fingerprintU32(hash, binding.position_offset);
    try fingerprintU32(hash, logical_start);
    try fingerprintU32(hash, visible_tokens);
    try fingerprintU32(hash, key_row_bytes);
    try fingerprintU32(hash, value_row_stride);
    try fingerprintU32(hash, value_element_bytes);
    try fingerprintQualificationCoverage(hash, logical_start, logical_tokens, page_size);
}

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
    /// Exact logical attention window. Ring capacity is page rounded and also
    /// includes admitted in-flight rows, so it is not a visibility bound.
    sliding_window_tokens: usize = 0,
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

pub const AttentionMatchBinding = struct {
    diagnostic_only: bool = false,
    sequence: storage_runtime.SequenceId,
    device: attention_match_storage.DeviceState,
    control: AttentionMatchControl,
    invalid: bool = false,
    attention_calls: usize = 0,
    donor_appends: usize = 0,
    rollback_tokens: usize = 0,

    pub fn deinit(self: *AttentionMatchBinding) void {
        self.device.runner.cancelDeferredFrame();
        std.log.info("{s} source_tokens={d} logical_tokens={d} attention_calls={d} donor_appends={d} allocated_device_bytes={d} invalid={d} rollback_tokens={d}", .{
            if (self.diagnostic_only) @as([]const u8, "attention_match_shadow_serving") else "attention_match_serving",
            self.device.initial_logical_length,
            self.device.logical_length,
            self.attention_calls,
            self.donor_appends,
            self.device.allocated_bytes,
            @intFromBool(self.invalid),
            self.rollback_tokens,
        });
        self.device.deinit();
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
    allocation_peak_bytes: u64 = 0,
    /// Non-null only for a request-exclusive hook. Shared prompt-cache hooks
    /// leave this null and disclose their retained residency separately.
    owner_allocation_tracker: ?*owner_alloc.HostAllocationTracker = null,
    slot_accounted_live_bytes: [metal_runtime.attention_span_slot_capacity]usize = @splat(0),
    cyclic_page_table_cache: storage_runtime.CyclicPageTableCache = .{},
    attention_match_runner: ?attention_match_storage.Runner = null,
    attention_match_binding: ?*AttentionMatchBinding = null,
    attention_match_revision: u64 = 0,

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
        return createTracked(allocator, runtime, dtype, num_kv_heads, head_dim, page_size_tokens, null);
    }

    pub fn createTracked(
        allocator: std.mem.Allocator,
        runtime: *metal_runtime.RawMetalDecodeRuntime,
        dtype: pool_mod.KvDType,
        num_kv_heads: u32,
        head_dim: u32,
        page_size_tokens: u16,
        owner_allocation_tracker: ?*owner_alloc.HostAllocationTracker,
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
            .owner_allocation_tracker = owner_allocation_tracker,
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
        errdefer self.reclaimSlot(slot);
        self.slot_ring_page_count[slot] = 0;
        self.slot_ring_policy_initialized[slot] = false;
        self.slot_buffer_capacity_tokens[slot] = 0;
        // A released raw-runtime slot may retain model-owned cached buffers.
        // Once leased, their actual allocatedSize is request-exclusive for
        // this hook's lifetime and belongs in the request ledger.
        try self.reconcileSlotAccounting(slot, 0);
        return slot;
    }

    fn reclaimSlot(self: *MetalKvStorage, slot: usize) void {
        if (slot >= self.leased_slots.len or !self.leased_slots[slot]) return;
        if (metal_runtime.termite_metal_decode_runtime_release_paged_kv_slot(self.runtime, slot) != 0)
            @panic("request-exclusive Metal KV slot released before backend drain");
        self.reconcileSlotAccounting(slot, 0) catch
            @panic("request-exclusive Metal KV release accounting failed");
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
        self.reconcileSlotAccounting(slot, 0) catch
            @panic("request-exclusive Metal KV discard accounting failed");
        self.leased_slots[slot] = false;
        self.slot_ring_page_count[slot] = 0;
        self.slot_ring_policy_initialized[slot] = false;
        self.slot_buffer_capacity_tokens[slot] = 0;
    }

    fn slotAllocationInfo(self: *MetalKvStorage, slot: usize) !metal_runtime.PagedKvSlotAllocationInfo {
        var info: metal_runtime.PagedKvSlotAllocationInfo = undefined;
        if (metal_runtime.termite_metal_decode_runtime_attention_span_slot_allocation_info(self.runtime, slot, &info) != 0)
            return error.DeviceAllocationSnapshotUnavailable;
        return info;
    }

    fn slotLiveBytes(info: metal_runtime.PagedKvSlotAllocationInfo) !usize {
        const installed: usize = @intCast(info.installed_live_bytes);
        const active: usize = @intCast(info.active_frame_retained_bytes);
        const submitted: usize = @intCast(info.submitted_frame_retained_bytes);
        return try std.math.add(usize, try std.math.add(usize, installed, active), submitted);
    }

    fn slotTransitionInfo(self: *MetalKvStorage, slot: usize) !metal_runtime.PagedKvSlotTransitionInfo {
        var info: metal_runtime.PagedKvSlotTransitionInfo = undefined;
        if (metal_runtime.termite_metal_decode_runtime_attention_span_slot_transition_info(self.runtime, slot, &info) != 0)
            return error.DeviceAllocationSnapshotUnavailable;
        return info;
    }

    fn reconcileSlotAccounting(self: *MetalKvStorage, slot: usize, newly_allocated_bytes: usize) !void {
        const tracker = self.owner_allocation_tracker orelse return;
        const prior = self.slot_accounted_live_bytes[slot];
        const info = try self.slotAllocationInfo(slot);
        const final = if (info.leased == 0) 0 else try slotLiveBytes(info);
        try tracker.recordDeviceTransition(.private_device, prior, newly_allocated_bytes, final);
        self.slot_accounted_live_bytes[slot] = final;
    }

    /// Bind a sequence/layer to either the layer-global full-history page pool
    /// or a dedicated cyclic SWA ring. `share_global` must be true before
    /// global sharing is allowed; legacy contiguous writes remain
    /// sequence-owned.
    fn acquireBinding(
        self: *MetalKvStorage,
        key: SlotKey,
        ring_page_count: usize,
        sliding_window_tokens: usize,
        share_global: bool,
        compactable_global: bool,
    ) !*SlotBinding {
        if (self.slot_map.getPtr(key)) |binding| {
            if (binding.ring_page_count != ring_page_count) return error.DeviceWriteFallback;
            if (compactable_global and !binding.compactable_global) return error.DeviceWriteFallback;
            if (binding.sliding_window_tokens != sliding_window_tokens) return error.DeviceWriteFallback;
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

        try self.slot_map.put(self.allocator, key, .{
            .slot = slot,
            .ring_page_count = ring_page_count,
            .sliding_window_tokens = sliding_window_tokens,
            .sequence_owned = owns_slot,
            .compactable_global = compactable_global,
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

    fn identityPageTokenOffsets(
        self: *MetalKvStorage,
        needed_blocks: usize,
        page_size_tokens: usize,
    ) !storage_runtime.PageTokenOffsets {
        if (needed_blocks == 0 or page_size_tokens == 0) return error.KvCapacityTooSmall;
        const offsets = try self.allocator.alloc(u32, needed_blocks);
        errdefer self.allocator.free(offsets);
        for (offsets, 0..) |*offset, block_index| {
            const token_offset = std.math.mul(usize, block_index, page_size_tokens) catch return error.KvCapacityTooSmall;
            offset.* = std.math.cast(u32, token_offset) orelse return error.KvCapacityTooSmall;
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
        // Reconcile generations retired since the previous frame before
        // recording this transition. This has no allocation overlap.
        try self.reconcileSlotAccounting(slot, 0);
        const transition_before = try self.slotTransitionInfo(slot);
        const rc = metal_runtime.termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
            self.runtime,
            slot,
            @backingInt(self.format),
            target,
            key_row_bytes,
            v_row_stride,
        );
        const transition_after = try self.slotTransitionInfo(slot);
        if (transition_after.operation_nonce != transition_before.operation_nonce +% 1)
            return error.DeviceAllocationSnapshotUnavailable;
        const newly_allocated = std.math.cast(usize, transition_after.newly_allocated_bytes) orelse
            return error.DeviceAllocationReceiptOverflow;
        // The C receipt survives every failure return, including a new
        // allocation that is released before this after-call snapshot.
        try self.reconcileSlotAccounting(slot, newly_allocated);
        if (transition_after.overflow != 0)
            return error.DeviceAllocationReceiptOverflow;
        if (rc != 0) return error.DeviceWriteFallback;
        self.slot_buffer_capacity_tokens[slot] = target;
        _ = self.allocationSnapshot();
    }

    fn allocationSnapshotOp(ctx: *anyopaque) storage_runtime.DeviceAllocationSnapshot {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        return self.allocationSnapshot();
    }

    fn allocationSnapshot(self: *MetalKvStorage) storage_runtime.DeviceAllocationSnapshot {
        var snapshot = storage_runtime.DeviceAllocationSnapshot{ .available = true };
        for (self.leased_slots, 0..) |leased, slot| {
            if (!leased) continue;
            var info: metal_runtime.PagedKvSlotAllocationInfo = undefined;
            if (metal_runtime.termite_metal_decode_runtime_attention_span_slot_allocation_info(self.runtime, slot, &info) != 0 or info.leased == 0)
                return .{};
            snapshot.kv_bytes +|= info.installed_live_bytes;
            snapshot.live_bytes +|= info.installed_live_bytes;
            snapshot.live_bytes +|= info.active_frame_retained_bytes;
            snapshot.live_bytes +|= info.submitted_frame_retained_bytes;
            snapshot.readback_bytes +|= info.lease_readback_bytes;
        }
        self.allocation_peak_bytes = @max(self.allocation_peak_bytes, snapshot.live_bytes);
        snapshot.peak_bytes = self.allocation_peak_bytes;
        return snapshot;
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
            .owner_accounting = if (self.owner_allocation_tracker != null)
                .exact_private_device
            else
                .unavailable,
        };
    }

    pub fn fromDeviceWriteHook(hook: storage_runtime.DeviceWriteHook) !*MetalKvStorage {
        if (hook.vtable != &hook_vtable) return error.UnsupportedAttentionMatchRoute;
        return @ptrCast(@alignCast(hook.ctx));
    }

    pub fn attentionMatchBackend(
        self: *MetalKvStorage,
        allocator: std.mem.Allocator,
        limits: @import("../runtime/kv/attention_match_checkpoint.zig").Limits,
        max_device_bytes: usize,
    ) !attention_match_storage.Backend {
        if (self.format != .f16) return error.UnsupportedMetalKvCheckpointEncoding;
        const tracker = self.owner_allocation_tracker orelse return error.ContextPhysicalAccountingUnavailable;
        if (self.attention_match_runner == null) {
            self.attention_match_runner = try attention_match_storage.Runner.init(
                max_device_bytes,
                tracker,
            );
        } else self.attention_match_runner.?.raiseMaxDeviceBytes(max_device_bytes);
        return self.attention_match_runner.?.backend(allocator, limits);
    }

    pub fn attentionMatchRunner(self: *MetalKvStorage) ?*attention_match_storage.Runner {
        if (self.attention_match_runner) |*runner| return runner;
        return null;
    }

    pub fn retireExactKvForAttentionMatch(self: *MetalKvStorage) void {
        var binding_it = self.slot_map.valueIterator();
        while (binding_it.next()) |binding| {
            if (binding.selected_positions) |positions| self.allocator.free(positions);
        }
        self.slot_map.clearRetainingCapacity();
        self.global_layer_slots.clearRetainingCapacity();
        for (&self.leased_slots, 0..) |*leased, slot| {
            if (!leased.*) continue;
            self.reclaimSlot(slot);
        }
    }

    pub fn validateExactAttentionMatchSource(
        self: *MetalKvStorage,
        sequence: storage_runtime.SequenceId,
        expected_length: usize,
        candidate: *attention_match_storage.DeviceState,
    ) !void {
        if (self.format != .f16 or self.attention_match_binding != null or candidate.heads.len == 0)
            return error.UnsupportedAttentionMatchRoute;
        var donors: usize = 0;
        for (candidate.heads) |head| {
            if (head.logical_length != @as(u64, @intCast(expected_length))) return error.InvalidKvCheckpoint;
            if (head.head != 0) continue;
            donors += 1;
            const heads = try candidate.layer(head.layer);
            const binding = self.slot_map.get(.{ .sequence_id = sequence, .layer_index = head.layer }) orelse
                return error.StaleKvCompaction;
            if (binding.selected_positions != null or binding.ring_page_count != 0 or
                !binding.logical_contiguous or binding.position_offset != 0 or
                binding.physical_base_tokens != 0 or binding.written_tokens != expected_length)
                return error.StaleKvCompaction;
            try validateExactAttentionMatchGeometry(self.num_kv_heads, self.head_dim, heads, try self.slotInfo(binding.slot), expected_length);
        }
        if (donors != self.slot_map.count()) return error.UnsupportedAttentionMatchRoute;
    }

    fn validateExactAttentionMatchGeometry(pool_heads: u32, maximum_dim: u32, heads: []const attention_match_storage.Head, info: SlotInfo, expected_length: usize) !void {
        // Pool dimensions are allocation maxima. Gemma4 local/global donors
        // have different widths; the encoded source slot proves the actual
        // width. Keep the existing fixed-head-count route restriction so a
        // different head grouping cannot alias the same combined row width.
        if (heads.len == 0 or heads.len != pool_heads or heads[0].dim == 0 or heads[0].dim > maximum_dim)
            return error.InvalidKvCheckpoint;
        const dim = heads[0].dim;
        for (heads, 0..) |head, index| {
            if (head.dim != dim or head.head != index or head.layer != heads[0].layer or
                head.window != heads[0].window or head.logical_length != expected_length)
                return error.InvalidKvCheckpoint;
        }
        const width = try std.math.mul(usize, heads.len, dim);
        const row_bytes = try std.math.mul(usize, width, @sizeOf(f16));
        const bytes = try std.math.mul(usize, expected_length, row_bytes);
        if (info.tokens != expected_length or info.position_offset != 0 or
            info.key_row_bytes != row_bytes or info.v_row_stride != width or
            info.encoded_key_handle == null or info.v_handle == null or
            info.encoded_key_capacity < bytes or info.v_capacity < bytes)
            return error.InvalidKvCheckpoint;
    }

    fn cloneAllocationBytes(request: storage_runtime.device_clone.Request) !usize {
        const common = try request.allocationBytes(@sizeOf(f16));
        var metal_bytes: usize = 0;
        for (request.donors) |donor| {
            const width = try std.math.mul(usize, donor.heads, donor.dim);
            const row_bytes = try std.math.mul(usize, width, @sizeOf(f16));
            const buffer_bytes = try std.math.mul(usize, request.capacity, row_bytes);
            // Metal private buffers may round their backing allocation to a
            // 16 KiB VM page. Bound K and V independently before allocating.
            const rounded = try std.math.add(usize, buffer_bytes, 16 * 1024 - 1);
            const per_buffer = rounded & ~@as(usize, 16 * 1024 - 1);
            metal_bytes = try std.math.add(usize, metal_bytes, try std.math.mul(usize, per_buffer, 2));
        }
        const allocation_bound = @max(common, metal_bytes);
        if (allocation_bound > request.max_bytes) return error.DeviceKvCloneBudgetExceeded;
        return allocation_bound;
    }

    fn cloneExactPrefix(
        ctx: *anyopaque,
        source_hook: storage_runtime.DeviceWriteHook,
        request: storage_runtime.device_clone.Request,
    ) anyerror!storage_runtime.device_clone.Result {
        const destination: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (source_hook.vtable != &hook_vtable or source_hook.ctx == ctx)
            return error.InvalidDeviceKvClone;
        const source: *MetalKvStorage = @ptrCast(@alignCast(source_hook.ctx));
        if (source.runtime != destination.runtime or source.format != .f16 or destination.format != .f16 or
            source.attention_match_binding != null or destination.attention_match_binding != null or
            source.page_size_tokens != request.page_size or destination.page_size_tokens != request.page_size or
            source.slot_map.count() != request.donors.len or destination.slot_map.count() != 0 or
            destination.global_layer_slots.count() != 0 or metal_runtime.hasActiveFrame(destination.runtime) or
            metal_runtime.hasSubmittedFrame(destination.runtime))
            return error.DeviceKvCloneUnsupported;

        const allocation_bound = try cloneAllocationBytes(request);
        try request.check();

        const Staged = struct {
            key: SlotKey,
            slot: usize,
            layout: @TypeOf(destination.rowLayout(1, 1)),
        };
        var staged: [256]Staged = undefined;

        // Validate the complete immutable source before reserving destination
        // map storage or leasing a single Metal slot.
        for (request.donors, 0..) |donor, index| {
            const key = SlotKey{ .sequence_id = request.source_sequence, .layer_index = donor.layer };
            const binding = source.slot_map.get(key) orelse return error.InvalidDeviceKvClone;
            const layout = source.rowLayout(donor.heads, donor.dim);
            if (binding.selected_positions != null or binding.ring_page_count != 0 or
                !binding.logical_contiguous or binding.physical_base_tokens != 0 or
                binding.written_tokens != request.tokens or binding.position_offset != 0)
                return error.UnsupportedDeviceKvCloneLayout;
            const info = try source.slotInfo(binding.slot);
            const key_bytes = try std.math.mul(usize, request.tokens, layout.key_row_bytes);
            const value_bytes = try std.math.mul(
                usize,
                try std.math.mul(usize, request.tokens, layout.v_row_stride),
                @sizeOf(f16),
            );
            if (info.tokens != request.tokens or info.position_offset != 0 or
                info.key_row_bytes != layout.key_row_bytes or info.v_row_stride != layout.v_row_stride or
                info.encoded_key_handle == null or info.v_handle == null or
                info.encoded_key_capacity < key_bytes or info.v_capacity < value_bytes)
                return error.UnsupportedDeviceKvCloneLayout;
            staged[index] = .{
                .key = .{ .sequence_id = request.destination_sequence, .layer_index = donor.layer },
                .slot = binding.slot,
                .layout = layout,
            };
        }
        try destination.slot_map.ensureUnusedCapacity(destination.allocator, @intCast(request.donors.len));

        var staged_count: usize = 0;
        errdefer for (staged[0..staged_count]) |entry| destination.discardPrivateSlot(entry.slot);
        var copied_bytes: usize = 0;
        for (staged[0..request.donors.len]) |*entry| {
            try request.check();
            const source_slot = entry.slot;
            var destination_slot = try destination.allocateSlot();
            errdefer destination.discardPrivateSlot(destination_slot);

            const requested_buffer_bytes = try std.math.mul(usize, request.capacity, entry.layout.key_row_bytes);
            const rounded = try std.math.add(usize, requested_buffer_bytes, 16 * 1024 - 1);
            const per_donor_bound = try std.math.mul(usize, rounded & ~@as(usize, 16 * 1024 - 1), 2);
            var allocation = try destination.slotAllocationInfo(destination_slot);
            if (try slotLiveBytes(allocation) > per_donor_bound) {
                // A released slot may retain a larger previous tenant's
                // backing. Drop it so this owner's admitted bound stays exact.
                destination.discardPrivateSlot(destination_slot);
                destination_slot = try destination.allocateSlot();
                allocation = try destination.slotAllocationInfo(destination_slot);
                if (allocation.installed_live_bytes != 0) return error.DeviceKvCloneBudgetExceeded;
            }
            try destination.reserveSlotCapacity(
                destination_slot,
                request.capacity,
                request.page_size,
                0,
                entry.layout.key_row_bytes,
                entry.layout.v_row_stride,
            );
            allocation = try destination.slotAllocationInfo(destination_slot);
            if (try slotLiveBytes(allocation) > per_donor_bound)
                return error.DeviceKvCloneBudgetExceeded;

            const clone_rc = metal_runtime.termite_metal_decode_runtime_clone_attention_span_slot_prefix(
                destination.runtime,
                source_slot,
                destination_slot,
                @backingInt(destination.format),
                request.tokens,
                entry.layout.key_row_bytes,
                entry.layout.v_row_stride,
                0,
            );
            if (clone_rc != 0) return error.DeviceKvCloneUnsupported;
            entry.slot = destination_slot;
            destination.slot_ring_page_count[destination_slot] = 0;
            destination.slot_ring_policy_initialized[destination_slot] = true;
            copied_bytes = try std.math.add(usize, copied_bytes, try std.math.mul(
                usize,
                request.tokens,
                try std.math.mul(usize, entry.layout.key_row_bytes, 2),
            ));
            staged_count += 1;
        }
        try request.check();

        // Map capacity was reserved before any device mutation. Publication is
        // now infallible and transfers ownership of every staged slot together.
        for (staged[0..staged_count]) |entry| {
            destination.slot_map.putAssumeCapacityNoClobber(entry.key, .{
                .slot = entry.slot,
                .sequence_owned = true,
                .logical_contiguous = true,
                .physical_base_tokens = 0,
                .written_tokens = request.tokens,
                .position_offset = 0,
            });
        }
        staged_count = 0;
        destination.attention_match_revision +|= 1;
        return .{
            .allocation_bound_bytes = allocation_bound,
            .copied_bytes = copied_bytes,
            .donors = request.donors.len,
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
        if (self.attention_match_binding != null) return error.UnsupportedAttentionMatchRoute;
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
            if (requested_ring_pages > 0) write.sliding_window else 0,
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
            self.attention_match_revision +|= 1;
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
        self.attention_match_revision +|= 1;
    }

    fn reserveLayerKvDevice(
        ctx: *anyopaque,
        reserve: storage_runtime.DeviceKvLayerReserve,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (self.attention_match_binding != null) return error.UnsupportedAttentionMatchRoute;
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
            if (requested_ring_pages > 0) reserve.sliding_window else 0,
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
        if (self.attention_match_binding != null) return error.UnsupportedAttentionMatchRoute;
        const binding = self.slot_map.getPtr(.{
            .sequence_id = commit.sequence_id,
            .layer_index = @intCast(commit.layer_index),
        }) orelse return error.DeviceWriteFallback;
        if (binding.selected_positions != null) return error.DeviceWriteFallback;
        if (!binding.commitWrite(commit.total_token_count, commit.position_offset)) return error.DeviceWriteFallback;
        self.attention_match_revision +|= 1;
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
        try self.reserveSlotCapacity(
            destination_slot,
            destination_capacity,
            self.page_size_tokens,
            0,
            layout.key_row_bytes,
            layout.v_row_stride,
        );
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
        const layer_index = std.math.cast(u32, request.layer_index) orelse return error.InvalidKvShape;
        const binding = self.slot_map.get(.{ .sequence_id = request.sequence_id, .layer_index = layer_index }) orelse return error.DeviceReadFallback;
        if (request.key_positions.len == 0 or request.scores_out.len != request.key_positions.len) return error.InvalidKvShape;
        const query_head_values = std.math.mul(usize, request.query_positions.len, request.num_heads) catch return error.InvalidKvShape;
        const query_values = std.math.mul(usize, query_head_values, request.head_dim) catch return error.InvalidKvShape;
        if (request.query_positions.len == 0 or request.queries.len != query_values) return error.InvalidKvShape;
        if (request.num_heads == 0 or request.num_heads > 256 or request.num_kv_heads == 0 or request.head_dim == 0 or
            request.num_kv_heads > std.math.maxInt(u32) or request.head_dim > std.math.maxInt(u32) or
            request.num_heads % request.num_kv_heads != 0) return error.InvalidKvShape;
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
        // The scorer consumes compact physical rows from zero. A paged/ring
        // binding with a nonzero physical base needs the generic attention
        // route; silently treating its logical row zero as physical row zero
        // would score another sequence's keys.
        if (!binding.logical_contiguous or binding.physical_base_tokens != 0) return error.DeviceReadFallback;
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

    fn hookDeinit(ctx: *anyopaque, _: std.mem.Allocator) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        // The hook owns its allocator-backed state. The storage runtime may
        // have a different allocator; destruction must use the one that
        // created the hook and its maps.
        const allocator = self.allocator;
        drainForOwnerSnapshot(self) catch
            @panic("request-exclusive Metal KV drain failed during teardown");
        if (self.attention_match_binding) |binding| {
            binding.deinit();
            allocator.destroy(binding);
            self.attention_match_binding = null;
        }
        var binding_it = self.slot_map.valueIterator();
        while (binding_it.next()) |binding| {
            if (binding.selected_positions) |positions| allocator.free(positions);
            if (binding.selected_positions != null and binding.sequence_owned) self.discardPrivateSlot(binding.slot);
        }
        for (&self.leased_slots, 0..) |*leased, slot| {
            if (!leased.*) continue;
            self.reclaimSlot(slot);
        }
        self.slot_map.deinit(allocator);
        self.global_layer_slots.deinit(allocator);
        self.cyclic_page_table_cache.deinit(allocator);
        if (self.attention_match_runner) |*runner| runner.deinit();
        allocator.destroy(self);
    }

    fn drainForOwnerSnapshot(ctx: *anyopaque) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        const runtime = self.runtime;
        if (metal_runtime.hasActiveFrame(runtime)) try metal_runtime.cancelFrame(runtime);
        if (metal_runtime.hasSubmittedFrame(runtime)) try metal_runtime.waitFrame(runtime);
        for (self.leased_slots, 0..) |leased, slot| {
            if (!leased) continue;
            try self.reconcileSlotAccounting(slot, 0);
        }
    }

    fn releaseSequenceOp(ctx: *anyopaque, sequence_id: storage_runtime.SequenceId) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (self.attention_match_binding) |binding| if (binding.sequence == sequence_id) {
            binding.deinit();
            self.allocator.destroy(binding);
            self.attention_match_binding = null;
        };
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
        self.attention_match_revision +|= 1;
        if (self.attention_match_binding) |binding| if (binding.sequence == sequence_id) {
            if (binding.invalid) return;
            const previous = binding.device.logical_length;
            binding.device.truncate(@intCast(retained_token_count), .{}) catch {
                binding.invalid = true;
                return;
            };
            binding.rollback_tokens +|= @intCast(previous - @as(u64, @intCast(retained_token_count)));
            return;
        };
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

    fn readExactAttentionMatchKv(
        ctx: *anyopaque,
        range: storage_runtime.attention_match_snapshot.Range,
        keys: []f32,
        values: []f32,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (self.attention_match_binding != null) return error.UnsupportedAttentionMatchRoute;
        if (self.format != .f16 or range.count == 0 or range.heads == 0 or range.dim == 0 or
            range.start > range.logical_length or range.count > range.logical_length - range.start)
            return error.UnsupportedKvSnapshotLayout;
        const width = std.math.mul(usize, range.heads, range.dim) catch return error.InvalidKvCheckpoint;
        const elements = std.math.mul(usize, range.count, width) catch return error.InvalidKvCheckpoint;
        if (keys.len != elements or values.len != elements or
            elements > storage_runtime.attention_match_snapshot.chunk_elements)
            return error.InvalidKvCheckpoint;
        const binding = self.slot_map.get(.{
            .sequence_id = range.sequence,
            .layer_index = range.layer,
        }) orelse return error.DeviceReadUnsupported;
        if (binding.selected_positions != null or binding.ring_page_count != 0 or
            !binding.logical_contiguous or binding.position_offset != 0 or
            binding.written_tokens != range.logical_length)
            return error.UnsupportedKvSnapshotLayout;
        const info = try self.slotInfo(binding.slot);
        const row_bytes = std.math.mul(usize, width, @sizeOf(f16)) catch return error.InvalidKvCheckpoint;
        if (info.key_row_bytes != row_bytes or info.v_row_stride != width or info.tokens < range.logical_length)
            return error.UnsupportedKvSnapshotLayout;
        const first_row = std.math.add(usize, binding.physical_base_tokens, range.start) catch return error.InvalidKvCheckpoint;
        const byte_offset = std.math.mul(usize, first_row, row_bytes) catch return error.InvalidKvCheckpoint;
        const byte_count = std.math.mul(usize, range.count, row_bytes) catch return error.InvalidKvCheckpoint;
        if (byte_offset > info.encoded_key_capacity or byte_count > info.encoded_key_capacity - byte_offset or
            byte_offset > info.v_capacity or byte_count > info.v_capacity - byte_offset)
            return error.InvalidKvCheckpoint;
        const encoded_keys = std.mem.sliceAsBytes(keys)[0..byte_count];
        const encoded_values = std.mem.sliceAsBytes(values)[0..byte_count];
        if (metal_runtime.termite_metal_buffer_download(
            self.runtime,
            info.encoded_key_handle orelse return error.DeviceReadUnsupported,
            byte_offset,
            @ptrCast(encoded_keys.ptr),
            byte_count,
        ) != 0) return error.MetalDeviceReadFailed;
        if (metal_runtime.termite_metal_buffer_download(
            self.runtime,
            info.v_handle orelse return error.DeviceReadUnsupported,
            byte_offset,
            @ptrCast(encoded_values.ptr),
            byte_count,
        ) != 0) return error.MetalDeviceReadFailed;
        const readback_bytes = std.math.mul(usize, byte_count, 2) catch return error.InvalidKvCheckpoint;
        if (metal_runtime.termite_metal_decode_runtime_attention_span_slot_note_readback(
            self.runtime,
            binding.slot,
            readback_bytes,
        ) != 0) return error.DeviceAllocationSnapshotUnavailable;
        // Expand backwards so the caller's bounded f32 output is also the
        // only staging allocation needed for exact f16 readback.
        for ([_][]f32{ keys, values }) |output| {
            const half: [*]const f16 = @ptrCast(@alignCast(output.ptr));
            var index = output.len;
            while (index != 0) {
                index -= 1;
                output[index] = @floatCast(half[index]);
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

    pub const QualificationFingerprintRequest = struct {
        context_instance_id: u64,
        sequence_id: storage_runtime.SequenceId,
        logical_tokens: u32,
        position_offset: u32,
        logical_blocks: []const block.KvBlockId,
        /// Actual KV owners in canonical loaded-layout order. Pool
        /// `num_layers_packed` is a physical stride/capacity and includes
        /// shared consumers that never own a storage binding.
        owner_layers: []const u32,
    };

    /// Qualification-only raw fingerprint of an ordinary exact Metal KV
    /// context. Serving never calls this method: it deliberately drains and
    /// downloads every logically visible K/V byte into bounded page staging.
    /// Reserved capacity and overwritten SWA rows are excluded. Payload order
    /// is layer, logical token, K row, V row.
    pub fn qualificationFingerprintDeviceHook(
        hook: storage_runtime.DeviceWriteHook,
        allocator: std.mem.Allocator,
        request: QualificationFingerprintRequest,
    ) !context_teacher_forced.QualityFingerprint {
        if (!builtin.is_test) return error.QualificationOnly;
        if (hook.vtable != &hook_vtable) return error.OrdinaryExactMetalKvRequired;
        const self: *MetalKvStorage = @ptrCast(@alignCast(hook.ctx));
        return self.qualificationFingerprint(allocator, request);
    }

    fn qualificationPhysicalRow(
        binding: SlotBinding,
        logical_blocks: []const block.KvBlockId,
        page_size: usize,
        logical_token: usize,
    ) !usize {
        if (page_size == 0) return error.InvalidQualificationFingerprint;
        const logical_page = logical_token / page_size;
        const in_page = logical_token % page_size;
        const physical_page = if (binding.ring_page_count > 0)
            storage_runtime.physicalPageForLogical(logical_page, binding.ring_page_count)
        else blk: {
            if (logical_page >= logical_blocks.len) return error.InvalidQualificationFingerprint;
            break :blk @as(usize, logical_blocks[logical_page]);
        };
        return std.math.add(
            usize,
            try std.math.mul(usize, physical_page, page_size),
            in_page,
        ) catch return error.QualificationFingerprintOverflow;
    }

    fn qualificationFingerprint(
        self: *MetalKvStorage,
        allocator: std.mem.Allocator,
        request: QualificationFingerprintRequest,
    ) !context_teacher_forced.QualityFingerprint {
        const owner_count = request.owner_layers.len;
        if (self.format != .f16 or request.context_instance_id == 0 or request.logical_tokens == 0 or
            request.position_offset != 0)
            return error.InvalidQualificationFingerprint;
        try validateQualificationOwnerLayers(
            request.owner_layers,
            self.global_layer_slots.count() + @as(usize, self.countSequenceOwnedLayers(request.sequence_id)),
        );
        if (metal_runtime.hasActiveFrame(self.runtime)) return error.QualificationFingerprintFrameActive;
        if (metal_runtime.hasSubmittedFrame(self.runtime)) try metal_runtime.waitFrame(self.runtime);
        if (metal_runtime.hasActiveFrame(self.runtime) or metal_runtime.hasSubmittedFrame(self.runtime))
            return error.QualificationFingerprintFrameActive;

        const logical_tokens: usize = request.logical_tokens;
        const page_size: usize = self.page_size_tokens;
        if (page_size == 0) return error.InvalidQualificationFingerprint;
        const needed_blocks = std.math.divCeil(usize, logical_tokens, page_size) catch
            return error.QualificationFingerprintOverflow;
        if (request.logical_blocks.len < needed_blocks) return error.InvalidQualificationFingerprint;

        var descriptor = std.crypto.hash.sha2.Sha256.init(.{});
        descriptor.update("antfly.ordinary-exact-context.descriptor.v1\x00");
        try fingerprintU32(&descriptor, @backingInt(self.format));
        try fingerprintU32(&descriptor, self.num_kv_heads);
        try fingerprintU32(&descriptor, self.head_dim);
        try fingerprintU32(&descriptor, page_size);
        try fingerprintU32(&descriptor, owner_count);
        try fingerprintU32(&descriptor, logical_tokens);
        try fingerprintU32(&descriptor, request.position_offset);

        for (request.owner_layers) |layer_u32| {
            const layer: usize = @intCast(layer_u32);
            const binding = self.slot_map.get(.{
                .sequence_id = request.sequence_id,
                .layer_index = layer_u32,
            }) orelse return error.InvalidQualificationFingerprint;
            if (binding.written_tokens != logical_tokens) return error.InvalidQualificationFingerprint;
            const info = try self.slotInfo(binding.slot);
            const v_element_bytes: usize = if (self.format == .f16) @sizeOf(u16) else @sizeOf(f32);
            const v_row_bytes = std.math.mul(usize, info.v_row_stride, v_element_bytes) catch
                return error.QualificationFingerprintOverflow;
            if (info.key_row_bytes == 0 or v_row_bytes == 0 or info.encoded_key_handle == null or info.v_handle == null)
                return error.InvalidQualificationFingerprint;
            if (info.tokens != binding.written_tokens or info.position_offset != binding.position_offset)
                return error.InvalidQualificationFingerprint;
            const visible = try qualificationVisibleRange(binding, logical_tokens, page_size);
            const visible_tokens = visible.count;
            const logical_start = visible.start;
            try fingerprintQualificationLayer(
                &descriptor,
                layer,
                binding,
                logical_start,
                visible_tokens,
                logical_tokens,
                page_size,
                info.key_row_bytes,
                info.v_row_stride,
                v_element_bytes,
            );
        }
        var descriptor_sha256: [32]u8 = undefined;
        descriptor.final(&descriptor_sha256);

        var payload = std.crypto.hash.sha2.Sha256.init(.{});
        payload.update("antfly.ordinary-exact-context.payload.v1\x00");
        var readback_bytes: u64 = 0;
        for (request.owner_layers) |layer_u32| {
            const binding = self.slot_map.get(.{
                .sequence_id = request.sequence_id,
                .layer_index = layer_u32,
            }).?;
            const info = try self.slotInfo(binding.slot);
            const key_handle = info.encoded_key_handle.?;
            const v_handle = info.v_handle.?;
            const v_element_bytes: usize = if (self.format == .f16) @sizeOf(u16) else @sizeOf(f32);
            const v_row_bytes = try std.math.mul(usize, info.v_row_stride, v_element_bytes);
            const visible = try qualificationVisibleRange(binding, logical_tokens, page_size);
            var logical_token = visible.start;
            while (logical_token < logical_tokens) {
                const page_end = @min(logical_tokens, (logical_token / page_size + 1) * page_size);
                const row_count = page_end - logical_token;
                const physical_row = try qualificationPhysicalRow(binding, request.logical_blocks, page_size, logical_token);
                const key_bytes = try std.math.mul(usize, row_count, info.key_row_bytes);
                const value_bytes = try std.math.mul(usize, row_count, v_row_bytes);
                const key_offset = try std.math.mul(usize, physical_row, info.key_row_bytes);
                const value_offset = try std.math.mul(usize, physical_row, v_row_bytes);
                if (key_offset > info.encoded_key_capacity or key_bytes > info.encoded_key_capacity - key_offset or
                    value_offset > info.v_capacity or value_bytes > info.v_capacity - value_offset)
                    return error.InvalidQualificationFingerprint;
                const keys = try allocator.alloc(u8, key_bytes);
                defer allocator.free(keys);
                const values = try allocator.alloc(u8, value_bytes);
                defer allocator.free(values);
                if (metal_runtime.termite_metal_buffer_download(self.runtime, key_handle, key_offset, @ptrCast(keys.ptr), keys.len) != 0 or
                    metal_runtime.termite_metal_buffer_download(self.runtime, v_handle, value_offset, @ptrCast(values.ptr), values.len) != 0)
                    return error.QualificationFingerprintReadbackFailed;
                var row: usize = 0;
                while (row < row_count) : (row += 1) {
                    payload.update(keys[row * info.key_row_bytes ..][0..info.key_row_bytes]);
                    payload.update(values[row * v_row_bytes ..][0..v_row_bytes]);
                }
                const segment_bytes = std.math.add(usize, key_bytes, value_bytes) catch return error.QualificationFingerprintOverflow;
                readback_bytes = std.math.add(u64, readback_bytes, std.math.cast(u64, segment_bytes) orelse
                    return error.QualificationFingerprintOverflow) catch return error.QualificationFingerprintOverflow;
                logical_token = page_end;
            }
        }
        payload.update(&descriptor_sha256);
        var payload_sha256: [32]u8 = undefined;
        payload.final(&payload_sha256);
        if (readback_bytes == 0) return error.QualificationFingerprintOverflow;
        return .{
            .context_instance_id = request.context_instance_id,
            .publication_epoch = 1,
            .logical_tokens = request.logical_tokens,
            .payload_sha256 = payload_sha256,
            .descriptor_sha256 = descriptor_sha256,
            .diagnostic_kv_readback_bytes = readback_bytes,
        };
    }

    fn countSequenceOwnedLayers(self: *const MetalKvStorage, sequence_id: storage_runtime.SequenceId) u32 {
        var count: u32 = 0;
        var iterator = self.slot_map.iterator();
        while (iterator.next()) |entry| {
            if (entry.key_ptr.sequence_id == sequence_id and entry.value_ptr.sequence_owned) count +|= 1;
        }
        return count;
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

test "attention_match exact source validates actual local and global slot widths" {
    var heads = [_]attention_match_storage.Head{
        .{ .layer = 0, .head = 0, .dim = 256, .logical_length = 3 },
        .{ .layer = 0, .head = 1, .dim = 256, .logical_length = 3 },
    };
    var info: MetalKvStorage.SlotInfo = .{
        .encoded_key_handle = @ptrFromInt(0x1000),
        .v_handle = @ptrFromInt(0x2000),
        .tokens = 3,
        .key_row_bytes = 1024,
        .v_row_stride = 512,
        .encoded_key_capacity = 3072,
        .v_capacity = 3072,
    };
    try MetalKvStorage.validateExactAttentionMatchGeometry(2, 512, &heads, info, 3);
    for (&heads) |*head| head.dim = 512;
    // A wider candidate must not reinterpret the smaller source slot.
    try std.testing.expectError(error.InvalidKvCheckpoint, MetalKvStorage.validateExactAttentionMatchGeometry(2, 512, &heads, info, 3));
    info.key_row_bytes = 2048;
    info.v_row_stride = 1024;
    info.encoded_key_capacity = 6144;
    info.v_capacity = 6144;
    try MetalKvStorage.validateExactAttentionMatchGeometry(2, 512, &heads, info, 3);
    heads[0].dim = 256;
    try std.testing.expectError(error.InvalidKvCheckpoint, MetalKvStorage.validateExactAttentionMatchGeometry(2, 512, &heads, info, 3));
    heads[0].dim = 512;
    info.v_capacity -= 1;
    try std.testing.expectError(error.InvalidKvCheckpoint, MetalKvStorage.validateExactAttentionMatchGeometry(2, 512, &heads, info, 3));
    info.v_capacity += 1;
    info.tokens -= 1;
    try std.testing.expectError(error.InvalidKvCheckpoint, MetalKvStorage.validateExactAttentionMatchGeometry(2, 512, &heads, info, 3));
}

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

test "context ordinary exact fingerprint maps logical pages canonically" {
    const blocks = [_]block.KvBlockId{ 7, 3, 11, 2 };
    const full = SlotBinding{ .slot = 1 };
    try std.testing.expectEqual(@as(usize, 28), try MetalKvStorage.qualificationPhysicalRow(full, &blocks, 4, 0));
    try std.testing.expectEqual(@as(usize, 13), try MetalKvStorage.qualificationPhysicalRow(full, &blocks, 4, 5));
    try std.testing.expectEqual(@as(usize, 9), try MetalKvStorage.qualificationPhysicalRow(full, &blocks, 4, 13));

    const ring = SlotBinding{ .slot = 2, .ring_page_count = 3, .sequence_owned = true };
    try std.testing.expectEqual(@as(usize, 0), try MetalKvStorage.qualificationPhysicalRow(ring, &blocks, 4, 0));
    try std.testing.expectEqual(@as(usize, 9), try MetalKvStorage.qualificationPhysicalRow(ring, &blocks, 4, 9));
    try std.testing.expectEqual(@as(usize, 1), try MetalKvStorage.qualificationPhysicalRow(ring, &blocks, 4, 13));
    try std.testing.expectError(error.InvalidQualificationFingerprint, MetalKvStorage.qualificationPhysicalRow(full, &.{}, 4, 0));
    try std.testing.expectError(error.InvalidQualificationFingerprint, MetalKvStorage.qualificationPhysicalRow(full, &blocks, 0, 0));
}

test "context ordinary exact fingerprint uses actual owners rather than pool layer capacity" {
    // The pool reserves a stride for every model layer. Shared-tail consumers
    // do not own bindings, so the canonical list contains only the loaded
    // layout's non-shared owner prefix.
    var owners: [24]u32 = undefined;
    for (&owners, 0..) |*owner, layer| owner.* = @intCast(layer);
    try validateQualificationOwnerLayers(&owners, owners.len);
    try std.testing.expectError(
        error.InvalidQualificationFingerprint,
        validateQualificationOwnerLayers(&owners, 35),
    );
    var duplicate = owners;
    duplicate[23] = 22;
    try std.testing.expectError(
        error.InvalidQualificationFingerprint,
        validateQualificationOwnerLayers(&duplicate, duplicate.len),
    );
    var reordered = owners;
    std.mem.swap(u32, &reordered[7], &reordered[8]);
    try std.testing.expectError(
        error.InvalidQualificationFingerprint,
        validateQualificationOwnerLayers(&reordered, reordered.len),
    );
}

test "context ordinary exact fingerprint excludes allocator page identity" {
    const binding = SlotBinding{ .slot = 1, .written_tokens = 13 };
    const sibling_binding = SlotBinding{ .slot = 9, .written_tokens = 13 };
    const blocks_a = [_]block.KvBlockId{ 7, 3, 11, 2 };
    const blocks_b = [_]block.KvBlockId{ 17, 13, 21, 12 };
    try std.testing.expect(try MetalKvStorage.qualificationPhysicalRow(binding, &blocks_a, 4, 5) !=
        try MetalKvStorage.qualificationPhysicalRow(binding, &blocks_b, 4, 5));

    var a = std.crypto.hash.sha2.Sha256.init(.{});
    var b = std.crypto.hash.sha2.Sha256.init(.{});
    try fingerprintQualificationLayer(&a, 0, binding, 0, 13, 13, 4, 32, 16, 2);
    try fingerprintQualificationLayer(&b, 0, sibling_binding, 0, 13, 13, 4, 32, 16, 2);
    var digest_a: [32]u8 = undefined;
    var digest_b: [32]u8 = undefined;
    a.final(&digest_a);
    b.final(&digest_b);
    try std.testing.expectEqualSlices(u8, &digest_a, &digest_b);

    var changed = std.crypto.hash.sha2.Sha256.init(.{});
    try fingerprintQualificationLayer(&changed, 0, binding, 1, 12, 13, 4, 32, 16, 2);
    var changed_digest: [32]u8 = undefined;
    changed.final(&changed_digest);
    try std.testing.expect(!std.mem.eql(u8, &digest_a, &changed_digest));
}

test "context ordinary exact fingerprint excludes SWA ring slack" {
    const binding = SlotBinding{
        .slot = 3,
        .ring_page_count = 2,
        .sliding_window_tokens = 5,
        .sequence_owned = true,
    };
    const visible = try qualificationVisibleRange(binding, 11, 4);
    try std.testing.expectEqual(@as(usize, 6), visible.start);
    try std.testing.expectEqual(@as(usize, 5), visible.count);

    const blocks = [_]block.KvBlockId{ 0, 1, 2 };
    const base = [_]u8{ 10, 11, 12, 90, 91, 92, 16, 17 };
    var poisoned_slack = base;
    poisoned_slack[3] = 1;
    poisoned_slack[4] = 2;
    poisoned_slack[5] = 3;
    var changed_visible = base;
    changed_visible[0] ^= 0xff; // Logical token 8 maps to physical row zero.

    var base_hash = std.crypto.hash.sha2.Sha256.init(.{});
    var slack_hash = std.crypto.hash.sha2.Sha256.init(.{});
    var changed_hash = std.crypto.hash.sha2.Sha256.init(.{});
    var token = visible.start;
    while (token < visible.start + visible.count) : (token += 1) {
        const physical = try MetalKvStorage.qualificationPhysicalRow(binding, &blocks, 4, token);
        base_hash.update(base[physical..][0..1]);
        slack_hash.update(poisoned_slack[physical..][0..1]);
        changed_hash.update(changed_visible[physical..][0..1]);
    }
    var base_digest: [32]u8 = undefined;
    var slack_digest: [32]u8 = undefined;
    var changed_digest: [32]u8 = undefined;
    base_hash.final(&base_digest);
    slack_hash.final(&slack_digest);
    changed_hash.final(&changed_digest);
    try std.testing.expectEqualSlices(u8, &base_digest, &slack_digest);
    try std.testing.expect(!std.mem.eql(u8, &base_digest, &changed_digest));
}

const hook_vtable: storage_runtime.DeviceWriteHook.VTable = .{
    .readExactAttentionMatchKv = MetalKvStorage.readExactAttentionMatchKv,
    .cloneExactPrefix = MetalKvStorage.cloneExactPrefix,
    .writeLayerKvSuffix = MetalKvStorage.writeLayerKvSuffix,
    .gatherLayerKv = MetalKvStorage.gatherLayerKv,
    .gatherLayerKeys = MetalKvStorage.gatherLayerKeys,
    .gatherLayerKvDevice = MetalKvStorage.gatherLayerKvDevice,
    .pagedLayerKvDevice = MetalKvStorage.pagedLayerKvDevice,
    .reserveLayerKvDevice = MetalKvStorage.reserveLayerKvDevice,
    .commitLayerKvDeviceWrite = MetalKvStorage.commitLayerKvDeviceWrite,
    .repackSelectedGlobalKv = MetalKvStorage.repackSelectedGlobalKv,
    .scoreSelectedAttention = MetalKvStorage.scoreSelectedAttention,
    .truncateSequence = MetalKvStorage.truncateSequenceOp,
    .releaseSequence = MetalKvStorage.releaseSequenceOp,
    .supportsSelectedGlobalKv = MetalKvStorage.supportsSelectedGlobalKv,
    .allocationSnapshot = MetalKvStorage.allocationSnapshotOp,
    .drainForOwnerSnapshot = MetalKvStorage.drainForOwnerSnapshot,
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

test "attention_match Metal exact prefix clone is transactional owner independent and records readback" {
    if (!build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const metal = metal_runtime.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_decode_runtime_destroy(metal);
    if (metal_runtime.termite_metal_decode_runtime_ready(metal) == 0) return error.SkipZigTest;

    const source = try MetalKvStorage.create(allocator, metal, .f16, 1, 2, 4);
    const source_hook = source.deviceWriteHook();
    defer source_hook.deinit(allocator);
    const destination = try MetalKvStorage.create(allocator, metal, .f16, 1, 2, 4);
    const destination_hook = destination.deviceWriteHook();
    defer destination_hook.deinit(allocator);

    const tokens: usize = 3;
    const width: usize = 2;
    const blocks = [_]u32{0};
    const layer_one_keys = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const layer_one_values = [_]f32{ -1, -2, -3, -4, -5, -6 };
    const layer_four_keys = [_]f32{ 7, 8, 9, 10, 11, 12 };
    const layer_four_values = [_]f32{ -7, -8, -9, -10, -11, -12 };
    const bytes = tokens * width * @sizeOf(f32);
    const key_handle = metal_runtime.termite_metal_buffer_alloc(metal, bytes, 0) orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_buffer_release(key_handle);
    const value_handle = metal_runtime.termite_metal_buffer_alloc(metal, bytes, 0) orelse return error.SkipZigTest;
    defer metal_runtime.termite_metal_buffer_release(value_handle);
    const fixtures = [_]struct { layer: u32, keys: []const f32, values: []const f32 }{
        .{ .layer = 1, .keys = &layer_one_keys, .values = &layer_one_values },
        .{ .layer = 4, .keys = &layer_four_keys, .values = &layer_four_values },
    };
    for (fixtures) |fixture| {
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(
            metal,
            key_handle,
            0,
            @ptrCast(fixture.keys.ptr),
            bytes,
        ));
        try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(
            metal,
            value_handle,
            0,
            @ptrCast(fixture.values.ptr),
            bytes,
        ));
        try source_hook.writeLayerKvSuffix(.{
            .sequence_id = 11,
            .layer_index = fixture.layer,
            .total_token_count = tokens,
            .suffix_token_count = tokens,
            .position_offset = 0,
            .num_kv_heads = 1,
            .head_dim = 2,
            .logical_blocks = &blocks,
            .page_size_tokens = 4,
        }, .{ .handle = key_handle, .byte_offset = 0, .byte_len = bytes }, .{
            .handle = value_handle,
            .byte_offset = 0,
            .byte_len = bytes,
        });
    }

    const request: storage_runtime.device_clone.Request = .{
        .source_sequence = 11,
        .destination_sequence = 22,
        .tokens = tokens,
        .capacity = 8,
        .page_size = 4,
        .source_blocks = &blocks,
        .destination_blocks = &blocks,
        .donors = &.{
            .{ .layer = 1, .heads = 1, .dim = 2 },
            .{ .layer = 4, .heads = 1, .dim = 2 },
        },
        .max_bytes = 1024 * 1024,
    };
    var oom_owner = owner_alloc.HostAllocationTracker.init(null, 0);
    const oom_destination = try MetalKvStorage.createTracked(
        oom_owner.allocator(),
        metal,
        .f16,
        1,
        2,
        4,
        &oom_owner,
    );
    const oom_hook = oom_destination.deviceWriteHook();
    defer oom_hook.deinit(allocator);
    try oom_owner.armNextPositiveAllocationFailure();
    try std.testing.expectError(
        error.OutOfMemory,
        oom_hook.cloneExactPrefix(source_hook, request),
    );
    try std.testing.expectEqual(@as(usize, 0), oom_destination.slot_map.count());
    try std.testing.expectEqual(@as(u64, 0), oom_hook.allocationSnapshot().live_bytes);

    var over_budget = request;
    over_budget.max_bytes = 1;
    try std.testing.expectError(
        error.DeviceKvCloneBudgetExceeded,
        destination_hook.cloneExactPrefix(source_hook, over_budget),
    );
    try std.testing.expectEqual(@as(usize, 0), destination.slot_map.count());

    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            // The hook wrapper, preflight, and first donor pass. Cancel before
            // the second donor and require the first staged slot to roll back.
            if (self.calls == 4) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    var cancelled = request;
    cancelled.check_ctx = &cancel;
    cancelled.check_fn = Cancel.check;
    try std.testing.expectError(
        error.CompactionCancelled,
        destination_hook.cloneExactPrefix(source_hook, cancelled),
    );
    try std.testing.expectEqual(@as(usize, 0), destination.slot_map.count());
    try std.testing.expectEqual(@as(u64, 0), destination_hook.allocationSnapshot().live_bytes);

    const result = try destination_hook.cloneExactPrefix(source_hook, request);
    try std.testing.expectEqual(@as(usize, 2), result.donors);
    try std.testing.expectEqual(@as(usize, tokens * width * @sizeOf(f16) * 2 * 2), result.copied_bytes);
    try std.testing.expect(result.allocation_bound_bytes <= request.max_bytes);

    var source_k: [tokens * width]f32 = undefined;
    var source_v: [tokens * width]f32 = undefined;
    var destination_k: [tokens * width]f32 = undefined;
    var destination_v: [tokens * width]f32 = undefined;
    try source_hook.gatherLayerKv(.{
        .sequence_id = 11,
        .layer_index = 1,
        .token_count = tokens,
        .num_kv_heads = 1,
        .head_dim = 2,
    }, &source_k, &source_v);
    try destination_hook.gatherLayerKv(.{
        .sequence_id = 22,
        .layer_index = 1,
        .token_count = tokens,
        .num_kv_heads = 1,
        .head_dim = 2,
    }, &destination_k, &destination_v);
    try std.testing.expectEqualSlices(f32, &source_k, &destination_k);
    try std.testing.expectEqualSlices(f32, &source_v, &destination_v);

    const suffix_keys = [_]f32{ 13, 14 };
    const suffix_values = [_]f32{ -13, -14 };
    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(
        metal,
        key_handle,
        0,
        @ptrCast(&suffix_keys),
        suffix_keys.len * @sizeOf(f32),
    ));
    try std.testing.expectEqual(@as(c_int, 0), metal_runtime.termite_metal_buffer_upload(
        metal,
        value_handle,
        0,
        @ptrCast(&suffix_values),
        suffix_values.len * @sizeOf(f32),
    ));
    try destination_hook.writeLayerKvSuffix(.{
        .sequence_id = 22,
        .layer_index = 1,
        .total_token_count = 4,
        .suffix_token_count = 1,
        .position_offset = 0,
        .num_kv_heads = 1,
        .head_dim = 2,
        .logical_blocks = &blocks,
        .page_size_tokens = 4,
    }, .{ .handle = key_handle, .byte_offset = 0, .byte_len = suffix_keys.len * @sizeOf(f32) }, .{
        .handle = value_handle,
        .byte_offset = 0,
        .byte_len = suffix_values.len * @sizeOf(f32),
    });
    var destination_appended_k: [4 * width]f32 = undefined;
    var destination_appended_v: [4 * width]f32 = undefined;
    try destination_hook.gatherLayerKv(.{
        .sequence_id = 22,
        .layer_index = 1,
        .token_count = 4,
        .num_kv_heads = 1,
        .head_dim = 2,
    }, &destination_appended_k, &destination_appended_v);
    try std.testing.expectEqualSlices(f32, &source_k, destination_appended_k[0 .. tokens * width]);
    try std.testing.expectApproxEqAbs(@as(f32, 13), destination_appended_k[6], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 14), destination_appended_k[7], 1e-3);

    const readback_before = source_hook.allocationSnapshot().readback_bytes;
    var readback_k: [2 * width]f32 = undefined;
    var readback_v: [2 * width]f32 = undefined;
    const reader = try source_hook.attentionMatchReader();
    try reader.read_fn(reader.context, .{
        .sequence = 11,
        .layer = 1,
        .logical_length = tokens,
        .start = 1,
        .count = 2,
        .heads = 1,
        .dim = 2,
    }, &readback_k, &readback_v);
    const readback_after = source_hook.allocationSnapshot().readback_bytes;
    try std.testing.expectEqual(
        @as(u64, 2 * width * @sizeOf(f16) * 2),
        readback_after - readback_before,
    );
    for (readback_k, layer_one_keys[width..]) |actual, expected|
        try std.testing.expectApproxEqAbs(expected, actual, 1e-3);

    destination_hook.releaseSequence(22);
    try std.testing.expectEqual(@as(usize, 0), destination.slot_map.count());
    var source_after_release_k: [tokens * width]f32 = undefined;
    var source_after_release_v: [tokens * width]f32 = undefined;
    try source_hook.gatherLayerKv(.{
        .sequence_id = 11,
        .layer_index = 1,
        .token_count = tokens,
        .num_kv_heads = 1,
        .head_dim = 2,
    }, &source_after_release_k, &source_after_release_v);
    try std.testing.expectEqualSlices(f32, &source_k, &source_after_release_k);
    try std.testing.expectEqualSlices(f32, &source_v, &source_after_release_v);
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
        const binding = try owned.acquireBinding(.{ .sequence_id = 91, .layer_index = 0 }, 0, 0, false, true);
        // This fixture seeds the native slot ABI directly instead of going
        // through reserveLayerKvDevice, so publish the same compact-contiguous
        // layout metadata that the production reserve path would establish.
        binding.logical_contiguous = true;
        binding.physical_base_tokens = 0;
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
    const cached_binding = try cached.acquireBinding(.{ .sequence_id = 1, .layer_index = 0 }, 0, 0, true, false);
    const cached_slot = cached_binding.slot;

    const request_local = try MetalKvStorage.create(allocator, runtime, .f32, 1, 2, 2);
    const local_binding = try request_local.acquireBinding(.{ .sequence_id = 1, .layer_index = 0 }, 0, 0, true, false);
    const local_slot = local_binding.slot;
    try std.testing.expect(cached_slot >= metal_runtime.paged_kv_slot_base);
    try std.testing.expect(local_slot >= metal_runtime.paged_kv_slot_base);
    try std.testing.expect(cached_slot != local_slot);

    // Returning a request-local hook publishes only its own lease. The cache
    // still owns its original slot, while its next layer can reuse the slot
    // that the completed request returned to the shared runtime.
    request_local.deviceWriteHook().deinit(allocator);
    const cached_layer_1 = try cached.acquireBinding(.{ .sequence_id = 1, .layer_index = 1 }, 0, 0, true, false);
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
