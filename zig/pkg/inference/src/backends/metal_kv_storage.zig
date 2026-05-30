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
//! - Multi-sequence via a (sequence_id, layer_index) → slot map. Slots are
//!   reclaimed on `releaseSequence`. Capacity-exhaustion returns
//!   `error.DeviceWriteFallback` so callers downgrade gracefully. Kernel
//!   encode failures also return `DeviceWriteFallback`; the hook contract is
//!   best-effort, not a fatal runtime boundary.
//! - Caller is responsible for keeping the host KvPool in sync if any path
//!   still reads from it. In the full Phase 6 rollout `ensurePagedKvSuffixWritten`
//!   calls the device path first and skips the host write when the hook
//!   accepts; gather-driven paths download from the slot buffer instead.

const std = @import("std");
const build_options = @import("build_options");
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

pub const MetalKvStorage = struct {
    allocator: std.mem.Allocator,
    runtime: *metal_runtime.RawMetalDecodeRuntime,
    format: KeyFormat,
    num_kv_heads: u32,
    head_dim: u32,
    page_size_tokens: u16,
    /// (seq, layer) → slot. Inserted on first write for the pair; removed and
    /// the slot returned to `free_slots` when the sequence is released.
    slot_map: std.AutoHashMapUnmanaged(SlotKey, usize) = .empty,
    /// Slot indices reclaimed from released sequences, consumed LIFO before
    /// bumping `next_slot`. Reclaimed slots have their backing MTLBuffers
    /// preserved for capacity reuse but their slot metadata reset so the C
    /// kernel's incremental-append check starts clean for the next tenant.
    free_slots: std.ArrayListUnmanaged(usize) = .empty,
    /// Highest slot index handed out so far + 1. Grows until it hits the
    /// runtime's attention_span_slot_capacity; after that only `free_slots`
    /// entries can be acquired and exhaustion signals fallback.
    next_slot: usize = 0,
    /// True when logical KV rows for this slot map to a single contiguous
    /// physical token range. Raw f32 device gathers can then expose a borrowed
    /// view at `slot_physical_base_tokens[slot]`; non-contiguous tables must
    /// use the paged attention operator or a gathered-span fallback.
    slot_logical_contiguous: [metal_runtime.attention_span_slot_capacity]bool = @as([metal_runtime.attention_span_slot_capacity]bool, @splat(false)),
    slot_physical_base_tokens: [metal_runtime.attention_span_slot_capacity]usize = @as([metal_runtime.attention_span_slot_capacity]usize, @splat(0)),

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

    /// Acquire a slot for the given (sequence, layer) pair. Reuses the slot
    /// already bound to this pair if one exists, otherwise pulls from the
    /// free list, otherwise bumps `next_slot` up to the capacity ceiling.
    /// Returns `error.DeviceWriteFallback` when exhausted so the caller can
    /// downgrade to the host path for this write.
    fn acquireSlot(self: *MetalKvStorage, key: SlotKey) !usize {
        if (self.slot_map.get(key)) |slot| return slot;
        const slot: usize = if (self.free_slots.pop()) |reused| blk: {
            _ = metal_runtime.termite_metal_decode_runtime_reset_attention_span_slot(self.runtime, reused);
            break :blk reused;
        } else if (self.next_slot < metal_runtime.attention_span_slot_capacity) blk: {
            const s = self.next_slot;
            self.next_slot += 1;
            break :blk s;
        } else {
            return error.DeviceWriteFallback;
        };
        try self.slot_map.put(self.allocator, key, slot);
        return slot;
    }

    /// Release every slot bound to `sequence_id` back to the free pool and
    /// reset their slot metadata so the next tenant re-encodes from scratch.
    /// Safe to call for a sequence that holds no slots.
    fn releaseSequenceSlots(self: *MetalKvStorage, sequence_id: storage_runtime.SequenceId) void {
        var it = self.slot_map.iterator();
        var to_release: std.ArrayListUnmanaged(SlotKey) = .empty;
        defer to_release.deinit(self.allocator);
        while (it.next()) |entry| {
            if (entry.key_ptr.sequence_id != sequence_id) continue;
            to_release.append(self.allocator, entry.key_ptr.*) catch {
                // On allocation failure, reset the slot in place — we won't
                // be able to reuse it, but we also won't double-bind it.
                _ = metal_runtime.termite_metal_decode_runtime_reset_attention_span_slot(self.runtime, entry.value_ptr.*);
                continue;
            };
        }
        for (to_release.items) |key| {
            if (self.slot_map.fetchRemove(key)) |removed| {
                _ = metal_runtime.termite_metal_decode_runtime_reset_attention_span_slot(self.runtime, removed.value);
                self.free_slots.append(self.allocator, removed.value) catch {
                    // If we can't record the reuse slot, the slot leaks for
                    // this session — it still gets cleared on reset_state.
                };
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

        const slot = try self.acquireSlot(.{
            .sequence_id = write.sequence_id,
            .layer_index = @intCast(write.layer_index),
        });

        const rc = if (write.logical_blocks) |logical_blocks| paged: {
            if (logical_blocks.len == 0 or write.page_size_tokens == 0) break :paged -9999;
            const needed_blocks = std.math.divCeil(usize, write.total_token_count, write.page_size_tokens) catch break :paged -9999;
            if (logical_blocks.len < needed_blocks) break :paged -9999;
            const block_offsets = self.allocator.alloc(u32, needed_blocks) catch break :paged -9999;
            defer self.allocator.free(block_offsets);
            for (block_offsets, logical_blocks[0..needed_blocks]) |*offset, block_id| {
                const token_offset = std.math.mul(usize, @as(usize, block_id), write.page_size_tokens) catch break :paged -9999;
                if (token_offset > std.math.maxInt(u32)) break :paged -9999;
                offset.* = @intCast(token_offset);
            }
            self.slot_physical_base_tokens[slot] = block_offsets[0];
            self.slot_logical_contiguous[slot] = true;
            for (block_offsets, 0..) |offset, block_idx| {
                const expected = self.slot_physical_base_tokens[slot] + block_idx * @as(usize, write.page_size_tokens);
                if (offset != expected) {
                    self.slot_logical_contiguous[slot] = false;
                    break;
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
            self.slot_logical_contiguous[slot] = true;
            self.slot_physical_base_tokens[slot] = 0;
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
    }

    fn reserveLayerKvDevice(
        ctx: *anyopaque,
        reserve: storage_runtime.DeviceKvLayerReserve,
    ) anyerror!void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        if (reserve.token_capacity == 0) return;
        const layout = self.rowLayout(reserve.num_kv_heads, reserve.head_dim);
        if (layout.key_row_bytes == 0 or layout.v_row_stride == 0) return error.InvalidKvShape;

        const slot = try self.acquireSlot(.{
            .sequence_id = reserve.sequence_id,
            .layer_index = @intCast(reserve.layer_index),
        });

        const token_capacity = if (reserve.logical_blocks) |logical_blocks| blk: {
            if (logical_blocks.len == 0 or reserve.page_size_tokens == 0) break :blk reserve.token_capacity;
            const needed_blocks = std.math.divCeil(usize, reserve.token_capacity, reserve.page_size_tokens) catch break :blk reserve.token_capacity;
            if (logical_blocks.len < needed_blocks) break :blk reserve.token_capacity;
            var capacity_tokens: usize = 0;
            self.slot_physical_base_tokens[slot] = @as(usize, logical_blocks[0]) * reserve.page_size_tokens;
            self.slot_logical_contiguous[slot] = true;
            for (logical_blocks[0..needed_blocks], 0..) |block_id, block_idx| {
                const block_start = @as(usize, block_id) * reserve.page_size_tokens;
                const expected = self.slot_physical_base_tokens[slot] + block_idx * @as(usize, reserve.page_size_tokens);
                if (block_start != expected) self.slot_logical_contiguous[slot] = false;
                const block_end = block_start + reserve.page_size_tokens;
                if (block_end > capacity_tokens) capacity_tokens = block_end;
            }
            break :blk capacity_tokens;
        } else blk: {
            self.slot_logical_contiguous[slot] = true;
            self.slot_physical_base_tokens[slot] = 0;
            break :blk reserve.token_capacity;
        };

        const rc = metal_runtime.termite_metal_decode_runtime_reserve_attention_span_slot_buffers(
            self.runtime,
            slot,
            @intFromEnum(self.format),
            token_capacity,
            layout.key_row_bytes,
            layout.v_row_stride,
        );
        if (rc != 0) return error.DeviceWriteFallback;
    }

    fn hookDeinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        self.slot_map.deinit(allocator);
        self.free_slots.deinit(allocator);
        allocator.destroy(self);
    }

    fn releaseSequenceOp(ctx: *anyopaque, sequence_id: storage_runtime.SequenceId) void {
        const self: *MetalKvStorage = @ptrCast(@alignCast(ctx));
        self.releaseSequenceSlots(sequence_id);
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

        const slot = self.slot_map.get(.{
            .sequence_id = gather.sequence_id,
            .layer_index = @intCast(gather.layer_index),
        }) orelse return error.DeviceReadFallback;
        if (!self.slot_logical_contiguous[slot]) return error.DeviceReadFallback;
        const info = try self.slotInfo(slot);
        if (info.tokens != 0 and info.tokens < gather.token_count) return error.DeviceReadFallback;

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
        const physical_base = self.slot_physical_base_tokens[slot];
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

        const slot = self.slot_map.get(.{
            .sequence_id = gather.sequence_id,
            .layer_index = @intCast(gather.layer_index),
        }) orelse return error.DeviceReadFallback;
        if (!self.slot_logical_contiguous[slot]) return error.DeviceReadFallback;
        const info = try self.slotInfo(slot);
        if (info.tokens < gather.token_count) return error.DeviceReadFallback;
        if (info.key_row_bytes != token_width * @sizeOf(f32)) return error.DeviceReadFallback;
        if (info.v_row_stride != token_width) return error.DeviceReadFallback;

        const k_handle = info.encoded_key_handle orelse return error.DeviceReadFallback;
        const v_handle = info.v_handle orelse return error.DeviceReadFallback;
        const byte_offset = self.slot_physical_base_tokens[slot] * token_width * @sizeOf(f32);
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
        const slot = self.slot_map.get(key) orelse blk: {
            if (!active_frame) return error.DeviceReadFallback;
            break :blk try self.acquireSlot(key);
        };
        const info_opt = self.slotInfo(slot) catch |err| blk: {
            if (!active_frame) return err;
            break :blk null;
        };
        const position_offset = if (info_opt) |info| blk: {
            if (!active_frame and info.tokens < gather.token_count) return error.DeviceReadFallback;
            if (info.tokens != 0 and info.tokens < gather.token_count and !active_frame) return error.DeviceReadFallback;
            if (info.key_row_bytes != 0 and info.key_row_bytes != key_row_bytes) return error.DeviceReadFallback;
            if (info.v_row_stride != 0 and info.v_row_stride != token_width) return error.DeviceReadFallback;
            break :blk info.position_offset;
        } else 0;
        return .{
            .runtime = @ptrCast(self.runtime),
            .slot = slot,
            .format = @intFromEnum(self.format),
            .token_count = gather.token_count,
            .key_row_bytes = key_row_bytes,
            .base_key_row_bytes = base_key_row_bytes,
            .v_row_stride = token_width,
            .page_size_tokens = self.page_size_tokens,
            .position_offset = position_offset,
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

const hook_vtable: storage_runtime.DeviceWriteHook.VTable = .{
    .writeLayerKvSuffix = MetalKvStorage.writeLayerKvSuffix,
    .gatherLayerKv = MetalKvStorage.gatherLayerKv,
    .gatherLayerKvDevice = MetalKvStorage.gatherLayerKvDevice,
    .pagedLayerKvDevice = MetalKvStorage.pagedLayerKvDevice,
    .reserveLayerKvDevice = MetalKvStorage.reserveLayerKvDevice,
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
