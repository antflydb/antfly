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
const turboquant = @import("turboquant.zig");

pub const BackendKind = enum {
    metal,
    native,
    cuda,
};

pub const KvDType = enum {
    f16,
    bf16,
    f32,
    int8,
    fp8,
    int4,
    polar4,
    turbo3,

    /// Returns the simple per-element size for formats without metadata overhead.
    /// For int8 (per-head scale) and int4 (per-group scale), use bytesForTokenRow instead.
    pub fn bytesPerElement(self: KvDType) usize {
        return switch (self) {
            .f32 => 4,
            .f16, .bf16 => 2,
            .int8, .fp8 => 1,
            .int4 => 1, // approximate; use bytesForTokenRow for accurate calculation
            .polar4 => 1, // packed 4-bit keys plus int8-style values; use row helpers
            .turbo3 => 1, // packed 3-bit keys plus int8-style values; use row helpers
        };
    }

    /// Returns the exact byte size needed to store one key row for this dtype.
    pub fn bytesForKeyRow(self: KvDType, num_kv_heads: u32, head_dim: u32) usize {
        const token_values: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
        return switch (self) {
            .f32 => token_values * 4,
            .f16, .bf16 => token_values * 2,
            .fp8 => token_values,
            .int8 => int8BytesForRow(num_kv_heads, head_dim),
            .int4 => int4BytesForRow(token_values),
            .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
            .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim) + turboquant.turbo3ResidualBytes(num_kv_heads, head_dim),
        };
    }

    /// Returns the exact byte size needed to store one value row for this dtype.
    /// TurboQuant-style presets use asymmetric storage: compressed keys plus a
    /// conservative int8-style value codec.
    pub fn bytesForValueRow(self: KvDType, num_kv_heads: u32, head_dim: u32) usize {
        const token_values: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
        return switch (self) {
            .f32 => token_values * 4,
            .f16, .bf16 => token_values * 2,
            .fp8 => token_values,
            .int8 => int8BytesForRow(num_kv_heads, head_dim),
            .int4 => int4BytesForRow(token_values),
            .polar4 => int8BytesForRow(num_kv_heads, head_dim),
            .turbo3 => int8BytesForRow(num_kv_heads, head_dim),
        };
    }

    /// Returns the legacy symmetric row size used by existing estimators. For
    /// asymmetric dtypes this returns the sum of the key and value rows.
    pub fn bytesForTokenRow(self: KvDType, num_kv_heads: u32, head_dim: u32) usize {
        return switch (self) {
            .polar4, .turbo3 => self.bytesForKeyRow(num_kv_heads, head_dim) + self.bytesForValueRow(num_kv_heads, head_dim),
            else => self.bytesForKeyRow(num_kv_heads, head_dim),
        };
    }

    pub fn bytesForTokenPair(self: KvDType, num_kv_heads: u32, head_dim: u32) usize {
        return self.bytesForKeyRow(num_kv_heads, head_dim) + self.bytesForValueRow(num_kv_heads, head_dim);
    }

    pub fn bytesForFlatKeyRow(self: KvDType, values_per_token: usize, num_kv_heads: u32, head_dim: u32) usize {
        const default_width = @as(usize, num_kv_heads) * @as(usize, head_dim);
        if (values_per_token == default_width) return self.bytesForKeyRow(num_kv_heads, head_dim);
        return switch (self) {
            .f32 => values_per_token * 4,
            .f16, .bf16 => values_per_token * 2,
            .fp8 => values_per_token,
            .int4 => int4BytesForRow(values_per_token),
            .int8, .polar4, .turbo3 => 0,
        };
    }

    pub fn bytesForFlatValueRow(self: KvDType, values_per_token: usize, num_kv_heads: u32, head_dim: u32) usize {
        const default_width = @as(usize, num_kv_heads) * @as(usize, head_dim);
        if (values_per_token == default_width) return self.bytesForValueRow(num_kv_heads, head_dim);
        return switch (self) {
            .f32 => values_per_token * 4,
            .f16, .bf16 => values_per_token * 2,
            .fp8 => values_per_token,
            .int4 => int4BytesForRow(values_per_token),
            .int8, .polar4, .turbo3 => 0,
        };
    }
};

const int4_group_size: usize = 32;
const int4_bytes_per_group: usize = 2 + int4_group_size / 2; // f16 scale (2 bytes) + 16 packed bytes

fn int8BytesForRow(num_kv_heads: u32, head_dim: u32) usize {
    return @as(usize, num_kv_heads) * (@as(usize, head_dim) + @sizeOf(f32));
}

fn int4BytesForRow(token_values: usize) usize {
    const num_groups = (token_values + int4_group_size - 1) / int4_group_size;
    return num_groups * int4_bytes_per_group;
}

pub const KvPoolConfig = struct {
    backend: BackendKind,
    dtype: KvDType,
    page_size_tokens: u16,
    num_layers_packed: u16 = 1,
    num_kv_heads: u32,
    head_dim: u32,
    key_values_per_token: ?u32 = null,
    value_values_per_token: ?u32 = null,
    sliding_window_size: ?u32 = null,
    store_cpu_bytes: bool = true,

    pub fn defaultValuesPerToken(self: KvPoolConfig) usize {
        return @as(usize, self.num_kv_heads) * @as(usize, self.head_dim);
    }

    pub fn keyValuesPerToken(self: KvPoolConfig) usize {
        return self.key_values_per_token orelse self.defaultValuesPerToken();
    }

    pub fn valueValuesPerToken(self: KvPoolConfig) usize {
        return self.value_values_per_token orelse self.defaultValuesPerToken();
    }

    pub fn valuesPerToken(self: KvPoolConfig) usize {
        std.debug.assert(self.keyValuesPerToken() == self.valueValuesPerToken());
        return self.keyValuesPerToken();
    }

    pub fn hasSymmetricValueWidth(self: KvPoolConfig) bool {
        return self.keyValuesPerToken() == self.valueValuesPerToken();
    }

    pub fn usesDefaultValueWidths(self: KvPoolConfig) bool {
        const default_width = self.defaultValuesPerToken();
        return self.keyValuesPerToken() == default_width and self.valueValuesPerToken() == default_width;
    }

    pub fn bytesPerKeyTokenRow(self: KvPoolConfig) usize {
        return self.dtype.bytesForFlatKeyRow(self.keyValuesPerToken(), self.num_kv_heads, self.head_dim);
    }

    pub fn bytesPerValueTokenRow(self: KvPoolConfig) usize {
        return self.dtype.bytesForFlatValueRow(self.valueValuesPerToken(), self.num_kv_heads, self.head_dim);
    }

    pub fn bytesPerTokenRow(self: KvPoolConfig) usize {
        return if (self.usesDefaultValueWidths())
            self.dtype.bytesForTokenRow(self.num_kv_heads, self.head_dim)
        else
            self.bytesPerKeyTokenRow();
    }

    pub fn bytesPerTokenPair(self: KvPoolConfig) usize {
        return self.bytesPerKeyTokenRow() + self.bytesPerValueTokenRow();
    }

    pub fn compatible(self: KvPoolConfig, other: KvPoolConfig) bool {
        return self.backend == other.backend and
            self.dtype == other.dtype and
            self.page_size_tokens == other.page_size_tokens and
            self.num_layers_packed == other.num_layers_packed and
            self.num_kv_heads == other.num_kv_heads and
            self.head_dim == other.head_dim and
            self.key_values_per_token == other.key_values_per_token and
            self.value_values_per_token == other.value_values_per_token and
            self.store_cpu_bytes == other.store_cpu_bytes and
            self.sliding_window_size == other.sliding_window_size;
    }
};

pub const KvBlockStorage = struct {
    meta: block.KvBlockMeta,
    k_data: []align(4) u8,
    v_data: []align(4) u8,

    pub fn deinit(self: *KvBlockStorage, allocator: std.mem.Allocator) void {
        allocator.free(self.k_data);
        allocator.free(self.v_data);
    }
};

pub const EncodedTokenRow = struct {
    k_bytes: []const u8,
    v_bytes: []const u8,
};

pub const KvPool = struct {
    pool_id: block.KvPoolId = 0,
    config: KvPoolConfig,
    free_list: std.ArrayListUnmanaged(block.KvBlockId) = .empty,
    blocks: std.ArrayListUnmanaged(KvBlockStorage) = .empty,
    next_block_id: block.KvBlockId = 0,
    /// Soft cap on the total number of blocks the pool may hold. When set, the
    /// scheduler-side step admission consults `availableBlocks` to gate
    /// admission of additional pending work; the pool itself does not refuse
    /// `acquire` calls when the cap is exceeded so that critical paths
    /// (decode of in-flight requests) continue to make progress.
    target_max_blocks: ?usize = null,
    // Scratch buffers for f16 dequantization in readToken.
    read_scratch_k: ?[]f32 = null,
    read_scratch_v: ?[]f32 = null,

    pub fn init(config: KvPoolConfig) KvPool {
        return .{ .config = config };
    }

    /// Configure the soft block cap. Pass `null` to disable.
    pub fn setTargetMaxBlocks(self: *KvPool, target: ?usize) void {
        self.target_max_blocks = target;
    }

    /// Total number of blocks currently held by the pool, including blocks
    /// sitting on the free list awaiting reuse.
    pub fn liveBlocks(self: *const KvPool) usize {
        return self.blocks.items.len;
    }

    /// Number of blocks immediately available without growing the pool — i.e.
    /// blocks already on the free list ready for reacquire.
    pub fn freeListBlocks(self: *const KvPool) usize {
        return self.free_list.items.len;
    }

    /// Headroom under the configured `target_max_blocks` cap, expressed as
    /// the number of additional block acquires that can be served without
    /// pushing total live blocks past the cap. Returns `null` when no cap is
    /// set, meaning admission should treat the pool as unbounded.
    ///
    /// Free-list reuse counts toward headroom because it does not grow the
    /// pool; only fresh allocations expand `liveBlocks`. So the answer is
    /// "free-list size + remaining grow room".
    pub fn availableBlocks(self: *const KvPool) ?usize {
        const cap = self.target_max_blocks orelse return null;
        const live = self.blocks.items.len;
        const free = self.free_list.items.len;
        const grow_room = if (cap > live) cap - live else 0;
        return free + grow_room;
    }

    pub fn deinit(self: *KvPool, allocator: std.mem.Allocator) void {
        if (self.read_scratch_k) |s| allocator.free(s);
        if (self.read_scratch_v) |s| allocator.free(s);
        for (self.blocks.items) |*entry| entry.deinit(allocator);
        self.blocks.deinit(allocator);
        self.free_list.deinit(allocator);
    }

    pub fn acquire(self: *KvPool, allocator: std.mem.Allocator) !block.KvBlockId {
        if (self.free_list.pop()) |id| {
            const entry = self.storageMut(id) orelse return error.InvalidBlockId;
            entry.meta.tokens_written = 0;
            entry.meta.refcount = 1;
            if (self.config.store_cpu_bytes) {
                @memset(entry.k_data, 0);
                @memset(entry.v_data, 0);
            }
            return id;
        }

        const id = self.next_block_id;
        self.next_block_id += 1;

        var k_data: []align(4) u8 = &.{};
        errdefer if (k_data.len != 0) allocator.free(k_data);
        var v_data: []align(4) u8 = &.{};
        errdefer if (v_data.len != 0) allocator.free(v_data);
        if (self.config.store_cpu_bytes) {
            const key_bytes_per_block = self.bytesPerKeyBlock();
            const value_bytes_per_block = self.bytesPerValueBlock();
            if (key_bytes_per_block == 0 or value_bytes_per_block == 0) return error.UnsupportedKvHeadDim;
            k_data = try allocator.alignedAlloc(u8, .@"4", key_bytes_per_block);
            v_data = try allocator.alignedAlloc(u8, .@"4", value_bytes_per_block);
            @memset(k_data, 0);
            @memset(v_data, 0);
        }

        // Lazily allocate scratch buffers for dequantization on first block acquire.
        if (self.config.dtype != .f32 and self.read_scratch_k == null) {
            self.read_scratch_k = try allocator.alloc(f32, self.keyValuesPerToken());
            self.read_scratch_v = try allocator.alloc(f32, self.valueValuesPerToken());
        }

        try self.blocks.append(allocator, .{
            .meta = .{
                .pool_id = self.pool_id,
                .block_id = id,
                .token_capacity = self.config.page_size_tokens,
            },
            .k_data = k_data,
            .v_data = v_data,
        });
        return id;
    }

    pub fn setPoolId(self: *KvPool, pool_id: block.KvPoolId) void {
        self.pool_id = pool_id;
        for (self.blocks.items) |*entry| entry.meta.pool_id = pool_id;
    }

    pub fn release(self: *KvPool, allocator: std.mem.Allocator, id: block.KvBlockId) !void {
        const entry = self.storageMut(id) orelse return error.InvalidBlockId;
        entry.meta.refcount = 0;
        entry.meta.tokens_written = 0;
        try self.free_list.append(allocator, id);
    }

    pub fn retain(self: *KvPool, id: block.KvBlockId) !void {
        const entry = self.storageMut(id) orelse return error.InvalidBlockId;
        entry.meta.refcount += 1;
    }

    pub fn releaseRef(self: *KvPool, allocator: std.mem.Allocator, id: block.KvBlockId) !bool {
        const entry = self.storageMut(id) orelse return error.InvalidBlockId;
        if (entry.meta.refcount == 0) return error.InvalidBlockRefcount;
        entry.meta.refcount -= 1;
        if (entry.meta.refcount > 0) return false;
        entry.meta.tokens_written = 0;
        try self.free_list.append(allocator, id);
        return true;
    }

    pub fn writeToken(self: *KvPool, id: block.KvBlockId, layer_index: usize, token_offset: usize, k_row: []const f32, v_row: []const f32) !void {
        const entry = self.storageMut(id) orelse return error.InvalidBlockId;
        if (layer_index >= self.config.num_layers_packed) return error.InvalidLayerIndex;
        if (token_offset >= self.config.page_size_tokens) return error.InvalidTokenOffset;

        const key_width = self.keyValuesPerToken();
        const value_width = self.valueValuesPerToken();
        if (k_row.len != key_width or v_row.len != value_width) return error.InvalidKvRowWidth;

        if (!self.config.store_cpu_bytes) {
            entry.meta.tokens_written = @max(entry.meta.tokens_written, @as(u16, @intCast(token_offset + 1)));
            return;
        }

        const key_byte_start = self.keyTokenByteOffset(layer_index, token_offset);
        const value_byte_start = self.valueTokenByteOffset(layer_index, token_offset);
        const key_row_bytes = self.bytesPerKeyTokenRow();
        const value_row_bytes = self.bytesPerValueTokenRow();

        switch (self.config.dtype) {
            .f32 => {
                @memcpy(asF32Mut(entry.k_data[key_byte_start..][0..key_row_bytes]), k_row);
                @memcpy(asF32Mut(entry.v_data[value_byte_start..][0..value_row_bytes]), v_row);
            },
            .f16 => {
                quantizeF32ToF16(k_row, entry.k_data[key_byte_start..][0..key_row_bytes]);
                quantizeF32ToF16(v_row, entry.v_data[value_byte_start..][0..value_row_bytes]);
            },
            .fp8 => {
                quantizeF32ToFp8(k_row, entry.k_data[key_byte_start..][0..key_row_bytes]);
                quantizeF32ToFp8(v_row, entry.v_data[value_byte_start..][0..value_row_bytes]);
            },
            .int8 => {
                if (key_width != self.config.defaultValuesPerToken() or value_width != self.config.defaultValuesPerToken()) return error.UnsupportedKvRowWidth;
                quantizeF32ToInt8PerHead(k_row, entry.k_data[key_byte_start..][0..key_row_bytes], self.config.num_kv_heads, self.config.head_dim);
                quantizeF32ToInt8PerHead(v_row, entry.v_data[value_byte_start..][0..value_row_bytes], self.config.num_kv_heads, self.config.head_dim);
            },
            .int4 => {
                quantizeF32ToInt4Group(k_row, entry.k_data[key_byte_start..][0..key_row_bytes]);
                quantizeF32ToInt4Group(v_row, entry.v_data[value_byte_start..][0..value_row_bytes]);
            },
            .polar4 => {
                if (key_width != self.config.defaultValuesPerToken() or value_width != self.config.defaultValuesPerToken()) return error.UnsupportedKvRowWidth;
                try turboquant.encodePolar4Key(k_row, entry.k_data[key_byte_start..][0..key_row_bytes], self.config.num_kv_heads, self.config.head_dim);
                quantizeF32ToInt8PerHead(v_row, entry.v_data[value_byte_start..][0..value_row_bytes], self.config.num_kv_heads, self.config.head_dim);
            },
            .turbo3 => {
                if (key_width != self.config.defaultValuesPerToken() or value_width != self.config.defaultValuesPerToken()) return error.UnsupportedKvRowWidth;
                const key_dst = entry.k_data[key_byte_start..][0..key_row_bytes];
                const base_bytes = turboquant.turbo3KeyBytes(self.config.num_kv_heads, self.config.head_dim);
                const residual_bytes = turboquant.turbo3ResidualBytes(self.config.num_kv_heads, self.config.head_dim);
                try turboquant.encodeTurbo3Key(k_row, key_dst[0..base_bytes], self.config.num_kv_heads, self.config.head_dim);
                try turboquant.encodeTurbo3ResidualSketch(k_row, key_dst[0..base_bytes], key_dst[base_bytes..][0..residual_bytes], self.config.num_kv_heads, self.config.head_dim);
                quantizeF32ToInt8PerHead(v_row, entry.v_data[value_byte_start..][0..value_row_bytes], self.config.num_kv_heads, self.config.head_dim);
            },
            .bf16 => return error.UnsupportedKvDType,
        }
        entry.meta.tokens_written = @max(entry.meta.tokens_written, @as(u16, @intCast(token_offset + 1)));
    }

    pub fn readToken(self: *KvPool, id: block.KvBlockId, layer_index: usize, token_offset: usize) !struct { k: []const f32, v: []const f32 } {
        const entry = self.storage(id) orelse return error.InvalidBlockId;
        if (layer_index >= self.config.num_layers_packed) return error.InvalidLayerIndex;
        if (token_offset >= self.config.page_size_tokens) return error.InvalidTokenOffset;
        if (!self.config.store_cpu_bytes) return error.KvBytesUnavailable;

        const key_byte_start = self.keyTokenByteOffset(layer_index, token_offset);
        const value_byte_start = self.valueTokenByteOffset(layer_index, token_offset);
        const key_row_bytes = self.bytesPerKeyTokenRow();
        const value_row_bytes = self.bytesPerValueTokenRow();

        switch (self.config.dtype) {
            .f32 => {
                return .{
                    .k = asF32Const(entry.k_data[key_byte_start..][0..key_row_bytes]),
                    .v = asF32Const(entry.v_data[value_byte_start..][0..value_row_bytes]),
                };
            },
            .f16 => {
                const scratch_k = self.read_scratch_k orelse return error.ScratchNotAllocated;
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeF16ToF32(entry.k_data[key_byte_start..][0..key_row_bytes], scratch_k);
                dequantizeF16ToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v);
                return .{ .k = scratch_k, .v = scratch_v };
            },
            .fp8 => {
                const scratch_k = self.read_scratch_k orelse return error.ScratchNotAllocated;
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeFp8ToF32(entry.k_data[key_byte_start..][0..key_row_bytes], scratch_k);
                dequantizeFp8ToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v);
                return .{ .k = scratch_k, .v = scratch_v };
            },
            .int8 => {
                const scratch_k = self.read_scratch_k orelse return error.ScratchNotAllocated;
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeInt8PerHeadToF32(entry.k_data[key_byte_start..][0..key_row_bytes], scratch_k, self.config.num_kv_heads, self.config.head_dim);
                dequantizeInt8PerHeadToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v, self.config.num_kv_heads, self.config.head_dim);
                return .{ .k = scratch_k, .v = scratch_v };
            },
            .int4 => {
                const scratch_k = self.read_scratch_k orelse return error.ScratchNotAllocated;
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeInt4GroupToF32(entry.k_data[key_byte_start..][0..key_row_bytes], scratch_k);
                dequantizeInt4GroupToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v);
                return .{ .k = scratch_k, .v = scratch_v };
            },
            .polar4 => {
                const scratch_k = self.read_scratch_k orelse return error.ScratchNotAllocated;
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                try turboquant.decodePolar4Key(entry.k_data[key_byte_start..][0..key_row_bytes], scratch_k, self.config.num_kv_heads, self.config.head_dim);
                dequantizeInt8PerHeadToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v, self.config.num_kv_heads, self.config.head_dim);
                return .{ .k = scratch_k, .v = scratch_v };
            },
            .turbo3 => {
                const scratch_k = self.read_scratch_k orelse return error.ScratchNotAllocated;
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                const base_bytes = turboquant.turbo3KeyBytes(self.config.num_kv_heads, self.config.head_dim);
                try turboquant.decodeTurbo3Key(entry.k_data[key_byte_start..][0..base_bytes], scratch_k, self.config.num_kv_heads, self.config.head_dim);
                dequantizeInt8PerHeadToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v, self.config.num_kv_heads, self.config.head_dim);
                return .{ .k = scratch_k, .v = scratch_v };
            },
            .bf16 => return error.UnsupportedKvDType,
        }
    }

    pub fn readEncodedToken(self: *const KvPool, id: block.KvBlockId, layer_index: usize, token_offset: usize) !EncodedTokenRow {
        const entry = self.storage(id) orelse return error.InvalidBlockId;
        if (layer_index >= self.config.num_layers_packed) return error.InvalidLayerIndex;
        if (token_offset >= self.config.page_size_tokens) return error.InvalidTokenOffset;
        if (!self.config.store_cpu_bytes) return error.KvBytesUnavailable;

        const key_byte_start = self.keyTokenByteOffset(layer_index, token_offset);
        const value_byte_start = self.valueTokenByteOffset(layer_index, token_offset);
        const key_row_bytes = self.bytesPerKeyTokenRow();
        const value_row_bytes = self.bytesPerValueTokenRow();
        return .{
            .k_bytes = entry.k_data[key_byte_start..][0..key_row_bytes],
            .v_bytes = entry.v_data[value_byte_start..][0..value_row_bytes],
        };
    }

    pub fn readValueToken(self: *KvPool, id: block.KvBlockId, layer_index: usize, token_offset: usize) ![]const f32 {
        const entry = self.storage(id) orelse return error.InvalidBlockId;
        if (layer_index >= self.config.num_layers_packed) return error.InvalidLayerIndex;
        if (token_offset >= self.config.page_size_tokens) return error.InvalidTokenOffset;
        if (!self.config.store_cpu_bytes) return error.KvBytesUnavailable;

        const value_byte_start = self.valueTokenByteOffset(layer_index, token_offset);
        const value_row_bytes = self.bytesPerValueTokenRow();

        switch (self.config.dtype) {
            .f32 => return asF32Const(entry.v_data[value_byte_start..][0..value_row_bytes]),
            .f16 => {
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeF16ToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v);
                return scratch_v;
            },
            .fp8 => {
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeFp8ToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v);
                return scratch_v;
            },
            .int8, .polar4, .turbo3 => {
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeInt8PerHeadToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v, self.config.num_kv_heads, self.config.head_dim);
                return scratch_v;
            },
            .int4 => {
                const scratch_v = self.read_scratch_v orelse return error.ScratchNotAllocated;
                dequantizeInt4GroupToF32(entry.v_data[value_byte_start..][0..value_row_bytes], scratch_v);
                return scratch_v;
            },
            .bf16 => return error.UnsupportedKvDType,
        }
    }

    pub fn storage(self: *const KvPool, id: block.KvBlockId) ?*const KvBlockStorage {
        if (id >= self.blocks.items.len) return null;
        return &self.blocks.items[id];
    }

    pub fn storageMut(self: *KvPool, id: block.KvBlockId) ?*KvBlockStorage {
        if (id >= self.blocks.items.len) return null;
        return &self.blocks.items[id];
    }

    pub fn valuesPerToken(self: *const KvPool) usize {
        return self.config.valuesPerToken();
    }

    pub fn keyValuesPerToken(self: *const KvPool) usize {
        return self.config.keyValuesPerToken();
    }

    pub fn valueValuesPerToken(self: *const KvPool) usize {
        return self.config.valueValuesPerToken();
    }

    pub fn bytesPerTokenRow(self: *const KvPool) usize {
        return self.config.bytesPerTokenRow();
    }

    pub fn bytesPerKeyTokenRow(self: *const KvPool) usize {
        return self.config.bytesPerKeyTokenRow();
    }

    pub fn bytesPerValueTokenRow(self: *const KvPool) usize {
        return self.config.bytesPerValueTokenRow();
    }

    pub fn bytesPerKeyBlock(self: *const KvPool) usize {
        return @as(usize, self.config.num_layers_packed) * @as(usize, self.config.page_size_tokens) * self.bytesPerKeyTokenRow();
    }

    pub fn bytesPerValueBlock(self: *const KvPool) usize {
        return @as(usize, self.config.num_layers_packed) * @as(usize, self.config.page_size_tokens) * self.bytesPerValueTokenRow();
    }

    pub fn bytesPerBlock(self: *const KvPool) usize {
        return self.bytesPerKeyBlock() + self.bytesPerValueBlock();
    }

    fn keyTokenByteOffset(self: *const KvPool, layer_index: usize, token_offset: usize) usize {
        const row_bytes = self.bytesPerKeyTokenRow();
        return (layer_index * @as(usize, self.config.page_size_tokens) + token_offset) * row_bytes;
    }

    fn valueTokenByteOffset(self: *const KvPool, layer_index: usize, token_offset: usize) usize {
        const row_bytes = self.bytesPerValueTokenRow();
        return (layer_index * @as(usize, self.config.page_size_tokens) + token_offset) * row_bytes;
    }
};

fn asF32Mut(bytes: []u8) []f32 {
    return @as([*]f32, @ptrCast(@alignCast(bytes.ptr)))[0 .. bytes.len / 4];
}

fn asF32Const(bytes: []const u8) []const f32 {
    return @as([*]const f32, @ptrCast(@alignCast(bytes.ptr)))[0 .. bytes.len / 4];
}

fn quantizeF32ToF16(src: []const f32, dst: []u8) void {
    const count = src.len;
    std.debug.assert(dst.len == count * 2);
    const dst_f16: [*]f16 = @ptrCast(@alignCast(dst.ptr));
    for (0..count) |i| {
        dst_f16[i] = @floatCast(src[i]);
    }
}

fn dequantizeF16ToF32(src: []const u8, dst: []f32) void {
    const count = dst.len;
    std.debug.assert(src.len == count * 2);
    const src_f16: [*]const f16 = @ptrCast(@alignCast(src.ptr));
    for (0..count) |i| {
        dst[i] = @floatCast(src_f16[i]);
    }
}

// --- fp8 E4M3 conversion ---
// E4M3: 1 sign bit, 4 exponent bits (bias 7), 3 mantissa bits.
// Max normal value: 448.0, min subnormal: 2^-9 ≈ 0.001953125. No infinity; NaN = 0x7F/0xFF.

fn f32ToFp8E4M3(val: f32) u8 {
    const bits: u32 = @bitCast(val);
    const sign: u8 = @intCast((bits >> 31) & 1);
    const f32_exp: i32 = @as(i32, @intCast((bits >> 23) & 0xFF)) - 127;
    const f32_mant: u32 = bits & 0x7FFFFF;

    // Handle zero.
    if ((bits & 0x7FFFFFFF) == 0) return @as(u8, sign) << 7;

    // Handle NaN/Inf -> clamp to max.
    if (f32_exp == 128) return (sign << 7) | 0x7E; // max finite E4M3 = ±448

    // Clamp to E4M3 range: max exponent 8 (bias 7 -> stored 15), max mantissa 0b111.
    // Max value = 2^8 * 1.875 = 448.
    const e4m3_exp = f32_exp + 7; // apply E4M3 bias

    if (e4m3_exp > 15) return (sign << 7) | 0x7E; // clamp to max finite

    if (e4m3_exp > 0) {
        // Normal number: round mantissa from 23 bits to 3 bits.
        const shifted = f32_mant + (1 << 19); // round to nearest, ties to even
        var mant3: u8 = @intCast((shifted >> 20) & 0x7);
        var exp4: u8 = @intCast(@as(u32, @intCast(e4m3_exp)));
        // Handle mantissa overflow from rounding.
        if (shifted >= (1 << 23)) {
            mant3 = 0;
            exp4 += 1;
            if (exp4 > 15) return (sign << 7) | 0x7E;
        }
        return (sign << 7) | (exp4 << 3) | mant3;
    }

    if (e4m3_exp >= -2) {
        // Subnormal in E4M3: exponent stored as 0, mantissa includes implicit bit.
        const shift: u5 = @intCast(1 - e4m3_exp); // 1..3
        const subnorm_mant = (f32_mant | 0x800000) >> (20 + @as(u5, shift));
        return (sign << 7) | @as(u8, @intCast(subnorm_mant & 0x7));
    }

    // Too small for E4M3 subnormal -> zero.
    return @as(u8, sign) << 7;
}

fn fp8E4M3ToF32(bits: u8) f32 {
    const sign: u32 = @as(u32, (bits >> 7) & 1);
    const exp4: u32 = @as(u32, (bits >> 3) & 0xF);
    const mant3: u32 = @as(u32, bits & 0x7);

    // NaN (0x7F or 0xFF in E4M3).
    if (exp4 == 0xF and mant3 == 0x7) return @bitCast(@as(u32, 0x7FC00000));

    if (exp4 == 0) {
        if (mant3 == 0) return @bitCast(sign << 31); // signed zero
        // Subnormal: value = (-1)^s * 2^(-6) * (0.mant3)
        const mant_f: f32 = @as(f32, @floatFromInt(mant3)) / 8.0;
        const val: f32 = mant_f * (1.0 / 64.0); // 2^-6
        return if (sign != 0) -val else val;
    }

    // Normal: value = (-1)^s * 2^(exp-7) * (1 + mant/8)
    const f32_exp: u32 = @intCast(@as(i32, @intCast(exp4)) - 7 + 127); // convert E4M3 bias to f32 bias
    const f32_mant: u32 = mant3 << 20; // 3 mantissa bits -> 23 bit field
    return @bitCast((sign << 31) | (f32_exp << 23) | f32_mant);
}

fn quantizeF32ToFp8(src: []const f32, dst: []u8) void {
    std.debug.assert(dst.len == src.len);
    for (0..src.len) |i| {
        dst[i] = f32ToFp8E4M3(src[i]);
    }
}

// Comptime lookup table for fp8 E4M3 -> f32 conversion (256 entries, 1 KB).
// A table lookup is faster than per-element bit manipulation and eliminates branching.
const fp8_to_f32_table: [256]f32 = blk: {
    @setEvalBranchQuota(4096);
    var table: [256]f32 = undefined;
    for (0..256) |i| {
        table[i] = fp8E4M3ToF32(@intCast(i));
    }
    break :blk table;
};

fn dequantizeFp8ToF32(src: []const u8, dst: []f32) void {
    std.debug.assert(src.len == dst.len);
    for (0..dst.len) |i| {
        dst[i] = fp8_to_f32_table[src[i]];
    }
}

// --- int8 per-head symmetric quantization ---

fn quantizeF32ToInt8PerHead(src: []const f32, dst: []u8, num_kv_heads: u32, head_dim: u32) void {
    const hd: usize = head_dim;
    var dst_off: usize = 0;
    for (0..num_kv_heads) |h| {
        const head_start = h * hd;
        const head_values = src[head_start..][0..hd];

        // SIMD max_abs reduction.
        var max_abs_vec: @Vector(8, f32) = @splat(0);
        var i: usize = 0;
        while (i + 8 <= hd) : (i += 8) {
            var vals: @Vector(8, f32) = undefined;
            inline for (0..8) |j| vals[j] = head_values[i + j];
            max_abs_vec = @max(max_abs_vec, @abs(vals));
        }
        var max_abs: f32 = @reduce(.Max, max_abs_vec);
        while (i < hd) : (i += 1) max_abs = @max(max_abs, @abs(head_values[i]));
        const scale: f32 = if (max_abs == 0) 1.0 else max_abs / 127.0;

        // Store f32 scale.
        const scale_bytes = std.mem.asBytes(&scale);
        @memcpy(dst[dst_off..][0..4], scale_bytes);
        dst_off += 4;

        // SIMD quantize.
        const inv_scale_vec: @Vector(8, f32) = @splat(1.0 / scale);
        const min_vec: @Vector(8, f32) = @splat(-127.0);
        const max_vec: @Vector(8, f32) = @splat(127.0);
        i = 0;
        while (i + 8 <= hd) : (i += 8) {
            var vals: @Vector(8, f32) = undefined;
            inline for (0..8) |j| vals[j] = head_values[i + j];
            const scaled = @round(vals * inv_scale_vec);
            const clamped = @min(@max(scaled, min_vec), max_vec);
            inline for (0..8) |j| {
                const q: i8 = @intFromFloat(clamped[j]);
                dst[dst_off + i + j] = @bitCast(q);
            }
        }
        // Scalar remainder.
        const inv_scale = 1.0 / scale;
        while (i < hd) : (i += 1) {
            const q = @as(i8, @intFromFloat(std.math.clamp(@round(head_values[i] * inv_scale), -127.0, 127.0)));
            dst[dst_off + i] = @bitCast(q);
        }
        dst_off += hd;
    }
}

pub fn dequantizeInt8PerHeadToF32(src: []const u8, dst: []f32, num_kv_heads: u32, head_dim: u32) void {
    const hd: usize = head_dim;
    var src_off: usize = 0;
    var dst_off: usize = 0;
    for (0..num_kv_heads) |_| {
        // Read f32 scale.
        const scale: f32 = @bitCast(src[src_off..][0..4].*);
        src_off += 4;

        // Dequantize values with SIMD where possible.
        const scale_vec: @Vector(8, f32) = @splat(scale);
        var i: usize = 0;
        while (i + 8 <= hd) : (i += 8) {
            var int_vec: @Vector(8, f32) = undefined;
            inline for (0..8) |j| {
                const q: i8 = @bitCast(src[src_off + i + j]);
                int_vec[j] = @floatFromInt(q);
            }
            const result = int_vec * scale_vec;
            inline for (0..8) |j| {
                dst[dst_off + i + j] = result[j];
            }
        }
        // Scalar remainder.
        while (i < hd) : (i += 1) {
            const q: i8 = @bitCast(src[src_off + i]);
            dst[dst_off + i] = @as(f32, @floatFromInt(q)) * scale;
        }
        src_off += hd;
        dst_off += hd;
    }
}

// --- int4 group-32 symmetric quantization ---

fn quantizeF32ToInt4Group(src: []const f32, dst: []u8) void {
    var src_off: usize = 0;
    var dst_off: usize = 0;
    const total = src.len;

    while (src_off < total) {
        const remaining = @min(int4_group_size, total - src_off);
        const group = src[src_off..][0..remaining];

        // SIMD max_abs reduction.
        var max_abs_vec: @Vector(8, f32) = @splat(0);
        var mi: usize = 0;
        while (mi + 8 <= remaining) : (mi += 8) {
            var vals: @Vector(8, f32) = undefined;
            inline for (0..8) |j| vals[j] = group[mi + j];
            max_abs_vec = @max(max_abs_vec, @abs(vals));
        }
        var max_abs: f32 = @reduce(.Max, max_abs_vec);
        while (mi < remaining) : (mi += 1) max_abs = @max(max_abs, @abs(group[mi]));

        const scale: f16 = @floatCast(if (max_abs == 0) @as(f32, 1.0) else max_abs / 7.0);

        // Store f16 scale (2 bytes).
        const scale_bytes = std.mem.asBytes(&scale);
        @memcpy(dst[dst_off..][0..2], scale_bytes);
        dst_off += 2;

        // Quantize and pack pairs of 4-bit values into bytes (low nibble first).
        const inv_scale: f32 = 1.0 / @as(f32, scale);
        var i: usize = 0;
        while (i < int4_group_size) : (i += 2) {
            const lo_f: f32 = if (i < remaining) std.math.clamp(@round(group[i] * inv_scale), -7.0, 7.0) else 0;
            const hi_f: f32 = if (i + 1 < remaining) std.math.clamp(@round(group[i + 1] * inv_scale), -7.0, 7.0) else 0;
            const lo_i: u8 = @bitCast(@as(i8, @intFromFloat(lo_f)));
            const hi_i: u8 = @bitCast(@as(i8, @intFromFloat(hi_f)));
            dst[dst_off] = (lo_i & 0x0F) | ((hi_i & 0x0F) << 4);
            dst_off += 1;
        }
        src_off += remaining;
    }
}

fn dequantizeInt4GroupToF32(src: []const u8, dst: []f32) void {
    var src_off: usize = 0;
    var dst_off: usize = 0;
    const total = dst.len;

    while (dst_off < total) {
        const remaining = @min(int4_group_size, total - dst_off);

        // Read f16 scale.
        const scale: f32 = @floatCast(@as(f16, @bitCast(src[src_off..][0..2].*)));
        src_off += 2;

        // SIMD unpack: process 8 values (4 packed bytes) at a time.
        const scale_vec: @Vector(8, f32) = @splat(scale);
        var i: usize = 0;
        while (i + 8 <= remaining) : (i += 8) {
            // Unpack 4 bytes -> 8 signed 4-bit values -> 8 f32.
            var int_vec: @Vector(8, f32) = undefined;
            inline for (0..4) |j| {
                const b = src[src_off + j];
                const lo_u4: u8 = b & 0x0F;
                const hi_u4: u8 = (b >> 4) & 0x0F;
                const lo_i: i8 = if (lo_u4 & 0x08 != 0) @bitCast(lo_u4 | 0xF0) else @bitCast(lo_u4);
                const hi_i: i8 = if (hi_u4 & 0x08 != 0) @bitCast(hi_u4 | 0xF0) else @bitCast(hi_u4);
                int_vec[j * 2] = @floatFromInt(lo_i);
                int_vec[j * 2 + 1] = @floatFromInt(hi_i);
            }
            const result = int_vec * scale_vec;
            inline for (0..8) |j| {
                dst[dst_off + i + j] = result[j];
            }
            src_off += 4;
        }
        // Scalar remainder.
        while (i < int4_group_size) : (i += 2) {
            const b = src[src_off];
            src_off += 1;

            const lo_u4: u8 = b & 0x0F;
            const lo_i: i8 = if (lo_u4 & 0x08 != 0) @bitCast(lo_u4 | 0xF0) else @bitCast(lo_u4);
            if (i < remaining) {
                dst[dst_off + i] = @as(f32, @floatFromInt(lo_i)) * scale;
            }

            const hi_u4: u8 = (b >> 4) & 0x0F;
            const hi_i: i8 = if (hi_u4 & 0x08 != 0) @bitCast(hi_u4 | 0xF0) else @bitCast(hi_u4);
            if (i + 1 < remaining) {
                dst[dst_off + i + 1] = @as(f32, @floatFromInt(hi_i)) * scale;
            }
        }
        dst_off += remaining;
    }
}

/// Parse a KV dtype name from a user-provided string.
pub fn parseKvDType(name: []const u8) ?KvDType {
    const entries = .{
        .{ "f16", KvDType.f16 },
        .{ "f32", KvDType.f32 },
        .{ "bf16", KvDType.bf16 },
        .{ "int8", KvDType.int8 },
        .{ "fp8", KvDType.fp8 },
        .{ "int4", KvDType.int4 },
        .{ "polar4", KvDType.polar4 },
        .{ "turbo3", KvDType.turbo3 },
    };
    inline for (entries) |entry| {
        if (std.mem.eql(u8, name, entry[0])) return entry[1];
    }
    return null;
}

test "pool reuses released blocks" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .metal,
        .dtype = .f16,
        .page_size_tokens = 16,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    defer pool.deinit(allocator);

    const first = try pool.acquire(allocator);
    const second = try pool.acquire(allocator);
    try std.testing.expectEqual(@as(block.KvBlockId, 0), first);
    try std.testing.expectEqual(@as(block.KvBlockId, 1), second);

    try pool.release(allocator, first);
    const third = try pool.acquire(allocator);
    try std.testing.expectEqual(first, third);
}

test "pool stores kv rows per layer and token" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    try pool.writeToken(block_id, 1, 2, &.{ 1, 2, 3, 4 }, &.{ 5, 6, 7, 8 });
    const row = try pool.readToken(block_id, 1, 2);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, row.k);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8 }, row.v);
}

test "pool f16 round-trip" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    try pool.writeToken(block_id, 0, 0, &.{ 1.0, 2.0, 3.0, 4.0 }, &.{ 5.0, 6.0, 7.0, 8.0 });
    const row = try pool.readToken(block_id, 0, 0);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), row.k[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), row.k[3], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), row.v[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 8.0), row.v[3], 0.01);
}

test "pool retain and releaseRef tracks block ownership" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .metal,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    try pool.retain(block_id);
    try std.testing.expectEqual(@as(u32, 2), pool.storage(block_id).?.meta.refcount);

    try std.testing.expectEqual(false, try pool.releaseRef(allocator, block_id));
    try std.testing.expectEqual(@as(u32, 1), pool.storage(block_id).?.meta.refcount);
    try std.testing.expectEqual(true, try pool.releaseRef(allocator, block_id));
    try std.testing.expectEqual(@as(u32, 0), pool.storage(block_id).?.meta.refcount);
}

test "pool fp8 round-trip" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .fp8,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    try pool.writeToken(block_id, 0, 0, &.{ 1.0, 2.0, 3.0, 4.0 }, &.{ 5.0, 6.0, 7.0, 8.0 });
    const row = try pool.readToken(block_id, 0, 0);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), row.k[0], 0.125);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), row.k[3], 0.25);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), row.v[0], 0.5);
    try std.testing.expectApproxEqAbs(@as(f32, 8.0), row.v[3], 0.5);
}

test "pool int8 round-trip" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .int8,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 2,
        .head_dim = 4,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    // Two heads of 4 values each.
    try pool.writeToken(block_id, 0, 0, &.{ 1.0, -1.0, 0.5, -0.5, 0.1, 0.2, 0.3, 0.4 }, &.{ 5.0, 6.0, 7.0, 8.0, -1.0, -2.0, -3.0, -4.0 });
    const row = try pool.readToken(block_id, 0, 0);
    // int8 symmetric: error should be within ~max_abs/127 per head.
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), row.k[0], 0.02);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), row.k[1], 0.02);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), row.k[2], 0.02);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), row.k[4], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), row.v[0], 0.08);
    try std.testing.expectApproxEqAbs(@as(f32, -4.0), row.v[7], 0.08);
}

test "pool int4 round-trip" {
    const allocator = std.testing.allocator;
    // Use head_dim=16 so one head = half a group of 32.
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .int4,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 2,
        .head_dim = 16,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    var k_row: [32]f32 = undefined;
    var v_row: [32]f32 = undefined;
    for (0..32) |i| {
        k_row[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i)))) * 0.1 - 1.5;
        v_row[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i)))) * -0.05 + 0.5;
    }
    try pool.writeToken(block_id, 0, 0, &k_row, &v_row);
    const row = try pool.readToken(block_id, 0, 0);
    // int4 symmetric: error should be within ~max_abs/7 per group.
    try std.testing.expectApproxEqAbs(k_row[0], row.k[0], 0.25);
    try std.testing.expectApproxEqAbs(k_row[15], row.k[15], 0.25);
    try std.testing.expectApproxEqAbs(v_row[0], row.v[0], 0.15);
    try std.testing.expectApproxEqAbs(v_row[31], row.v[31], 0.15);
}

test "pool polar4 uses asymmetric key and value row sizes" {
    // 8 * 128 = 1024 key values -> 512 packed polar4 bytes.
    // Values use int8-style per-head quantization -> 1024 bytes + 8 f32 scales.
    try std.testing.expectEqual(@as(usize, 512), KvDType.polar4.bytesForKeyRow(8, 128));
    try std.testing.expectEqual(@as(usize, 1024 + 8 * 4), KvDType.polar4.bytesForValueRow(8, 128));
    try std.testing.expectEqual(@as(usize, 512 + 1024 + 8 * 4), KvDType.polar4.bytesForTokenRow(8, 128));
    try std.testing.expectEqual(@as(usize, 512 + 1024 + 8 * 4), KvDType.polar4.bytesForTokenPair(8, 128));
}

test "pool turbo3 uses asymmetric key and value row sizes" {
    // 8 * 128 = 1024 key values -> 384 packed turbo3 bytes.
    // Residual sketch adds 32 bytes: 32 one-bit projections for each KV head.
    // Values use int8-style per-head quantization -> 1024 bytes + 8 f32 scales.
    try std.testing.expectEqual(@as(usize, 384 + 32), KvDType.turbo3.bytesForKeyRow(8, 128));
    try std.testing.expectEqual(@as(usize, 1024 + 8 * 4), KvDType.turbo3.bytesForValueRow(8, 128));
    try std.testing.expectEqual(@as(usize, 384 + 32 + 1024 + 8 * 4), KvDType.turbo3.bytesForTokenRow(8, 128));
    try std.testing.expectEqual(@as(usize, 384 + 32 + 1024 + 8 * 4), KvDType.turbo3.bytesForTokenPair(8, 128));
}

test "pool polar4 round-trip uses polar keys and int8 values" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .polar4,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 64,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    var k_row: [64]f32 = undefined;
    var v_row: [64]f32 = undefined;
    for (0..64) |i| {
        k_row[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) / 8.0;
        v_row[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i % 13)) - 6)) / 3.0;
    }

    try pool.writeToken(block_id, 0, 0, &k_row, &v_row);
    const row = try pool.readToken(block_id, 0, 0);

    for (k_row, row.k) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.08);
    }
    for (v_row, row.v) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.02);
    }
}

test "pool turbo3 round-trip uses packed 3-bit keys and int8 values" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .turbo3,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 64,
    });
    defer pool.deinit(allocator);

    const block_id = try pool.acquire(allocator);
    var k_row: [64]f32 = undefined;
    var v_row: [64]f32 = undefined;
    for (0..64) |i| {
        k_row[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) / 8.0;
        v_row[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i % 13)) - 6)) / 3.0;
    }

    try pool.writeToken(block_id, 0, 0, &k_row, &v_row);
    const row = try pool.readToken(block_id, 0, 0);
    const encoded = try pool.readEncodedToken(block_id, 0, 0);
    const base_bytes = turboquant.turbo3KeyBytes(1, 64);
    const residual_bytes = turboquant.turbo3ResidualBytes(1, 64);
    try std.testing.expectEqual(base_bytes + residual_bytes, encoded.k_bytes.len);
    try std.testing.expectEqual(@as(usize, 4), residual_bytes);

    for (k_row, row.k) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.15);
    }
    for (v_row, row.v) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.02);
    }
}

test "pool polar4 accepts supported head dimensions" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .polar4,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 80,
    });
    defer pool.deinit(allocator);

    const page = try pool.acquire(allocator);
    try pool.release(allocator, page);
}

test "pool turbo3 accepts supported head dimensions" {
    const allocator = std.testing.allocator;
    var pool = KvPool.init(.{
        .backend = .native,
        .dtype = .turbo3,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 80,
    });
    defer pool.deinit(allocator);

    const page = try pool.acquire(allocator);
    try pool.release(allocator, page);
}

test "fp8 E4M3 special values" {
    // Zero.
    try std.testing.expectEqual(@as(f32, 0.0), fp8E4M3ToF32(f32ToFp8E4M3(0.0)));
    // Negative zero.
    try std.testing.expectEqual(@as(f32, 0.0), @abs(fp8E4M3ToF32(f32ToFp8E4M3(-0.0))));
    // ±1.0 should round-trip exactly (representable in E4M3).
    try std.testing.expectEqual(@as(f32, 1.0), fp8E4M3ToF32(f32ToFp8E4M3(1.0)));
    try std.testing.expectEqual(@as(f32, -1.0), fp8E4M3ToF32(f32ToFp8E4M3(-1.0)));
    // Max value clamps to 448.
    try std.testing.expectApproxEqAbs(@as(f32, 448.0), fp8E4M3ToF32(f32ToFp8E4M3(448.0)), 1.0);
    try std.testing.expectApproxEqAbs(@as(f32, 448.0), fp8E4M3ToF32(f32ToFp8E4M3(1000.0)), 1.0);
}

test "int8 per-head scale isolation" {
    // head_dim=4, num_kv_heads=2 -> bytesForTokenRow = 2 * (4 + 4) = 16
    var dst16: [16]u8 = undefined;
    // Head 0: values around 100, head 1: values around 0.01.
    quantizeF32ToInt8PerHead(&.{ 100, 50, -100, 75, 0.01, -0.005, 0.008, -0.01 }, &dst16, 2, 4);

    // Dequantize and check both heads retain precision.
    var out: [8]f32 = undefined;
    dequantizeInt8PerHeadToF32(&dst16, &out, 2, 4);
    try std.testing.expectApproxEqAbs(@as(f32, 100.0), out[0], 1.0);
    try std.testing.expectApproxEqAbs(@as(f32, -100.0), out[2], 1.0);
    // Small head should also be well-quantized with its own scale.
    try std.testing.expectApproxEqAbs(@as(f32, 0.01), out[4], 0.0002);
    try std.testing.expectApproxEqAbs(@as(f32, -0.01), out[7], 0.0002);
}

test "int4 group boundary with partial group" {
    // 20 values = 1 group of 32, but only 20 are real.
    // bytesForTokenRow with num_kv_heads=1, head_dim=20:
    // groups = ceil(20/32) = 1, bytes = 1 * 18 = 18
    var dst: [18]u8 = undefined;
    var src: [20]f32 = undefined;
    for (0..20) |i| src[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i)))) - 10.0;

    quantizeF32ToInt4Group(&src, &dst);
    var out: [20]f32 = undefined;
    dequantizeInt4GroupToF32(&dst, &out);

    // Check first and last values.
    try std.testing.expectApproxEqAbs(@as(f32, -10.0), out[0], 2.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[10], 2.0);
    try std.testing.expectApproxEqAbs(@as(f32, 9.0), out[19], 2.0);
}

test "parseKvDType returns correct types" {
    try std.testing.expectEqual(KvDType.f16, parseKvDType("f16").?);
    try std.testing.expectEqual(KvDType.f32, parseKvDType("f32").?);
    try std.testing.expectEqual(KvDType.int8, parseKvDType("int8").?);
    try std.testing.expectEqual(KvDType.fp8, parseKvDType("fp8").?);
    try std.testing.expectEqual(KvDType.int4, parseKvDType("int4").?);
    try std.testing.expectEqual(KvDType.polar4, parseKvDType("polar4").?);
    try std.testing.expectEqual(KvDType.turbo3, parseKvDType("turbo3").?);
    try std.testing.expect(parseKvDType("invalid") == null);
}

test "bytesForTokenRow matches expected sizes" {
    // 8 heads, 128 dim = 1024 values.
    try std.testing.expectEqual(@as(usize, 1024 * 4), KvDType.f32.bytesForTokenRow(8, 128));
    try std.testing.expectEqual(@as(usize, 1024 * 2), KvDType.f16.bytesForTokenRow(8, 128));
    try std.testing.expectEqual(@as(usize, 1024), KvDType.fp8.bytesForTokenRow(8, 128));
    // int8: 1024 values + 8 heads * 4 bytes scale = 1056.
    try std.testing.expectEqual(@as(usize, 1024 + 8 * 4), KvDType.int8.bytesForTokenRow(8, 128));
    // int4: 1024/32 = 32 groups, 32 * 18 = 576.
    try std.testing.expectEqual(@as(usize, 32 * 18), KvDType.int4.bytesForTokenRow(8, 128));
    // polar4 is asymmetric: 4-bit packed keys plus int8-style values.
    try std.testing.expectEqual(@as(usize, 512 + 1024 + 8 * 4), KvDType.polar4.bytesForTokenRow(8, 128));
    // turbo3 is asymmetric: 3-bit packed keys plus residual sketch and int8-style values.
    try std.testing.expectEqual(@as(usize, 384 + 32 + 1024 + 8 * 4), KvDType.turbo3.bytesForTokenRow(8, 128));
}
