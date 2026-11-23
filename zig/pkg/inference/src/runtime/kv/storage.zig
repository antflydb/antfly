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
const pool_mod = @import("pool.zig");

pub const KvRowPair = struct {
    k: []const f32,
    v: []const f32,
};

/// Public projection of a block's metadata for consumers that don't need
/// access to the backing storage bytes. Host impls derive this from
/// `KvBlockMeta`; device impls synthesize it from per-slot state.
pub const BlockInfo = struct {
    pool_id: block.KvPoolId,
    block_id: block.KvBlockId,
    token_capacity: u16,
    tokens_written: u16,
    refcount: u32,
    residency: block.KvResidency,
};

/// Storage-agnostic handle to KV cache bytes. The config is a copy so callers
/// can read geometry (`page_size_tokens`, `num_kv_heads`, dtype, …) without
/// dispatching through the vtable on the hot path. Impl-specific mutable state
/// lives behind the opaque `ptr`.
pub const KvStorage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    config: pool_mod.KvPoolConfig,
    pool_id: block.KvPoolId = 0,

    pub const VTable = struct {
        deinit: *const fn (*anyopaque, std.mem.Allocator) void,
        setPoolId: *const fn (*anyopaque, block.KvPoolId) void,

        acquire: *const fn (*anyopaque, std.mem.Allocator) anyerror!block.KvBlockId,
        release: *const fn (*anyopaque, std.mem.Allocator, block.KvBlockId) anyerror!void,
        retain: *const fn (*anyopaque, block.KvBlockId) anyerror!void,
        releaseRef: *const fn (*anyopaque, std.mem.Allocator, block.KvBlockId) anyerror!bool,

        writeToken: *const fn (*anyopaque, block.KvBlockId, usize, usize, []const f32, []const f32) anyerror!void,
        readToken: *const fn (*anyopaque, block.KvBlockId, usize, usize) anyerror!KvRowPair,
        readValueToken: *const fn (*anyopaque, block.KvBlockId, usize, usize) anyerror![]const f32,
        readEncodedToken: *const fn (*anyopaque, block.KvBlockId, usize, usize) anyerror!pool_mod.EncodedTokenRow,

        blockInfo: *const fn (*anyopaque, block.KvBlockId) ?BlockInfo,

        /// Number of blocks currently held by the storage, including any on a
        /// free list awaiting reuse. Used by step admission to size budgets.
        liveBlocks: *const fn (*const anyopaque) usize,

        /// Headroom under the configured soft cap, or `null` when unbounded.
        /// See `KvPool.availableBlocks` for semantics.
        availableBlocks: *const fn (*const anyopaque) ?usize,

        /// Configure the soft block cap on the underlying storage. A null
        /// argument disables the cap.
        setTargetMaxBlocks: *const fn (*anyopaque, ?usize) void,

        /// Escape hatch for consumers that still require the concrete host pool
        /// (paged cache rebuild, tests that poke at refcounts). Device
        /// impls return null — callers then pick an alternate code path.
        hostPool: *const fn (*anyopaque) ?*pool_mod.KvPool,
    };

    pub fn deinit(self: *KvStorage, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }

    pub fn setPoolId(self: *KvStorage, id: block.KvPoolId) void {
        self.pool_id = id;
        self.vtable.setPoolId(self.ptr, id);
    }

    /// Exact host encoded copies are available only for the complete canonical
    /// host provider. Copied tables do not silently inherit this contract.
    /// Infallible scalar publication after a checked complete payload copy.
    /// Canonical metadata ownership keeps these IDs valid through the caller's
    /// exclusive transaction; this grants no extra physical/refcount capacity.
    pub fn copyCanonicalBlockPrefixMetadata(self: *KvStorage, source_id: block.KvBlockId, target_id: block.KvBlockId, tokens: usize) void {
        std.debug.assert(self.hasCanonicalHostBlockOwnership() and source_id != target_id and tokens <= self.config.page_size_tokens);
        const pool = hostPtr(self.ptr);
        const source = pool.storage(source_id).?;
        const target = pool.storageMut(target_id).?;
        std.debug.assert(source.meta.refcount != 0 and target.meta.refcount == 1);
        target.meta.tokens_written = @intCast(@min(tokens, source.meta.tokens_written));
        target.meta.model_tag = source.meta.model_tag;
        target.meta.priority = source.meta.priority;
        target.meta.prefix_cacheable = false;
    }

    pub fn hasCanonicalHostBlockOwnership(self: *const KvStorage) bool {
        return self.vtable == &host_owned_vtable or self.vtable == &host_borrowed_vtable;
    }

    pub fn supportsExactHostBlockCopy(self: *const KvStorage) bool {
        return self.config.store_cpu_bytes and self.hasCanonicalHostBlockOwnership();
    }

    pub fn copyExactHostBlockPrefix(self: *KvStorage, source_id: block.KvBlockId, target_id: block.KvBlockId, tokens: usize) !void {
        if (!self.supportsExactHostBlockCopy()) return error.SharedKvWriteUnsupported;
        if (source_id == target_id or tokens > self.config.page_size_tokens) return error.InvalidKvShape;
        const pool = hostPtr(self.ptr);
        const source = pool.storage(source_id) orelse return error.InvalidBlockId;
        const target = pool.storageMut(target_id) orelse return error.InvalidBlockId;
        if (source.meta.refcount == 0 or target.meta.refcount != 1) return error.InvalidBlockRefcount;
        const key_row = pool.bytesPerKeyTokenRow();
        const value_row = pool.bytesPerValueTokenRow();
        const key_layer = try std.math.mul(usize, pool.config.page_size_tokens, key_row);
        const value_layer = try std.math.mul(usize, pool.config.page_size_tokens, value_row);
        const key_bytes = try std.math.mul(usize, tokens, key_row);
        const value_bytes = try std.math.mul(usize, tokens, value_row);
        // Validate every byte span before changing an existing reserved page.
        if (source.k_data.len != try std.math.mul(usize, self.config.num_layers_packed, key_layer) or target.k_data.len != source.k_data.len or source.v_data.len != try std.math.mul(usize, self.config.num_layers_packed, value_layer) or target.v_data.len != source.v_data.len) return error.InvalidKvShape;
        for (0..self.config.num_layers_packed) |layer| {
            @memcpy(target.k_data[layer * key_layer ..][0..key_bytes], source.k_data[layer * key_layer ..][0..key_bytes]);
            @memcpy(target.v_data[layer * value_layer ..][0..value_bytes], source.v_data[layer * value_layer ..][0..value_bytes]);
        }
        self.copyCanonicalBlockPrefixMetadata(source_id, target_id, tokens);
    }

    pub fn acquire(self: *KvStorage, allocator: std.mem.Allocator) !block.KvBlockId {
        return self.vtable.acquire(self.ptr, allocator);
    }

    pub fn release(self: *KvStorage, allocator: std.mem.Allocator, id: block.KvBlockId) !void {
        return self.vtable.release(self.ptr, allocator, id);
    }

    pub fn retain(self: *KvStorage, id: block.KvBlockId) !void {
        return self.vtable.retain(self.ptr, id);
    }

    pub fn releaseRef(self: *KvStorage, allocator: std.mem.Allocator, id: block.KvBlockId) !bool {
        return self.vtable.releaseRef(self.ptr, allocator, id);
    }

    pub fn writeToken(
        self: *KvStorage,
        id: block.KvBlockId,
        layer_index: usize,
        token_offset: usize,
        k_row: []const f32,
        v_row: []const f32,
    ) !void {
        return self.vtable.writeToken(self.ptr, id, layer_index, token_offset, k_row, v_row);
    }

    pub fn readToken(
        self: *KvStorage,
        id: block.KvBlockId,
        layer_index: usize,
        token_offset: usize,
    ) !KvRowPair {
        return self.vtable.readToken(self.ptr, id, layer_index, token_offset);
    }

    pub fn readValueToken(
        self: *KvStorage,
        id: block.KvBlockId,
        layer_index: usize,
        token_offset: usize,
    ) ![]const f32 {
        return self.vtable.readValueToken(self.ptr, id, layer_index, token_offset);
    }

    pub fn readEncodedToken(
        self: *KvStorage,
        id: block.KvBlockId,
        layer_index: usize,
        token_offset: usize,
    ) !pool_mod.EncodedTokenRow {
        return self.vtable.readEncodedToken(self.ptr, id, layer_index, token_offset);
    }

    pub fn blockInfo(self: *const KvStorage, id: block.KvBlockId) ?BlockInfo {
        return self.vtable.blockInfo(self.ptr, id);
    }

    pub fn hostPool(self: *KvStorage) ?*pool_mod.KvPool {
        return self.vtable.hostPool(self.ptr);
    }

    pub fn liveBlocks(self: *const KvStorage) usize {
        return self.vtable.liveBlocks(self.ptr);
    }

    pub fn availableBlocks(self: *const KvStorage) ?usize {
        return self.vtable.availableBlocks(self.ptr);
    }

    pub fn setTargetMaxBlocks(self: *KvStorage, target: ?usize) void {
        self.vtable.setTargetMaxBlocks(self.ptr, target);
    }

    pub fn valuesPerToken(self: *const KvStorage) usize {
        return self.config.valuesPerToken();
    }

    pub fn keyValuesPerToken(self: *const KvStorage) usize {
        return self.config.keyValuesPerToken();
    }

    pub fn valueValuesPerToken(self: *const KvStorage) usize {
        return self.config.valueValuesPerToken();
    }

    pub fn bytesPerKeyTokenRow(self: *const KvStorage) usize {
        return self.config.bytesPerKeyTokenRow();
    }

    pub fn bytesPerValueTokenRow(self: *const KvStorage) usize {
        return self.config.bytesPerValueTokenRow();
    }

    pub fn bytesPerKeyBlock(self: *const KvStorage) usize {
        return @as(usize, self.config.num_layers_packed) *
            @as(usize, self.config.page_size_tokens) *
            self.bytesPerKeyTokenRow();
    }

    pub fn bytesPerValueBlock(self: *const KvStorage) usize {
        return @as(usize, self.config.num_layers_packed) *
            @as(usize, self.config.page_size_tokens) *
            self.bytesPerValueTokenRow();
    }

    pub fn bytesPerBlock(self: *const KvStorage) usize {
        return self.bytesPerKeyBlock() + self.bytesPerValueBlock();
    }
};

/// Create a host-backed KvStorage. The returned storage heap-allocates a KvPool
/// and owns it — `deinit` frees both the blocks and the pool wrapper.
pub fn initHost(allocator: std.mem.Allocator, config: pool_mod.KvPoolConfig) !KvStorage {
    const owned = try allocator.create(pool_mod.KvPool);
    errdefer allocator.destroy(owned);
    owned.* = pool_mod.KvPool.init(config);
    return .{
        .ptr = owned,
        .vtable = &host_owned_vtable,
        .config = config,
        .pool_id = owned.pool_id,
    };
}

/// Wrap a caller-owned `KvPool` in a `KvStorage` handle without taking
/// ownership — `deinit` is a no-op. Used by `KvManager` callers that want a
/// uniform `KvStorage` surface without migrating the multi-pool manager.
pub fn wrapHostPool(pool: *pool_mod.KvPool) KvStorage {
    return .{
        .ptr = pool,
        .vtable = &host_borrowed_vtable,
        .config = pool.config,
        .pool_id = pool.pool_id,
    };
}

// --- Host vtable bindings ---

fn hostPtr(ptr: *anyopaque) *pool_mod.KvPool {
    return @ptrCast(@alignCast(ptr));
}

fn hostOwnedDeinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
    const pool = hostPtr(ptr);
    pool.deinit(allocator);
    allocator.destroy(pool);
}

fn hostBorrowedDeinit(_: *anyopaque, _: std.mem.Allocator) void {}

fn hostSetPoolId(ptr: *anyopaque, id: block.KvPoolId) void {
    hostPtr(ptr).setPoolId(id);
}

fn hostAcquire(ptr: *anyopaque, allocator: std.mem.Allocator) !block.KvBlockId {
    return hostPtr(ptr).acquire(allocator);
}

fn hostRelease(ptr: *anyopaque, allocator: std.mem.Allocator, id: block.KvBlockId) !void {
    return hostPtr(ptr).release(allocator, id);
}

fn hostRetain(ptr: *anyopaque, id: block.KvBlockId) !void {
    return hostPtr(ptr).retain(id);
}

fn hostReleaseRef(ptr: *anyopaque, allocator: std.mem.Allocator, id: block.KvBlockId) !bool {
    return hostPtr(ptr).releaseRef(allocator, id);
}

fn hostWriteToken(
    ptr: *anyopaque,
    id: block.KvBlockId,
    layer_index: usize,
    token_offset: usize,
    k_row: []const f32,
    v_row: []const f32,
) !void {
    return hostPtr(ptr).writeToken(id, layer_index, token_offset, k_row, v_row);
}

fn hostReadToken(
    ptr: *anyopaque,
    id: block.KvBlockId,
    layer_index: usize,
    token_offset: usize,
) !KvRowPair {
    const row = try hostPtr(ptr).readToken(id, layer_index, token_offset);
    return .{ .k = row.k, .v = row.v };
}

fn hostReadValueToken(
    ptr: *anyopaque,
    id: block.KvBlockId,
    layer_index: usize,
    token_offset: usize,
) ![]const f32 {
    return hostPtr(ptr).readValueToken(id, layer_index, token_offset);
}

fn hostReadEncodedToken(
    ptr: *anyopaque,
    id: block.KvBlockId,
    layer_index: usize,
    token_offset: usize,
) !pool_mod.EncodedTokenRow {
    return hostPtr(ptr).readEncodedToken(id, layer_index, token_offset);
}

fn hostBlockInfo(ptr: *anyopaque, id: block.KvBlockId) ?BlockInfo {
    const pool = hostPtr(ptr);
    const entry = pool.storage(id) orelse return null;
    return .{
        .pool_id = entry.meta.pool_id,
        .block_id = entry.meta.block_id,
        .token_capacity = entry.meta.token_capacity,
        .tokens_written = entry.meta.tokens_written,
        .refcount = entry.meta.refcount,
        .residency = entry.meta.residency,
    };
}

fn hostHostPool(ptr: *anyopaque) ?*pool_mod.KvPool {
    return hostPtr(ptr);
}

fn hostLiveBlocks(ptr: *const anyopaque) usize {
    const pool: *const pool_mod.KvPool = @ptrCast(@alignCast(ptr));
    return pool.liveBlocks();
}

fn hostAvailableBlocks(ptr: *const anyopaque) ?usize {
    const pool: *const pool_mod.KvPool = @ptrCast(@alignCast(ptr));
    return pool.availableBlocks();
}

fn hostSetTargetMaxBlocks(ptr: *anyopaque, target: ?usize) void {
    hostPtr(ptr).setTargetMaxBlocks(target);
}

const host_owned_vtable: KvStorage.VTable = .{
    .deinit = hostOwnedDeinit,
    .setPoolId = hostSetPoolId,
    .acquire = hostAcquire,
    .release = hostRelease,
    .retain = hostRetain,
    .releaseRef = hostReleaseRef,
    .writeToken = hostWriteToken,
    .readToken = hostReadToken,
    .readValueToken = hostReadValueToken,
    .readEncodedToken = hostReadEncodedToken,
    .blockInfo = hostBlockInfo,
    .liveBlocks = hostLiveBlocks,
    .availableBlocks = hostAvailableBlocks,
    .setTargetMaxBlocks = hostSetTargetMaxBlocks,
    .hostPool = hostHostPool,
};

const host_borrowed_vtable: KvStorage.VTable = .{
    .deinit = hostBorrowedDeinit,
    .setPoolId = hostSetPoolId,
    .acquire = hostAcquire,
    .release = hostRelease,
    .retain = hostRetain,
    .releaseRef = hostReleaseRef,
    .writeToken = hostWriteToken,
    .readToken = hostReadToken,
    .readValueToken = hostReadValueToken,
    .readEncodedToken = hostReadEncodedToken,
    .blockInfo = hostBlockInfo,
    .liveBlocks = hostLiveBlocks,
    .availableBlocks = hostAvailableBlocks,
    .setTargetMaxBlocks = hostSetTargetMaxBlocks,
    .hostPool = hostHostPool,
};

test "host storage acquire/write/read round-trip" {
    const allocator = std.testing.allocator;
    var storage = try initHost(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer storage.deinit(allocator);

    const block_id = try storage.acquire(allocator);
    try storage.writeToken(block_id, 1, 2, &.{ 1, 2, 3, 4 }, &.{ 5, 6, 7, 8 });
    const row = try storage.readToken(block_id, 1, 2);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, row.k);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8 }, row.v);
}

test "host storage blockInfo exposes refcount and tokens" {
    const allocator = std.testing.allocator;
    var storage = try initHost(allocator, .{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer storage.deinit(allocator);

    const block_id = try storage.acquire(allocator);
    try storage.retain(block_id);
    const info = storage.blockInfo(block_id).?;
    try std.testing.expectEqual(@as(u32, 2), info.refcount);
    try std.testing.expectEqual(@as(u16, 0), info.tokens_written);
    try std.testing.expectEqual(block_id, info.block_id);
}

test "host storage geometry matches config" {
    const allocator = std.testing.allocator;
    var storage = try initHost(allocator, .{
        .backend = .native,
        .dtype = .polar4,
        .page_size_tokens = 8,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    defer storage.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 8 * 128), storage.valuesPerToken());
    try std.testing.expectEqual(@as(usize, 512), storage.bytesPerKeyTokenRow());
    try std.testing.expectEqual(@as(usize, 1024 + 8 * 4), storage.bytesPerValueTokenRow());
}

test "host storage hostPool escape hatch returns concrete pool" {
    const allocator = std.testing.allocator;
    var storage = try initHost(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });
    defer storage.deinit(allocator);

    const pool = storage.hostPool().?;
    try std.testing.expectEqual(storage.config.dtype, pool.config.dtype);
    try std.testing.expectEqual(storage.config.head_dim, pool.config.head_dim);
}

test "canonical host KV block copy preserves exact encoded prefixes for every layer and dtype" {
    const a = std.testing.allocator;
    for ([_]pool_mod.KvDType{ .f32, .f16, .bf16, .fp8, .int8, .int4 }) |dtype| {
        var storage = try initHost(a, .{ .backend = .native, .dtype = dtype, .page_size_tokens = 2, .num_layers_packed = 2, .num_kv_heads = 1, .head_dim = 64 });
        defer storage.deinit(a);
        const source_id = try storage.acquire(a);
        const target_id = try storage.acquire(a);
        const pool = storage.hostPool().?;
        const source = pool.storageMut(source_id).?;
        for (source.k_data, 0..) |*byte, index| byte.* = @truncate(index *% 17 +% 3);
        for (source.v_data, 0..) |*byte, index| byte.* = @truncate(index *% 31 +% 7);
        source.meta.tokens_written = 2;
        try storage.copyExactHostBlockPrefix(source_id, target_id, 1);
        const target = pool.storage(target_id).?;
        const key_row = storage.bytesPerKeyTokenRow();
        const value_row = storage.bytesPerValueTokenRow();
        for (0..2) |layer| {
            try std.testing.expectEqualSlices(u8, source.k_data[layer * 2 * key_row ..][0..key_row], target.k_data[layer * 2 * key_row ..][0..key_row]);
            try std.testing.expectEqualSlices(u8, source.v_data[layer * 2 * value_row ..][0..value_row], target.v_data[layer * 2 * value_row ..][0..value_row]);
            for (target.k_data[(layer * 2 + 1) * key_row ..][0..key_row]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
            for (target.v_data[(layer * 2 + 1) * value_row ..][0..value_row]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
        }
        try std.testing.expectEqual(@as(u16, 1), target.meta.tokens_written);
        try std.testing.expectError(error.InvalidKvShape, storage.copyExactHostBlockPrefix(source_id, source_id, 1));
        const original_table = storage.vtable;
        var copied_table = original_table.*;
        storage.vtable = &copied_table;
        try std.testing.expect(!storage.supportsExactHostBlockCopy());
        try std.testing.expectError(error.SharedKvWriteUnsupported, storage.copyExactHostBlockPrefix(source_id, target_id, 1));
        storage.vtable = original_table;
    }
}
