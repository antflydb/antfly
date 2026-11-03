// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared-page writes preserve immutable cache/source readers. Logical tables
//! publish replacements only after exact encoded copies and ref retirement.
const std = @import("std");
const block = @import("block.zig");
const tables = @import("block_table.zig");
const storage_mod = @import("storage.zig");

pub fn appendAcquired(storage: *storage_mod.KvStorage, table: *tables.SequenceBlockTable, allocator: std.mem.Allocator) !block.KvBlockId {
    // Reserve logical publication metadata first. Once a provider returns an
    // owned block, publication is infallible and needs no provider rollback.
    try table.blocks.ensureUnusedCapacity(allocator, 1);
    const id = try storage.acquire(allocator);
    table.appendAssumeCapacity(id);
    return id;
}

/// Number of extra physical pages required before extending a partial shared
/// tail. The refcount includes cache-held references on the original sequence.
pub fn tailCopies(storage: *const storage_mod.KvStorage, table: *const tables.SequenceBlockTable) ?usize {
    const tail = table.last() orelse return 0;
    if (table.tail_tokens >= storage.config.page_size_tokens) return 0;
    const info = storage.blockInfo(tail) orelse return null;
    if (info.refcount == 0) return null;
    return @intFromBool(info.refcount > 1);
}

pub const CopyProvider = @import("block_copy.zig").Provider;
const BlockCopy = @import("block_copy.zig");

pub fn ensureRange(storage: *storage_mod.KvStorage, table: *tables.SequenceBlockTable, reserved: *std.ArrayListUnmanaged(block.KvBlockId), allocator: std.mem.Allocator, first_token: usize, token_count: usize, host_copy_allowed: bool) !void {
    return ensureRangeWithProvider(storage, table, reserved, allocator, first_token, token_count, 0, host_copy_allowed, null);
}

/// Plan every replacement before copying or publishing any page. Canonical
/// metadata ownership proves infallible ref retirement after publication.
pub fn ensureRangeWithProvider(storage: *storage_mod.KvStorage, table: *tables.SequenceBlockTable, reserved: *std.ArrayListUnmanaged(block.KvBlockId), allocator: std.mem.Allocator, first_token: usize, token_count: usize, sequence: u32, host_copy_allowed: bool, provider: ?CopyProvider) !void {
    if (token_count == 0 or table.blocks.items.len == 0) return;
    const page: usize = storage.config.page_size_tokens;
    if (page == 0) return error.InvalidKvShape;
    const end = try std.math.add(usize, first_token, token_count);
    const begin_block = @min(first_token / page, table.blocks.items.len);
    const end_block = @min(try std.math.divCeil(usize, end, page), table.blocks.items.len);
    var count: usize = 0;
    for (table.blocks.items[begin_block..end_block]) |id| {
        const info = storage.blockInfo(id) orelse return error.InvalidBlockId;
        if (info.refcount == 0) return error.InvalidBlockRefcount;
        if (info.refcount > 1) count += 1;
    }
    if (count == 0) return; // No allocation, provider query, or fence on ordinary writes.
    if (!storage.hasCanonicalHostBlockOwnership()) return error.SharedKvWriteUnsupported;
    if (provider == null and (!host_copy_allowed or !storage.supportsExactHostBlockCopy())) return error.SharedKvWriteUnsupported;
    const conservative_end = std.math.cast(u32, table.blocks.items.len) orelse return error.InvalidKvShape;
    var inline_items: [8]BlockCopy.Item = undefined;
    const spill = if (count > inline_items.len) try allocator.alloc(BlockCopy.Item, count) else null;
    defer if (spill) |items| allocator.free(items);
    const items = if (spill) |allocated| allocated else inline_items[0..count];
    const reserved_used = @min(reserved.items.len, count);
    var prepared: usize = 0;
    var committed = false;
    // Canonical host metadata returns valid refs without SDK calls/allocation.
    // A violated ownership invariant is fatal, never a silently lost token.
    const pool = storage.hostPool().?;
    defer if (!committed and prepared > reserved_used) {
        for (items[reserved_used..prepared]) |item| _ = pool.releaseRef(allocator, item.destination) catch @panic("invalid staged KV page retirement");
    };
    for (begin_block..end_block) |index| {
        const source = table.blocks.items[index];
        const info = pool.storage(source).?.meta;
        if (info.refcount <= 1) continue;
        const target = if (prepared < reserved_used) reserved.items[reserved.items.len - 1 - prepared] else try storage.acquire(allocator);
        const prefix: u16 = if (index + 1 == table.blocks.items.len) table.tail_tokens else storage.config.page_size_tokens;
        items[prepared] = .{ .source = source, .destination = target, .prefix_tokens = prefix, .table_index = index };
        prepared += 1;
    }
    std.debug.assert(prepared == count);
    const request = BlockCopy.Request{ .sequence = sequence, .page_size = storage.config.page_size_tokens, .layers = storage.config.num_layers_packed, .items = items };
    try request.validate();
    // No metadata/ID is published until ALL numerical copies finish. Providers
    // must prove drain on every attempted-submit error before returning here.
    if (provider) |device| try device.copy_and_wait(device.context, device.implementation, request);
    for (items) |item| {
        if (storage.config.store_cpu_bytes) try storage.copyExactHostBlockPrefix(item.source, item.destination, item.prefix_tokens) else storage.copyCanonicalBlockPrefixMetadata(item.source, item.destination, item.prefix_tokens);
    }
    // Everything below is infallible for the canonical source owner. Maintain
    // a conservative marker through every individual publication as well.
    table.shared_prefix_blocks = @max(table.shared_prefix_blocks, conservative_end);
    for (items) |item| {
        table.blocks.items[item.table_index] = item.destination;
        _ = pool.releaseRef(allocator, item.source) catch @panic("invalid committed KV source retirement");
    }
    reserved.items.len -= reserved_used;
    committed = true;
    var shared_end: u32 = 0;
    for (table.blocks.items, 0..) |id, index| if (pool.storage(id).?.meta.refcount > 1) {
        shared_end = @intCast(index + 1);
    };
    table.shared_prefix_blocks = shared_end;
}

test "KV tail metadata failure never calls the provider acquire or retirement" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    defer storage.deinit(a);
    const original = storage;
    const Probe = struct {
        acquired: usize = 0,
        retired: usize = 0,
        fn acquire(raw: *anyopaque, _: std.mem.Allocator) !block.KvBlockId {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.acquired += 1;
            return error.InjectedProviderAcquireFailure;
        }
        fn release(raw: *anyopaque, _: std.mem.Allocator, _: block.KvBlockId) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.retired += 1;
            return error.UnexpectedProviderRetirement;
        }
    };
    var probe = Probe{};
    var custom = original.vtable.*;
    custom.acquire = Probe.acquire;
    custom.releaseRef = Probe.release;
    storage.ptr = &probe;
    storage.vtable = &custom;
    defer storage = original;
    var table = tables.SequenceBlockTable{};
    defer table.deinit(a);
    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, appendAcquired(&storage, &table, failure.allocator()));
    try std.testing.expectEqual(@as(usize, 0), table.blocks.items.len);
    try std.testing.expectEqual(@as(usize, 0), probe.acquired);
    try std.testing.expectEqual(@as(usize, 0), probe.retired);
    try std.testing.expectError(error.InjectedProviderAcquireFailure, appendAcquired(&storage, &table, a));
    try std.testing.expectEqual(@as(usize, 0), table.blocks.items.len);
    try std.testing.expectEqual(@as(usize, 1), probe.acquired);
    try std.testing.expectEqual(@as(usize, 0), probe.retired);
}

test "device-only KV copy publishes canonical prefix metadata after provider completion" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .cuda, .dtype = .f16, .page_size_tokens = 4, .num_layers_packed = 2, .num_kv_heads = 1, .head_dim = 8, .store_cpu_bytes = false });
    defer storage.deinit(a);
    var table = tables.SequenceBlockTable{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    const source = try storage.acquire(a);
    try table.append(a, source);
    table.tail_tokens = 2;
    try storage.retain(source); // Independent prefix cache keeps its own ref.
    defer _ = storage.hostPool().?.releaseRef(a, source) catch unreachable;
    const metadata = storage.hostPool().?.storageMut(source).?;
    metadata.meta.tokens_written = 4;
    metadata.meta.model_tag = 0x1234;
    metadata.meta.priority = 9;
    metadata.meta.prefix_cacheable = true;
    const Probe = struct {
        table: *tables.SequenceBlockTable,
        original: block.KvBlockId,
        done: bool = false,
        fn copy(raw: *anyopaque, _: *const anyopaque, request: BlockCopy.Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try request.validate();
            try std.testing.expectEqual(self.original, self.table.last().?);
            try std.testing.expectEqual(@as(u16, 2), self.table.tail_tokens);
            self.done = true; // Stands for checked device completion.
        }
    };
    var probe = Probe{ .table = &table, .original = source };
    try ensureRangeWithProvider(&storage, &table, &reserved, a, 0, 1, 1, false, .{ .context = &probe, .implementation = &probe, .copy_and_wait = Probe.copy });
    try std.testing.expect(probe.done);
    const target = table.last().?;
    defer _ = storage.hostPool().?.releaseRef(a, target) catch unreachable;
    try std.testing.expect(source != target);
    const copied = storage.hostPool().?.storage(target).?.meta;
    try std.testing.expectEqual(@as(u16, 2), copied.tokens_written);
    try std.testing.expectEqual(@as(u64, 0x1234), copied.model_tag);
    try std.testing.expectEqual(@as(i32, 9), copied.priority);
    try std.testing.expect(!copied.prefix_cacheable);
    try std.testing.expectEqual(@as(u32, 1), copied.refcount);
    try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(source).?.refcount);
}

test "multi-page COW cold failure leaves every logical ID value and shared marker unchanged" {
    const a = std.testing.allocator;
    var failed = false;
    var succeeded = false;
    for (0..32) |failure_index| {
        var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
        defer storage.deinit(a);
        var table = tables.SequenceBlockTable{};
        defer table.deinit(a);
        var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer reserved.deinit(a);
        var originals: [3]block.KvBlockId = undefined;
        for (&originals, 0..) |*id, index| {
            id.* = try storage.acquire(a);
            try table.append(a, id.*);
            try storage.retain(id.*);
            const value: f32 = @floatFromInt(index + 1);
            try storage.writeToken(id.*, 0, 0, &.{value}, &.{value});
        }
        table.tail_tokens = 2;
        table.shared_prefix_blocks = 3;
        var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = failure_index });
        if (ensureRange(&storage, &table, &reserved, failure.allocator(), 0, 6, true)) |_| {
            succeeded = true;
        } else |err| {
            if (err != error.OutOfMemory) return err;
            failed = true;
            try std.testing.expectEqualSlices(block.KvBlockId, &originals, table.blocks.items);
            try std.testing.expectEqual(@as(u32, 3), table.shared_prefix_blocks);
        }
        var refs: usize = 0;
        for (storage.hostPool().?.blocks.items) |entry| refs += entry.meta.refcount;
        try std.testing.expectEqual(@as(usize, 6), refs);
        for (originals, 0..) |id, index| {
            const row = try storage.readToken(id, 0, 0);
            const value: f32 = @floatFromInt(index + 1);
            try std.testing.expectEqualSlices(f32, &.{value}, row.k);
            try std.testing.expectEqualSlices(f32, &.{value}, row.v);
        }
        // Return both independently owned sets; normal host retirement allocates nothing.
        for (originals) |id| _ = try storage.releaseRef(a, id);
        for (table.blocks.items) |id| _ = try storage.releaseRef(a, id);
        if (succeeded) break;
    }
    try std.testing.expect(failed and succeeded);
}

test "multi-plane provider refusal publishes no COW IDs or scalar metadata" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .cuda, .dtype = .f16, .page_size_tokens = 2, .num_layers_packed = 2, .num_kv_heads = 1, .head_dim = 4, .store_cpu_bytes = false });
    defer storage.deinit(a);
    var table = tables.SequenceBlockTable{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    var originals: [3]block.KvBlockId = undefined;
    for (&originals) |*id| {
        id.* = try storage.acquire(a);
        try table.append(a, id.*);
        try storage.retain(id.*);
        storage.hostPool().?.storageMut(id.*).?.meta.model_tag = 99;
    }
    table.tail_tokens = 2;
    table.shared_prefix_blocks = 3;
    const Probe = struct {
        done: bool = false,
        fn refuse(raw: *anyopaque, _: *const anyopaque, request: BlockCopy.Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try request.validate();
            try std.testing.expectEqual(@as(usize, 3), request.items.len);
            self.done = true;
            return error.ProviderLayoutChangedBeforeSubmission;
        }
    };
    var probe = Probe{};
    try std.testing.expectError(error.ProviderLayoutChangedBeforeSubmission, ensureRangeWithProvider(&storage, &table, &reserved, a, 0, 6, 1, false, .{ .context = &probe, .implementation = &probe, .copy_and_wait = Probe.refuse }));
    try std.testing.expect(probe.done);
    try std.testing.expectEqualSlices(block.KvBlockId, &originals, table.blocks.items);
    try std.testing.expectEqual(@as(u32, 3), table.shared_prefix_blocks);
    var refs: usize = 0;
    for (storage.hostPool().?.blocks.items) |entry| refs += entry.meta.refcount;
    try std.testing.expectEqual(@as(usize, 6), refs);
    for (originals) |id| {
        try std.testing.expectEqual(@as(u64, 99), storage.hostPool().?.storage(id).?.meta.model_tag);
        _ = try storage.releaseRef(a, id);
        _ = try storage.releaseRef(a, id);
    }
}
