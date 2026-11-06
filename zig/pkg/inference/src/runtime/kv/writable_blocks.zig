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

/// Only the exact host metadata owner can promise infallible staged-ref
/// retirement. Unknown storage callbacks retain their compatibility contract.
pub fn canPrepareAppend(storage: *storage_mod.KvStorage, table: *const tables.SequenceBlockTable, reserved: *const std.ArrayListUnmanaged(block.KvBlockId), host_copy_allowed: bool, provider: ?CopyProvider) bool {
    _ = reserved;
    if (!storage.hasCanonicalHostBlockOwnership() or storage.config.page_size_tokens == 0) return false;
    if (table.tail_tokens > storage.config.page_size_tokens) return false;
    if (table.blocks.items.len == 0) return table.tail_tokens == 0;
    const tail = storage.blockInfo(table.last().?) orelse return false;
    if (tail.refcount == 0) return false;
    if (table.tail_tokens == storage.config.page_size_tokens or tail.refcount == 1) return true;
    return provider != null or (host_copy_allowed and storage.supportsExactHostBlockCopy());
}

/// A synchronous exclusive transaction. Only private destinations change
/// during preparation; logical IDs, tail, reservations and old refs publish
/// together after every participating owner has prepared successfully.
/// Always defer abort(): after commit it releases temporary plan metadata
/// without rolling back the published pages.
pub const PreparedAppend = struct {
    storage: *storage_mod.KvStorage,
    table: *tables.SequenceBlockTable,
    reserved: *std.ArrayListUnmanaged(block.KvBlockId),
    allocator: std.mem.Allocator,
    old_blocks: usize,
    old_reserved: usize,
    append_pages: usize,
    drop_pages: usize,
    tail: u16,
    shared: u32,
    future_tokens: usize,
    future_reserved: usize,
    base_reserved: usize,
    recycle_count: usize = 0,
    recycle_inline: [8]block.KvBlockId = undefined,
    recycle_spill: ?[]block.KvBlockId = null,
    cow: ?BlockCopy.Item = null,
    recycle_cow: bool = false,
    acquired: usize = 0,
    acquire_inline: [8]block.KvBlockId = undefined,
    acquire_spill: ?[]block.KvBlockId = null,
    retire_count: usize = 0,
    retire_inline: [8]block.KvBlockId = undefined,
    retire_spill: ?[]block.KvBlockId = null,
    active: bool = true,

    fn acquiredItems(self: *@This()) []block.KvBlockId {
        return if (self.acquire_spill) |items| items else &self.acquire_inline;
    }
    fn recycleItems(self: *@This()) []block.KvBlockId {
        return if (self.recycle_spill) |items| items else &self.recycle_inline;
    }
    fn recycled(self: *@This(), id: block.KvBlockId) bool {
        if (self.recycle_cow) if (self.cow) |item| if (item.destination == id) return true;
        return std.sort.binarySearch(block.KvBlockId, self.recycleItems()[0..self.recycle_count], id, struct {
            fn order(a: block.KvBlockId, b: block.KvBlockId) std.math.Order {
                return std.math.order(a, b);
            }
        }.order) != null;
    }
    fn retireItems(self: *@This()) []block.KvBlockId {
        return if (self.retire_spill) |items| items else &self.retire_inline;
    }
    fn reservedAt(self: *@This(), index: usize) block.KvBlockId {
        return if (index < self.old_reserved) self.reserved.items[index] else self.acquiredItems()[index - self.old_reserved];
    }
    fn logicalAt(self: *@This(), index: usize) block.KvBlockId {
        if (index >= self.old_blocks) return self.reservedAt(index - self.old_blocks);
        if (self.cow) |item| if (item.table_index == index) return item.destination;
        return self.table.blocks.items[index];
    }
    fn freeMetadata(self: *@This()) void {
        if (self.acquire_spill) |items| self.allocator.free(items);
        if (self.retire_spill) |items| self.allocator.free(items);
        if (self.recycle_spill) |items| self.allocator.free(items);
        self.recycle_spill = null;
        self.acquire_spill = null;
        self.retire_spill = null;
    }
    /// Always defer cleanup, including after commit: releasing staging
    /// metadata can call an allocator and belongs AFTER paired publication.
    pub fn abort(self: *@This()) void {
        if (self.active) {
            self.active = false;
            const pool = self.storage.hostPool().?;
            for (self.acquiredItems()[0..self.acquired]) |id| _ = pool.releaseRef(self.allocator, id) catch @panic("invalid staged append ref");
        }
        self.freeMetadata();
    }
    pub fn commit(self: *@This()) void {
        std.debug.assert(self.active);
        if (self.cow) |item| self.table.blocks.items[item.table_index] = item.destination;
        for (0..self.append_pages) |index| self.table.blocks.appendAssumeCapacity(self.reservedAt(index));
        // Read reserved sources forwards before shortening their original
        // array: every source index is at or after its destination index.
        if (self.append_pages != 0 or self.cow != null or self.acquired != 0 or self.recycle_count != 0) {
            for (0..self.base_reserved) |index| self.reserved.items.ptr[index] = self.reservedAt(self.append_pages + index);
            for (self.recycleItems()[0..self.recycle_count], 0..) |id, index| self.reserved.items.ptr[self.base_reserved + index] = id;
            if (self.recycle_cow) self.reserved.items.ptr[self.base_reserved + self.recycle_count] = self.cow.?.destination;
            self.reserved.items.len = self.future_reserved;
        }
        if (self.drop_pages != 0) {
            const remaining = self.table.blocks.items.len - self.drop_pages;
            std.mem.copyForwards(block.KvBlockId, self.table.blocks.items[0..remaining], self.table.blocks.items[self.drop_pages..]);
            self.table.blocks.items.len = remaining;
        }
        self.table.tail_tokens = self.tail;
        self.table.shared_prefix_blocks = self.shared;
        const pool = self.storage.hostPool().?;
        // These exact unique front refs become unused reservations only now,
        // after publication removed their old logical pages. Match canonical
        // free-list acquire reset without releasing/reacquiring an owning ref.
        const reset_count = self.recycle_count + @intFromBool(self.recycle_cow);
        for (0..reset_count) |index| {
            const id = if (index < self.recycle_count) self.recycleItems()[index] else self.cow.?.destination;
            const entry = pool.storageMut(id).?;
            entry.meta.tokens_written = 0;
            std.debug.assert(entry.meta.refcount == 1);
            if (self.storage.config.store_cpu_bytes) {
                @memset(entry.k_data, 0);
                @memset(entry.v_data, 0);
            }
        }
        for (self.retireItems()[0..self.retire_count]) |id| _ = pool.releaseRef(self.allocator, id) catch @panic("invalid committed append ref");
        self.active = false;
        // The caller's deferred abort frees staging metadata only after every
        // participating commit and total/view publication has completed.
    }
    pub fn idsChanged(self: *const @This()) bool {
        return self.append_pages != 0 or self.drop_pages != 0 or self.cow != null or self.future_reserved != self.old_reserved;
    }
    pub fn futureLogicalBlockCount(self: *const @This()) usize {
        return self.old_blocks + self.append_pages - self.drop_pages;
    }
    pub fn futureReservedBlockCount(self: *const @This()) usize {
        return self.future_reserved;
    }
    pub fn futureTokenCount(self: *const @This()) usize {
        return self.future_tokens;
    }
};

pub fn prepareAppend(storage: *storage_mod.KvStorage, table: *tables.SequenceBlockTable, reserved: *std.ArrayListUnmanaged(block.KvBlockId), allocator: std.mem.Allocator, count: usize, reserve_to: usize, window: ?usize, sequence: u32, host_copy_allowed: bool, provider: ?CopyProvider) !PreparedAppend {
    if (count == 0 or !canPrepareAppend(storage, table, reserved, host_copy_allowed, provider)) return error.SharedKvWriteUnsupported;
    const page: usize = storage.config.page_size_tokens;
    const before = if (table.blocks.items.len == 0) 0 else try std.math.add(usize, try std.math.mul(usize, table.blocks.items.len - 1, page), table.tail_tokens);
    const after = try std.math.add(usize, before, count);
    const logical = try std.math.divCeil(usize, after, page);
    const copies = tailCopies(storage, table) orelse return error.InvalidBlockId;
    const append_pages = logical - table.blocks.items.len;
    const tail: u16 = @intCast(if (after % page == 0) page else after % page);
    const full_pages = logical - @intFromBool(tail != page);
    const drop_pages = if (window) |keep| @min((after -| keep) / page, full_pages) else 0;
    const consumed_reservations = try std.math.add(usize, append_pages, copies);
    const reserve_pages = try std.math.divCeil(usize, reserve_to, page);
    // Replay reservation is for the FINAL post-window view, as in the original
    // append -> trim -> reserve sequence. Old front pages remain live throughout
    // preparation; their future release is never fictional allocation credit.
    const future_reserved = @max(reserved.items.len -| consumed_reservations, reserve_pages -| (logical - drop_pages));
    var plan = PreparedAppend{ .storage = storage, .table = table, .reserved = reserved, .allocator = allocator, .old_blocks = table.blocks.items.len, .old_reserved = reserved.items.len, .append_pages = append_pages, .drop_pages = drop_pages, .tail = if (drop_pages == logical) 0 else tail, .shared = 0, .future_tokens = after - drop_pages * page, .future_reserved = future_reserved, .base_reserved = future_reserved };
    errdefer plan.abort();
    const missing_reserve = future_reserved -| (reserved.items.len -| consumed_reservations);
    const old_dropped = @min(drop_pages, table.blocks.items.len - @intFromBool(copies != 0));
    const recycle_limit = @min(missing_reserve, old_dropped);
    if (recycle_limit > plan.recycle_inline.len) plan.recycle_spill = try allocator.alloc(block.KvBlockId, recycle_limit);
    for (table.blocks.items[0..old_dropped]) |id| {
        if (plan.recycle_count == recycle_limit) break;
        const info = storage.blockInfo(id) orelse return error.InvalidBlockId;
        if (info.refcount == 0) return error.InvalidBlockRefcount;
        if (info.refcount != 1) continue;
        plan.recycleItems()[plan.recycle_count] = id;
        plan.recycle_count += 1;
    }
    const recycled = plan.recycleItems()[0..plan.recycle_count];
    std.mem.sort(block.KvBlockId, recycled, {}, std.sort.asc(block.KvBlockId));
    for (recycled, 0..) |id, index| if (index != 0 and recycled[index - 1] == id) return error.InvalidBlockRefcount;
    plan.recycle_cow = copies != 0 and drop_pages >= table.blocks.items.len and missing_reserve > plan.recycle_count;
    plan.base_reserved -= plan.recycle_count + @intFromBool(plan.recycle_cow);
    const acquire_count = (try std.math.add(usize, consumed_reservations, plan.base_reserved)) -| reserved.items.len;
    const total_reserved = try std.math.add(usize, reserved.items.len, acquire_count);
    if (acquire_count > plan.acquire_inline.len) plan.acquire_spill = try allocator.alloc(block.KvBlockId, acquire_count);
    const retirement_count = (try std.math.add(usize, drop_pages, copies)) - plan.recycle_count - @intFromBool(plan.recycle_cow);
    if (retirement_count > plan.retire_inline.len) plan.retire_spill = try allocator.alloc(block.KvBlockId, retirement_count);
    try table.blocks.ensureTotalCapacity(allocator, logical);
    try reserved.ensureTotalCapacity(allocator, @max(total_reserved, future_reserved));
    for (0..acquire_count) |_| {
        const id = try storage.acquire(allocator);
        plan.acquiredItems()[plan.acquired] = id;
        plan.acquired += 1;
    }
    // Consumed reservation pages are private, uniquely owned destinations.
    for (0..append_pages) |index| {
        const info = storage.blockInfo(plan.reservedAt(index)) orelse return error.InvalidBlockId;
        if (info.refcount != 1 or plan.recycled(plan.reservedAt(index))) return error.InvalidBlockRefcount;
    }
    var shared = @min(@as(usize, table.shared_prefix_blocks), table.blocks.items.len);
    if (copies != 0) {
        const source = table.last().?;
        const target = plan.reservedAt(total_reserved - 1);
        const info = storage.blockInfo(target) orelse return error.InvalidBlockId;
        if (info.refcount != 1) return error.InvalidBlockRefcount;
        shared = 0;
        for (table.blocks.items, 0..) |id, index| {
            if (id == target) return error.InvalidKvBlockCopy;
            const prior = storage.blockInfo(id) orelse return error.InvalidBlockId;
            if (prior.refcount == 0) return error.InvalidBlockRefcount;
            if (index + 1 != table.blocks.items.len and prior.refcount > 1) shared = index + 1;
        }
        plan.cow = .{ .source = source, .destination = target, .prefix_tokens = table.tail_tokens, .table_index = table.blocks.items.len - 1 };
        plan.retireItems()[plan.retire_count] = source;
        plan.retire_count += 1;
    }
    plan.shared = std.math.cast(u32, shared -| drop_pages) orelse return error.InvalidKvShape;
    for (0..drop_pages) |index| {
        const id = plan.logicalAt(index);
        if (plan.recycled(id)) continue;
        plan.retireItems()[plan.retire_count] = id;
        plan.retire_count += 1;
    }
    const retired = plan.retireItems()[0..plan.retire_count];
    std.mem.sort(block.KvBlockId, retired, {}, std.sort.asc(block.KvBlockId));
    var cursor: usize = 0;
    while (cursor < retired.len) {
        var end = cursor + 1;
        while (end < retired.len and retired[end] == retired[cursor]) : (end += 1) {}
        const info = storage.blockInfo(retired[cursor]) orelse return error.InvalidBlockId;
        if (info.refcount < end - cursor) return error.InvalidBlockRefcount;
        cursor = end;
    }
    if (plan.cow) |item| {
        const items = [_]BlockCopy.Item{item};
        try copyPreparedPrefixes(storage, sequence, &items, provider);
    }
    return plan;
}

/// One exact nonallocating copy/drain protocol for compatibility COW and
/// staged append. Publication and reference ownership remain with each caller.
fn copyPreparedPrefixes(storage: *storage_mod.KvStorage, sequence: u32, items: []const BlockCopy.Item, provider: ?CopyProvider) !void {
    const request = BlockCopy.Request{ .sequence = sequence, .page_size = storage.config.page_size_tokens, .layers = storage.config.num_layers_packed, .items = items, .cleanup = if (provider) |value| value.cleanup else .{} };
    try request.validate();
    // Attempted submissions must drain or fail closed before callers can
    // recycle any private destination. No logical ID is published here.
    if (provider) |device| try device.copy_and_wait(device.context, device.implementation, request);
    for (items) |item| {
        if (storage.config.store_cpu_bytes) try storage.copyExactHostBlockPrefix(item.source, item.destination, item.prefix_tokens) else storage.copyCanonicalBlockPrefixMetadata(item.source, item.destination, item.prefix_tokens);
    }
}

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
    try copyPreparedPrefixes(storage, sequence, items, provider);
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

test "prepared KV append keeps shared tail reservations and window unpublished until infallible commit" {
    const Fixture = struct {
        fn check(a: std.mem.Allocator) !void {
            var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
            defer storage.deinit(a);
            var table: tables.SequenceBlockTable = .{};
            defer table.deinit(a);
            var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
            defer reserved.deinit(a);
            defer {
                for (table.blocks.items) |id| _ = storage.releaseRef(a, id) catch unreachable;
                for (reserved.items) |id| _ = storage.releaseRef(a, id) catch unreachable;
            }
            const source = try appendAcquired(&storage, &table, a);
            table.tail_tokens = 1;
            try storage.writeToken(source, 0, 0, &.{3}, &.{7});
            try storage.retain(source);
            defer _ = storage.releaseRef(a, source) catch unreachable;
            {
                var abandoned = try prepareAppend(&storage, &table, &reserved, a, 4, 8, 3, 1, true, null);
                defer abandoned.abort();
                try std.testing.expectEqualSlices(block.KvBlockId, &.{source}, table.blocks.items);
                try std.testing.expectEqual(@as(u16, 1), table.tail_tokens);
                try std.testing.expectEqual(@as(usize, 0), reserved.items.len);
                try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(source).?.refcount);
                try std.testing.expectEqual(@as(usize, 3), abandoned.futureTokenCount());
                try std.testing.expectEqual(@as(usize, 2), abandoned.futureLogicalBlockCount());
                try std.testing.expectEqual(@as(usize, 2), abandoned.futureReservedBlockCount());
            }
            try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(source).?.refcount);
            var plan = try prepareAppend(&storage, &table, &reserved, a, 4, 8, 3, 1, true, null);
            defer plan.abort();
            plan.commit();
            plan.abort(); // transferred ownership is never retired twice
            try std.testing.expectEqual(@as(usize, 3), table.tokenCount(2));
            try std.testing.expectEqual(@as(usize, 2), table.blocks.items.len);
            try std.testing.expectEqual(@as(usize, 2), reserved.items.len);
            try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(source).?.refcount);
            const original = try storage.readToken(source, 0, 0);
            try std.testing.expectEqualSlices(f32, &.{3}, original.k);
            try std.testing.expectEqualSlices(f32, &.{7}, original.v);
        }
    };
    try Fixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "prepared KV append ordinary unique tail performs no allocation or copy callback" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 1 });
    defer storage.deinit(a);
    var table: tables.SequenceBlockTable = .{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    const id = try appendAcquired(&storage, &table, a);
    defer _ = storage.releaseRef(a, id) catch unreachable;
    table.tail_tokens = 1;
    const Probe = struct {
        calls: usize = 0,
        fn copy(raw: *anyopaque, _: *const anyopaque, _: BlockCopy.Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedWarmCopy;
        }
    };
    var probe: Probe = .{};
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var plan = try prepareAppend(&storage, &table, &reserved, failing.allocator(), 1, 0, null, 1, false, .{ .context = &probe, .implementation = &probe, .copy_and_wait = Probe.copy });
    defer plan.abort();
    try std.testing.expectEqual(@as(u16, 1), table.tail_tokens);
    try std.testing.expect(!plan.idsChanged());
    plan.commit();
    try std.testing.expectEqual(@as(u16, 2), table.tail_tokens);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
}

test "prepared KV append copy failure and custom providers cannot publish logical mutation" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    defer storage.deinit(a);
    var table: tables.SequenceBlockTable = .{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    const id = try appendAcquired(&storage, &table, a);
    defer _ = storage.releaseRef(a, id) catch unreachable;
    table.tail_tokens = 1;
    try storage.retain(id);
    defer _ = storage.releaseRef(a, id) catch unreachable;
    const Probe = struct {
        calls: usize = 0,
        fn copy(raw: *anyopaque, _: *const anyopaque, _: BlockCopy.Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.DrainedCopyRefused;
        }
    };
    var probe: Probe = .{};
    try std.testing.expectError(error.DrainedCopyRefused, prepareAppend(&storage, &table, &reserved, a, 2, 0, null, 1, false, .{ .context = &probe, .implementation = &probe, .copy_and_wait = Probe.copy }));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqualSlices(block.KvBlockId, &.{id}, table.blocks.items);
    try std.testing.expectEqual(@as(u16, 1), table.tail_tokens);
    try std.testing.expectEqual(@as(usize, 0), reserved.items.len);
    try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(id).?.refcount);
    const copy = storage.vtable.*;
    var foreign = storage;
    foreign.vtable = &copy;
    try std.testing.expect(!canPrepareAppend(&foreign, &table, &reserved, true, null));
    try std.testing.expectError(error.SharedKvWriteUnsupported, prepareAppend(&foreign, &table, &reserved, a, 1, 0, null, 1, true, null));
}

test "prepared KV window recycles unique old fronts only at commit without extra page allocation" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    defer storage.deinit(a);
    var table: tables.SequenceBlockTable = .{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    defer {
        for (table.blocks.items) |id| _ = storage.releaseRef(a, id) catch unreachable;
        for (reserved.items) |id| _ = storage.releaseRef(a, id) catch unreachable;
    }
    for (0..3) |_| _ = try appendAcquired(&storage, &table, a);
    table.tail_tokens = 2;
    try table.blocks.ensureTotalCapacity(a, 4);
    try reserved.ensureTotalCapacity(a, 2);
    reserved.appendAssumeCapacity(try storage.acquire(a));
    const first = table.blocks.items[0];
    const second = table.blocks.items[1];
    try storage.writeToken(first, 0, 0, &.{3}, &.{7});
    try storage.writeToken(second, 0, 0, &.{5}, &.{9});
    const physical_before = storage.liveBlocks();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var plan = try prepareAppend(&storage, &table, &reserved, failing.allocator(), 2, 8, 4, 1, true, null);
    defer plan.abort();
    try std.testing.expect(plan.idsChanged());
    try std.testing.expectEqual(@as(usize, 2), plan.futureLogicalBlockCount());
    try std.testing.expectEqual(@as(usize, 2), plan.futureReservedBlockCount());
    try std.testing.expectEqual(@as(usize, 6), table.tokenCount(2));
    try std.testing.expectEqualSlices(f32, &.{3}, (try storage.readToken(first, 0, 0)).k);
    try std.testing.expectEqualSlices(f32, &.{5}, (try storage.readToken(second, 0, 0)).k);
    plan.commit();
    try std.testing.expectEqual(@as(usize, 4), table.tokenCount(2));
    try std.testing.expectEqual(@as(usize, 2), reserved.items.len);
    try std.testing.expectEqual(physical_before, storage.liveBlocks());
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    for (reserved.items) |id| {
        try std.testing.expect(id == first or id == second);
        try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(id).?.refcount);
        try std.testing.expectEqual(@as(u16, 0), storage.blockInfo(id).?.tokens_written);
    }
}

test "prepared KV metadata-only canonical shared tail requires an exact payload copy provider" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .cuda, .dtype = .f16, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 1, .store_cpu_bytes = false });
    defer storage.deinit(a);
    var table: tables.SequenceBlockTable = .{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    const id = try appendAcquired(&storage, &table, a);
    defer _ = storage.releaseRef(a, id) catch unreachable;
    table.tail_tokens = 1;
    try std.testing.expect(storage.hasCanonicalHostBlockOwnership());
    try std.testing.expect(!storage.supportsExactHostBlockCopy());
    try std.testing.expect(canPrepareAppend(&storage, &table, &reserved, true, null));
    try storage.retain(id);
    defer _ = storage.releaseRef(a, id) catch unreachable;
    try std.testing.expect(!canPrepareAppend(&storage, &table, &reserved, true, null));
    try std.testing.expectError(error.SharedKvWriteUnsupported, prepareAppend(&storage, &table, &reserved, a, 1, 0, null, 1, true, null));
    const Copy = struct {
        fn run(_: *anyopaque, _: *const anyopaque, _: BlockCopy.Request) !void {
            return error.CopyMustNotRunDuringEligibility;
        }
    };
    var context: u8 = 0;
    try std.testing.expect(canPrepareAppend(&storage, &table, &reserved, false, .{ .context = &context, .implementation = &context, .copy_and_wait = Copy.run }));
    try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(id).?.refcount);
    try std.testing.expectEqualSlices(block.KvBlockId, &.{id}, table.blocks.items);
    try std.testing.expectEqual(@as(u16, 1), table.tail_tokens);
    try std.testing.expectEqual(@as(usize, 0), reserved.items.len);
}
