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
const append_inline_capacity = 8;

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

/// Upper bound for the THREE literal temporary ID-array allocations below.
/// The caller bounds logical history, one generated append (the request's
/// remaining-token budget, not a guessed speculative burst), and replay reserve.
/// Existing reserved IDs cancel out of new acquisition demand; they are not
/// another allocation or a transferable quota credit. Two simultaneously
/// prepared owners must independently declare this amount.
pub fn spillHostBytes(max_logical_tokens: usize, max_append_tokens: usize, max_replay_tokens: usize, page_size: usize, admitted: bool) !usize {
    if (page_size == 0) return error.InvalidPoolId;
    if (max_append_tokens == 0) return 0;
    const logical_pages = try std.math.divCeil(usize, max_logical_tokens, page_size);
    const append_pages = try std.math.divCeil(usize, max_append_tokens, page_size);
    const replay_pages = try std.math.divCeil(usize, max_replay_tokens, page_size);
    // A = Lafter-Lbefore <= ceil(count/page), including a partial tail.
    // acquire = (A+C+baseFutureReserved-oldReserved)^+ <= A+1+R.
    // retire <= dropped+C <= L+1; recycleLimit <= min(oldDropped,R).
    const acquire = try std.math.add(usize, try std.math.add(usize, append_pages, 1), replay_pages);
    const retire = try std.math.add(usize, logical_pages, 1);
    const recycle = @min(logical_pages, replay_pages);
    var total: usize = 0;
    for ([_]usize{ acquire, retire, recycle }) |count| {
        if (count <= append_inline_capacity) continue; // No out-of-line allocation.
        const payload = try std.math.mul(usize, count, @sizeOf(block.KvBlockId));
        const bytes = if (admitted) try @import("../../backends/admitted_allocator.zig").AdmittedAllocator.allocationHostBytes(payload, .of(block.KvBlockId)) else payload;
        total = try std.math.add(usize, total, bytes);
    }
    return total;
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
    recycle_inline: [append_inline_capacity]block.KvBlockId = undefined,
    recycle_spill: ?[]block.KvBlockId = null,
    cow: ?BlockCopy.Item = null,
    recycle_cow: bool = false,
    acquired: usize = 0,
    acquire_inline: [append_inline_capacity]block.KvBlockId = undefined,
    acquire_spill: ?[]block.KvBlockId = null,
    retire_count: usize = 0,
    retire_inline: [append_inline_capacity]block.KvBlockId = undefined,
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
    /// Existing logical IDs followed by reservations keep their exact order
    /// when a unique append only consumes their boundary and appends new IDs.
    /// This is stronger than idsChanged: that method also tracks the boundary.
    pub fn preservedCombinedPrefix(self: *const @This()) ?usize {
        if (self.cow != null or self.drop_pages != 0 or self.recycle_count != 0 or self.recycle_cow) return null;
        const prefix = std.math.add(usize, self.old_blocks, self.old_reserved) catch return null;
        const logical = std.math.add(usize, self.old_blocks, self.append_pages) catch return null;
        const future = std.math.add(usize, logical, self.future_reserved) catch return null;
        const appended = std.math.add(usize, prefix, self.acquired) catch return null;
        return if (future == appended) prefix else null;
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

pub fn canPrepareTruncate(storage: *const storage_mod.KvStorage) bool {
    return storage.hasCanonicalHostBlockOwnership() and storage.config.page_size_tokens != 0;
}

/// Logical tail retirement owns only a sorted copy of the removed ID span.
/// Physical encoded tails and shared readers remain unchanged. As with append,
/// defer abort() even after commit: temporary frees follow paired publication.
pub const PreparedTruncate = struct {
    storage: *storage_mod.KvStorage,
    table: *tables.SequenceBlockTable,
    allocator: std.mem.Allocator,
    logical: usize,
    reserved: usize,
    tokens: usize,
    tail: u16,
    shared: u32,
    old_logical: usize,
    retired: usize = 0,
    inline_ids: [append_inline_capacity]block.KvBlockId = undefined,
    spill_ids: ?[]block.KvBlockId = null,
    active: bool = true,

    fn items(self: *@This()) []block.KvBlockId {
        return if (self.spill_ids) |ids| ids else &self.inline_ids;
    }
    pub fn futureLogicalBlockCount(self: *const @This()) usize {
        return self.logical;
    }
    pub fn futureReservedBlockCount(self: *const @This()) usize {
        return self.reserved;
    }
    pub fn futureTokenCount(self: *const @This()) usize {
        return self.tokens;
    }
    pub fn idsChanged(self: *const @This()) bool {
        return self.old_logical != self.logical;
    }
    pub fn abort(self: *@This()) void {
        self.active = false;
        if (self.spill_ids) |ids| self.allocator.free(ids);
        self.spill_ids = null;
    }
    pub fn commit(self: *@This()) void {
        std.debug.assert(self.active);
        self.table.blocks.items.len = self.logical;
        self.table.tail_tokens = self.tail;
        self.table.shared_prefix_blocks = self.shared;
        const pool = self.storage.hostPool().?;
        for (self.items()[0..self.retired]) |id| _ = pool.releaseRef(self.allocator, id) catch @panic("invalid prepared KV truncate ref");
        self.active = false;
    }
};

/// count retains the original API's REMOVE semantics, saturating at empty.
pub fn prepareTruncate(storage: *storage_mod.KvStorage, table: *tables.SequenceBlockTable, reserved: *std.ArrayListUnmanaged(block.KvBlockId), allocator: std.mem.Allocator, count: usize) !PreparedTruncate {
    if (!canPrepareTruncate(storage)) return error.SharedKvWriteUnsupported;
    const page: usize = storage.config.page_size_tokens;
    if (table.tail_tokens > page) return error.InvalidKvShape;
    const before = if (table.blocks.items.len == 0) 0 else try std.math.add(usize, try std.math.mul(usize, table.blocks.items.len - 1, page), table.tail_tokens);
    const unchanged = count == 0 or before == 0;
    const after = before -| count;
    const logical = if (unchanged) table.blocks.items.len else if (after == 0) 0 else try std.math.divCeil(usize, after, page);
    var plan = PreparedTruncate{ .storage = storage, .table = table, .allocator = allocator, .logical = logical, .reserved = reserved.items.len, .tokens = after, .tail = if (unchanged) table.tail_tokens else if (after == 0) 0 else @intCast(if (after % page == 0) page else after % page), .shared = if (unchanged) table.shared_prefix_blocks else @intCast(@min(@as(usize, table.shared_prefix_blocks), logical)), .old_logical = table.blocks.items.len, .retired = table.blocks.items.len - logical };
    errdefer plan.abort();
    if (plan.retired > plan.inline_ids.len) plan.spill_ids = try allocator.alloc(block.KvBlockId, plan.retired);
    const ids = plan.items()[0..plan.retired];
    @memcpy(ids, table.blocks.items[logical..]);
    std.mem.sort(block.KvBlockId, ids, {}, std.sort.asc(block.KvBlockId));
    var cursor: usize = 0;
    while (cursor < ids.len) {
        var end = cursor + 1;
        while (end < ids.len and ids[end] == ids[cursor]) : (end += 1) {}
        const info = storage.blockInfo(ids[cursor]) orelse return error.InvalidBlockId;
        if (info.refcount < end - cursor) return error.InvalidBlockRefcount;
        cursor = end;
    }
    return plan;
}

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
    try reserved.ensureTotalCapacity(allocator, future_reserved);
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
            try std.testing.expectEqual(@as(?usize, null), plan.preservedCombinedPrefix());
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
    try std.testing.expectEqual(@as(?usize, null), plan.preservedCombinedPrefix());
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

test "prepared KV page boundary consumes a free page without unused reservation metadata allocation" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 1 });
    defer storage.deinit(a);
    var table: tables.SequenceBlockTable = .{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    defer {
        for (table.blocks.items) |id| _ = storage.releaseRef(a, id) catch unreachable;
    }
    _ = try appendAcquired(&storage, &table, a);
    table.tail_tokens = 2;
    try table.blocks.ensureTotalCapacity(a, 2);
    const free = try storage.acquire(a);
    _ = try storage.releaseRef(a, free);
    const before = storage.liveBlocks();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var prepared = try prepareAppend(&storage, &table, &reserved, failing.allocator(), 1, 0, null, 1, true, null);
    defer prepared.abort();
    try std.testing.expectEqual(@as(usize, 0), prepared.futureReservedBlockCount());
    prepared.commit();
    try std.testing.expectEqual(@as(usize, 3), table.tokenCount(2));
    try std.testing.expectEqual(free, table.last().?);
    try std.testing.expectEqual(@as(usize, 0), reserved.capacity);
    try std.testing.expectEqual(before, storage.liveBlocks());
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
}

test "prepared KV spill declaration counts only literal out of line owner arrays" {
    try std.testing.expectEqual(@as(usize, 0), try spillHostBytes(7 * 16, 7 * 16, 0, 16, false));
    try std.testing.expectEqual(@as(usize, 18 * @sizeOf(block.KvBlockId)), try spillHostBytes(8 * 16, 8 * 16, 0, 16, false));
    try std.testing.expectEqual(@as(usize, 1502 * @sizeOf(block.KvBlockId)), try spillHostBytes(24001, 1, 0, 16, false));
    // All three temporary owners can spill; each real allocation contributes
    // one tracking Header, without inventing a RunScope/Reservation per array.
    const counts = [_]usize{ 14, 13, 9 }; // L12,A4,R9 => acquire14,retire13,recycle9
    const admitted_mod = @import("../../backends/admitted_allocator.zig");
    var expected: usize = 0;
    for (counts) |count| expected += try admitted_mod.AdmittedAllocator.allocationHostBytes(count * @sizeOf(block.KvBlockId), .of(block.KvBlockId));
    try std.testing.expectEqual(expected, try spillHostBytes(12 * 16, 4 * 16, 9 * 16, 16, true));
    try std.testing.expectEqual(@as(usize, 0), try spillHostBytes(std.math.maxInt(usize), 0, 0, 16, true));
    try std.testing.expectError(error.InvalidPoolId, spillHostBytes(1, 1, 0, 0, false));
    try std.testing.expectError(error.Overflow, spillHostBytes(std.math.maxInt(usize), 1, 0, 1, false));
    try std.testing.expectError(error.Overflow, spillHostBytes(1, 1, std.math.maxInt(usize), 1, false));
}

test "prepared KV truncate keeps old encoded rows reservations and shared refs until commit" {
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
            for (0..12) |_| _ = try appendAcquired(&storage, &table, a);
            table.tail_tokens = 1;
            try reserved.ensureTotalCapacity(a, 2);
            for (0..2) |_| reserved.appendAssumeCapacity(try storage.acquire(a));
            const last = table.last().?;
            try storage.writeToken(last, 0, 0, &.{3}, &.{7});
            try storage.retain(last);
            defer _ = storage.releaseRef(a, last) catch unreachable;
            const first = table.blocks.items[0];
            {
                var abandoned = try prepareTruncate(&storage, &table, &reserved, a, 100);
                defer abandoned.abort();
                try std.testing.expectEqual(@as(usize, 23), table.tokenCount(2));
                try std.testing.expectEqual(@as(usize, 12), table.blocks.items.len);
                try std.testing.expectEqual(@as(usize, 0), abandoned.futureTokenCount());
                try std.testing.expectEqual(@as(usize, 2), abandoned.futureReservedBlockCount());
                try std.testing.expectEqual(@as(u32, 2), storage.blockInfo(last).?.refcount);
            }
            var prepared = try prepareTruncate(&storage, &table, &reserved, a, 22);
            defer prepared.abort();
            try std.testing.expect(prepared.idsChanged());
            try std.testing.expectEqual(@as(usize, 1), prepared.futureTokenCount());
            try std.testing.expectEqual(@as(usize, 23), table.tokenCount(2));
            prepared.commit();
            prepared.abort();
            try std.testing.expectEqualSlices(block.KvBlockId, &.{first}, table.blocks.items);
            try std.testing.expectEqual(@as(u16, 1), table.tail_tokens);
            try std.testing.expectEqual(@as(usize, 2), reserved.items.len);
            try std.testing.expectEqual(@as(u32, 1), storage.blockInfo(last).?.refcount);
            const held = try storage.readToken(last, 0, 0);
            try std.testing.expectEqualSlices(f32, &.{3}, held.k);
            try std.testing.expectEqualSlices(f32, &.{7}, held.v);
        }
    };
    try Fixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "prepared KV in-page truncate changes visibility without allocation or encoded row mutation" {
    const a = std.testing.allocator;
    var storage = try storage_mod.initHost(a, .{ .backend = .native, .dtype = .f32, .page_size_tokens = 8, .num_kv_heads = 1, .head_dim = 1 });
    defer storage.deinit(a);
    var table: tables.SequenceBlockTable = .{};
    defer table.deinit(a);
    var reserved: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
    defer reserved.deinit(a);
    const id = try appendAcquired(&storage, &table, a);
    defer _ = storage.releaseRef(a, id) catch unreachable;
    table.tail_tokens = 3;
    try storage.writeToken(id, 0, 2, &.{3}, &.{7});
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var prepared = try prepareTruncate(&storage, &table, &reserved, failing.allocator(), 1);
    defer prepared.abort();
    try std.testing.expect(!prepared.idsChanged());
    prepared.commit();
    try std.testing.expectEqual(@as(u16, 2), table.tail_tokens);
    try std.testing.expectEqualSlices(f32, &.{3}, (try storage.readToken(id, 0, 2)).k);
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    var zero = try prepareTruncate(&storage, &table, &reserved, failing.allocator(), 0);
    defer zero.abort();
    zero.commit();
    try std.testing.expectEqual(@as(u16, 2), table.tail_tokens);
    const copied = storage.vtable.*;
    var foreign = storage;
    foreign.vtable = &copied;
    try std.testing.expect(!canPrepareTruncate(&foreign));
    try std.testing.expectError(error.SharedKvWriteUnsupported, prepareTruncate(&foreign, &table, &reserved, a, 1));
}
