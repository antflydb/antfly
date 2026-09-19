// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const resources = @import("../resource_manager.zig");

/// Internal, heap-stable ownership of a prepaid physical allocation domain.
/// The manager and backing allocator must outlive the final physical free.
/// Every allocation retains the owner; publication forbids further allocation.
pub const Owner = struct {
    backing: std.mem.Allocator,
    metadata: resources.Reservation,
    credit: resources.CompletionCredit,
    tracked: u64 = 0,
    refs: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    retired: bool = false,

    pub fn create(backing: std.mem.Allocator, manager: *resources.ResourceManager, capacity: u64) !*Owner {
        if (capacity < @sizeOf(Owner)) return error.ResourceBudgetExceeded;
        var metadata = try manager.reserveWithoutReclaim(.lsm_in_memory_state, @sizeOf(Owner));
        errdefer metadata.release();
        const self = try backing.create(Owner);
        errdefer backing.destroy(self);
        self.* = .{ .backing = backing, .metadata = metadata, .credit = undefined };
        self.credit = try resources.CompletionCredit.init(manager, .lsm_in_memory_state, capacity - @sizeOf(Owner), &self.tracked);
        return self;
    }

    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn markPublished(self: *Owner) !void {
        self.lock();
        defer self.mutex.unlock();
        try self.credit.markPublished();
    }

    /// Release the submitting handle and unused capacity. Live buffers keep
    /// their exact charge and the allocator context through final reclamation.
    pub fn retire(self: *Owner) void {
        self.lock();
        std.debug.assert(!self.retired);
        self.retired = true;
        self.credit.releaseUnused();
        self.mutex.unlock();
        self.release();
    }

    fn release(self: *Owner) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        self.credit.deinit() catch unreachable;
        var metadata = self.metadata;
        const backing = self.backing;
        backing.destroy(self);
        metadata.release();
    }

    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.retired) return null;
        self.credit.stage(len) catch return null;
        const result = self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.credit.rollback(len) catch unreachable;
            return null;
        };
        _ = self.refs.fetchAdd(1, .monotonic);
        return result;
    }

    // Force reallocations through allocate-copy-free so their physical peak
    // is prepaid too; no backing allocator's resize semantics are assumed.
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.lock();
        self.credit.reclaim(memory.len) catch unreachable;
        if (self.retired) self.credit.releaseUnused();
        self.mutex.unlock();
        self.release();
    }
};

/// Only this trusted allocator can exclude bytes from the aggregate observer:
/// it has already staged those exact bytes on the dedicated credit observer.
pub fn isPrepaid(allocator: std.mem.Allocator) bool {
    return allocator.vtable == &Owner.vtable or allocator.vtable == &Arena.vtable or allocator.vtable == &RecyclingScratch.vtable or allocator.vtable == &PublicationReservation.vtable;
}

/// A physically allocated, heap-stable completion domain. All backing memory
/// is obtained before prepare. Later allocations only partition this slab;
/// readers retain the whole slab until its final allocation is released.
/// This deliberately does not recycle freed ranges during a completion attempt.
pub const Arena = struct {
    owner: *Owner,
    storage: []u8,
    used: usize = 0,
    refs: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,

    pub fn create(backing: std.mem.Allocator, manager: *resources.ResourceManager, bytes: usize) !*Arena {
        const overhead = @sizeOf(Owner) + @sizeOf(Arena);
        const total = std.math.add(usize, bytes, overhead) catch return error.ResourceBudgetExceeded;
        const owner = try Owner.create(backing, manager, total);
        errdefer owner.retire();
        const arena_alloc = owner.allocator();
        const self = try arena_alloc.create(Arena);
        errdefer arena_alloc.destroy(self);
        const storage = try arena_alloc.alloc(u8, bytes);
        self.* = .{ .owner = owner, .storage = storage };
        try owner.markPublished();
        return self;
    }

    pub fn allocator(self: *Arena) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Reuse physical backing only after every allocated buffer is retired.
    /// The pool owner must serialize this with new allocations and retain its
    /// own reference throughout. Published roots/readers prevent recycling.
    /// Reset retains the original physical charge and performs no allocation.
    pub fn resetIfExclusive(self: *Arena) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.refs.load(.acquire) != 1) return error.CompletionReservationBusy;
        self.used = 0;
    }

    /// Drop the preparing/dispatching owner's reference. Allocations retain
    /// their context independently, including after the backend retires a run.
    pub fn release(self: *Arena) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const owner = self.owner;
        const arena_alloc = owner.allocator();
        arena_alloc.free(self.storage);
        arena_alloc.destroy(self);
        owner.retire();
    }

    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
        const self: *Arena = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const base = @intFromPtr(self.storage.ptr);
        const start = std.mem.alignForward(usize, base + self.used, alignment.toByteUnits()) - base;
        if (start > self.storage.len or len > self.storage.len - start) return null;
        self.used = start + len;
        _ = self.refs.fetchAdd(1, .monotonic);
        return self.storage.ptr + start;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, _: std.mem.Alignment, _: usize) void {
        const self: *Arena = @ptrCast(@alignCast(raw));
        std.debug.assert(@intFromPtr(memory.ptr) >= @intFromPtr(self.storage.ptr));
        std.debug.assert(@intFromPtr(memory.ptr) + memory.len <= @intFromPtr(self.storage.ptr) + self.storage.len);
        self.release();
    }
};

test "completion arena preparation unwinds every backing allocation failure" {
    const Fixture = struct {
        fn check(backing: std.mem.Allocator) !void {
            var manager = resources.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            defer std.debug.assert(manager.snapshot().memory.used_bytes == 0);
            const arena = try Arena.create(backing, &manager, 4096);
            arena.release();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "completion arena aligns sealed allocations and retains full slab through final free" {
    var manager = resources.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const arena = try Arena.create(backing.allocator(), &manager, 4096);
    const allocation = arena.allocator();
    const charged = manager.snapshot().memory.used_bytes;
    try std.testing.expect(charged >= 4096);
    backing.fail_index = backing.alloc_index;
    const prefix = try allocation.alloc(u8, 3);
    const alignment = std.mem.Alignment.fromByteUnits(256);
    const aligned = allocation.rawAlloc(31, alignment, @returnAddress()) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(aligned) % 256);
    @memset(aligned[0..31], 0xa5);
    try std.testing.expectError(error.OutOfMemory, allocation.alloc(u8, 4096));
    try std.testing.expect(!backing.has_induced_failure);
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
    arena.release();
    allocation.free(prefix);
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u8, 0xa5), aligned[30]);
    allocation.rawFree(aligned[0..31], alignment, @returnAddress());
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
}

test "workload admission completion arena refuses recycle while readers retain physical buffers" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    const arena = try Arena.create(backing.allocator(), &manager, 4096);
    defer arena.release();
    const before = manager.snapshot().memory.used_bytes;
    const first = try arena.allocator().alloc(u8, 3072);
    @memset(first, 0x5a);
    try std.testing.expectError(error.CompletionReservationBusy, arena.resetIfExclusive());
    try std.testing.expectEqual(@as(u8, 0x5a), first[0]);
    arena.allocator().free(first);
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    try arena.resetIfExclusive();
    const next = try arena.allocator().alloc(u8, 4096);
    try std.testing.expectEqual(@intFromPtr(first.ptr), @intFromPtr(next.ptr));
    @memset(next, 0x31);
    arena.allocator().free(next);
    try arena.resetIfExclusive();
    try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
}

/// Reusable, physically backed scratch for streaming cursors/builders. Unlike
/// Arena it coalesces freed spans, so reading successive blocks does not consume
/// capacity proportional to the whole database. Scratch destruction refuses
/// outstanding allocations; published metadata instead retires the owner and
/// retains this allocator context until the last allocation is freed.
pub const RecyclingScratch = struct {
    const none = std.math.maxInt(usize);
    const Block = struct {
        size: usize,
        previous_size: usize,
        next_free: usize = none,
        previous_free: usize = none,
        allocated: bool = false,
    };
    const block_alignment = @alignOf(Block);
    const minimum_block = std.mem.alignForward(usize, @sizeOf(Block) + @sizeOf(usize) + 1, block_alignment);

    domain: *Arena,
    storage: []align(@alignOf(Block)) u8,
    free_head: usize = 0,
    live: usize = 0,
    retired: bool = false,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn create(backing: std.mem.Allocator, manager: *resources.ResourceManager, bytes: usize) !*RecyclingScratch {
        const usable = std.mem.alignBackward(usize, bytes, block_alignment);
        if (usable < minimum_block) return error.ResourceBudgetExceeded;
        const total = std.math.add(usize, usable, @sizeOf(RecyclingScratch) + 2 * @alignOf(RecyclingScratch)) catch return error.ResourceBudgetExceeded;
        const domain = try Arena.create(backing, manager, total);
        errdefer domain.release();
        const owned = domain.allocator();
        const self = try owned.create(RecyclingScratch);
        errdefer owned.destroy(self);
        const storage = try owned.alignedAlloc(u8, .fromByteUnits(block_alignment), usable);
        self.* = .{ .domain = domain, .storage = storage };
        self.block(0).* = .{ .size = usable, .previous_size = 0 };
        return self;
    }

    pub fn allocator(self: *RecyclingScratch) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Only the owner may introduce allocations; readers only free them. A
    /// serialized owner can therefore reuse an empty physical generation
    /// without treating unrelated free fragments as a contiguous reservation.
    pub fn isEmpty(self: *RecyclingScratch) bool {
        self.lock();
        defer self.mutex.unlock();
        return !self.retired and self.live == 0;
    }

    /// Physical span consumed by one allocation, including prefix, alignment
    /// and the largest unsplittable tail. Used by bounded publication proofs.
    pub fn allocationFootprint(bytes: usize, alignment: usize) !usize {
        if (alignment == 0 or !std.math.isPowerOfTwo(alignment)) return error.ResourceBudgetExceeded;
        var size = try std.math.add(usize, @sizeOf(Block) + @sizeOf(usize), alignment - 1);
        size = try std.math.add(usize, size, @max(bytes, 1));
        size = try std.math.add(usize, size, block_alignment - 1 + minimum_block - 1);
        return size;
    }

    pub fn destroy(self: *RecyclingScratch) !void {
        self.lock();
        if (self.live != 0) {
            self.mutex.unlock();
            return error.CompletionReservationBusy;
        }
        self.mutex.unlock();
        self.destroyEmpty();
    }

    /// Relinquish the publication owner without invalidating retained tree nodes
    /// or readers. The final physical free releases the slab and its resource
    /// charge. No subsequent allocation is allowed after retirement.
    pub fn retire(self: *RecyclingScratch) void {
        self.lock();
        std.debug.assert(!self.retired);
        self.retired = true;
        const empty = self.live == 0;
        self.mutex.unlock();
        if (empty) self.destroyEmpty();
    }

    fn destroyEmpty(self: *RecyclingScratch) void {
        const domain = self.domain;
        const owned = domain.allocator();
        const storage = self.storage;
        owned.free(storage);
        owned.destroy(self);
        domain.release();
    }

    fn lock(self: *RecyclingScratch) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn block(self: *RecyclingScratch, offset: usize) *Block {
        return @ptrCast(@alignCast(self.storage.ptr + offset));
    }
    fn removeFree(self: *RecyclingScratch, offset: usize) void {
        const item = self.block(offset);
        if (item.previous_free == none) self.free_head = item.next_free else self.block(item.previous_free).next_free = item.next_free;
        if (item.next_free != none) self.block(item.next_free).previous_free = item.previous_free;
        item.next_free = none;
        item.previous_free = none;
    }
    fn addFree(self: *RecyclingScratch, offset: usize) void {
        const item = self.block(offset);
        item.next_free = self.free_head;
        item.previous_free = none;
        if (self.free_head != none) self.block(self.free_head).previous_free = offset;
        self.free_head = offset;
    }
    fn updateFollowing(self: *RecyclingScratch, offset: usize) void {
        const item = self.block(offset);
        const next = offset + item.size;
        if (next < self.storage.len) self.block(next).previous_size = item.size;
    }

    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
        const self: *RecyclingScratch = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.retired) return null;
        var offset = self.free_head;
        const base = @intFromPtr(self.storage.ptr);
        while (offset != none) {
            const item = self.block(offset);
            const next_free = item.next_free;
            const unaligned = base + offset + @sizeOf(Block) + @sizeOf(usize);
            const rounded = std.math.add(usize, unaligned, alignment.toByteUnits() - 1) catch return null;
            const address = std.mem.alignBackward(usize, rounded, alignment.toByteUnits());
            const end = std.math.add(usize, address - base, @max(len, 1)) catch return null;
            const rounded_end = std.math.add(usize, end, block_alignment - 1) catch return null;
            const needed = std.mem.alignBackward(usize, rounded_end, block_alignment) - offset;
            if (needed <= item.size) {
                const original_size = item.size;
                self.removeFree(offset);
                if (original_size - needed >= minimum_block) {
                    item.size = needed;
                    const split = offset + needed;
                    self.block(split).* = .{ .size = original_size - needed, .previous_size = needed };
                    self.updateFollowing(split);
                    self.addFree(split);
                }
                item.allocated = true;
                self.updateFollowing(offset);
                const result: [*]u8 = @ptrFromInt(address);
                @memcpy((result - @sizeOf(usize))[0..@sizeOf(usize)], std.mem.asBytes(&offset));
                self.live += 1;
                return result;
            }
            offset = next_free;
        }
        return null;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, _: std.mem.Alignment, _: usize) void {
        const self: *RecyclingScratch = @ptrCast(@alignCast(raw));
        self.lock();
        defer {
            const destroy_domain = self.retired and self.live == 0;
            self.mutex.unlock();
            if (destroy_domain) self.destroyEmpty();
        }
        std.debug.assert(@intFromPtr(memory.ptr) >= @intFromPtr(self.storage.ptr) + @sizeOf(usize));
        std.debug.assert(@intFromPtr(memory.ptr) + memory.len <= @intFromPtr(self.storage.ptr) + self.storage.len);
        var offset = std.mem.bytesToValue(usize, (memory.ptr - @sizeOf(usize))[0..@sizeOf(usize)]);
        var item = self.block(offset);
        std.debug.assert(item.allocated and self.live != 0);
        item.allocated = false;
        self.live -= 1;
        if (item.previous_size != 0) {
            const previous_offset = offset - item.previous_size;
            const previous = self.block(previous_offset);
            if (!previous.allocated) {
                self.removeFree(previous_offset);
                previous.size += item.size;
                offset = previous_offset;
                item = previous;
            }
        }
        const next_offset = offset + item.size;
        if (next_offset < self.storage.len) {
            const next = self.block(next_offset);
            if (!next.allocated) {
                self.removeFree(next_offset);
                item.size += next.size;
            }
        }
        self.updateFollowing(offset);
        self.addFree(offset);
    }
};

test "workload admission completion scratch recycles interleaved streaming buffers under failed backing" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    const scratch = try RecyclingScratch.create(backing.allocator(), &manager, 64 * 1024);
    defer scratch.destroy() catch unreachable;
    const charged = manager.snapshot().memory.used_bytes;
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    const working = scratch.allocator();
    const pinned = try working.alignedAlloc(u8, .@"64", 513);
    @memset(pinned, 0xac);
    var streams: [8]?[]u8 = @splat(null);
    defer for (streams) |stream| {
        if (stream) |bytes| working.free(bytes);
    };
    for (0..2048) |i| {
        const index = (i * 5) % streams.len;
        if (streams[index]) |bytes| {
            try std.testing.expectEqual(@as(u8, @intCast(index)), bytes[0]);
            working.free(bytes);
        }
        streams[index] = try working.alloc(u8, 1024 + (i % 17) * 97);
        @memset(streams[index].?, @intCast(index));
    }
    try std.testing.expectError(error.CompletionReservationBusy, scratch.destroy());
    try std.testing.expectEqual(@as(u8, 0xac), pinned[512]);
    working.free(pinned);
    for (&streams) |*stream| if (stream.*) |bytes| {
        working.free(bytes);
        stream.* = null;
    };
    const coalesced = try working.alloc(u8, 60 * 1024);
    working.free(coalesced);
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
}

/// Single-borrow compiler workspace backed before consensus admission. Templates
/// must be copied to accepted-slot ownership before release. This component alone
/// is not a completion-capacity attestation. Keep its address stable while borrowed.
pub const CompilerWorkspace = struct {
    scratch: *RecyclingScratch,
    mutex: std.atomic.Mutex = .unlocked,
    generation: u64 = 0,
    borrowed: bool = false,
    completion_scope: bool = false,

    pub const Borrow = struct {
        workspace: *CompilerWorkspace,
        generation: u64,

        pub fn allocator(self: Borrow) !std.mem.Allocator {
            const owner = self.workspace;
            owner.lock();
            defer owner.mutex.unlock();
            if (!owner.borrowed or owner.completion_scope or owner.generation != self.generation) return error.CompletionReservationBusy;
            return owner.scratch.allocator();
        }

        /// Failure leaves this borrow active so the caller can free remaining
        /// buffers and retry. A copied old token cannot release a newer borrow.
        pub fn release(self: Borrow) !void {
            const owner = self.workspace;
            owner.lock();
            defer owner.mutex.unlock();
            if (!owner.borrowed or owner.completion_scope or owner.generation != self.generation) return error.CompletionReservationBusy;
            owner.scratch.lock();
            defer owner.scratch.mutex.unlock();
            if (owner.scratch.live != 0) return error.CompletionReservationBusy;
            owner.borrowed = false;
        }
    };

    pub fn init(backing: std.mem.Allocator, manager: *resources.ResourceManager, bytes: usize) !CompilerWorkspace {
        return .{ .scratch = try RecyclingScratch.create(backing, manager, bytes) };
    }

    fn lock(self: *CompilerWorkspace) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn tryBorrow(self: *CompilerWorkspace) !Borrow {
        self.lock();
        defer self.mutex.unlock();
        if (self.borrowed or self.generation == std.math.maxInt(u64)) return error.CompletionReservationBusy;
        self.generation += 1;
        self.borrowed = true;
        return .{ .workspace = self, .generation = self.generation };
    }

    /// Mandatory completion uses an exclusive lexical scope, not an escaping
    /// epoch token. It cannot exhaust the normal-admission counter, and stale
    /// normal tokens cannot allocate or release while this scope is active.
    /// The callback must free every scratch allocation before returning; owned
    /// results must be copied into their publication domain. A violated cleanup
    /// contract leaves the workspace unavailable instead of reusing live bytes.
    pub fn withCompletion(self: *CompilerWorkspace, comptime T: type, context: anytype, comptime callback: anytype) anyerror!T {
        self.lock();
        if (self.borrowed) {
            self.mutex.unlock();
            return error.CompletionReservationBusy;
        }
        self.borrowed = true;
        self.completion_scope = true;
        self.mutex.unlock();
        const result: anyerror!T = callback(context, self.scratch.allocator());
        self.lock();
        defer self.mutex.unlock();
        if (!self.scratch.isEmpty()) return error.CompletionReservationBusy;
        self.completion_scope = false;
        self.borrowed = false;
        return result;
    }

    /// Owner excludes further borrow calls before destroying the workspace.
    pub fn deinit(self: *CompilerWorkspace) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.borrowed) return error.CompletionReservationBusy;
        try self.scratch.destroy();
    }
};

test "workload admission completion compiler borrows retain preowned capacity and reject stale release" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    var workspace = try CompilerWorkspace.init(backing.allocator(), &manager, 64 * 1024);
    defer workspace.deinit() catch unreachable;
    const charged = manager.snapshot().memory.used_bytes;
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    const first = try workspace.tryBorrow();
    const allocator = try first.allocator();
    const bytes = try allocator.alloc(u8, 48 * 1024);
    @memset(bytes, 0x5a);
    try std.testing.expectError(error.CompletionReservationBusy, workspace.tryBorrow());
    try std.testing.expectError(error.CompletionReservationBusy, first.release());
    try std.testing.expectError(error.CompletionReservationBusy, workspace.deinit());
    try std.testing.expectEqual(@as(u8, 0x5a), bytes[bytes.len - 1]);
    allocator.free(bytes);
    try first.release();
    const second = try workspace.tryBorrow();
    try std.testing.expectError(error.CompletionReservationBusy, first.release());
    try std.testing.expectError(error.CompletionReservationBusy, first.allocator());
    const next_allocator = try second.allocator();
    const next = try next_allocator.alloc(u8, 60 * 1024);
    next_allocator.free(next);
    try second.release();
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
    try std.testing.expect(!backing.has_induced_failure);
}

test "workload admission completion recycling publication retains readers through owner retirement" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    const before = manager.snapshot().memory.used_bytes;
    const publication = try RecyclingScratch.create(backing.allocator(), &manager, 64 * 1024);
    const allocator = publication.allocator();
    const reader = try allocator.alloc(u8, 8192);
    @memset(reader, 0x42);
    const current = try allocator.alloc(u8, 8192);
    @memset(current, 0x19);
    const charged = manager.snapshot().memory.used_bytes;
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    // A live published buffer does not prevent recycling unrelated free spans.
    for (0..128) |_| {
        const replacement = try allocator.alloc(u8, 32 * 1024);
        allocator.free(replacement);
    }
    publication.retire();
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 1));
    allocator.free(current);
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u8, 0x42), reader[reader.len - 1]);
    allocator.free(reader);
    try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
    try std.testing.expect(!backing.has_induced_failure);
}

/// A concrete contiguous publication allowance carved out before admission.
/// Allocation partitions this owned span without consulting the free list.
/// Published children return individual spans, so copied persistent-tree nodes
/// do not pin an entire admission allowance. Single owner calls finish once.
/// Already reserved capacity remains usable after domain owner retirement;
/// retirement prevents creating new reservations, not consuming existing ones.
pub const PublicationReservation = struct {
    domain: *RecyclingScratch,
    remaining: usize,
    children: usize = 0,
    finished: bool = false,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn backingFootprint(bytes: usize) !usize {
        return std.math.add(usize, try RecyclingScratch.allocationFootprint(@sizeOf(PublicationReservation), @alignOf(PublicationReservation)), try RecyclingScratch.allocationFootprint(bytes, 1));
    }

    /// Conservative usable bytes still in this reservation's private tail.
    /// Free child allocations do not replenish this monotonic allowance.
    pub fn remainingBytes(self: *PublicationReservation) usize {
        self.lock();
        defer self.mutex.unlock();
        if (self.finished or self.remaining == RecyclingScratch.none) return 0;
        self.domain.lock();
        defer self.domain.mutex.unlock();
        return self.domain.block(self.remaining).size -| (RecyclingScratch.allocationFootprint(0, 1) catch unreachable);
    }

    pub fn create(domain: *RecyclingScratch, bytes: usize) !*PublicationReservation {
        if (bytes == 0) return error.ResourceBudgetExceeded;
        const domain_alloc = domain.allocator();
        const self = try domain_alloc.create(PublicationReservation);
        errdefer domain_alloc.destroy(self);
        const memory = try domain_alloc.alloc(u8, bytes);
        const offset = std.mem.bytesToValue(usize, (memory.ptr - @sizeOf(usize))[0..@sizeOf(usize)]);
        self.* = .{ .domain = domain, .remaining = offset };
        return self;
    }

    pub fn allocator(self: *PublicationReservation) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn lock(self: *PublicationReservation) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Stop allocation and return the unused tail. Children retain this allocator
    /// context through their physical free, including after domain retirement.
    pub fn finish(self: *PublicationReservation) void {
        self.lock();
        std.debug.assert(!self.finished);
        self.finished = true;
        if (self.remaining != RecyclingScratch.none) {
            // A reserved tail is a normal allocated domain block. Construct its
            // private free prefix; no caller has ever received these bytes.
            const offset = self.remaining;
            self.remaining = RecyclingScratch.none;
            const ptr = self.domain.storage.ptr + offset + @sizeOf(RecyclingScratch.Block) + @sizeOf(usize);
            @memcpy((ptr - @sizeOf(usize))[0..@sizeOf(usize)], std.mem.asBytes(&offset));
            self.domain.allocator().rawFree(ptr[0..1], .@"1", @returnAddress());
        }
        const destroy_context = self.children == 0;
        const domain = self.domain;
        self.mutex.unlock();
        if (destroy_context) domain.allocator().destroy(self);
    }

    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
        const self: *PublicationReservation = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.finished or self.remaining == RecyclingScratch.none) return null;
        const domain = self.domain;
        domain.lock();
        defer domain.mutex.unlock();
        const offset = self.remaining;
        const item = domain.block(offset);
        std.debug.assert(item.allocated);
        const base = @intFromPtr(domain.storage.ptr);
        const unaligned = base + offset + @sizeOf(RecyclingScratch.Block) + @sizeOf(usize);
        const rounded = std.math.add(usize, unaligned, alignment.toByteUnits() - 1) catch return null;
        const address = std.mem.alignBackward(usize, rounded, alignment.toByteUnits());
        const end = std.math.add(usize, address - base, @max(len, 1)) catch return null;
        const rounded_end = std.math.add(usize, end, RecyclingScratch.block_alignment - 1) catch return null;
        const needed = std.mem.alignBackward(usize, rounded_end, RecyclingScratch.block_alignment) - offset;
        if (needed > item.size) return null;
        const previous_size = item.size;
        if (previous_size - needed >= RecyclingScratch.minimum_block) {
            item.size = needed;
            const next = offset + needed;
            domain.block(next).* = .{ .size = previous_size - needed, .previous_size = needed, .allocated = true };
            domain.updateFollowing(next);
            self.remaining = next;
            domain.live += 1;
        } else self.remaining = RecyclingScratch.none;
        const result: [*]u8 = @ptrFromInt(address);
        @memcpy((result - @sizeOf(usize))[0..@sizeOf(usize)], std.mem.asBytes(&offset));
        self.children += 1;
        return result;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PublicationReservation = @ptrCast(@alignCast(raw));
        self.lock();
        std.debug.assert(self.children != 0);
        self.children -= 1;
        const domain = self.domain;
        domain.allocator().rawFree(memory, alignment, ret_addr);
        const destroy_context = self.finished and self.children == 0;
        self.mutex.unlock();
        if (destroy_context) domain.allocator().destroy(self);
    }
};

test "workload admission completion publication reservation isolates capacity and frees individual retained spans" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    const domain = try RecyclingScratch.create(backing.allocator(), &manager, 256 * 1024);
    const first = try PublicationReservation.create(domain, 96 * 1024);
    const second = try PublicationReservation.create(domain, 96 * 1024);
    try std.testing.expectError(error.OutOfMemory, PublicationReservation.create(domain, 96 * 1024));
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    const a = first.allocator();
    const b = second.allocator();
    const retained = try a.alignedAlloc(u8, .@"64", 32 * 1024);
    @memset(retained, 0x5a);
    const transient = try a.alloc(u8, 32 * 1024);
    a.free(transient);
    first.finish();
    // Most of the first reservation is reusable while its reader stays live.
    const third = try PublicationReservation.create(domain, 48 * 1024);
    const independent = try b.alloc(u8, 80 * 1024);
    @memset(independent, 0x19);
    const another = try third.allocator().alloc(u8, 32 * 1024);
    third.allocator().free(another);
    third.finish();
    second.finish();
    const charged = manager.snapshot().memory.used_bytes;
    domain.retire();
    b.free(independent);
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u8, 0x5a), retained[retained.len - 1]);
    a.free(retained);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expect(!backing.has_induced_failure);
}

test "workload admission completion preowned publication remains usable after domain owner retirement" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    const domain = try RecyclingScratch.create(alloc, &manager, 64 * 1024);
    const reserved = try PublicationReservation.create(domain, 48 * 1024);
    const allocator = reserved.allocator();
    domain.retire();
    // Owner close prevents new reservations, not consumption of ownership that
    // was physically retained before close. The reservation pins its context.
    const completion = try allocator.alloc(u8, 32 * 1024);
    @memset(completion, 0x41);
    reserved.finish();
    try std.testing.expectEqual(@as(u8, 0x41), completion[completion.len - 1]);
    allocator.free(completion);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "workload admission completion compiler critical scopes survive exhausted epochs and reject stale or nested borrowing" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var workspace = try CompilerWorkspace.init(alloc, &manager, 64 * 1024);
    defer workspace.deinit() catch unreachable;
    workspace.generation = std.math.maxInt(u64) - 1;
    const last_normal = try workspace.tryBorrow();
    const Context = struct {
        owner: *CompilerWorkspace,
        stale: CompilerWorkspace.Borrow,
        fail: bool = false,
        fn empty(_: void, _: std.mem.Allocator) !void {}
        fn run(context: @This(), scratch: std.mem.Allocator) !usize {
            try std.testing.expectError(error.CompletionReservationBusy, context.stale.allocator());
            try std.testing.expectError(error.CompletionReservationBusy, context.stale.release());
            try std.testing.expectError(error.CompletionReservationBusy, context.owner.tryBorrow());
            try std.testing.expectError(error.CompletionReservationBusy, context.owner.withCompletion(void, {}, empty));
            const bytes = try scratch.alloc(u8, 32768);
            defer scratch.free(bytes);
            @memset(bytes, 0x73);
            if (context.fail) return error.InjectedCompletionFailure;
            return bytes.len;
        }
    };
    try std.testing.expectError(error.CompletionReservationBusy, workspace.withCompletion(void, {}, Context.empty));
    try last_normal.release();
    try std.testing.expectError(error.CompletionReservationBusy, workspace.tryBorrow());
    try std.testing.expectError(error.InjectedCompletionFailure, workspace.withCompletion(usize, Context{ .owner = &workspace, .stale = last_normal, .fail = true }, Context.run));
    try std.testing.expect(workspace.scratch.isEmpty());
    for (0..32) |_| {
        try std.testing.expectEqual(@as(usize, 32768), try workspace.withCompletion(usize, Context{ .owner = &workspace, .stale = last_normal }, Context.run));
        try std.testing.expect(workspace.scratch.isEmpty());
    }
    try std.testing.expectEqual(std.math.maxInt(u64), workspace.generation);
    try std.testing.expectError(error.CompletionReservationBusy, workspace.tryBorrow());
}
