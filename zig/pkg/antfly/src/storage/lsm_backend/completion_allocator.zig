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
    return allocator.vtable == &Owner.vtable or allocator.vtable == &Arena.vtable;
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
