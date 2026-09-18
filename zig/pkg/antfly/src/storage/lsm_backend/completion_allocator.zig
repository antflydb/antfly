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
    return allocator.vtable == &Owner.vtable;
}
