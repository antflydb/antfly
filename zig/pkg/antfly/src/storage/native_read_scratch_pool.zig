//! Bounded, reclaimable native read arenas. Leases own only temporary bytes,
//! never generation pointers; callers join all I/O before returning a lease.
const std = @import("std");
const resources = @import("resource_manager.zig");

pub const Pool = struct {
    const max_slots = 32;
    const max_slot_bytes = 4 * 1024 * 1024;
    const max_idle_bytes = 64 * 1024 * 1024;

    pub const Slot = struct {
        budget: resources.BudgetedAllocator,
        arena: std.heap.ArenaAllocator,

        pub fn allocator(self: *Slot) std.mem.Allocator {
            return self.arena.allocator();
        }

        pub fn allocationError(self: *const Slot, err: anyerror) anyerror {
            return if (err == error.OutOfMemory and self.budget.denied()) error.ResourceBudgetExceeded else err;
        }
    };

    alloc: std.mem.Allocator,
    manager: *resources.ResourceManager,
    identity: u64 = 0,
    mutex: std.atomic.Mutex = .unlocked,
    idle: [max_slots]*Slot = undefined,
    idle_count: usize = 0,
    idle_bytes: u64 = 0,
    active: std.atomic.Value(usize) = .init(0),

    pub fn create(alloc: std.mem.Allocator, manager: *resources.ResourceManager) !*Pool {
        const self = try alloc.create(Pool);
        errdefer alloc.destroy(self);
        self.* = .{ .alloc = alloc, .manager = manager };
        self.identity = try manager.registerReclaimer(.dense_search_working_set, self, reclaimCallback);
        return self;
    }

    pub fn destroy(self: *Pool) void {
        self.manager.unregisterReclaimer(self.identity);
        std.debug.assert(self.active.load(.acquire) == 0);
        _ = self.reclaim(std.math.maxInt(u64));
        self.alloc.destroy(self);
    }

    fn lock(self: *Pool) void {
        // Only pointer/count operations happen under this short lock. Never
        // allocate, release reservations, reset arenas, or perform I/O here.
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn acquire(self: *Pool) !*Slot {
        _ = self.active.fetchAdd(1, .monotonic);
        errdefer _ = self.active.fetchSub(1, .release);
        self.lock();
        if (self.idle_count != 0) {
            self.idle_count -= 1;
            const slot = self.idle[self.idle_count];
            self.idle_bytes -= slot.budget.reservation.bytes;
            self.mutex.unlock();
            slot.budget.budget_denied = false;
            return slot;
        }
        self.mutex.unlock();
        const slot = try self.alloc.create(Slot);
        // Stable allocation: arena allocator handles point to the slot's
        // budget, not a query-stack value that can be moved or destroyed.
        slot.budget = resources.BudgetedAllocator.init(self.manager, .dense_search_working_set, std.heap.page_allocator, 1);
        slot.arena = std.heap.ArenaAllocator.init(slot.budget.allocator());
        return slot;
    }

    pub fn release(self: *Pool, slot: *Slot) void {
        defer _ = self.active.fetchSub(1, .release);
        // Oversized requests are serviceable under admission but may not
        // permanently inflate the ordinary-query cache. Check before reset,
        // which can otherwise consolidate a large arena needlessly.
        if (slot.budget.reservation.bytes > max_slot_bytes) {
            self.destroySlot(slot);
            return;
        }
        _ = slot.arena.reset(.retain_capacity);
        const bytes = slot.budget.reservation.bytes;
        self.lock();
        const keep = bytes <= max_slot_bytes and self.idle_count < max_slots and
            bytes <= max_idle_bytes -| self.idle_bytes;
        if (keep) {
            self.idle[self.idle_count] = slot;
            self.idle_count += 1;
            self.idle_bytes += bytes;
        }
        self.mutex.unlock();
        if (!keep) self.destroySlot(slot);
    }

    fn destroySlot(self: *Pool, slot: *Slot) void {
        slot.arena.deinit();
        slot.budget.deinit();
        self.alloc.destroy(slot);
    }

    fn reclaimCallback(ctx: *anyopaque, target: u64) u64 {
        const self: *Pool = @ptrCast(@alignCast(ctx));
        return self.reclaim(target);
    }

    pub fn reclaim(self: *Pool, target: u64) u64 {
        var freed: u64 = 0;
        while (freed < target) {
            self.lock();
            if (self.idle_count == 0) {
                self.mutex.unlock();
                break;
            }
            self.idle_count -= 1;
            const slot = self.idle[self.idle_count];
            const bytes = slot.budget.reservation.bytes;
            self.idle_bytes -= bytes;
            self.mutex.unlock();
            self.destroySlot(slot);
            freed +|= bytes;
        }
        return freed;
    }
};
