// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Returned pages and running pulls borrow the stream. Fence their physical
//! release before native teardown, and retain the callback capsule through it.
const std = @import("std");

pub const Lifetime = struct {
    context: *anyopaque,
    retire: *const fn (*anyopaque) void,
    destroy: *const fn (*anyopaque) void,
    references: std.atomic.Value(usize) = .init(1),
    detached: std.atomic.Value(bool) = .init(false),
    mutex: std.atomic.Mutex = .unlocked,
    borrowers: usize = 0,
    retirement_started: bool = false,

    fn lock(self: *Lifetime) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Additional supervisor or callback pins must be acquired while an
    /// existing reference still owns this stable capsule.
    pub fn retain(self: *Lifetime) void {
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
    }

    pub fn release(self: *Lifetime) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous == 1) self.destroy(self.context);
        // The final callback can destroy this address. No accesses follow it.
    }

    /// Acquire before starting a pull or publishing a page. Admission is
    /// serialized with detach; rejected publication must physically free its
    /// unreturned page while the running pull still owns its resource borrow.
    pub fn retainResource(self: *Lifetime) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.detached.load(.acquire)) return error.InvalidRunnableTransition;
        self.retain();
        self.borrowers += 1;
    }

    /// Call only AFTER a page arena/wrapper physically frees or a pull future
    /// physically joins.
    /// This page's reference fences the retirement callback, even if the
    /// foreground close has already dropped its last reference.
    pub fn releaseResource(self: *Lifetime) void {
        self.lock();
        std.debug.assert(self.borrowers != 0);
        self.borrowers -= 1;
        const claimed = self.claimRetirement();
        self.mutex.unlock();
        if (claimed) self.retire(self.context);
        self.release();
    }

    fn claimRetirement(self: *Lifetime) bool {
        if (!self.detached.load(.acquire) or self.borrowers != 0 or self.retirement_started) return false;
        self.retirement_started = true;
        return true;
    }

    /// Logical detach closes page publication. Actual native retirement waits
    /// for the last physical page release and runs exactly once. The callback
    /// runs outside the metadata lock; its caller still owns a reference.
    pub fn detach(self: *Lifetime) void {
        self.lock();
        self.detached.store(true, .release);
        const claimed = self.claimRetirement();
        self.mutex.unlock();
        if (claimed) self.retire(self.context);
    }

    /// Consume the one foreground reference. Returned pages and any reserved
    /// supervisor pins continue to own the callback and allocator parents.
    pub fn close(self: *Lifetime) void {
        self.detach();
        self.release();
    }
};
