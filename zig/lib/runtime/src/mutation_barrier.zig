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
const builtin = @import("builtin");
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);

threadlocal var shared_barrier: ?*MutationBarrier = null;
threadlocal var shared_depth: usize = 0;

/// Process-wide mutation/capture barrier.
///
/// Every primary-side persistent mutation holds a shared lease from before its
/// first durable side effect until its matching replication record is complete. The
/// mutation lease is released before waiting for remote durability: the local
/// commit and replication tail are already atomically ordered at that point, and holding
/// it would deadlock the seed capture needed to restore that durability. Seed
/// capture holds the exclusive lease while it selects the exact checkpoint and
/// snapshots every catalog/data store. The reader gate is held by a queued
/// exclusive locker, preventing an unbounded stream of new writes from starving
/// capture.
pub const MutationBarrier = struct {
    reader_gate: std.atomic.Mutex = .unlocked,
    reader_mutex: std.atomic.Mutex = .unlocked,
    resource_mutex: std.atomic.Mutex = .unlocked,
    reader_count: usize = 0,
    shared_waiters: @import("antfly_platform").atomic.Value(u64) = .init(0),
    exclusive_waiters: @import("antfly_platform").atomic.Value(u64) = .init(0),

    pub const SharedLease = struct {
        barrier: *MutationBarrier,
        nested: bool = false,
        root: ?*RootSharedLease = null,
        /// This guard opened the local physical phase and retires it after all
        /// nested borrowers have physically joined and released.
        phase_owner: bool = false,

        pub fn release(self: *@This()) void {
            if (self.root) |root| {
                std.debug.assert(root.barrier == self.barrier);
                const previous = root.borrowers.fetchSub(1, .acq_rel);
                std.debug.assert(previous > 0);
                if (self.phase_owner) {
                    std.debug.assert(previous == 1);
                    root.retirePhase() catch unreachable;
                }
                self.* = undefined;
                return;
            }
            std.debug.assert(shared_barrier == self.barrier);
            std.debug.assert(shared_depth > 0);
            shared_depth -= 1;
            if (!self.nested) {
                std.debug.assert(shared_depth == 0);
                shared_barrier = null;
                self.barrier.unlockShared();
            }
            self.* = undefined;
        }
    };

    /// One independently owned root. Its physical reader is not thread-local
    /// nesting state. Nested consumers must borrow this root explicitly and
    /// physically join before release; they must not reacquire behind a writer.
    pub const RootSharedLease = struct {
        barrier: *MutationBarrier,
        borrowers: std.atomic.Value(usize) = .init(0),
        /// Phase transitions belong to the enclosing owner, after physical
        /// borrower joins. They must not race new borrow attempts.
        held: bool = true,

        /// Stable invocation identity without a physical reader. The enclosing
        /// owner serializes phase entry and joins borrowers before retirement.
        pub fn initRetired(barrier: *MutationBarrier) @This() {
            return .{ .barrier = barrier, .held = false };
        }

        /// Open a local phase if necessary. Borrowing an already-held caller
        /// phase never transfers ownership of its physical reader.
        pub fn borrowPhaseControlled(self: *@This(), io: std.Io, controls: anytype) !SharedLease {
            const opened = !self.held;
            if (opened) try self.reenterPhaseControlled(io, controls);
            errdefer if (opened) self.retirePhase() catch unreachable;
            var lease = try self.borrowControlled(controls);
            lease.phase_owner = opened;
            return lease;
        }

        /// Only before a decision: a non-lending caller must not occupy the
        /// resume capacity of a capture that has closed reader admission.
        /// An already-held root borrows without reacquiring behind that capture.
        pub fn borrowPhasePreDecisionControlled(self: *@This(), io: std.Io, controls: anytype) !SharedLease {
            const opened = !self.held;
            if (opened) {
                if (self.borrowers.load(.acquire) != 0) return error.InvalidRunnableTransition;
                self.* = try self.barrier.acquireSharedRootPreDecisionControlled(io, controls);
            }
            errdefer if (opened) self.retirePhase() catch unreachable;
            var lease = try self.borrowControlled(controls);
            lease.phase_owner = opened;
            return lease;
        }

        /// Recovery may yield with its durable obligation intact. Open one
        /// phase without waiting for capture; an already-held root only borrows
        /// its reader and cannot get stuck behind a newly waiting writer.
        pub fn tryBorrowPhase(self: *@This()) !?SharedLease {
            const opened = !self.held;
            if (opened) {
                if (self.borrowers.load(.acquire) != 0) return error.InvalidRunnableTransition;
                if (!self.barrier.tryLockShared()) return null;
                self.held = true;
            }
            var lease = self.borrow();
            lease.phase_owner = opened;
            return lease;
        }

        /// Mandatory legacy helpers retain their existing noncancelable wait.
        /// A retired root enters independently of unrelated thread-local leases.
        pub fn borrowPhase(self: *@This()) SharedLease {
            const opened = !self.held;
            if (opened) {
                std.debug.assert(self.borrowers.load(.acquire) == 0);
                self.barrier.lockShared();
                self.held = true;
            }
            var lease = self.borrow();
            lease.phase_owner = opened;
            return lease;
        }

        /// The root must stay at a stable address until every borrower has
        /// physically joined and released. Borrowing never enters reader_gate.
        pub fn borrow(self: *@This()) SharedLease {
            std.debug.assert(self.held);
            _ = self.borrowers.fetchAdd(1, .acq_rel);
            return .{ .barrier = self.barrier, .nested = true, .root = self };
        }

        pub fn borrowControlled(self: *@This(), controls: anytype) !SharedLease {
            try controls.checkpoint();
            if (!self.held) return error.InvalidRunnableTransition;
            var lease = self.borrow();
            errdefer lease.release();
            try controls.checkpoint();
            return lease;
        }

        /// Retire only the physical reader, preserving this stable address and
        /// barrier identity for later local work. The owner must first join all
        /// physical borrowers; a zero count alone does not prove those joins.
        /// Mandatory retirement never checks cancellation or mints a deadline.
        pub fn retirePhase(self: *@This()) !void {
            if (self.borrowers.load(.acquire) != 0) return error.InvalidRunnableTransition;
            if (!self.held) return;
            self.barrier.unlockShared();
            self.held = false;
        }

        /// Reentry uses the caller's original controls and writer priority.
        /// Failure leaves the root retired and usable for final release.
        /// Resume execution authority before attempting a new physical phase.
        pub fn reenterPhaseControlled(self: *@This(), io: std.Io, controls: anytype) !void {
            if (self.held or self.borrowers.load(.acquire) != 0) return error.InvalidRunnableTransition;
            const fresh = try self.barrier.acquireSharedRootControlled(io, controls);
            self.* = fresh;
        }

        pub fn release(self: *@This()) void {
            std.debug.assert(self.borrowers.load(.acquire) == 0);
            if (self.held) self.barrier.unlockShared();
            self.* = undefined;
        }
    };

    /// Fresh-operation acquisition. This never borrows another operation's
    /// same-thread legacy marker. Original controls outlive acquisition.
    pub fn acquireSharedRootControlled(self: *@This(), io: std.Io, controls: anytype) !RootSharedLease {
        try controls.checkpoint();
        _ = self.shared_waiters.fetchAdd(1, .acq_rel);
        defer _ = self.shared_waiters.fetchSub(1, .acq_rel);
        while (!self.tryLockShared()) try waitControlled(io, controls);
        var lease: RootSharedLease = .{ .barrier = self };
        errdefer lease.release();
        try controls.checkpoint();
        return lease;
    }

    /// Pre-decision acquisition preserves the caller's original scope. No
    /// physical reader or thread-local nesting authority survives rejection.
    pub fn acquireSharedRootPreDecisionControlled(self: *@This(), io: std.Io, controls: anytype) !RootSharedLease {
        if (controls.executionBoundary() != null) return self.acquireSharedRootControlled(io, controls);
        try controls.checkpoint();
        if (!self.tryLockShared()) {
            try controls.checkpoint();
            return error.Backpressured;
        }
        var lease: RootSharedLease = .{ .barrier = self };
        errdefer lease.release();
        try controls.checkpoint();
        return lease;
    }

    pub const ExclusiveLease = struct {
        barrier: *MutationBarrier,

        pub fn release(self: *@This()) void {
            self.barrier.unlockExclusive();
            self.* = undefined;
        }
    };

    pub fn acquireShared(self: *@This()) SharedLease {
        if (shared_barrier) |held| {
            std.debug.assert(held == self);
            shared_depth += 1;
            return .{ .barrier = self, .nested = true };
        }
        self.lockShared();
        shared_barrier = self;
        shared_depth = 1;
        return .{ .barrier = self };
    }

    /// Original non-yielding controls provide checkpoint() and an optional
    /// executionBoundary(). Keep their owner alive through physical acquisition.
    pub fn acquireSharedControlled(self: *@This(), io: std.Io, controls: anytype) !SharedLease {
        try controls.checkpoint();
        if (shared_barrier) |held| {
            if (held != self) return error.InvalidArgument;
            shared_depth += 1;
            var lease: SharedLease = .{ .barrier = self, .nested = true };
            errdefer lease.release();
            try controls.checkpoint();
            return lease;
        }
        _ = self.shared_waiters.fetchAdd(1, .acq_rel);
        defer _ = self.shared_waiters.fetchSub(1, .acq_rel);
        while (!self.tryLockShared()) try waitControlled(io, controls);
        shared_barrier = self;
        shared_depth = 1;
        var lease: SharedLease = .{ .barrier = self };
        errdefer lease.release();
        try controls.checkpoint();
        return lease;
    }

    /// Compatibility nesting still belongs to the caller's existing legacy
    /// operation; fresh explicit roots must use the root acquisition above.
    pub fn acquireSharedPreDecisionControlled(self: *@This(), io: std.Io, controls: anytype) !SharedLease {
        if (controls.executionBoundary() != null) return self.acquireSharedControlled(io, controls);
        try controls.checkpoint();
        if (shared_barrier) |held| if (held != self) return error.InvalidArgument;
        var lease = self.tryAcquireShared() orelse {
            try controls.checkpoint();
            return error.Backpressured;
        };
        errdefer lease.release();
        try controls.checkpoint();
        return lease;
    }

    /// Close reader admission before waiting for existing mutations. The
    /// writer intent survives CPU parking, but every rejection releases it.
    pub fn acquireExclusiveControlled(self: *@This(), io: std.Io, controls: anytype) !ExclusiveLease {
        try controls.checkpoint();
        if (shared_barrier == self) return error.InvalidArgument;
        return self.acquireExclusiveRootControlled(io, controls);
    }

    /// A fresh operation owns no mutation lease of its own. Another Io
    /// operation on the same OS thread may own a legacy shared lease; its
    /// thread-local marker is not this capture's nesting authority. Callers
    /// must preserve the root identity and all controls through acquisition.
    pub fn acquireExclusiveRootControlled(self: *@This(), io: std.Io, controls: anytype) !ExclusiveLease {
        try controls.checkpoint();
        _ = self.exclusive_waiters.fetchAdd(1, .acq_rel);
        defer _ = self.exclusive_waiters.fetchSub(1, .acq_rel);
        while (!self.reader_gate.tryLock()) try waitControlled(io, controls);
        errdefer self.reader_gate.unlock();
        while (!self.resource_mutex.tryLock()) try waitControlled(io, controls);
        errdefer self.resource_mutex.unlock();
        try controls.checkpoint();
        return .{ .barrier = self };
    }

    fn waitControlled(io: std.Io, controls: anytype) !void {
        try controls.checkpoint();
        const boundary = controls.executionBoundary();
        if (boundary) |wait| try wait.beginWait();
        // Finite owner-Io polling avoids a CPU spin without introducing a
        // second clock/deadline. Reentry is mandatory even after I/O rejection.
        const result = io.sleep(.fromMilliseconds(1), .awake);
        if (boundary) |wait| try wait.endWait();
        try result;
        try controls.checkpoint();
    }

    pub fn tryAcquireShared(self: *@This()) ?SharedLease {
        if (shared_barrier) |held| {
            if (held != self) return null;
            shared_depth += 1;
            return .{ .barrier = self, .nested = true };
        }
        if (!self.tryLockShared()) return null;
        shared_barrier = self;
        shared_depth = 1;
        return .{ .barrier = self };
    }

    pub fn acquireExclusive(self: *@This()) ExclusiveLease {
        self.lockExclusive();
        return .{ .barrier = self };
    }

    pub fn tryAcquireExclusive(self: *@This()) ?ExclusiveLease {
        if (!self.tryLockExclusive()) return null;
        return .{ .barrier = self };
    }

    pub fn pendingSharedAcquisitions(self: *const @This()) u64 {
        return self.shared_waiters.load(.acquire);
    }

    pub fn pendingExclusiveAcquisitions(self: *const @This()) u64 {
        return self.exclusive_waiters.load(.acquire);
    }

    fn lockShared(self: *@This()) void {
        _ = self.shared_waiters.fetchAdd(1, .acq_rel);
        defer _ = self.shared_waiters.fetchSub(1, .acq_rel);
        lockAtomic(&self.reader_gate);
        defer self.reader_gate.unlock();

        lockAtomic(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        self.reader_count += 1;
        if (self.reader_count == 1) lockAtomic(&self.resource_mutex);
    }

    fn tryLockShared(self: *@This()) bool {
        if (!self.reader_gate.tryLock()) return false;
        defer self.reader_gate.unlock();

        if (!self.reader_mutex.tryLock()) return false;
        defer self.reader_mutex.unlock();

        if (self.reader_count == 0 and !self.resource_mutex.tryLock()) return false;
        self.reader_count += 1;
        return true;
    }

    fn unlockShared(self: *@This()) void {
        lockAtomic(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        std.debug.assert(self.reader_count > 0);
        self.reader_count -= 1;
        if (self.reader_count == 0) self.resource_mutex.unlock();
    }

    fn lockExclusive(self: *@This()) void {
        _ = self.exclusive_waiters.fetchAdd(1, .acq_rel);
        defer _ = self.exclusive_waiters.fetchSub(1, .acq_rel);
        lockAtomic(&self.reader_gate);
        lockAtomic(&self.resource_mutex);
    }

    fn tryLockExclusive(self: *@This()) bool {
        if (!self.reader_gate.tryLock()) return false;
        if (!self.resource_mutex.tryLock()) {
            self.reader_gate.unlock();
            return false;
        }
        return true;
    }

    fn unlockExclusive(self: *@This()) void {
        self.resource_mutex.unlock();
        self.reader_gate.unlock();
        if (builtin.os.tag != .freestanding and !builtin.single_threaded) {
            @import("antfly_platform").time.yieldNow();
        }
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    @import("antfly_platform").sync.lockYielding(mutex);
}

test "storage.hot_standby mutation barrier excludes capture while a mutation lease is held" {
    var barrier: MutationBarrier = .{};
    var mutation = barrier.acquireShared();
    defer mutation.release();

    try std.testing.expect(barrier.tryAcquireExclusive() == null);
}

test "storage.hot_standby mutation barrier excludes new mutations while capture is held" {
    var barrier: MutationBarrier = .{};
    var capture = barrier.acquireExclusive();
    defer capture.release();

    try std.testing.expect(barrier.tryAcquireShared() == null);
}

test "storage.hot_standby mutation barrier recovers after failed try acquisition" {
    var barrier: MutationBarrier = .{};

    var mutation = barrier.acquireShared();
    try std.testing.expect(barrier.tryAcquireExclusive() == null);
    mutation.release();

    var capture = barrier.tryAcquireExclusive() orelse return error.TestExpectedEqual;
    capture.release();

    var next_mutation = barrier.tryAcquireShared() orelse return error.TestExpectedEqual;
    next_mutation.release();
}
