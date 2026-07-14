// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");

/// Process-wide HA capture barrier.
///
/// Every primary-side persistent mutation holds a shared lease from before its
/// first durable side effect until its HA WAL record and durability decision are
/// complete. Seed capture holds the exclusive lease while it selects the exact
/// checkpoint and snapshots every catalog/data store. The reader gate is held
/// by a queued exclusive locker, preventing an unbounded stream of new writes
/// from starving capture.
pub const MutationBarrier = struct {
    reader_gate: std.atomic.Mutex = .unlocked,
    reader_mutex: std.atomic.Mutex = .unlocked,
    resource_mutex: std.atomic.Mutex = .unlocked,
    reader_count: usize = 0,
    shared_waiters: std.atomic.Value(u64) = .init(0),

    pub const SharedLease = struct {
        barrier: *MutationBarrier,

        pub fn release(self: *@This()) void {
            self.barrier.unlockShared();
            self.* = undefined;
        }
    };

    pub const ExclusiveLease = struct {
        barrier: *MutationBarrier,

        pub fn release(self: *@This()) void {
            self.barrier.unlockExclusive();
            self.* = undefined;
        }
    };

    pub fn acquireShared(self: *@This()) SharedLease {
        self.lockShared();
        return .{ .barrier = self };
    }

    pub fn tryAcquireShared(self: *@This()) ?SharedLease {
        if (!self.tryLockShared()) return null;
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
            std.Thread.yield() catch {};
        }
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    var attempts: usize = 0;
    while (!mutex.tryLock()) : (attempts += 1) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded or attempts < 64) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
        }
    }
}

test "storage.ha mutation barrier excludes capture while a mutation lease is held" {
    var barrier: MutationBarrier = .{};
    var mutation = barrier.acquireShared();
    defer mutation.release();

    try std.testing.expect(barrier.tryAcquireExclusive() == null);
}

test "storage.ha mutation barrier excludes new mutations while capture is held" {
    var barrier: MutationBarrier = .{};
    var capture = barrier.acquireExclusive();
    defer capture.release();

    try std.testing.expect(barrier.tryAcquireShared() == null);
}

test "storage.ha mutation barrier recovers after failed try acquisition" {
    var barrier: MutationBarrier = .{};

    var mutation = barrier.acquireShared();
    try std.testing.expect(barrier.tryAcquireExclusive() == null);
    mutation.release();

    var capture = barrier.tryAcquireExclusive() orelse return error.TestExpectedEqual;
    capture.release();

    var next_mutation = barrier.tryAcquireShared() orelse return error.TestExpectedEqual;
    next_mutation.release();
}
