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
const BroadcastCondition = @import("antfly_platform").sync.BroadcastCondition;

/// Allocation-free FIFO admission. A timed-out waiter unlinks its stack node
/// before returning, so neither an abandoned ticket nor a borrowed pointer can
/// outlive the call. Every node and condition epoch is protected by `state`.
pub fn QueuedMutex(comptime defaultIo: fn () std.Io) type {
    return QueuedMutexWithLease(defaultIo, void);
}

pub fn QueuedMutexWithLease(comptime defaultIo: fn () std.Io, comptime Lease: type) type {
    return struct {
        const Self = @This();
        const Waiter = struct { previous: ?*Waiter = null, next: ?*Waiter = null, wants_lease: bool = false };

        state: std.Io.Mutex = .init,
        ready: BroadcastCondition = .init,
        held: bool = false,
        head: ?*Waiter = null,
        tail: ?*Waiter = null,
        retained: ?Lease = null,
        // Enrollment/completion counters also expose the number of live
        // admissions: next -% serving == held + queued. Cancellation completes
        // an enrollment, without changing the ordering of remaining nodes.
        next: u64 = 0,
        serving: u64 = 0,

        pub const retain_quantum_ns: i96 = 50 * std.time.ns_per_ms;
        pub const contended_quantum_ns: i96 = 20 * std.time.ns_per_ms;

        pub fn retainForWaiter(self: *Self, io: std.Io, lease: *Lease) bool {
            if (self.retained != null) return false;
            const age = std.Io.Clock.awake.now(io).nanoseconds - lease.acquired_ns;
            if (age >= retain_quantum_ns) return false;
            self.state.lockUncancelable(io);
            const next_wants_lease = if (self.head) |head| head.wants_lease else false;
            self.state.unlock(io);
            if (!next_wants_lease) return false;
            if (age >= contended_quantum_ns and lease.othersWaiting()) return false;
            if (lease.reader_registration_batch) |batch| batch.end();
            lease.reader_registration_batch = null;
            lease.endHold();
            self.retained = lease.*;
            return true;
        }

        pub fn takeRetained(self: *Self) ?Lease {
            const lease = self.retained orelse return null;
            self.retained = null;
            return lease;
        }

        pub fn dropRetained(self: *Self) void {
            if (self.takeRetained()) |lease| {
                var owned = lease;
                owned.close();
            }
        }

        fn append(self: *Self, waiter: *Waiter) void {
            waiter.previous = self.tail;
            if (self.tail) |tail| tail.next = waiter else self.head = waiter;
            self.tail = waiter;
        }

        fn remove(self: *Self, waiter: *Waiter) void {
            if (waiter.previous) |previous| previous.next = waiter.next else self.head = waiter.next;
            if (waiter.next) |next| next.previous = waiter.previous else self.tail = waiter.previous;
        }

        pub fn lockUncancelable(self: *Self, io: std.Io) void {
            self.lockUncancelableWithIntent(io, false);
        }

        pub fn lockForLease(self: *Self, io: std.Io) void {
            self.lockUncancelableWithIntent(io, true);
        }

        fn lockUncancelableWithIntent(self: *Self, io: std.Io, wants_lease: bool) void {
            self.state.lockUncancelable(io);
            defer self.state.unlock(io);
            self.next +%= 1;
            if (!self.held and self.head == null) {
                self.held = true;
                return;
            }
            var waiter: Waiter = .{ .wants_lease = wants_lease };
            self.append(&waiter);
            while (self.held or self.head != &waiter) self.ready.waitUncancelable(io, &self.state);
            self.remove(&waiter);
            self.held = true;
        }

        /// The first uncontended attempt remains legal after a slow proof.
        /// Once queued, expiry or cancellation removes this admission rather
        /// than leaving a hole for the next holder to discover.
        pub fn lockUntil(self: *Self, io: std.Io, deadline_ns: i96) !void {
            return self.lockUntilWithIntent(io, deadline_ns, false);
        }

        pub fn lockForLeaseUntil(self: *Self, io: std.Io, deadline_ns: i96) !void {
            return self.lockUntilWithIntent(io, deadline_ns, true);
        }

        fn lockUntilWithIntent(self: *Self, io: std.Io, deadline_ns: i96, wants_lease: bool) !void {
            self.state.lockUncancelable(io);
            defer self.state.unlock(io);
            try io.checkCancel();
            self.next +%= 1;
            if (!self.held and self.head == null) {
                self.held = true;
                return;
            }
            var waiter: Waiter = .{ .wants_lease = wants_lease };
            self.append(&waiter);
            errdefer {
                self.remove(&waiter);
                self.serving +%= 1;
                self.ready.broadcast(io);
            }
            while (true) {
                try io.checkCancel();
                const remaining = deadline_ns - std.Io.Clock.awake.now(io).nanoseconds;
                if (remaining <= 0) return error.FileBusy;
                if (!self.held and self.head == &waiter) {
                    self.remove(&waiter);
                    self.held = true;
                    return;
                }
                // The futex may return for either expiry or a spurious wake;
                // the loop rechecks both eligibility and the absolute budget.
                try self.waitTimeout(io, remaining);
            }
        }

        fn waitTimeout(self: *Self, io: std.Io, remaining_ns: i96) !void {
            self.ready.waiters += 1;
            const observed = self.ready.epoch.load(.acquire);
            self.state.unlock(io);
            defer {
                self.state.lockUncancelable(io);
                self.ready.waiters -= 1;
            }
            // Reading the epoch before releasing state preserves the same
            // lost-wake protection as BroadcastCondition.waitUncancelable.
            try io.futexWaitTimeout(u32, &self.ready.epoch.raw, observed, .{
                .duration = .{ .raw = .fromNanoseconds(remaining_ns), .clock = .awake },
            });
        }

        pub fn tryLock(self: *Self) bool {
            if (!self.state.tryLock()) return false;
            defer self.state.unlock(defaultIo());
            if (self.held or self.head != null) return false;
            self.next +%= 1;
            self.held = true;
            return true;
        }

        pub fn unlock(self: *Self, io: std.Io) void {
            self.state.lockUncancelable(io);
            defer self.state.unlock(io);
            std.debug.assert(self.held);
            self.held = false;
            self.serving +%= 1;
            if (comptime Lease != void) if (self.head == null) self.dropRetained();
            self.ready.broadcast(io);
        }
    };
}

fn testIo() std.Io {
    return std.testing.io;
}
const TestMutex = QueuedMutex(testIo);

test "admission mutex expired uncontended admission remains possible" {
    var mutex: TestMutex = .{};
    try mutex.lockUntil(testIo(), std.Io.Clock.awake.now(testIo()).nanoseconds - 1);
    try std.testing.expect(!mutex.tryLock());
    mutex.unlock(testIo());
    try std.testing.expect(mutex.tryLock());
    mutex.unlock(testIo());
    try std.testing.expectEqual(mutex.next, mutex.serving);
}

test "admission mutex timeout leaves no abandoned queue hole" {
    var mutex: TestMutex = .{};
    mutex.lockUncancelable(testIo());
    try std.testing.expectError(error.FileBusy, mutex.lockUntil(testIo(), std.Io.Clock.awake.now(testIo()).nanoseconds + 10 * std.time.ns_per_ms));
    try std.testing.expect(mutex.head == null and mutex.tail == null);
    try std.testing.expectEqual(@as(usize, 0), mutex.ready.waiters);
    mutex.unlock(testIo());
    try std.testing.expect(mutex.tryLock());
    mutex.unlock(testIo());
    try std.testing.expectEqual(mutex.next, mutex.serving);
}

fn waitQueued(mutex: *TestMutex, count: u64) !void {
    const deadline = std.Io.Clock.awake.now(testIo()).nanoseconds + 5 * std.time.ns_per_s;
    while (true) {
        mutex.state.lockUncancelable(testIo());
        const enrolled = mutex.next == count;
        mutex.state.unlock(testIo());
        if (enrolled) return;
        if (std.Io.Clock.awake.now(testIo()).nanoseconds >= deadline) return error.TestWatchdog;
        try testIo().sleep(.fromMilliseconds(1), .awake);
    }
}

fn timedLock(mutex: *TestMutex) !void {
    try mutex.lockUntil(testIo(), std.Io.Clock.awake.now(testIo()).nanoseconds + 5 * std.time.ns_per_s);
    mutex.unlock(testIo());
}

test "admission mutex cancellation removes a live waiter before holder release" {
    var mutex: TestMutex = .{};
    mutex.lockUncancelable(testIo());
    var held = true;
    defer if (held) mutex.unlock(testIo());
    var waiter = try std.Io.concurrent(testIo(), timedLock, .{&mutex});
    defer _ = waiter.cancel(testIo()) catch {};
    try waitQueued(&mutex, 2);
    try std.testing.expectError(error.Canceled, waiter.cancel(testIo()));
    try std.testing.expect(mutex.head == null and mutex.tail == null);
    try std.testing.expectEqual(@as(usize, 0), mutex.ready.waiters);
    mutex.unlock(testIo());
    held = false;
    try std.testing.expect(mutex.tryLock());
    mutex.unlock(testIo());
    try std.testing.expectEqual(mutex.next, mutex.serving);
}

test "admission mutex middle cancellation preserves remaining FIFO order" {
    const Actor = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn run(self: *@This(), mutex: *TestMutex) void {
            mutex.lockUncancelable(testIo());
            self.entered.set(testIo());
            self.release.waitUncancelable(testIo());
            mutex.unlock(testIo());
        }
    };
    var mutex: TestMutex = .{};
    mutex.lockUncancelable(testIo());
    var held = true;
    defer if (held) mutex.unlock(testIo());
    var first: Actor = .{};
    var first_task = try std.Io.concurrent(testIo(), Actor.run, .{ &first, &mutex });
    defer {
        if (held) {
            mutex.unlock(testIo());
            held = false;
        }
        first.release.set(testIo());
        first_task.await(testIo());
    }
    try waitQueued(&mutex, 2);
    var middle = try std.Io.concurrent(testIo(), timedLock, .{&mutex});
    defer _ = middle.cancel(testIo()) catch {};
    try waitQueued(&mutex, 3);
    var last: Actor = .{};
    var last_task = try std.Io.concurrent(testIo(), Actor.run, .{ &last, &mutex });
    defer {
        if (held) {
            mutex.unlock(testIo());
            held = false;
        }
        first.release.set(testIo());
        last.release.set(testIo());
        last_task.await(testIo());
    }
    try waitQueued(&mutex, 4);
    try std.testing.expectError(error.Canceled, middle.cancel(testIo()));
    mutex.unlock(testIo());
    held = false;
    try first.entered.waitTimeout(testIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(!last.entered.isSet());
    first.release.set(testIo());
    try last.entered.waitTimeout(testIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    last.release.set(testIo());
    first_task.await(testIo());
    last_task.await(testIo());
    try std.testing.expectEqual(mutex.next, mutex.serving);
}
