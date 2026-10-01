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
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const platform_time = @import("antfly_platform").time;
const AtomicU64 = platform.atomic.Value(u64);

/// Writer-preferring service fence. Shared acquisition is intentionally not
/// reentrant: once a writer closes admission, a call tree that already holds
/// shared must use lock-assuming helpers instead of acquiring shared again.
pub const ApplyRwLock = struct {
    pub const Stats = struct {
        shared_lock_calls: u64 = 0,
        shared_contended_calls: u64 = 0,
        shared_wait_ns: u64 = 0,
        shared_max_wait_ns: u64 = 0,
        exclusive_lock_calls: u64 = 0,
        exclusive_contended_calls: u64 = 0,
        exclusive_wait_ns: u64 = 0,
        exclusive_max_wait_ns: u64 = 0,
    };

    // One atomic word linearizes reader admission against writer closure.
    // Readers never acquire a mutex just to update their shared count.
    const writer_bit: usize = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    /// Synchronization authority borrowed from the owner, bound before use.
    /// Custom/cooperative callers must bind this to their Io so unlock wakes
    /// the same queue that acquisition parks on. Null retains native fallback.
    io: ?std.Io = null,
    state: std.atomic.Value(usize) = .init(0),
    writer_gate: std.atomic.Mutex = .unlocked,
    wake_epoch: std.atomic.Value(u32) = .init(0),
    parked_waiters: std.atomic.Value(u32) = .init(0),
    shared_waiters: AtomicU64 = .init(0),
    priority_shared_waiters: AtomicU64 = .init(0),
    exclusive_waiters: AtomicU64 = .init(0),
    shared_lock_calls: AtomicU64 = .init(0),
    shared_contended_calls: AtomicU64 = .init(0),
    shared_wait_ns: AtomicU64 = .init(0),
    shared_max_wait_ns: AtomicU64 = .init(0),
    exclusive_lock_calls: AtomicU64 = .init(0),
    exclusive_contended_calls: AtomicU64 = .init(0),
    exclusive_wait_ns: AtomicU64 = .init(0),
    exclusive_max_wait_ns: AtomicU64 = .init(0),

    fn signal(self: *@This()) void {
        // Pair with beginWait's registration/recheck. Sequential consistency
        // forbids both sides missing one another: either we observe the waiter
        // or it observes this epoch before parking. Uncontended unlock stays
        // entirely atomic, without an Io call or a kernel wake syscall.
        _ = self.wake_epoch.fetchAdd(1, .seq_cst);
        if (self.parked_waiters.load(.seq_cst) == 0) return;
        if (self.io) |io| {
            io.futexWake(u32, &self.wake_epoch.raw, std.math.maxInt(u32));
            return;
        }
        if (comptime builtin.os.tag != .freestanding and !builtin.single_threaded) {
            std.Io.Threaded.global_single_threaded.io().futexWake(u32, &self.wake_epoch.raw, std.math.maxInt(u32));
        }
    }

    fn beginWait(self: *@This(), epoch: u32) bool {
        _ = self.parked_waiters.fetchAdd(1, .seq_cst);
        if (self.wake_epoch.load(.seq_cst) == epoch) return true;
        self.endWait();
        return false;
    }

    fn endWait(self: *@This()) void {
        _ = self.parked_waiters.fetchSub(1, .seq_cst);
    }

    fn wait(self: *@This(), epoch: u32) void {
        if (!self.beginWait(epoch)) return;
        defer self.endWait();
        if (comptime builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
        } else {
            // The epoch is sampled before testing the predicate, so an unlock
            // between that test and parking cannot become a lost wakeup.
            std.Io.Threaded.global_single_threaded.io().futexWaitUncancelable(u32, &self.wake_epoch.raw, epoch);
        }
    }

    fn assertIo(self: *const @This(), io: std.Io) void {
        if (self.io) |owner| {
            std.debug.assert(owner.userdata == io.userdata and owner.vtable == io.vtable);
        }
    }

    fn waitForLock(self: *@This(), io: std.Io, epoch: u32, callback_cancellation: bool, deadline: ?std.Io.Clock.Timestamp, comptime uncancelable: bool) !void {
        if (!self.beginWait(epoch)) return;
        defer self.endWait();
        if (uncancelable) {
            io.futexWaitUncancelable(u32, &self.wake_epoch.raw, epoch);
        } else if (deadline) |until| {
            try io.futexWaitTimeout(u32, &self.wake_epoch.raw, epoch, .{ .deadline = until });
        } else if (callback_cancellation) {
            // These tokens expose only isCancelled, without a wake registration.
            // Unlock still wakes immediately; the timeout bounds cancellation
            // latency when the lock holder has not released its lease.
            try io.futexWaitTimeout(u32, &self.wake_epoch.raw, epoch, .{
                .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake },
            });
        } else {
            try io.futexWait(u32, &self.wake_epoch.raw, epoch);
        }
    }

    fn registerReader(self: *@This()) bool {
        _ = self.shared_lock_calls.fetchAdd(1, .monotonic);
        _ = self.shared_waiters.fetchAdd(1, .monotonic);
        const priority = self.exclusive_waiters.load(.acquire) == 0;
        if (priority) _ = self.priority_shared_waiters.fetchAdd(1, .acq_rel);
        return priority;
    }

    fn unregisterReader(self: *@This(), priority: bool) void {
        _ = self.shared_waiters.fetchSub(1, .monotonic);
        // The last priority reader changes writer admission. Do not gate the
        // signal on another atomic's writer count: a concurrently registering
        // writer may still be invisible here after observing our old priority.
        if (priority and self.priority_shared_waiters.fetchSub(1, .acq_rel) == 1) self.signal();
    }

    fn unregisterWriter(self: *@This()) void {
        _ = self.exclusive_waiters.fetchSub(1, .acq_rel);
        self.signal();
    }

    pub fn lockShared(self: *@This()) void {
        if (self.io) |io| {
            self.lockSharedWithIo(io, null, true) catch unreachable;
            return;
        }
        const started_ns = monotonicNs();
        const priority = self.registerReader();
        defer self.unregisterReader(priority);
        var contended = false;
        while (true) {
            const epoch = self.wake_epoch.load(.acquire);
            if (self.tryLockSharedQueued(priority)) break;
            contended = true;
            self.wait(epoch);
        }
        if (contended) {
            _ = self.shared_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .shared, monotonicNs() -| started_ns);
        }
    }

    pub fn tryLockShared(self: *@This()) bool {
        return self.tryLockSharedQueued(false);
    }

    fn tryLockSharedQueued(self: *@This(), priority: bool) bool {
        var current = self.state.load(.monotonic);
        while (true) {
            if (!priority and self.exclusive_waiters.load(.acquire) != 0) return false;
            if (current & writer_bit != 0 or current == writer_bit - 1) return false;
            if (self.state.cmpxchgWeak(current, current + 1, .acquire, .monotonic)) |observed| {
                current = observed;
            } else return true;
        }
    }

    /// The bound owner supplies synchronization even when a caller supplies a
    /// different filesystem/request lane. The argument is a native fallback for
    /// unbound locks. Callback-only cancellation retains bounded checks;
    /// backend task cancellation wakes the futex itself.
    pub fn lockSharedIo(self: *@This(), io: std.Io, cancellation: anytype) (std.Io.Cancelable || error{Cancelled})!void {
        return self.lockSharedWithIo(self.io orelse io, cancellation, false);
    }

    fn lockSharedWithIo(self: *@This(), io: std.Io, cancellation: anytype, comptime uncancelable: bool) !void {
        const started_ns = ioMonotonicNs(io);
        const priority = self.registerReader();
        defer self.unregisterReader(priority);
        var contended = false;
        self.assertIo(io);
        while (true) {
            // Capture before checking admission: a concurrent unlock then
            // changes the expected value and prevents parking on a lost wake.
            const epoch = self.wake_epoch.load(.acquire);
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (self.tryLockSharedQueued(priority)) break;
            contended = true;
            try self.waitForLock(io, epoch, cancellation != null, null, uncancelable);
        }
        if (contended) {
            _ = self.shared_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .shared, ioMonotonicNs(io) -| started_ns);
        }
    }

    pub fn lockExclusiveIo(self: *@This(), io: std.Io, cancellation: anytype) (std.Io.Cancelable || error{Cancelled})!void {
        return self.lockExclusiveWithIo(self.io orelse io, cancellation, null, false);
    }

    /// A deadline belongs to the bound owner's clock (or the fallback Io). Expiration releases writer
    /// intent and reader admission just like cancellation, returning Timeout.
    pub fn lockExclusiveDeadlineIo(self: *@This(), io: std.Io, deadline: std.Io.Clock.Timestamp) (std.Io.Cancelable || error{Timeout})!void {
        return self.lockExclusiveWithIo(self.io orelse io, null, @as(?std.Io.Clock.Timestamp, deadline), false);
    }

    fn lockExclusiveWithIo(self: *@This(), io: std.Io, cancellation: anytype, deadline: anytype, comptime uncancelable: bool) !void {
        const started_ns = ioMonotonicNs(io);
        _ = self.exclusive_lock_calls.fetchAdd(1, .monotonic);
        _ = self.exclusive_waiters.fetchAdd(1, .acq_rel);
        defer self.unregisterWriter();
        var contended = false;
        self.assertIo(io);
        while (true) {
            // Capture before checking admission: a concurrent unlock then
            // changes the expected value and prevents parking on a lost wake.
            const epoch = self.wake_epoch.load(.acquire);
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (deadline) |until| if (until.raw.nanoseconds <= until.clock.now(io).nanoseconds) return error.Timeout;
            if (self.priority_shared_waiters.load(.acquire) == 0 and self.writer_gate.tryLock()) break;
            contended = true;
            try self.waitForLock(io, epoch, cancellation != null, deadline, uncancelable);
        }
        errdefer {
            self.writer_gate.unlock();
            self.signal();
        }
        const previous = self.state.fetchOr(writer_bit, .acq_rel);
        std.debug.assert(previous & writer_bit == 0);
        errdefer {
            _ = self.state.fetchAnd(~writer_bit, .release);
            self.signal();
        }
        while (true) {
            // Capture before checking admission: a concurrent unlock then
            // changes the expected value and prevents parking on a lost wake.
            const epoch = self.wake_epoch.load(.acquire);
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (deadline) |until| if (until.raw.nanoseconds <= until.clock.now(io).nanoseconds) return error.Timeout;
            if (self.state.load(.acquire) == writer_bit) break;
            contended = true;
            try self.waitForLock(io, epoch, cancellation != null, deadline, uncancelable);
        }
        if (contended) {
            _ = self.exclusive_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .exclusive, ioMonotonicNs(io) -| started_ns);
        }
    }

    /// Retain an existing shared lease without re-entering reader admission.
    /// The caller must keep its original lease alive until this returns.
    pub fn retainShared(self: *@This()) void {
        const previous = self.state.fetchAdd(1, .acquire);
        const readers = previous & ~writer_bit;
        std.debug.assert(readers != 0 and readers < writer_bit - 1);
    }

    pub fn unlockShared(self: *@This()) void {
        const previous = self.state.fetchSub(1, .release);
        std.debug.assert(previous & ~writer_bit != 0);
        if (previous == writer_bit + 1) self.signal();
    }

    pub fn tryLockExclusive(self: *@This()) bool {
        if (self.exclusive_waiters.load(.acquire) != 0 or self.priority_shared_waiters.load(.acquire) != 0) return false;
        if (!self.writer_gate.tryLock()) return false;
        if (self.state.cmpxchgStrong(0, writer_bit, .acquire, .monotonic) != null) {
            self.writer_gate.unlock();
            self.signal();
            return false;
        }
        return true;
    }

    pub fn lockExclusive(self: *@This()) void {
        if (self.io) |io| {
            self.lockExclusiveWithIo(io, null, null, true) catch unreachable;
            return;
        }
        const started_ns = monotonicNs();
        _ = self.exclusive_lock_calls.fetchAdd(1, .monotonic);
        _ = self.exclusive_waiters.fetchAdd(1, .acq_rel);
        defer self.unregisterWriter();
        var contended = false;
        while (true) {
            const epoch = self.wake_epoch.load(.acquire);
            if (self.priority_shared_waiters.load(.acquire) == 0 and self.writer_gate.tryLock()) break;
            contended = true;
            self.wait(epoch);
        }
        const previous = self.state.fetchOr(writer_bit, .acq_rel);
        std.debug.assert(previous & writer_bit == 0);
        while (true) {
            const epoch = self.wake_epoch.load(.acquire);
            if (self.state.load(.acquire) == writer_bit) break;
            contended = true;
            self.wait(epoch);
        }
        if (contended) {
            _ = self.exclusive_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .exclusive, monotonicNs() -| started_ns);
        }
    }

    pub fn unlockExclusive(self: *@This()) void {
        const previous = self.state.swap(0, .release);
        std.debug.assert(previous == writer_bit);
        self.writer_gate.unlock();
        self.signal();
    }

    pub fn snapshot(self: *const @This()) Stats {
        return .{
            .shared_lock_calls = self.shared_lock_calls.load(.monotonic),
            .shared_contended_calls = self.shared_contended_calls.load(.monotonic),
            .shared_wait_ns = self.shared_wait_ns.load(.monotonic),
            .shared_max_wait_ns = self.shared_max_wait_ns.load(.monotonic),
            .exclusive_lock_calls = self.exclusive_lock_calls.load(.monotonic),
            .exclusive_contended_calls = self.exclusive_contended_calls.load(.monotonic),
            .exclusive_wait_ns = self.exclusive_wait_ns.load(.monotonic),
            .exclusive_max_wait_ns = self.exclusive_max_wait_ns.load(.monotonic),
        };
    }
};

fn ioMonotonicNs(io: std.Io) u64 {
    return @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds));
}

fn monotonicNs() u64 {
    return platform_time.monotonicNs();
}

fn noteWait(self: *ApplyRwLock, comptime kind: enum { shared, exclusive }, wait_ns: u64) void {
    switch (kind) {
        .shared => {
            _ = self.shared_wait_ns.fetchAdd(wait_ns, .monotonic);
            atomicMaxU64(&self.shared_max_wait_ns, wait_ns);
        },
        .exclusive => {
            _ = self.exclusive_wait_ns.fetchAdd(wait_ns, .monotonic);
            atomicMaxU64(&self.exclusive_max_wait_ns, wait_ns);
        },
    }
}

fn atomicMaxU64(value: *AtomicU64, candidate: u64) void {
    var current = value.load(.monotonic);
    while (candidate > current) {
        current = value.cmpxchgWeak(current, candidate, .monotonic, .monotonic) orelse return;
    }
}

test "apply rw lock permits nested shared acquisition while no writer is queued" {
    var lock: ApplyRwLock = .{};

    lock.lockShared();
    defer lock.unlockShared();

    lock.lockShared();
    defer lock.unlockShared();

    try std.testing.expect(!lock.tryLockExclusive());
}

test "apply rw lock exclusive blocks while shared held" {
    var lock: ApplyRwLock = .{};

    lock.lockShared();
    defer lock.unlockShared();

    try std.testing.expect(!lock.tryLockExclusive());
}

test "apply rw lock failed exclusive try does not poison future shared or exclusive lock" {
    var lock: ApplyRwLock = .{};

    lock.lockShared();
    try std.testing.expect(!lock.tryLockExclusive());
    lock.unlockShared();

    lock.lockShared();
    lock.unlockShared();

    try std.testing.expect(lock.tryLockExclusive());
    lock.unlockExclusive();
}

test "apply rw lock exclusive tryLock succeeds when idle" {
    var lock: ApplyRwLock = .{};

    try std.testing.expect(lock.tryLockExclusive());
    lock.unlockExclusive();
}

test "apply rw lock lets queued readers through sustained exclusive loop" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Context = struct {
        lock: ApplyRwLock = .{},
        reader_ready: std.atomic.Value(bool) = .init(false),
        reader_done: std.atomic.Value(bool) = .init(false),
        writer_done: std.atomic.Value(bool) = .init(false),

        fn writer(ctx: *@This()) void {
            var i: usize = 0;
            while (i < 10_000 and !ctx.reader_done.load(.acquire)) : (i += 1) {
                ctx.lock.lockExclusive();
                ctx.lock.unlockExclusive();
            }
            ctx.writer_done.store(true, .release);
        }

        fn reader(ctx: *@This()) void {
            ctx.reader_ready.store(true, .release);
            ctx.lock.lockShared();
            ctx.lock.unlockShared();
            ctx.reader_done.store(true, .release);
        }
    };

    var ctx = Context{};
    var writer_thread = try std.testing.io.concurrent(Context.writer, .{&ctx});
    defer writer_thread.await(std.testing.io);

    var reader_thread = try std.testing.io.concurrent(Context.reader, .{&ctx});
    defer reader_thread.await(std.testing.io);

    var spins: usize = 0;
    while (!ctx.reader_done.load(.acquire) and spins < 100_000) : (spins += 1) {
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    try std.testing.expect(ctx.reader_ready.load(.acquire));
    try std.testing.expect(ctx.reader_done.load(.acquire));
    try std.testing.expect(!ctx.writer_done.load(.acquire) or spins < 100_000);
}

test "apply rw lock runtime shared wait cancellation clears priority handoff" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Cancellation = struct {
        signal: *const std.atomic.Value(bool),

        fn isCancelled(self: @This()) bool {
            return self.signal.load(.acquire);
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var cancelled = std.atomic.Value(bool).init(true);
    var lock: ApplyRwLock = .{ .io = io };

    try std.testing.expectError(
        error.Cancelled,
        lock.lockSharedIo(io, @as(?Cancellation, .{ .signal = &cancelled })),
    );
    try std.testing.expectEqual(@as(u64, 0), lock.priority_shared_waiters.load(.acquire));
    try std.testing.expect(lock.tryLockExclusive());
    lock.unlockExclusive();
}

test "apply rw lock runtime writer cancellation clears intent and reader gate" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Cancellation = struct {
        signal: *const std.atomic.Value(bool),

        fn isCancelled(self: @This()) bool {
            return self.signal.load(.acquire);
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var cancelled = std.atomic.Value(bool).init(true);
    var lock: ApplyRwLock = .{ .io = io };
    lock.lockShared();

    try std.testing.expectError(
        error.Cancelled,
        lock.lockExclusiveIo(io, @as(?Cancellation, .{ .signal = &cancelled })),
    );
    try std.testing.expectEqual(@as(u64, 0), lock.exclusive_waiters.load(.acquire));
    lock.unlockShared();

    // Cancellation after reserving the reader gate must release it as well as
    // writer intent, otherwise every later reader and writer would wedge.
    try std.testing.expect(lock.tryLockShared());
    lock.unlockShared();
    try std.testing.expect(lock.tryLockExclusive());
    lock.unlockExclusive();
}

test "apply rw lock preserves backend task cancellation" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const NeverCancelled = struct {
        fn isCancelled(_: @This()) bool {
            return false;
        }
    };
    const Waiter = struct {
        fn shared(lock: *ApplyRwLock, io: std.Io) !void {
            try lock.lockSharedIo(io, @as(?NeverCancelled, null));
            lock.unlockShared();
        }

        fn exclusive(lock: *ApplyRwLock, io: std.Io) !void {
            try lock.lockExclusiveIo(io, @as(?NeverCancelled, null));
            lock.unlockExclusive();
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var lock: ApplyRwLock = .{ .io = io };

    lock.lockExclusive();
    var exclusive_held = true;
    defer if (exclusive_held) lock.unlockExclusive();
    var shared_waiter = std.Io.async(io, Waiter.shared, .{ &lock, io });
    var shared_waiter_active = true;
    defer if (shared_waiter_active) {
        _ = shared_waiter.cancel(io) catch {};
    };
    var shared_joined = false;
    for (0..5_000) |_| {
        if (lock.priority_shared_waiters.load(.acquire) != 0) {
            shared_joined = true;
            break;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(shared_joined);
    const shared_result = shared_waiter.cancel(io);
    shared_waiter_active = false;
    try std.testing.expectError(error.Canceled, shared_result);
    try std.testing.expectEqual(@as(u64, 0), lock.priority_shared_waiters.load(.acquire));
    lock.unlockExclusive();
    exclusive_held = false;

    lock.lockShared();
    var shared_held = true;
    defer if (shared_held) lock.unlockShared();
    var exclusive_waiter = std.Io.async(io, Waiter.exclusive, .{ &lock, io });
    var exclusive_waiter_active = true;
    defer if (exclusive_waiter_active) {
        _ = exclusive_waiter.cancel(io) catch {};
    };
    var exclusive_joined = false;
    for (0..5_000) |_| {
        if (lock.exclusive_waiters.load(.acquire) != 0) {
            exclusive_joined = true;
            break;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(exclusive_joined);
    const exclusive_result = exclusive_waiter.cancel(io);
    exclusive_waiter_active = false;
    try std.testing.expectError(error.Canceled, exclusive_result);
    try std.testing.expectEqual(@as(u64, 0), lock.exclusive_waiters.load(.acquire));
    lock.unlockShared();
    shared_held = false;

    // Both canceled paths must leave the lock immediately reusable.
    try std.testing.expect(lock.tryLockShared());
    lock.unlockShared();
    try std.testing.expect(lock.tryLockExclusive());
    lock.unlockExclusive();
}

test "apply rw lock cooperative writer yields to queued reader on one-worker runtime" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const NeverCancelled = struct {
        fn isCancelled(_: @This()) bool {
            return false;
        }
    };
    const Context = struct {
        lock: *ApplyRwLock,
        io: std.Io,
        reader_done: std.atomic.Value(bool) = .init(false),
        writer_done: std.atomic.Value(bool) = .init(false),
        writer_failed: std.atomic.Value(bool) = .init(false),

        fn reader(ctx: *@This()) !void {
            try ctx.lock.lockSharedIo(ctx.io, @as(?NeverCancelled, null));
            ctx.reader_done.store(true, .release);
            ctx.lock.unlockShared();
        }

        fn writer(ctx: *@This()) !void {
            try ctx.lock.lockExclusiveIo(ctx.io, @as(?NeverCancelled, null));
            ctx.writer_done.store(true, .release);
            ctx.lock.unlockExclusive();
        }

        fn writerThread(ctx: *@This()) void {
            writer(ctx) catch ctx.writer_failed.store(true, .release);
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .limited(1),
    });
    defer io_impl.deinit();
    const io = io_impl.io();
    var lock: ApplyRwLock = .{ .io = io };
    lock.lockExclusive();
    var exclusive_held = true;
    defer if (exclusive_held) lock.unlockExclusive();

    var ctx = Context{ .lock = &lock, .io = io };
    var reader = std.Io.async(io, Context.reader, .{&ctx});
    var reader_awaited = false;
    defer if (!reader_awaited) {
        _ = reader.await(io) catch {};
    };
    while (lock.priority_shared_waiters.load(.acquire) == 0) {
        try io.sleep(.fromMicroseconds(50), .awake);
    }

    // A second `Io.async` is permitted to execute inline when the only async
    // slot is occupied. Starting a lock waiter that way while this caller
    // still owns the lock makes the test itself deadlock before it can unlock.
    // Use an independent caller for the writer while both lock waits continue
    // to use the same one-worker backend Io.
    var writer_thread = try std.testing.io.concurrent(Context.writerThread, .{&ctx});
    defer writer_thread.await(std.testing.io);
    while (lock.exclusive_waiters.load(.acquire) == 0) {
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    lock.unlockExclusive();
    exclusive_held = false;

    try reader.await(io);
    reader_awaited = true;
    try std.testing.expect(ctx.reader_done.load(.acquire));
    while (!ctx.writer_done.load(.acquire) and !ctx.writer_failed.load(.acquire)) {
        try io.sleep(.fromMicroseconds(50), .awake);
    }
    try std.testing.expect(!ctx.writer_failed.load(.acquire));
    try std.testing.expect(ctx.writer_done.load(.acquire));
}

test "apply rw lock queued io writer blocks later shared barging" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const NeverCancelled = struct {
        fn isCancelled(_: @This()) bool {
            return false;
        }
    };
    const Context = struct {
        lock: *ApplyRwLock,
        io: std.Io,
        writer_done: std.atomic.Value(bool) = .init(false),
        writer_failed: std.atomic.Value(bool) = .init(false),

        fn writer(ctx: *@This()) void {
            ctx.lock.lockExclusiveIo(ctx.io, @as(?NeverCancelled, null)) catch {
                ctx.writer_failed.store(true, .release);
                return;
            };
            ctx.writer_done.store(true, .release);
            ctx.lock.unlockExclusive();
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var lock: ApplyRwLock = .{ .io = io };
    lock.lockShared();
    var shared_held = true;
    defer if (shared_held) lock.unlockShared();

    var ctx = Context{ .lock = &lock, .io = io };
    var writer_thread = try std.testing.io.concurrent(Context.writer, .{&ctx});
    defer writer_thread.await(std.testing.io);
    while (lock.exclusive_waiters.load(.acquire) == 0) {
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }

    // Once writer intent is visible, neither opportunistic nor blocking-new
    // readers may enter ahead of it.
    const barged = lock.tryLockShared();
    if (barged) lock.unlockShared();
    lock.unlockShared();
    shared_held = false;

    while (!ctx.writer_done.load(.acquire) and !ctx.writer_failed.load(.acquire)) {
        try io.sleep(.fromMicroseconds(50), .awake);
    }
    try std.testing.expect(!ctx.writer_failed.load(.acquire));
    try std.testing.expect(ctx.writer_done.load(.acquire));
    // Assert only after releasing both the opportunistic acquisition (if the
    // invariant regressed) and the original blocker. The failure path must not
    // deadlock its deferred writer join and turn a useful failure into a hung
    // test process.
    try std.testing.expect(!barged);
}

test "apply rw lock concurrent readers preserve writer exclusion through repeated wakeups" {
    const Context = struct {
        lock: ApplyRwLock = .{},
        value: u64 = 0,
        inverse: u64 = ~@as(u64, 0),
        done: std.atomic.Value(u32) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        fn writer(ctx: *@This()) void {
            for (0..5000) |_| {
                ctx.lock.lockExclusive();
                ctx.value += 1;
                ctx.inverse = ~ctx.value;
                ctx.lock.unlockExclusive();
            }
            _ = ctx.done.fetchAdd(1, .release);
        }
        fn reader(ctx: *@This()) void {
            while (ctx.done.load(.acquire) < 2) {
                ctx.lock.lockShared();
                if (ctx.inverse != ~ctx.value) ctx.failed.store(true, .release);
                ctx.lock.unlockShared();
            }
        }
    };
    for ([_]bool{ false, true }) |bound| {
        var context: Context = .{ .lock = .{ .io = if (bound) std.testing.io else null } };
        var tasks: std.Io.Group = .init;
        defer tasks.cancel(std.testing.io);
        // A failed task launch must also release the uncancelable readers.
        errdefer context.done.store(2, .release);
        for (0..8) |_| try tasks.concurrent(std.testing.io, Context.reader, .{&context});
        for (0..2) |_| try tasks.concurrent(std.testing.io, Context.writer, .{&context});
        try tasks.await(std.testing.io);
        try std.testing.expect(!context.failed.load(.acquire));
        try std.testing.expectEqual(@as(u64, 10000), context.value);
        try std.testing.expectEqual(@as(u32, 0), context.lock.parked_waiters.load(.acquire));
    }
}

const LockVoprHarness = struct {
    const vopr = @import("vopr");
    runtime: *vopr.vopr_io.VoprIo,
    enabled: vopr.transition.List = .{},
    events: vopr.event.Sink = .{},

    fn deinit(self: *@This()) void {
        self.enabled.deinit(std.testing.allocator);
        self.events.deinit(std.testing.allocator);
    }

    fn step(self: *@This()) !void {
        self.enabled.items.clearRetainingCapacity();
        try self.runtime.scheduler().enumerateReady(&self.enabled, std.testing.allocator);
        try self.enabled.canonicalize();
        try std.testing.expect(self.enabled.items.items.len != 0);
        try self.runtime.scheduler().executeReady(self.enabled.items.items[0].id, &self.events, std.testing.allocator);
    }

    fn park(self: *@This(), future: std.Io.Future(anyerror!void)) !@import("vopr").vopr_io_task.TaskSnapshot {
        for (0..16) |_| {
            const snapshot = self.runtime.futureTaskSnapshot(future.any_future.?).?;
            if (snapshot.waiting_on_futex) return snapshot;
            try self.step();
        }
        return error.LockWaiterDidNotPark;
    }

    fn drain(self: *@This()) !void {
        for (0..32) |_| {
            if (self.runtime.scheduler().quiescent()) return;
            try self.step();
        }
        return error.LockWaitersDidNotComplete;
    }
};

test "apply rw lock VOPR wakes shared and exclusive waiters without advancing time" {
    var runtime = try LockVoprHarness.vopr.vopr_io.VoprIo.init(.{});
    defer runtime.deinit();
    const io = runtime.io();
    var harness: LockVoprHarness = .{ .runtime = &runtime };
    defer harness.deinit();
    const Work = struct {
        fn run(lock: *ApplyRwLock, shared: bool, cancellable: bool, done: *bool) anyerror!void {
            if (shared) {
                // Even a caller's native fallback must not move this wait
                // away from the bound VOPR synchronization authority.
                if (cancellable) try lock.lockSharedIo(std.testing.io, null) else lock.lockShared();
                lock.unlockShared();
            } else {
                if (cancellable) try lock.lockExclusiveIo(std.testing.io, null) else lock.lockExclusive();
                lock.unlockExclusive();
            }
            done.* = true;
        }
    };
    for ([_]bool{ false, true }) |shared| {
        for ([_]bool{ false, true }) |cancellable| {
            var lock: ApplyRwLock = .{ .io = io };
            if (shared) lock.lockExclusive() else lock.lockShared();
            var held = true;
            var done = false;
            var future = io.async(Work.run, .{ &lock, shared, cancellable, &done });
            defer {
                if (held) {
                    if (shared) lock.unlockExclusive() else lock.unlockShared();
                }
                _ = runtime.cancelAndDrainTasksForTeardown(std.testing.allocator, 64) catch @panic("lock cleanup failed");
                _ = future.cancel(io) catch {};
            }
            const start = std.Io.Clock.awake.now(io).nanoseconds;
            const snapshot = try harness.park(future);
            try std.testing.expectEqual(null, snapshot.sleep_deadline_ns);
            try std.testing.expect(!done);
            if (shared) lock.unlockExclusive() else lock.unlockShared();
            held = false;
            try harness.drain();
            try future.await(io);
            try std.testing.expect(done);
            try std.testing.expectEqual(start, std.Io.Clock.awake.now(io).nanoseconds);
            try std.testing.expectEqual(@as(u32, 0), lock.parked_waiters.load(.acquire));
            try std.testing.expectEqual(@as(u64, 0), lock.shared_waiters.load(.acquire));
            try std.testing.expectEqual(@as(u64, 0), lock.exclusive_waiters.load(.acquire));
        }
    }
    try runtime.ensureNoCapabilityViolation();
}

test "apply rw lock VOPR deadline and callback cancellation reopen reader admission" {
    var runtime = try LockVoprHarness.vopr.vopr_io.VoprIo.init(.{ .monotonic_ns = 100 * std.time.ns_per_s });
    defer runtime.deinit();
    const io = runtime.io();
    var harness: LockVoprHarness = .{ .runtime = &runtime };
    defer harness.deinit();
    const Token = struct {
        cancelled: *const bool,
        fn isCancelled(self: @This()) bool {
            return self.cancelled.*;
        }
    };
    const Work = struct {
        fn run(lock: *ApplyRwLock, deadline: ?std.Io.Clock.Timestamp, cancelled: *const bool) anyerror!void {
            if (deadline) |until| {
                try lock.lockExclusiveDeadlineIo(lock.io.?, until);
            } else {
                try lock.lockExclusiveIo(lock.io.?, @as(?Token, .{ .cancelled = cancelled }));
            }
            lock.unlockExclusive();
        }
    };
    for ([_]bool{ false, true }) |use_deadline| {
        var lock: ApplyRwLock = .{ .io = io };
        lock.lockShared();
        defer lock.unlockShared();
        var cancelled = false;
        const start = std.Io.Clock.awake.now(io).nanoseconds;
        const deadline: ?std.Io.Clock.Timestamp = if (use_deadline) .fromNow(io, .{
            .raw = .fromMilliseconds(50),
            .clock = .awake,
        }) else null;
        var future = io.async(Work.run, .{ &lock, deadline, &cancelled });
        defer {
            _ = runtime.cancelAndDrainTasksForTeardown(std.testing.allocator, 64) catch @panic("lock cleanup failed");
            _ = future.cancel(io) catch {};
        }
        const parked = try harness.park(future);
        const wait_ns: i96 = (if (use_deadline) @as(i96, 50) else 1) * std.time.ns_per_ms;
        try std.testing.expectEqual(start + wait_ns, parked.sleep_deadline_ns.?);
        try std.testing.expect(!lock.tryLockShared());
        cancelled = true;
        try harness.drain();
        if (use_deadline) {
            try std.testing.expectError(error.Timeout, future.await(io));
        } else {
            try std.testing.expectError(error.Cancelled, future.await(io));
        }
        try std.testing.expectEqual(start + wait_ns, std.Io.Clock.awake.now(io).nanoseconds);
        try std.testing.expectEqual(@as(u32, 0), lock.parked_waiters.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), lock.exclusive_waiters.load(.acquire));
        try std.testing.expect(lock.tryLockShared());
        lock.unlockShared();
        try std.testing.expect(lock.writer_gate.tryLock());
        lock.writer_gate.unlock();
    }
    try runtime.ensureNoCapabilityViolation();
}

test "apply rw lock VOPR unlock before parking does not lose its wake" {
    var runtime = try LockVoprHarness.vopr.vopr_io.VoprIo.init(.{});
    defer runtime.deinit();
    const io = runtime.io();
    var harness: LockVoprHarness = .{ .runtime = &runtime };
    defer harness.deinit();
    var lock: ApplyRwLock = .{ .io = io };
    lock.lockExclusive();
    // Force the precise pre-park overlap: admission failed, then unlock ran
    // before the wait primitive could register this epoch on its futex queue.
    const epoch = lock.wake_epoch.load(.acquire);
    try std.testing.expect(!lock.tryLockShared());
    lock.unlockExclusive();
    const Work = struct {
        fn run(mutex: *ApplyRwLock, expected: u32) anyerror!void {
            try mutex.waitForLock(mutex.io.?, expected, false, null, false);
            try std.testing.expect(mutex.tryLockShared());
            mutex.unlockShared();
        }
    };
    var future = io.async(Work.run, .{ &lock, epoch });
    defer {
        _ = runtime.cancelAndDrainTasksForTeardown(std.testing.allocator, 64) catch @panic("lock cleanup failed");
        _ = future.cancel(io) catch {};
    }
    const start = std.Io.Clock.awake.now(io).nanoseconds;
    try harness.drain();
    try future.await(io);
    try std.testing.expectEqual(start, std.Io.Clock.awake.now(io).nanoseconds);
    try runtime.ensureNoCapabilityViolation();
}
