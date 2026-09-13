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
    state: std.atomic.Value(usize) = .init(0),
    writer_gate: std.atomic.Mutex = .unlocked,
    wake_epoch: std.atomic.Value(u32) = .init(0),
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
        _ = self.wake_epoch.fetchAdd(1, .release);
        if (comptime builtin.os.tag != .freestanding and !builtin.single_threaded) {
            std.Io.Threaded.global_single_threaded.io().futexWake(u32, &self.wake_epoch.raw, std.math.maxInt(u32));
        }
    }

    fn wait(self: *@This(), epoch: u32) void {
        if (comptime builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
        } else {
            // The epoch is sampled before testing the predicate, so an unlock
            // between that test and parking cannot become a lost wakeup.
            std.Io.Threaded.global_single_threaded.io().futexWaitUncancelable(u32, &self.wake_epoch.raw, epoch);
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
        if (priority and self.priority_shared_waiters.fetchSub(1, .acq_rel) == 1 and
            self.exclusive_waiters.load(.acquire) != 0) self.signal();
    }

    fn unregisterWriter(self: *@This()) void {
        _ = self.exclusive_waiters.fetchSub(1, .acq_rel);
        self.signal();
    }

    pub fn lockShared(self: *@This()) void {
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

    /// Runtime callers retain their owner's cancellable wait protocol. Unlike
    /// synchronous native waiters they may run on a cooperative/custom Io;
    /// bounded sleeps must not be replaced by another owner's futex queue.
    pub fn lockSharedIo(self: *@This(), io: std.Io, cancellation: anytype) !void {
        const started_ns = monotonicNs();
        const priority = self.registerReader();
        defer self.unregisterReader(priority);
        var contended = false;
        var delay_us: i64 = 50 + @as(i64, @intCast(monotonicNs() & 0x3f));
        while (true) {
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (self.tryLockSharedQueued(priority)) break;
            contended = true;
            try io.sleep(std.Io.Duration.fromMicroseconds(delay_us), .awake);
            delay_us = @min(delay_us * 2, 1_000);
        }
        if (contended) {
            _ = self.shared_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .shared, monotonicNs() -| started_ns);
        }
    }

    pub fn lockExclusiveIo(self: *@This(), io: std.Io, cancellation: anytype) !void {
        const started_ns = monotonicNs();
        _ = self.exclusive_lock_calls.fetchAdd(1, .monotonic);
        _ = self.exclusive_waiters.fetchAdd(1, .acq_rel);
        defer self.unregisterWriter();
        var contended = false;
        var delay_us: i64 = 50 + @as(i64, @intCast((monotonicNs() >> 6) & 0x3f));
        while (true) {
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (self.priority_shared_waiters.load(.acquire) == 0 and self.writer_gate.tryLock()) break;
            contended = true;
            try io.sleep(std.Io.Duration.fromMicroseconds(delay_us), .awake);
            delay_us = @min(delay_us * 2, 1_000);
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
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (self.state.load(.acquire) == writer_bit) break;
            contended = true;
            try io.sleep(std.Io.Duration.fromMicroseconds(delay_us), .awake);
            delay_us = @min(delay_us * 2, 1_000);
        }
        if (contended) {
            _ = self.exclusive_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .exclusive, monotonicNs() -| started_ns);
        }
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
    var lock: ApplyRwLock = .{};

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
    var lock: ApplyRwLock = .{};
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
    var lock: ApplyRwLock = .{};

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
    var lock: ApplyRwLock = .{};
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
    var lock: ApplyRwLock = .{};
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
    var context: Context = .{};
    var readers: [8]std.Thread = undefined;
    for (&readers) |*reader| reader.* = try std.Thread.spawn(.{}, Context.reader, .{&context});
    var writers: [2]std.Thread = undefined;
    for (&writers) |*writer| writer.* = try std.Thread.spawn(.{}, Context.writer, .{&context});
    for (writers) |writer| writer.join();
    for (readers) |reader| reader.join();
    try std.testing.expect(!context.failed.load(.acquire));
    try std.testing.expectEqual(@as(u64, 10000), context.value);
}
