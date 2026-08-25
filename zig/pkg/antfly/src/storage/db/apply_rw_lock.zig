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
/// reentrant: once a writer owns `reader_gate`, a call tree that already holds
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

    reader_gate: std.atomic.Mutex = .unlocked,
    reader_mutex: std.atomic.Mutex = .unlocked,
    resource_mutex: std.atomic.Mutex = .unlocked,
    reader_count: usize = 0,
    shared_waiters: AtomicU64 = .init(0),
    priority_shared_waiters: AtomicU64 = .init(0),
    shared_lock_calls: AtomicU64 = .init(0),
    shared_contended_calls: AtomicU64 = .init(0),
    shared_wait_ns: AtomicU64 = .init(0),
    shared_max_wait_ns: AtomicU64 = .init(0),
    exclusive_lock_calls: AtomicU64 = .init(0),
    exclusive_contended_calls: AtomicU64 = .init(0),
    exclusive_wait_ns: AtomicU64 = .init(0),
    exclusive_max_wait_ns: AtomicU64 = .init(0),

    pub fn lockShared(self: *@This()) void {
        const started_ns = monotonicNs();
        _ = self.shared_lock_calls.fetchAdd(1, .monotonic);
        _ = self.shared_waiters.fetchAdd(1, .monotonic);
        defer _ = self.shared_waiters.fetchSub(1, .monotonic);
        if (!lockAtomic(&self.reader_gate)) {
            _ = self.shared_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .shared, monotonicNs() -| started_ns);
        }
        defer self.reader_gate.unlock();

        _ = lockAtomic(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        self.reader_count += 1;
        if (self.reader_count == 1) {
            _ = lockAtomic(&self.resource_mutex);
        }
    }

    pub fn tryLockShared(self: *@This()) bool {
        if (!self.reader_gate.tryLock()) return false;
        defer self.reader_gate.unlock();

        if (!self.reader_mutex.tryLock()) return false;
        defer self.reader_mutex.unlock();

        if (self.reader_count == 0 and !self.resource_mutex.tryLock()) return false;
        self.reader_count += 1;
        return true;
    }

    /// Acquire shared ownership without blocking a backend-runtime worker on
    /// the synchronous atomic wait path. Register for the entire wait so an
    /// exclusive reacquisition loop yields to this reader, while bounded
    /// runtime sleeps preserve cancellation and avoid a hot polling herd.
    pub fn lockSharedIo(self: *@This(), io: std.Io, cancellation: anytype) !void {
        const started_ns = monotonicNs();
        _ = self.shared_lock_calls.fetchAdd(1, .monotonic);
        _ = self.shared_waiters.fetchAdd(1, .monotonic);
        defer _ = self.shared_waiters.fetchSub(1, .monotonic);
        _ = self.priority_shared_waiters.fetchAdd(1, .monotonic);
        defer _ = self.priority_shared_waiters.fetchSub(1, .monotonic);

        var contended = false;
        var delay_us: i64 = 50 + @as(i64, @intCast(monotonicNs() & 0x3f));
        while (true) {
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (self.tryLockShared()) break;
            contended = true;
            io.sleep(std.Io.Duration.fromMicroseconds(delay_us), .awake) catch |err| switch (err) {
                error.Canceled => return error.Cancelled,
            };
            delay_us = @min(delay_us * 2, 1_000);
        }
        if (contended) {
            _ = self.shared_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .shared, monotonicNs() -| started_ns);
        }
    }

    pub fn unlockShared(self: *@This()) void {
        _ = lockAtomic(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        std.debug.assert(self.reader_count > 0);
        self.reader_count -= 1;
        if (self.reader_count == 0) {
            self.resource_mutex.unlock();
        }
    }

    pub fn tryLockExclusive(self: *@This()) bool {
        if (self.priority_shared_waiters.load(.monotonic) > 0) return false;
        if (!self.reader_gate.tryLock()) return false;
        if (!self.resource_mutex.tryLock()) {
            self.reader_gate.unlock();
            return false;
        }
        return true;
    }

    pub fn lockExclusive(self: *@This()) void {
        const started_ns = monotonicNs();
        _ = self.exclusive_lock_calls.fetchAdd(1, .monotonic);
        yieldToPriorityReaders(self);
        yieldToQueuedReaders(self);
        const gate_idle = lockAtomic(&self.reader_gate);
        errdefer self.reader_gate.unlock();
        const resource_idle = lockAtomic(&self.resource_mutex);
        if (!(gate_idle and resource_idle)) {
            _ = self.exclusive_contended_calls.fetchAdd(1, .monotonic);
            noteWait(self, .exclusive, monotonicNs() -| started_ns);
        }
    }

    pub fn unlockExclusive(self: *@This()) void {
        self.resource_mutex.unlock();
        self.reader_gate.unlock();
        if (builtin.os.tag != .freestanding and !builtin.single_threaded) {
            std.Thread.yield() catch {};
        }
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

fn lockAtomic(mutex: *std.atomic.Mutex) bool {
    var attempts: usize = 0;
    while (!mutex.tryLock()) : (attempts += 1) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
            continue;
        }
        if (attempts < 64) {
            std.atomic.spinLoopHint();
            continue;
        }
        if (attempts < 128) {
            std.Thread.yield() catch {};
            continue;
        }
        std.Thread.yield() catch {};
    }
    return attempts == 0;
}

fn yieldToQueuedReaders(lock: *const ApplyRwLock) void {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;
    var attempts: usize = 0;
    while (attempts < 64 and lock.shared_waiters.load(.monotonic) > 0) : (attempts += 1) {
        std.Thread.yield() catch {};
    }
}

fn yieldToPriorityReaders(lock: *const ApplyRwLock) void {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;
    while (lock.priority_shared_waiters.load(.monotonic) > 0) {
        std.Thread.yield() catch {};
    }
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
    const writer_thread = try std.Thread.spawn(.{}, Context.writer, .{&ctx});
    defer writer_thread.join();

    const reader_thread = try std.Thread.spawn(.{}, Context.reader, .{&ctx});
    defer reader_thread.join();

    var spins: usize = 0;
    while (!ctx.reader_done.load(.acquire) and spins < 100_000) : (spins += 1) {
        std.Thread.yield() catch {};
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
