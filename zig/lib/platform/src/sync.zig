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

//! Lock helpers for try-lock-only mutexes (e.g. `std.atomic.Mutex`).
//!
//! A bare `while (!mutex.tryLock()) spinLoopHint()` pins a core per waiter.
//! When the holder runs long (storage opens, raft rounds, compaction),
//! spinners starve the holder — and on CPU-quota'd hosts such as CI runner
//! pods, everything else on the machine, including the runner agent itself.
//! Spin briefly for the uncontended fast path, then release the CPU between
//! attempts.
//!
//! Use `lockYielding` from plain OS threads. Use `lockYieldingIo` from code
//! that already has an `Io` handle so the wait reschedules through the Io
//! runtime instead of the OS scheduler.

const std = @import("std");
const builtin = @import("builtin");
const time = @import("time.zig");

const spin_limit = 64;

/// Broadcast-only condition for predicate loops sharing one mutex. Both wait
/// and broadcast require that mutex to be held. Every wake returns to the
/// caller's predicate: a re-entering competitor cannot consume a different
/// waiter's pending notification after a spurious wake.
pub const BroadcastCondition = struct {
    epoch: std.atomic.Value(u32) = .init(0),
    waiters: u32 = 0,

    pub const init: BroadcastCondition = .{};

    pub fn waitUncancelable(self: *BroadcastCondition, io: std.Io, mutex: *std.Io.Mutex) void {
        const observed = self.epoch.load(.acquire);
        std.debug.assert(self.waiters < std.math.maxInt(u32));
        self.waiters += 1;
        mutex.unlock(io);
        defer {
            mutex.lockUncancelable(io);
            self.waiters -= 1;
        }
        // The atomic value check also covers a broadcast between releasing
        // the mutex and entering the kernel wait. Spurious wakes are valid.
        io.futexWaitUncancelable(u32, &self.epoch.raw, observed);
    }

    pub fn broadcast(self: *BroadcastCondition, io: std.Io) void {
        if (self.waiters == 0) return;
        _ = self.epoch.fetchAdd(1, .release);
        io.futexWake(u32, &self.epoch.raw, self.waiters);
    }

    /// Like `waitUncancelable`, but also returns once `timeout` elapses,
    /// instead of parking indefinitely for a `broadcast` that might never
    /// come. For a predicate loop whose own progress must never depend on
    /// some *other* task existing to eventually satisfy it (e.g. a
    /// ride-along commit ticket whose idle-flush driver might never run --
    /// a spawned `concurrent()` task that failed to start, or an `Io` with
    /// no real background concurrency at all) -- the caller bounds the wait
    /// by its own idle window and, if its predicate is still unmet once
    /// this returns, drives the outcome itself instead of waiting again
    /// unconditionally. `std.Io.futexWaitTimeout` returns normally (not an
    /// error) on a plain timeout, same as a real wake or a spurious one --
    /// only genuine cancellation (`io`'s own shutdown) surfaces as
    /// `error.Canceled`, same as every other bounded wait in this codebase.
    pub fn waitTimeout(self: *BroadcastCondition, io: std.Io, mutex: *std.Io.Mutex, timeout: std.Io.Timeout) std.Io.Cancelable!void {
        const observed = self.epoch.load(.acquire);
        std.debug.assert(self.waiters < std.math.maxInt(u32));
        self.waiters += 1;
        mutex.unlock(io);
        defer {
            mutex.lockUncancelable(io);
            self.waiters -= 1;
        }
        try io.futexWaitTimeout(u32, &self.epoch.raw, observed, timeout);
    }
};

/// Acquire `mutex` (any type exposing `tryLock`), spinning briefly and then
/// releasing the CPU between attempts via `time.yieldBriefly`. On
/// freestanding targets there is no scheduler to yield to, so this degrades
/// to a pure spin.
pub fn lockYielding(mutex: anytype) void {
    var spins: u32 = 0;
    while (!mutex.tryLock()) {
        if (comptime builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
        } else if (spins < spin_limit) {
            std.atomic.spinLoopHint();
            spins +%= 1;
        } else {
            time.yieldBriefly();
        }
    }
}

/// Io-aware variant of `lockYielding`: after the spin budget, reschedules
/// through the Io runtime (`sleep(zero)`) so pool tasks yield cooperatively
/// instead of calling into the OS scheduler.
pub fn lockYieldingIo(mutex: anytype, io: std.Io) void {
    var spins: u32 = 0;
    while (!mutex.tryLock()) {
        if (spins < spin_limit) {
            std.atomic.spinLoopHint();
            spins +%= 1;
        } else {
            io.sleep(std.Io.Duration.zero, .awake) catch std.atomic.spinLoopHint();
        }
    }
}

/// `std.Io`'s own `futexWait*`/`futexWake` (used by `BroadcastCondition`
/// above) are always the OS's *private*, same-process-only futex variant
/// on every backend checked (`Io.Threaded` passes `.private = true` on
/// Linux and the non-`_SHARED` op on macOS) and cannot wake a waiter
/// parked in a *different* process. There is no cross-process wait/wake
/// anywhere in `std.Io` to reuse, so this wraps the one real platform
/// primitive that supports it directly: macOS `__ulock_wait`/
/// `__ulock_wake` with the *shared* compare-and-wait op, Linux `futex(2)`
/// without `FUTEX_PRIVATE_FLAG`. Exists here, not inline at each call
/// site, so there is exactly one place that owns this platform switch.
///
/// `rawFutexWaitShared`/`futexWakeShared` take no `Io` and never consult
/// cancellation: a process that forked specifically to avoid using its
/// parent's `Io` instance (a thread-pool `Io`'s background threads do
/// not survive `fork`) still needs to park on and wake this word, so the
/// *raw* primitive has to work with no `Io` available at all.
/// `futexWaitSharedBounded` is the `Io`-aware wrapper every other caller
/// should prefer: it honors `io`'s cancellation and a caller-supplied
/// deadline by polling the raw wait in short steps instead of blocking
/// the whole span uninterrupted.
pub fn rawFutexWaitShared(ptr: *const u32, expect: u32, timeout_ms: ?u32) void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            if (timeout_ms) |ms| {
                const ts: linux.timespec = .{
                    .sec = @intCast(@divTrunc(ms, 1000)),
                    .nsec = @intCast(@as(i64, @intCast(@mod(ms, 1000))) * std.time.ns_per_ms),
                };
                _ = linux.futex_4arg(ptr, .{ .cmd = .WAIT, .private = false }, expect, &ts);
            } else {
                _ = linux.futex_4arg(ptr, .{ .cmd = .WAIT, .private = false }, expect, null);
            }
        },
        .macos, .ios, .tvos, .watchos, .visionos, .maccatalyst, .driverkit => {
            const c = std.c;
            const flags: c.UL = .{ .op = .COMPARE_AND_WAIT_SHARED, .NO_ERRNO = true };
            const timeout_us: u32 = if (timeout_ms) |ms| ms * 1000 else 0;
            _ = c.__ulock_wait(flags, @ptrCast(@constCast(ptr)), expect, timeout_us);
        },
        else => @compileError("cross-process futex wait not implemented for this target"),
    }
}

/// Wakes every waiter parked (via either futex function above) on `ptr`
/// in any process, including this one. Never blocks, so unlike the wait
/// side there is no `Io`-aware variant to prefer -- this is already safe
/// to call from a context with no `Io` available.
pub fn futexWakeShared(ptr: *const u32) void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            _ = linux.futex_4arg(ptr, .{ .cmd = .WAKE, .private = false }, std.math.maxInt(u32), null);
        },
        .macos, .ios, .tvos, .watchos, .visionos, .maccatalyst, .driverkit => {
            const c = std.c;
            const flags: c.UL = .{ .op = .COMPARE_AND_WAIT_SHARED, .WAKE_ALL = true, .NO_ERRNO = true };
            _ = c.__ulock_wake(flags, @ptrCast(@constCast(ptr)), 0);
        },
        else => @compileError("cross-process futex wake not implemented for this target"),
    }
}

/// Parks on `ptr` (a cross-process futex word) until it no longer equals
/// `expect`, bounded by `deadline` and honoring `io`'s own cancellation.
/// Used by every caller that has a safe `Io` to consult -- a freshly
/// forked process that must not touch `Io` yet uses `rawFutexWaitShared`
/// directly instead (see that function's own doc comment).
pub fn futexWaitSharedBounded(io: std.Io, ptr: *const u32, expect: u32, deadline: std.Io.Timestamp) std.Io.Cancelable!void {
    const poll_step_ms: u32 = 20;
    while (true) {
        try io.checkCancel();
        if (@atomicLoad(u32, ptr, .acquire) != expect) return;
        const now = std.Io.Clock.now(.awake, io);
        const remaining_ns = now.durationTo(deadline).nanoseconds;
        if (remaining_ns <= 0) return;
        const step_ms: u32 = @min(poll_step_ms, @as(u32, @intCast(@divTrunc(remaining_ns, std.time.ns_per_ms) + 1)));
        rawFutexWaitShared(ptr, expect, step_ms);
    }
}

/// `fcntl(F_SETLK)` *byte-range* record locking -- a different primitive
/// from `Io.File.lock`'s whole-file `flock`, needed because cross-
/// process liveness tracking specifically needs per-byte-range
/// granularity `flock` cannot give, and because (confirmed empirically
/// on at least one Darwin kernel) `flock` and `fcntl` locks are *not*
/// independent locking domains on every platform this runs on: holding
/// an exclusive `flock` via `Io.File.lock` blocked that same process's
/// own later `fcntl(F_SETLK)` attempt on the same file. Using `fcntl`
/// exclusively, never mixing it with `flock` on the same file in the
/// same process, avoids that conflict entirely.
///
/// The kernel drops a process's `fcntl` locks unconditionally on its
/// death (`exit`, `kill -9`, OOM-kill), with no cleanup-code dependency
/// -- the only liveness signal callers of this should trust. Two more
/// traps callers must be aware of, neither specific to this function:
/// `fcntl` locks are scoped per *process*, not per fd or per logical
/// claim (so re-locking a range this same process already holds, via
/// any fd on the same file, trivially succeeds), and closing *any* fd a
/// process holds on a file releases *all* of that process's `fcntl`
/// locks on it, not just the one being closed.
///
/// Nonblocking (`F_SETLK`, not `F_SETLKW`): on success the byte range is
/// now locked by this process; on failure (`EAGAIN`/`EACCES`) some other
/// live process already holds it. Never blocks, so (like
/// `futexWakeShared`) there is no `Io`-aware variant to prefer.
pub fn fcntlTryLock(fd: std.posix.fd_t, offset: i64, lock_type: i16) bool {
    // Zeroed first: some platforms' Flock carries padding fields.
    var fl = std.mem.zeroes(std.c.Flock);
    fl.start = offset;
    fl.len = 1;
    fl.type = lock_type;
    fl.whence = std.c.SEEK.SET;
    return std.c.fcntl(fd, std.c.F.SETLK, &fl) != -1;
}

/// Releases a byte range `fcntlTryLock` took. Never blocks.
pub fn fcntlUnlock(fd: std.posix.fd_t, offset: i64) void {
    // Zeroed first: some platforms' Flock carries padding fields.
    var fl = std.mem.zeroes(std.c.Flock);
    fl.start = offset;
    fl.len = 1;
    fl.type = std.c.F.UNLCK;
    fl.whence = std.c.SEEK.SET;
    _ = std.c.fcntl(fd, std.c.F.SETLK, &fl);
}

/// `fcntlTryLock`, bounded by `deadline` and honoring `io`'s own
/// cancellation instead of failing immediately: polls the nonblocking
/// trylock in short steps (never `F_SETLKW`, which -- being a real
/// blocking kernel wait with no deadline or cancellation of its own --
/// is exactly what this function exists to avoid) so both the deadline
/// and `io`'s cancellation are checked between attempts rather than
/// blocking the whole span uninterrupted.
///
/// Unlike `futexWaitSharedBounded`, each step here is a genuine
/// `io.sleep`, not a kernel wait with a timeout tied to the lock
/// itself: POSIX gives `futex`/`__ulock_wait` a timeout parameter, but
/// `fcntl`'s only blocking mode (`F_SETLKW`) has none, nor any way to
/// wake early the instant the range frees up. Sleep-polling a
/// nonblocking trylock is the correct shape for a bounded `fcntl`
/// acquire, not a stand-in for a kernel wait this primitive doesn't
/// offer. The only caller in this codebase (`shared_lock_region.zig`'s
/// one-time create/validate section) is rare enough that the short
/// step's own latency is immaterial.
pub fn fcntlLockBounded(io: std.Io, fd: std.posix.fd_t, offset: i64, lock_type: i16, deadline: std.Io.Timestamp) (std.Io.Cancelable || error{Timeout})!void {
    const poll_step_ms: u32 = 5;
    while (true) {
        try io.checkCancel();
        if (fcntlTryLock(fd, offset, lock_type)) return;
        const now = std.Io.Clock.now(.awake, io);
        const remaining_ns = now.durationTo(deadline).nanoseconds;
        if (remaining_ns <= 0) return error.Timeout;
        const step_ms: u32 = @min(poll_step_ms, @as(u32, @intCast(@divTrunc(remaining_ns, std.time.ns_per_ms) + 1)));
        try io.sleep(std.Io.Duration.fromMilliseconds(step_ms), .awake);
    }
}

test "fcntlTryLock/fcntlUnlock round-trip on a real file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(io, &buf);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/lockfile", .{buf[0..dir_len]});
    defer std.testing.allocator.free(path);

    const f = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer f.close(io);
    const fd = std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDWR }, 0) catch return error.SkipZigTest;
    defer _ = std.posix.system.close(fd);

    try std.testing.expect(fcntlTryLock(fd, 0, std.c.F.WRLCK));
    fcntlUnlock(fd, 0);
    // Re-lockable after unlock, from the same fd/process (an
    // uncontended round trip; cross-process contention is exercised by
    // `shared_lock_region.zig`'s own, much larger, test suite).
    try std.testing.expect(fcntlTryLock(fd, 0, std.c.F.WRLCK));
    fcntlUnlock(fd, 0);
}

test "futexWaitSharedBounded returns once the word changes" {
    var word: std.atomic.Value(u32) = .init(0);
    const io = std.testing.io;
    const deadline = std.Io.Clock.now(.awake, io).addDuration(.fromMilliseconds(2000));
    word.store(1, .release);
    // Already changed before the first check: returns immediately, well
    // under the deadline, not merely eventually.
    try futexWaitSharedBounded(io, &word.raw, 0, deadline);
}

test "futexWaitSharedBounded honors its deadline when nothing wakes it" {
    var word: std.atomic.Value(u32) = .init(0);
    const io = std.testing.io;
    const start = std.Io.Clock.now(.awake, io);
    const deadline = start.addDuration(.fromMilliseconds(100));
    try futexWaitSharedBounded(io, &word.raw, 0, deadline);
    const elapsed = start.durationTo(std.Io.Clock.now(.awake, io));
    // Bounded by the deadline, generously: well under a second, not
    // hung, and not wildly over the 100ms asked for either.
    try std.testing.expect(elapsed.nanoseconds < 1 * std.time.ns_per_s);
}

test "lockYielding acquires an uncontended mutex" {
    var mutex: std.atomic.Mutex = .unlocked;
    lockYielding(&mutex);
    defer mutex.unlock();
    try std.testing.expect(!mutex.tryLock());
}

test "lockYielding acquires a mutex released by another thread" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var mutex: std.atomic.Mutex = .unlocked;
    lockYielding(&mutex);

    const Holder = struct {
        fn release(m: *std.atomic.Mutex) void {
            m.unlock();
        }
    };
    var thread = try std.testing.io.concurrent(Holder.release, .{&mutex});
    lockYielding(&mutex);
    thread.await(std.testing.io);
    mutex.unlock();
}
