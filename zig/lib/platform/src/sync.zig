// Copyright 2026 Antfly, Inc.
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

/// Try to acquire `mutex` once before falling back to `lockYielding`.
/// Returns `true` when the fast uncontended attempt acquired the lock.
pub fn lockAtomic(mutex: anytype) bool {
    if (mutex.tryLock()) return true;
    lockYielding(mutex);
    return false;
}

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

test "lockYielding acquires an uncontended mutex" {
    var mutex: std.atomic.Mutex = .unlocked;
    lockYielding(&mutex);
    defer mutex.unlock();
    try std.testing.expect(!mutex.tryLock());
}

test "lockAtomic reports uncontended acquisition" {
    var mutex: std.atomic.Mutex = .unlocked;
    try std.testing.expect(lockAtomic(&mutex));
    mutex.unlock();
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
    const thread = try std.Thread.spawn(.{}, Holder.release, .{&mutex});
    lockYielding(&mutex);
    thread.join();
    mutex.unlock();
}
