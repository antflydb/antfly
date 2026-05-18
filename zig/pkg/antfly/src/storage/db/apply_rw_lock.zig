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
const platform_time = @import("../../platform/time.zig");
const AtomicU64 = platform.atomic.Value(u64);

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

test "apply rw lock supports repeated shared acquisition" {
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
