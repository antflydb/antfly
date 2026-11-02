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

//! Cooperative mutex admission using the original caller's wait authority.
const std = @import("std");
// The acquired mutex may protect a dependency another admitted task needs.
// Lifetime checks must not yield CPU; cooperative checkpoints belong before
// acquisition or after the caller has released the protected dependency.
fn checkAcquired(budget: anytype) !void {
    const T = switch (@typeInfo(@TypeOf(budget))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(budget),
    };
    if (@hasDecl(T, "checkLifetime")) try budget.checkLifetime() else try budget.checkpoint();
}

pub fn lock(mutex: *std.Io.Mutex, unlock_io: std.Io, budget: anytype) !void {
    while (true) {
        try budget.checkpoint();
        if (mutex.tryLock()) {
            // Cancellation racing successful acquisition must release the
            // lock before returning; the caller receives ownership only here.
            errdefer mutex.unlock(unlock_io);
            try checkAcquired(budget);
            return;
        }
        try budget.waitForProgress();
    }
}

test "transaction routing control port wait releases capacity to the original progress callback" {
    var mutex: std.Io.Mutex = .init;
    try std.testing.expect(mutex.tryLock());
    const Budget = struct {
        mutex: *std.Io.Mutex,
        waits: usize = 0,
        fn checkpoint(_: *@This()) !void {}
        fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.mutex.unlock(std.testing.io);
        }
    };
    var budget = Budget{ .mutex = &mutex };
    try lock(&mutex, std.testing.io, &budget);
    defer mutex.unlock(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(!mutex.tryLock());
}

test "transaction routing control port wait preserves scoped failure without taking another owner's lock" {
    var mutex: std.Io.Mutex = .init;
    try std.testing.expect(mutex.tryLock());
    defer mutex.unlock(std.testing.io);
    const Budget = struct {
        failed: bool = false,
        waits: usize = 0,
        fn checkpoint(self: *@This()) !void {
            if (self.failed) return error.GenerationPublicationChanged;
        }
        fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.failed = true;
        }
    };
    var budget = Budget{};
    try std.testing.expectError(error.GenerationPublicationChanged, lock(&mutex, std.testing.io, &budget));
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(!mutex.tryLock());
}

test "transaction routing control port wait releases a newly acquired lock on admission failure" {
    var mutex: std.Io.Mutex = .init;
    const Budget = struct {
        checks: usize = 0,
        fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks == 2) return error.Canceled;
        }
        fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    try std.testing.expectError(error.Canceled, lock(&mutex, std.testing.io, &budget));
    try std.testing.expect(mutex.tryLock());
    mutex.unlock(std.testing.io);
}

/// Atomic backend and publication mutexes use the same original progress
/// capability. A failed tryLock must leave no ticket or ownership behind.
pub fn lockAtomic(mutex: anytype, budget: anytype) !void {
    while (true) {
        try budget.checkpoint();
        if (mutex.tryLock()) {
            errdefer mutex.unlock();
            try checkAcquired(budget);
            return;
        }
        try budget.waitForProgress();
    }
}

test "backend atomic admission releases capacity to the original progress callback" {
    var mutex: std.atomic.Mutex = .unlocked;
    try std.testing.expect(mutex.tryLock());
    const Budget = struct {
        mutex: *std.atomic.Mutex,
        waits: usize = 0,
        fn checkpoint(_: *@This()) !void {}
        fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.mutex.unlock();
        }
    };
    var budget = Budget{ .mutex = &mutex };
    try lockAtomic(&mutex, &budget);
    defer mutex.unlock();
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(!mutex.tryLock());
}

test "backend atomic admission preserves scoped failure without taking another owner's lock" {
    var mutex: std.atomic.Mutex = .unlocked;
    try std.testing.expect(mutex.tryLock());
    defer mutex.unlock();
    const Budget = struct {
        failed: bool = false,
        waits: usize = 0,
        fn checkpoint(self: *@This()) !void {
            if (self.failed) return error.GenerationPublicationChanged;
        }
        fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.failed = true;
        }
    };
    var budget = Budget{};
    try std.testing.expectError(error.GenerationPublicationChanged, lockAtomic(&mutex, &budget));
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(!mutex.tryLock());
}

test "backend atomic admission releases a newly acquired lock on admission failure" {
    var mutex: std.atomic.Mutex = .unlocked;
    const Budget = struct {
        checks: usize = 0,
        fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks == 2) return error.Canceled;
        }
        fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    try std.testing.expectError(error.Canceled, lockAtomic(&mutex, &budget));
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}

/// Read generation acquisition uses the same original wait authority.
pub fn lockShared(mutex: *std.Io.RwLock, unlock_io: std.Io, budget: anytype) !void {
    while (true) {
        try budget.checkpoint();
        if (mutex.tryLockShared(unlock_io)) {
            errdefer mutex.unlockShared(unlock_io);
            try checkAcquired(budget);
            return;
        }
        try budget.waitForProgress();
    }
}
