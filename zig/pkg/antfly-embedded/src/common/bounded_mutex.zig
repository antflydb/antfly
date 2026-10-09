// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Cooperative mutex admission using the original caller's wait authority.
const std = @import("std");
pub fn lock(mutex: *std.Io.Mutex, unlock_io: std.Io, budget: anytype) !void {
    while (true) {
        try budget.checkpoint();
        if (mutex.tryLock()) {
            // Cancellation racing successful acquisition must release the
            // lock before returning; the caller receives ownership only here.
            errdefer mutex.unlock(unlock_io);
            try budget.checkpoint();
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

/// Atomic backend mutexes use the same original progress capability.
pub fn lockAtomic(mutex: *std.atomic.Mutex, budget: anytype) !void {
    while (true) {
        try budget.checkpoint();
        if (mutex.tryLock()) {
            errdefer mutex.unlock();
            try budget.checkpoint();
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
            try budget.checkpoint();
            return;
        }
        try budget.waitForProgress();
    }
}
