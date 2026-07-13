// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

/// Process-local hard budget shared by independently implemented caches.
/// Consumers retain their own eviction policy and reserve before publishing
/// cache-owned memory.
pub const CacheBudget = struct {
    max_bytes: usize,
    used_bytes: std.atomic.Value(usize) = .init(0),
    rejected_reservations: std.atomic.Value(u64) = .init(0),

    pub const Stats = struct {
        max_bytes: usize,
        used_bytes: usize,
        rejected_reservations: u64,
    };

    pub fn init(max_bytes: usize) CacheBudget {
        return .{ .max_bytes = max_bytes };
    }

    pub fn tryReserve(self: *CacheBudget, bytes: usize) bool {
        if (bytes == 0) return true;
        var used = self.used_bytes.load(.acquire);
        while (true) {
            const limit = self.max_bytes;
            if (bytes > limit or used > limit - bytes) {
                _ = self.rejected_reservations.fetchAdd(1, .monotonic);
                return false;
            }
            used = self.used_bytes.cmpxchgWeak(used, used + bytes, .acq_rel, .acquire) orelse return true;
        }
    }

    pub fn release(self: *CacheBudget, bytes: usize) void {
        if (bytes == 0) return;
        const previous = self.used_bytes.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
    }

    pub fn stats(self: *const CacheBudget) Stats {
        return .{
            .max_bytes = self.max_bytes,
            .used_bytes = self.used_bytes.load(.acquire),
            .rejected_reservations = self.rejected_reservations.load(.monotonic),
        };
    }
};

pub fn testHardLimit() !void {
    var budget = CacheBudget.init(100);
    try std.testing.expect(budget.tryReserve(60));
    try std.testing.expect(!budget.tryReserve(41));
    try std.testing.expect(budget.tryReserve(40));
    budget.release(75);
    try std.testing.expect(budget.tryReserve(75));

    const current = budget.stats();
    try std.testing.expectEqual(@as(usize, 100), current.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), current.rejected_reservations);
}

test "cache budget atomically enforces its hard limit" {
    try testHardLimit();
}
