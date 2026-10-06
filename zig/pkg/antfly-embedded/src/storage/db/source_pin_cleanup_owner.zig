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
/// Retry/fairness diagnostics only. Durable released-pin authority and bounded
/// filesystem reconciliation remain with source_pin and source_pin_gc.
pub const Status = struct { pending: bool, failures: u64, last_error: ?[]const u8, next_attempt_ns: u64 };
pub const Owner = struct {
    epoch: @import("antfly_platform").atomic.Value(u64) = .init(1),
    turn: @import("antfly_platform").atomic.Value(u64) = .init(0),
    next_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    failures: @import("antfly_platform").atomic.Value(u64) = .init(0),
    failure_streak: @import("antfly_platform").atomic.Value(u64) = .init(0),
    work_units: @import("antfly_platform").atomic.Value(u64) = .init(0),
    error_code: std.atomic.Value(u32) = .init(0),
    log_next_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    pub fn takeTurn(self: *Owner) bool {
        return self.turn.fetchAdd(1, .monotonic) % 4 == 0;
    }
    pub fn due(self: *Owner, now: u64) bool {
        return self.epoch.load(.acquire) != 0 and now >= self.next_ns.load(.acquire);
    }
    pub fn succeeded(self: *Owner, progressed: bool, now: u64) void {
        self.error_code.store(0, .release);
        self.failure_streak.store(0, .release);
        self.next_ns.store(now +| (if (progressed) @as(u64, std.time.ns_per_ms) else 20 * std.time.ns_per_ms), .release);
    }
    pub fn failed(self: *Owner, err: anyerror, progressed: bool, now: u64, path: []const u8) void {
        const failures = self.failures.fetchAdd(1, .acq_rel) +| 1;
        const streak = self.failure_streak.fetchAdd(1, .acq_rel) +| 1;
        const previous = self.error_code.swap(@intFromError(err), .acq_rel);
        const delay = if (progressed) std.time.ns_per_ms else (20 * std.time.ns_per_ms) * (@as(u64, 1) << @as(u6, @intCast(@min(streak - 1, 8))));
        self.next_ns.store(now +| delay, .release);
        const log_after = self.log_next_ns.load(.acquire);
        if ((previous != @intFromError(err) or std.math.isPowerOfTwo(streak)) and now >= log_after and
            self.log_next_ns.cmpxchgStrong(log_after, now +| 30 * std.time.ns_per_s, .acq_rel, .acquire) == null)
        {
            std.log.warn("source pin cleanup pending path={s} err={s} failures={} retry_ms={}", .{ path, @errorName(err), failures, delay / std.time.ns_per_ms });
        }
    }

    pub fn status(self: *Owner) Status {
        const code = self.error_code.load(.acquire);
        return .{
            .pending = self.epoch.load(.acquire) != 0,
            .failures = self.failures.load(.acquire),
            .last_error = if (code == 0) null else @errorName(@errorFromInt(@as(u16, @intCast(code)))),
            .next_attempt_ns = self.next_ns.load(.acquire),
        };
    }
};

test "source pin cleanup supervisor preserves bounded backoff and progress priority" {
    var owner: Owner = .{};
    var turns: usize = 0;
    for (0..12) |_| if (owner.takeTurn()) {
        turns += 1;
    };
    try std.testing.expectEqual(@as(usize, 3), turns);
    for (0..12) |_| owner.failed(error.InjectedCleanupFailure, false, 0, "test");
    try std.testing.expectEqual(@as(u64, 5120 * std.time.ns_per_ms), owner.status().next_attempt_ns);
    try std.testing.expect(!owner.due(1));
    owner.failed(error.InjectedCleanupFailure, true, 10, "test");
    try std.testing.expectEqual(@as(u64, 10 + std.time.ns_per_ms), owner.status().next_attempt_ns);
    try std.testing.expectEqualStrings("InjectedCleanupFailure", owner.status().last_error.?);
    owner.succeeded(true, 20);
    try std.testing.expect(owner.status().last_error == null);
    try std.testing.expectEqual(@as(u64, 0), owner.failure_streak.load(.acquire));
    try std.testing.expectEqual(@as(u64, 13), owner.status().failures);
    owner.epoch.store(0, .release);
    try std.testing.expect(!owner.due(std.math.maxInt(u64)));
}
