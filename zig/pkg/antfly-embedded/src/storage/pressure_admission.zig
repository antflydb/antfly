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

const read = @import("../common/read_admission.zig");

/// No reservation or new lifetime is created here. A progress callback performs
/// one finite IO tick under the caller's existing holder/CPU policy.
pub fn awaitAdmission(manager: anytype, slice: anytype, additional_bytes: u64, control: read.Port) !void {
    return awaitAdmissionWithPolicy(manager, slice, additional_bytes, control, false);
}

/// Advisory admission before a foreground decision. Soft pacing must not hold
/// capacity needed by its producer; the later allocation/WAL hard guard remains
/// authoritative. Hard pressure rejects non-lending callers without waiting.
pub fn awaitAdmissionPreDecision(manager: anytype, slice: anytype, additional_bytes: u64, control: read.Port) !void {
    return awaitAdmissionWithPolicy(manager, slice, additional_bytes, control, true);
}

fn awaitAdmissionWithPolicy(manager: anytype, slice: anytype, additional_bytes: u64, control: read.Port, comptime pre_decision: bool) !void {
    while (true) {
        try control.checkpoint();
        const decision = try manager.admissionDecisionWithAdmission(slice, additional_bytes, control);
        try control.checkpoint();
        if (decision.hard_limit_bytes > 0 and additional_bytes > decision.hard_limit_bytes)
            return error.ResourceBudgetExceeded;
        switch (decision.action) {
            .report, .shrink_cache, .defer_background_work => return,
            .reject_work => return error.ResourceBudgetExceeded,
            .throttle_writes => {
                if (decision.pressure == .soft) {
                    if (pre_decision and !try control.hasExecutionWait()) return;
                    // Match legacy 10 ms pacing using bounded 1 ms ticks so
                    // cancellation and expiry can interrupt the delay.
                    for (0..10) |_| try control.waitForProgress();
                    return;
                }
            },
        }
        if (!manager.canWaitForPressureChange()) return error.ResourceBudgetExceeded;
        if (pre_decision and !try control.hasExecutionWait()) return error.Backpressured;
        // Do not use the legacy uninterruptible epoch wait. Re-evaluate current
        // pressure after a finite tick; notifications remain an optimization.
        try control.waitForProgress();
    }
}

test "controlled pressure admission preserves policy and original rejection while waiting" {
    const std = @import("std");
    const Manager = struct {
        const Pressure = enum { normal, soft, hard };
        const Action = enum { report, shrink_cache, defer_background_work, reject_work, throttle_writes };
        pressure: Pressure = .hard,
        action: Action = .throttle_writes,
        can_wait: bool = true,
        calls: usize = 0,
        pub fn admissionDecisionWithAdmission(self: *@This(), _: void, _: u64, _: read.Port) !struct { pressure: Pressure, action: Action, hard_limit_bytes: u64 } {
            self.calls += 1;
            return .{ .pressure = self.pressure, .action = self.action, .hard_limit_bytes = 20 };
        }
        pub fn canWaitForPressureChange(self: *@This()) bool {
            return self.can_wait;
        }
    };
    const Authority = struct {
        manager: *Manager,
        waits: usize = 0,
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        clear_after: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            if (self.waits == self.clear_after) {
                self.manager.pressure = .normal;
                self.manager.action = .report;
            }
        }
    };
    var manager: Manager = .{};
    var authority: Authority = .{ .manager = &manager, .clear_after = 3 };
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check, .wait_for_progress = Authority.wait } };
    const control = read.Port.init(&payload);
    try awaitAdmission(&manager, {}, 1, control);
    try std.testing.expectEqual(@as(usize, 3), authority.waits);
    try std.testing.expectEqual(@as(usize, 4), manager.calls);
    const checks = authority.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        for (1..checks + 1) |fail_at| {
            manager = .{};
            authority = .{ .manager = &manager, .clear_after = 3, .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, awaitAdmission(&manager, {}, 1, control));
            try std.testing.expectEqual(fail_at, authority.checks);
        }
    }
    manager = .{ .pressure = .soft };
    authority = .{ .manager = &manager };
    try awaitAdmission(&manager, {}, 1, control);
    try std.testing.expectEqual(@as(usize, 10), authority.waits);
    manager = .{};
    authority = .{ .manager = &manager };
    try std.testing.expectError(error.ResourceBudgetExceeded, awaitAdmission(&manager, {}, 21, control));
    try std.testing.expectEqual(@as(usize, 0), authority.waits);
    manager = .{ .can_wait = false };
    try std.testing.expectError(error.ResourceBudgetExceeded, awaitAdmission(&manager, {}, 1, control));
    for ([_]Manager.Action{ .report, .shrink_cache, .defer_background_work, .reject_work }) |action| {
        manager = .{ .action = action };
        if (action == .reject_work) {
            try std.testing.expectError(error.ResourceBudgetExceeded, awaitAdmission(&manager, {}, 1, control));
        } else try awaitAdmission(&manager, {}, 1, control);
        try std.testing.expectEqual(@as(usize, 0), authority.waits);
    }
}
