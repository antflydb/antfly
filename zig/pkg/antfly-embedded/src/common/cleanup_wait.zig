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

//! Synchronous cleanup waits retain the original execution and deadline.
const std = @import("std");
const admission = @import("read_admission.zig");

pub fn until(io: ?std.Io, original: admission.Port, context: *anyopaque, ready: *const fn (*anyopaque) anyerror!bool, event: ?*std.Io.Event) !void {
    var trace: admission.RejectionTrace = .{};
    const control = trace.bind(original).?;
    const boundary = (try control.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
    _ = (try control.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
    while (true) {
        try control.checkpoint();
        if (try ready(context)) return;
        const lane = io orelse return error.BackendRuntimeUnavailable;
        try boundary.beginWait();
        var failure: ?anyerror = null;
        while (true) {
            control.checkLifetime() catch |cause| {
                failure = trace.failure orelse cause;
                break;
            };
            const complete = ready(context) catch |cause| {
                failure = cause;
                break;
            };
            if (complete) break;
            if (event) |notification| {
                notification.waitTimeout(lane, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }) catch |cause| switch (cause) {
                    error.Timeout => {},
                    else => {
                        failure = cause;
                        break;
                    },
                };
            } else lane.sleep(.fromMilliseconds(1), .awake) catch |cause| {
                failure = cause;
                break;
            };
        }
        boundary.endWait() catch |cause| {
            if (failure) |first| return first;
            return trace.failure orelse cause;
        };
        if (failure) |cause| return cause;
    }
}

/// Retry a failed nonblocking acquisition after one physical polling wait.
/// The caller must release every service guard before entering this function;
/// the next acquisition attempt belongs strictly after successful reentry.
pub fn waitForRetry(io: std.Io, original: admission.Port) !void {
    var trace: admission.RejectionTrace = .{};
    const control = trace.bind(original).?;
    const boundary = (try control.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
    _ = (try control.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
    try control.checkpoint();
    try boundary.beginWait();
    var failure: ?anyerror = null;
    control.checkLifetime() catch |cause| {
        failure = trace.failure orelse cause;
    };
    if (failure == null) io.sleep(.fromMilliseconds(1), .awake) catch |cause| {
        failure = cause;
    };
    control.checkLifetime() catch |cause| {
        if (failure == null) failure = trace.failure orelse cause;
    };
    boundary.endWait() catch |cause| {
        if (failure) |first| return first;
        return trace.failure orelse cause;
    };
    if (failure) |cause| return cause;
    try control.checkpoint();
}

/// Readiness observation never takes a guard while execution is parked.
/// Acquire only after reentry; contention races simply retry the same root.
pub fn lockIo(mutex: *std.Io.Mutex, io: std.Io, original: admission.Port) !void {
    const Ready = struct {
        fn get(raw: *anyopaque) anyerror!bool {
            const value: *std.Io.Mutex = @ptrCast(@alignCast(raw));
            return value.state.load(.acquire) == .unlocked;
        }
    };
    while (true) {
        try until(io, original, mutex, Ready.get, null);
        if (mutex.tryLock()) return;
    }
}
pub fn lockAtomic(mutex: *std.atomic.Mutex, io: ?std.Io, original: admission.Port) !void {
    const Ready = struct {
        fn get(raw: *anyopaque) anyerror!bool {
            const value: *std.atomic.Mutex = @ptrCast(@alignCast(raw));
            return @atomicLoad(std.atomic.Mutex, value, .acquire) == .unlocked;
        }
    };
    while (true) {
        try until(io, original, mutex, Ready.get, null);
        if (mutex.tryLock()) return;
    }
}

test "maintenance runtime cleanup mutex wait never retains a guard after original reentry failure" {
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    for (std.enums.values(Case)) |case| {
        var mutex: std.Io.Mutex = .init;
        try std.testing.expect(mutex.tryLock());
        const F = struct {
            mutex: *std.Io.Mutex,
            case: Case,
            released: bool = false,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.case == .lifetime_in_wait and self.begins != 0) return error.DeadlineExceeded;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.case == .begin_failure) return error.NotFound;
                if (self.case != .lifetime_in_wait) {
                    self.mutex.unlock(std.testing.io);
                    self.released = true;
                }
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                if (!self.released) {
                    self.mutex.unlock(std.testing.io);
                    self.released = true;
                }
                if (self.case == .end_failure) return error.Canceled;
            }
        };
        var f: F = .{ .mutex = &mutex, .case = case };
        defer if (!f.released) mutex.unlock(std.testing.io);
        var payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
        const result = lockIo(&mutex, std.testing.io, .init(&payload));
        if (case == .success) {
            try result;
            try std.testing.expect(!mutex.tryLock());
            mutex.unlock(std.testing.io);
        } else {
            try std.testing.expectError(switch (case) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .lifetime_in_wait => error.DeadlineExceeded,
                .success => unreachable,
            }, result);
            if (f.released) {
                try std.testing.expect(mutex.tryLock());
                mutex.unlock(std.testing.io);
            }
        }
        try std.testing.expectEqual(@as(usize, 1), f.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), f.ends);
    }
}
