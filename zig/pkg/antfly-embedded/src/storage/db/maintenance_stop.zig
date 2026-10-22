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

//! Supervised, close-safe warming. Durable progress belongs to the repository;
//! this owner only schedules bounded turns and supplies cancellation.
const std = @import("std");
const Handle = @import("../../common/maintenance_scheduler.zig").Scheduler.Handle;
const admission = @import("../../common/read_admission.zig");

/// Synchronous borrow of a recurring owner's lifecycle fields. No registration,
/// operation context or execution control escapes to this helper. The owner
/// fences start with the same mutex and stopping flag.
pub const Borrowed = struct {
    mutex: *std.atomic.Mutex,
    registration: *?Handle,
    stopping: *std.atomic.Value(bool),
    finisher: *bool,
    joined: *std.Io.Event,
    changed: *std.Io.Event,

    fn lock(self: Borrowed) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    fn signal(event: *std.Io.Event, io: ?std.Io) void {
        if (io) |lane| event.set(lane) else @atomicStore(std.Io.Event, event, .is_set, .release);
    }
    fn complete(self: Borrowed, io: ?std.Io) void {
        self.lock();
        defer self.mutex.unlock();
        self.registration.* = null;
        self.finisher.* = false;
        signal(self.joined, io);
        signal(self.changed, io);
    }
    fn abandon(self: Borrowed, io: ?std.Io) void {
        self.lock();
        defer self.mutex.unlock();
        self.finisher.* = false;
        // Wake a competing quiesced joiner even though retirement failed.
        // joined must remain unset until physical completion.
        signal(self.changed, io);
    }
    fn available(self: Borrowed) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.joined.isSet() or !self.finisher.*;
    }
    /// Compatibility path requires quiesced cleanup ownership. Detaching the
    /// handle transfers its physical join to this exclusive finisher.
    pub fn stop(self: Borrowed, io: ?std.Io, cancel_active: bool) void {
        while (true) {
            self.lock();
            self.stopping.store(true, .release);
            if (self.joined.isSet()) {
                self.mutex.unlock();
                return;
            }
            if (self.finisher.*) {
                self.mutex.unlock();
                if (io) |lane| self.changed.waitUncancelable(lane) else @import("antfly_platform").time.yieldNow();
                continue;
            }
            self.finisher.* = true;
            self.changed.reset();
            var registration = self.registration.*;
            self.registration.* = null;
            self.mutex.unlock();
            if (registration) |*handle| {
                const lane = io orelse @panic("maintenance registration outlived runtime I/O");
                if (cancel_active) handle.cancel(lane) else handle.await(lane);
            }
            self.complete(io);
            return;
        }
    }
    /// Interruptible stop preserves the original registration after any failed
    /// begin, lifetime or reentry check. A competitor may resume that join.
    pub fn stopWithAdmission(self: Borrowed, io: ?std.Io, original: admission.Port) !void {
        self.stopping.store(true, .release);
        var trace: admission.RejectionTrace = .{};
        const control = trace.bind(original).?;
        const boundary = (try control.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
        _ = (try control.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
        while (true) {
            try control.checkpoint();
            self.lock();
            if (self.joined.isSet()) {
                self.mutex.unlock();
                return;
            }
            if (!self.finisher.*) {
                self.finisher.* = true;
                self.changed.reset();
                var registration = self.registration.*;
                self.mutex.unlock();
                errdefer self.abandon(io);
                if (registration) |*handle| {
                    const lane = io orelse return error.BackendRuntimeUnavailable;
                    handle.cancelWithAdmission(lane, control) catch |cause| return trace.failure orelse cause;
                }
                self.complete(io);
                return;
            }
            self.mutex.unlock();
            try boundary.beginWait();
            var failure: ?anyerror = null;
            while (!self.available()) {
                control.checkLifetime() catch |cause| {
                    failure = trace.failure orelse cause;
                    break;
                };
                @import("antfly_platform").time.yieldNow();
            }
            boundary.endWait() catch |cause| {
                if (failure) |first| return first;
                return trace.failure orelse cause;
            };
            if (failure) |cause| return cause;
        }
    }
};

test "maintenance runtime controlled owner stops preserve registrations across original failures" {
    const runtime = @import("../background_runtime.zig");
    const independent = @import("independent_maintenance.zig").Owner;
    const graph = @import("graph_cleanup_owner.zig").Owner;
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    inline for (.{ independent, graph, @import("quarantine_recovery.zig").Owner, @import("native_query_warming.zig").Owner }) |Owner| {
        for (std.enums.values(Case)) |case| {
            var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{});
            defer backend.deinit();
            const io = backend.ptr().io().?;
            var owner: Owner = .{};
            const F = struct {
                io: std.Io,
                entered: std.Io.Event = .unset,
                release: std.Io.Event = .unset,
                retired: bool = false,
                fn run(self: *@This()) ?u64 {
                    self.entered.set(self.io);
                    self.release.waitUncancelable(self.io);
                    self.retired = true;
                    return null;
                }
            };
            const Control = struct {
                f: *F,
                case: Case,
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
                    if (self.case != .lifetime_in_wait) self.f.release.set(self.f.io);
                }
                fn end(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.ends += 1;
                    self.f.release.set(self.f.io);
                    if (self.case == .end_failure) return error.Canceled;
                }
            };
            var f: F = .{ .io = io };
            defer {
                f.release.set(io);
                if (Owner == graph) owner.stop(io) else owner.stop(backend.ptr());
            }
            const slot = if (@hasField(Owner, "future")) &owner.future else &owner.registration;
            const scheduler = try backend.ptr().maintenanceScheduler();
            slot.* = try scheduler.register(&f, F.run);
            try f.entered.wait(io);
            var controls: Control = .{ .f = &f, .case = case };
            var payload: admission.Payload = .{ .ptr = &controls, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &controls, .begin = Control.begin, .end = Control.end } };
            const result = if (Owner == graph) owner.stopWithAdmission(io, .init(&payload)) else owner.stopWithAdmission(backend.ptr(), .init(&payload));
            if (case == .success) try result else {
                try std.testing.expectError(switch (case) {
                    .begin_failure => error.NotFound,
                    .end_failure => error.Canceled,
                    .lifetime_in_wait => error.DeadlineExceeded,
                    .success => unreachable,
                }, result);
                try std.testing.expect(slot.* != null and !owner.stop_started and !owner.joined.isSet());
                try std.testing.expect(owner.stop_progress.isSet());
                try std.testing.expectEqual(@as(usize, 1), scheduler.snapshot().owned_registrations);
                f.release.set(io);
                if (Owner == graph) owner.stop(io) else owner.stop(backend.ptr());
            }
            try std.testing.expect(owner.stopping.load(.acquire));
            try std.testing.expect(owner.joined.isSet() and slot.* == null and f.retired);
            try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
            try std.testing.expectEqual(@as(usize, 1), controls.begins);
            try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), controls.ends);
        }
    }
}

test "maintenance runtime abandoned controlled finisher wakes a competing quiesced joiner" {
    const runtime = @import("../background_runtime.zig");
    const Owner = @import("independent_maintenance.zig").Owner;
    var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer backend.deinit();
    const io = backend.ptr().io().?;
    var owner: Owner = .{};
    const F = struct {
        owner: *Owner,
        backend: *runtime.BackendRuntime,
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        thread: ?std.Thread = null,
        joined: bool = false,
        fn run(self: *@This()) ?u64 {
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            return null;
        }
        fn stop(self: *@This()) void {
            self.owner.stop(self.backend);
            self.joined = true;
        }
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.thread = try std.Thread.spawn(.{}, stop, .{self});
            const until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
            while (@atomicLoad(std.Io.Event, &self.owner.stop_progress, .acquire) != .waiting) {
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestUnexpectedResult;
                @import("antfly_platform").time.yieldNow();
            }
            return error.NotFound;
        }
        fn end(_: *anyopaque) !void {
            @panic("failed begin must not reenter");
        }
    };
    var f: F = .{ .owner = &owner, .backend = backend.ptr(), .io = io };
    defer {
        f.release.set(io);
        if (f.thread) |thread| thread.join();
        owner.stop(backend.ptr());
    }
    const scheduler = try backend.ptr().maintenanceScheduler();
    owner.future = try scheduler.register(&f, F.run);
    try f.entered.wait(io);
    var payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
    try std.testing.expectError(error.NotFound, owner.stopWithAdmission(backend.ptr(), .init(&payload)));
    f.release.set(io);
    f.thread.?.join();
    f.thread = null;
    try std.testing.expect(f.joined and owner.joined.isSet());
    try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
}
