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

//! Recurring callback retirement preserves original execution and ownership.
const std = @import("std");
const Handle = @import("../../common/maintenance_scheduler.zig").Scheduler.Handle;
const admission = @import("../../common/read_admission.zig");

/// Synchronous borrow of a recurring owner's lifecycle fields. No registration,
/// operation context or execution control escapes to this helper. The owner
/// fences start with the same mutex and stopping flag.
pub const Borrowed = BorrowedFor(std.atomic.Mutex);
pub const BorrowedIo = BorrowedFor(std.Io.Mutex);

fn BorrowedFor(comptime Mutex: type) type {
    return struct {
        const Self = @This();
        mutex: *Mutex,
        registration: *?Handle,
        stopping: *std.atomic.Value(bool),
        finisher: *bool,
        joined: *std.Io.Event,
        changed: *std.Io.Event,
        generation: *std.atomic.Value(u64),
        waiters: *std.atomic.Value(u32),
        cancel_active: bool = false,

        fn lock(self: Self, io: ?std.Io) void {
            if (Mutex == std.Io.Mutex) {
                self.mutex.lockUncancelable(io orelse std.Options.debug_io);
            } else {
                while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
            }
        }
        fn unlock(self: Self, io: ?std.Io) void {
            if (Mutex == std.Io.Mutex) self.mutex.unlock(io orelse std.Options.debug_io) else self.mutex.unlock();
        }
        fn lockWithAdmission(self: Self, io: ?std.Io, control: admission.Port) !void {
            if (Mutex == std.Io.Mutex) {
                try @import("../../common/cleanup_wait.zig").lockIo(self.mutex, io orelse std.Options.debug_io, control);
            } else {
                try @import("../../common/cleanup_wait.zig").lockAtomic(self.mutex, io, control);
            }
        }
        fn signal(event: *std.Io.Event, io: ?std.Io) void {
            if (io) |lane| event.set(lane) else @atomicStore(std.Io.Event, event, .is_set, .release);
        }
        fn complete(self: Self, io: ?std.Io) void {
            self.lock(io);
            defer self.unlock(io);
            self.registration.* = null;
            self.finisher.* = false;
            signal(self.joined, io);
            _ = self.generation.fetchAdd(1, .release);
            signal(self.changed, io);
        }
        fn abandon(self: Self, io: ?std.Io) void {
            self.lock(io);
            defer self.unlock(io);
            self.finisher.* = false;
            // Wake a competing quiesced joiner even though retirement failed.
            // joined must remain unset until physical completion.
            _ = self.generation.fetchAdd(1, .release);
            signal(self.changed, io);
        }
        /// Compatibility path requires quiesced cleanup ownership. Detaching the
        /// handle transfers its physical join to this exclusive finisher.
        pub fn stop(self: Self, io: ?std.Io, cancel_active: bool) void {
            while (true) {
                self.lock(io);
                self.stopping.store(true, .release);
                if (self.joined.isSet()) {
                    self.unlock(io);
                    return;
                }
                if (self.finisher.*) {
                    const observed = self.generation.load(.acquire);
                    _ = self.waiters.fetchAdd(1, .acquire);
                    self.unlock(io);
                    while (self.generation.load(.acquire) == observed) {
                        if (io) |lane| lane.sleep(.fromMilliseconds(1), .awake) catch {} else @import("antfly_platform").time.yieldNow();
                    }
                    _ = self.waiters.fetchSub(1, .release);
                    continue;
                }
                self.finisher.* = true;
                var registration = self.registration.*;
                self.registration.* = null;
                self.unlock(io);
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
        pub fn stopWithAdmission(self: Self, io: ?std.Io, original: admission.Port) !void {
            self.stopping.store(true, .release);
            var trace: admission.RejectionTrace = .{};
            const control = trace.bind(original).?;
            _ = (try control.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            _ = (try control.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
            while (true) {
                try control.checkpoint();
                try self.lockWithAdmission(io, control);
                if (self.joined.isSet()) {
                    self.unlock(io);
                    return;
                }
                if (!self.finisher.*) {
                    self.finisher.* = true;
                    var registration = self.registration.*;
                    self.unlock(io);
                    errdefer self.abandon(io);
                    if (registration) |*handle| {
                        const lane = io orelse return error.BackendRuntimeUnavailable;
                        const result = if (self.cancel_active) handle.cancelActiveWithAdmission(lane, control) else handle.cancelWithAdmission(lane, control);
                        result catch |cause| return trace.failure orelse cause;
                    }
                    self.complete(io);
                    return;
                }
                const observed = self.generation.load(.acquire);
                _ = self.waiters.fetchAdd(1, .acquire);
                self.unlock(io);
                defer _ = self.waiters.fetchSub(1, .release);
                const Progress = struct {
                    generation: *std.atomic.Value(u64),
                    previous: u64,
                    fn ready(raw: *anyopaque) anyerror!bool {
                        const value: *@This() = @ptrCast(@alignCast(raw));
                        return value.generation.load(.acquire) != value.previous;
                    }
                };
                var progress: Progress = .{ .generation = self.generation, .previous = observed };
                try @import("../../common/cleanup_wait.zig").until(io, control, &progress, Progress.ready, null);
            }
        }
    };
}

// Exercise Io lifecycle ownership through the same rejection and concurrency
// matrix as production atomic-mutex owners.
const IoStopFixture = struct {
    mutex: std.Io.Mutex = .init,
    future: ?Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    stop_progress: std.Io.Event = .unset,
    stop_generation: std.atomic.Value(u64) = .init(0),
    stop_waiters: std.atomic.Value(u32) = .init(0),
    fn borrow(self: *@This()) BorrowedIo {
        return .{ .mutex = &self.mutex, .registration = &self.future, .stopping = &self.stopping, .finisher = &self.stop_started, .joined = &self.joined, .changed = &self.stop_progress, .generation = &self.stop_generation, .waiters = &self.stop_waiters };
    }
    fn stop(self: *@This(), backend: *@import("../background_runtime.zig").BackendRuntime) void {
        self.borrow().stop(backend.io(), false);
    }
    fn stopWithAdmission(self: *@This(), backend: *@import("../background_runtime.zig").BackendRuntime, original: admission.Port) !void {
        try self.borrow().stopWithAdmission(backend.io(), original);
    }
};

test "maintenance runtime controlled owner stops preserve registrations across original failures" {
    const runtime = @import("../background_runtime.zig");
    const independent = @import("independent_maintenance.zig").Owner;
    const graph = @import("graph_cleanup_owner.zig").Owner;
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    inline for (.{ IoStopFixture, independent, graph, @import("quarantine_recovery.zig").Owner, @import("native_query_warming.zig").Owner, @import("native_projection_owner.zig").Owner }) |Owner| {
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
                if (Owner == graph or Owner == @import("native_projection_owner.zig").Owner) owner.stop(io) else owner.stop(backend.ptr());
            }
            const slot = if (@hasField(Owner, "future")) &owner.future else &owner.registration;
            const scheduler = try backend.ptr().maintenanceScheduler();
            slot.* = try scheduler.register(&f, F.run);
            try f.entered.wait(io);
            var controls: Control = .{ .f = &f, .case = case };
            var payload: admission.Payload = .{ .ptr = &controls, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &controls, .begin = Control.begin, .end = Control.end } };
            const result = if (Owner == graph or Owner == @import("native_projection_owner.zig").Owner) owner.stopWithAdmission(io, .init(&payload)) else owner.stopWithAdmission(backend.ptr(), .init(&payload));
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
                if (Owner == graph or Owner == @import("native_projection_owner.zig").Owner) owner.stop(io) else owner.stop(backend.ptr());
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
    inline for (.{ @import("independent_maintenance.zig").Owner, IoStopFixture }) |Owner| {
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
                while (self.owner.stop_waiters.load(.acquire) == 0) {
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
}

test "maintenance runtime controlled lifecycle mutex contention lends CPU before claiming a finisher" {
    const runtime = @import("../background_runtime.zig");
    const Owner = @import("graph_cleanup_owner.zig").Owner;
    const Mode = enum { success, begin_failure, end_failure, expired };
    for (std.enums.values(Mode)) |mode| {
        var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{});
        defer backend.deinit();
        const io = backend.ptr().io().?;
        var owner: Owner = .{};
        const F = struct {
            owner: *Owner,
            io: std.Io,
            mode: Mode,
            held: bool = false,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.mode == .expired and self.begins != 0) return error.DeadlineExceeded;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.mode == .begin_failure) return error.NotFound;
                self.owner.mutex.unlock();
                self.held = false;
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                try std.testing.expect(self.owner.mutex.tryLock());
                self.owner.mutex.unlock();
                if (self.mode == .end_failure) return error.Canceled;
            }
            fn step(_: *@This()) ?u64 {
                return null;
            }
        };
        var f: F = .{ .owner = &owner, .io = io, .mode = mode };
        defer {
            if (f.held) owner.mutex.unlock();
            owner.stop(io);
        }
        const scheduler = try backend.ptr().maintenanceScheduler();
        owner.registration = try scheduler.register(&f, F.step);
        // Isolate lifecycle-lock contention from callback completion races.
        owner.registration.?.requestStop();
        const deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (!try owner.registration.?.readyToFinish()) {
            if (@import("antfly_platform").time.monotonicNs() >= deadline_ns) return error.Timeout;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expect(owner.mutex.tryLock());
        f.held = true;
        const payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
        const result = owner.stopWithAdmission(io, .init(&payload));
        if (mode == .success) try result else {
            try std.testing.expectError(switch (mode) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .expired => error.DeadlineExceeded,
                else => unreachable,
            }, result);
            try std.testing.expect(owner.stopping.load(.acquire));
            try std.testing.expect(!owner.stop_started and !owner.joined.isSet() and owner.registration != null);
            if (f.held) {
                owner.mutex.unlock();
                f.held = false;
            }
            f.mode = .success;
            try owner.stopWithAdmission(io, .init(&payload));
        }
        try std.testing.expect(owner.joined.isSet() and owner.registration == null);
        try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
        try std.testing.expectEqual(@as(usize, 1), f.begins);
        try std.testing.expectEqual(@as(usize, if (mode == .begin_failure) 0 else 1), f.ends);
    }
}
