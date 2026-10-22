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
const runtime = @import("../background_runtime.zig");
const work = @import("group_source_worker.zig");
/// Resident-only lifecycle. Stop joins the slice before releasing cursor state.
pub const Owner = struct {
    pub const Port = struct { ptr: *anyopaque, runtime: *runtime.BackendRuntime, run: *const fn (*anyopaque) anyerror!bool };
    port: Port = undefined,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    mutex: std.atomic.Mutex = .unlocked,
    slice_mutex: std.atomic.Mutex = .unlocked,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    stop_progress: std.Io.Event = .unset,
    stop_generation: std.atomic.Value(u64) = .init(0),
    stop_waiters: std.atomic.Value(u32) = .init(0),
    worker: ?work.Worker = null,
    next_definition: usize = 0,
    submitted: u64 = 0,
    pub fn start(self: *Owner, port: Port) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or self.registration != null) return;
        self.port = port;
        const scheduler = port.runtime.maintenanceScheduler() catch return;
        self.registration = scheduler.registerClass(.producer, self, step) catch return;
    }
    fn shutdown(self: *Owner) @import("maintenance_stop.zig").Borrowed {
        return .{ .mutex = &self.mutex, .registration = &self.registration, .stopping = &self.stopping, .finisher = &self.stop_started, .joined = &self.joined, .changed = &self.stop_progress, .generation = &self.stop_generation, .waiters = &self.stop_waiters };
    }
    pub fn stop(self: *Owner, io: ?std.Io) void {
        self.shutdown().stop(io, false);
        while (!self.slice_mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        defer self.slice_mutex.unlock();
        self.clearWorker();
    }
    pub fn stopWithAdmission(self: *Owner, io: ?std.Io, original: @import("../../common/read_admission.zig").Port) !void {
        try self.shutdown().stopWithAdmission(io, original);
        try @import("../../common/cleanup_wait.zig").lockAtomic(&self.slice_mutex, io, original);
        defer self.slice_mutex.unlock();
        self.clearWorker();
    }
    fn clearWorker(self: *Owner) void {
        if (self.worker) |*worker| worker.deinit();
        self.worker = null;
    }
    fn step(self: *Owner) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        const changed = self.port.run(self.port.ptr) catch |err| {
            std.log.warn("group source processing deferred: {}", .{err});
            return 250;
        };
        return if (changed) 1 else 100;
    }
};

test "maintenance runtime group source retirement retains cursor state after original failure" {
    const admission = @import("../../common/read_admission.zig");
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    for (std.enums.values(Case)) |case| {
        var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{});
        defer backend.deinit();
        const io = backend.ptr().io().?;
        var owner: Owner = .{};
        owner.worker = .{ .allocator = std.testing.allocator, .identity = @splat(1) };
        try owner.worker.?.scopes.put(std.testing.allocator, @splat(2), .{ .source = try std.testing.allocator.dupe(u8, "source"), .group = try std.testing.allocator.dupe(u8, "group") });
        const F = struct {
            owner: *Owner,
            io: std.Io,
            entered: std.Io.Event = .unset,
            release: std.Io.Event = .unset,
            fn run(raw: *anyopaque) !bool {
                const self: *@This() = @ptrCast(@alignCast(raw));
                while (!self.owner.slice_mutex.tryLock()) @import("antfly_platform").time.yieldNow();
                defer self.owner.slice_mutex.unlock();
                self.entered.set(self.io);
                self.release.waitUncancelable(self.io);
                std.debug.assert(self.owner.worker != null);
                return false;
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
        var f: F = .{ .owner = &owner, .io = io };
        defer {
            f.release.set(io);
            owner.stop(io);
        }
        owner.start(.{ .ptr = &f, .runtime = backend.ptr(), .run = F.run });
        try f.entered.wait(io);
        var control: Control = .{ .f = &f, .case = case };
        var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = owner.stopWithAdmission(io, .init(&payload));
        if (case == .success) try result else {
            try std.testing.expectError(switch (case) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .lifetime_in_wait => error.DeadlineExceeded,
                .success => unreachable,
            }, result);
            try std.testing.expect(owner.registration != null and owner.worker != null);
            try std.testing.expectEqual(@as(usize, 1), owner.worker.?.scopes.count());
            f.release.set(io);
            owner.stop(io);
        }
        try std.testing.expect(owner.worker == null and owner.registration == null);
        try std.testing.expectEqual(@as(usize, 1), control.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), control.ends);
    }
}
