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

//! Final propagation retirement retains registrations and synchronous replay borrows.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const cleanup = @import("../../common/cleanup_wait.zig");
const maintenance = @import("maintenance_stop.zig");
const Handle = @import("../../common/maintenance_scheduler.zig").Scheduler.Handle;

pub const State = struct {
    mutex: std.atomic.Mutex = .unlocked,
    closing: std.atomic.Value(bool) = .init(false),
    finisher: bool = false,
    joined: std.Io.Event = .unset,
    changed: std.Io.Event = .unset,
    generation: std.atomic.Value(u64) = .init(0),
    waiters: std.atomic.Value(u32) = .init(0),

    pub fn lock(self: *State) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub fn unlock(self: *State) void {
        self.mutex.unlock();
    }
    fn borrow(self: *State, future: *?Handle) maintenance.Borrowed {
        return .{ .mutex = &self.mutex, .registration = future, .stopping = &self.closing, .finisher = &self.finisher, .joined = &self.joined, .changed = &self.changed, .generation = &self.generation, .waiters = &self.waiters };
    }
    pub fn stopWithAdmission(self: *State, future: *?Handle, io: ?std.Io, shutdown: *std.atomic.Value(bool), replay: *std.atomic.Mutex, started: *std.atomic.Value(bool), original: admission.Port) !void {
        self.closing.store(true, .release);
        shutdown.store(true, .release);
        try self.borrow(future).stopWithAdmission(io, original);
        // New entrants recheck closing under this same gate. Acquiring it
        // after valid reentry proves every earlier callback/reader has left.
        try cleanup.lockAtomic(replay, io, original);
        replay.unlock();
        started.store(false, .release);
    }
    pub fn finish(self: *State, future: *?Handle, io: ?std.Io, shutdown: *std.atomic.Value(bool), replay: *std.atomic.Mutex, started: *std.atomic.Value(bool)) void {
        self.closing.store(true, .release);
        shutdown.store(true, .release);
        self.borrow(future).stop(io, false);
        @import("antfly_platform").sync.lockYielding(replay);
        replay.unlock();
        started.store(false, .release);
    }
};

test "controlled propagation shutdown retains scheduled and direct replay owners across original failures" {
    const background = @import("../background_runtime.zig");
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    inline for (.{ @import("resolution_runtime.zig").ResolutionRuntime, @import("promotion_runtime.zig").PromotionRuntime }) |Runtime| {
        for (std.enums.values(Case)) |case| {
            for ([_]bool{ false, true }) |direct| {
                var host = try background.BackendRuntimeHandle.init(std.testing.allocator, .{});
                defer host.deinit();
                const io = host.ptr().io().?;
                var backend = @import("../mem_backend.zig").Backend.init(std.testing.allocator, .{});
                defer backend.close();
                var store = try backend.runtimeStore(std.testing.allocator, .{ .name = "propagation-close" });
                defer store.deinit();
                var runtime: Runtime = if (Runtime == @import("resolution_runtime.zig").ResolutionRuntime) .{
                    .alloc = std.testing.allocator,
                    .store_handle = .{ .store = store, .owned = false },
                    .replay_source = undefined,
                    .index_manager = undefined,
                    .write_ctx = undefined,
                    .write_fn = undefined,
                    .io = io,
                    .backend_runtime = host.ptr(),
                    .candidate_source = null,
                    .embedder = null,
                    .applied_sequence = .init(0),
                    .target_sequence = .init(2),
                    .shutdown_flag = .init(false),
                    .worker_started = .init(false),
                    .future = null,
                    .replay_progress = .{ .base_applied_sequence = 0, .sequence = 1, .next_key = 7 },
                } else .{
                    .alloc = std.testing.allocator,
                    .store_handle = .{ .store = store, .owned = false },
                    .replay_source = undefined,
                    .owner = null,
                    .sink = null,
                    .sink_available = .init(false),
                    .missing_sink_blocked = .init(false),
                    .missing_sink_policy = .wait,
                    .applied_sequence = .init(0),
                    .target_sequence = .init(2),
                    .error_count = .init(0),
                    .shutdown_flag = .init(false),
                    .io = io,
                    .backend_runtime = host.ptr(),
                    .future = null,
                };
                defer runtime.deinit();
                const F = struct {
                    runtime: *Runtime,
                    io: std.Io,
                    entered: std.Io.Event = .unset,
                    release: std.Io.Event = .unset,
                    fn run(self: *@This()) void {
                        // This is the same exclusive gate held by real replay,
                        // candidate callbacks and promotion sink publication.
                        @import("antfly_platform").sync.lockYielding(&self.runtime.catch_up_mutex);
                        defer self.runtime.catch_up_mutex.unlock();
                        self.entered.set(self.io);
                        self.release.waitUncancelable(self.io);
                        var write = self.runtime.store_handle.store.beginWrite() catch @panic("store retired early");
                        errdefer write.abort();
                        write.put("retained", "complete") catch @panic("store retired early");
                        write.commit() catch @panic("store retired early");
                    }
                    fn step(self: *@This()) ?u64 {
                        self.run();
                        return null;
                    }
                };
                var fixture: F = .{ .runtime = &runtime, .io = io };
                var task: ?std.Io.Future(void) = null;
                defer {
                    fixture.release.set(io);
                    if (task) |*future| future.await(io);
                    runtime.stop();
                }
                if (direct) task = try io.concurrent(F.run, .{&fixture}) else {
                    runtime.future = try (try host.ptr().maintenanceScheduler()).registerClass(.propagation, &fixture, F.step);
                    runtime.worker_started.store(true, .release);
                }
                try fixture.entered.wait(io);
                const Control = struct {
                    fixture: *F,
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
                        if (self.case != .lifetime_in_wait) self.fixture.release.set(self.fixture.io);
                    }
                    fn end(raw: *anyopaque) !void {
                        const self: *@This() = @ptrCast(@alignCast(raw));
                        self.ends += 1;
                        self.fixture.release.set(self.fixture.io);
                        if (self.case == .end_failure) return error.Canceled;
                    }
                };
                var control: Control = .{ .fixture = &fixture, .case = case };
                var payload: admission.Payload = .{
                    .ptr = &control,
                    .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline },
                    .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end },
                };
                const result = runtime.stopWithAdmission(.init(&payload));
                if (case == .success) try result else {
                    try std.testing.expectError(switch (case) {
                        .begin_failure => error.NotFound,
                        .end_failure => error.Canceled,
                        .lifetime_in_wait => error.DeadlineExceeded,
                        .success => unreachable,
                    }, result);
                    try std.testing.expect(runtime.lifecycle.closing.load(.acquire));
                    if (!direct) try std.testing.expect(runtime.future != null);
                    if (Runtime == @import("resolution_runtime.zig").ResolutionRuntime) try std.testing.expectEqual(@as(usize, 7), runtime.replay_progress.?.next_key);
                    fixture.release.set(io);
                    control.case = .success;
                    try runtime.stopWithAdmission(.init(&payload));
                }
                try std.testing.expect(runtime.future == null and !runtime.worker_started.load(.acquire));
                try std.testing.expectEqual(@as(u64, 0), runtime.applied_sequence.load(.acquire));
                try std.testing.expectEqual(@as(u64, 2), runtime.target_sequence.load(.acquire));
                try std.testing.expectError(error.Canceled, runtime.start());
                try std.testing.expectError(error.Canceled, runtime.catchUp());
                if (Runtime == @import("resolution_runtime.zig").ResolutionRuntime) {
                    try std.testing.expectError(error.Canceled, runtime.catchUpOnce(1000));
                    try std.testing.expectError(error.Canceled, runtime.requestReresolveBacklog());
                    try std.testing.expectEqual(@as(usize, 7), runtime.replay_progress.?.next_key);
                }
                var read = try store.beginRead();
                defer read.abort();
                try std.testing.expectEqualStrings("complete", try read.get("retained"));
                try std.testing.expectEqual(control.begins - @intFromBool(case == .begin_failure), control.ends);
            }
        }
    }
}
