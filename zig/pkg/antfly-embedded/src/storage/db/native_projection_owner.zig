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
/// Local projection supervision. The borrowed round owns catalog pins and publication checks.
/// Idle owners retain a registration, never a sleeping per-owner I/O future.
pub const Owner = struct {
    pub const Port = struct { ptr: *anyopaque, io: std.Io, closing: *const std.atomic.Value(bool), round: *const fn (*anyopaque) anyerror!bool };
    enabled: bool = false,
    runtime: ?*runtime.BackendRuntime = null,
    mutex: std.atomic.Mutex = .unlocked,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    stop_progress: std.Io.Event = .unset,
    stop_generation: std.atomic.Value(u64) = .init(0),
    stop_waiters: std.atomic.Value(u32) = .init(0),
    port: Port = undefined,
    pub fn schedule(self: *Owner, port: Port) void {
        if (!self.enabled or self.stopping.load(.acquire) or port.closing.load(.acquire)) return;
        const backend = self.runtime orelse return;
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or port.closing.load(.acquire)) return;
        if (self.registration == null) {
            const scheduler = backend.maintenanceScheduler() catch |err| {
                std.log.warn("native projection scheduler admission deferred err={s}", .{@errorName(err)});
                return;
            };
            self.port = port;
            self.registration = scheduler.registerClass(.maintenance, self, step) catch |err| {
                std.log.warn("native projection maintenance admission deferred err={s}", .{@errorName(err)});
                return;
            };
            // Registration already queues the first round. A second wake here
            // could race that round and manufacture an extra initial pass.
            return;
        }
        backend.wakeMaintenance(self);
    }
    fn shutdown(self: *Owner) @import("maintenance_stop.zig").Borrowed {
        return .{ .mutex = &self.mutex, .registration = &self.registration, .stopping = &self.stopping, .finisher = &self.stop_started, .joined = &self.joined, .changed = &self.stop_progress, .generation = &self.stop_generation, .waiters = &self.stop_waiters };
    }
    pub fn stop(self: *Owner, io: ?std.Io) void {
        self.shutdown().stop(io, false);
    }
    pub fn stopWithAdmission(self: *Owner, io: ?std.Io, original: @import("../../common/read_admission.zig").Port) !void {
        try self.shutdown().stopWithAdmission(io, original);
    }
    fn closing(self: *Owner) bool {
        return self.stopping.load(.acquire) or self.port.closing.load(.acquire);
    }
    fn step(self: *Owner) ?u64 {
        if (self.closing()) return null;
        const pending = self.port.round(self.port.ptr) catch |err| blk: {
            std.log.warn("native projection maintenance deferred err={s}", .{@errorName(err)});
            break :blk retryable(err);
        };
        // Return the worker to the shared scheduler between rounds. Explicit
        // wakeups during a round remain recorded by the scheduler's epoch.
        return if (pending and !self.closing()) 50 else null;
    }
    pub fn retryable(err: anyerror) bool {
        return switch (err) {
            error.ResourceBudgetExceeded, error.OutOfMemory, error.WriterLocked, error.PostingCheckpointCaptureActive, error.PostingCheckpointSequenceMismatch, error.VectorBlockSnapshotAdvancedWithoutWal, error.VectorBlockGenerationReservationLost => true,
            else => false,
        };
    }
};

test "native projection owner joins observation and rejects admission after shutdown" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer backend.deinit();
    const io = backend.ptr().io().?;
    const F = struct {
        closing: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn round(ptr: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.closing.store(true, .release);
            return false;
        }
    };
    var f: F = .{};
    var owner: Owner = .{ .enabled = true, .runtime = backend.ptr() };
    defer owner.stop(io);
    const port: Owner.Port = .{ .ptr = &f, .io = io, .closing = &f.closing, .round = F.round };
    owner.schedule(port);
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!f.closing.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline)
        io.sleep(.fromMilliseconds(1), .awake) catch {};
    try std.testing.expect(f.closing.load(.acquire));
    owner.stop(io);
    f.closing.store(false, .release);
    owner.schedule(port);
    try std.testing.expect(owner.registration == null);
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    try std.testing.expect(Owner.retryable(error.ResourceBudgetExceeded));
    try std.testing.expect(!Owner.retryable(error.ArtifactCatalogCorrupt));
}

test "native projection owner controlled close lends the sole ordinary CPU and retains rejected joins" {
    const admission = @import("../../common/read_admission.zig");
    const cpu = @import("../../common/runnable_scheduler.zig");
    const now = @import("antfly_platform").time.monotonicNs;
    const Mode = enum { success, begin_failure, end_failure };
    for (std.enums.values(Mode)) |mode| {
        var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .lane_limits = .{ .durable_background = 8 } });
        defer backend.deinit();
        const io = backend.ptr().io().?;
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_wait_ms = 5000 });
        defer slots.close();
        var producer: cpu.Execution = .{};
        defer producer.finish();
        try producer.start(&slots, .{ .io = io, .native_now_ns = now, .allow_wait = true }, .background);
        try producer.park(.resource_blocked);
        var closer: cpu.Execution = .{};
        defer closer.finish();
        try closer.start(&slots, .{ .io = io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 5 * std.time.ns_per_s }, .query);
        var health: cpu.Execution = .{};
        defer health.finish();
        try health.start(&slots, .{ .io = io, .native_now_ns = now }, .control);
        const F = struct {
            io: std.Io,
            producer: *cpu.Execution,
            closing: std.atomic.Value(bool) = .init(false),
            entered: std.Io.Event = .unset,
            ran: bool = false,
            fn round(raw: *anyopaque) !bool {
                const self: *@This() = @ptrCast(@alignCast(raw));
                defer self.producer.finish();
                self.entered.set(self.io);
                try self.producer.enter(.background);
                self.ran = true;
                return false;
            }
        };
        const Control = struct {
            execution: *cpu.Execution,
            mode: Mode,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.execution.checkLifetime();
            }
            fn deadline(raw: *anyopaque) !?u64 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.execution.entry.?.options.deadline_ns;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.mode == .begin_failure) return error.NotFound;
                try self.execution.park(.resource_blocked);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                if (self.mode == .end_failure) return error.Canceled;
                try self.execution.enter(.query);
            }
        };
        var f: F = .{ .io = io, .producer = &producer };
        var owner: Owner = .{ .enabled = true, .runtime = backend.ptr() };
        defer {
            closer.finish();
            owner.stop(io);
        }
        owner.schedule(.{ .ptr = &f, .io = io, .closing = &f.closing, .round = F.round });
        try f.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        var control: Control = .{ .execution = &closer, .mode = mode };
        const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = owner.stopWithAdmission(io, .init(&payload));
        if (mode == .success) try result else {
            try std.testing.expectError(if (mode == .begin_failure) error.NotFound else error.Canceled, result);
            try std.testing.expect(owner.registration != null and !owner.stop_started and !owner.joined.isSet());
            closer.finish();
            owner.stop(io);
        }
        try std.testing.expect(f.ran and owner.registration == null and owner.joined.isSet());
        try std.testing.expectEqual(@as(usize, 1), control.begins);
        try std.testing.expectEqual(@as(usize, if (mode == .begin_failure) 0 else 1), control.ends);
        const scheduler = try backend.ptr().maintenanceScheduler();
        try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
        try std.testing.expectEqual(@as(usize, 1), scheduler.snapshot().peak_active);
        try std.testing.expectEqual(cpu.State.running, slots.state(&health.entry.?));
    }
}
