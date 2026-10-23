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
const DB = @import("../db.zig").DB;
const background = @import("../../background_runtime.zig");
const admission = @import("../../../common/read_admission.zig");
const cpu = @import("../../../common/runnable_scheduler.zig");
const now = @import("antfly_platform").time.monotonicNs;

test "maintenance producer close retains real DB owners through original failures and lends sole CPU" {
    const Mode = enum { success, begin_failure, end_failure, expired };
    inline for (.{ true, false }) |is_ttl| {
        for (std.enums.values(Mode)) |mode| {
            const a = std.testing.allocator;
            var backend = try background.BackendRuntimeHandle.init(a, .{});
            defer backend.deinit();
            const io = backend.ptr().io().?;
            var directory = std.testing.tmpDir(.{});
            defer directory.cleanup();
            const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-close", .{directory.sub_path});
            defer a.free(path);
            var db = try DB.open(a, path, .{
                .backend_runtime = backend.ptr(),
                .primary_backend = .{ .mem = .{} },
                .executor = .{ .backend = .manual },
                .start_index_workers = true,
                .start_optional_runtimes = false,
                .start_independent_maintenance = false,
            });
            defer db.close();
            if (is_ttl) try db.initOptionalTtlRuntime(.{ .enabled = true, .lease_owned = true }) else try db.initOptionalSparseCompactionRuntime(.{ .enabled = true });
            const runtime = if (is_ttl) db.ttl_runtime.? else db.sparse_compaction_runtime.?;
            // Ordinary stop remains restartable until permanent close.
            try runtime.start();
            _ = runtime.stop();
            try runtime.start();
            _ = runtime.stop();
            if (is_ttl) {
                try runtime.runOnce();
                try std.testing.expect(runtime.stats().has_lease);
            }
            var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
            defer slots.close();
            var closer: cpu.Execution = .{};
            defer closer.finish();
            try closer.start(&slots, .{ .io = io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 5 * std.time.ns_per_s }, .query);
            const Runtime = @TypeOf(runtime.*);
            const F = struct {
                runtime: *Runtime,
                io: std.Io,
                slots: *cpu.Scheduler,
                entered: std.Io.Event = .unset,
                release: std.Io.Event = .unset,
                retired: std.atomic.Value(bool) = .init(false),
                guards_free: bool = false,
                failure: ?anyerror = null,
                fn step(self: *@This()) ?u64 {
                    self.entered.set(self.io);
                    self.release.waitUncancelable(self.io);
                    self.work() catch |cause| {
                        self.failure = cause;
                    };
                    self.retired.store(true, .release);
                    return null;
                }
                fn work(self: *@This()) !void {
                    var execution: cpu.Execution = .{};
                    defer execution.finish();
                    try execution.start(self.slots, .{ .io = self.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 5 * std.time.ns_per_s }, .background);
                    if (!self.runtime.lifecycle_mutex.tryLock()) return error.TestUnexpectedResult;
                    if (is_ttl) self.runtime.lifecycle_mutex.unlock(self.io) else self.runtime.lifecycle_mutex.unlock();
                    if (!self.runtime.mutex.tryLock()) return error.TestUnexpectedResult;
                    self.runtime.mutex.unlock(self.io);
                    self.guards_free = true;
                }
            };
            var f: F = .{ .runtime = runtime, .io = io, .slots = &slots };
            defer {
                closer.finish();
                f.release.set(io);
                _ = runtime.stop();
            }
            const scheduler = try backend.ptr().maintenanceScheduler();
            runtime.future = try scheduler.register(&f, F.step);
            try f.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            const Control = struct {
                f: *F,
                closer: *cpu.Execution,
                mode: Mode,
                begins: usize = 0,
                ends: usize = 0,
                fn lifetime(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.closer.checkLifetime();
                    if (self.mode == .expired and self.begins != 0) return error.DeadlineExceeded;
                }
                fn checkpoint(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try lifetime(raw);
                    if (self.f.slots.state(&self.closer.entry.?) != .running) return error.InvalidRunnableTransition;
                }
                fn deadline(raw: *anyopaque) !?u64 {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    return self.closer.entry.?.options.deadline_ns;
                }
                fn begin(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.begins += 1;
                    if (self.mode == .begin_failure) return error.NotFound;
                    try self.closer.park(.resource_blocked);
                    if (self.mode != .expired) self.f.release.set(self.f.io);
                }
                fn end(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.ends += 1;
                    self.f.release.set(self.f.io);
                    if (self.mode == .end_failure) return error.Canceled;
                    try self.closer.enter(.query);
                }
            };
            var control: Control = .{ .f = &f, .closer = &closer, .mode = mode };
            var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.checkpoint, .check_lifetime = Control.lifetime, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
            const original = admission.Port.init(&payload);
            const result = runtime.stopWithAdmission(original);
            if (mode == .success) try result else {
                try std.testing.expectError(switch (mode) {
                    .begin_failure => error.NotFound,
                    .end_failure => error.Canceled,
                    .expired => error.DeadlineExceeded,
                    else => unreachable,
                }, result);
                try std.testing.expect(runtime.future != null and !runtime.joined.isSet() and !runtime.join_finisher);
                if (mode == .end_failure) try closer.enter(.query);
                control.mode = .success;
                try runtime.stopWithAdmission(original);
            }
            try std.testing.expect(runtime.closing.load(.acquire));
            try std.testing.expect(runtime.joined.isSet() and runtime.future == null);
            try std.testing.expect(f.retired.load(.acquire) and f.guards_free);
            if (f.failure) |cause| return cause;
            try std.testing.expect(control.begins != 0);
            try std.testing.expectEqual(control.begins, control.ends + @as(usize, if (mode == .begin_failure) 1 else 0));
            try std.testing.expectError(error.Canceled, runtime.start());
            try std.testing.expectError(error.Canceled, runtime.resumeAfterPause());
            try std.testing.expectError(error.Canceled, runtime.ensureRunning());
            try std.testing.expectError(error.Canceled, runtime.runOnce());
            if (is_ttl) {
                try std.testing.expect(runtime.stats().has_lease);
                try std.testing.expect(db.ttl_cleanup_context != null);
            } else runtime.notify();
            try runtime.stopWithAdmission(original);
            try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
        }
    }
}
