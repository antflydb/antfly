// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! An actual dependency wait after attempt-local service leases have retired.
//! The caller retains the original request, buffers and physical ownership.
const operation = @import("antfly_local_sources").api_operation;
const runtime = @import("antfly_runtime_abi");

pub fn run(request: operation.RequestContext, comptime wait: anytype, args: anytype) !void {
    try request.ensureExecutionActive();
    const team = request.execution_team orelse {
        try @call(.auto, wait, args);
        try request.ensureExecutionActive();
        return;
    };
    const borrow = team.borrow;
    try borrow.validate();
    if (borrow.enter_checked == null) return error.ExecutionWaitUnsupported;
    const class = try borrow.currentClass();
    const parked = borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource));
    if (!parked.isOk()) return runtime.error_abi.errorFromStatus(parked);
    const result = @call(.auto, wait, args);
    const Restriction = struct {
        request: operation.RequestContext,
        failure: ?anyerror = null,
        fn active(self: *@This()) !void {
            if (self.failure) |cause| return cause;
            self.request.ensureExecutionActive() catch |cause| {
                self.failure = cause;
                return cause;
            };
        }
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active() catch |cause| return runtime.error_abi.statusFromError(cause);
            return .ok;
        }
    };
    var restriction: Restriction = .{ .request = request };
    restriction.active() catch {};
    // Always attempt the original mandatory rejoin, including after wait or
    // scope failure. The original scope also restricts every queued/grant check.
    const resumed = borrow.enterChecked(class, &.{ .context = &restriction, .check = Restriction.check });
    if (restriction.failure) |cause| return cause;
    try resumed;
    try restriction.active();
    try result;
}

test "runnable dependency wait retains original charges permits producers and rejects late scope changes" {
    const std = @import("std");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Read = @import("runnable_local_read.zig").Read;
    const Mode = enum { success, physical_error, canceled, expired, shutdown, scope_error, wait_scope_error, late_scope, unsupported, park_error, background };
    const Clock = struct {
        var now_ns: u64 = 10;
        fn now() u64 {
            return now_ns;
        }
    };
    const Fixture = struct {
        scheduler: *native.Scheduler,
        mode: Mode,
        signal: *std.atomic.Value(bool),
        bytes: []u8,
        calls: usize = 0,
        scope_calls: usize = 0,
        after_wait: bool = false,
        cause: anyerror = error.CatalogGenerationChanged,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (!self.after_wait) return;
            self.scope_calls += 1;
            if (self.mode == .scope_error or (self.mode == .wait_scope_error and self.scope_calls == 1) or (self.mode == .late_scope and self.scope_calls == 4)) return self.cause;
        }
        fn rejectPark(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.Canceled);
        }
        fn wait(self: *@This()) !void {
            self.calls += 1;
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().resource_blocked);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), self.scheduler.stats().retained_bytes);
            var producer: native.Execution = .{};
            defer producer.finish();
            try producer.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false, .native_now_ns = Clock.now, .retained_bytes = 17 }, .write);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: native.Execution = .{};
            defer health.finish();
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false, .native_now_ns = Clock.now }, .control);
            @memset(self.bytes, 42);
            self.after_wait = true;
            switch (self.mode) {
                .physical_error => return error.PhysicalWaitFailure,
                .wait_scope_error => try check(self),
                .canceled => self.signal.store(true, .release),
                .expired => Clock.now_ns = 100,
                .shutdown => self.scheduler.close(),
                else => {},
            }
        }
    };
    for (std.enums.values(Mode)) |mode| {
        for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.CatalogGenerationChanged }) |cause| {
            Clock.now_ns = 10;
            var scheduler = try native.Scheduler.init(.{ .slots = 2, .max_operations = 8, .max_bytes = 512, .control_bytes = 64 });
            defer scheduler.close();
            var signal = std.atomic.Value(bool).init(false);
            var execution: native.Execution = .{};
            defer execution.finish();
            const bytes = try std.testing.allocator.alloc(u8, 17);
            defer std.testing.allocator.free(bytes);
            try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 23, .cancellation = .{ .ptr = &signal, .requested = struct {
                fn requested(raw: *const anyopaque) bool {
                    return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
                }
            }.requested } }, if (mode == .background) .background else .query);
            var read: Read = .{ .execution = &execution, .allocator = std.testing.allocator };
            var team = try read.team();
            if (mode == .unsupported) team.borrow.enter_checked = null;
            if (mode == .park_error) team.borrow.park = Fixture.rejectPark;
            var fixture: Fixture = .{ .scheduler = &scheduler, .mode = mode, .signal = &signal, .bytes = bytes, .cause = cause };
            const request: operation.RequestContext = .{ .execution_team = &team, .cancellation = .{ .ptr = &fixture, .check_fn = Fixture.check } };
            const result = run(request, Fixture.wait, .{&fixture});
            const expected: ?anyerror = switch (mode) {
                .success, .background => null,
                .physical_error => error.PhysicalWaitFailure,
                .canceled, .park_error => error.Canceled,
                .expired => error.DeadlineExceeded,
                .shutdown => error.SchedulerClosed,
                .scope_error, .wait_scope_error, .late_scope => cause,
                .unsupported => error.ExecutionWaitUnsupported,
            };
            if (expected) |failure| try std.testing.expectError(failure, result) else try result;
            try std.testing.expectEqual(@as(usize, if (mode == .unsupported or mode == .park_error) 0 else 1), fixture.calls);
            if (fixture.calls != 0) try std.testing.expect(std.mem.allEqual(u8, bytes, 42));
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
            if (mode == .late_scope) try std.testing.expectEqual(@as(usize, 4), fixture.scope_calls);
            if (mode == .scope_error or mode == .late_scope) try std.testing.expectEqual(native.State.stopped, execution.entry.?.state);
            if (mode == .background) try std.testing.expectEqual(native.Class.background, try execution.retainedClass());
            execution.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}
