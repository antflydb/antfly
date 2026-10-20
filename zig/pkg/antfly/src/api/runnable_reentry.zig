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

//! Synchronous ABI-to-native CPU-return restrictions. Neither adapter stores
//! the caller's check beyond return or replaces the execution's authority.
const runtime = @import("antfly_runtime_abi");
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub fn enter(execution: *native.Execution, class: native.Class, restriction: *const runtime.runnable_abi.Reentry) !void {
    try restriction.validate();
    // AdmissionCheck is synchronous and const; the opaque ABI context does
    // not imply mutable or retained ownership of the caller's restriction.
    try execution.enterWithCheck(class, .{ .context = @ptrCast(@constCast(restriction)), .check = check });
}
pub fn activateChild(execution: *native.Execution, restriction: *const runtime.runnable_abi.Reentry) !void {
    try restriction.validate();
    try execution.activateChildWithCheck(.{ .context = @ptrCast(@constCast(restriction)), .check = check });
}
pub fn enterWithResources(execution: *native.Execution, resources: native.GrantResources, restriction: ?*const runtime.runnable_abi.Reentry) !void {
    if (restriction) |value| try value.validate();
    try execution.enterWithResources(resources, if (restriction) |value| .{ .context = @ptrCast(@constCast(value)), .check = check } else null);
}

fn check(raw: *anyopaque) !void {
    const restriction: *const runtime.runnable_abi.Reentry = @ptrCast(@alignCast(raw));
    try restriction.checkpoint();
}

test "controlled CPU reentry stops queued and newly granted executions without retiring ownership" {
    const std = @import("std");
    const Fixture = struct {
        scheduler: *native.Scheduler,
        cause: anyerror,
        calls: usize = 0,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // This locks the scheduler: callback execution under its mutex
            // would deadlock, including the post-grant checkpoint.
            std.debug.assert(self.scheduler.stats().operations >= 1);
            self.calls += 1;
            if (self.calls == 3) return runtime.error_abi.statusFromError(self.cause);
            return .ok;
        }
    };
    for ([_]bool{ false, true }) |occupied| {
        for ([_]anyerror{ error.CatalogRoutingSnapshotTimeout, error.Canceled, error.PreDecisionDeadlineExceeded }) |cause| {
            var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
            defer scheduler.close();
            var original: native.Execution = .{};
            defer original.finish();
            try original.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .write);
            try original.park(.resource_blocked);
            var holder: native.Execution = .{};
            defer holder.finish();
            if (occupied) try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
            var fixture: Fixture = .{ .scheduler = &scheduler, .cause = cause };
            try std.testing.expectError(cause, enter(&original, .write, &.{ .context = &fixture, .check = Fixture.check }));
            try std.testing.expectEqual(@as(usize, 3), fixture.calls);
            try std.testing.expectEqual(native.State.stopped, original.entry.?.state);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().queued);
            try std.testing.expectEqual(@as(usize, @intFromBool(occupied)), scheduler.stats().runnable_slots);
            try std.testing.expectEqual(1 + @as(usize, @intFromBool(occupied)), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, if (occupied) 40 else 17), scheduler.stats().retained_bytes);
            try std.testing.expectError(cause, original.checkLifetime());
            var health: native.Execution = .{};
            try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            original.finish();
            holder.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}

test "controlled CPU reentry forwards the original restriction through native and ABI borrowing" {
    const std = @import("std");
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var original: native.Execution = .{};
    defer original.finish();
    try original.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    var read: @import("runnable_local_read.zig").Read = .{ .execution = &original, .allocator = std.testing.allocator };
    const team = try read.team();
    var bridge = try @import("runnable_native_borrow.zig").Bridge.init(&team.borrow, std.testing.allocator);
    var borrower: native.Execution = .{};
    defer borrower.finish();
    try bridge.bind(&borrower);
    try borrower.park(.resource_blocked);
    var holder: native.Execution = .{};
    defer holder.finish();
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    const Check = struct {
        scheduler: *native.Scheduler,
        calls: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scheduler.stats().operations == 2);
            self.calls += 1;
            if (self.calls >= 3) return error.CatalogRoutingSnapshotTimeout;
        }
    };
    var restriction: Check = .{ .scheduler = &scheduler };
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, borrower.enterWithCheck(.query, .{ .context = &restriction, .check = Check.check }));
    borrower.finish();
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
    original.finish();
    holder.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "controlled CPU reentry rejects incompatible ABI restrictions without invoking callbacks" {
    const std = @import("std");
    const Probe = struct {
        calls: usize = 0,
        fn ok(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return .ok;
        }
        fn phase(raw: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return ok(raw);
        }
        fn enterChecked(raw: *anyopaque, _: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
            restriction.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
            return ok(raw);
        }
    };
    var probe: Probe = .{};
    var borrow: runtime.runnable_abi.Borrowed = .{ .context = &probe, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Probe.ok, .park = Probe.phase, .enter = Probe.phase };
    var restriction: runtime.runnable_abi.Reentry = .{ .context = &probe, .check = Probe.ok };
    try std.testing.expectError(error.ExecutionWaitUnsupported, borrow.enterChecked(.query, &restriction));
    borrow.enter_checked = Probe.enterChecked;
    restriction.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.enterChecked(.query, &restriction));
    restriction.version += 1;
    restriction.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.enterChecked(.query, &restriction));
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    restriction.struct_size += 1;
    try borrow.enterChecked(.query, &restriction);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}
