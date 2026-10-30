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

//! Embedded inference execution ownership for the compiled provider.
//! Embedded inference uses the same native scheduler and checked owner ABI as
//! node-hosted execution. The resource parent funds every retained capsule.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const native = @import("../common/runnable_scheduler.zig");
const resources = @import("resource_manager.zig");

pub const Port = struct {
    scheduler: *native.Scheduler,
    manager: *resources.ResourceManager,
    backing: std.mem.Allocator,
    lifetime_context: *anyopaque,
    retain_lifetime: *const fn (*anyopaque) callconv(.c) runtime.error_abi.Status,
    release_lifetime: *const fn (*anyopaque) callconv(.c) void,

    pub fn owner(self: *Port) abi.Owner {
        return .{ .context = self, .retain = retain, .release = release, .start = start, .checkpoint = checkpoint, .park = park, .enter = enter, .finish = finish, .check_lifetime = lifetime, .current_class = currentClass, .resize_team = resize, .enter_checked = enterChecked };
    }
    const Capsule = struct {
        port: *Port,
        io: runtime.io_abi.Receiver,
        clock: ?runtime.io_abi.Receiver,
        cancellation: runtime.http_abi.CancellationView,
        execution: native.Execution = .{},
        fn cancelled(raw: *const anyopaque) bool {
            const self: *const Capsule = @ptrCast(@alignCast(raw));
            return self.cancellation.requested();
        }
    };
    fn port(raw: *anyopaque) *Port {
        return @ptrCast(@alignCast(raw));
    }
    fn capsule(raw: *anyopaque) *Capsule {
        return @ptrCast(@alignCast(raw));
    }
    fn retain(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = port(raw);
        return self.retain_lifetime(self.lifetime_context);
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self = port(raw);
        self.release_lifetime(self.lifetime_context);
    }
    fn start(raw: *anyopaque, options: *const abi.Start) callconv(.c) runtime.error_abi.Status {
        startChecked(port(raw), options) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn startChecked(self: *Port, options: *const abi.Start) !void {
        try options.validate();
        // This embedded provider publishes only ordinary execution. It must
        // refuse unsupported durable/cleanup authority rather than emulate it.
        if (options.cleanup != 0 or options.resources != null) return error.UnsupportedOperation;
        const retained_bytes = std.math.add(u64, options.retained_bytes, @sizeOf(Capsule)) catch return error.ResourceBudgetExceeded;
        const retained = self.retain_lifetime(self.lifetime_context);
        if (!retained.isOk()) return runtime.error_abi.errorFromStatus(retained);
        errdefer self.release_lifetime(self.lifetime_context);
        var metadata = self.manager.controlMetadataAllocation(self.backing);
        const value = metadata.allocator().create(Capsule) catch |err| return metadata.allocationError(err);
        errdefer metadata.allocator().destroy(value);
        value.* = .{ .port = self, .io = try options.io.receive(), .clock = if (options.clock_io) |clock| try clock.receive() else null, .cancellation = options.cancellation };
        errdefer value.execution.finish();
        try value.execution.startWithSlots(self.scheduler, .{
            .io = value.io.io(),
            .clock_io = if (value.clock) |*clock| clock.io() else null,
            .native_now_ns = if (options.native_deadline != 0) @import("antfly_platform").time.monotonicNs else null,
            .deadline_ns = if (options.has_deadline != 0) options.deadline_ns else null,
            .cancellation = .{ .ptr = value, .requested = Capsule.cancelled },
            .allow_wait = options.allow_wait != 0,
            .retained_bytes = retained_bytes,
        }, std.enums.fromInt(native.Class, options.class) orelse return error.InvalidArgument, options.slots);
        options.out_handle.* = value;
    }
    fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        capsule(raw).execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        capsule(raw).execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
        const class = capsule(raw).execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err);
        out.* = @intCast(@backingInt(class));
        return .ok;
    }
    fn resize(raw: *anyopaque, slots: usize) callconv(.c) runtime.error_abi.Status {
        capsule(raw).execution.resizeTeam(slots) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
        const wait = std.enums.fromInt(abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        capsule(raw).execution.park(if (wait == .resource) .resource_blocked else .output_blocked) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
        const next = std.enums.fromInt(native.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        capsule(raw).execution.enter(next) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enterChecked(raw: *anyopaque, class: c_int, restriction: *const abi.Reentry) callconv(.c) runtime.error_abi.Status {
        restriction.validate() catch |err| return runtime.error_abi.statusFromError(err);
        const next = std.enums.fromInt(native.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        const Hook = struct {
            fn check(context: *anyopaque) !void {
                const fence: *const abi.Reentry = @ptrCast(@alignCast(context));
                try fence.checkpoint();
            }
        };
        capsule(raw).execution.enterWithCheck(next, .{ .context = @constCast(restriction), .check = Hook.check }) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn finish(raw: *anyopaque) callconv(.c) void {
        const value = capsule(raw);
        const self = value.port;
        value.execution.finish();
        var metadata = self.manager.controlMetadataAllocation(self.backing);
        metadata.allocator().destroy(value);
        self.release_lifetime(self.lifetime_context);
    }
};
